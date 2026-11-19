"""Fraud model artifact: a swappable scorer loaded from a pickle.

``FraudModel`` is the integration seam (ADR 0006). Two implementations ship:

* :class:`SklearnFraudModel` — wraps a fitted scikit-learn estimator;
* :class:`HeuristicFraudModel` — a deterministic, dependency-free fallback used
  when no artifact is available, so the gold pipeline always produces scores.

Security note: the pickle is only ever read from a path this project writes
itself (the ``train_fraud_model`` job → the ``tp-models`` volume). Never point
``GOLD_MODEL_PATH`` at an artifact from an untrusted source — unpickling
executes code. See SECURITY.md.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import os
import pickle  # nosec B403
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from lakehouse.ml.features import FEATURE_NAMES

if TYPE_CHECKING:  # pragma: no cover
    import numpy as np
    import pandas as pd

DEFAULT_THRESHOLD = 0.5


@runtime_checkable
class FraudModel(Protocol):
    """A scorer mapping a feature DataFrame to fraud probabilities in [0, 1]."""

    feature_names: tuple[str, ...]
    threshold: float
    reference: dict[str, Any]

    def score(self, features: pd.DataFrame) -> np.ndarray:
        """Return P(fraud) for each row of ``features``."""
        ...


@dataclass
class SklearnFraudModel:
    """Wraps a fitted scikit-learn estimator exposing ``predict_proba``."""

    estimator: Any
    feature_names: tuple[str, ...] = FEATURE_NAMES
    threshold: float = DEFAULT_THRESHOLD
    reference: dict[str, Any] = field(default_factory=dict)
    kind: str = "sklearn"

    def score(self, features: pd.DataFrame) -> np.ndarray:
        proba = self.estimator.predict_proba(features[list(self.feature_names)])
        return proba[:, 1]


@dataclass
class HeuristicFraudModel:
    """Deterministic logistic scorer over the engineered features (no deps)."""

    feature_names: tuple[str, ...] = FEATURE_NAMES
    threshold: float = DEFAULT_THRESHOLD
    reference: dict[str, Any] = field(default_factory=dict)
    kind: str = "heuristic"

    #: Hand-tuned logistic weights; intercept keeps the base rate low.
    _intercept: float = -4.2
    _weights: dict[str, float] = field(
        default_factory=lambda: {
            "log_amount_pln": 0.25,
            "is_night": 0.4,
            "is_online": 1.0,
            "is_atm": 0.5,
            "is_high_risk_category": 1.3,
            "is_foreign": 0.9,
        }
    )

    def score(self, features: pd.DataFrame) -> np.ndarray:
        import numpy as np

        z = np.full(len(features), self._intercept, dtype="float64")
        for name, weight in self._weights.items():
            if name in features:
                z += weight * features[name].to_numpy(dtype="float64")
        return 1.0 / (1.0 + np.exp(-z))

    def score_one(self, feature_vector: dict[str, float]) -> float:
        """Score a single feature dict (pure Python; used in lightweight tests)."""
        z = self._intercept + sum(
            weight * float(feature_vector.get(name, 0.0)) for name, weight in self._weights.items()
        )
        return 1.0 / (1.0 + math.exp(-z))


# TP-02: artefakt modelu to pickle (estymator scikit-learn), a unpickling wykonuje kod.
# Obok artefaktu zapisujemy podpis: HMAC-SHA256 z kluczem TRANSACTPULSE_MODEL_HMAC_KEY
# (zalecane — chroni przed podmianą przez kogoś z zapisem do wolumenu), a bez klucza
# SHA-256 (wykrywa uszkodzenie lub podmianę pliku bez aktualizacji sygnatury).
# load_model NIE rozpakowuje artefaktu bez zgodnego podpisu — wraca None (fallback).
MODEL_KEY_ENV = "TRANSACTPULSE_MODEL_HMAC_KEY"


def _signature_path(path: Path) -> Path:
    return path.with_name(path.name + ".sig")


def _sign(data: bytes) -> str:
    key = os.environ.get(MODEL_KEY_ENV, "")
    if key:
        return "hmac-sha256:" + hmac.new(key.encode(), data, hashlib.sha256).hexdigest()
    return "sha256:" + hashlib.sha256(data).hexdigest()


def save_model(model: FraudModel, path: str | Path) -> None:
    """Pickle a model artifact to ``path`` and write its integrity signature."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = pickle.dumps(model)
    p.write_bytes(data)
    _signature_path(p).write_text(_sign(data) + "\n", encoding="ascii")


def load_model(path: str | Path) -> FraudModel | None:
    """Load a signed model artifact, or ``None`` if missing, unsigned, tampered or unloadable."""
    p = Path(path)
    sig = _signature_path(p)
    if not p.exists() or not sig.exists():
        return None
    try:
        data = p.read_bytes()
        expected = sig.read_text(encoding="ascii").strip()
        # Klucz skonfigurowany = akceptujemy wyłącznie podpis HMAC (sam SHA-256 może
        # wyliczyć każdy, kto podmienia plik).
        if os.environ.get(MODEL_KEY_ENV) and not expected.startswith("hmac-sha256:"):
            return None
        if not hmac.compare_digest(expected, _sign(data)):
            return None
        # Artefakt zweryfikowany powyżej; pochodzi z naszego joba treningowego.
        return pickle.loads(data)  # noqa: S301  # nosec B301
    except Exception:  # noqa: BLE001 - any load failure -> caller falls back
        return None


def load_or_default(path: str | Path) -> FraudModel:
    """Load the artifact at ``path`` or fall back to the heuristic model."""
    return load_model(path) or HeuristicFraudModel()
