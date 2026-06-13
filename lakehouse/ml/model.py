"""Fraud model artifact: a swappable scorer loaded from a pickle.

``FraudModel`` is the integration seam (ADR 0006). Two implementations ship:

* :class:`SklearnFraudModel` — wraps a fitted scikit-learn estimator;
* :class:`HeuristicFraudModel` — a deterministic, dependency-free fallback used
  when no artifact is available, so the gold pipeline always produces scores.
"""

from __future__ import annotations

import math
import pickle
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
            weight * float(feature_vector.get(name, 0.0))
            for name, weight in self._weights.items()
        )
        return 1.0 / (1.0 + math.exp(-z))


def save_model(model: FraudModel, path: str | Path) -> None:
    """Pickle a model artifact to ``path`` (creating parent dirs)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("wb") as fh:
        pickle.dump(model, fh)


def load_model(path: str | Path) -> FraudModel | None:
    """Load a pickled model artifact, or ``None`` if missing/unloadable."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        with p.open("rb") as fh:
            return pickle.load(fh)  # noqa: S301 - trusted local artifact
    except Exception:  # noqa: BLE001 - any load failure -> caller falls back
        return None


def load_or_default(path: str | Path) -> FraudModel:
    """Load the artifact at ``path`` or fall back to the heuristic model."""
    return load_model(path) or HeuristicFraudModel()
