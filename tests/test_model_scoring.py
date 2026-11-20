"""Tests for the fraud model wrapper and pandas scoring (no Spark)."""

from __future__ import annotations

import pytest

from lakehouse.ml.features import FEATURE_NAMES
from lakehouse.ml.model import (
    HeuristicFraudModel,
    load_model,
    load_or_default,
    save_model,
)
from lakehouse.ml.scoring import score_pandas

pd = pytest.importorskip("pandas")


def _silver_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {  # benign domestic POS grocery
                "event_time": "2026-06-13T14:00:00+00:00",
                "amount_pln": 50.0,
                "channel": "POS",
                "merchant_category": "grocery",
                "country": "PL",
            },
            {  # risky: night, online, foreign, gambling, high amount
                "event_time": "2026-06-13T03:00:00+00:00",
                "amount_pln": 9000.0,
                "channel": "ONLINE",
                "merchant_category": "gambling",
                "country": "US",
            },
        ]
    )


def test_heuristic_scores_in_unit_interval() -> None:
    model = HeuristicFraudModel()
    scores = score_pandas(_silver_frame(), model)
    assert len(scores) == 2
    assert all(0.0 <= s <= 1.0 for s in scores)


def test_heuristic_ranks_risky_above_benign() -> None:
    model = HeuristicFraudModel()
    scores = score_pandas(_silver_frame(), model).tolist()
    assert scores[1] > scores[0]


def test_score_one_matches_vectorized() -> None:
    model = HeuristicFraudModel()
    from lakehouse.ml.features import build_features

    record = {
        "event_time": "2026-06-13T03:00:00+00:00",
        "amount_pln": 9000.0,
        "channel": "ONLINE",
        "merchant_category": "gambling",
        "country": "US",
    }
    one = model.score_one(build_features(record))
    vec = float(score_pandas(pd.DataFrame([record]), model).iloc[0])
    assert one == pytest.approx(vec, rel=1e-9)


def test_empty_frame_returns_empty_series() -> None:
    model = HeuristicFraudModel()
    empty = pd.DataFrame(
        {c: [] for c in ("event_time", "amount_pln", "channel", "merchant_category", "country")}
    )
    assert len(score_pandas(empty, model)) == 0


def test_save_load_roundtrip(tmp_path) -> None:
    model = HeuristicFraudModel(threshold=0.7)
    path = tmp_path / "m.pkl"
    save_model(model, path)
    loaded = load_model(path)
    assert loaded is not None
    assert loaded.threshold == 0.7
    assert tuple(loaded.feature_names) == FEATURE_NAMES


def test_load_or_default_falls_back(tmp_path) -> None:
    missing = tmp_path / "does-not-exist.pkl"
    model = load_or_default(missing)
    assert isinstance(model, HeuristicFraudModel)


def test_load_missing_returns_none(tmp_path) -> None:
    assert load_model(tmp_path / "nope.pkl") is None


# --- TP-02: integralność artefaktu przed unpicklingiem ------------------------


def test_tampered_artifact_is_not_unpickled(tmp_path) -> None:
    path = tmp_path / "m.pkl"
    save_model(HeuristicFraudModel(threshold=0.7), path)
    path.write_bytes(path.read_bytes() + b"tampered")
    assert load_model(path) is None
    assert isinstance(load_or_default(path), HeuristicFraudModel)


def test_unsigned_artifact_is_rejected(tmp_path) -> None:
    import pickle

    path = tmp_path / "m.pkl"
    path.write_bytes(pickle.dumps(HeuristicFraudModel()))
    assert load_model(path) is None


def test_hmac_key_requires_hmac_signature(tmp_path, monkeypatch) -> None:
    path = tmp_path / "m.pkl"
    save_model(HeuristicFraudModel(threshold=0.6), path)  # podpis SHA-256 (bez klucza)
    monkeypatch.setenv("TRANSACTPULSE_MODEL_HMAC_KEY", "k" * 32)
    assert load_model(path) is None  # sam hash nie wystarcza, gdy klucz jest ustawiony
    save_model(HeuristicFraudModel(threshold=0.6), path)
    assert load_model(path).threshold == 0.6
    monkeypatch.setenv("TRANSACTPULSE_MODEL_HMAC_KEY", "x" * 32)
    assert load_model(path) is None  # inny klucz = odrzucenie
