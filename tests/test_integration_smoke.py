"""Pipeline smoke test exercising the pure components end to end (no Spark/Docker).

generator → schema validation → feature engineering → model scoring → drift.
This verifies the components compose correctly; the full Spark/Kafka/MinIO flow is
covered by ``infra/integration-test.sh`` against a running stack.
"""

from __future__ import annotations

import pytest

from ingestion.config import GeneratorConfig
from ingestion.generator import TransactionGenerator
from ingestion.schema import validate
from lakehouse.drift import population_stability_index
from lakehouse.fx_rates import FX_RATES_TO_PLN
from lakehouse.ml.model import HeuristicFraudModel
from lakehouse.ml.scoring import score_pandas

pd = pytest.importorskip("pandas")


def _silver_like(n: int, seed: int) -> pd.DataFrame:
    """Generate transactions and project them into a silver-like frame."""
    gen = TransactionGenerator(GeneratorConfig(seed=seed, num_accounts=500, num_devices=700))
    rows = []
    for tx in gen.stream(n):
        record = tx.to_dict()
        assert validate(record) == []  # generator output is always schema-valid
        rows.append(
            {
                "event_time": tx.timestamp,
                "amount_pln": round(tx.amount * FX_RATES_TO_PLN[tx.currency], 2),
                "channel": tx.channel,
                "merchant_category": tx.merchant_category,
                "country": tx.country,
                "is_fraud_label": tx.is_fraud_label,
            }
        )
    return pd.DataFrame(rows)


def test_pipeline_smoke_generate_score_drift() -> None:
    frame = _silver_like(2000, seed=21)
    assert len(frame) == 2000

    model = HeuristicFraudModel()
    scores = score_pandas(frame, model)

    # Scoring produces a valid probability per row.
    assert len(scores) == len(frame)
    assert scores.between(0.0, 1.0).all()

    # Same generator config -> low drift; a shifted one -> higher drift.
    baseline = score_pandas(_silver_like(2000, seed=21), model).tolist()
    same = scores.tolist()
    shifted = score_pandas(_silver_like(2000, seed=99), model).tolist()

    psi_same = population_stability_index(baseline, same)
    psi_shifted = population_stability_index(baseline, shifted)
    assert psi_same <= psi_shifted  # identical inputs never drift more than different ones


def test_pipeline_smoke_fraud_present() -> None:
    frame = _silver_like(5000, seed=5)
    # The synthetic stream contains some fraud and some foreign transactions.
    assert frame["is_fraud_label"].sum() >= 1
    assert (frame["country"] != "PL").sum() >= 1
