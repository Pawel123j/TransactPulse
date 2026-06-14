"""Train a baseline fraud model on synthetic data and persist the artifact.

This stands in for an "existing" model: it generates labelled synthetic
transactions with the Stage-2 generator, engineers features, fits a
scikit-learn classifier, captures a reference distribution for drift, and writes:

* ``models/fraud_model.pkl``       — the pickled :class:`SklearnFraudModel`;
* ``models/fraud_model.meta.json`` — committed metadata (features, AUC, quantiles).

Run::

    python -m lakehouse.ml.train_model --samples 60000 --seed 42
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from ingestion.config import GeneratorConfig
from ingestion.generator import TransactionGenerator
from lakehouse.fx_rates import FX_RATES_TO_PLN
from lakehouse.ml.features import FEATURE_NAMES, features_frame
from lakehouse.ml.model import SklearnFraudModel, save_model

DEFAULT_MODEL_PATH = "models/fraud_model.pkl"
_REFERENCE_SAMPLE_SIZE = 5000


def synthetic_silver_frame(samples: int, seed: int) -> pd.DataFrame:
    """Generate a silver-like labelled DataFrame using the transaction generator."""
    gen = TransactionGenerator(
        GeneratorConfig(seed=seed, fraud_rate=0.02, num_accounts=4000, num_devices=6000)
    )
    rows = []
    for tx in gen.stream(samples):
        rate = FX_RATES_TO_PLN[tx.currency]
        rows.append(
            {
                "event_time": tx.timestamp,
                "amount_pln": round(tx.amount * rate, 2),
                "channel": tx.channel,
                "merchant_category": tx.merchant_category,
                "country": tx.country,
                "is_fraud_label": bool(tx.is_fraud_label),
            }
        )
    return pd.DataFrame(rows)


def _deciles(series: pd.Series) -> list[float]:
    return [round(float(series.quantile(q / 10.0)), 6) for q in range(11)]


def train(samples: int, seed: int, model_path: str) -> dict:
    """Train, evaluate and persist the model. Returns the metadata dict."""
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split

    df = synthetic_silver_frame(samples, seed)
    features = features_frame(df)
    labels = df["is_fraud_label"].astype(int)

    x_train, x_test, y_train, y_test = train_test_split(
        features, labels, test_size=0.25, random_state=seed, stratify=labels
    )
    estimator = GradientBoostingClassifier(random_state=seed)
    estimator.fit(x_train, y_train)

    test_scores = estimator.predict_proba(x_test)[:, 1]
    auc = float(roc_auc_score(y_test, test_scores))

    # Reference distribution for drift (a capped sample of training scores/amounts).
    train_scores = estimator.predict_proba(x_train)[:, 1]
    ref_scores = pd.Series(train_scores).head(_REFERENCE_SAMPLE_SIZE).round(6).tolist()
    ref_amounts = df["amount_pln"].head(_REFERENCE_SAMPLE_SIZE).round(2).tolist()
    reference = {"fraud_score": ref_scores, "amount_pln": ref_amounts}

    model = SklearnFraudModel(
        estimator=estimator,
        feature_names=FEATURE_NAMES,
        threshold=0.5,
        reference=reference,
    )
    save_model(model, model_path)

    meta = {
        "kind": "sklearn",
        "estimator": type(estimator).__name__,
        "feature_names": list(FEATURE_NAMES),
        "threshold": 0.5,
        "trained_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "n_samples": int(samples),
        "n_fraud": int(labels.sum()),
        "roc_auc": round(auc, 4),
        "reference_quantiles": {
            "fraud_score": _deciles(pd.Series(train_scores)),
            "amount_pln": _deciles(df["amount_pln"]),
        },
    }
    meta_path = Path(model_path).with_suffix(".meta.json")
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train the baseline fraud model.")
    parser.add_argument("--samples", type=int, default=60000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--model-path", default=DEFAULT_MODEL_PATH)
    args = parser.parse_args(argv)

    meta = train(args.samples, args.seed, args.model_path)
    print(json.dumps(meta, indent=2))  # noqa: T201 - CLI feedback
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
