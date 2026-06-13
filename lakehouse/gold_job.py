"""Gold batch job: silver -> analytical aggregates + ML fraud scoring + drift.

Writes the gold Delta tables (``daily_volume_by_country``, ``merchant_category_kpi``,
``account_velocity``, ``fraud_signals``, ``scored_transactions``), then computes
PSI/KS drift of the live ``fraud_score`` / ``amount_pln`` against the model's
training reference and writes a report under ``docs/drift/``.

Run via spark-submit (jars supplied with ``--packages`` — see the Dockerfile).
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lakehouse.config import GoldConfig
from lakehouse.drift import classify_psi, ks_statistic, population_stability_index
from lakehouse.gold_aggregates import (
    account_velocity,
    daily_volume_by_country,
    fraud_signals,
    merchant_category_kpi,
)
from lakehouse.ml.model import load_or_default
from lakehouse.ml.scoring import add_fraud_scores
from lakehouse.spark_session import build_spark_session

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("lakehouse.gold")

_DRIFT_SAMPLE_SIZE = 5000


def _write(df: DataFrame, path: str, partition_by: str | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)


def write_aggregates(silver: DataFrame, config: GoldConfig) -> None:
    """Compute and persist the four analytical gold tables."""
    _write(
        daily_volume_by_country(silver),
        config.gold_path("daily_volume_by_country"),
        "event_date",
    )
    _write(merchant_category_kpi(silver), config.gold_path("merchant_category_kpi"))
    _write(account_velocity(silver), config.gold_path("account_velocity"), "event_date")
    _write(fraud_signals(silver), config.gold_path("fraud_signals"), "event_date")


def score_and_write(silver: DataFrame, config: GoldConfig) -> DataFrame:
    """Score transactions and persist ``gold/scored_transactions``."""
    scored = add_fraud_scores(silver, config.model_path, config.fraud_threshold)
    _write(scored, config.gold_path("scored_transactions"), "event_date")
    return scored


def compute_drift(scored: DataFrame, config: GoldConfig) -> dict[str, Any]:
    """Compute PSI/KS of live scores/amounts vs the model's training reference."""
    from pyspark.sql import functions as F

    model = load_or_default(config.model_path)
    reference = getattr(model, "reference", {}) or {}

    live = (
        scored.select("fraud_score", "amount_pln")
        .orderBy(F.rand(seed=7))
        .limit(_DRIFT_SAMPLE_SIZE)
        .toPandas()
    )
    live_scores = live["fraud_score"].dropna().tolist()
    live_amounts = live["amount_pln"].dropna().tolist()

    result: dict[str, Any] = {
        "generated": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "model_kind": getattr(model, "kind", "unknown"),
        "metrics": {},
    }
    for name, live_values in (("fraud_score", live_scores), ("amount_pln", live_amounts)):
        ref_values = reference.get(name, [])
        psi = population_stability_index(ref_values, live_values)
        ks = ks_statistic(ref_values, live_values)
        result["metrics"][name] = {
            "psi": round(psi, 5),
            "psi_band": classify_psi(psi),
            "ks": round(ks, 5),
            "reference_n": len(ref_values),
            "live_n": len(live_values),
        }
    return result


def write_drift_report(result: dict[str, Any], docs_dir: str) -> None:
    """Write the drift result as JSON + Markdown under ``docs_dir``."""
    out = Path(docs_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "drift_report.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    lines = [
        "# Fraud-score & amount drift report",
        "",
        f"- **Generated:** {result['generated']}",
        f"- **Model:** {result['model_kind']}",
        "",
        "| Feature | PSI | Band | KS | ref n | live n |",
        "| ------- | --- | ---- | -- | ----- | ------ |",
    ]
    for feature, m in result["metrics"].items():
        lines.append(
            f"| `{feature}` | {m['psi']} | {m['psi_band']} | {m['ks']} "
            f"| {m['reference_n']} | {m['live_n']} |"
        )
    lines.append("")
    (out / "drift_report.md").write_text("\n".join(lines), encoding="utf-8")


def run_step(step: str, config: GoldConfig, spark: SparkSession) -> None:
    """Run a single gold step. Each step reads from Delta, so steps are independent
    and individually restartable (used as discrete Airflow tasks)."""
    if step in ("aggregates", "all"):
        silver = spark.read.format("delta").load(config.silver_path)
        write_aggregates(silver, config)

    scored = None
    if step in ("scoring", "all"):
        silver = spark.read.format("delta").load(config.silver_path)
        scored = score_and_write(silver, config)

    if step in ("drift", "all"):
        if scored is None:
            scored = spark.read.format("delta").load(config.gold_path("scored_transactions"))
        drift = compute_drift(scored, config)
        write_drift_report(drift, config.docs_dir)
        logger.info("drift=%s", json.dumps(drift["metrics"]))


def main(argv: list[str] | None = None) -> int:
    """Entry point for the gold batch job."""
    parser = argparse.ArgumentParser(description="Gold aggregates + ML scoring + drift.")
    parser.add_argument(
        "--step",
        choices=("aggregates", "scoring", "drift", "all"),
        default="all",
        help="Which gold step to run (the Airflow DAG runs them separately).",
    )
    args = parser.parse_args(argv)

    config = GoldConfig.from_env()
    logger.info("gold job step=%s from %s", args.step, config.silver_path)
    spark: SparkSession = build_spark_session(config)
    run_step(args.step, config, spark)
    logger.info("gold step %s done", args.step)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
