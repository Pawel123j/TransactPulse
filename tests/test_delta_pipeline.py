"""Actual Spark/Delta files and batch jobs; no Kafka, S3 or Airflow claim."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest

from tests.spark_support import requires_spark

requires_spark()

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from lakehouse.config import GoldConfig, SilverConfig  # noqa: E402
from lakehouse.dq_rules import RULES  # noqa: E402
from lakehouse.fx_rates import fx_rates_dataframe  # noqa: E402
from lakehouse.gold_job import run_step  # noqa: E402
from lakehouse.quality import generate_report  # noqa: E402
from lakehouse.silver_job import read_bronze, upsert_silver, write_quarantine  # noqa: E402
from lakehouse.transforms import to_silver  # noqa: E402
from streaming.schema import TRANSACTION_PAYLOAD_SCHEMA  # noqa: E402
from streaming.transforms import build_bronze_frame  # noqa: E402


@dataclass
class LocalSilver(SilverConfig):
    """Same jobs, with owned local Delta paths instead of S3 paths."""

    directory: str = ""

    @property
    def bronze_path(self):
        return f"{self.directory}/bronze"

    @property
    def silver_path(self):
        return f"{self.directory}/silver"

    @property
    def quarantine_path(self):
        return f"{self.directory}/quarantine"


@dataclass
class LocalGold(GoldConfig):
    directory: str = ""

    @property
    def silver_path(self):
        return f"{self.directory}/silver"

    def gold_path(self, table):
        return f"{self.directory}/gold/{table}"


def start_spark():
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("transactpulse-delta-replay")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    jars = os.environ.get("TP_DELTA_TEST_JARS")
    if jars:
        builder = builder.config("spark.jars", jars)
    else:
        builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def test_null_quarantine_and_silver_gold_replay_survive_new_spark_session(tmp_path):
    # A preceding plain-Spark test can leave the Py4J JVM alive after stop().
    # Delta jars must be present at JVM startup, so use a fresh interpreter.
    result = subprocess.run(
        [sys.executable, __file__, str(tmp_path)],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _assert_pipeline(tmp_path):
    valid = {
        "transaction_id": "valid-id",
        "timestamp": "2026-11-15T12:00:00+00:00",
        "account_id": "ACC-1",
        "amount": 100.0,
        "currency": "EUR",
        "merchant_category": "grocery",
        "country": "DE",
        "channel": "POS",
        "device_id": "DEV-1",
        "is_fraud_label": False,
    }
    records = [valid, {**valid, "amount": 150.0}]
    records.extend({**valid, "transaction_id": f"null-{key}", key: None} for key in valid)
    silver = LocalSilver(
        directory=str(tmp_path),
        watermark_horizon_days=-1,
        optimize_zorder=False,
        docs_dir=str(tmp_path / "quality"),
    )
    gold = LocalGold(
        directory=str(tmp_path),
        model_path=str(tmp_path / "absent-model.pkl"),
        docs_dir=str(tmp_path / "drift"),
    )
    spark = start_spark()
    try:
        raw = spark.createDataFrame(
            [
                (
                    b"",
                    json.dumps(record).encode(),
                    "fixture-topic",
                    0,
                    offset,
                    datetime(2026, 11, 15, 12),
                )
                for offset, record in enumerate(records)
            ],
            "key binary, value binary, topic string, partition int, "
            "offset long, timestamp timestamp",
        )
        build_bronze_frame(raw, TRANSACTION_PAYLOAD_SCHEMA).write.format("delta").save(
            silver.bronze_path
        )
        clean, rejected = to_silver(read_bronze(spark, silver), fx_rates_dataframe(spark))
        assert clean.count() == 1
        write_quarantine(rejected, silver)
        quarantine = spark.read.format("delta").load(silver.quarantine_path).collect()
        assert len(quarantine) == len(valid)
        assert {error for row in quarantine for error in row.dq_errors} == {
            rule.name for rule in RULES
        }
        upsert_silver(spark, clean, silver)
        upsert_silver(spark, clean, silver)
        stored = spark.read.format("delta").load(silver.silver_path)
        assert stored.count() == 1
        assert stored.first().amount_pln == pytest.approx(645.0)
        report = generate_report(stored, quarantine_count=len(quarantine), docs_dir=silver.docs_dir)
        assert report["null_amount_pln"] == 0
        for _ in range(2):
            run_step("all", gold, spark)
        spark.stop()
        spark = start_spark()
        # Read actual persisted Delta logs with a new Spark context/session.
        assert spark.read.format("delta").load(silver.silver_path).count() == 1
        assert spark.read.format("delta").load(silver.quarantine_path).count() == len(valid)
        scored = spark.read.format("delta").load(gold.gold_path("scored_transactions"))
        assert scored.count() == 1
        assert 0 <= scored.first().fraud_score <= 1
        aggregate = (
            spark.read.format("delta").load(gold.gold_path("daily_volume_by_country")).first()
        )
        assert aggregate.tx_count == 1
        assert aggregate.total_volume_pln == pytest.approx(645.0)
        drift = json.loads((tmp_path / "drift/drift_report.json").read_text())
        assert drift["metrics"]["fraud_score"]["live_n"] == 1
    finally:
        spark.stop()


if __name__ == "__main__":
    _assert_pipeline(Path(sys.argv[1]))
