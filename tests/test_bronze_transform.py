"""Tests for the bronze transformation.

These run against a plain local SparkSession (no Kafka/Delta/S3 required) and are
skipped automatically if PySpark is not installed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from streaming.schema import TRANSACTION_PAYLOAD_SCHEMA  # noqa: E402
from streaming.transforms import BRONZE_COLUMNS, build_bronze_frame  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transactpulse-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _kafka_like_row(value: dict, *, partition: int = 0, offset: int = 0):
    return (
        "ACC-1",                                   # key
        json.dumps(value),                          # value (raw JSON)
        "transactions.raw",                         # topic
        partition,                                  # partition
        offset,                                      # offset
        datetime(2026, 6, 13, 12, 0, tzinfo=UTC),  # timestamp
    )


def _sample_payload() -> dict:
    return {
        "transaction_id": "11111111-1111-4111-8111-111111111111",
        "timestamp": "2026-06-13T12:00:00+00:00",
        "account_id": "ACC-1",
        "amount": 42.5,
        "currency": "PLN",
        "merchant_category": "grocery",
        "country": "PL",
        "channel": "POS",
        "device_id": "DEV-1",
        "is_fraud_label": False,
    }


def _raw_df(spark, rows):
    cols = ["key", "value", "topic", "partition", "offset", "timestamp"]
    return spark.createDataFrame(rows, cols)


def test_bronze_has_expected_columns(spark):
    raw = _raw_df(spark, [_kafka_like_row(_sample_payload())])
    out = build_bronze_frame(raw, TRANSACTION_PAYLOAD_SCHEMA)
    assert set(BRONZE_COLUMNS).issubset(set(out.columns))


def test_bronze_preserves_raw_value_and_parses_data(spark):
    payload = _sample_payload()
    raw = _raw_df(spark, [_kafka_like_row(payload, offset=7)])
    out = build_bronze_frame(raw, TRANSACTION_PAYLOAD_SCHEMA).collect()
    assert len(out) == 1
    row = out[0]
    # Raw value preserved verbatim.
    assert json.loads(row["value"]) == payload
    # Metadata carried through.
    assert row["source_topic"] == "transactions.raw"
    assert row["_offset"] == 7
    # Parsed struct exposes typed business fields.
    assert row["data"]["transaction_id"] == payload["transaction_id"]
    assert row["data"]["amount"] == 42.5
    assert row["data"]["is_fraud_label"] is False
    # Ingestion metadata present.
    assert row["ingestion_time"] is not None
    assert row["ingestion_date"] is not None


def test_bronze_is_append_only_no_dedup(spark):
    # Bronze must NOT deduplicate — two identical-id records both survive.
    payload = _sample_payload()
    raw = _raw_df(
        spark,
        [_kafka_like_row(payload, offset=1), _kafka_like_row(payload, offset=2)],
    )
    out = build_bronze_frame(raw, TRANSACTION_PAYLOAD_SCHEMA)
    assert out.count() == 2


def test_bronze_tolerates_malformed_json(spark):
    # A bad payload must not crash the job; data parses to null, raw value kept.
    rows = [("ACC-x", "{not-json", "transactions.raw", 0, 9, None)]
    raw = _raw_df(spark, rows)
    row = build_bronze_frame(raw, TRANSACTION_PAYLOAD_SCHEMA).collect()[0]
    assert row["value"] == "{not-json"
    assert row["data"] is None
