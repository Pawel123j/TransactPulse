"""Tests for the bronze transformation.

These run against a plain local SparkSession (no Kafka/Delta/S3 required). They
are skipped when PySpark is missing locally, and are mandatory in the CI
``spark-tests`` job (``TP_REQUIRE_SPARK=1``).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from tests.spark_support import local_spark_session, requires_spark

requires_spark()

from pyspark.sql.types import (  # noqa: E402
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.schema import TRANSACTION_PAYLOAD_SCHEMA  # noqa: E402
from streaming.transforms import BRONZE_COLUMNS, build_bronze_frame  # noqa: E402

#: Schema of the Kafka source DataFrame, declared explicitly so rows with null
#: columns (e.g. the malformed-payload case) do not rely on type inference.
_KAFKA_SOURCE_SCHEMA = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = local_spark_session("transactpulse-bronze-tests")
    yield session
    session.stop()


def _kafka_like_row(value: dict, *, partition: int = 0, offset: int = 0):
    return (
        "ACC-1",  # key
        json.dumps(value),  # value (raw JSON)
        "transactions.raw",  # topic
        partition,  # partition
        offset,  # offset
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
    return spark.createDataFrame(rows, _KAFKA_SOURCE_SCHEMA)


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
    # A bad payload must not crash the job and must keep the raw value verbatim.
    # `from_json` in PERMISSIVE mode yields a struct whose fields are all null —
    # the silver data-quality gate then routes such a record to quarantine.
    rows = [("ACC-x", "{not-json", "transactions.raw", 0, 9, datetime(2026, 6, 13, tzinfo=UTC))]
    raw = _raw_df(spark, rows)
    row = build_bronze_frame(raw, TRANSACTION_PAYLOAD_SCHEMA).collect()[0]
    assert row["value"] == "{not-json"
    assert all(value is None for value in row["data"].asDict().values())


@pytest.mark.parametrize("session_tz", ["UTC", "Europe/Warsaw", "America/New_York"])
@pytest.mark.parametrize(
    "ingested_at,expected_date",
    [
        # 00:30 on the 14th in Warsaw (CEST), still the 13th in UTC.
        (datetime(2026, 6, 13, 22, 30, tzinfo=UTC), date(2026, 6, 13)),
        # 21:00 on the 13th in New York (EDT), already the 14th in UTC.
        (datetime(2026, 6, 14, 1, 0, tzinfo=UTC), date(2026, 6, 14)),
        # Exactly UTC midnight belongs to the new day.
        (datetime(2026, 6, 14, 0, 0, tzinfo=UTC), date(2026, 6, 14)),
    ],
    ids=["late-utc-evening", "early-utc-morning", "utc-midnight"],
)
def test_ingestion_date_is_utc_day(spark, session_tz, ingested_at, expected_date):
    """The bronze partition column is the UTC day, whatever the session time zone."""
    from pyspark.sql import functions as F

    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        raw = _raw_df(spark, [_kafka_like_row(_sample_payload())])
        row = build_bronze_frame(
            raw, TRANSACTION_PAYLOAD_SCHEMA, ingestion_time=F.lit(ingested_at)
        ).collect()[0]
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)
    assert row["ingestion_date"] == expected_date
