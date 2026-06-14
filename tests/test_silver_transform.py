"""Spark tests for the silver transformations (skipped without PySpark)."""

from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.fx_rates import fx_rates_dataframe  # noqa: E402
from lakehouse.transforms import (  # noqa: E402
    SILVER_COLUMNS,
    cast_and_extract,
    deduplicate,
    normalize,
    split_valid_quarantine,
    to_silver,
)

# Bronze-like schema: a `data` struct + ingestion metadata.
_DATA_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("account_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("merchant_category", StringType()),
        StructField("country", StringType()),
        StructField("channel", StringType()),
        StructField("device_id", StringType()),
        StructField("is_fraud_label", BooleanType()),
    ]
)
_BRONZE_SCHEMA = StructType(
    [
        StructField("data", _DATA_SCHEMA),
        StructField("ingestion_time", TimestampType()),
        StructField("_offset", StringType()),  # cast handled downstream
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transactpulse-silver-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _data(**over):
    base = {
        "transaction_id": "id-1",
        "timestamp": "2026-06-13T12:00:00+00:00",
        "account_id": "ACC-1",
        "amount": 100.0,
        "currency": "eur",  # lower-case on purpose -> normalized to EUR
        "merchant_category": "Grocery",
        "country": "de",
        "channel": "pos",
        "device_id": "DEV-1",
        "is_fraud_label": False,
    }
    base.update(over)
    return base


def _bronze(spark, records):
    rows = [
        (
            r["data"],
            datetime.fromisoformat(r.get("ingestion_time", "2026-06-13T12:00:05+00:00")),
            str(r.get("_offset", "0")),
        )
        for r in records
    ]
    return spark.createDataFrame(rows, _BRONZE_SCHEMA)


def test_cast_and_normalize_currency_to_pln(spark):
    bronze = _bronze(spark, [{"data": _data()}])
    typed = cast_and_extract(bronze)
    normalized = normalize(typed, fx_rates_dataframe(spark)).collect()[0]
    assert normalized["currency"] == "EUR"  # upper-cased
    assert normalized["country"] == "DE"  # upper-cased
    assert normalized["channel"] == "POS"
    assert normalized["merchant_category"] == "grocery"  # lower-cased
    assert normalized["fx_rate_to_pln"] == 4.30
    assert normalized["amount_pln"] == 430.0  # 100 * 4.30
    assert normalized["event_date"] is not None


def test_valid_and_quarantine_split(spark):
    good = {"data": _data(transaction_id="ok")}
    bad_currency = {"data": _data(transaction_id="bad-cur", currency="xxx")}
    bad_amount = {"data": _data(transaction_id="bad-amt", amount=-1.0)}
    bronze = _bronze(spark, [good, bad_currency, bad_amount])

    typed = normalize(cast_and_extract(bronze), fx_rates_dataframe(spark))
    valid, quarantine = split_valid_quarantine(typed)

    valid_ids = {r["transaction_id"] for r in valid.collect()}
    assert valid_ids == {"ok"}
    assert set(valid.columns) == set(SILVER_COLUMNS)

    q = {r["transaction_id"]: r["dq_errors"] for r in quarantine.collect()}
    assert "currency_allowed" in q["bad-cur"]
    assert "amount_in_range" in q["bad-amt"]
    assert all(r["quarantine_time"] is not None for r in quarantine.collect())


def test_deduplicate_keeps_latest_by_ingestion_time(spark):
    older = {
        "data": _data(amount=10.0),
        "ingestion_time": "2026-06-13T12:00:00+00:00",
        "_offset": "1",
    }
    newer = {
        "data": _data(amount=20.0),
        "ingestion_time": "2026-06-13T12:05:00+00:00",
        "_offset": "2",
    }
    bronze = _bronze(spark, [older, newer])
    typed = normalize(cast_and_extract(bronze), fx_rates_dataframe(spark))
    valid, _ = split_valid_quarantine(typed)
    deduped = deduplicate(valid).collect()
    assert len(deduped) == 1
    assert deduped[0]["amount"] == 20.0  # latest wins


def test_to_silver_end_to_end(spark):
    records = [
        {"data": _data(transaction_id="a")},
        {
            "data": _data(transaction_id="a"),
            "ingestion_time": "2026-06-13T13:00:00+00:00",
            "_offset": "9",
        },
        {"data": _data(transaction_id="b", country="zz")},  # quarantined
    ]
    bronze = _bronze(spark, records)
    valid, quarantine = to_silver(bronze, fx_rates_dataframe(spark))
    assert valid.count() == 1  # 'a' deduped to one row, 'b' quarantined
    assert {r["transaction_id"] for r in valid.collect()} == {"a"}
    assert quarantine.count() == 1
