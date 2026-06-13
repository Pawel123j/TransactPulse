"""Spark tests for gold aggregations (skipped without PySpark)."""

from __future__ import annotations

from datetime import date, datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from lakehouse.gold_aggregates import (  # noqa: E402
    account_velocity,
    daily_volume_by_country,
    fraud_signals,
    merchant_category_kpi,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transactpulse-gold-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _silver(spark):
    d = date(2026, 6, 13)
    # cols: txid, event_time, event_date, account, amount_pln, country, channel,
    #       category, fraud, device
    rows = [
        ("t1", datetime(2026, 6, 13, 14), d, "A1", 100.0, "PL", "POS", "grocery", False, "D1"),
        ("t2", datetime(2026, 6, 13, 3), d, "A1", 8000.0, "US", "ONLINE", "gambling", True, "D2"),
        ("t3", datetime(2026, 6, 13, 12), d, "A2", 200.0, "DE", "POS", "travel", False, "D3"),
    ]
    cols = [
        "transaction_id", "event_time", "event_date", "account_id", "amount_pln",
        "country", "channel", "merchant_category", "is_fraud_label", "device_id",
    ]
    return spark.createDataFrame(rows, cols)


def test_daily_volume_by_country(spark):
    out = {r["country"]: r for r in daily_volume_by_country(_silver(spark)).collect()}
    assert out["PL"]["tx_count"] == 1
    assert out["US"]["fraud_count"] == 1
    assert out["DE"]["total_volume_pln"] == 200.0


def test_merchant_category_kpi(spark):
    out = {r["merchant_category"]: r for r in merchant_category_kpi(_silver(spark)).collect()}
    assert out["gambling"]["fraud_count"] == 1
    assert out["grocery"]["distinct_accounts"] == 1


def test_account_velocity(spark):
    out = {r["account_id"]: r for r in account_velocity(_silver(spark)).collect()}
    assert out["A1"]["tx_count"] == 2
    assert out["A1"]["distinct_countries"] == 2
    assert out["A1"]["max_amount_pln"] == 8000.0


def test_fraud_signals(spark):
    out = {r["transaction_id"]: r for r in fraud_signals(_silver(spark)).collect()}
    # t2 is night + high amount + foreign + high-risk category + card-not-present.
    assert out["t2"]["signal_count"] == 5
    assert out["t1"]["signal_count"] == 0
