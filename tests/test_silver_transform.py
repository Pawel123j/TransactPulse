"""Spark tests for the silver transformations (mandatory in the CI Spark job)."""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.spark_support import local_spark_session, requires_spark

requires_spark()

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.fx_rates import fx_rates_dataframe  # noqa: E402
from lakehouse.quality import compute_metrics, evaluate_checks  # noqa: E402
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
    session = local_spark_session("transactpulse-silver-tests")
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


def test_every_required_null_is_quarantined_before_gold(spark):
    from ingestion.schema import FIELDS
    from lakehouse.dq_rules import evaluate
    from lakehouse.gold_aggregates import daily_volume_by_country

    records = [{"data": _data(transaction_id="good")}]
    for index, field in enumerate(FIELDS, start=1):
        records.append({"data": _data(**{field: None}), "_offset": index})
    records.append({"data": dict.fromkeys(FIELDS), "_offset": len(FIELDS) + 1})
    valid, quarantine = to_silver(_bronze(spark, records), fx_rates_dataframe(spark))

    clean = valid.collect()
    assert len(clean) == 1 and clean[0]["transaction_id"] == "good"
    assert clean[0]["amount_pln"] == 430.0
    rejected = quarantine.collect()
    assert len(rejected) == len(FIELDS) + 1
    for row in rejected:
        assert row["dq_errors"]
        assert set(row["dq_errors"]) == set(evaluate(row.asDict()))
        assert row["quarantine_time"] is not None
    gold = daily_volume_by_country(valid).collect()
    assert len(gold) == 1
    assert gold[0]["tx_count"] == 1 and gold[0]["total_volume_pln"] == 430.0


def test_unknown_sql_predicate_fails_closed(spark, monkeypatch):
    from lakehouse import transforms
    from lakehouse.dq_rules import Rule

    # A future rule can accidentally return SQL UNKNOWN; the generic split must
    # still quarantine it even when that rule omits an explicit IS NOT NULL.
    monkeypatch.setattr(
        transforms,
        "RULES",
        (Rule("nullable_rule", "regression", lambda _: False, "CAST(NULL AS BOOLEAN)"),),
    )
    valid, quarantine = to_silver(_bronze(spark, [{"data": _data()}]), fx_rates_dataframe(spark))
    assert valid.count() == 0
    assert quarantine.collect()[0]["dq_errors"] == ["nullable_rule"]


def test_quality_metrics_detect_missing_pln_amount(spark):
    from pyspark.sql import functions as F

    valid, _ = to_silver(_bronze(spark, [{"data": _data()}]), fx_rates_dataframe(spark))
    metrics = compute_metrics(valid)
    assert metrics["null_amount_pln"] == 0
    assert all(check["passed"] for check in evaluate_checks(metrics))

    damaged = valid.withColumn("amount_pln", F.lit(None).cast("double"))
    metrics = compute_metrics(damaged)
    assert metrics["null_amount_pln"] == 1
    checks = {check["check"]: check["passed"] for check in evaluate_checks(metrics)}
    assert checks["no_null_amount_pln"] is False


@pytest.mark.parametrize("session_tz", ["UTC", "Europe/Warsaw", "America/New_York"])
@pytest.mark.parametrize(
    "late_ts,early_ts",
    [
        ("2026-06-13T23:30:00+00:00", "2026-06-14T00:30:00+00:00"),  # typical
        ("2026-06-13T23:59:59+00:00", "2026-06-14T00:00:00+00:00"),  # boundary
        ("2026-06-13T19:30:00-04:00", "2026-06-14T02:30:00+02:00"),  # offsets
    ],
    ids=["typical", "boundary", "non-utc-offsets"],
)
def test_daily_totals_split_at_utc_midnight(spark, session_tz, late_ts, early_ts):
    """event_date is the UTC calendar day, whatever the Spark session time zone."""
    from datetime import date

    from lakehouse.gold_aggregates import daily_volume_by_country

    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        bronze = _bronze(
            spark,
            [
                {
                    "data": _data(
                        transaction_id="a",
                        timestamp=late_ts,
                        amount=100.0,
                        currency="PLN",
                        country="PL",
                    )
                },
                {
                    "data": _data(
                        transaction_id="b",
                        timestamp=early_ts,
                        amount=50.0,
                        currency="PLN",
                        country="PL",
                    )
                },
            ],
        )
        valid, quarantine = to_silver(bronze, fx_rates_dataframe(spark))
        assert quarantine.count() == 0
        totals = {
            (r["event_date"], r["country"]): (r["tx_count"], r["total_volume_pln"])
            for r in daily_volume_by_country(valid).collect()
        }
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)
    assert totals == {
        (date(2026, 6, 13), "PL"): (1, 100.0),
        (date(2026, 6, 14), "PL"): (1, 50.0),
    }
