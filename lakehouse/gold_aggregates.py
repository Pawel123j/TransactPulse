"""Pure, unit-testable gold aggregations over the silver table.

Each function maps the silver DataFrame to one analytical gold table:

* :func:`daily_volume_by_country` — daily volume/value/fraud per country.
* :func:`merchant_category_kpi` — KPIs per merchant category.
* :func:`account_velocity` — per-account/day activity (velocity/risk features).
* :func:`fraud_signals` — per-transaction rule-based fraud signals.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from lakehouse.ml.features import HIGH_RISK_CATEGORIES, HOME_COUNTRY

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


def daily_volume_by_country(silver: DataFrame) -> DataFrame:
    """Daily transaction count, value (PLN) and fraud per country."""
    return (
        silver.groupBy("event_date", "country")
        .agg(
            F.count(F.lit(1)).alias("tx_count"),
            F.round(F.sum("amount_pln"), 2).alias("total_volume_pln"),
            F.round(F.avg("amount_pln"), 2).alias("avg_amount_pln"),
            F.sum(F.col("is_fraud_label").cast("int")).alias("fraud_count"),
        )
        .withColumn(
            "fraud_rate",
            F.round(F.col("fraud_count") / F.col("tx_count"), 5),
        )
    )


def merchant_category_kpi(silver: DataFrame) -> DataFrame:
    """KPIs per merchant category."""
    return (
        silver.groupBy("merchant_category")
        .agg(
            F.count(F.lit(1)).alias("tx_count"),
            F.countDistinct("account_id").alias("distinct_accounts"),
            F.round(F.sum("amount_pln"), 2).alias("total_volume_pln"),
            F.round(F.avg("amount_pln"), 2).alias("avg_amount_pln"),
            F.sum(F.col("is_fraud_label").cast("int")).alias("fraud_count"),
        )
        .withColumn("fraud_rate", F.round(F.col("fraud_count") / F.col("tx_count"), 5))
    )


def account_velocity(silver: DataFrame) -> DataFrame:
    """Per-account, per-day activity — velocity and spread features."""
    return silver.groupBy("account_id", "event_date").agg(
        F.count(F.lit(1)).alias("tx_count"),
        F.round(F.sum("amount_pln"), 2).alias("total_amount_pln"),
        F.round(F.max("amount_pln"), 2).alias("max_amount_pln"),
        F.countDistinct("country").alias("distinct_countries"),
        F.countDistinct("device_id").alias("distinct_devices"),
        F.countDistinct("merchant_category").alias("distinct_categories"),
        F.sum(F.col("is_fraud_label").cast("int")).alias("fraud_count"),
    )


def fraud_signals(silver: DataFrame, high_amount_pln: float = 5000.0) -> DataFrame:
    """Per-transaction rule-based fraud signals (independent of the ML score)."""
    hour = F.hour(F.col("event_time"))
    is_high_risk = F.col("merchant_category").isin(list(HIGH_RISK_CATEGORIES))
    return silver.select(
        "transaction_id",
        "event_time",
        "event_date",
        "account_id",
        "amount_pln",
        "country",
        "channel",
        "merchant_category",
        "is_fraud_label",
        ((hour < 6) | (hour >= 22)).alias("signal_night"),
        (F.col("amount_pln") >= high_amount_pln).alias("signal_high_amount"),
        (F.col("country") != F.lit(HOME_COUNTRY)).alias("signal_foreign"),
        is_high_risk.alias("signal_high_risk_category"),
        (F.col("channel") == F.lit("ONLINE")).alias("signal_card_not_present"),
    ).withColumn(
        "signal_count",
        (
            F.col("signal_night").cast("int")
            + F.col("signal_high_amount").cast("int")
            + F.col("signal_foreign").cast("int")
            + F.col("signal_high_risk_category").cast("int")
            + F.col("signal_card_not_present").cast("int")
        ),
    )
