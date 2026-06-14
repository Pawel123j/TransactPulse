"""Pure, unit-testable silver transformations (no I/O).

Pipeline: bronze ``data`` struct -> typed columns -> normalized (currency->PLN,
ISO upper) -> data-quality split (valid / quarantine) -> deduplicated.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lakehouse.dq_rules import RULES

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

#: Business columns carried through silver.
SILVER_COLUMNS: tuple[str, ...] = (
    "transaction_id",
    "event_time",
    "event_date",
    "account_id",
    "amount",
    "currency",
    "fx_rate_to_pln",
    "amount_pln",
    "merchant_category",
    "country",
    "channel",
    "device_id",
    "is_fraud_label",
    "ingestion_time",
    "_offset",
)


def cast_and_extract(bronze: DataFrame) -> DataFrame:
    """Flatten the bronze ``data`` struct into typed, trimmed business columns."""
    return bronze.select(
        F.col("data.transaction_id").cast("string").alias("transaction_id"),
        F.to_timestamp(F.col("data.timestamp")).alias("event_time"),
        F.col("data.account_id").cast("string").alias("account_id"),
        F.col("data.amount").cast("double").alias("amount"),
        F.upper(F.trim(F.col("data.currency"))).alias("currency"),
        F.lower(F.trim(F.col("data.merchant_category"))).alias("merchant_category"),
        F.upper(F.trim(F.col("data.country"))).alias("country"),
        F.upper(F.trim(F.col("data.channel"))).alias("channel"),
        F.col("data.device_id").cast("string").alias("device_id"),
        F.col("data.is_fraud_label").cast("boolean").alias("is_fraud_label"),
        F.col("ingestion_time"),
        F.col("_offset"),
    )


def normalize(df: DataFrame, fx_rates: DataFrame) -> DataFrame:
    """Add ``event_date``, join FX rates and compute ``amount_pln``.

    A left join is used so rows with an unknown currency are preserved (with a
    null rate) and later quarantined by the currency rule, rather than dropped.
    """
    return (
        df.withColumn("event_date", F.to_date(F.col("event_time")))
        .join(F.broadcast(fx_rates), on="currency", how="left")
        .withColumn(
            "amount_pln",
            F.round(F.col("amount") * F.col("fx_rate_to_pln"), 2),
        )
    )


def _dq_errors_column():
    """Build an array column of the names of every failing rule (null-safe)."""
    candidates = [F.when(~F.expr(rule.spark_expr), F.lit(rule.name)) for rule in RULES]
    return F.filter(F.array(*candidates), lambda x: x.isNotNull())


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split ``df`` into (valid, quarantine) using the data-quality rules.

    Returns:
        A tuple ``(valid, quarantine)``. Valid rows expose :data:`SILVER_COLUMNS`;
        quarantine rows additionally carry ``dq_errors`` and ``quarantine_time``.
    """
    tagged = df.withColumn("dq_errors", _dq_errors_column())
    valid = tagged.filter(F.size("dq_errors") == 0).select(*SILVER_COLUMNS)
    quarantine = tagged.filter(F.size("dq_errors") > 0).withColumn(
        "quarantine_time", F.current_timestamp()
    )
    return valid, quarantine


def deduplicate(df: DataFrame) -> DataFrame:
    """Deduplicate by ``transaction_id``, keeping the latest record.

    "Latest" is by ``ingestion_time`` then ``_offset`` (both descending), so the
    most recently ingested copy of a duplicated id wins.
    """
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("ingestion_time").desc_nulls_last(),
        F.col("_offset").desc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def to_silver(bronze: DataFrame, fx_rates: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Full bronze -> (silver_valid, quarantine) transformation.

    Deduplication is applied to the valid set so quarantine retains every rejected
    occurrence for auditing.
    """
    typed = cast_and_extract(bronze)
    normalized = normalize(typed, fx_rates)
    valid, quarantine = split_valid_quarantine(normalized)
    return deduplicate(valid), quarantine
