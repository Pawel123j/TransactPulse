"""Silver batch job: bronze -> cleansed, deduped, quality-gated silver + quarantine.

Idempotent: clean rows are upserted into the silver Delta table via MERGE on
``transaction_id`` (so re-runs and late-arriving records converge), bad rows are
appended to the quarantine table, and the silver table is OPTIMIZE/ZORDER-ed by
``account_id``. A data-quality report is written under ``docs/data_quality/``.

Run via spark-submit (jars supplied with ``--packages`` — see the Dockerfile).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from lakehouse.config import SilverConfig
from lakehouse.fx_rates import fx_rates_dataframe
from lakehouse.quality import generate_report
from lakehouse.spark_session import build_spark_session
from lakehouse.transforms import SILVER_COLUMNS, to_silver

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("lakehouse.silver")


def read_bronze(spark: SparkSession, config: SilverConfig) -> DataFrame:
    """Read bronze, bounded to the watermark horizon for late-data handling."""
    from pyspark.sql import functions as F

    bronze = spark.read.format("delta").load(config.bronze_path)
    if config.watermark_horizon_days >= 0:
        cutoff = F.date_sub(F.current_date(), config.watermark_horizon_days)
        bronze = bronze.filter(F.col("ingestion_date") >= cutoff)
    return bronze


def _table_exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def upsert_silver(spark: SparkSession, valid: DataFrame, config: SilverConfig) -> None:
    """Idempotently MERGE the valid rows into the silver Delta table."""
    from delta.tables import DeltaTable

    if not _table_exists(spark, config.silver_path):
        logger.info("creating silver table at %s", config.silver_path)
        (
            valid.write.format("delta")
            .partitionBy("event_date")
            .mode("overwrite")
            .save(config.silver_path)
        )
        return

    target = DeltaTable.forPath(spark, config.silver_path)
    update_cols = {c: f"s.{c}" for c in SILVER_COLUMNS}
    (
        target.alias("t")
        .merge(valid.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdate(set=update_cols)
        .whenNotMatchedInsert(values=update_cols)
        .execute()
    )


def write_quarantine(quarantine: DataFrame, config: SilverConfig) -> None:
    """Append rejected rows (with reasons) to the quarantine table."""
    (
        quarantine.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(config.quarantine_path)
    )


def optimize_silver(spark: SparkSession, config: SilverConfig) -> None:
    """Compact and Z-order the silver table by ``account_id`` for read locality."""
    from delta.tables import DeltaTable

    if not config.optimize_zorder or not _table_exists(spark, config.silver_path):
        return
    logger.info("OPTIMIZE ZORDER BY (account_id) on silver")
    DeltaTable.forPath(spark, config.silver_path).optimize().executeZOrderBy("account_id")


def main() -> None:
    """Entry point for the silver batch job."""
    config = SilverConfig.from_env()
    logger.info("silver job: %s -> %s", config.bronze_path, config.silver_path)
    spark = build_spark_session(config)

    bronze = read_bronze(spark, config)
    fx_rates = fx_rates_dataframe(spark)
    valid, quarantine = to_silver(bronze, fx_rates)

    quarantine_count = quarantine.count()
    upsert_silver(spark, valid, config)
    if quarantine_count:
        write_quarantine(quarantine, config)
    optimize_silver(spark, config)

    silver = spark.read.format("delta").load(config.silver_path)
    metrics = generate_report(silver, quarantine_count=quarantine_count, docs_dir=config.docs_dir)
    logger.info("silver done: %s rows, %s quarantined", metrics["row_count"], quarantine_count)


if __name__ == "__main__":
    main()
