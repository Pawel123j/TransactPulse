"""Pure, unit-testable transformations for the bronze layer.

Kept free of I/O (no Kafka, no Delta, no S3) so they can be exercised with a plain
local SparkSession.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json, to_date
from pyspark.sql.types import StructType

#: Columns the bronze table is expected to expose.
BRONZE_COLUMNS: tuple[str, ...] = (
    "key",
    "value",
    "source_topic",
    "_partition",
    "_offset",
    "_kafka_timestamp",
    "data",
    "ingestion_time",
    "ingestion_date",
)


def build_bronze_frame(raw: DataFrame, payload_schema: StructType) -> DataFrame:
    """Shape a raw Kafka source DataFrame into the bronze schema.

    Adds ingestion metadata and a parsed ``data`` struct, **without** applying any
    business transformation (no dedup, no normalization) — bronze stays as-is.

    Args:
        raw: A DataFrame with the standard Kafka source columns
            (``key``, ``value``, ``topic``, ``partition``, ``offset``,
            ``timestamp``).
        payload_schema: Schema used to parse the JSON ``value`` into ``data``.

    Returns:
        A DataFrame with :data:`BRONZE_COLUMNS`, partition-ready on
        ``ingestion_date``.
    """
    return (
        raw.select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
            col("topic").alias("source_topic"),
            col("partition").alias("_partition"),
            col("offset").cast("long").alias("_offset"),
            col("timestamp").alias("_kafka_timestamp"),
        )
        .withColumn("data", from_json(col("value"), payload_schema))
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )
