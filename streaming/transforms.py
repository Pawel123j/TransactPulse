"""Pure, unit-testable transformations for the bronze layer.

Kept free of I/O (no Kafka, no Delta, no S3) so they can be exercised with a plain
local SparkSession.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, current_timestamp, expr, from_json
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


def build_bronze_frame(
    raw: DataFrame,
    payload_schema: StructType,
    ingestion_time: Column | None = None,
) -> DataFrame:
    """Shape a raw Kafka source DataFrame into the bronze schema.

    Adds ingestion metadata and a parsed ``data`` struct, **without** applying any
    business transformation (no dedup, no normalization) — bronze stays as-is.

    Args:
        raw: A DataFrame with the standard Kafka source columns
            (``key``, ``value``, ``topic``, ``partition``, ``offset``,
            ``timestamp``).
        payload_schema: Schema used to parse the JSON ``value`` into ``data``.
        ingestion_time: Column for ``ingestion_time``; defaults to
            ``current_timestamp()``. Injectable so tests can pin the instant.

    Returns:
        A DataFrame with :data:`BRONZE_COLUMNS`, partition-ready on
        ``ingestion_date`` (the UTC calendar day of ``ingestion_time``).
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
        .withColumn(
            "ingestion_time",
            current_timestamp() if ingestion_time is None else ingestion_time,
        )
        # UTC calendar day of ingestion. ``to_date`` would use
        # spark.sql.session.timeZone (the JVM default when unset), so partition
        # boundaries would depend on the host; epoch-day arithmetic is TZ-free.
        .withColumn(
            "ingestion_date",
            expr("date_from_unix_date(CAST(floor(unix_seconds(ingestion_time) / 86400) AS INT))"),
        )
    )
