"""Bronze ingest: Kafka ``transactions.raw`` -> Delta ``s3a://lakehouse/bronze``.

Run via spark-submit (jars provided with ``--packages``)::

    spark-submit \
      --packages io.delta:delta-spark_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      streaming/bronze_job.py

Configuration is environment-driven (see :class:`streaming.config.BronzeConfig`).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from streaming.config import BronzeConfig
from streaming.schema import TRANSACTION_PAYLOAD_SCHEMA
from streaming.spark_session import build_spark_session
from streaming.transforms import build_bronze_frame

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("streaming.bronze")


def read_kafka_stream(spark: SparkSession, config: BronzeConfig) -> DataFrame:
    """Open the Kafka source as a streaming DataFrame."""
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", config.starting_offsets)
        # Resilience for a local demo where the topic may be recreated (ADR 0004).
        .option("failOnDataLoss", "false")
    )
    if config.max_offsets_per_trigger is not None:
        reader = reader.option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
    return reader.load()


def _apply_trigger(writer: DataStreamWriter, trigger: str) -> DataStreamWriter:
    """Translate the configured trigger string into a writer trigger."""
    normalized = trigger.strip().lower()
    if normalized in {"availablenow", "available_now"}:
        return writer.trigger(availableNow=True)
    if normalized == "once":
        return writer.trigger(once=True)
    return writer.trigger(processingTime=trigger)


def write_bronze(frame: DataFrame, config: BronzeConfig) -> StreamingQuery:
    """Write the bronze frame to Delta with checkpointing and date partitioning."""
    writer = (
        frame.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path)
        .partitionBy("ingestion_date")
    )
    return _apply_trigger(writer, config.trigger).start(config.bronze_path)


def main() -> None:
    """Entry point: build the session and run the bronze stream to completion."""
    config = BronzeConfig.from_env()
    logger.info(
        "starting bronze ingest: topic=%s -> %s (trigger=%s)",
        config.kafka_topic,
        config.bronze_path,
        config.trigger,
    )
    spark = build_spark_session(config)

    raw = read_kafka_stream(spark, config)
    bronze = build_bronze_frame(raw, TRANSACTION_PAYLOAD_SCHEMA)
    query = write_bronze(bronze, config)

    logger.info("bronze stream started (id=%s); awaiting termination", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()
