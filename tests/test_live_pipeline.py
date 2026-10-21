"""Opt-in real Kafka/S3A/Delta pipeline on explicitly isolated test resources.

Requires TP_LIVE_INTEGRATION=1, TP_TEST_BUCKET (new private bucket),
TP_TEST_JARS (comma-separated compatible connector jars), KAFKA_BOOTSTRAP_SERVERS,
S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY. Never use production resources.
The caller provisions/removes its unique bucket. This test owns its Kafka topic.
"""

import json
import os
import re
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("TP_LIVE_INTEGRATION") != "1", reason="isolated real services required"
)


def test_live_medallion_pipeline_and_checkpoint_recovery(  # noqa: PLR0915 -- one service lifecycle
    tmp_path, monkeypatch
):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    from pyspark.sql import SparkSession

    from ingestion.config import GeneratorConfig
    from ingestion.generator import TransactionGenerator
    from ingestion.serializers import JsonSerializer
    from streaming.config import BronzeConfig
    from streaming.spark_session import build_spark_session

    bucket = os.environ["TP_TEST_BUCKET"]
    assert re.fullmatch(r"tp-stage[23]-[a-f0-9]{32}", bucket), "Use a new owned test bucket"
    topic = "tp-stage3-" + uuid4().hex
    broker = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    admin = AdminClient({"bootstrap.servers": broker})
    admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(timeout=20)
    spark = None
    try:
        producer = Producer({"bootstrap.servers": broker})
        delivered, errors = [], []

        def delivery(error, message):
            (errors if error else delivered).append(error or message.offset())

        serializer = JsonSerializer()
        records = list(TransactionGenerator(GeneratorConfig(seed=21)).stream(40))
        for record in records:
            producer.produce(topic, serializer.serialize(record.to_dict()), callback=delivery)
        assert producer.flush(20) == 0
        assert not errors and len(delivered) == len(records)

        builder = (
            SparkSession.builder.master("local[1]")
            .config("spark.jars", os.environ["TP_TEST_JARS"])
            .config("spark.ui.enabled", "false")
            .config("spark.databricks.delta.snapshotPartitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.sql.adaptive.enabled", "false")
        )
        monkeypatch.setattr(SparkSession, "builder", builder)
        storage = {
            "s3_endpoint": os.environ["S3_ENDPOINT"],
            "s3_access_key": os.environ["S3_ACCESS_KEY"],
            "s3_secret_key": os.environ["S3_SECRET_KEY"],
            "bucket": bucket,
        }
        bronze_config = BronzeConfig(
            **storage, kafka_bootstrap_servers=broker, kafka_topic=topic, trigger="availableNow"
        )
        spark = build_spark_session(bronze_config)

        _ingest(spark, bronze_config)
        assert spark.read.format("delta").load(bronze_config.bronze_path).count() == 40
        # Restart with the durable S3 checkpoint: committed Kafka offsets must
        # prevent the same messages from being appended for a second time.
        _ingest(spark, bronze_config)
        assert spark.read.format("delta").load(bronze_config.bronze_path).count() == 40

        clean_count = _assert_silver_gold(spark, storage, tmp_path)
        dashboard = _assert_duckdb_dashboard(storage, tmp_path, monkeypatch, clean_count)

        extra = next(TransactionGenerator(GeneratorConfig(seed=22)).stream(1))
        producer.produce(topic, serializer.serialize(extra.to_dict()), callback=delivery)
        assert producer.flush(20) == 0 and not errors
        _ingest(spark, bronze_config)
        assert spark.read.format("delta").load(bronze_config.bronze_path).count() == 41
        new_count = _assert_silver_gold(spark, storage, tmp_path, expected_records=41)
        assert new_count > clean_count
        dashboard.button(key="refresh_pipeline").click().run(timeout=30)
        _assert_dashboard_count(dashboard, new_count)
        _assert_storage_outage(spark, storage)
    finally:
        if spark is not None:
            spark.stop()
        admin.delete_topics([topic])[topic].result(timeout=20)


def _assert_dashboard_count(dashboard, count):
    assert not dashboard.exception
    assert not dashboard.error
    assert next(
        metric.value for metric in dashboard.metric if metric.label == "Transactions"
    ) == str(count)


def _assert_silver_gold(spark, storage, tmp_path, expected_records=40):
    from lakehouse.config import GoldConfig, SilverConfig
    from lakehouse.fx_rates import fx_rates_dataframe
    from lakehouse.gold_job import run_step
    from lakehouse.quality import generate_report
    from lakehouse.silver_job import read_bronze, upsert_silver
    from lakehouse.transforms import to_silver

    silver_config = SilverConfig(
        **storage, optimize_zorder=False, docs_dir=str(tmp_path / "quality")
    )
    valid, quarantine = to_silver(read_bronze(spark, silver_config), fx_rates_dataframe(spark))
    clean_count, rejected_count = valid.count(), quarantine.count()
    assert clean_count > 0 and clean_count + rejected_count == expected_records
    upsert_silver(spark, valid, silver_config)
    upsert_silver(spark, valid, silver_config)
    silver = spark.read.format("delta").load(silver_config.silver_path)
    assert silver.count() == silver.select("transaction_id").distinct().count() == clean_count
    quality = generate_report(
        silver, quarantine_count=rejected_count, docs_dir=silver_config.docs_dir
    )
    assert quality["row_count"] == clean_count

    gold_config = GoldConfig(
        **storage,
        docs_dir=str(tmp_path / "drift"),
        model_path=str(tmp_path / "no-trained-model.pkl"),
    )
    run_step("all", gold_config, spark)
    scored = spark.read.format("delta").load(gold_config.gold_path("scored_transactions"))
    assert scored.count() == clean_count
    assert scored.filter("fraud_score < 0 OR fraud_score > 1 OR fraud_score IS NULL").count() == 0
    assert (
        spark.read.format("delta").load(gold_config.gold_path("daily_volume_by_country")).count()
        > 0
    )
    drift = json.loads((tmp_path / "drift/drift_report.json").read_text())
    assert drift["metrics"]["fraud_score"]["live_n"] == clean_count
    return clean_count


def _assert_duckdb_dashboard(storage, tmp_path, monkeypatch, clean_count):
    from pathlib import Path

    from streamlit.testing.v1 import AppTest

    from analytics.duckdb_query import DuckDBConfig
    from dashboard.data_access import KPI_SQL, DashboardConfig, open_gold, query_df

    cfg = DuckDBConfig(**storage)
    with open_gold(DashboardConfig(cfg)) as con:
        from dashboard import data_access

        for name, sql in vars(data_access).items():
            if name.endswith("_SQL"):
                query_df(con, sql)  # Every dashboard query must match actual Gold schemas.
        assert query_df(con, KPI_SQL).iloc[0]["total_tx"] == clean_count
        assert con.execute("SELECT COUNT(*) FROM scored_transactions").fetchone() == (clean_count,)
    for key, value in {
        "S3_ENDPOINT": storage["s3_endpoint"],
        "S3_ACCESS_KEY": storage["s3_access_key"],
        "S3_SECRET_KEY": storage["s3_secret_key"],
        "LAKEHOUSE_BUCKET": storage["bucket"],
        "REPORTS_DIR": str(tmp_path / "quality"),
        "DUCKDB_DELTA_BACKEND": "arrow",
    }.items():
        monkeypatch.setenv(key, value)
    dashboard = AppTest.from_file(str(Path(__file__).parents[1] / "dashboard/app.py")).run(
        timeout=30
    )
    assert not dashboard.exception
    assert not dashboard.error
    assert next(
        metric.value for metric in dashboard.metric if metric.label == "Transactions"
    ) == str(clean_count)
    return dashboard


def _ingest(spark, config):
    from streaming.bronze_job import read_kafka_stream, write_bronze
    from streaming.schema import TRANSACTION_PAYLOAD_SCHEMA
    from streaming.transforms import build_bronze_frame

    frame = build_bronze_frame(read_kafka_stream(spark, config), TRANSACTION_PAYLOAD_SCHEMA)
    query = write_bronze(frame, config)
    try:
        assert query.awaitTermination(90), "Bronze stream did not finish"
        assert query.exception() is None
    finally:
        if query.isActive:
            query.stop()


def _assert_storage_outage(spark, storage):
    """A real refused S3A connection must propagate an error, never empty data."""
    import socket

    from py4j.protocol import Py4JJavaError

    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
        jvm = spark.sparkContext._jvm
        config = jvm.org.apache.hadoop.conf.Configuration(
            spark.sparkContext._jsc.hadoopConfiguration()
        )
        config.set("fs.s3a.endpoint", f"http://127.0.0.1:{port}")
        config.set("fs.s3a.connection.ssl.enabled", "false")
        config.setInt("fs.s3a.connection.establish.timeout", 500)
        config.setInt("fs.s3a.connection.timeout", 500)
        config.setInt("fs.s3a.attempts.maximum", 1)
        config.setInt("fs.s3a.retry.limit", 0)
        filesystem = None
        try:
            with pytest.raises(Py4JJavaError, match="(?i)connection refused"):
                # newInstance bypasses only Hadoop's client cache, preserving
                # the existing working client and every actual storage object.
                filesystem = jvm.org.apache.hadoop.fs.FileSystem.newInstance(
                    jvm.java.net.URI.create("s3a://" + storage["bucket"]), config
                )
                filesystem.listStatus(jvm.org.apache.hadoop.fs.Path("/"))
        finally:
            if filesystem is not None:
                filesystem.close()
