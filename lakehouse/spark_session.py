"""SparkSession factory for lakehouse batch jobs (Delta + MinIO/S3A)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

from lakehouse.config import GoldConfig, SilverConfig


def build_spark_session(config: SilverConfig | GoldConfig) -> SparkSession:
    """Create a SparkSession configured for Delta and S3A access to MinIO."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.optimize.repartition.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
