"""SparkSession factory wired for Delta Lake and MinIO (S3A).

The required jars (delta-spark, spark-sql-kafka, hadoop-aws, aws-java-sdk-bundle)
are supplied to ``spark-submit`` via ``--packages`` (see the Dockerfile); here we
only set the runtime configuration.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from streaming.config import BronzeConfig


def build_spark_session(config: BronzeConfig) -> SparkSession:
    """Create a SparkSession configured for Delta + S3A access to MinIO.

    Args:
        config: Bronze job configuration (endpoint, credentials, app name).

    Returns:
        A ready-to-use :class:`~pyspark.sql.SparkSession`.
    """
    builder = (
        SparkSession.builder.appName(config.app_name)
        # --- Delta Lake ---
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # --- S3A / MinIO ---
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
        # Smaller shuffle for a single-node demo.
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
