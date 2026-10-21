"""Keep HTTPS enabled in both real Spark builder configurations."""

from unittest.mock import MagicMock

import pytest

from lakehouse.config import GoldConfig, SilverConfig
from streaming.config import BronzeConfig
from streaming.s3_config import ssl_enabled


@pytest.mark.parametrize("config_type", [BronzeConfig, SilverConfig, GoldConfig])
@pytest.mark.parametrize("scheme, expected", [("http", "false"), ("https", "true")])
def test_spark_builders_preserve_transport(monkeypatch, config_type, scheme, expected):
    import pyspark.sql

    from lakehouse.spark_session import build_spark_session as batch
    from streaming.spark_session import build_spark_session as stream

    builder = MagicMock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    monkeypatch.setattr(pyspark.sql.SparkSession, "builder", builder)
    build = stream if config_type is BronzeConfig else batch
    build(config_type(s3_endpoint=f"{scheme}://storage.example:9000"))
    builder.config.assert_any_call("spark.hadoop.fs.s3a.connection.ssl.enabled", expected)
    builder.config.assert_any_call(
        "spark.hadoop.fs.s3a.endpoint", f"{scheme}://storage.example:9000"
    )


@pytest.mark.parametrize(
    "endpoint",
    [
        "storage.example:9000",
        "ftp://storage.example",
        "https://",
        "https://user:pass@host",
        "https://host/bucket",
        "https://host?key=value",
        "http://host:bad",
        "http://host:70000",
    ],
)
def test_invalid_endpoint_rejected(endpoint):
    with pytest.raises(ValueError):
        ssl_enabled(endpoint)
