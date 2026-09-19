"""Shared PySpark test helpers.

The Spark test modules are skipped on machines without PySpark so the fast unit
suite stays installable with ``requirements-dev.txt`` alone. In CI the dedicated
``spark-tests`` job sets ``TP_REQUIRE_SPARK=1``, which turns that skip into a
hard failure — a green CI run therefore means the Spark tests really executed.
"""

from __future__ import annotations

import os

import pytest

#: Set to ``1`` in the CI Spark job; a missing PySpark then fails instead of skipping.
REQUIRE_SPARK_ENV = "TP_REQUIRE_SPARK"


def requires_spark() -> None:
    """Skip the calling module without PySpark — unless PySpark is mandatory."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        if os.getenv(REQUIRE_SPARK_ENV) == "1":
            pytest.fail(
                f"PySpark is required ({REQUIRE_SPARK_ENV}=1) but is not installed; "
                "install requirements-spark.txt",
                pytrace=False,
            )
        pytest.skip("PySpark is not installed", allow_module_level=True)


def local_spark_session(app_name: str = "transactpulse-tests"):
    """Return a minimal local SparkSession used by the transformation tests."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
