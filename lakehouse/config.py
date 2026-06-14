"""Configuration for the silver batch job (env-driven)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default


@dataclass(slots=True)
class SilverConfig:
    """Settings for the bronze -> silver refinement.

    Attributes:
        s3_endpoint / s3_access_key / s3_secret_key: MinIO connection.
        bucket: Lakehouse bucket.
        watermark_horizon_days: How far back (by ``ingestion_date``) to reprocess,
            bounding late-arriving data handling.
        optimize_zorder: Run ``OPTIMIZE ... ZORDER BY (account_id)`` after writing.
        docs_dir: Where the data-quality report is written.
        app_name: Spark application name.
    """

    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"
    bucket: str = "lakehouse"
    watermark_horizon_days: int = 2
    optimize_zorder: bool = True
    docs_dir: str = "docs/data_quality"
    app_name: str = "transactpulse-silver"

    @property
    def bronze_path(self) -> str:
        return f"s3a://{self.bucket}/bronze/transactions"

    @property
    def silver_path(self) -> str:
        return f"s3a://{self.bucket}/silver/transactions"

    @property
    def quarantine_path(self) -> str:
        return f"s3a://{self.bucket}/silver/quarantine"

    @classmethod
    def from_env(cls) -> SilverConfig:
        """Build the config from environment variables, falling back to defaults."""
        d = cls()
        return cls(
            s3_endpoint=_env("S3_ENDPOINT", d.s3_endpoint),
            s3_access_key=_env("S3_ACCESS_KEY", d.s3_access_key),
            s3_secret_key=_env("S3_SECRET_KEY", d.s3_secret_key),
            bucket=_env("LAKEHOUSE_BUCKET", d.bucket),
            watermark_horizon_days=_env_int(
                "SILVER_WATERMARK_HORIZON_DAYS", d.watermark_horizon_days
            ),
            optimize_zorder=_env("SILVER_OPTIMIZE_ZORDER", "true").lower()
            in {"1", "true", "yes", "on"},
            docs_dir=_env("SILVER_DOCS_DIR", d.docs_dir),
            app_name=_env("SILVER_APP_NAME", d.app_name),
        )


@dataclass(slots=True)
class GoldConfig:
    """Settings for the silver -> gold aggregates + ML scoring + drift job.

    Attributes:
        s3_endpoint / s3_access_key / s3_secret_key / bucket: MinIO connection.
        model_path: Path to the pickled fraud model artifact.
        fraud_threshold: Score threshold for the ``fraud_flag`` (overrides model).
        docs_dir: Where the drift report is written.
        app_name: Spark application name.
    """

    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"
    bucket: str = "lakehouse"
    model_path: str = "models/fraud_model.pkl"
    fraud_threshold: float = 0.5
    docs_dir: str = "docs/drift"
    app_name: str = "transactpulse-gold"

    @property
    def silver_path(self) -> str:
        return f"s3a://{self.bucket}/silver/transactions"

    def gold_path(self, table: str) -> str:
        """Delta path for a named gold table."""
        return f"s3a://{self.bucket}/gold/{table}"

    @classmethod
    def from_env(cls) -> GoldConfig:
        """Build the config from environment variables, falling back to defaults."""
        d = cls()
        threshold = os.getenv("GOLD_FRAUD_THRESHOLD")
        return cls(
            s3_endpoint=_env("S3_ENDPOINT", d.s3_endpoint),
            s3_access_key=_env("S3_ACCESS_KEY", d.s3_access_key),
            s3_secret_key=_env("S3_SECRET_KEY", d.s3_secret_key),
            bucket=_env("LAKEHOUSE_BUCKET", d.bucket),
            model_path=_env("GOLD_MODEL_PATH", d.model_path),
            fraud_threshold=float(threshold) if threshold not in (None, "") else d.fraud_threshold,
            docs_dir=_env("GOLD_DOCS_DIR", d.docs_dir),
            app_name=_env("GOLD_APP_NAME", d.app_name),
        )
