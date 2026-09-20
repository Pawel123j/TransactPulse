"""Configuration for the bronze streaming job (env-driven, 12-factor)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_opt_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw in (None, ""):
        return None
    return int(raw)


@dataclass(slots=True)
class BronzeConfig:
    """Settings for the Kafka -> Delta bronze ingest.

    Attributes:
        kafka_bootstrap_servers: Broker list (``kafka:9092`` in Docker,
            ``localhost:9092`` from the host).
        kafka_topic: Source topic.
        starting_offsets: ``earliest`` / ``latest`` for a fresh checkpoint.
        s3_endpoint: MinIO S3 endpoint URL.
        s3_access_key: MinIO access key.
        s3_secret_key: MinIO secret key.
        bucket: Lakehouse bucket name.
        trigger: ``processingTime`` value (e.g. ``10 seconds``), ``availableNow``
            or ``once`` — controls streaming vs batch execution.
        max_offsets_per_trigger: Optional rate limit (records per micro-batch).
        app_name: Spark application name.
    """

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "transactions.raw"
    starting_offsets: str = "earliest"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"
    bucket: str = "lakehouse"
    trigger: str = "10 seconds"
    max_offsets_per_trigger: int | None = None
    app_name: str = "transactpulse-bronze"

    @property
    def bronze_path(self) -> str:
        """Delta table location for raw transactions."""
        return f"s3a://{self.bucket}/bronze/transactions"

    @property
    def checkpoint_path(self) -> str:
        """Structured Streaming checkpoint location (couples to the bronze sink)."""
        return f"s3a://{self.bucket}/_checkpoints/bronze_transactions"

    @classmethod
    def from_env(cls) -> BronzeConfig:
        """Build the config from environment variables, falling back to defaults."""
        d = cls()
        return cls(
            kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            kafka_topic=_env("KAFKA_TOPIC", d.kafka_topic),
            starting_offsets=_env("STREAMING_STARTING_OFFSETS", d.starting_offsets),
            s3_endpoint=_env("S3_ENDPOINT", d.s3_endpoint),
            s3_access_key=_env("S3_ACCESS_KEY", d.s3_access_key),
            s3_secret_key=_env("S3_SECRET_KEY", d.s3_secret_key),
            bucket=_env("LAKEHOUSE_BUCKET", d.bucket),
            trigger=_env("STREAMING_TRIGGER", d.trigger),
            max_offsets_per_trigger=_env_opt_int("STREAMING_MAX_OFFSETS_PER_TRIGGER"),
            app_name=_env("STREAMING_APP_NAME", d.app_name),
        )
