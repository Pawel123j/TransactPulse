"""Configuration objects for the generator and Kafka producer.

All settings can be supplied programmatically or sourced from environment
variables via the ``from_env`` constructors, keeping the runtime 12-factor
friendly and the Docker setup simple.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"environment variable {name}={raw!r} is not a float") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"environment variable {name}={raw!r} is not an int") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class GeneratorConfig:
    """Parameters controlling synthetic data generation and emission.

    Attributes:
        rate: Target throughput in transactions per second (base rate).
        duration_seconds: How long to emit for; ``0`` means run indefinitely.
        fraud_rate: Probability a transaction is labelled fraudulent (~0.003).
        late_event_rate: Probability an event is late-arriving (delayed timestamp).
        late_max_delay_seconds: Maximum backdating applied to a late event.
        num_accounts: Size of the synthetic account pool.
        num_devices: Size of the synthetic device pool.
        apply_seasonality: Modulate the emission rate by an hourly weight curve.
        burst_enabled: Periodically multiply the rate to simulate traffic spikes.
        burst_factor: Rate multiplier during a burst window.
        burst_interval_seconds: Period between the start of consecutive bursts.
        burst_duration_seconds: Duration of each burst window.
        seed: RNG seed for reproducible output (idempotency); ``None`` = random.
    """

    rate: float = 20.0
    duration_seconds: int = 0
    fraud_rate: float = 0.003
    late_event_rate: float = 0.02
    late_max_delay_seconds: int = 600
    num_accounts: int = 5000
    num_devices: int = 8000
    apply_seasonality: bool = True
    burst_enabled: bool = False
    burst_factor: float = 5.0
    burst_interval_seconds: int = 60
    burst_duration_seconds: int = 10
    seed: int | None = None

    def __post_init__(self) -> None:
        if self.rate <= 0:
            raise ValueError("rate must be > 0")
        if not 0.0 <= self.fraud_rate <= 1.0:
            raise ValueError("fraud_rate must be in [0, 1]")
        if not 0.0 <= self.late_event_rate <= 1.0:
            raise ValueError("late_event_rate must be in [0, 1]")
        if self.num_accounts <= 0 or self.num_devices <= 0:
            raise ValueError("num_accounts and num_devices must be > 0")
        if self.burst_factor < 1.0:
            raise ValueError("burst_factor must be >= 1")

    @classmethod
    def from_env(cls) -> GeneratorConfig:
        """Build a config from ``GEN_*`` environment variables, falling back to defaults."""
        # A default instance exposes the field defaults (slots shadow class attrs).
        d = cls()
        seed_raw = os.getenv("GEN_SEED")
        return cls(
            rate=_env_float("GEN_RATE", d.rate),
            duration_seconds=_env_int("GEN_DURATION_SECONDS", d.duration_seconds),
            fraud_rate=_env_float("GEN_FRAUD_RATE", d.fraud_rate),
            late_event_rate=_env_float("GEN_LATE_EVENT_RATE", d.late_event_rate),
            late_max_delay_seconds=_env_int(
                "GEN_LATE_MAX_DELAY_SECONDS", d.late_max_delay_seconds
            ),
            num_accounts=_env_int("GEN_NUM_ACCOUNTS", d.num_accounts),
            num_devices=_env_int("GEN_NUM_DEVICES", d.num_devices),
            apply_seasonality=_env_bool("GEN_APPLY_SEASONALITY", d.apply_seasonality),
            burst_enabled=_env_bool("GEN_BURST_ENABLED", d.burst_enabled),
            burst_factor=_env_float("GEN_BURST_FACTOR", d.burst_factor),
            burst_interval_seconds=_env_int(
                "GEN_BURST_INTERVAL_SECONDS", d.burst_interval_seconds
            ),
            burst_duration_seconds=_env_int(
                "GEN_BURST_DURATION_SECONDS", d.burst_duration_seconds
            ),
            seed=int(seed_raw) if seed_raw not in (None, "") else None,
        )


@dataclass(slots=True)
class KafkaConfig:
    """Kafka producer connection settings.

    Attributes:
        bootstrap_servers: Comma-separated broker list.
        topic: Destination topic for raw transactions.
        client_id: Producer client identifier.
        acks: Producer acknowledgement policy ("all" for durability).
        linger_ms: Batching delay to improve throughput.
        compression_type: Wire compression codec.
        extra: Additional librdkafka config overrides.
    """

    bootstrap_servers: str = "localhost:9092"
    topic: str = "transactions.raw"
    client_id: str = "transactpulse-generator"
    acks: str = "all"
    linger_ms: int = 50
    compression_type: str = "snappy"
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> KafkaConfig:
        """Build Kafka settings from ``KAFKA_*`` environment variables."""
        d = cls()
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", d.bootstrap_servers),
            topic=os.getenv("KAFKA_TOPIC", d.topic),
            client_id=os.getenv("KAFKA_CLIENT_ID", d.client_id),
            acks=os.getenv("KAFKA_ACKS", d.acks),
            linger_ms=_env_int("KAFKA_LINGER_MS", d.linger_ms),
            compression_type=os.getenv("KAFKA_COMPRESSION_TYPE", d.compression_type),
        )

    def to_librdkafka(self) -> dict[str, object]:
        """Render the settings as a confluent-kafka producer config dict."""
        conf: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "linger.ms": self.linger_ms,
            "compression.type": self.compression_type,
            "enable.idempotence": True,
        }
        conf.update(self.extra)
        return conf
