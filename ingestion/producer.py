"""Kafka producer for synthetic transactions and a stdout sink for dry runs.

``confluent_kafka`` is imported lazily so that the generator, schema and
serializers remain importable (and unit-testable) without librdkafka installed.
"""

from __future__ import annotations

import logging
import sys
from typing import Any, Protocol

from ingestion.config import KafkaConfig
from ingestion.schema import Transaction
from ingestion.serializers import JsonSerializer, Serializer

logger = logging.getLogger(__name__)


class Sink(Protocol):
    """Destination for generated transactions."""

    def send(self, transaction: Transaction) -> None:
        """Publish a single transaction."""
        ...

    def flush(self) -> None:
        """Flush any buffered records."""
        ...

    @property
    def delivered(self) -> int:
        """Number of successfully delivered records."""
        ...

    @property
    def failed(self) -> int:
        """Number of failed deliveries."""
        ...


class StdoutSink:
    """A no-Kafka sink that writes serialized records to stdout (for ``--dry-run``)."""

    def __init__(self, serializer: Serializer | None = None) -> None:
        self._serializer = serializer or JsonSerializer()
        self._delivered = 0

    def send(self, transaction: Transaction) -> None:
        payload = self._serializer.serialize(transaction.to_dict())
        sys.stdout.write(payload.decode("utf-8") + "\n")
        self._delivered += 1

    def flush(self) -> None:
        sys.stdout.flush()

    @property
    def delivered(self) -> int:
        return self._delivered

    @property
    def failed(self) -> int:
        return 0


class KafkaTransactionProducer:
    """Publish transactions to Kafka, keyed by ``account_id`` for stable partitioning.

    Args:
        config: Kafka connection settings.
        serializer: Value serializer (defaults to JSON per ADR 0003).
        poll_interval: Produce calls between background ``poll(0)`` invocations.

    Use as a context manager to guarantee a final ``flush``::

        with KafkaTransactionProducer(KafkaConfig.from_env()) as producer:
            producer.send(transaction)
    """

    def __init__(
        self,
        config: KafkaConfig,
        serializer: Serializer | None = None,
        poll_interval: int = 1,
    ) -> None:
        self.config = config
        self._serializer = serializer or JsonSerializer()
        self._poll_interval = max(poll_interval, 1)
        self._since_poll = 0
        self._delivered = 0
        self._failed = 0

        try:
            from confluent_kafka import Producer  # noqa: PLC0415 (lazy, optional dep)
        except ImportError as exc:  # pragma: no cover - exercised only without the dep
            raise RuntimeError(
                "confluent-kafka is required for Kafka publishing. "
                "Install it (pip install -r ingestion/requirements.txt) or use --dry-run."
            ) from exc

        self._producer = Producer(config.to_librdkafka())

    def _on_delivery(self, err: Any, msg: Any) -> None:
        """librdkafka delivery report callback."""
        if err is not None:
            self._failed += 1
            logger.error("delivery failed for key=%s: %s", msg.key(), err)
        else:
            self._delivered += 1

    def send(self, transaction: Transaction) -> None:
        """Serialize and enqueue a transaction for asynchronous delivery."""
        value = self._serializer.serialize(transaction.to_dict())
        key = transaction.account_id.encode("utf-8")
        try:
            self._producer.produce(
                topic=self.config.topic,
                key=key,
                value=value,
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # Local queue is full: block briefly to let deliveries drain, then retry.
            self._producer.poll(1)
            self._producer.produce(
                topic=self.config.topic,
                key=key,
                value=value,
                on_delivery=self._on_delivery,
            )

        self._since_poll += 1
        if self._since_poll >= self._poll_interval:
            self._producer.poll(0)
            self._since_poll = 0

    def flush(self, timeout: float = 30.0) -> None:
        """Block until all outstanding messages are delivered or ``timeout`` elapses."""
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.warning("%d message(s) still in queue after flush timeout", remaining)

    @property
    def delivered(self) -> int:
        return self._delivered

    @property
    def failed(self) -> int:
        return self._failed

    def __enter__(self) -> KafkaTransactionProducer:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.flush()
