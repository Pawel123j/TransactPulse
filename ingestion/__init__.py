"""TransactPulse ingestion package.

Synthetic financial-transaction generation and Kafka publishing.

Public surface:
    - :class:`~ingestion.schema.Transaction` and schema helpers.
    - :class:`~ingestion.generator.TransactionGenerator`.
    - :class:`~ingestion.config.GeneratorConfig` / :class:`~ingestion.config.KafkaConfig`.
    - :class:`~ingestion.serializers.JsonSerializer`.
"""

from ingestion.config import GeneratorConfig, KafkaConfig
from ingestion.generator import TransactionGenerator
from ingestion.schema import Transaction, validate

__all__ = [
    "GeneratorConfig",
    "KafkaConfig",
    "Transaction",
    "TransactionGenerator",
    "validate",
]

__version__ = "0.2.0"
