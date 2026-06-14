"""TransactPulse streaming package.

Spark Structured Streaming ingestion from Kafka into the Delta **bronze** layer
on MinIO. Business transformations are intentionally absent here — bronze stores
data as-is (see ADR 0001 / ADR 0004).
"""

__all__ = ["__version__"]

__version__ = "0.4.0"
