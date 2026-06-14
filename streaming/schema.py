"""Spark read schema for the transaction JSON payload.

Mirrors the producer's record schema (``ingestion/schema.py``). Note that
``timestamp`` is kept as a *string* in bronze — bronze stores data as-is; casting
to a real timestamp is a silver-layer concern (Stage 5).
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

#: Schema used to parse the raw JSON ``value`` from Kafka into a ``data`` struct.
TRANSACTION_PAYLOAD_SCHEMA: StructType = StructType(
    [
        StructField("transaction_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("account_id", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("merchant_category", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
        StructField("device_id", StringType(), nullable=True),
        StructField("is_fraud_label", BooleanType(), nullable=True),
    ]
)
