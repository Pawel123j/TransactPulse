"""Fraud-scoring ML integration for the gold layer.

A small, swappable model artifact (``FraudModel``) is applied to silver
transactions as a vectorized Spark ``pandas_udf`` to produce ``fraud_score`` /
``fraud_flag`` in ``gold/scored_transactions`` (see ADR 0006).
"""

__all__ = ["__version__"]

__version__ = "0.6.0"
