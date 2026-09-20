"""TransactPulse lakehouse package.

Silver (and, from Stage 6, gold) batch transformations over Delta tables:
cleansing, deduplication, currency normalization, data-quality gating with a
quarantine table, and ML scoring.
"""

__all__ = ["__version__"]

__version__ = "0.5.0"
