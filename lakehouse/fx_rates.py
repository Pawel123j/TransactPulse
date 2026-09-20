"""Synthetic FX rates used to normalize transaction amounts to PLN.

These are **fixed, synthetic** rates (no live FX feed) — appropriate for a
synthetic-data portfolio project (see ADR 0005). ``amount_pln = amount * rate``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

#: ISO 4217 currency -> multiplier to PLN. Must cover every currency the
#: generator can emit (kept consistent with ``ingestion.schema.CURRENCIES``).
FX_RATES_TO_PLN: dict[str, float] = {
    "PLN": 1.00,
    "EUR": 4.30,
    "USD": 3.95,
    "GBP": 5.05,
    "CZK": 0.17,
    "UAH": 0.10,
    "CNY": 0.55,
}


def fx_rates_dataframe(spark: SparkSession) -> DataFrame:
    """Return the FX table as a Spark DataFrame ``(currency, fx_rate_to_pln)``."""
    rows = [(currency, float(rate)) for currency, rate in FX_RATES_TO_PLN.items()]
    return spark.createDataFrame(rows, ["currency", "fx_rate_to_pln"])
