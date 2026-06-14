"""Feature engineering for fraud scoring.

The same features are computable two ways, kept consistent by tests:

* :func:`build_features` — pure-Python, one record at a time (readable, unit-tested);
* :func:`features_frame` — vectorized over a pandas DataFrame (used by the Spark
  ``pandas_udf`` scoring path).

Only fields available in the **silver** table are used (no label leakage):
``event_time``, ``amount_pln``, ``channel``, ``merchant_category``, ``country``.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

#: Ordered model feature names (the estimator expects this column order).
FEATURE_NAMES: tuple[str, ...] = (
    "log_amount_pln",
    "hour",
    "is_night",
    "is_online",
    "is_atm",
    "is_high_risk_category",
    "is_foreign",
)

#: Merchant categories empirically associated with elevated fraud risk.
HIGH_RISK_CATEGORIES: frozenset[str] = frozenset(
    {"cash_withdrawal", "gambling", "electronics", "online_services", "travel"}
)

#: Home country; anything else is treated as cross-border.
HOME_COUNTRY = "PL"


def _hour_of(event_time: Any) -> int:
    """Extract hour-of-day from a datetime or ISO-8601 string (0 on failure)."""
    if isinstance(event_time, datetime):
        return int(event_time.hour)
    if isinstance(event_time, str) and event_time:
        try:
            return int(datetime.fromisoformat(event_time).hour)
        except ValueError:
            return 0
    return 0


def build_features(record: dict[str, Any]) -> dict[str, float]:
    """Build the model feature vector for a single silver record."""
    amount_pln = float(record.get("amount_pln") or 0.0)
    hour = _hour_of(record.get("event_time"))
    channel = (record.get("channel") or "").upper()
    category = (record.get("merchant_category") or "").lower()
    country = (record.get("country") or "").upper()

    return {
        "log_amount_pln": math.log1p(max(amount_pln, 0.0)),
        "hour": float(hour),
        "is_night": 1.0 if (hour < 6 or hour >= 22) else 0.0,
        "is_online": 1.0 if channel == "ONLINE" else 0.0,
        "is_atm": 1.0 if channel == "ATM" else 0.0,
        "is_high_risk_category": 1.0 if category in HIGH_RISK_CATEGORIES else 0.0,
        "is_foreign": 1.0 if country != HOME_COUNTRY else 0.0,
    }


def features_frame(pdf: pd.DataFrame) -> pd.DataFrame:
    """Vectorized feature computation over a pandas DataFrame of silver rows.

    Returns a DataFrame with exactly :data:`FEATURE_NAMES` columns (in order).
    """
    import numpy as np
    import pandas as pd

    amount = pd.to_numeric(pdf.get("amount_pln"), errors="coerce").fillna(0.0).clip(lower=0.0)
    hours = pd.to_datetime(pdf.get("event_time"), errors="coerce", utc=True).dt.hour.fillna(0)
    channel = pdf.get("channel").astype("string").str.upper().fillna("")
    category = pdf.get("merchant_category").astype("string").str.lower().fillna("")
    country = pdf.get("country").astype("string").str.upper().fillna("")

    out = pd.DataFrame(
        {
            "log_amount_pln": np.log1p(amount.to_numpy(dtype="float64")),
            "hour": hours.astype("float64"),
            "is_night": ((hours < 6) | (hours >= 22)).astype("float64"),
            "is_online": (channel == "ONLINE").astype("float64"),
            "is_atm": (channel == "ATM").astype("float64"),
            "is_high_risk_category": category.isin(HIGH_RISK_CATEGORIES).astype("float64"),
            "is_foreign": (country != HOME_COUNTRY).astype("float64"),
        }
    )
    return out[list(FEATURE_NAMES)]
