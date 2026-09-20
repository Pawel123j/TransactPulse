"""Data access for the Streamlit dashboard.

Reads the gold Delta tables through DuckDB (reusing ``analytics.duckdb_query``)
and the per-run JSON reports (data quality, drift) from the shared reports volume.
Kept import-light and defensive so the dashboard degrades gracefully when a table
or report does not exist yet (fresh stack).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from analytics.duckdb_query import DuckDBConfig, connect, register_gold_views

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd


@dataclass(slots=True)
class DashboardConfig:
    """Dashboard data-source settings."""

    duckdb: DuckDBConfig
    reports_dir: str = "/reports"

    @classmethod
    def from_env(cls) -> DashboardConfig:
        return cls(
            duckdb=DuckDBConfig.from_env(),
            reports_dir=os.getenv("REPORTS_DIR", "/reports"),
        )


def open_gold(config: DashboardConfig):
    """Open a DuckDB connection with the gold tables registered as views."""
    con = connect(config.duckdb)
    register_gold_views(con, config.duckdb)
    return con


def query_df(con, sql: str) -> pd.DataFrame:
    """Run ``sql`` and return a DataFrame; empty DataFrame on any failure."""
    import pandas as pd

    try:
        return con.execute(sql).fetchdf()
    except Exception:  # noqa: BLE001 - dashboard must not crash on missing tables
        return pd.DataFrame()


def read_json_report(path: str | Path) -> dict[str, Any] | None:
    """Load a JSON report (DQ / drift), or ``None`` if missing/unreadable."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (ValueError, OSError):  # pragma: no cover - defensive
        return None


# --- Query strings (pure; unit-testable) ------------------------------------ #

KPI_SQL = """
SELECT
    COALESCE(SUM(tx_count), 0)            AS total_tx,
    COALESCE(SUM(total_volume_pln), 0)   AS total_volume_pln,
    COALESCE(SUM(fraud_count), 0)        AS total_fraud,
    COUNT(DISTINCT country)              AS countries
FROM daily_volume_by_country
"""

VOLUME_BY_COUNTRY_SQL = """
SELECT country,
       SUM(tx_count)            AS tx_count,
       SUM(total_volume_pln)   AS total_volume_pln,
       SUM(fraud_count)        AS fraud_count
FROM daily_volume_by_country
GROUP BY country
ORDER BY total_volume_pln DESC
"""

DAILY_TREND_SQL = """
SELECT event_date,
       SUM(tx_count)    AS tx_count,
       SUM(fraud_count) AS fraud_count
FROM daily_volume_by_country
GROUP BY event_date
ORDER BY event_date
"""

CATEGORY_KPI_SQL = """
SELECT merchant_category, tx_count, fraud_count, fraud_rate, total_volume_pln
FROM merchant_category_kpi
ORDER BY fraud_rate DESC
"""

TOP_SCORED_SQL = """
SELECT transaction_id, account_id, amount_pln, country, merchant_category,
       ROUND(fraud_score, 4) AS fraud_score, is_fraud_label
FROM scored_transactions
WHERE fraud_flag = TRUE
ORDER BY fraud_score DESC
LIMIT 50
"""

TOP_SIGNALS_SQL = """
SELECT transaction_id, account_id, amount_pln, country, channel,
       merchant_category, signal_count
FROM fraud_signals
WHERE signal_count >= 3
ORDER BY signal_count DESC, amount_pln DESC
LIMIT 50
"""

#: ISO 3166-1 alpha-2 -> alpha-3 for the choropleth map.
ISO2_TO_ISO3: dict[str, str] = {
    "PL": "POL",
    "DE": "DEU",
    "FR": "FRA",
    "ES": "ESP",
    "IT": "ITA",
    "NL": "NLD",
    "SK": "SVK",
    "GB": "GBR",
    "US": "USA",
    "CZ": "CZE",
    "UA": "UKR",
    "CN": "CHN",
}
