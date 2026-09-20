"""Tests for the dashboard data-access helpers (no Streamlit/DuckDB needed)."""

from __future__ import annotations

import json

from dashboard.data_access import (
    ISO2_TO_ISO3,
    KPI_SQL,
    VOLUME_BY_COUNTRY_SQL,
    DashboardConfig,
    read_json_report,
)
from lakehouse.dq_rules import ALLOWED_COUNTRIES


def test_read_json_report_roundtrip(tmp_path) -> None:
    path = tmp_path / "r.json"
    path.write_text(json.dumps({"metrics": {"row_count": 5}}), encoding="utf-8")
    report = read_json_report(path)
    assert report is not None
    assert report["metrics"]["row_count"] == 5


def test_read_json_report_missing_returns_none(tmp_path) -> None:
    assert read_json_report(tmp_path / "nope.json") is None


def test_iso_map_covers_all_allowed_countries() -> None:
    # Every domain country must have an ISO-3 mapping for the choropleth.
    assert set(ISO2_TO_ISO3) >= ALLOWED_COUNTRIES
    assert all(len(v) == 3 for v in ISO2_TO_ISO3.values())


def test_sql_constants_are_nonempty_selects() -> None:
    for sql in (KPI_SQL, VOLUME_BY_COUNTRY_SQL):
        assert "SELECT" in sql.upper()


def test_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("REPORTS_DIR", "/tmp/reports")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lh")
    config = DashboardConfig.from_env()
    assert config.reports_dir == "/tmp/reports"
    assert config.duckdb.bucket == "lh"
