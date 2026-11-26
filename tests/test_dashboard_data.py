"""Tests for the dashboard data-access helpers (no Streamlit/DuckDB needed)."""

from __future__ import annotations

import json

import pytest

from dashboard.data_access import (
    KPI_SQL,
    VOLUME_BY_COUNTRY_SQL,
    DashboardConfig,
    query_df,
    read_json_report,
)


def test_read_json_report_roundtrip(tmp_path) -> None:
    path = tmp_path / "r.json"
    path.write_text(json.dumps({"metrics": {"row_count": 5}}), encoding="utf-8")
    report = read_json_report(path)
    assert report is not None
    assert report["metrics"]["row_count"] == 5


def test_read_json_report_missing_returns_none(tmp_path) -> None:
    assert read_json_report(tmp_path / "nope.json") is None


def test_sql_constants_are_nonempty_selects() -> None:
    for sql in (KPI_SQL, VOLUME_BY_COUNTRY_SQL):
        assert "SELECT" in sql.upper()


def test_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("REPORTS_DIR", "/tmp/reports")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lh")
    config = DashboardConfig.from_env()
    assert config.reports_dir == "/tmp/reports"
    assert config.duckdb.bucket == "lh"


def test_query_failure_is_not_reported_as_zero_transactions() -> None:
    import duckdb

    with duckdb.connect() as con, pytest.raises(duckdb.CatalogException):
        query_df(con, KPI_SQL)


def _local_gold(tmp_path, monkeypatch, *, skip: str | None = None) -> DashboardConfig:
    """Map every ``s3://…/gold/<table>`` to an empty local Delta table (or none)."""
    deltalake = pytest.importorskip("deltalake")
    pa = pytest.importorskip("pyarrow")
    from analytics.duckdb_query import GOLD_TABLES, DuckDBConfig

    schema = pa.schema(
        [
            ("event_date", pa.date32()),
            ("country", pa.string()),
            ("tx_count", pa.int64()),
            ("total_volume_pln", pa.float64()),
            ("fraud_count", pa.int64()),
        ]
    )
    for table in GOLD_TABLES:
        if table != skip:
            deltalake.write_deltalake(tmp_path / table, schema.empty_table())
    real = deltalake.DeltaTable
    monkeypatch.setattr(
        deltalake,
        "DeltaTable",
        lambda uri, storage_options: real(tmp_path / uri.rsplit("/", 1)[1]),
    )
    return DashboardConfig(duckdb=DuckDBConfig(s3_endpoint="https://storage.example.test"))


def test_empty_gold_table_reads_as_zero_activity(tmp_path, monkeypatch) -> None:
    from dashboard.data_access import DAILY_TREND_SQL, open_gold

    con = open_gold(_local_gold(tmp_path, monkeypatch))
    try:
        # Aggregate without GROUP BY: always one row, COALESCEd to zeros.
        assert query_df(con, KPI_SQL).values.tolist() == [[0, 0, 0, 0]]
        assert query_df(con, VOLUME_BY_COUNTRY_SQL).empty
        assert query_df(con, DAILY_TREND_SQL).empty
    finally:
        con.close()


def test_missing_gold_table_is_an_error_not_zero(tmp_path, monkeypatch) -> None:
    from deltalake.exceptions import TableNotFoundError

    from dashboard.data_access import open_gold

    config = _local_gold(tmp_path, monkeypatch, skip="daily_volume_by_country")
    with pytest.raises(TableNotFoundError):
        open_gold(config)


def test_unreachable_storage_is_an_error_not_zero() -> None:
    pytest.importorskip("deltalake")
    from analytics.duckdb_query import DuckDBConfig
    from dashboard.data_access import open_gold

    # Real S3 client, nothing listens on the discard port: no mocks involved.
    config = DashboardConfig(duckdb=DuckDBConfig(s3_endpoint="http://127.0.0.1:9"))
    with pytest.raises(OSError):
        open_gold(config)
