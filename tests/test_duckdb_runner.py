"""Tests for the DuckDB query runner (pure parts + optional DuckDB round-trip)."""

from __future__ import annotations

import pytest

from analytics.duckdb_query import (
    DuckDBConfig,
    run_statements,
    split_statements,
)


def test_split_statements_counts_and_strips() -> None:
    sql = """
    -- a comment
    SELECT 1;
    SELECT 2;  -- trailing
    """
    stmts = split_statements(sql)
    assert stmts == ["SELECT 1", "SELECT 2"]


def test_split_ignores_comment_only_and_blank() -> None:
    assert split_statements("-- just a comment\n\n") == []


def test_split_real_queries_file() -> None:
    from pathlib import Path

    sql = Path("analytics/queries.sql").read_text(encoding="utf-8")
    stmts = split_statements(sql)
    assert len(stmts) == 7
    assert all(s.upper().startswith("SELECT") for s in stmts)


def test_config_s3_host_and_ssl() -> None:
    cfg = DuckDBConfig(s3_endpoint="http://minio:9000")
    assert cfg.s3_host == "minio:9000"
    assert cfg.use_ssl is False

    secure = DuckDBConfig(s3_endpoint="https://s3.example.com")
    assert secure.s3_host == "s3.example.com"
    assert secure.use_ssl is True


def test_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_ENDPOINT", "http://example:9000")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lh")
    cfg = DuckDBConfig.from_env()
    assert cfg.s3_host == "example:9000"
    assert cfg.bucket == "lh"


def test_run_statements_roundtrip_on_local_duckdb() -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, name)")
    results = run_statements(con, "SELECT COUNT(*) FROM t; SELECT id FROM t ORDER BY id DESC;")
    assert results[0] == [(2,)]
    assert results[1] == [(2,), (1,)]
