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


@pytest.mark.parametrize(
    "endpoint",
    [
        "ftp://host",
        "https://user:password@host",
        "https://host/path",
        "http://host?x=1",
    ],
)
def test_invalid_storage_origins_are_rejected(endpoint) -> None:
    with pytest.raises(ValueError, match="S3_ENDPOINT"):
        DuckDBConfig(s3_endpoint=endpoint)


def test_arrow_reads_delta_snapshot_and_refreshes_without_resurrecting_removed_files(
    tmp_path, monkeypatch
) -> None:
    import deltalake
    import pyarrow as pa

    from analytics.duckdb_query import GOLD_TABLES, connect, register_gold_views

    actual_delta_table = deltalake.DeltaTable
    for table in GOLD_TABLES:
        deltalake.write_deltalake(tmp_path / table, pa.table({"count": [3]}))

    def local_snapshot(uri, storage_options):
        assert storage_options["AWS_ALLOW_HTTP"] == "false"
        return actual_delta_table(tmp_path / uri.rsplit("/", 1)[1])

    monkeypatch.setattr(deltalake, "DeltaTable", local_snapshot)
    cfg = DuckDBConfig(s3_endpoint="https://storage.example.test")
    con = connect(cfg)
    try:
        register_gold_views(con, cfg)
        assert con.execute("SELECT SUM(count) FROM scored_transactions").fetchone() == (3,)
        deltalake.write_deltalake(
            tmp_path / "scored_transactions", pa.table({"count": [5]}), mode="overwrite"
        )
        # A snapshot is intentionally stable until explicitly refreshed.
        assert con.execute("SELECT SUM(count) FROM scored_transactions").fetchone() == (3,)
        register_gold_views(con, cfg)
        assert con.execute("SELECT SUM(count) FROM scored_transactions").fetchone() == (5,)
    finally:
        con.close()
