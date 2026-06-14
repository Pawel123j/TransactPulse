"""DuckDB query runner over the gold Delta tables on MinIO (ADR 0008).

Registers each gold table as a DuckDB view (via the ``delta`` + ``httpfs``
extensions pointed at MinIO) and executes the statements in ``analytics/queries.sql``.
The same connection path backs the Streamlit dashboard (Stage 8).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

#: Gold Delta tables registered as views (view name == table name).
GOLD_TABLES: tuple[str, ...] = (
    "daily_volume_by_country",
    "merchant_category_kpi",
    "account_velocity",
    "fraud_signals",
    "scored_transactions",
)


@dataclass(slots=True)
class DuckDBConfig:
    """Connection settings for reading the lakehouse from DuckDB."""

    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin123"
    bucket: str = "lakehouse"

    @classmethod
    def from_env(cls) -> DuckDBConfig:
        d = cls()
        return cls(
            s3_endpoint=os.getenv("S3_ENDPOINT", d.s3_endpoint),
            s3_access_key=os.getenv("S3_ACCESS_KEY", d.s3_access_key),
            s3_secret_key=os.getenv("S3_SECRET_KEY", d.s3_secret_key),
            bucket=os.getenv("LAKEHOUSE_BUCKET", d.bucket),
        )

    @property
    def s3_host(self) -> str:
        """Endpoint host:port without the URL scheme (what DuckDB expects)."""
        return re.sub(r"^https?://", "", self.s3_endpoint)

    @property
    def use_ssl(self) -> bool:
        return self.s3_endpoint.startswith("https://")


def split_statements(sql: str) -> list[str]:
    """Split a SQL script into individual statements, stripping ``--`` comments.

    Both full-line and inline ``--`` comments are removed. (Assumes no ``--``
    appears inside string literals, which holds for the project's queries.)
    """
    cleaned: list[str] = []
    for line in sql.splitlines():
        idx = line.find("--")
        cleaned.append(line if idx == -1 else line[:idx])
    text = "\n".join(cleaned)
    return [stmt.strip() for stmt in text.split(";") if stmt.strip()]


def connect(config: DuckDBConfig) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection configured for Delta-on-MinIO access."""
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL delta; LOAD delta;")
    con.execute(f"SET s3_endpoint='{config.s3_host}';")
    con.execute(f"SET s3_use_ssl={'true' if config.use_ssl else 'false'};")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{config.s3_access_key}';")
    con.execute(f"SET s3_secret_access_key='{config.s3_secret_key}';")
    con.execute("SET s3_region='us-east-1';")
    return con


def register_gold_views(con: duckdb.DuckDBPyConnection, config: DuckDBConfig) -> None:
    """Register each gold Delta table as a DuckDB view."""
    for table in GOLD_TABLES:
        location = f"s3://{config.bucket}/gold/{table}"
        con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM delta_scan('{location}')")


def run_statements(con: duckdb.DuckDBPyConnection, sql: str) -> list[list[tuple[Any, ...]]]:
    """Execute every statement in ``sql`` and return each statement's rows."""
    results = []
    for statement in split_statements(sql):
        results.append(con.execute(statement).fetchall())
    return results


def main() -> int:
    """Run ``analytics/queries.sql`` against the gold layer and print results."""
    config = DuckDBConfig.from_env()
    sql_path = Path(__file__).with_name("queries.sql")
    statements = split_statements(sql_path.read_text(encoding="utf-8"))

    con = connect(config)
    register_gold_views(con, config)
    for i, statement in enumerate(statements, start=1):
        print(f"\n=== query {i} ===")  # noqa: T201
        print(con.execute(statement).fetchdf().to_string(index=False))  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
