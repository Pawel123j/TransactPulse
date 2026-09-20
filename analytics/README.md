# Analytics — DuckDB query layer

Interactive SQL over the gold Delta tables on MinIO, powered by **DuckDB**
(ADR [0008](../docs/adr/0008-duckdb-vs-trino.md)). DuckDB reads Delta via its
`delta` extension and MinIO via `httpfs` — no server to run.

## Files

- [`queries.sql`](queries.sql) — example analytical queries (top countries, fraud
  rate by category, account velocity, ML lift, …).
- [`duckdb_query.py`](duckdb_query.py) — registers each gold table as a view and
  executes the queries.

## Run

```bash
pip install duckdb pandas
# point at MinIO (defaults shown)
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin123
export LAKEHOUSE_BUCKET=lakehouse

python analytics/duckdb_query.py
```

The runner:

1. opens DuckDB and loads `httpfs` + `delta`, pointed at MinIO;
2. registers `daily_volume_by_country`, `merchant_category_kpi`,
   `account_velocity`, `fraud_signals`, `scored_transactions` as views over
   `s3://lakehouse/gold/<table>`;
3. executes each statement in `queries.sql` and prints the result.

## Tests

`tests/test_duckdb_runner.py` covers statement splitting and endpoint parsing
(pure) and a DuckDB round-trip on local data (skipped if `duckdb` is absent).
