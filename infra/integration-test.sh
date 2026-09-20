#!/usr/bin/env bash
# Full-stack integration smoke test (requires Docker).
#
# Brings up the whole stack, lets the generator feed Kafka, runs the medallion
# pipeline (silver → gold → scoring → drift), and verifies the gold layer is
# queryable via DuckDB. Intended for a machine with a running Docker daemon
# (e.g. locally or a docker-enabled CI runner) — not the unit-test suite.
#
# Usage:  ./infra/integration-test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/docker-compose.yml")

echo ">> [1/5] Building and starting the full stack ..."
"${COMPOSE[@]}" up -d --build

echo ">> [2/5] Waiting for Kafka, MinIO and the bronze stream to settle ..."
sleep 60   # generator → bronze needs a few micro-batches

echo ">> [3/5] Triggering the medallion_pipeline DAG ..."
"${COMPOSE[@]}" exec -T airflow-scheduler airflow dags unpause medallion_pipeline || true
"${COMPOSE[@]}" exec -T airflow-scheduler airflow dags trigger medallion_pipeline

echo ">> [4/5] Waiting for the pipeline to populate gold ..."
sleep 120

echo ">> [5/5] Querying the gold layer via DuckDB ..."
"${COMPOSE[@]}" run --rm --no-deps \
  -e S3_ENDPOINT=http://minio:9000 \
  -e S3_ACCESS_KEY=minioadmin -e S3_SECRET_KEY=minioadmin123 \
  -e LAKEHOUSE_BUCKET=lakehouse \
  --entrypoint python3 dashboard \
  -c "from analytics.duckdb_query import connect, register_gold_views, DuckDBConfig; \
cfg=DuckDBConfig.from_env(); con=connect(cfg); register_gold_views(con, cfg); \
n=con.execute(\"SELECT COUNT(*) FROM daily_volume_by_country\").fetchone()[0]; \
print('daily_volume_by_country rows:', n); assert n > 0, 'gold is empty'"

echo ">> Integration smoke test OK ✅"
