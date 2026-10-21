#!/usr/bin/env bash
# Local transformation validation. Does not start Kafka, MinIO or Airflow.
set -euo pipefail
cd "$(dirname "$0")/.."
export TP_REQUIRE_SPARK=1
export SPARK_LOCAL_IP=127.0.0.1
python -m pytest -q -rs \
  tests/test_integration_smoke.py \
  tests/test_bronze_transform.py \
  tests/test_silver_transform.py \
  tests/test_gold_aggregates.py
