#!/usr/bin/env bash
# End-to-end smoke test: generator -> Kafka topic -> CLI consumer.
#
# Prerequisites:
#   1. Infra is up:        docker compose -f infra/docker-compose.yml up -d
#   2. Generator deps:     pip install -r ingestion/requirements.txt
#
# Usage:
#   ./infra/smoke-test.sh [N]     # N = number of messages (default 20)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/.." && pwd)"
COMPOSE=(docker compose -f "${HERE}/docker-compose.yml")
N="${1:-20}"
TOPIC="transactions.raw"

echo ">> [1/2] Producing ${N} transactions to '${TOPIC}' via the generator ..."
(
  cd "${REPO_ROOT}"
  python3 -m ingestion \
    --rate 100 --max-records "${N}" --seed 7 \
    --bootstrap-servers localhost:9092 \
    --topic "${TOPIC}"
)

echo ">> [2/2] Consuming up to ${N} messages from '${TOPIC}' ..."
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "${TOPIC}" \
  --from-beginning \
  --max-messages "${N}" \
  --timeout-ms 20000

echo ">> Smoke test OK ✅"
