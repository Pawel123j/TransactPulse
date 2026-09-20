#!/usr/bin/env bash
# Create (idempotently) the Kafka topics used by TransactPulse.
#
# Usage:
#   ./infra/create-topics.sh                 # creates transactions.raw (3 partitions)
#   TOPIC_PARTITIONS=6 ./infra/create-topics.sh
#   ./infra/create-topics.sh my.topic 4      # custom topic + partition count
#
# Requires the infra stack to be running (docker compose ... up -d).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "${HERE}/docker-compose.yml")

TOPIC="${1:-transactions.raw}"
PARTITIONS="${2:-${TOPIC_PARTITIONS:-3}}"

echo ">> Creating topic '${TOPIC}' (${PARTITIONS} partitions, replication 1) ..."
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor 1

echo ">> Current topics:"
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
