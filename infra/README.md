# Infrastructure — Kafka (KRaft) + MinIO

Base services for TransactPulse, defined in [`docker-compose.yml`](docker-compose.yml):

| Service         | Image                          | Purpose                                  | Host endpoint                |
| --------------- | ------------------------------ | ---------------------------------------- | ---------------------------- |
| `kafka`         | `apache/kafka:3.8.1`           | Event backbone (KRaft, no ZooKeeper)     | `localhost:9092` (bootstrap) |
| `kafka-init`    | `apache/kafka:3.8.1`           | One-shot: create `transactions.raw`      | —                            |
| `kafka-ui`      | `provectuslabs/kafka-ui:0.7.2` | Browse topics/messages                   | http://localhost:8080        |
| `minio`         | `minio/minio`                  | S3-compatible object store (lakehouse)   | http://localhost:9000 (API)  |
| `createbuckets` | `minio/mc`                     | One-shot: create the `lakehouse` bucket  | —                            |

MinIO console: http://localhost:9001 (default login `minioadmin` / `minioadmin123`).

## Quickstart

```bash
# 1. (optional) customise credentials / partitions
cp infra/.env.example infra/.env

# 2. bring up the stack
docker compose -f infra/docker-compose.yml up -d

# 3. watch services become healthy
docker compose -f infra/docker-compose.yml ps
```

The `kafka-init` and `createbuckets` containers run once, create the topic and
bucket, then exit `0` — that is expected. Topic creation is also available
on demand and is idempotent:

```bash
./infra/create-topics.sh           # transactions.raw, 3 partitions
```

## Verify

**Kafka UI** — open http://localhost:8080 → cluster `transactpulse` → you should
see the `transactions.raw` topic with 3 partitions.

**MinIO console** — open http://localhost:9001, log in, and confirm the
`lakehouse` bucket exists.

**End-to-end smoke test** (generator → topic → CLI consumer):

```bash
pip install -r ingestion/requirements.txt
./infra/smoke-test.sh 20
```

Expected: the generator produces 20 records to `transactions.raw`, then the
console consumer prints 20 JSON messages and the script reports `Smoke test OK`.

## Networking model

The broker exposes two PLAINTEXT listeners so the same topic is reachable from
both inside and outside the Docker network:

| From                        | Bootstrap server  | Listener   |
| --------------------------- | ----------------- | ---------- |
| Host (generator, scripts)   | `localhost:9092`  | `EXTERNAL` |
| Other containers (kafka-ui) | `kafka:9092`      | `PLAINTEXT`|

Internally the host listener lives on container port `29092` and is published as
`9092`, and is advertised back as `localhost:9092` so host clients reconnect
correctly. KRaft uses a third `CONTROLLER` listener on `9093`.

## Persistence & teardown

State is kept in named volumes (`tp-kafka-data`, `tp-minio-data`).

```bash
docker compose -f infra/docker-compose.yml down       # stop, keep data
docker compose -f infra/docker-compose.yml down -v     # stop, wipe data
```
