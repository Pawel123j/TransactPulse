# Streaming — Bronze layer (Kafka → Delta)

Spark Structured Streaming job that ingests `transactions.raw` from Kafka and
appends it **as-is** to a Delta table on MinIO. No business transformations happen
here (no dedup, no normalization) — that is the silver layer's job (Stage 5).

- Source: Kafka topic `transactions.raw`
- Sink: Delta `s3a://lakehouse/bronze/transactions`, partitioned by `ingestion_date`
- Checkpoint: `s3a://lakehouse/_checkpoints/bronze_transactions` (exactly-once — see
  [ADR 0004](../docs/adr/0004-strategia-checkpoint-i-exactly-once.md))

## Bronze table schema

| Column             | Type        | Notes                                            |
| ------------------ | ----------- | ------------------------------------------------ |
| `key`              | string      | Kafka message key (the `account_id`)             |
| `value`            | string      | **Raw JSON payload** (source of truth, untouched)|
| `source_topic`     | string      | Origin topic                                     |
| `_partition`       | int         | Kafka partition                                  |
| `_offset`          | long        | Kafka offset                                     |
| `_kafka_timestamp` | timestamp   | Kafka record timestamp                           |
| `data`             | struct      | Parsed payload (structural only, no cleansing)   |
| `ingestion_time`   | timestamp   | When Spark ingested the record                   |
| `ingestion_date`   | date        | **Partition column** (derived from ingestion)    |

## Configuration (env)

| Env var                              | Default            | Notes                                  |
| ------------------------------------ | ------------------ | -------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`            | `localhost:9092`   | `kafka:9092` inside Docker             |
| `KAFKA_TOPIC`                        | `transactions.raw` |                                        |
| `STREAMING_STARTING_OFFSETS`         | `earliest`         | for a fresh checkpoint                 |
| `STREAMING_TRIGGER`                  | `10 seconds`       | or `availableNow` / `once` (batch)     |
| `STREAMING_MAX_OFFSETS_PER_TRIGGER`  | _(unset)_          | optional micro-batch rate limit        |
| `S3_ENDPOINT`                        | `http://localhost:9000` | `http://minio:9000` in Docker     |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY`    | `minioadmin` / …   | MinIO credentials                      |
| `LAKEHOUSE_BUCKET`                   | `lakehouse`        |                                        |

## Run

### With Docker (recommended)

The `spark-bronze` service is wired into [`infra/docker-compose.yml`](../infra/docker-compose.yml):

```bash
docker compose -f infra/docker-compose.yml up -d --build spark-bronze
docker compose -f infra/docker-compose.yml logs -f spark-bronze
```

> First start downloads the Spark packages (Delta, kafka, hadoop-aws) into the
> `tp-spark-ivy` volume; subsequent starts are fast.

Then produce some data and watch bronze fill up:

```bash
python -m ingestion --rate 50 --duration 30      # host -> localhost:9092
```

### From the host (spark-submit)

```bash
pip install -r streaming/requirements.txt        # pyspark + delta-spark
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  streaming/bronze_job.py
```

## Verify

- **MinIO console** (http://localhost:9001) → bucket `lakehouse` →
  `bronze/transactions/ingestion_date=YYYY-MM-DD/` contains Parquet + a `_delta_log/`.
- A one-shot batch read (using `STREAMING_TRIGGER=availableNow`) is handy in CI.

## Tests

`tests/test_bronze_transform.py` exercises `build_bronze_frame` on a local
SparkSession (no Kafka/Delta/S3). It is skipped automatically where PySpark is not
installed and runs in CI (Stage 8).
