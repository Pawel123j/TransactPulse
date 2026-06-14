# 0002. Technology stack: Kafka + Spark + Delta + MinIO

- **Status:** accepted
- **Date:** 2026-06-13
- **Deciders:** Paweł Jankowicz (data engineering)

## Context and problem statement

Having decided on a medallion lakehouse ([ADR 0001](0001-architektura-medalionowa.md)),
we must pick the concrete engines for the four core capabilities: an **event
backbone**, a **stream/batch processing engine**, a **table storage format**, and
an **object store**. Constraints: the whole stack must run locally via
`docker compose`, use open-source / open formats, and reflect tools a working data
engineer is expected to know.

## Decision drivers

- **Industry relevance** — technologies recruiters and teams actually use.
- **Single-machine, one-command** local run (Docker Compose).
- **Open source / open formats**, no cloud account required.
- **Streaming + batch** on the same processing engine.
- **S3 compatibility** so the lakehouse code is cloud-portable unchanged.
- Healthy ecosystem, documentation, and Python-first ergonomics.

## Considered options

### Event backbone
- **Apache Kafka (KRaft)** — de-facto standard event log; KRaft removes the
  ZooKeeper dependency, simplifying the local stack.
- Redpanda — Kafka-API compatible, lighter; less "canonical" for a learning portfolio.
- RabbitMQ — message broker, not a replayable log; weaker fit for streaming ingestion.

### Processing engine
- **PySpark Structured Streaming** — unified streaming + batch, mature Delta
  integration, exactly-once with checkpointing, ubiquitous in industry.
- Apache Flink — excellent streaming, steeper local setup, smaller Python ecosystem here.
- Kafka Streams / Faust — JVM/Python stream libs without Spark's batch + Delta synergy.

### Table format
- **Delta Lake** — ACID, time travel, schema evolution, first-class Spark support.
- Apache Iceberg / Hudi — strong alternatives; Delta has the smoothest local Spark DX.

### Object store
- **MinIO** — S3-compatible, single container, web console; lakehouse paths stay
  `s3a://...` and port to real S3 unchanged.
- Local filesystem — simplest, but doesn't exercise the S3 access path or prove
  cloud portability.

## Decision

We adopt **Kafka (KRaft) + PySpark Structured Streaming + Delta Lake + MinIO**:

- **Kafka in KRaft mode** as the durable, replayable event backbone
  (`transactions.raw`), no ZooKeeper.
- **PySpark Structured Streaming** as the single engine for streaming ingestion
  (bronze) and batch refinement (silver/gold), with checkpoint-based exactly-once.
- **Delta Lake** as the table format for all medallion tiers.
- **MinIO** as the S3-compatible object store backing `s3a://lakehouse/...`.

## Consequences

### Positive

- One coherent, widely-used stack covering ingestion → processing → storage.
- KRaft and MinIO each collapse to a single container → simpler `docker compose`.
- S3-compatible paths make the lakehouse **cloud-portable** without code changes.
- Spark unifies streaming and batch, reducing cognitive and operational overhead.
- Strong portfolio signal: these are mainstream, in-demand technologies.

### Negative / trade-offs

- **Spark is heavy** — JVM memory footprint and slower cold start than Flink/Redpanda
  alternatives; noticeable on a laptop.
- Spark ⇄ MinIO requires explicit `s3a` Hadoop/AWS dependency and endpoint/credential
  configuration (env-driven) — an extra setup step.
- Running Kafka + Spark + MinIO together is resource-intensive; we mitigate with
  modest partition counts and tuned Spark memory in the demo.

### Neutral / follow-ups

- Serialization format on the wire (JSON vs Avro) is decided separately in
  [ADR 0003](0003-format-serializacji.md) (Stage 2).
- Checkpoint / exactly-once strategy is detailed in
  [ADR 0004](0004-strategia-checkpoint-i-exactly-once.md) (Stage 4).

## Links

- [ADR 0001 — Medallion lakehouse architecture](0001-architektura-medalionowa.md)
- [Apache Kafka — KRaft](https://kafka.apache.org/documentation/#kraft)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake](https://delta.io/) · [MinIO](https://min.io/)
