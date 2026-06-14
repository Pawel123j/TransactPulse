# 0004. Checkpointing & exactly-once for the bronze ingest

- **Status:** accepted
- **Date:** 2026-06-13
- **Deciders:** Paweł Jankowicz (data engineering)

## Context and problem statement

The bronze job (Stage 4) continuously reads `transactions.raw` from Kafka with
Spark Structured Streaming and appends to a Delta table on MinIO. Streaming jobs
restart — on deploys, failures, or scaling — so we must guarantee that, across
restarts, **every Kafka record lands in bronze exactly once**: no gaps (lost
events) and no duplicates (double-counted volume/fraud).

How do we achieve reliable, replayable, exactly-once ingestion from Kafka into a
Delta table?

## Decision drivers

- **No duplicates, no loss** across job restarts and failures.
- **Replayability** — ability to reprocess from a known offset.
- **Idempotency** — re-running the job must not corrupt or duplicate data.
- **Operational simplicity** on a single-node demo (no external state store).
- Works over the **S3A** path to MinIO.

## Considered options

- **Option A — At-least-once + downstream dedup**: simplest sink semantics, accept
  duplicates in bronze and remove them in silver by `transaction_id`. Bronze
  counts become unreliable; pushes correctness downstream.
- **Option B — Structured Streaming checkpoint + Delta sink (exactly-once)**: the
  checkpoint persists Kafka offsets and a write-ahead/commit log; the Delta sink
  commits each micro-batch transactionally and idempotently (a re-attempted batch
  with the same batch id is not re-committed). Together this yields end-to-end
  exactly-once.
- **Option C — Manual offset management** (store offsets ourselves, custom commit):
  maximum control, maximum complexity and bug surface; reinvents the framework.

## Decision

We use **Option B**: Spark Structured Streaming **checkpointing** combined with the
**Delta Lake sink** for end-to-end exactly-once ingestion.

- **Checkpoint location**: `s3a://lakehouse/_checkpoints/bronze_transactions`
  (durable, survives container restarts) — stores source offsets and the commit log.
- **Delta sink** commits each micro-batch atomically; replayed batches are
  deduplicated by batch id, so a crash between Kafka read and Delta commit is
  recovered without duplication.
- **`startingOffsets=earliest`** on a fresh checkpoint so no historical data is
  skipped; subsequent runs resume from the checkpoint.
- **`failOnDataLoss=false`** for demo resilience (e.g. topic retention/recreation
  during experimentation) — a deliberate trade-off, see below.
- **Partitioning by `ingestion_date`** keeps bronze append-only and avoids parsing
  business fields to lay out files (true "as-is" bronze).

## Consequences

### Positive

- **Exactly-once** end to end: the checkpoint + Delta transaction log make restarts
  safe and the job idempotent.
- **Replay** is possible by pointing at a new checkpoint (reprocess from earliest)
  without touching producer or downstream code.
- Bronze row counts are trustworthy (no duplicates to reconcile).

### Negative / trade-offs

- The **checkpoint is coupled to the sink path**: deleting/moving the bronze table
  requires clearing the matching checkpoint, or the stream refuses to start.
- **`failOnDataLoss=false`** can mask genuine data loss if Kafka retention expires
  before consumption; acceptable for a local demo, would be revisited for prod.
- Structured Streaming checkpoints on object storage (S3A/MinIO) rely on the
  store's consistency; fine for MinIO, but a dedicated/consistent location is
  preferred at scale.

### Neutral / follow-ups

- Silver (Stage 5) still deduplicates by `transaction_id` to defend against
  **producer-side** duplicates (the generator/app, not the ingest), which bronze
  exactly-once does not cover.
- Trigger mode (`processingTime` vs `availableNow`/`once`) is configurable so the
  same job serves both continuous streaming and Airflow-triggered batch runs.

## Links

- [ADR 0001 — Medallion lakehouse architecture](0001-architektura-medalionowa.md)
- [ADR 0002 — Technology stack selection](0002-wybor-stacku.md)
- [Structured Streaming — Fault Tolerance Semantics](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics)
- [Delta Lake — streaming](https://docs.delta.io/latest/delta-streaming.html)
