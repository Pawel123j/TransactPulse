# 0001. Medallion lakehouse architecture over a classic data warehouse

- **Status:** accepted
- **Date:** 2026-06-13
- **Deciders:** Paweł Jankowicz (data engineering)

## Context and problem statement

TransactPulse ingests a continuous stream of financial transaction events and
must serve both **low-latency operational analytics** (fraud signals, live KPIs)
and **historical, ad-hoc analytical queries**. We need a storage and modeling
approach that handles streaming and batch on the same data, tolerates schema
evolution, keeps raw data for replay/audit, and runs entirely on a single
machine for a portfolio demo.

How should we structure storage and data refinement: a classic relational **data
warehouse**, or a **lakehouse** organized with the **medallion** (bronze →
silver → gold) pattern?

## Decision drivers

- Unified **streaming + batch** over one copy of the data.
- **Replayability & auditability** — keep raw events untouched (regulatory mindset
  for financial data, even though data here is synthetic).
- **Schema evolution** — transaction payloads change over time.
- **Open formats**, no vendor lock-in; runnable locally via Docker.
- **ACID** guarantees and idempotent, re-runnable transforms.
- Clear separation of concerns / data quality tiers that read well in a portfolio.

## Considered options

- **Option A — Classic data warehouse** (e.g. PostgreSQL / a columnar DW): load
  cleaned, modeled tables via ETL. Strong SQL and BI ergonomics.
- **Option B — Plain data lake** (raw files on object storage, no table format):
  cheap and flexible, but no ACID, no reliable upserts, weak schema management.
- **Option C — Lakehouse with medallion layers** (Delta Lake on object storage,
  bronze/silver/gold): table semantics + open files; one platform for streaming
  and batch.

## Decision

We chose **Option C — a lakehouse with the medallion architecture** (Delta Lake
tables on MinIO/S3), with three tiers:

- **Bronze** — raw events as-is from Kafka plus ingestion metadata. The immutable
  landing zone; enables reprocessing of downstream layers at any time.
- **Silver** — cleansed and conformed: deduplicated by `transaction_id`, typed,
  currency-normalized, ISO-standardized, watermarked, with bad records routed to
  a quarantine table.
- **Gold** — business-ready aggregates and ML-scored tables consumed by the query
  engine and dashboard.

The medallion split maps cleanly onto the project's stages and makes data-quality
boundaries explicit.

## Consequences

### Positive

- One open table format (Delta) serves **both** streaming ingestion and batch
  refinement — no separate warehouse to sync.
- Bronze immutability gives **full replay/audit** and lets us re-derive silver/gold
  after logic changes — strongly supports idempotency.
- **ACID + schema evolution + time travel** out of the box.
- Each tier is independently testable and has a clear quality contract.
- No proprietary engine; everything runs in Docker on a laptop.

### Negative / trade-offs

- More moving parts than a single warehouse (Spark, Delta, object store).
- Data is physically materialized three times → extra storage and compute vs. a
  single modeled DW (acceptable for a demo; mitigated by partitioning/OPTIMIZE).
- Requires discipline so layer responsibilities don't blur (e.g. no business logic
  in bronze).

### Neutral / follow-ups

- Query ergonomics on gold are delegated to DuckDB/Trino — see
  [ADR 0008](0008-duckdb-vs-trino.md) (added in Stage 7).
- Concrete engine choices (Kafka, Spark, Delta, MinIO) are justified in
  [ADR 0002](0002-wybor-stacku.md).

## Links

- [ADR 0002 — Technology stack selection](0002-wybor-stacku.md)
- [Databricks — Medallion architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake](https://delta.io/)
