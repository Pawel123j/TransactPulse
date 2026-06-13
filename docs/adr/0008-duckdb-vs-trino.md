# 0008. Query engine: DuckDB (over Trino) for gold analytics

- **Status:** accepted
- **Date:** 2026-06-13
- **Deciders:** Paweł Jankowicz (data engineering)

## Context and problem statement

Analysts and the Streamlit dashboard (Stage 8) need to run interactive SQL over the
gold **Delta** tables on MinIO. We must pick a query engine that reads Delta/S3,
runs well on a single-node demo, and keeps the stack light.

## Decision drivers

- **Reads Delta Lake on S3/MinIO** directly.
- **Low/zero infrastructure** for a single-machine demo.
- **Fast** interactive analytics over modest data volumes.
- **Easy to embed** in a Python dashboard.
- Familiar **SQL**.

## Considered options

- **DuckDB** — in-process analytical engine; reads Delta via the `delta` extension
  and S3/MinIO via `httpfs`. No server, embeds directly in Python; excellent
  single-node performance.
- **Trino** — distributed MPP SQL engine with a mature Delta connector; great for
  federated, multi-source, multi-user querying at scale — but needs a
  coordinator (+ workers) service and more configuration.
- **Spark SQL** — already present, but heavy to spin up for ad-hoc interactive
  queries and awkward to embed in a dashboard.

## Decision

We use **DuckDB** as the query/serving engine over the gold tables. A small runner
([`analytics/duckdb_query.py`](../../analytics/duckdb_query.py)) configures DuckDB's
`httpfs`/`delta` extensions to point at MinIO and runs the analytical statements in
[`analytics/queries.sql`](../../analytics/queries.sql). The Streamlit dashboard
queries gold through the same path.

## Consequences

### Positive

- **Zero extra services** — DuckDB is a library; nothing to run or scale.
- Reads Delta on MinIO directly; embeds trivially in the Python dashboard.
- Very fast for the demo's data sizes; trivial local setup.

### Negative / trade-offs

- **Single-node**, not a multi-user/distributed engine — would not scale out like
  Trino for large concurrent workloads.
- Delta read support relies on DuckDB's `delta` extension (younger than Trino's
  connector); advanced Delta features may lag.

### Neutral / follow-ups

- The runner is engine-pluggable; swapping in Trino later is a localized change if
  multi-user/federated querying is needed.

## Links

- [ADR 0001 — Medallion lakehouse architecture](0001-architektura-medalionowa.md)
- [DuckDB](https://duckdb.org/) · [DuckDB Delta extension](https://duckdb.org/docs/extensions/delta) · [Trino](https://trino.io/)
