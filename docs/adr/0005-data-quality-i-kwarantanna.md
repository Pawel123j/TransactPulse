# 0005. Data quality gates and a quarantine table for silver

- **Status:** accepted
- **Date:** 2026-06-13
- **Deciders:** Paweł Jankowicz (data engineering)

## Context and problem statement

The silver layer (Stage 5) refines bronze into clean, conformed, deduplicated
transactions that downstream gold aggregates and ML scoring depend on. Bronze is
intentionally raw, so some records will be malformed (bad amounts, unknown
currencies, missing keys, unparseable timestamps). We need to decide **how data
quality is enforced**, **what happens to bad records**, and **which tool reports
quality**, without letting one poison batch fail the whole pipeline or silently
corrupt analytics.

## Decision drivers

- **Trustworthy silver** — bad records must never reach gold.
- **No data loss / auditability** — rejected records must be inspectable, not
  dropped.
- **Pipeline resilience** — a few bad rows must not crash the job.
- **Observability** — a human-readable quality report for each run.
- **Idempotency** — re-running silver must converge to the same state.
- Keep the local stack light; avoid fragile, version-churny tooling on the
  critical path.

## Considered options

### Enforcement
- **A1 — Native Spark gate**: encode rules as Spark boolean expressions, split the
  batch into valid/invalid. Fast, no extra runtime, full control.
- **A2 — Great Expectations as a blocking gate**: let GE validation pass/fail the
  job. Rich semantics, but GE on the hot path couples pipeline success to a
  fast-moving library and is heavier.

### Bad records
- **B1 — Drop**: simplest, but loses data and hides problems.
- **B2 — Quarantine table** (`silver/quarantine`) with per-record failure reasons.

### Quality reporting tool
- **C1 — Great Expectations** (declarative suite + report).
- **C2 — Soda** (SodaCL checks).
- **C3 — Hand-rolled metrics only.**

## Decision

- **Enforcement: native Spark gate (A1).** Each record is evaluated against a set
  of boolean rules (non-null keys, `0 < amount ≤ max`, allowed currency / country /
  channel / category, parseable `event_time`). A `dq_errors` array captures every
  failing rule per row.
- **Bad records: quarantine (B2).** Rows with a non-empty `dq_errors` are written
  to `s3a://lakehouse/silver/quarantine` with their reasons and a
  `quarantine_time`; only clean rows go to `silver/transactions`.
- **Reporting: Great Expectations (C1)** runs *off the critical path* — after the
  gate — producing a quality report saved under `docs/data_quality/`. The
  expectation suite mirrors the native rules, so GE documents and observes the same
  contract the gate enforces. GE is imported lazily; if unavailable, a native
  Markdown report is still written (the pipeline never hard-fails on reporting).
- **Dedup & idempotency:** within a batch, duplicates are removed by
  `transaction_id` (keep latest by `ingestion_time`); across runs, silver is an
  idempotent **Delta MERGE** upsert keyed on `transaction_id`, which also absorbs
  late-arriving records within a bounded watermark horizon.
- **Layout:** silver is partitioned by `event_date` and periodically
  **OPTIMIZE … ZORDER BY (account_id)** for read locality on account-centric queries.

This split — native enforcement, GE for observability — is how mature pipelines
typically run: the gate is reliable and fast; GE adds documentation and trend
visibility without owning pipeline success.

## Consequences

### Positive

- Gold only ever sees validated data; analytics stay trustworthy.
- Quarantine preserves every rejected record with explainable reasons (auditable).
- A bad batch degrades gracefully (rows quarantined) instead of crashing.
- MERGE-based upsert makes silver idempotent and late-data tolerant.
- ZORDER improves account-centric query/scan performance.

### Negative / trade-offs

- Rules live in two representations (Spark expressions for the gate, GE
  expectations for the report); they must be kept in sync (covered by tests).
- GE off the critical path means a failing expectation **reports** but does not by
  itself block promotion — enforcement is the native gate's job by design.
- FX normalization uses a **static synthetic rates table** (no live FX feed);
  acceptable for a synthetic-data portfolio.

### Neutral / follow-ups

- The quarantine table can feed a future "data quality" panel in the dashboard
  (Stage 8) and alerting in Airflow (Stage 7).
- If GE is later promoted to a blocking gate, this ADR would be revisited.

## Links

- [ADR 0001 — Medallion lakehouse architecture](0001-architektura-medalionowa.md)
- [ADR 0004 — Checkpointing & exactly-once](0004-strategia-checkpoint-i-exactly-once.md)
- [Great Expectations](https://greatexpectations.io/) · [Delta MERGE](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
