# 0007. Orchestration with Apache Airflow (over Prefect)

- **Status:** accepted
- **Date:** 2026-06-13
- **Deciders:** Paweł Jankowicz (data engineering)

## Context and problem statement

The medallion batch path — silver cleansing, a data-quality gate, gold aggregates,
ML scoring and drift — must run on a schedule, in order, with retries and failure
alerting, and be safely re-runnable. We need a workflow orchestrator that fits a
local `docker compose` stack and reflects tooling a data engineer is expected to
know.

## Decision drivers

- **Industry ubiquity** — the de-facto orchestrator most teams/recruiters expect.
- **Scheduling, retries, alerting, backfills** out of the box.
- **DAG visibility** — a UI showing task graph and run history (portfolio value).
- **Runs locally** in Docker without a managed cloud account.
- **Idempotent, restartable** task model.

## Considered options

- **Apache Airflow** — the industry-standard, mature scheduler; rich UI, operators
  (incl. `DockerOperator`), retries/alerting; heavier footprint.
- **Prefect** — modern, Pythonic, lighter; great DX, but less ubiquitous in job
  descriptions and smaller operator ecosystem.
- **Dagster** — asset-oriented, excellent lineage; newer, different mental model.
- **Cron + scripts** — trivial, but no UI, retries, dependencies, or observability.

## Decision

We use **Apache Airflow** (LocalExecutor on Postgres) in docker-compose. The
`medallion_pipeline` DAG orchestrates the batch path; each Spark step runs as a
**`DockerOperator`** launching the `transactpulse/streaming` image (so Airflow
itself stays light and the jobs reuse the existing Spark image), while the
data-quality **gate** runs as a lightweight container that fails the DAG if silver
quality checks did not pass. Tasks are idempotent (Delta MERGE / overwrite) and
restartable; retries and an on-failure callback provide resilience and alerting.

## Consequences

### Positive

- Strongest portfolio signal; matches real-world stacks.
- Built-in retries, scheduling, backfill, and a task-graph UI.
- `DockerOperator` cleanly separates orchestration from compute (jobs run in the
  Spark image, Airflow only schedules).
- The DQ gate makes "promotion to gold" conditional on quality.

### Negative / trade-offs

- Airflow is **heavyweight** (webserver + scheduler + metadata DB) versus Prefect.
- `DockerOperator` needs the Docker socket mounted into Airflow (a privilege to be
  aware of; acceptable for a local demo).
- More moving parts in the compose stack.

### Neutral / follow-ups

- A separate `train_fraud_model` DAG handles (re)training on a slower cadence.
- Alerting is a logging callback here; wiring Slack/email is a small follow-up.

## Links

- [ADR 0005 — Data quality & quarantine](0005-data-quality-i-kwarantanna.md)
- [ADR 0006 — Batch ML scoring](0006-batch-scoring-ml-jako-udf.md)
- [Apache Airflow](https://airflow.apache.org/) · [DockerOperator](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/operators.html)
