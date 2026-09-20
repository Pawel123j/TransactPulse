# 0006. Batch fraud scoring as a Spark pandas UDF, model from a pickle artifact

- **Status:** accepted
- **Date:** 2026-06-13
- **Deciders:** Paweł Jankowicz (data engineering)

## Context and problem statement

The gold layer (Stage 6) must attach a `fraud_score` (and a boolean flag) to
transactions for analytics and the dashboard. A fraud model already exists as an
artifact; the data platform's job is **integration**, not model research: load the
model and apply it to silver data at scale, reproducibly, on the existing Spark
runtime. We must decide **how the model is packaged/loaded** and **how inference is
executed in Spark**.

## Decision drivers

- **Scale & fit with the stack** — inference should run on Spark over Delta, no new
  serving service.
- **Throughput** — vectorized inference, not row-at-a-time Python.
- **Reproducibility & idempotency** — the same input yields the same scores; re-runs
  are safe.
- **Simplicity of the local stack** — avoid standing up extra infrastructure for a
  demo.
- **Resilience** — a missing/incompatible artifact must not break the whole gold
  pipeline.

## Considered options

### Inference execution
- **E1 — Row-at-a-time Python UDF**: simplest, but slow (per-row serialization).
- **E2 — Spark `pandas_udf` (vectorized)**: batches rows into pandas Series via
  Arrow; the model scores a whole batch at once. Fast, native to Spark.
- **E3 — External model server (REST)**: decouples model, but adds a service,
  network hop, and failure mode — overkill here.

### Model packaging
- **P1 — Pickle artifact** (`models/fraud_model.pkl` + JSON metadata): zero extra
  infra, trivial to load on executors.
- **P2 — MLflow Model Registry**: proper lineage/versioning, but requires an MLflow
  tracking server/registry in the stack.

## Decision

- **Inference: Spark `pandas_udf` (E2).** A vectorized pandas UDF loads the model
  **once per executor** (cached at module level) and scores batches, producing
  `fraud_score ∈ [0, 1]`; `fraud_flag = fraud_score ≥ threshold`. Output is written
  to `gold/scored_transactions`.
- **Packaging: pickle artifact (P1).** The model and its metadata (feature list,
  training reference distribution for drift) are loaded from `models/`. MLflow is
  documented as the production upgrade path (P2) but intentionally not run locally.
- **Resilience:** if the artifact is missing or fails to load, scoring falls back
  to a deterministic, dependency-free **heuristic model** implementing the same
  interface, so gold always produces a `scored_transactions` table. The integration
  point (`FraudModel`) is artifact-agnostic.
- **Drift:** PSI and KS are computed (pure Python) on the live `fraud_score` and
  `amount_pln` versus the training reference distribution, logged and written to a
  report under `docs/drift/`.

## Consequences

### Positive

- Inference scales on the existing Spark cluster with Arrow-vectorized batches; no
  new service.
- Model loaded once per executor → low overhead.
- Pickle keeps the local stack minimal; the `FraudModel` seam makes swapping in
  MLflow or XGBoost a localized change.
- Heuristic fallback makes the pipeline robust to a missing model.
- Deterministic scoring keeps gold idempotent.

### Negative / trade-offs

- Pickle has **no built-in versioning/lineage** (mitigated by metadata JSON; MLflow
  is the upgrade path).
- Pickle is **Python/version sensitive**; the artifact must be regenerated if the
  training environment changes (a `train_model.py` script makes this one command).
- `pandas_udf` requires Arrow and a pandas-capable Python on executors.

### Neutral / follow-ups

- The Airflow DAG (Stage 7) triggers training/scoring/drift as tasks.
- Drift thresholds (PSI > 0.2 "moderate", > 0.25 "significant") are configurable.

## Links

- [ADR 0002 — Technology stack selection](0002-wybor-stacku.md)
- [ADR 0005 — Data quality & quarantine](0005-data-quality-i-kwarantanna.md)
- [Spark pandas UDFs](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [Population Stability Index / KS test](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test)
