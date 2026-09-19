# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — 2026-09-19

First stable release: a complete, runnable real-time lakehouse for synthetic
financial transactions, built in eight checkpointed stages.

### Added

- **Ingestion** — synthetic transaction generator (Faker + log-normal amounts,
  hourly seasonality, ~0.3 % fraud, late-arriving events) with a Kafka producer.
- **Infrastructure** — Kafka (KRaft, no ZooKeeper), MinIO, Kafka UI and topic
  bootstrap as Docker Compose services; a full-stack `docker-compose.yml` that
  `include`s the infra and orchestration slices.
- **Bronze** — Spark Structured Streaming from Kafka to Delta on MinIO, raw and
  append-only, with checkpoint-based exactly-once semantics.
- **Silver** — cleansing, deduplication by `transaction_id`, currency
  normalization to PLN, ISO conformance, watermarking, a blocking data-quality
  gate with a quarantine table, and `OPTIMIZE … ZORDER`.
- **Gold + ML** — analytical aggregates (`daily_volume_by_country`,
  `merchant_category_kpi`, `account_velocity`, `fraud_signals`,
  `scored_transactions`), fraud scoring as a vectorized Spark `pandas_udf` with a
  deterministic heuristic fallback, and PSI/KS drift reporting.
- **Orchestration** — Airflow `medallion_pipeline` DAG (silver → DQ gate → gold →
  scoring → drift) and a `@weekly` `train_fraud_model` DAG.
- **Query & serving** — DuckDB runner over the gold Delta tables and a Streamlit
  dashboard with KPI, fraud, data-quality and drift panels.
- **Docs** — README, `docs/ARCHITECTURE.md` and eight MADR-format ADRs.
- **Quality** — 121 automated tests (109 fast + 12 Spark), ruff lint/format, and
  a GitHub Actions pipeline.
- **Security** — `SECURITY.md` documenting the development-stack trade-offs;
  bandit (SAST) and gitleaks (secret scanning, full history) as blocking CI jobs.

### Changed (during release preparation)

- CI now really executes the Spark transformation tests in a dedicated
  `spark-tests` job (Java 17 + PySpark). `TP_REQUIRE_SPARK=1` turns a missing
  PySpark into a failure, so those tests can no longer skip silently.
- `analytics/duckdb_query.py` validates view names and the bucket against strict
  allowlists and escapes literals before interpolating them into DuckDB SQL.
- README corrected to match the implementation: status is stable/v1.0.0, DuckDB
  is the query engine (Trino moved to the v2.0 roadmap), scikit-learn is the ML
  library (no XGBoost), and Great Expectations is documented as an optional
  reporting dependency over a native Spark gate.

### Fixed

- `test_bronze_tolerates_malformed_json` relied on type inference from an
  all-null column (which fails on Spark 3.5) and asserted that a malformed
  payload parses to a `NULL` struct; `from_json` in PERMISSIVE mode returns a
  struct with all fields null. The test now declares the Kafka source schema
  explicitly and asserts the real behaviour.

### Known limitations

- The **live end-to-end run** (`docker compose up -d --build` →
  `infra/integration-test.sh`) has not been executed in CI or during release
  preparation — no Docker daemon was available. CI validates all three compose
  files and builds both images; see the README for the manual procedure.
- Spark and Airflow jobs are covered by unit tests of their transformation
  logic, not by an executed pipeline run.
- **Dependency/CVE scanning is not wired up.** Trivy was evaluated but left out
  of v1.0.0: it would have required an unpinned third-party action or an
  unverified image tag inside the supply-chain gate itself. Planned for v1.1
  together with SBOM generation and Dependabot.

[1.0.0]: https://github.com/Pawel123j/TransactPulse/releases/tag/v1.0.0
