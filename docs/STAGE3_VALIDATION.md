# Stage 3 — 2026-10-20

Status: **PARTIAL**. Original scoring behavior is preserved.

## Verified now

- 186 ordinary tests passed (including real Spark transformations), two opt-in modules skipped. The skipped scopes were run separately: one real pipeline test and four real Airflow parser/serialization tests passed.
- Real Kafka 3.8.0 → Spark 3.5.9/S3A/Delta 3.2.1 → Bronze/Silver/Gold/scoring → DuckDB/Arrow → Streamlit AppTest passed. A second Kafka batch increased the dashboard count. Restarted checkpoint and repeated MERGE did not duplicate records; a refused S3A connection propagated an error.
- The dashboard no longer caches an old Delta snapshot, disguises query failures as zero activity, or crashes on Streamlit's handling of conditional dataframe expressions. Three isolated UI-runtime regressions passed; AppTest is not a browser test.
- Real backup/restore copied 60 Delta objects to an empty, distinct bucket. Restored DuckDB KPIs and scored row counts matched the source without Spark checkpoints. Six safety regressions cover corruption, changed source, existing target, filename safety and explicit writer pause.
- A separate local HTTPS storage fixture passed trusted-certificate Delta-rs writes and DuckDB reads. Echo's signed audio requests passed with both positive and negative TLS/signature checks. This does not validate Spark JVM TLS, Kafka TLS or production IAM.
- Ruff check/format passed. Installed TransactPulse and Airflow Python environments have zero known vulnerabilities in the new audits. Airflow's PyJWT/urllib3 advisories were fixed rather than ignored.

Evidence in the engineering workspace: `project-engineering/stage3/logs/transactpulse-full-regression.log`, `transactpulse-live-dashboard-fixed-2.log`, `transactpulse-airflow-security-validated.log`, `storage-tls-contract-2.log`, `transactpulse-quality-final.log`. Do not treat Stage 2 logs as new test results.

## Reading Gold

Install `dashboard/requirements.txt`. The default `DUCKDB_DELTA_BACKEND=arrow` opens actual committed Delta snapshots via delta-rs/Arrow and registers lazy datasets in DuckDB. It handles removed files rather than scanning every Parquet object. No DuckDB extension download is required. Optional `extension` mode still needs upstream `httpfs`/`delta` downloads and is not validated in this environment.

## Offline backup and restore

Install `lakehouse/requirements.txt`. Stop/coordinate all writers first. Supply S3 endpoint and credentials through environment settings; keep HTTPS verification enabled and use `AWS_CA_BUNDLE` for a private CA with Boto3.

```sh
python -m lakehouse.backup backup SOURCE_BUCKET /absolute/new/snapshot --writers-paused
python -m lakehouse.backup restore NEW_EMPTY_BUCKET /absolute/new/snapshot
```

Backs up `bronze/`, `silver/`, `gold/` including Delta logs and files; excludes `checkpoints/`. The destination directory must not exist. A second inventory detects source changes. Every file's SHA-256 and size is checked before the first restore write; conditional PUT prevents overwrites. On interruption, leave the incomplete backup/restore untouched for inspection and start with a new directory/bucket. No automatic deletion or reset occurs. Resume ingestion with a consciously chosen new checkpoint/offset policy: old Kafka offsets cannot be recovered from this data backup, and blind replay can duplicate Bronze input. Model artifacts/reports and Airflow metadata need their own snapshots.

## Remaining gates

Full Docker Compose and actual Airflow scheduling/retry execution are not PASS. Compose configuration validates, and the official Spark 3.5.9 image manifest exists, but its build/run was not performed. The jobs image now declares the Kafka producer dependency it previously omitted. Airflow 3 uses a built image, private generated login settings and proper API/DAG processor services. The workspace has under 1 GB free; images need several GB. The historical MinIO image remains unavailable; tests use a verified local source build, which is not a recommendation for new production hosting.

Browser GUI, deployed TLS/IAM and production outage/recovery operations remain unverified. No public deployment or push was performed.
