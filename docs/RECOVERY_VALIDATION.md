# Recovery validation — 2026-11-15

This change fixes required NULL values passing the native Spark quality rules,
missing normalized PLN measurements, and Python 3.10 in the previous Spark image
being incompatible with the current ML/Arrow dependencies.

The jobs image and host requirements now use Python 3.11, Java 17,
PySpark 3.5.9 and Delta 3.2.1, with matching Kafka/Scala/Hadoop connectors.
MinIO is built from an official release instead of relying on an unavailable
historical container tag. No production deployment is part of these checks.

## Automated evidence

- `tests/test_silver_transform.py` rejects every required NULL and a future rule
  returning SQL UNKNOWN; invalid rows cannot reach Gold aggregates.
- `tests/test_quality_gate.py` rejects incomplete/stale reports, absent PLN
  measurements, invalid counts/thresholds, missing checks and success flags that
  contradict measured failures. Empty datasets still require a complete report.
- `tests/test_delta_pipeline.py` starts a separate Python/JVM process and writes
  actual Bronze, quarantine, Silver and Gold Delta files. Repeated Silver MERGE
  and Gold overwrites keep totals stable. A new Spark context reads them back.
  It has no Kafka, S3 or Airflow dependency and does not claim to test them.
- CI's `docker-build` job builds the runtime images and checks writable report
  and model volumes as uid 185, including a report volume initialized by the
  dashboard first. It starts the actual Kafka/MinIO Compose services, creates a
  private fixture bucket, and runs `tests/test_live_pipeline.py` in the jobs image.
  That test checks a NULL-currency record through Kafka into quarantine,
  checkpoint restart, repeated Silver MERGE, Gold scoring, DuckDB queries,
  Streamlit refresh after another event, and a real refused S3A connection.
- The real Airflow parser/serialization tests run in a separate CI environment.

Final commit evidence is recorded in [PR #53](https://github.com/Pawel123j/TransactPulse/pull/53)
and its linked GitHub Actions checks. Historical Stage 3 results remain in
[STAGE3_VALIDATION.md](STAGE3_VALIDATION.md); they are not new results for this change.

## Repeat locally

Use Python 3.11 and set `JAVA_HOME` to Java 17. Install
`requirements-dev.txt`, `requirements-spark.txt` and `dashboard/requirements.txt`,
then run `TP_REQUIRE_SPARK=1 pytest -q`. Delta resolves its pinned jars from Maven;
an offline environment can supply `TP_DELTA_TEST_JARS` with those verified jars.
Missing Spark is an error when `TP_REQUIRE_SPARK=1`.

The live test additionally requires `TP_LIVE_INTEGRATION=1`, a **new private**
`TP_TEST_BUCKET` named `tp-stage3-<32 lowercase hex digits>`, Kafka and S3 endpoint
credentials, and either `TP_TEST_JARS` or compatible `PYSPARK_SUBMIT_ARGS`.
The caller owns the bucket lifecycle; the test creates/removes its unique topic.
`scripts/test-live-container.sh` is restricted to disposable CI runners because
its teardown removes the fixture Compose volumes.

## Scope and remaining acceptance

Quarantine is an append-only record of rejected processing attempts: rerunning
Silver can add another rejection entry. The replay assertions cover Bronze
checkpoint recovery, Silver entity counts and Gold totals, not quarantine dedup.
The ML path uses the documented heuristic fallback when no trained artifact is
supplied; these tests are not evidence of model accuracy.

The entire Airflow scheduler/task/retry lifecycle, Spark JVM TLS, Kafka TLS,
production IAM and recovery after host failure still require separate acceptance.
Streamlit AppTest exercises the real Python UI runtime, not a browser.

Fresh report/model volumes are initialized for uid 185. Existing volumes created
by older root containers may require an ownership migration: stop writers, back
up those two volumes, and change only `tp-reports` / `tp-models` to uid/gid 185.
Do not delete volumes or change Kafka, MinIO or Airflow data to repair permissions.
Regenerate the Silver quality report before using the stricter gate; old reports
without the PLN counter now fail closed.
