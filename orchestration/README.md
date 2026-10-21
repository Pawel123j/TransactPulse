# Airflow orchestration

Airflow 3.3.2 (Python 3.12, LocalExecutor/Postgres) schedules Silver → quality gate → Gold aggregates/scoring → drift → retraining decision. Bronze remains the continuously running Kafka consumer; do not start another writer with the same checkpoint. Delta MERGE/overwrite keeps the batch path repeatable. Two retries and a failure callback log an `ALERT` after failure; no external notification service is configured.

The image installs pinned Docker/standard providers at build time and includes the `lakehouse` module needed during DAG import. API server, scheduler and DAG processor are separate Airflow 3 services. Both DAGs allow one active run. The Docker socket is mounted only into the scheduler, using a configured socket group. Socket access still grants host control: use only in a trusted, isolated development environment.

## New isolated installation

```sh
python orchestration/init_local.py /absolute/new/private-settings
export TP_AIRFLOW_ENV=/absolute/new/private-settings/airflow.env
docker compose --env-file "$TP_AIRFLOW_ENV" -f infra/docker-compose.yml up -d --build
docker compose --env-file "$TP_AIRFLOW_ENV" -f orchestration/docker-compose.yml up -d --build
```

The generator refuses an existing directory and creates mode-0600 settings. Obtain the local admin password from `passwords.json` privately; do not log or commit it. For existing volumes, retain existing DB credentials and back up metadata before an Airflow major upgrade. This is not an automatic migration of an existing deployment. The API/UI listens on loopback port 8088.

For a proxy CA, pass `--secret id=proxy_ca,src=...` to the Airflow/jobs/dashboard Docker builds. TLS verification remains enabled. `S3_ENDPOINT=https://...` preserves HTTPS in tasks; configure trusted certificates in the Spark JVM and analytics runtime. Local Kafka remains PLAINTEXT; this Compose is not a production TLS deployment.

## Verification

```sh
pip install -r orchestration/requirements.txt -c orchestration/constraints-3.12.txt pytest
pytest -q tests/test_airflow_dags.py
```

Four tests passed with real Airflow: parsing/serialization, the quality gate dependency, retry/alert configuration, and XCom branch routing. Official 3.3.2 constraints were updated only for PyJWT 2.15.0 and urllib3 2.8.0 after five advisories were found; the installed environment audit is now clean. These tests do not execute scheduled Docker tasks. Full Compose and an actual scheduled DAG run remain unverified here because large images cannot fit the remaining workspace disk and the legacy MinIO image is unavailable. See [current evidence](../docs/STAGE3_VALIDATION.md).
