"""``medallion_pipeline`` — orchestrates the batch path of the lakehouse.

Flow: **silver** (cleanse/dedup + DQ report) → **data-quality gate** →
**gold aggregates** → **fraud scoring** → **drift**.

Each Spark step runs as a ``DockerOperator`` launching the ``transactpulse/streaming``
image on the ``transactpulse`` network; the gate runs the same image with a
lightweight Python entrypoint. Tasks are idempotent (Delta MERGE / overwrite) and
restartable, with retries and an on-failure alert callback (ADR 0007).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

logger = logging.getLogger("transactpulse.dag")

IMAGE = os.getenv("TP_JOBS_IMAGE", "transactpulse/streaming:latest")
NETWORK = os.getenv("TP_NETWORK", "transactpulse")

# Delta + S3A jars (no Kafka needed for the batch path).
PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

# Shared environment for the jobs (reports + model live on shared volumes).
JOB_ENV = {
    "S3_ENDPOINT": "http://minio:9000",
    "S3_ACCESS_KEY": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "S3_SECRET_KEY": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
    "LAKEHOUSE_BUCKET": os.getenv("LAKEHOUSE_BUCKET", "lakehouse"),
    "SILVER_DOCS_DIR": "/reports",
    "GOLD_DOCS_DIR": "/reports",
    "GOLD_MODEL_PATH": "/app/models/fraud_model.pkl",
}

# Named volumes (created by the compose stack) shared across tasks.
MOUNTS = [
    Mount(source="tp-reports", target="/reports", type="volume"),
    Mount(source="tp-models", target="/app/models", type="volume"),
    Mount(source="tp-spark-ivy", target="/tmp/.ivy2", type="volume"),
]


def alert_on_failure(context: dict) -> None:
    """On-failure callback — alerting hook (extend to Slack/email)."""
    task = context.get("task_instance")
    logger.error(
        "ALERT: task %s failed in DAG %s (run %s)",
        getattr(task, "task_id", "?"),
        getattr(task, "dag_id", "?"),
        context.get("run_id"),
    )


def _spark_task(dag: DAG, task_id: str, script: str, *args: str) -> DockerOperator:
    """A DockerOperator that spark-submits a job script in the Spark image."""
    return DockerOperator(
        task_id=task_id,
        image=IMAGE,
        entrypoint="/opt/spark/bin/spark-submit",
        command=["--packages", PACKAGES, "--conf", "spark.jars.ivy=/tmp/.ivy2", script, *args],
        environment=JOB_ENV,
        network_mode=NETWORK,
        mounts=MOUNTS,
        auto_remove="success",
        mount_tmp_dir=False,
        dag=dag,
    )


default_args = {
    "owner": "transactpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="medallion_pipeline",
    description="Silver → DQ gate → gold aggregates → scoring → drift",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["transactpulse", "medallion", "batch"],
) as dag:
    silver = _spark_task(dag, "silver", "/app/lakehouse/silver_job.py")

    data_quality_gate = DockerOperator(
        task_id="data_quality_gate",
        image=IMAGE,
        entrypoint="python3",
        command=[
            "-m",
            "lakehouse.quality_gate",
            "--report",
            "/reports/silver_quality_report.json",
        ],
        environment=JOB_ENV,
        network_mode=NETWORK,
        mounts=MOUNTS,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    gold = "/app/lakehouse/gold_job.py"
    gold_aggregates = _spark_task(dag, "gold_aggregates", gold, "--step", "aggregates")
    fraud_scoring = _spark_task(dag, "fraud_scoring", gold, "--step", "scoring")
    drift = _spark_task(dag, "drift", gold, "--step", "drift")

    silver >> data_quality_gate
    data_quality_gate >> gold_aggregates
    data_quality_gate >> fraud_scoring
    fraud_scoring >> drift
