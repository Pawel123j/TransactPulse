"""``train_fraud_model`` — (re)train the baseline fraud model on a slow cadence.

Runs the trainer in the jobs image and persists the artifact to the shared
``tp-models`` volume that ``medallion_pipeline``'s scoring task reads from.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

IMAGE = os.getenv("TP_JOBS_IMAGE", "transactpulse/streaming:latest")
NETWORK = os.getenv("TP_NETWORK", "transactpulse")

with DAG(
    dag_id="train_fraud_model",
    description="Train the baseline fraud model and persist the artifact",
    default_args={"owner": "transactpulse", "retries": 1, "retry_delay": timedelta(minutes=2)},
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["transactpulse", "ml"],
) as dag:
    DockerOperator(
        task_id="train_model",
        image=IMAGE,
        entrypoint="python3",
        command=[
            "-m",
            "lakehouse.ml.train_model",
            "--samples",
            os.getenv("TRAIN_SAMPLES", "60000"),
            "--model-path",
            "/app/models/fraud_model.pkl",
        ],
        network_mode=NETWORK,
        mounts=[Mount(source="tp-models", target="/app/models", type="volume")],
        auto_remove="success",
        mount_tmp_dir=False,
    )
