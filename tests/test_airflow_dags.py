"""Real Airflow parser/serialization checks, not execution of Spark tasks."""

import importlib.util
from pathlib import Path

import pytest

if importlib.util.find_spec("airflow") is None:
    pytest.skip(
        "Install orchestration/requirements.txt in the Airflow job", allow_module_level=True
    )

from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization

ROOT = Path(__file__).parents[1]


@pytest.fixture(scope="module")
def dags():
    bag = DagBag(ROOT / "orchestration/dags")
    assert not bag.import_errors
    assert set(bag.dags) == {"medallion_pipeline", "train_fraud_model"}
    return bag.dags


def test_real_dags_serialize_for_scheduler(dags):
    for dag in dags.values():
        serialized = DagSerialization.to_dict(dag)
        assert serialized
        assert dag.max_active_runs == 1


def test_failed_quality_gate_blocks_all_gold_outputs(dags):
    dag = dags["medallion_pipeline"]
    gate = dag.get_task("data_quality_gate")
    assert gate.upstream_task_ids == {"silver"}
    assert gate.downstream_task_ids == {"gold_aggregates", "fraud_scoring"}
    for name in gate.downstream_task_ids:
        assert dag.get_task(name).trigger_rule == "all_success"


def test_retry_and_failure_alert_are_attached_to_actual_tasks(dags, caplog):
    dag = dags["medallion_pipeline"]
    task = dag.get_task("silver")
    assert task.retries == 2
    assert task.retry_delay.total_seconds() == 120
    assert task.on_failure_callback
    for callback in task.on_failure_callback:
        callback({"task_instance": task, "run_id": "test-failure"})
    assert "ALERT: task silver failed" in caplog.text
    assert "test-failure" in caplog.text


def test_retraining_branch_uses_actual_operator_and_declared_targets(dags):
    dag = dags["medallion_pipeline"]
    branch = dag.get_task("decide_retraining")
    from types import SimpleNamespace

    for verdict, target in [("RETRAIN", "trigger_retraining"), ("SKIP", "skip_retraining")]:
        ti = SimpleNamespace(xcom_pull=lambda *, task_ids, value=verdict: value)
        assert branch.python_callable(ti=ti) == target
    assert branch.downstream_task_ids == {"trigger_retraining", "skip_retraining"}
    assert dag.get_task("trigger_retraining").trigger_dag_id == "train_fraud_model"
