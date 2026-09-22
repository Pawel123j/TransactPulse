"""Runtime validation of the drift → retraining loop and its Airflow glue.

The loop spans three pieces that are edited independently:

    gold_job (drift report) → lakehouse.retraining (decision) → medallion_pipeline
    (BranchPythonOperator routes to trigger_retraining / skip_retraining)

The unit tests in ``test_retraining.py`` cover the decision policy in isolation.
What they cannot catch is the *glue* breaking: if a task in the DAG is renamed
but the ``TASK_*`` constant is not (or vice versa), the branch operator returns a
task id that no longer exists and the whole pipeline fails at run time — Airflow
raises ``BranchPythonOperator ... are not in the task group``.

This module closes that gap without installing Airflow:

1. It *executes* the gate glue (``verdict_to_task_id``) over the full matrix of
   stdout shapes the Docker gate can emit, and asserts every result is one of the
   two known task ids — the branch can never route somewhere undefined.
2. It parses the real DAG file and asserts those two task ids are actually
   declared there, so the runtime routing targets and the DAG stay in lock-step.

Both run in the ordinary ``pytest`` job — no Spark, no Airflow, no Docker.
"""

from __future__ import annotations

import ast
import pathlib

from lakehouse.retraining import (
    TASK_SKIP_RETRAINING,
    TASK_TRIGGER_RETRAINING,
    VERDICT_RETRAIN,
    decide_retraining,
    verdict_to_task_id,
)

DAG_FILE = (
    pathlib.Path(__file__).resolve().parent.parent
    / "orchestration"
    / "dags"
    / "medallion_pipeline.py"
)

# Every stdout shape the Docker gate can realistically hand back, paired with the
# branch it must select. This is the routing contract exercised at run time.
_GATE_STDOUT_CASES = [
    (VERDICT_RETRAIN, TASK_TRIGGER_RETRAINING),
    (f"some log line\n{VERDICT_RETRAIN}", TASK_TRIGGER_RETRAINING),
    (f"{VERDICT_RETRAIN}\n", TASK_TRIGGER_RETRAINING),
    ("SKIP", TASK_SKIP_RETRAINING),
    (f"{VERDICT_RETRAIN}\ntrailing noise", TASK_SKIP_RETRAINING),  # RETRAIN not last
    ("retrain", TASK_SKIP_RETRAINING),  # case-sensitive on purpose
    ("", TASK_SKIP_RETRAINING),
    ("   ", TASK_SKIP_RETRAINING),
    (None, TASK_SKIP_RETRAINING),
]


def _declared_task_ids(dag_source: str) -> set[str]:
    """Collect every task id declared in the DAG, without importing Airflow.

    Covers both the ``task_id=`` keyword on operator constructors and the second
    positional argument of the ``_spark_task(dag, "<name>", ...)`` helper.
    """
    tree = ast.parse(dag_source)
    task_ids: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for kw in node.keywords:
            if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                task_ids.add(kw.value.value)
        func = node.func
        if (
            isinstance(func, ast.Name)
            and func.id == "_spark_task"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
        ):
            task_ids.add(node.args[1].value)
    return task_ids


def test_gate_routing_only_ever_targets_known_tasks() -> None:
    known = {TASK_TRIGGER_RETRAINING, TASK_SKIP_RETRAINING}
    for raw, expected in _GATE_STDOUT_CASES:
        result = verdict_to_task_id(raw)
        assert result == expected, f"stdout {raw!r} routed to {result!r}, expected {expected!r}"
        assert result in known


def test_branch_targets_exist_in_the_dag() -> None:
    declared = _declared_task_ids(DAG_FILE.read_text(encoding="utf-8"))
    # If either of these fails, the BranchPythonOperator would return a task id
    # that Airflow cannot find and the pipeline would break on every run.
    assert TASK_TRIGGER_RETRAINING in declared, (
        f"{TASK_TRIGGER_RETRAINING!r} not declared in {DAG_FILE.name}; declared={sorted(declared)}"
    )
    assert TASK_SKIP_RETRAINING in declared, (
        f"{TASK_SKIP_RETRAINING!r} not declared in {DAG_FILE.name}; declared={sorted(declared)}"
    )


def test_significant_drift_drives_the_loop_to_retraining() -> None:
    """End-to-end over the loop's pure stages: report → decision → branch id."""
    report = {
        "metrics": {
            "amount_pln": {
                "psi": 0.42,
                "psi_band": "significant",
                "reference_n": 50_000,
                "live_n": 8_000,
            }
        }
    }
    decision = decide_retraining(report)
    assert decision.should_retrain is True

    # The Docker gate prints VERDICT_RETRAIN as its last line when should_retrain.
    gate_stdout = f"drift gate: {decision.reason}\n{VERDICT_RETRAIN}"
    assert verdict_to_task_id(gate_stdout) == TASK_TRIGGER_RETRAINING


def test_unusable_report_never_triggers_retraining() -> None:
    """A missing model reference is a monitoring gap, not a reason to retrain."""
    report = {
        "metrics": {
            "amount_pln": {"psi": 0.0, "psi_band": "stable", "reference_n": 0, "live_n": 8_000}
        }
    }
    decision = decide_retraining(report)
    assert decision.should_retrain is False

    gate_stdout = f"drift gate: {decision.reason}\nSKIP"
    assert verdict_to_task_id(gate_stdout) == TASK_SKIP_RETRAINING
