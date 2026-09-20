"""Decide whether observed drift warrants retraining the fraud model.

This is the piece that closes the loop: the medallion pipeline computes PSI/KS
against the model's training reference, and this module turns that report into a
yes/no answer that Airflow can branch on.

It is deliberately a pure function over the drift report — no Spark, no Airflow,
no filesystem. That keeps the policy testable in isolation, which matters because
the failure mode we care about is not a crash but a *wrong decision*: retraining
on noise wastes compute, and not retraining on real drift silently degrades
fraud detection.

Two guards exist because "stable" in a drift report is ambiguous:

``population_stability_index`` returns ``0.0`` when either side is empty, and
``classify_psi(0.0)`` is ``"stable"``. So a model with no training reference
produces a report that looks exactly like a perfectly healthy one. The same
applies to a run that scored only a handful of rows — a PSI computed from 12
live values against 50k reference values is arithmetic, not evidence.

Both cases are reported as *not comparable* rather than folded into "stable",
so a missing reference shows up as a gap in monitoring instead of a clean bill
of health.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: PSI bands (from :func:`lakehouse.drift.classify_psi`) that justify retraining.
DEFAULT_TRIGGER_BANDS: tuple[str, ...] = ("significant",)

#: Minimum number of scored rows before a drift metric is treated as evidence.
DEFAULT_MIN_LIVE_SAMPLES = 200

#: Verdict token the gate prints when retraining is warranted.
VERDICT_RETRAIN = "RETRAIN"

#: Airflow task ids the branch chooses between.
TASK_TRIGGER_RETRAINING = "trigger_retraining"
TASK_SKIP_RETRAINING = "skip_retraining"


def verdict_to_task_id(raw: str | None) -> str:
    """Map the gate's stdout to the Airflow task that should run next.

    Lives here rather than in the DAG so it can be tested without installing
    Airflow. The rule is deliberately strict: only an exact ``RETRAIN`` on the
    last non-empty line selects retraining. An empty, missing or unexpected
    verdict means the gate could not reach a conclusion, and guessing "retrain"
    on a broken signal would spend an hour of compute on noise.
    """
    lines = [line.strip() for line in (raw or "").splitlines() if line.strip()]
    last = lines[-1] if lines else ""
    return TASK_TRIGGER_RETRAINING if last == VERDICT_RETRAIN else TASK_SKIP_RETRAINING


@dataclass(frozen=True)
class RetrainDecision:
    """Outcome of the drift policy.

    Attributes:
        should_retrain: Whether the training DAG should be triggered.
        reason: Human-readable justification, surfaced in Airflow logs.
        triggered_by: Features whose drift crossed a trigger band.
        not_comparable: Features skipped because the metric had no evidential
            value (missing reference or too few live rows).
    """

    should_retrain: bool
    reason: str
    triggered_by: tuple[str, ...] = ()
    not_comparable: tuple[str, ...] = field(default=())


def decide_retraining(
    report: dict[str, Any],
    *,
    trigger_bands: tuple[str, ...] = DEFAULT_TRIGGER_BANDS,
    min_live_samples: int = DEFAULT_MIN_LIVE_SAMPLES,
) -> RetrainDecision:
    """Decide whether ``report`` justifies retraining the fraud model.

    Args:
        report: Parsed ``drift_report.json`` as written by
            :func:`lakehouse.gold_job.write_drift_report`.
        trigger_bands: PSI bands that justify retraining.
        min_live_samples: Below this many scored rows a metric is ignored.

    Returns:
        A :class:`RetrainDecision`. ``should_retrain`` is ``False`` whenever the
        report carries no usable evidence — an absent reference is a monitoring
        gap, not a reason to burn compute on a retrain.
    """
    metrics = report.get("metrics") or {}
    if not metrics:
        return RetrainDecision(False, "Raport driftu nie zawiera metryk.")

    triggered: list[str] = []
    not_comparable: list[str] = []
    comparable: list[str] = []

    for feature, metric in metrics.items():
        reference_n = int(metric.get("reference_n") or 0)
        live_n = int(metric.get("live_n") or 0)

        if reference_n == 0:
            not_comparable.append(f"{feature} (brak referencji modelu)")
            continue
        if live_n < min_live_samples:
            not_comparable.append(f"{feature} (tylko {live_n} wierszy, próg {min_live_samples})")
            continue

        comparable.append(feature)
        if metric.get("psi_band") in trigger_bands:
            triggered.append(f"{feature} (PSI={metric.get('psi')}, {metric.get('psi_band')})")

    if triggered:
        reason = "Drift przekroczył próg: " + ", ".join(triggered)
        return RetrainDecision(True, reason, tuple(triggered), tuple(not_comparable))

    if not comparable:
        reason = (
            "Żadnej metryki nie dało się porównać: "
            + ", ".join(not_comparable)
            + ". To luka w monitorowaniu, nie potwierdzenie stabilności modelu."
        )
        return RetrainDecision(False, reason, (), tuple(not_comparable))

    reason = f"Drift w normie dla: {', '.join(comparable)}."
    if not_comparable:
        reason += " Pominięto: " + ", ".join(not_comparable) + "."
    return RetrainDecision(False, reason, (), tuple(not_comparable))
