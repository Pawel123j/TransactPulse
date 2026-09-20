"""Tests for the drift -> retraining policy.

The thing worth testing here is not that the function runs, but that it draws
the right line. Two mistakes cost real money in opposite directions: retraining
on noise burns compute on every pipeline run, and staying quiet through genuine
drift lets fraud detection decay without anyone noticing.
"""

from __future__ import annotations

import json

import pytest

from lakehouse.retrain_gate import main as gate_main
from lakehouse.retraining import (
    DEFAULT_MIN_LIVE_SAMPLES,
    TASK_SKIP_RETRAINING,
    TASK_TRIGGER_RETRAINING,
    RetrainDecision,
    decide_retraining,
    verdict_to_task_id,
)


def report(**features: dict) -> dict:
    """Build a drift report shaped like ``gold_job.write_drift_report`` output."""
    return {
        "generated": "2026-09-20T10:00:00+00:00",
        "model_kind": "logistic",
        "metrics": features,
    }


def metric(psi: float, band: str, *, reference_n: int = 50_000, live_n: int = 5_000) -> dict:
    return {
        "psi": psi,
        "psi_band": band,
        "ks": 0.1,
        "reference_n": reference_n,
        "live_n": live_n,
    }


# --------------------------------------------------------------- triggers


def test_significant_drift_triggers_retraining() -> None:
    decision = decide_retraining(report(fraud_score=metric(0.31, "significant")))

    assert decision.should_retrain is True
    assert decision.triggered_by == ("fraud_score (PSI=0.31, significant)",)
    assert "fraud_score" in decision.reason


def test_drift_in_any_feature_is_enough() -> None:
    decision = decide_retraining(
        report(
            fraud_score=metric(0.02, "stable"),
            amount_pln=metric(0.44, "significant"),
        )
    )

    assert decision.should_retrain is True
    assert len(decision.triggered_by) == 1
    assert "amount_pln" in decision.triggered_by[0]


def test_every_drifting_feature_is_listed() -> None:
    decision = decide_retraining(
        report(
            fraud_score=metric(0.31, "significant"),
            amount_pln=metric(0.44, "significant"),
        )
    )

    assert decision.should_retrain is True
    assert len(decision.triggered_by) == 2


# ----------------------------------------------------------- no triggers


def test_stable_drift_does_not_trigger() -> None:
    decision = decide_retraining(
        report(
            fraud_score=metric(0.02, "stable"),
            amount_pln=metric(0.05, "stable"),
        )
    )

    assert decision.should_retrain is False
    assert decision.triggered_by == ()


def test_moderate_band_does_not_trigger_by_default() -> None:
    """``moderate`` is a watch signal, not an action signal.

    PSI between 0.1 and 0.2 is the band where distributions wobble for ordinary
    reasons — seasonality, a marketing campaign, a new merchant. Retraining on
    every wobble means retraining constantly.
    """
    decision = decide_retraining(report(fraud_score=metric(0.15, "moderate")))

    assert decision.should_retrain is False


def test_moderate_can_be_opted_into() -> None:
    decision = decide_retraining(
        report(fraud_score=metric(0.15, "moderate")),
        trigger_bands=("moderate", "significant"),
    )

    assert decision.should_retrain is True


# ------------------------------------------------- "stable" that is not stable


def test_missing_reference_is_not_treated_as_stable() -> None:
    """A model with no training reference must not look healthy.

    ``population_stability_index`` returns 0.0 when either side is empty, and
    ``classify_psi(0.0)`` is "stable" — so a report for a model that was never
    given a reference is byte-for-byte indistinguishable from a report for a
    perfectly calibrated one. Folding that into "stable" would hide a broken
    monitoring setup behind a green signal.
    """
    decision = decide_retraining(report(fraud_score=metric(0.0, "stable", reference_n=0)))

    assert decision.should_retrain is False
    assert decision.not_comparable == ("fraud_score (brak referencji modelu)",)
    assert "luka w monitorowaniu" in decision.reason
    assert "stabil" not in decision.reason.lower().replace("stabilności", "")


def test_too_few_live_rows_is_not_evidence() -> None:
    """PSI over a handful of rows is arithmetic, not evidence."""
    decision = decide_retraining(report(fraud_score=metric(0.9, "significant", live_n=12)))

    assert decision.should_retrain is False
    assert decision.not_comparable == (
        f"fraud_score (tylko 12 wierszy, próg {DEFAULT_MIN_LIVE_SAMPLES})",
    )


def test_comparable_feature_still_triggers_when_another_is_skipped() -> None:
    """One unusable metric must not mask drift in a usable one."""
    decision = decide_retraining(
        report(
            fraud_score=metric(0.31, "significant"),
            amount_pln=metric(0.0, "stable", reference_n=0),
        )
    )

    assert decision.should_retrain is True
    assert decision.triggered_by == ("fraud_score (PSI=0.31, significant)",)
    assert decision.not_comparable == ("amount_pln (brak referencji modelu)",)


def test_skipped_features_are_reported_even_when_calm() -> None:
    decision = decide_retraining(
        report(
            fraud_score=metric(0.02, "stable"),
            amount_pln=metric(0.0, "stable", reference_n=0),
        )
    )

    assert decision.should_retrain is False
    assert "Pominięto" in decision.reason
    assert "amount_pln" in decision.reason


# ------------------------------------------------------------ degenerate input


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"metrics": {}},
        {"generated": "2026-09-20T10:00:00+00:00"},
    ],
)
def test_empty_report_does_not_trigger(payload: dict) -> None:
    decision = decide_retraining(payload)

    assert decision.should_retrain is False
    assert "nie zawiera metryk" in decision.reason


def test_missing_counts_are_treated_as_zero() -> None:
    """A malformed metric must not be read as a healthy one."""
    decision = decide_retraining({"metrics": {"fraud_score": {"psi_band": "significant"}}})

    assert decision.should_retrain is False
    assert decision.not_comparable == ("fraud_score (brak referencji modelu)",)


def test_decision_is_immutable() -> None:
    decision = decide_retraining(report(fraud_score=metric(0.02, "stable")))

    assert isinstance(decision, RetrainDecision)
    with pytest.raises(AttributeError):
        decision.should_retrain = True  # type: ignore[misc]


# ------------------------------------------------- mapowanie werdyktu na task


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("RETRAIN", TASK_TRIGGER_RETRAINING),
        ("  RETRAIN  ", TASK_TRIGGER_RETRAINING),
        ("uzasadnienie\nRETRAIN", TASK_TRIGGER_RETRAINING),
        ("RETRAIN\n", TASK_TRIGGER_RETRAINING),
        ("NO_RETRAIN", TASK_SKIP_RETRAINING),
        ("", TASK_SKIP_RETRAINING),
        (None, TASK_SKIP_RETRAINING),
        ("retrain", TASK_SKIP_RETRAINING),
        ("RETRAIN_LATER", TASK_SKIP_RETRAINING),
        ("RETRAIN\nNO_RETRAIN", TASK_SKIP_RETRAINING),
    ],
)
def test_verdict_mapping(raw: str | None, expected: str) -> None:
    """Only an exact RETRAIN on the last non-empty line selects retraining.

    An unreadable verdict means the gate reached no conclusion. Guessing
    "retrain" there would spend compute on a broken signal, so the strict
    reading is the safe one.
    """
    assert verdict_to_task_id(raw) == expected


# ------------------------------------------------------------- bramka CLI


def write_report(tmp_path, payload) -> str:
    path = tmp_path / "drift_report.json"
    path.write_text(json.dumps(payload) if isinstance(payload, dict) else payload, encoding="utf-8")
    return str(path)


def run_gate(capsys, *args: str) -> tuple[str, str]:
    code = gate_main(list(args))
    assert code == 0, "bramka nigdy nie powinna wywracać zadania"
    captured = capsys.readouterr()
    return captured.out.strip().splitlines()[-1], captured.err


def test_gate_prints_retrain_on_significant_drift(tmp_path, capsys) -> None:
    path = write_report(tmp_path, report(fraud_score=metric(0.31, "significant")))
    verdict, stderr = run_gate(capsys, "--report", path)

    assert verdict == "RETRAIN"
    assert "fraud_score" in stderr


def test_gate_prints_no_retrain_on_stable(tmp_path, capsys) -> None:
    path = write_report(tmp_path, report(fraud_score=metric(0.02, "stable")))
    verdict, _ = run_gate(capsys, "--report", path)

    assert verdict == "NO_RETRAIN"


def test_gate_survives_missing_report(tmp_path, capsys) -> None:
    verdict, stderr = run_gate(capsys, "--report", str(tmp_path / "nie_ma.json"))

    assert verdict == "NO_RETRAIN"
    assert "Brak raportu" in stderr


def test_gate_survives_malformed_json(tmp_path, capsys) -> None:
    path = write_report(tmp_path, "to nie jest json")
    verdict, stderr = run_gate(capsys, "--report", path)

    assert verdict == "NO_RETRAIN"
    assert "poprawnym JSON" in stderr


def test_gate_reports_missing_reference_on_stderr(tmp_path, capsys) -> None:
    path = write_report(tmp_path, report(fraud_score=metric(0.0, "stable", reference_n=0)))
    verdict, stderr = run_gate(capsys, "--report", path)

    assert verdict == "NO_RETRAIN"
    assert "luka w monitorowaniu" in stderr


def test_gate_verdict_is_the_last_stdout_line(tmp_path, capsys) -> None:
    """The DAG reads the last stdout line as XCom — nothing may follow it."""
    path = write_report(tmp_path, report(fraud_score=metric(0.31, "significant")))
    gate_main(["--report", path])
    out = capsys.readouterr().out

    assert out.strip().splitlines()[-1] == "RETRAIN"
    assert out.count("RETRAIN") == 1
