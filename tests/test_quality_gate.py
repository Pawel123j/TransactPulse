"""Pure-Python tests for the silver data-quality gate."""

from __future__ import annotations

import json

import pytest

from lakehouse.quality import evaluate_checks
from lakehouse.quality_gate import evaluate_gate, run_gate


def _report(*, checks_pass: bool = True, row_count: int = 1000, quarantined: int = 10) -> dict:
    metrics = {
        "row_count": row_count,
        "quarantined_count": quarantined,
        "duplicate_ids": 0,
        "null_transaction_id": 0,
        "null_account_id": 0,
        "null_amount": 0,
        "null_amount_pln": 0,
        "amount_min": 1.0 if row_count else None,
        "amount_max": 100.0 if row_count else None,
    }
    checks = evaluate_checks(metrics)
    if not checks_pass:
        checks[0]["passed"] = False
    return {"metrics": metrics, "checks": checks}


def test_gate_passes_clean_report() -> None:
    assert evaluate_gate(_report()) == []


def test_gate_fails_on_failed_check() -> None:
    failures = evaluate_gate(_report(checks_pass=False))
    assert any("check failed" in f for f in failures)


def test_gate_fails_on_high_quarantine_ratio() -> None:
    # 200 / (1000 + 200) = 0.167 > 0.05
    failures = evaluate_gate(_report(row_count=1000, quarantined=200))
    assert any("quarantine ratio" in f for f in failures)


def test_gate_passes_within_quarantine_threshold() -> None:
    # 40 / (1000 + 40) = 0.038 < 0.05
    assert evaluate_gate(_report(row_count=1000, quarantined=40)) == []


def test_gate_handles_zero_rows() -> None:
    report = _report(row_count=0, quarantined=0)
    assert evaluate_gate(report) == []


@pytest.mark.parametrize("report", [{}, [], None, {"metrics": {}, "checks": []}])
def test_incomplete_report_never_passes(report):
    assert evaluate_gate(report)


@pytest.mark.parametrize("value", [None, -1, "0", False, float("nan")])
def test_invalid_counter_never_passes(value):
    report = _report()
    report["metrics"]["null_amount_pln"] = value
    assert evaluate_gate(report)


def test_old_report_without_pln_measurement_fails():
    report = _report()
    del report["metrics"]["null_amount_pln"]
    assert evaluate_gate(report)


def test_summary_cannot_override_measured_failure():
    report = _report()
    report["metrics"]["null_amount_pln"] = 1
    assert "check failed: no_null_amount_pln" in evaluate_gate(report)


@pytest.mark.parametrize("checks", [[], None, [None], [{"check": "x", "passed": "true"}]])
def test_invalid_checks_fail(checks):
    report = _report()
    report["checks"] = checks
    assert evaluate_gate(report)


@pytest.mark.parametrize("threshold", [-1.0, 1.1, float("nan"), float("inf")])
def test_invalid_threshold_fails(threshold):
    assert evaluate_gate(_report(), threshold)


def test_run_gate_exit_codes(tmp_path) -> None:
    good = tmp_path / "good.json"
    good.write_text(json.dumps(_report()), encoding="utf-8")
    assert run_gate(good) == 0

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(_report(checks_pass=False)), encoding="utf-8")
    assert run_gate(bad) == 1

    assert run_gate(tmp_path / "missing.json") == 2
