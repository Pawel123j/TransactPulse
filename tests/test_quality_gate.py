"""Pure-Python tests for the silver data-quality gate."""

from __future__ import annotations

import json

from lakehouse.quality_gate import evaluate_gate, run_gate


def _report(*, checks_pass: bool = True, row_count: int = 1000, quarantined: int = 10) -> dict:
    checks = [
        {"check": "no_null_transaction_id", "passed": checks_pass},
        {"check": "transaction_id_unique", "passed": checks_pass},
    ]
    return {
        "metrics": {"row_count": row_count, "quarantined_count": quarantined},
        "checks": checks,
    }


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
    report = {"metrics": {"row_count": 0, "quarantined_count": 0}, "checks": []}
    assert evaluate_gate(report) == []


def test_run_gate_exit_codes(tmp_path) -> None:
    good = tmp_path / "good.json"
    good.write_text(json.dumps(_report()), encoding="utf-8")
    assert run_gate(good) == 0

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(_report(checks_pass=False)), encoding="utf-8")
    assert run_gate(bad) == 1

    assert run_gate(tmp_path / "missing.json") == 2
