"""Data-quality gate: fail the pipeline if the silver quality report regressed.

Reads the JSON report written by the silver job (``silver_quality_report.json``)
and exits non-zero when any check failed or the quarantine ratio exceeds a
threshold. Pure-Python and Spark-free, so it runs as a lightweight Airflow task
(see the ``medallion_pipeline`` DAG).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

from lakehouse.quality import evaluate_checks

_COUNTS = (
    "row_count",
    "quarantined_count",
    "duplicate_ids",
    "null_transaction_id",
    "null_account_id",
    "null_amount",
    "null_amount_pln",
)


def _valid_metrics(metrics: Any) -> bool:
    if not isinstance(metrics, dict):
        return False
    if any(type(metrics.get(key)) is not int or metrics[key] < 0 for key in _COUNTS):
        return False
    for key in ("amount_min", "amount_max"):
        if key not in metrics:
            return False
        value = metrics[key]
        if value is None:
            if metrics["row_count"] != 0:
                return False
        elif type(value) not in (int, float) or not math.isfinite(value):
            return False
    return True


def evaluate_gate(report: dict[str, Any], max_quarantine_ratio: float = 0.05) -> list[str]:
    """Return a list of gate failures for a silver quality ``report`` (empty == pass).

    Args:
        report: Parsed ``silver_quality_report.json`` (``{"metrics": ..., "checks": ...}``).
        max_quarantine_ratio: Maximum tolerated quarantined / (silver + quarantined).
    """
    if not math.isfinite(max_quarantine_ratio) or not 0 <= max_quarantine_ratio <= 1:
        return ["invalid quarantine threshold"]
    if not isinstance(report, dict) or not _valid_metrics(report.get("metrics")):
        return ["missing or invalid quality metrics; regenerate the silver report"]
    metrics = report["metrics"]
    computed = evaluate_checks(metrics)
    expected = {check["check"] for check in computed}
    checks = report.get("checks")
    if not isinstance(checks, list) or any(
        not isinstance(check, dict)
        or not isinstance(check.get("check"), str)
        or type(check.get("passed")) is not bool
        for check in checks
    ):
        return ["missing or invalid quality checks"]
    names = [check["check"] for check in checks]
    if not expected.issubset(names) or len(names) != len(set(names)):
        return ["missing or duplicate quality checks; regenerate the silver report"]
    # Recompute the required checks: a stale/success-looking summary must not
    # override the measured NULL or duplicate counters.
    failures = sorted(
        {f"check failed: {check['check']}" for check in [*checks, *computed] if not check["passed"]}
    )
    row_count = metrics["row_count"]
    quarantined = metrics["quarantined_count"]
    total = row_count + quarantined
    if total > 0:
        ratio = quarantined / total
        if ratio > max_quarantine_ratio:
            failures.append(f"quarantine ratio {ratio:.3f} exceeds max {max_quarantine_ratio:.3f}")
    return failures


def run_gate(report_path: str | Path, max_quarantine_ratio: float = 0.05) -> int:
    """Evaluate the gate at ``report_path``; return a process exit code (0 = pass)."""
    path = Path(report_path)
    if not path.exists():
        print(f"data-quality gate: report not found at {path}", file=sys.stderr)  # noqa: T201
        return 2

    report = json.loads(path.read_text(encoding="utf-8"))
    failures = evaluate_gate(report, max_quarantine_ratio=max_quarantine_ratio)
    if failures:
        print("data-quality gate FAILED:", file=sys.stderr)  # noqa: T201
        for f in failures:
            print(f"  - {f}", file=sys.stderr)  # noqa: T201
        return 1

    print("data-quality gate passed ✅")  # noqa: T201
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Silver data-quality gate.")
    parser.add_argument(
        "--report",
        default="docs/data_quality/silver_quality_report.json",
        help="Path to the silver quality report JSON.",
    )
    parser.add_argument("--max-quarantine-ratio", type=float, default=0.05)
    args = parser.parse_args(argv)
    return run_gate(args.report, max_quarantine_ratio=args.max_quarantine_ratio)


if __name__ == "__main__":
    raise SystemExit(main())
