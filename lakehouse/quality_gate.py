"""Data-quality gate: fail the pipeline if the silver quality report regressed.

Reads the JSON report written by the silver job (``silver_quality_report.json``)
and exits non-zero when any check failed or the quarantine ratio exceeds a
threshold. Pure-Python and Spark-free, so it runs as a lightweight Airflow task
(see the ``medallion_pipeline`` DAG).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def evaluate_gate(report: dict[str, Any], max_quarantine_ratio: float = 0.05) -> list[str]:
    """Return a list of gate failures for a silver quality ``report`` (empty == pass).

    Args:
        report: Parsed ``silver_quality_report.json`` (``{"metrics": ..., "checks": ...}``).
        max_quarantine_ratio: Maximum tolerated quarantined / (silver + quarantined).
    """
    failures: list[str] = []

    for check in report.get("checks", []):
        if not check.get("passed", False):
            failures.append(f"check failed: {check.get('check', '<unknown>')}")

    metrics = report.get("metrics", {})
    row_count = int(metrics.get("row_count", 0) or 0)
    quarantined = int(metrics.get("quarantined_count", 0) or 0)
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
