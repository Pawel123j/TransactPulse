"""CLI gate: read the drift report and print a retraining verdict.

Runs inside the jobs image, where the ``tp-reports`` volume is mounted. The
Airflow scheduler does not mount that volume, so the decision is made here —
next to the data — and travels back to the DAG as the final line of stdout,
which ``DockerOperator(do_xcom_push=True)`` publishes as an XCom value.

That final line is a contract between this module and the DAG: it is exactly
``RETRAIN`` or ``NO_RETRAIN`` and nothing else. The human-readable justification
goes to stderr so it lands in the task log without polluting the XCom value.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from lakehouse.retraining import VERDICT_RETRAIN, decide_retraining

VERDICT_SKIP = "NO_RETRAIN"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        default="/reports/drift_report.json",
        help="Path to drift_report.json produced by the gold drift step.",
    )
    parser.add_argument(
        "--min-live-samples",
        type=int,
        default=None,
        help="Override the minimum number of scored rows treated as evidence.",
    )
    args = parser.parse_args(argv)

    path = Path(args.report)
    if not path.is_file():
        # A missing report means the drift step did not produce one. Retraining
        # on that basis would be guessing, so the gate stays closed and says why.
        print(f"Brak raportu driftu: {path}", file=sys.stderr)
        print(VERDICT_SKIP)
        return 0

    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Raport driftu nie jest poprawnym JSON-em: {exc}", file=sys.stderr)
        print(VERDICT_SKIP)
        return 0

    kwargs = {}
    if args.min_live_samples is not None:
        kwargs["min_live_samples"] = args.min_live_samples

    decision = decide_retraining(report, **kwargs)

    print(decision.reason, file=sys.stderr)
    if decision.not_comparable:
        skipped = ", ".join(decision.not_comparable)
        print(f"Metryki bez wartości dowodowej: {skipped}", file=sys.stderr)

    # Final stdout line is the contract with the DAG — nothing else may follow.
    print(VERDICT_RETRAIN if decision.should_retrain else VERDICT_SKIP)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
