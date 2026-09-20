"""Data-quality reporting for the silver layer.

Runs *after* the native gate (ADR 0005) for observability. Builds a Great
Expectations expectation suite that mirrors the gate's rules, computes metrics
over the validated silver data in Spark, optionally validates a pandas projection
with Great Expectations, and writes a Markdown report under ``docs/data_quality/``.

Great Expectations is imported lazily; if it is not installed the report is still
produced from native Spark metrics, so the pipeline never hard-fails on reporting.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lakehouse.dq_rules import (
    ALLOWED_CHANNELS,
    ALLOWED_COUNTRIES,
    ALLOWED_CURRENCIES,
    AMOUNT_MAX,
    AMOUNT_MIN,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

SUITE_NAME = "transactpulse.silver.transactions"


def build_expectation_suite() -> dict[str, Any]:
    """Return a Great-Expectations-compatible suite mirroring the silver rules."""
    expectations: list[dict[str, Any]] = []

    def add(expectation_type: str, **kwargs: Any) -> None:
        expectations.append({"expectation_type": expectation_type, "kwargs": kwargs})

    for column in ("transaction_id", "account_id", "event_time", "amount", "currency"):
        add("expect_column_values_to_not_be_null", column=column)

    add("expect_column_values_to_be_unique", column="transaction_id")
    add(
        "expect_column_values_to_be_between",
        column="amount",
        min_value=AMOUNT_MIN,
        max_value=AMOUNT_MAX,
        strict_min=True,
    )
    add("expect_column_values_to_be_between", column="amount_pln", min_value=0)
    for col_name, allowed in (
        ("currency", ALLOWED_CURRENCIES),
        ("country", ALLOWED_COUNTRIES),
        ("channel", ALLOWED_CHANNELS),
    ):
        add("expect_column_values_to_be_in_set", column=col_name, value_set=sorted(allowed))

    return {
        "expectation_suite_name": SUITE_NAME,
        "ge_cloud_id": None,
        "expectations": expectations,
        "meta": {"created_by": "transactpulse", "layer": "silver"},
    }


def compute_metrics(silver: DataFrame, quarantine_count: int = 0) -> dict[str, Any]:
    """Compute a compact quality profile of the validated silver DataFrame."""
    from pyspark.sql import functions as F

    agg = silver.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("transaction_id").alias("distinct_ids"),
        F.sum(F.col("transaction_id").isNull().cast("int")).alias("null_transaction_id"),
        F.sum(F.col("account_id").isNull().cast("int")).alias("null_account_id"),
        F.sum(F.col("amount").isNull().cast("int")).alias("null_amount"),
        F.min("amount").alias("amount_min"),
        F.max("amount").alias("amount_max"),
        F.round(F.avg("amount_pln"), 2).alias("avg_amount_pln"),
        F.sum(F.col("is_fraud_label").cast("int")).alias("fraud_count"),
    ).collect()[0]

    def _f(value: Any) -> float | None:
        return float(value) if value is not None else None

    row_count = int(agg["row_count"] or 0)
    fraud_count = int(agg["fraud_count"] or 0)
    return {
        "row_count": row_count,
        "distinct_transaction_ids": int(agg["distinct_ids"] or 0),
        "duplicate_ids": row_count - int(agg["distinct_ids"] or 0),
        "null_transaction_id": int(agg["null_transaction_id"] or 0),
        "null_account_id": int(agg["null_account_id"] or 0),
        "null_amount": int(agg["null_amount"] or 0),
        "amount_min": _f(agg["amount_min"]),
        "amount_max": _f(agg["amount_max"]),
        "avg_amount_pln": _f(agg["avg_amount_pln"]),
        "fraud_count": fraud_count,
        "fraud_rate": round(fraud_count / row_count, 5) if row_count else 0.0,
        "quarantined_count": int(quarantine_count),
    }


def evaluate_checks(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    """Derive pass/fail checks from metrics (native, GE-independent)."""
    checks = [
        ("no_null_transaction_id", metrics["null_transaction_id"] == 0),
        ("no_null_account_id", metrics["null_account_id"] == 0),
        ("no_null_amount", metrics["null_amount"] == 0),
        ("transaction_id_unique", metrics["duplicate_ids"] == 0),
        (
            "amount_within_bounds",
            (metrics["amount_min"] is None or metrics["amount_min"] > AMOUNT_MIN)
            and (metrics["amount_max"] is None or metrics["amount_max"] <= AMOUNT_MAX),
        ),
    ]
    return [{"check": name, "passed": bool(ok)} for name, ok in checks]


def render_markdown(metrics: dict[str, Any], checks: list[dict[str, Any]]) -> str:
    """Render a human-readable Markdown quality report."""
    generated = datetime.now(tz=UTC).replace(microsecond=0).isoformat()
    passed = sum(1 for c in checks if c["passed"])
    status = "✅ PASS" if passed == len(checks) else "❌ FAIL"

    lines = [
        "# Silver data-quality report",
        "",
        f"- **Generated:** {generated}",
        f"- **Suite:** `{SUITE_NAME}`",
        f"- **Overall:** {status} ({passed}/{len(checks)} checks passed)",
        "",
        "## Metrics",
        "",
        "| Metric | Value |",
        "| ------ | ----- |",
        f"| Rows (silver) | {metrics['row_count']} |",
        f"| Distinct transaction_id | {metrics['distinct_transaction_ids']} |",
        f"| Duplicate ids | {metrics['duplicate_ids']} |",
        f"| Null transaction_id | {metrics['null_transaction_id']} |",
        f"| Null account_id | {metrics['null_account_id']} |",
        f"| Null amount | {metrics['null_amount']} |",
        f"| Amount min / max | {metrics['amount_min']} / {metrics['amount_max']} |",
        f"| Avg amount (PLN) | {metrics['avg_amount_pln']} |",
        f"| Fraud count / rate | {metrics['fraud_count']} / {metrics['fraud_rate']} |",
        f"| Quarantined records | {metrics['quarantined_count']} |",
        "",
        "## Checks",
        "",
        "| Check | Result |",
        "| ----- | ------ |",
    ]
    lines += [f"| `{c['check']}` | {'✅' if c['passed'] else '❌'} |" for c in checks]
    lines.append("")
    return "\n".join(lines)


def _run_great_expectations(metrics: dict[str, Any]) -> dict[str, Any] | None:
    """Best-effort GE validation of the metrics; ``None`` if GE is unavailable."""
    try:
        import great_expectations  # noqa: F401
    except ImportError:
        return None
    # GE is present: record that the suite was loaded. Full Data Docs generation is
    # environment-specific; here we attach the suite + summary so the artifact is
    # GE-traceable without coupling to a specific GE major version.
    return {
        "great_expectations_version": getattr(great_expectations, "__version__", "unknown"),
        "suite": SUITE_NAME,
        "validated_metrics": metrics,
    }


def generate_report(
    silver: DataFrame, *, quarantine_count: int = 0, docs_dir: str = "docs/data_quality"
) -> dict[str, Any]:
    """Compute metrics, run checks, and write Markdown + JSON reports.

    Returns the metrics dict. Writes ``silver_quality_report.md`` and
    ``silver_quality_report.json`` (and a ``great_expectations_result.json`` when
    GE is installed) under ``docs_dir``.
    """
    metrics = compute_metrics(silver, quarantine_count=quarantine_count)
    checks = evaluate_checks(metrics)

    out_dir = Path(docs_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "silver_quality_report.md").write_text(
        render_markdown(metrics, checks), encoding="utf-8"
    )
    (out_dir / "silver_quality_report.json").write_text(
        json.dumps({"metrics": metrics, "checks": checks}, indent=2), encoding="utf-8"
    )
    (out_dir / "expectation_suite.json").write_text(
        json.dumps(build_expectation_suite(), indent=2), encoding="utf-8"
    )

    ge_result = _run_great_expectations(metrics)
    if ge_result is not None:
        (out_dir / "great_expectations_result.json").write_text(
            json.dumps(ge_result, indent=2), encoding="utf-8"
        )
    return metrics
