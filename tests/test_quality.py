"""Pure-Python unit tests for the silver quality reporting (no Spark)."""

from __future__ import annotations

from lakehouse.quality import (
    SUITE_NAME,
    build_expectation_suite,
    evaluate_checks,
    render_markdown,
)


def _good_metrics() -> dict:
    return {
        "row_count": 1000,
        "distinct_transaction_ids": 1000,
        "duplicate_ids": 0,
        "null_transaction_id": 0,
        "null_account_id": 0,
        "null_amount": 0,
        "amount_min": 1.0,
        "amount_max": 9999.0,
        "avg_amount_pln": 120.5,
        "fraud_count": 3,
        "fraud_rate": 0.003,
        "quarantined_count": 12,
    }


def test_expectation_suite_has_expected_shape() -> None:
    suite = build_expectation_suite()
    assert suite["expectation_suite_name"] == SUITE_NAME
    types = {e["expectation_type"] for e in suite["expectations"]}
    assert "expect_column_values_to_not_be_null" in types
    assert "expect_column_values_to_be_unique" in types
    assert "expect_column_values_to_be_in_set" in types
    # transaction_id uniqueness is asserted
    assert any(
        e["expectation_type"] == "expect_column_values_to_be_unique"
        and e["kwargs"]["column"] == "transaction_id"
        for e in suite["expectations"]
    )


def test_checks_pass_on_clean_metrics() -> None:
    checks = evaluate_checks(_good_metrics())
    assert all(c["passed"] for c in checks)


def test_checks_fail_on_duplicates_and_nulls() -> None:
    metrics = _good_metrics()
    metrics["duplicate_ids"] = 5
    metrics["null_amount"] = 2
    checks = {c["check"]: c["passed"] for c in evaluate_checks(metrics)}
    assert checks["transaction_id_unique"] is False
    assert checks["no_null_amount"] is False


def test_checks_fail_on_out_of_bounds_amount() -> None:
    metrics = _good_metrics()
    metrics["amount_max"] = 5_000_000.0
    checks = {c["check"]: c["passed"] for c in evaluate_checks(metrics)}
    assert checks["amount_within_bounds"] is False


def test_render_markdown_contains_status_and_metrics() -> None:
    metrics = _good_metrics()
    md = render_markdown(metrics, evaluate_checks(metrics))
    assert "# Silver data-quality report" in md
    assert "✅ PASS" in md
    assert "| Rows (silver) | 1000 |" in md
    assert SUITE_NAME in md


def test_render_markdown_reports_failure() -> None:
    metrics = _good_metrics()
    metrics["duplicate_ids"] = 1
    md = render_markdown(metrics, evaluate_checks(metrics))
    assert "❌ FAIL" in md
