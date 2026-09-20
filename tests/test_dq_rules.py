"""Pure-Python unit tests for the silver data-quality rules."""

from __future__ import annotations

from datetime import UTC

import pytest

from ingestion.schema import CHANNELS, COUNTRIES, CURRENCIES, MERCHANT_CATEGORIES
from lakehouse.dq_rules import (
    ALLOWED_CATEGORIES,
    ALLOWED_CHANNELS,
    ALLOWED_COUNTRIES,
    ALLOWED_CURRENCIES,
    RULES,
    Rule,
    evaluate,
    is_valid,
)
from lakehouse.fx_rates import FX_RATES_TO_PLN


def _valid_record() -> dict:
    return {
        "transaction_id": "11111111-1111-4111-8111-111111111111",
        "account_id": "ACC-1",
        "event_time": "2026-06-13T12:00:00+00:00",
        "amount": 42.5,
        "currency": "PLN",
        "country": "PL",
        "channel": "POS",
        "merchant_category": "grocery",
    }


def test_valid_record_passes_all_rules() -> None:
    assert evaluate(_valid_record()) == []
    assert is_valid(_valid_record())


@pytest.mark.parametrize(
    "field,bad_value,expected_rule",
    [
        ("transaction_id", "", "transaction_id_present"),
        ("transaction_id", None, "transaction_id_present"),
        ("account_id", "  ", "account_id_present"),
        ("event_time", "not-a-date", "event_time_valid"),
        ("event_time", None, "event_time_valid"),
        ("amount", 0, "amount_in_range"),
        ("amount", -5, "amount_in_range"),
        ("amount", 2_000_000, "amount_in_range"),
        ("amount", "10", "amount_in_range"),
        ("currency", "XXX", "currency_allowed"),
        ("country", "ZZ", "country_allowed"),
        ("channel", "TELEPATHY", "channel_allowed"),
        ("merchant_category", "smuggling", "merchant_category_allowed"),
    ],
)
def test_each_violation_is_detected(field: str, bad_value, expected_rule: str) -> None:
    record = _valid_record()
    record[field] = bad_value
    errors = evaluate(record)
    assert expected_rule in errors


def test_amount_bool_is_rejected() -> None:
    record = _valid_record()
    record["amount"] = True  # bool must not count as a number
    assert "amount_in_range" in evaluate(record)


def test_multiple_violations_are_all_reported() -> None:
    record = _valid_record()
    record["currency"] = "XXX"
    record["country"] = "ZZ"
    errors = evaluate(record)
    assert {"currency_allowed", "country_allowed"} <= set(errors)


def test_datetime_event_time_accepted() -> None:
    from datetime import datetime

    record = _valid_record()
    record["event_time"] = datetime(2026, 6, 13, tzinfo=UTC)
    assert is_valid(record)


def test_rule_names_are_unique() -> None:
    names = [r.name for r in RULES]
    assert len(names) == len(set(names))


def test_every_rule_has_both_forms() -> None:
    for rule in RULES:
        assert isinstance(rule, Rule)
        assert callable(rule.predicate)
        assert isinstance(rule.spark_expr, str) and rule.spark_expr.strip()


def test_allowed_sets_consistent_with_fx_and_schema() -> None:
    # The locally-defined silver vocabulary must not drift from the producer's
    # canonical vocabulary (ingestion.schema) or the FX table.
    assert frozenset(FX_RATES_TO_PLN) == ALLOWED_CURRENCIES
    assert frozenset(CURRENCIES) == ALLOWED_CURRENCIES
    assert frozenset(COUNTRIES) == ALLOWED_COUNTRIES
    assert frozenset(CHANNELS) == ALLOWED_CHANNELS
    assert frozenset(MERCHANT_CATEGORIES) == ALLOWED_CATEGORIES
