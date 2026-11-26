"""Unit tests for the transaction schema and validator."""

from __future__ import annotations

import copy

import pytest

from ingestion.schema import (
    CURRENCIES,
    FIELDS,
    Transaction,
    is_valid,
    validate,
)


def _valid_record() -> dict:
    return Transaction(
        transaction_id="11111111-1111-4111-8111-111111111111",
        timestamp="2026-06-13T12:00:00+00:00",
        account_id="ACC-0001",
        amount=42.5,
        currency="PLN",
        merchant_category="grocery",
        country="PL",
        channel="POS",
        device_id="DEV-0001",
        is_fraud_label=False,
    ).to_dict()


def test_to_dict_has_exact_field_set_and_order() -> None:
    record = _valid_record()
    assert tuple(record.keys()) == FIELDS


def test_valid_record_passes() -> None:
    assert validate(_valid_record()) == []
    assert is_valid(_valid_record())


@pytest.mark.parametrize("field", FIELDS)
def test_missing_field_is_rejected(field: str) -> None:
    record = _valid_record()
    del record[field]
    errors = validate(record)
    assert any("missing fields" in e and field in e for e in errors)


def test_record_without_transaction_id_reports_only_that_field() -> None:
    record = _valid_record()
    del record["transaction_id"]
    # Exactly one error: the missing id must not hide or cascade into others.
    assert validate(record) == ["missing fields: transaction_id"]
    assert not is_valid(record)


@pytest.mark.parametrize("field", ["transaction_id", "account_id", "device_id"])
@pytest.mark.parametrize("bad", ["", None, 123], ids=["empty", "none", "int"])
def test_identifier_must_be_non_empty_string(field: str, bad: object) -> None:
    record = _valid_record()
    record[field] = bad
    assert validate(record) == [f"{field} must be a non-empty string"]


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"amount": 0.01},
        {"amount": 1},
        {"transaction_id": "x", "account_id": "a", "device_id": "d"},
        {"is_fraud_label": True},
    ],
    ids=["typical", "smallest-cent", "int-amount", "one-char-ids", "fraud-true"],
)
def test_minimal_valid_transaction_passes(overrides: dict) -> None:
    record = {**_valid_record(), **overrides}
    assert validate(record) == []
    assert is_valid(record)


def test_unexpected_field_is_rejected() -> None:
    record = _valid_record()
    record["surprise"] = 1
    assert any("unexpected fields" in e for e in validate(record))


@pytest.mark.parametrize("amount", [0, -1, -0.01])
def test_non_positive_amount_rejected(amount: float) -> None:
    record = _valid_record()
    record["amount"] = amount
    assert any("amount must be > 0" in e for e in validate(record))


def test_amount_must_be_number_not_bool() -> None:
    record = _valid_record()
    record["amount"] = True
    assert any("amount must be a number" in e for e in validate(record))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("currency", "XXX"),
        ("country", "ZZ"),
        ("channel", "TELEPATHY"),
        ("merchant_category", "smuggling"),
    ],
)
def test_enum_fields_reject_unknown_values(field: str, bad: str) -> None:
    record = _valid_record()
    record[field] = bad
    assert any(field in e for e in validate(record))


def test_bad_timestamp_rejected() -> None:
    record = _valid_record()
    record["timestamp"] = "13/06/2026 12:00"
    assert any("timestamp" in e for e in validate(record))


def test_is_fraud_label_must_be_bool() -> None:
    record = _valid_record()
    record["is_fraud_label"] = "yes"
    assert any("is_fraud_label" in e for e in validate(record))


def test_currency_derivation_consistency() -> None:
    # Every currency the schema advertises is a real ISO-ish 3-letter code.
    assert all(isinstance(c, str) and len(c) == 3 for c in CURRENCIES)


def test_validate_does_not_mutate_input() -> None:
    record = _valid_record()
    before = copy.deepcopy(record)
    validate(record)
    assert record == before
