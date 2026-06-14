"""Transaction record schema, allowed value sets and a dependency-free validator.

The wire format is JSON (see ADR 0003), but records are validated against this
schema *before* they are produced, so malformed events never reach Kafka.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

# --------------------------------------------------------------------------- #
# Controlled vocabularies                                                       #
# --------------------------------------------------------------------------- #

#: Allowed merchant categories (MCC-like buckets).
MERCHANT_CATEGORIES: tuple[str, ...] = (
    "grocery",
    "restaurant",
    "travel",
    "electronics",
    "fashion",
    "fuel",
    "entertainment",
    "health",
    "utilities",
    "cash_withdrawal",
    "online_services",
    "gambling",
)

#: Allowed transaction channels.
CHANNELS: tuple[str, ...] = ("ONLINE", "POS", "ATM", "MOBILE")

#: Country (ISO 3166-1 alpha-2) -> ISO 4217 currency mapping used by the generator.
COUNTRY_CURRENCY: dict[str, str] = {
    "PL": "PLN",
    "DE": "EUR",
    "FR": "EUR",
    "ES": "EUR",
    "IT": "EUR",
    "NL": "EUR",
    "SK": "EUR",
    "GB": "GBP",
    "US": "USD",
    "CZ": "CZK",
    "UA": "UAH",
    "CN": "CNY",
}

#: Allowed ISO country codes.
COUNTRIES: tuple[str, ...] = tuple(COUNTRY_CURRENCY.keys())

#: Allowed ISO currency codes.
CURRENCIES: tuple[str, ...] = tuple(sorted(set(COUNTRY_CURRENCY.values())))

#: Ordered list of record fields (also the JSON key order).
FIELDS: tuple[str, ...] = (
    "transaction_id",
    "timestamp",
    "account_id",
    "amount",
    "currency",
    "merchant_category",
    "country",
    "channel",
    "device_id",
    "is_fraud_label",
)


@dataclass(slots=True)
class Transaction:
    """A single synthetic financial transaction.

    Attributes:
        transaction_id: Unique event identifier (UUID4 string).
        timestamp: Event time as an ISO-8601 UTC string (may be late-arriving).
        account_id: Stable account identifier.
        amount: Positive monetary amount in ``currency``, rounded to 2 decimals.
        currency: ISO 4217 currency code (member of :data:`CURRENCIES`).
        merchant_category: Member of :data:`MERCHANT_CATEGORIES`.
        country: ISO 3166-1 alpha-2 code (member of :data:`COUNTRIES`).
        channel: Member of :data:`CHANNELS`.
        device_id: Identifier of the device used for the transaction.
        is_fraud_label: Ground-truth fraud flag (synthetic).
    """

    transaction_id: str
    timestamp: str
    account_id: str
    amount: float
    currency: str
    merchant_category: str
    country: str
    channel: str
    device_id: str
    is_fraud_label: bool

    def to_dict(self) -> dict[str, Any]:
        """Return the record as a plain, JSON-serializable dict (stable key order)."""
        data = asdict(self)
        return {field: data[field] for field in FIELDS}


#: A JSON-Schema (draft-2020-12) description, exposed for documentation/tests.
TRANSACTION_JSON_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Transaction",
    "type": "object",
    "additionalProperties": False,
    "required": list(FIELDS),
    "properties": {
        "transaction_id": {"type": "string", "minLength": 1},
        "timestamp": {"type": "string", "format": "date-time"},
        "account_id": {"type": "string", "minLength": 1},
        "amount": {"type": "number", "exclusiveMinimum": 0},
        "currency": {"type": "string", "enum": list(CURRENCIES)},
        "merchant_category": {"type": "string", "enum": list(MERCHANT_CATEGORIES)},
        "country": {"type": "string", "enum": list(COUNTRIES)},
        "channel": {"type": "string", "enum": list(CHANNELS)},
        "device_id": {"type": "string", "minLength": 1},
        "is_fraud_label": {"type": "boolean"},
    },
}


def _is_iso8601(value: str) -> bool:
    """Return True if ``value`` parses as an ISO-8601 datetime."""
    try:
        datetime.fromisoformat(value)
        return True
    except (ValueError, TypeError):
        return False


def validate(record: dict[str, Any]) -> list[str]:
    """Validate a transaction record against the schema.

    This is a dependency-free validator (no ``jsonschema`` requirement) suitable
    for the producer hot path and unit tests.

    Args:
        record: The candidate record as a dict.

    Returns:
        A list of human-readable error messages. An empty list means the record
        is valid.
    """
    errors: list[str] = []

    if not isinstance(record, dict):
        return [f"record must be a dict, got {type(record).__name__}"]

    missing = [f for f in FIELDS if f not in record]
    if missing:
        errors.append(f"missing fields: {', '.join(missing)}")

    extra = [k for k in record if k not in FIELDS]
    if extra:
        errors.append(f"unexpected fields: {', '.join(sorted(extra))}")

    # Validate individual fields only when present.
    if "transaction_id" in record and not _non_empty_str(record["transaction_id"]):
        errors.append("transaction_id must be a non-empty string")

    if "account_id" in record and not _non_empty_str(record["account_id"]):
        errors.append("account_id must be a non-empty string")

    if "device_id" in record and not _non_empty_str(record["device_id"]):
        errors.append("device_id must be a non-empty string")

    if "timestamp" in record:
        ts = record["timestamp"]
        if not isinstance(ts, str) or not _is_iso8601(ts):
            errors.append("timestamp must be an ISO-8601 string")

    if "amount" in record:
        amount = record["amount"]
        # bool is a subclass of int -> reject explicitly.
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            errors.append("amount must be a number")
        elif amount <= 0:
            errors.append(f"amount must be > 0, got {amount}")

    _check_enum(record, "currency", CURRENCIES, errors)
    _check_enum(record, "merchant_category", MERCHANT_CATEGORIES, errors)
    _check_enum(record, "country", COUNTRIES, errors)
    _check_enum(record, "channel", CHANNELS, errors)

    if "is_fraud_label" in record and not isinstance(record["is_fraud_label"], bool):
        errors.append("is_fraud_label must be a boolean")

    return errors


def is_valid(record: dict[str, Any]) -> bool:
    """Return True iff ``record`` passes :func:`validate`."""
    return not validate(record)


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string (seconds precision)."""
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def _check_enum(
    record: dict[str, Any], field: str, allowed: tuple[str, ...], errors: list[str]
) -> None:
    if field in record and record[field] not in allowed:
        errors.append(f"{field} must be one of {allowed}, got {record[field]!r}")
