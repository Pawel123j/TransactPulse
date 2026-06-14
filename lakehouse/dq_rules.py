"""Data-quality rules for the silver layer.

Each :class:`Rule` carries two equivalent forms of the same check:

* ``predicate`` — a pure-Python function ``(record: dict) -> bool`` (``True`` =
  passes), used in fast unit tests and as the canonical, readable definition;
* ``spark_expr`` — a **null-safe** boolean Spark SQL expression (``True`` =
  passes), used by the silver job to split valid rows from quarantine.

The two must stay in sync; ``tests/test_dq_rules.py`` guards that intent.

The allowed value sets are deliberately defined **locally** (rather than imported
from ``ingestion``) so the silver job carries no runtime dependency on the
generator/Faker stack inside the Spark image. ``tests/test_dq_rules.py`` asserts
they stay consistent with the canonical ``ingestion.schema`` vocabulary.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from lakehouse.fx_rates import FX_RATES_TO_PLN

#: Allowed value sets (post-normalization, i.e. upper-cased where relevant).
#: Kept consistent with ``ingestion.schema`` via tests.
ALLOWED_CURRENCIES: frozenset[str] = frozenset(FX_RATES_TO_PLN)
ALLOWED_COUNTRIES: frozenset[str] = frozenset(
    {"PL", "DE", "FR", "ES", "IT", "NL", "SK", "GB", "US", "CZ", "UA", "CN"}
)
ALLOWED_CHANNELS: frozenset[str] = frozenset({"ONLINE", "POS", "ATM", "MOBILE"})
ALLOWED_CATEGORIES: frozenset[str] = frozenset(
    {
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
    }
)

#: Plausibility bounds for a single transaction amount (PLN-agnostic; pre-FX).
AMOUNT_MIN: float = 0.0  # exclusive
AMOUNT_MAX: float = 1_000_000.0


def _non_empty(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _valid_event_time(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if not isinstance(value, str) or not value:
        return False
    try:
        datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


@dataclass(frozen=True, slots=True)
class Rule:
    """A single data-quality rule in both Python and Spark forms."""

    name: str
    description: str
    predicate: Callable[[dict[str, Any]], bool]
    spark_expr: str


RULES: tuple[Rule, ...] = (
    Rule(
        "transaction_id_present",
        "transaction_id must be a non-empty string",
        lambda r: _non_empty(r.get("transaction_id")),
        "transaction_id IS NOT NULL AND length(trim(transaction_id)) > 0",
    ),
    Rule(
        "account_id_present",
        "account_id must be a non-empty string",
        lambda r: _non_empty(r.get("account_id")),
        "account_id IS NOT NULL AND length(trim(account_id)) > 0",
    ),
    Rule(
        "event_time_valid",
        "event_time must be present and parseable",
        lambda r: _valid_event_time(r.get("event_time")),
        "event_time IS NOT NULL",
    ),
    Rule(
        "amount_in_range",
        f"amount must be a number in ({AMOUNT_MIN}, {AMOUNT_MAX}]",
        lambda r: _is_number(r.get("amount")) and AMOUNT_MIN < float(r["amount"]) <= AMOUNT_MAX,
        f"amount IS NOT NULL AND amount > {AMOUNT_MIN} AND amount <= {AMOUNT_MAX}",
    ),
    Rule(
        "currency_allowed",
        "currency must be a known ISO 4217 code",
        lambda r: r.get("currency") in ALLOWED_CURRENCIES,
        "currency IN ({})".format(", ".join(f"'{c}'" for c in sorted(ALLOWED_CURRENCIES))),
    ),
    Rule(
        "country_allowed",
        "country must be a known ISO 3166-1 alpha-2 code",
        lambda r: r.get("country") in ALLOWED_COUNTRIES,
        "country IN ({})".format(", ".join(f"'{c}'" for c in sorted(ALLOWED_COUNTRIES))),
    ),
    Rule(
        "channel_allowed",
        "channel must be a known channel",
        lambda r: r.get("channel") in ALLOWED_CHANNELS,
        "channel IN ({})".format(", ".join(f"'{c}'" for c in sorted(ALLOWED_CHANNELS))),
    ),
    Rule(
        "merchant_category_allowed",
        "merchant_category must be a known category",
        lambda r: r.get("merchant_category") in ALLOWED_CATEGORIES,
        "merchant_category IN ({})".format(", ".join(f"'{c}'" for c in sorted(ALLOWED_CATEGORIES))),
    ),
)


def evaluate(record: dict[str, Any]) -> list[str]:
    """Return the names of all rules a ``record`` violates (empty == valid)."""
    return [rule.name for rule in RULES if not rule.predicate(record)]


def is_valid(record: dict[str, Any]) -> bool:
    """Return True iff ``record`` passes every rule."""
    return not evaluate(record)
