"""Synthetic transaction generator with realistic patterns.

Patterns injected (see ADR 0003 and the Stage-2 README section):
    * **Log-normal amounts** — a long-tailed, realistic spend distribution.
    * **~0.3 % fraud** — rare positive class, with mildly distinct behaviour
      (higher amounts, foreign country/device, riskier merchant categories).
    * **Hourly seasonality** — a weight curve used by the emitter to modulate rate.
    * **Late-arriving events** — a fraction of records carry a backdated timestamp
      to exercise watermarking / late-data handling downstream.

The generator is deterministic for a fixed ``seed`` (idempotency).
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from faker import Faker

from ingestion.config import GeneratorConfig
from ingestion.schema import (
    CHANNELS,
    COUNTRIES,
    COUNTRY_CURRENCY,
    MERCHANT_CATEGORIES,
    Transaction,
)

# Hourly emission/volume weights (index 0..23). Low at night, lunch & evening peaks.
HOURLY_WEIGHTS: tuple[float, ...] = (
    0.30, 0.20, 0.15, 0.10, 0.10, 0.15,  # 00-05
    0.30, 0.60, 0.90, 1.00, 1.00, 1.10,  # 06-11
    1.20, 1.10, 1.00, 1.00, 1.05, 1.20,  # 12-17
    1.30, 1.20, 1.00, 0.80, 0.60, 0.40,  # 18-23
)

# Country selection weights (home market PL dominant, EU heavy, others lighter).
_COUNTRY_WEIGHTS: dict[str, float] = {
    "PL": 0.45, "DE": 0.12, "GB": 0.08, "US": 0.07, "FR": 0.06, "ES": 0.05,
    "IT": 0.04, "NL": 0.04, "CZ": 0.03, "SK": 0.03, "UA": 0.02, "CN": 0.01,
}

# Merchant categories that are over-represented among fraudulent transactions.
_FRAUD_PRONE_CATEGORIES: tuple[str, ...] = (
    "cash_withdrawal", "gambling", "electronics", "online_services", "travel",
)


@dataclass(slots=True)
class _AccountProfile:
    """Stable attributes of a synthetic account (home country, currency, devices)."""

    account_id: str
    home_country: str
    home_currency: str
    devices: tuple[str, ...]


def hourly_weight(hour: int) -> float:
    """Return the seasonality weight for an hour-of-day in ``[0, 23]``."""
    return HOURLY_WEIGHTS[hour % 24]


class TransactionGenerator:
    """Produce :class:`~ingestion.schema.Transaction` records with realistic patterns.

    Args:
        config: Generation parameters.

    The generator pre-builds stable pools of accounts and devices so that the same
    entities recur across events — enabling meaningful per-account aggregations and
    velocity/fraud features in later layers.
    """

    def __init__(self, config: GeneratorConfig) -> None:
        self.config = config
        self._rng = random.Random(config.seed)
        self._faker = Faker()
        if config.seed is not None:
            Faker.seed(config.seed)

        self._devices: list[str] = self._build_devices(config.num_devices)
        self._accounts: list[_AccountProfile] = self._build_accounts(config.num_accounts)
        self._country_choices = list(_COUNTRY_WEIGHTS.keys())
        self._country_weights = list(_COUNTRY_WEIGHTS.values())

    # ------------------------------------------------------------------ #
    # Pool construction                                                    #
    # ------------------------------------------------------------------ #
    def _build_devices(self, n: int) -> list[str]:
        return [f"DEV-{self._faker.uuid4()[:12]}" for _ in range(n)]

    def _build_accounts(self, n: int) -> list[_AccountProfile]:
        accounts: list[_AccountProfile] = []
        for _ in range(n):
            country = self._weighted_country()
            # 1-3 devices bound to the account; takeover/foreign devices added at runtime.
            k = self._rng.randint(1, 3)
            devices = tuple(self._rng.choice(self._devices) for _ in range(k))
            accounts.append(
                _AccountProfile(
                    account_id=f"ACC-{self._faker.uuid4()[:10]}",
                    home_country=country,
                    home_currency=COUNTRY_CURRENCY[country],
                    devices=devices,
                )
            )
        return accounts

    def _weighted_country(self) -> str:
        return self._rng.choices(
            list(_COUNTRY_WEIGHTS.keys()), weights=list(_COUNTRY_WEIGHTS.values()), k=1
        )[0]

    # ------------------------------------------------------------------ #
    # Sampling primitives                                                  #
    # ------------------------------------------------------------------ #
    def _sample_amount(self, is_fraud: bool) -> float:
        """Sample a positive, log-normally distributed amount (2 decimals)."""
        amount = self._rng.lognormvariate(mu=3.2, sigma=1.1)
        if is_fraud and self._rng.random() < 0.4:
            # A subset of frauds are high-value outliers.
            amount *= self._rng.uniform(2.0, 6.0)
        return round(max(amount, 0.01), 2)

    def _sample_channel(self, is_fraud: bool) -> str:
        if is_fraud:
            # Fraud skews toward card-not-present channels.
            return self._rng.choices(CHANNELS, weights=(0.55, 0.10, 0.15, 0.20), k=1)[0]
        return self._rng.choices(CHANNELS, weights=(0.30, 0.40, 0.10, 0.20), k=1)[0]

    def _sample_category(self, is_fraud: bool) -> str:
        if is_fraud and self._rng.random() < 0.6:
            return self._rng.choice(_FRAUD_PRONE_CATEGORIES)
        return self._rng.choice(MERCHANT_CATEGORIES)

    def _event_timestamp(self, now: datetime) -> str:
        """Return an ISO-8601 event timestamp, occasionally backdated (late event)."""
        ts = now
        if self._rng.random() < self.config.late_event_rate:
            delay = self._rng.randint(1, self.config.late_max_delay_seconds)
            ts = now - timedelta(seconds=delay)
        return ts.replace(microsecond=0).isoformat()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #
    def generate_one(self, now: datetime | None = None) -> Transaction:
        """Generate a single transaction.

        Args:
            now: Reference wall-clock time (UTC). Defaults to the current time.

        Returns:
            A populated :class:`~ingestion.schema.Transaction`.
        """
        if now is None:
            now = datetime.now(tz=UTC)

        account = self._rng.choice(self._accounts)
        is_fraud = self._rng.random() < self.config.fraud_rate

        if is_fraud and self._rng.random() < 0.5:
            # Account-takeover style anomaly: foreign country + unfamiliar device.
            country = self._rng.choice(COUNTRIES)
            device_id = self._rng.choice(self._devices)
        else:
            country = account.home_country
            device_id = self._rng.choice(account.devices)

        currency = COUNTRY_CURRENCY[country]

        return Transaction(
            transaction_id=str(uuid.UUID(int=self._rng.getrandbits(128), version=4)),
            timestamp=self._event_timestamp(now),
            account_id=account.account_id,
            amount=self._sample_amount(is_fraud),
            currency=currency,
            merchant_category=self._sample_category(is_fraud),
            country=country,
            channel=self._sample_channel(is_fraud),
            device_id=device_id,
            is_fraud_label=is_fraud,
        )

    def stream(self, count: int, now: datetime | None = None):
        """Yield ``count`` transactions (generator). Useful for tests and batching."""
        for _ in range(count):
            yield self.generate_one(now)
