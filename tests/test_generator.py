"""Unit tests for the synthetic transaction generator."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from faker import Faker

from ingestion.config import GeneratorConfig
from ingestion.generator import HOURLY_WEIGHTS, TransactionGenerator, hourly_weight
from ingestion.schema import COUNTRY_CURRENCY, validate


@pytest.fixture
def generator() -> TransactionGenerator:
    cfg = GeneratorConfig(seed=1234, num_accounts=200, num_devices=300)
    return TransactionGenerator(cfg)


def test_generated_records_are_schema_valid(generator: TransactionGenerator) -> None:
    for tx in generator.stream(500):
        assert validate(tx.to_dict()) == [], tx.to_dict()


def test_currency_matches_country(generator: TransactionGenerator) -> None:
    for tx in generator.stream(500):
        assert tx.currency == COUNTRY_CURRENCY[tx.country]


def test_amounts_are_positive(generator: TransactionGenerator) -> None:
    assert all(tx.amount > 0 for tx in generator.stream(500))


def test_determinism_with_seed() -> None:
    cfg = GeneratorConfig(seed=99, num_accounts=100, num_devices=120)
    now = datetime(2026, 6, 13, 12, 0, 0, tzinfo=UTC)
    a = [tx.to_dict() for tx in TransactionGenerator(cfg).stream(50, now=now)]
    b = [tx.to_dict() for tx in TransactionGenerator(cfg).stream(50, now=now)]
    assert a == b


def _pools(gen: TransactionGenerator) -> tuple[list[str], list[str]]:
    return [a.account_id for a in gen._accounts], list(gen._devices)


def test_seeded_pools_independent_of_shared_faker_state() -> None:
    cfg = GeneratorConfig(seed=99, num_accounts=50, num_devices=60)
    first = _pools(TransactionGenerator(cfg))
    Faker.seed(12345)  # perturb the process-wide Faker random state
    Faker().uuid4()
    assert _pools(TransactionGenerator(cfg)) == first


def test_unseeded_generator_after_seeded_one_is_random() -> None:
    small = {"num_accounts": 50, "num_devices": 60}
    unseeded_pools = []
    for _ in range(2):
        TransactionGenerator(GeneratorConfig(seed=1234, **small))
        unseeded_pools.append(_pools(TransactionGenerator(GeneratorConfig(seed=None, **small))))
    a, b = unseeded_pools
    assert a[0] != b[0]
    assert a[1] != b[1]


def test_seeded_generator_does_not_reseed_other_faker_instances() -> None:
    Faker.seed(2024)
    baseline_faker = Faker()
    expected = [baseline_faker.uuid4() for _ in range(5)]

    Faker.seed(2024)
    other = Faker()
    TransactionGenerator(GeneratorConfig(seed=1234, num_accounts=50, num_devices=60))
    assert [other.uuid4() for _ in range(5)] == expected


def test_transaction_ids_are_unique(generator: TransactionGenerator) -> None:
    ids = [tx.transaction_id for tx in generator.stream(2000)]
    assert len(set(ids)) == len(ids)


def test_fraud_rate_within_tolerance() -> None:
    cfg = GeneratorConfig(seed=7, fraud_rate=0.03, num_accounts=500, num_devices=600)
    gen = TransactionGenerator(cfg)
    n = 20000
    frauds = sum(1 for tx in gen.stream(n) if tx.is_fraud_label)
    observed = frauds / n
    # Expect ~3% (looser bound to keep the test stable across platforms).
    assert 0.02 <= observed <= 0.04, observed


def test_late_events_produce_backdated_timestamps() -> None:
    cfg = GeneratorConfig(
        seed=3,
        late_event_rate=1.0,
        late_max_delay_seconds=300,
        num_accounts=50,
        num_devices=60,
    )
    gen = TransactionGenerator(cfg)
    now = datetime(2026, 6, 13, 12, 0, 0, tzinfo=UTC)
    for tx in gen.stream(200, now=now):
        assert datetime.fromisoformat(tx.timestamp) < now


def test_no_late_events_when_rate_zero() -> None:
    cfg = GeneratorConfig(
        seed=3,
        late_event_rate=0.0,
        num_accounts=50,
        num_devices=60,
    )
    gen = TransactionGenerator(cfg)
    now = datetime(2026, 6, 13, 12, 0, 0, tzinfo=UTC)
    for tx in gen.stream(200, now=now):
        assert datetime.fromisoformat(tx.timestamp) == now.replace(microsecond=0)


def test_hourly_weights_table_is_complete() -> None:
    assert len(HOURLY_WEIGHTS) == 24
    assert hourly_weight(25) == HOURLY_WEIGHTS[1]  # wraps modulo 24


def test_invalid_config_rejected() -> None:
    with pytest.raises(ValueError):
        GeneratorConfig(rate=0)
    with pytest.raises(ValueError):
        GeneratorConfig(fraud_rate=2.0)
