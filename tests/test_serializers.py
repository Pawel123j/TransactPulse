"""Unit tests for serializers and the emission loop (with a fake sink)."""

from __future__ import annotations

import json

from ingestion.config import GeneratorConfig
from ingestion.emitter import run_emitter
from ingestion.generator import TransactionGenerator
from ingestion.schema import Transaction, validate
from ingestion.serializers import JsonSerializer, Serializer


def _sample() -> Transaction:
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
    )


def test_json_serializer_roundtrip() -> None:
    serializer = JsonSerializer()
    payload = serializer.serialize(_sample().to_dict())
    assert isinstance(payload, bytes)
    decoded = json.loads(payload)
    assert validate(decoded) == []
    assert decoded["account_id"] == "ACC-0001"


def test_json_serializer_is_compact() -> None:
    payload = JsonSerializer().serialize(_sample().to_dict())
    assert b", " not in payload  # compact separators, no spaces


def test_json_serializer_satisfies_protocol() -> None:
    assert isinstance(JsonSerializer(), Serializer)


class _CollectingSink:
    """In-memory sink used to test the emitter without Kafka."""

    def __init__(self) -> None:
        self.records: list[Transaction] = []

    def send(self, transaction: Transaction) -> None:
        self.records.append(transaction)

    def flush(self) -> None:  # pragma: no cover - trivial
        pass

    @property
    def delivered(self) -> int:
        return len(self.records)

    @property
    def failed(self) -> int:
        return 0


def test_emitter_respects_stop_after() -> None:
    cfg = GeneratorConfig(
        seed=5, rate=1000, apply_seasonality=False,
        num_accounts=50, num_devices=60,
    )
    sink = _CollectingSink()
    generator = TransactionGenerator(cfg)
    stats = run_emitter(cfg, sink, generator=generator, stop_after=300)
    assert stats.produced == 300
    assert sink.delivered == 300
    assert all(validate(tx.to_dict()) == [] for tx in sink.records)


def test_emitter_stats_account_fraud_and_currency() -> None:
    cfg = GeneratorConfig(
        seed=8, rate=2000, fraud_rate=0.5, apply_seasonality=False,
        num_accounts=80, num_devices=90,
    )
    sink = _CollectingSink()
    stats = run_emitter(cfg, sink, stop_after=400)
    assert stats.fraud > 0
    assert sum(stats.by_currency.values()) == stats.produced == 400
