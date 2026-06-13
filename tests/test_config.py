"""Unit tests for environment-driven configuration."""

from __future__ import annotations

import pytest

from ingestion.config import GeneratorConfig, KafkaConfig


def test_generator_from_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure a clean environment so we exercise the field defaults.
    for var in (
        "GEN_RATE", "GEN_DURATION_SECONDS", "GEN_FRAUD_RATE", "GEN_SEED",
        "GEN_BURST_ENABLED", "GEN_APPLY_SEASONALITY",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = GeneratorConfig.from_env()
    assert cfg.rate == 20.0
    assert cfg.fraud_rate == 0.003
    assert cfg.apply_seasonality is True
    assert cfg.seed is None


def test_generator_from_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEN_RATE", "100")
    monkeypatch.setenv("GEN_FRAUD_RATE", "0.01")
    monkeypatch.setenv("GEN_BURST_ENABLED", "true")
    monkeypatch.setenv("GEN_SEED", "123")
    cfg = GeneratorConfig.from_env()
    assert cfg.rate == 100.0
    assert cfg.fraud_rate == 0.01
    assert cfg.burst_enabled is True
    assert cfg.seed == 123


def test_kafka_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "custom.topic")
    cfg = KafkaConfig.from_env()
    assert cfg.bootstrap_servers == "broker:9092"
    assert cfg.topic == "custom.topic"
    conf = cfg.to_librdkafka()
    assert conf["bootstrap.servers"] == "broker:9092"
    assert conf["enable.idempotence"] is True


def test_kafka_to_librdkafka_merges_extra() -> None:
    cfg = KafkaConfig(extra={"debug": "broker"})
    assert cfg.to_librdkafka()["debug"] == "broker"
