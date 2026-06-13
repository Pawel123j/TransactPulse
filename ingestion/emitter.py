"""Rate-controlled emission loop tying the generator to a sink.

Handles throughput shaping (base rate, hourly seasonality, burst windows) and
collects run statistics. Kept separate from the CLI so it is unit-testable.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ingestion.config import GeneratorConfig
from ingestion.generator import TransactionGenerator, hourly_weight
from ingestion.producer import Sink

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RunStats:
    """Aggregate counters for an emission run."""

    produced: int = 0
    fraud: int = 0
    late: int = 0
    by_currency: dict[str, int] = field(default_factory=dict)
    started_at: float = 0.0
    finished_at: float = 0.0

    @property
    def elapsed_seconds(self) -> float:
        end = self.finished_at or time.monotonic()
        return max(end - self.started_at, 1e-9)

    @property
    def effective_rate(self) -> float:
        return self.produced / self.elapsed_seconds

    def as_dict(self) -> dict[str, object]:
        return {
            "produced": self.produced,
            "fraud": self.fraud,
            "late": self.late,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "effective_rate": round(self.effective_rate, 2),
            "by_currency": dict(sorted(self.by_currency.items())),
        }


def _burst_multiplier(config: GeneratorConfig, elapsed: float) -> float:
    """Return the burst rate multiplier active at ``elapsed`` seconds into the run."""
    if not config.burst_enabled:
        return 1.0
    phase = elapsed % config.burst_interval_seconds
    return config.burst_factor if phase < config.burst_duration_seconds else 1.0


def run_emitter(
    config: GeneratorConfig,
    sink: Sink,
    *,
    generator: TransactionGenerator | None = None,
    stop_after: int | None = None,
) -> RunStats:
    """Emit transactions to ``sink`` under the configured rate shape.

    The loop ticks once per second, computing the number of records to emit from
    the base rate, the current hour's seasonality weight and any active burst.
    Fractional remainders are carried across ticks so the long-run rate is exact.

    Args:
        config: Generation/emission parameters.
        sink: Destination implementing the :class:`~ingestion.producer.Sink` protocol.
        generator: Optional pre-built generator (defaults to a fresh one).
        stop_after: Optional hard cap on the number of records (for tests/smoke runs).

    Returns:
        A :class:`RunStats` summary of the run.
    """
    gen = generator or TransactionGenerator(config)
    stats = RunStats(started_at=time.monotonic())
    carry = 0.0

    try:
        while True:
            tick_start = time.monotonic()
            elapsed = tick_start - stats.started_at

            if config.duration_seconds and elapsed >= config.duration_seconds:
                break

            now = datetime.now(tz=UTC)
            weight = hourly_weight(now.hour) if config.apply_seasonality else 1.0
            target = config.rate * weight * _burst_multiplier(config, elapsed)

            carry += target
            to_emit = int(carry)
            carry -= to_emit

            for _ in range(to_emit):
                if stop_after is not None and stats.produced >= stop_after:
                    return _finalize(stats)
                tx = gen.generate_one(now)
                sink.send(tx)
                _account(stats, tx, now)

            # Sleep the remainder of the 1-second tick.
            sleep_for = 1.0 - (time.monotonic() - tick_start)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        logger.info("interrupted; flushing...")

    return _finalize(stats)


def _account(stats: RunStats, tx, now: datetime) -> None:
    stats.produced += 1
    if tx.is_fraud_label:
        stats.fraud += 1
    # An event whose timestamp precedes the tick's wall clock is late-arriving.
    if datetime.fromisoformat(tx.timestamp) < now.replace(microsecond=0):
        stats.late += 1
    stats.by_currency[tx.currency] = stats.by_currency.get(tx.currency, 0) + 1


def _finalize(stats: RunStats) -> RunStats:
    stats.finished_at = time.monotonic()
    return stats
