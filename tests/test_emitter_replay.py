"""Contract: transaction ids when the same batch is emitted again via run_emitter.

* Same ``seed`` and same generator config -> the replay carries the **same ids**
  (and the same non-time fields), so silver's MERGE on ``transaction_id``
  updates rows instead of inserting duplicates.
* No ``seed`` -> every run draws fresh ids, so a replay is **new transactions**.

``timestamp`` is excluded on purpose: it is taken from the emitter's wall clock,
so it legitimately differs between runs.
"""

from __future__ import annotations

import pytest

from ingestion.config import GeneratorConfig
from ingestion.emitter import run_emitter
from ingestion.schema import Transaction

BATCH = 5


class _CollectingSink:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def send(self, transaction: Transaction) -> None:
        self.records.append(transaction.to_dict())

    def flush(self) -> None:  # pragma: no cover - trivial
        pass

    @property
    def delivered(self) -> int:
        return len(self.records)

    @property
    def failed(self) -> int:
        return 0


def _emit(seed: int | None, *, batch: int = BATCH, **overrides: object) -> list[dict]:
    params: dict = {
        "seed": seed,
        "rate": 1000,
        "apply_seasonality": False,
        "num_accounts": 20,
        "num_devices": 25,
    }
    cfg = GeneratorConfig(**{**params, **overrides})
    sink = _CollectingSink()
    run_emitter(cfg, sink, stop_after=batch)
    return sink.records


def _ids(records: list[dict]) -> list[str]:
    return [r["transaction_id"] for r in records]


def _without_timestamp(records: list[dict]) -> list[dict]:
    return [{k: v for k, v in r.items() if k != "timestamp"} for r in records]


def test_replay_with_same_seed_reuses_transaction_ids() -> None:
    first, replay = _emit(seed=42), _emit(seed=42)
    assert [r["transaction_id"] for r in replay] == [r["transaction_id"] for r in first]
    assert len({r["transaction_id"] for r in first}) == BATCH
    assert _without_timestamp(replay) == _without_timestamp(first)


def test_replay_without_seed_draws_new_transaction_ids() -> None:
    first, replay = _emit(seed=None), _emit(seed=None)
    assert not {r["transaction_id"] for r in first} & {r["transaction_id"] for r in replay}


def test_replay_with_seed_zero_is_deterministic() -> None:
    # 0 is falsy: guards against an ``if config.seed:`` style regression in the
    # id RNG *and* in the Faker-built account/device pools.
    first, replay = _emit(seed=0), _emit(seed=0)
    assert _without_timestamp(replay) == _without_timestamp(first)
    assert _ids(_emit(seed=0)) != _ids(_emit(seed=None))


def test_replay_ids_do_not_depend_on_tick_split(monkeypatch: pytest.MonkeyPatch) -> None:
    # rate=2 spreads the batch over several 1 s ticks; skip the real sleeps.
    monkeypatch.setattr("ingestion.emitter.time.sleep", lambda _s: None)
    one_tick = _ids(_emit(seed=42))
    assert _ids(_emit(seed=42, rate=2)) == one_tick
    assert _ids(_emit(seed=42, burst_enabled=True)) == one_tick


def test_shorter_replay_is_a_prefix() -> None:
    assert _ids(_emit(seed=42, batch=2)) == _ids(_emit(seed=42))[:2]
