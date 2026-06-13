"""Tests for ML feature engineering (pure-Python + pandas)."""

from __future__ import annotations

import pytest

from lakehouse.ml.features import FEATURE_NAMES, build_features

pd = pytest.importorskip("pandas")


def _record(**over) -> dict:
    base = {
        "event_time": "2026-06-13T14:00:00+00:00",
        "amount_pln": 100.0,
        "channel": "POS",
        "merchant_category": "grocery",
        "country": "PL",
    }
    base.update(over)
    return base


def test_build_features_keys_match_feature_names() -> None:
    feats = build_features(_record())
    assert tuple(feats.keys()) == FEATURE_NAMES


def test_benign_record_flags_are_zero() -> None:
    feats = build_features(_record())
    assert feats["is_online"] == 0.0
    assert feats["is_foreign"] == 0.0
    assert feats["is_high_risk_category"] == 0.0
    assert feats["is_night"] == 0.0


def test_risky_record_flags_are_set() -> None:
    feats = build_features(
        _record(
            event_time="2026-06-13T03:00:00+00:00",
            channel="ONLINE",
            merchant_category="gambling",
            country="US",
        )
    )
    assert feats["is_online"] == 1.0
    assert feats["is_foreign"] == 1.0
    assert feats["is_high_risk_category"] == 1.0
    assert feats["is_night"] == 1.0
    assert feats["hour"] == 3.0


def test_log_amount_is_monotonic() -> None:
    low = build_features(_record(amount_pln=10.0))["log_amount_pln"]
    high = build_features(_record(amount_pln=10000.0))["log_amount_pln"]
    assert high > low


def test_pure_and_vectorized_features_agree() -> None:
    records = [
        _record(),
        _record(channel="ONLINE", country="DE", amount_pln=2500.0),
        _record(event_time="2026-06-13T23:30:00+00:00", merchant_category="cash_withdrawal"),
    ]
    from lakehouse.ml.features import features_frame

    vectorized = features_frame(pd.DataFrame(records)).reset_index(drop=True)
    for i, rec in enumerate(records):
        pure = build_features(rec)
        for name in FEATURE_NAMES:
            assert vectorized.loc[i, name] == pytest.approx(pure[name]), (name, i)
