"""Pure-Python unit tests for drift metrics (PSI / KS)."""

from __future__ import annotations

import random

from lakehouse.drift import (
    classify_psi,
    ks_statistic,
    population_stability_index,
)


def test_psi_zero_for_identical_distributions() -> None:
    rng = random.Random(1)
    data = [rng.gauss(0, 1) for _ in range(5000)]
    psi = population_stability_index(data, list(data))
    assert psi < 1e-6


def test_psi_small_for_same_distribution_different_sample() -> None:
    rng = random.Random(2)
    expected = [rng.gauss(0, 1) for _ in range(5000)]
    actual = [rng.gauss(0, 1) for _ in range(5000)]
    psi = population_stability_index(expected, actual)
    assert psi < 0.1  # "stable"
    assert classify_psi(psi) == "stable"


def test_psi_large_for_shifted_distribution() -> None:
    rng = random.Random(3)
    expected = [rng.gauss(0, 1) for _ in range(5000)]
    actual = [rng.gauss(3, 1) for _ in range(5000)]  # mean shift
    psi = population_stability_index(expected, actual)
    assert psi > 0.25
    assert classify_psi(psi) == "significant"


def test_psi_empty_inputs_return_zero() -> None:
    assert population_stability_index([], [1.0, 2.0]) == 0.0
    assert population_stability_index([1.0], []) == 0.0


def test_ks_zero_for_identical_samples() -> None:
    data = [float(i) for i in range(100)]
    assert ks_statistic(data, list(data)) == 0.0


def test_ks_one_for_disjoint_samples() -> None:
    a = [float(i) for i in range(100)]
    b = [float(i) for i in range(1000, 1100)]
    assert ks_statistic(a, b) == 1.0


def test_ks_detects_partial_shift() -> None:
    rng = random.Random(4)
    a = [rng.gauss(0, 1) for _ in range(2000)]
    b = [rng.gauss(1, 1) for _ in range(2000)]
    ks = ks_statistic(a, b)
    assert 0.2 < ks < 1.0


def test_classify_psi_bands() -> None:
    assert classify_psi(0.05) == "stable"
    assert classify_psi(0.15) == "moderate"
    assert classify_psi(0.30) == "significant"
