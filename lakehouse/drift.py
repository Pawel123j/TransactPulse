"""Distribution drift metrics: Population Stability Index (PSI) and KS statistic.

Pure-Python (stdlib only) so they are dependency-light and unit-testable without
Spark/numpy. Computed on the driver over modest samples collected from gold.
"""

from __future__ import annotations

from collections.abc import Sequence

_EPSILON = 1e-6


def _quantile_bin_edges(values: Sequence[float], buckets: int) -> list[float]:
    """Return ``buckets+1`` monotonically increasing quantile edges of ``values``."""
    ordered = sorted(values)
    n = len(ordered)
    if n == 0:
        return []
    edges = [ordered[0]]
    for i in range(1, buckets):
        idx = min(n - 1, int(round(i / buckets * (n - 1))))
        edges.append(ordered[idx])
    edges.append(ordered[-1])
    # Make strictly increasing so digitize-style binning is well defined.
    for i in range(1, len(edges)):
        if edges[i] <= edges[i - 1]:
            edges[i] = edges[i - 1] + _EPSILON
    return edges


def _bucket_shares(values: Sequence[float], edges: list[float]) -> list[float]:
    """Fraction of ``values`` falling in each bucket defined by ``edges``."""
    buckets = len(edges) - 1
    counts = [0] * buckets
    for v in values:
        placed = False
        for b in range(buckets):
            upper = edges[b + 1]
            if v <= upper or b == buckets - 1:
                counts[b] += 1
                placed = True
                break
        if not placed:  # pragma: no cover - defensive
            counts[-1] += 1
    total = len(values) or 1
    return [c / total for c in counts]


def population_stability_index(
    expected: Sequence[float], actual: Sequence[float], buckets: int = 10
) -> float:
    """Compute PSI of ``actual`` vs the ``expected`` (reference) distribution.

    PSI = Σ (a_i − e_i) · ln(a_i / e_i) over quantile buckets of ``expected``.
    Returns ``0.0`` if either input is empty.
    """
    if not expected or not actual:
        return 0.0
    edges = _quantile_bin_edges(expected, buckets)
    e_shares = _bucket_shares(expected, edges)
    a_shares = _bucket_shares(actual, edges)

    psi = 0.0
    for e, a in zip(e_shares, a_shares, strict=True):
        e_adj = max(e, _EPSILON)
        a_adj = max(a, _EPSILON)
        psi += (a_adj - e_adj) * _log(a_adj / e_adj)
    return psi


def ks_statistic(sample_a: Sequence[float], sample_b: Sequence[float]) -> float:
    """Two-sample Kolmogorov–Smirnov statistic (max abs CDF difference)."""
    if not sample_a or not sample_b:
        return 0.0
    a = sorted(sample_a)
    b = sorted(sample_b)
    grid = sorted(set(a) | set(b))
    na, nb = len(a), len(b)

    def cdf(ordered: list[float], n: int, x: float) -> float:
        # Count of values <= x via binary search.
        lo, hi = 0, n
        while lo < hi:
            mid = (lo + hi) // 2
            if ordered[mid] <= x:
                lo = mid + 1
            else:
                hi = mid
        return lo / n

    return max(abs(cdf(a, na, x) - cdf(b, nb, x)) for x in grid)


def classify_psi(psi: float) -> str:
    """Bucket a PSI value into a human-readable stability band."""
    if psi < 0.1:
        return "stable"
    if psi < 0.2:
        return "moderate"
    return "significant"


def _log(x: float) -> float:
    import math

    return math.log(x)
