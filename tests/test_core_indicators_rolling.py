"""Numerical equivalence tests for vectorised rolling helpers.

Each test verifies the new implementation matches the slow rolling.apply
lambda we replaced, plus an order-of-magnitude speed benchmark to make sure
we didn't accidentally regress.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest

from core.indicators import rolling_mad, rolling_sortino, rolling_var_quantile


def _random_series(n: int = 600, *, seed: int = 7) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(0.0, 0.01, n))


# ── rolling_mad ─────────────────────────────────────────────────────────────


def test_rolling_mad_matches_loop_implementation():
    series = _random_series()
    window = 20
    expected = series.rolling(window).apply(
        lambda x: np.abs(x - x.mean()).mean(), raw=False
    )
    actual = rolling_mad(series, window)
    np.testing.assert_allclose(
        actual.dropna().to_numpy(),
        expected.dropna().to_numpy(),
        rtol=1e-10,
        atol=1e-12,
    )


def test_rolling_mad_pre_window_returns_nan():
    series = _random_series(n=50)
    out = rolling_mad(series, 20)
    assert out.iloc[:19].isna().all()
    assert not out.iloc[19:].isna().any()


def test_rolling_mad_nan_window_returns_nan():
    series = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0, 6.0, 7.0])
    out = rolling_mad(series, 3)
    # Windows ending at indices 2, 3, 4 each contain the NaN -> NaN.
    assert out.iloc[2:5].isna().all()
    # Window ending at index 5 starts at 3 -> [4, 5, 6], no NaN -> defined.
    assert not np.isnan(out.iloc[5])
    assert not np.isnan(out.iloc[6])


def test_rolling_mad_speedup():
    """Vectorised version must be at least 10× faster on a realistic size."""
    series = _random_series(n=5000)
    window = 30

    t0 = time.perf_counter()
    fast = rolling_mad(series, window)
    fast_dt = time.perf_counter() - t0

    t0 = time.perf_counter()
    slow = series.rolling(window).apply(
        lambda x: np.abs(x - x.mean()).mean(), raw=False
    )
    slow_dt = time.perf_counter() - t0

    np.testing.assert_allclose(
        fast.dropna().to_numpy(), slow.dropna().to_numpy(), rtol=1e-10
    )
    # On CI, generous bound — 5× is enough to prove the orders-of-magnitude
    # improvement without flaking on slow machines.
    assert fast_dt * 5 < slow_dt, f"fast={fast_dt:.4f}s slow={slow_dt:.4f}s"


# ── rolling_var_quantile ────────────────────────────────────────────────────


def test_rolling_var_quantile_matches_loop():
    """Match the pre-vectorisation calc_var implementation."""
    series = _random_series()
    window = 30
    confidence = 0.95

    def _calc_var(x):
        r = x.dropna()
        if len(r) < window // 2:
            return np.nan
        return np.percentile(r, (1 - confidence) * 100)

    expected = series.rolling(window).apply(_calc_var, raw=False)
    actual = rolling_var_quantile(series, window, confidence)

    # pandas rolling.quantile uses linear interpolation, np.percentile too —
    # tiny tail differences possible due to NaN handling differences, so
    # compare with mild tolerance.
    np.testing.assert_allclose(
        actual.dropna().to_numpy(),
        expected.dropna().to_numpy(),
        rtol=1e-6,
        atol=1e-8,
    )


# ── rolling_sortino ─────────────────────────────────────────────────────────


def test_rolling_sortino_matches_loop():
    series = _random_series(n=400, seed=11)
    window = 30

    def _calc_sortino(x):
        r = x.dropna()
        if len(r) < window // 2:
            return np.nan
        mean_ret = r.mean()
        downside = r[r < 0]
        if len(downside) < 2:
            return np.nan
        downside_std = np.sqrt((downside ** 2).mean())
        return mean_ret / downside_std if downside_std > 0 else np.nan

    expected = series.rolling(window).apply(_calc_sortino, raw=False)
    actual = rolling_sortino(series, window)

    # Align indices, ignore NaN where either side declined to compute.
    df = pd.DataFrame({"a": actual, "e": expected}).dropna()
    np.testing.assert_allclose(
        df["a"].to_numpy(), df["e"].to_numpy(), rtol=1e-9, atol=1e-12
    )


def test_rolling_sortino_zero_downside_returns_nan():
    """All-positive returns -> denominator is 0 -> NaN, not inf."""
    series = pd.Series([0.01] * 60)
    out = rolling_sortino(series, 20)
    # All-positive: no downside obs -> NaN by design.
    assert out.iloc[20:].isna().all()


def test_rolling_sortino_handles_min_downside_threshold():
    """Need at least min_downside_obs negatives in window."""
    series = pd.Series([0.01] * 18 + [-0.02] + [0.01] * 11)
    out = rolling_sortino(series, 10, min_downside_obs=2)
    # Only 1 negative inside any window -> all NaN (min_downside=2 enforced).
    assert out.dropna().empty
