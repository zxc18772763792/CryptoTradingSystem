"""Tests for the backtest-scoped factor cache (core/factors_ts/cache.py).

The cache exists to remove per-bar factor recomputation, which profiling showed
was 81% of backtest wall time. Its correctness contract is narrow and worth
pinning down: it must return exactly what the uncached path would, and it must
refuse to serve any factor whose windowed value does not match the full-series
slice (EWM/cumsum style path dependence).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.factors_ts.cache import factor_cache_scope, scope_stats
from core.factors_ts.registry import _compute_uncached, compute_factor


def _ohlcv(n: int = 600, seed: int = 3) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="5min", tz="UTC")
    rng = np.random.default_rng(seed)
    close = 30000 * np.exp(np.cumsum(rng.normal(0, 0.002, n)))
    high = close * (1 + np.abs(rng.normal(0, 0.001, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.001, n)))
    return pd.DataFrame(
        {
            "open": np.concatenate([[close[0]], close[:-1]]),
            "high": high,
            "low": low,
            "close": close,
            "volume": rng.lognormal(3, 0.4, n),
        },
        index=idx,
    )


ROLLING_FACTORS = [
    ("atr_pct", {}),
    ("spread_proxy", {}),
    ("volume_z", {}),
    ("zscore_price", {"lookback": 30}),
    ("realized_vol", {"lookback": 60}),
]


def test_cache_is_inert_outside_scope():
    df = _ohlcv()
    window = df.iloc[100:300]
    assert scope_stats() is None
    got = compute_factor("atr_pct", window)
    expected = _compute_uncached("atr_pct", window)
    pd.testing.assert_series_equal(got, expected)


@pytest.mark.parametrize("name,params", ROLLING_FACTORS)
def test_cached_values_match_uncached(name, params):
    """Inside a scope, every window must yield the uncached value."""
    df = _ohlcv()
    window_len = 200
    with factor_cache_scope(df):
        for end in (250, 300, 420, 599):
            window = df.iloc[end - window_len: end]
            cached = compute_factor(name, window, params=params)
            expected = _compute_uncached(name, window, params=params)
            last_c = float(cached.iloc[-1])
            last_e = float(expected.iloc[-1])
            if np.isnan(last_e):
                assert np.isnan(last_c)
            else:
                assert last_c == pytest.approx(last_e, rel=1e-9, abs=1e-12)


def test_scope_actually_serves_from_cache():
    """Repeated calls must be served from a small number of full-series passes."""
    df = _ohlcv()
    with factor_cache_scope(df):
        for end in range(300, 400):
            window = df.iloc[end - 200: end]
            compute_factor("atr_pct", window)
        stats = scope_stats()
        assert stats is not None
        assert stats["served"] > 50, stats
        # One full-series computation for the single factor used here.
        assert stats["full_computations"] == 1, stats
        assert "atr_pct" in stats["verified"], stats


def test_window_from_a_different_frame_is_not_served():
    """A window that is not a slice of the scope frame must fall through."""
    df = _ohlcv(seed=3)
    other = _ohlcv(seed=99)
    with factor_cache_scope(df):
        window = other.iloc[100:300]
        got = compute_factor("atr_pct", window)
        expected = _compute_uncached("atr_pct", window)
        pd.testing.assert_series_equal(got, expected)
        stats = scope_stats()
        assert stats["served"] == 0, stats


def test_obv_is_cacheable_because_its_cumsum_offset_cancels():
    """OBV uses cumsum but is returned as a rolling z-score, so it IS window-safe.

    Guards against someone "fixing" the cache by denylisting cumsum factors: the
    constant offset between a window's cumsum and the full series' cumsum cancels
    in (obv - obv_ma), so the value is genuinely window-invariant here. This is
    why acceptance is decided by measurement rather than by a hand-kept list.
    """
    df = _ohlcv()
    with factor_cache_scope(df):
        for end in (300, 350, 420):
            window = df.iloc[end - 200: end]
            got = compute_factor("obv", window)
            expected = _compute_uncached("obv", window)
            assert float(got.iloc[-1]) == pytest.approx(float(expected.iloc[-1]), rel=1e-9)
        assert "obv" in scope_stats()["verified"]


def test_path_dependent_factor_is_rejected_not_silently_wrong():
    """A genuinely path-dependent factor must be rejected and fall back.

    Exercises the rejection mechanism directly with a raw cumulative sum, whose
    value at a timestamp really does depend on where the frame starts.
    """
    from core.factors_ts.cache import try_cached

    def path_dependent(name, frame, params):
        return pd.to_numeric(frame["volume"], errors="coerce").cumsum()

    df = _ohlcv()
    with factor_cache_scope(df):
        for end in (300, 350, 420):
            window = df.iloc[end - 200: end]
            got = try_cached("fake_cumsum", window, None, path_dependent)
            honest = path_dependent("fake_cumsum", window, None)
            if got is not None:
                # Never allowed to hand back a cached value that disagrees.
                assert float(got.iloc[-1]) == pytest.approx(float(honest.iloc[-1]), rel=1e-9)
        stats = scope_stats()
        assert "fake_cumsum" in stats["rejected"], stats
        assert "fake_cumsum" not in stats["verified"], stats


def test_nested_scope_does_not_replace_outer_frame():
    df = _ohlcv()
    other = _ohlcv(seed=42)
    with factor_cache_scope(df):
        with factor_cache_scope(other):
            # Windows of the OUTER frame must still be servable inside the nested
            # scope; the nested one is ignored rather than rebinding the frame.
            window = df.iloc[100:300]
            got = compute_factor("atr_pct", window)
            expected = _compute_uncached("atr_pct", window)
            assert float(got.iloc[-1]) == pytest.approx(float(expected.iloc[-1]), rel=1e-9)
        assert scope_stats() is not None
