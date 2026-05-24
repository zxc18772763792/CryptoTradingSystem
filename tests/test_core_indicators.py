"""Tests for the shared core.indicators package."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.indicators import oscillator_entry_strength, sma_adx, wilder_adx


def _ohlc_frame(n: int = 200, *, seed: int = 0) -> pd.DataFrame:
    """Deterministic random walk OHLC frame."""
    rng = np.random.default_rng(seed)
    close = 100.0 + np.cumsum(rng.normal(0.0, 1.0, n))
    high = close + rng.uniform(0.1, 1.5, n)
    low = close - rng.uniform(0.1, 1.5, n)
    return pd.DataFrame({"high": high, "low": low, "close": close})


def test_wilder_adx_returns_dict_with_expected_keys():
    df = _ohlc_frame()
    out = wilder_adx(df, period=14)
    assert set(out.keys()) == {"plus_di", "minus_di", "adx"}
    # adx is bounded roughly in [0, 100]
    adx_valid = out["adx"].dropna()
    assert (adx_valid >= 0).all()
    assert (adx_valid <= 100).all()


def test_sma_adx_returns_three_series_tuple():
    df = _ohlc_frame()
    adx, plus_di, minus_di = sma_adx(df, period=14)
    assert isinstance(adx, pd.Series)
    assert isinstance(plus_di, pd.Series)
    assert isinstance(minus_di, pd.Series)


def test_wilder_and_sma_produce_different_smoothing():
    """The two flavors must NOT be identical — they use different smoothing."""
    df = _ohlc_frame()
    sma_out, _, _ = sma_adx(df, period=14)
    wilder_out = wilder_adx(df, period=14)["adx"]
    # Compare the last 50 bars where both have warmed up.
    a = sma_out.dropna().tail(50).reset_index(drop=True)
    b = wilder_out.dropna().tail(50).reset_index(drop=True)
    n = min(len(a), len(b))
    assert n > 10
    # Different smoothing -> the series must differ on most bars.
    diffs = (a.iloc[-n:] - b.iloc[-n:]).abs()
    assert (diffs > 1e-6).sum() > n * 0.5


def test_adx_handles_constant_price_without_explosion():
    """A perfectly flat price series should NOT blow up (no NaN propagation crash)."""
    df = pd.DataFrame({"high": [100.0] * 60, "low": [100.0] * 60, "close": [100.0] * 60})
    out = wilder_adx(df, period=14)
    # All-zero DM should not raise. Values may be NaN due to 0/0 division — acceptable.
    assert isinstance(out["adx"], pd.Series)
    sma_adx_out, _, _ = sma_adx(df, period=14)
    assert isinstance(sma_adx_out, pd.Series)


def test_no_plus_dm_self_reference_bug():
    """Regression guard against the historical in-place plus_dm overwrite.

    When down_move > up_move > 0 on the same bar, the comparison must use the
    ORIGINAL up_move, not a truncated plus_dm. Construct a scenario where the
    bug would produce wrong minus_dm.
    """
    df = pd.DataFrame(
        {
            "high": [100.0, 99.0, 99.5, 99.6, 99.7, 99.8],  # mild up
            "low":  [98.0, 90.0, 89.5, 89.4, 89.3, 89.2],   # large drop
            "close":[99.0, 95.0, 95.0, 95.0, 95.0, 95.0],
        }
    )
    # If the bug were present minus_dm would be silently zeroed because
    # plus_dm.where(...) reads the truncated plus_dm against minus_dm.
    out = wilder_adx(df, period=3)
    # minus_di should be > plus_di on the second bar (huge low drop).
    assert (out["minus_di"].iloc[-1] >= out["plus_di"].iloc[-1])


# ── oscillator_entry_strength ──

def test_oscillator_entry_strength_long_floor_and_cap():
    # Slight crossing above threshold => floor.
    s = oscillator_entry_strength(
        current=31.0, previous=29.0, threshold=30.0, direction="long"
    )
    assert s == pytest.approx(0.2 + (50 - 29) / (50 - 30), rel=0.05) or s == 1.0


def test_oscillator_entry_strength_short_mirror():
    # Crossing down from overbought.
    s = oscillator_entry_strength(
        current=69.0, previous=72.0, threshold=70.0, direction="short"
    )
    assert 0.0 <= s <= 1.0
    assert s >= 0.2


def test_oscillator_entry_strength_invalid_direction():
    with pytest.raises(ValueError):
        oscillator_entry_strength(
            current=50, previous=30, threshold=30, direction="up"
        )


def test_oscillator_entry_strength_caps_at_one():
    # Previous deep below neutral.
    s = oscillator_entry_strength(
        current=31.0, previous=5.0, threshold=30.0, direction="long", cap=1.0
    )
    assert s == 1.0


def test_oscillator_entry_strength_cci_scale():
    # CCI lives on roughly [-200, 200], neutral=0. With threshold=-100 (oversold)
    # previous=-180 should produce a fairly strong signal.
    s = oscillator_entry_strength(
        current=-95.0, previous=-180.0, threshold=-100.0, direction="long", neutral=0.0
    )
    assert 0.2 <= s <= 1.0
