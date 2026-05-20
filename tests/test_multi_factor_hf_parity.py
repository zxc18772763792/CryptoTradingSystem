"""Phase 2 parity tests: ``build_multifactor_hf_position_series`` must
match ``_replay_signal_strategy_position(MultiFactorHFStrategy, ...)``
position-for-position on every fixture.

If any fixture drifts, the fast path is unsafe to enable and the trusted
per-bar replay is the source of truth. We test five distinct regimes —
trend, chop, high volatility, low volume, missing volume — each crafted
to exercise different gate paths and state-machine transitions.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from strategies.quantitative.multi_factor_hf import MultiFactorHFStrategy
from strategies.quantitative.multi_factor_hf_fast import (
    build_multifactor_hf_position_series,
)
from web.api.backtest import _replay_signal_strategy_position


N_BARS = 600
LIVE_WINDOW_LIMIT = 200  # max(120, min_length=180 + 20)


def _base_index() -> pd.DatetimeIndex:
    return pd.date_range("2026-01-01", periods=N_BARS, freq="5min")


def _ohlcv_from_close(close: np.ndarray, vol: np.ndarray) -> pd.DataFrame:
    idx = _base_index()
    high = close + np.abs(np.diff(close, prepend=close[0])) + 1.0
    low = close - np.abs(np.diff(close, prepend=close[0])) - 1.0
    open_ = np.concatenate([[close[0]], close[:-1]])
    return pd.DataFrame(
        {
            "open": open_.astype(float),
            "high": high.astype(float),
            "low": low.astype(float),
            "close": close.astype(float),
            "volume": vol.astype(float),
        },
        index=idx,
    )


@pytest.fixture(scope="module")
def fixture_trend() -> pd.DataFrame:
    """Persistent uptrend with mild noise — exercises long entries."""
    rng = np.random.default_rng(42)
    t = np.arange(N_BARS)
    close = 30000.0 + 12.0 * t + 80.0 * rng.standard_normal(N_BARS).cumsum() * 0.05
    vol = 100.0 + 25.0 * rng.random(N_BARS)
    return _ohlcv_from_close(close, vol)


@pytest.fixture(scope="module")
def fixture_chop() -> pd.DataFrame:
    """Mean-reverting oscillation — exercises score-flips and exit_th."""
    t = np.arange(N_BARS)
    close = 30000.0 + 600.0 * np.sin(t / 17.0) + 200.0 * np.sin(t / 5.0)
    vol = 100.0 + 10.0 * np.cos(t / 11.0)
    return _ohlcv_from_close(close, vol)


@pytest.fixture(scope="module")
def fixture_high_vol() -> pd.DataFrame:
    """Heavy realized vol — exercises the rv/atr gate blocks."""
    rng = np.random.default_rng(7)
    base = 30000.0 + 800.0 * rng.standard_normal(N_BARS).cumsum() * 0.4
    close = base
    vol = 200.0 + 100.0 * rng.random(N_BARS)
    return _ohlcv_from_close(close, vol)


@pytest.fixture(scope="module")
def fixture_low_volume() -> pd.DataFrame:
    """Suppressed volume — exercises the volume_z gate."""
    t = np.arange(N_BARS)
    close = 30000.0 + 40.0 * t + 200.0 * np.sin(t / 13.0)
    vol = np.full(N_BARS, 5.0)  # constant low volume → volume_z ≈ 0
    return _ohlcv_from_close(close, vol)


@pytest.fixture(scope="module")
def fixture_missing_volume() -> pd.DataFrame:
    """Volume column with NaN gaps — exercises factor robustness."""
    rng = np.random.default_rng(13)
    t = np.arange(N_BARS)
    close = 30000.0 + 8.0 * t + 150.0 * rng.standard_normal(N_BARS).cumsum() * 0.1
    vol = 100.0 + 20.0 * rng.random(N_BARS)
    # Drop random 10% of volume readings to NaN
    holes = rng.choice(N_BARS, size=int(0.1 * N_BARS), replace=False)
    vol[holes] = np.nan
    return _ohlcv_from_close(close, vol)


_FIXTURES = [
    "fixture_trend",
    "fixture_chop",
    "fixture_high_vol",
    "fixture_low_volume",
    "fixture_missing_volume",
]


@pytest.mark.parametrize("fx_name", _FIXTURES)
def test_fast_path_matches_replay_bar_by_bar(fx_name, request):
    df = request.getfixturevalue(fx_name).copy()
    trusted = _replay_signal_strategy_position(
        MultiFactorHFStrategy,
        df,
        params=None,
        allow_long=True,
        allow_short=True,
        reverse_on_signal=True,
    )
    fast = build_multifactor_hf_position_series(
        df,
        params=None,
        allow_long=True,
        allow_short=True,
        reverse_on_signal=True,
    )
    assert len(fast) == len(trusted), f"{fx_name}: length mismatch"

    # Bar-by-bar exact equality (no tolerance — these are -1/0/+1 ints
    # represented as floats; any drift means the state machine diverged).
    diff_idx = np.flatnonzero(
        pd.to_numeric(fast, errors="coerce").fillna(0.0).values
        != pd.to_numeric(trusted, errors="coerce").fillna(0.0).values
    )
    if diff_idx.size > 0:
        # Helpful failure message: show the first 5 disagreement bars.
        first = diff_idx[:5]
        msg = "\n".join(
            f"  bar {int(i)}: fast={float(fast.iat[int(i)])!r} trusted={float(trusted.iat[int(i)])!r}"
            for i in first
        )
        pytest.fail(
            f"{fx_name}: {diff_idx.size} bar(s) diverged from trusted replay\n"
            f"first diffs:\n{msg}"
        )
