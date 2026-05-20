"""Backtest-page vs live-runtime consistency.

After Phase 2 the backtest page builds positions by replaying the real
strategy class (``_build_backtest_position_series`` → ``_replay_signal_
strategy_position``), so it runs the same code that trades live.

Tests in this file:
  1. Broad guard: every research-supported strategy runs through the
     backtest page's actual position builder without raising and yields a
     well-formed position series.
  2. Wiring correctness: when ``BACKTEST_USE_REAL_STRATEGY`` is on (the
     default), the backtest page output is identical to a direct
     ``_replay_signal_strategy_position`` call. Catches anyone routing the
     page back to the legacy vectorized model.
  3. Legacy diagnostic: directional agreement between the *legacy*
     ``_build_positions`` vectorized model and the real strategy class.
     Strategies known to diverge are tracked in ``_KNOWN_DIVERGENCE`` with
     a measured baseline; any new strategy falling below the floor without
     being declared fails the test (regression protection on the legacy
     fallback, which still exists for emergency rollback).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import strategies as strategy_pkg
from config.strategy_registry import STRATEGY_REGISTRY
from core.research.strategy_research import RESEARCH_SUPPORTED_STRATEGIES
from web.api.backtest import (
    _build_backtest_position_series,
    _build_positions,
    _replay_signal_strategy_position,
    _resolve_backtest_trade_policy,
)

# Strategies whose vectorized fallback is expected to track the class logic
# closely; below the legacy floor without being known-diverged is a failure.
_CURATED = ["RSIStrategy", "BollingerBandsStrategy", "MACDStrategy", "MomentumStrategy"]
_LEGACY_DIRECTION_FLOOR = 0.85

# Strategies that cannot run on a single-symbol synthetic frame (need a second
# leg, a trained model, or a cross-sectional universe). Excluded from the
# broad "runs without error" guard — not a product defect.
_SKIP_SINGLE_SYMBOL = {
    "PairsTradingStrategy",
    "MLXGBoostStrategy",
}

# Known divergences of the LEGACY vectorized ``_build_positions`` model from
# the real strategy class. Now that the backtest page runs the real class by
# default, these only matter when ``BACKTEST_USE_REAL_STRATEGY`` is disabled
# for emergency rollback. The numbers are the measured directional agreement
# on the synthetic series in this file — drops vs that baseline are flagged.
_KNOWN_DIVERGENCE: dict[str, float] = {
    "RSIStrategy": 0.51,
    "BollingerBandsStrategy": 0.70,
    "MACDStrategy": 0.46,
    "MomentumStrategy": 0.64,
}
_BASELINE_TOLERANCE = 0.05  # allow ±5% drift around the recorded baseline


def _defaults(name: str) -> dict:
    return dict((STRATEGY_REGISTRY.get(name, {}) or {}).get("defaults", {}) or {})


@pytest.fixture(scope="module")
def synthetic_ohlcv() -> pd.DataFrame:
    """Deterministic trend + oscillation series on a tz-naive UTC index."""
    n = 420
    idx = pd.date_range("2026-01-01", periods=n, freq="15min")  # tz-naive UTC
    t = np.arange(n)
    trend = 30000 + 40.0 * t
    wave = 900.0 * np.sin(t / 11.0) + 350.0 * np.sin(t / 3.0)
    close = trend + wave
    high = close + 120.0
    low = close - 120.0
    open_ = np.concatenate([[close[0]], close[:-1]])
    vol = 100.0 + 25.0 * np.abs(np.sin(t / 5.0))
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol},
        index=idx,
    )


@pytest.mark.parametrize(
    "name", [s for s in RESEARCH_SUPPORTED_STRATEGIES if s not in _SKIP_SINGLE_SYMBOL]
)
def test_backtest_page_position_builder_runs_without_error(
    name: str, synthetic_ohlcv: pd.DataFrame
):
    pos = _build_backtest_position_series(
        name, synthetic_ohlcv.copy(), params=_defaults(name)
    )
    assert isinstance(pos, pd.Series)
    assert len(pos) == len(synthetic_ohlcv)
    coerced = pd.to_numeric(pos, errors="coerce")
    assert coerced.notna().all(), f"{name} produced non-numeric positions"
    assert np.isfinite(coerced.to_numpy()).all(), f"{name} produced non-finite positions"


@pytest.mark.parametrize("name", _CURATED)
def test_backtest_page_uses_real_strategy_path(
    name: str, synthetic_ohlcv: pd.DataFrame
):
    """Backtest page must equal a direct replay of the real strategy class
    so the page's numbers describe what trades in production."""
    cls = getattr(strategy_pkg, name)
    params = _defaults(name)
    allow_long, allow_short, reverse_on_signal = _resolve_backtest_trade_policy(
        name, params=params
    )
    page_pos = _build_backtest_position_series(name, synthetic_ohlcv.copy(), params=dict(params))
    real_pos = _replay_signal_strategy_position(
        cls,
        synthetic_ohlcv.copy(),
        params=dict(params),
        allow_long=allow_long,
        allow_short=allow_short,
        reverse_on_signal=reverse_on_signal,
    )
    pd.testing.assert_series_equal(
        pd.Series(pd.to_numeric(page_pos, errors="coerce").fillna(0.0).values),
        pd.Series(pd.to_numeric(real_pos, errors="coerce").fillna(0.0).values),
        check_names=False,
    )


def _direction_agreement(a: pd.Series, b: pd.Series) -> float:
    sa = np.sign(pd.to_numeric(a, errors="coerce").fillna(0.0).to_numpy())
    sb = np.sign(pd.to_numeric(b, errors="coerce").fillna(0.0).to_numpy())
    return float(np.mean(sa == sb))


@pytest.mark.parametrize("name", _CURATED)
def test_legacy_vectorized_model_baseline(
    name: str, synthetic_ohlcv: pd.DataFrame, capsys
):
    """Lock in legacy ``_build_positions`` directional agreement vs the real
    class. New regressions in the vectorized model (or a strategy newly
    falling below the floor without being declared) fail the test."""
    cls = getattr(strategy_pkg, name)
    params = _defaults(name)
    legacy = _build_positions(name, synthetic_ohlcv.copy(), params=dict(params))
    real = _replay_signal_strategy_position(
        cls,
        synthetic_ohlcv.copy(),
        params=dict(params),
        allow_long=True,
        allow_short=True,
        reverse_on_signal=False,
    )
    agreement = _direction_agreement(legacy, real)
    with capsys.disabled():
        print(f"\n[legacy vs real] {name}: {agreement:.1%}")

    if name in _KNOWN_DIVERGENCE:
        baseline = _KNOWN_DIVERGENCE[name]
        assert agreement >= baseline - _BASELINE_TOLERANCE, (
            f"{name}: legacy/real agreement {agreement:.1%} dropped below "
            f"recorded baseline {baseline:.1%} (±{_BASELINE_TOLERANCE:.0%}); "
            f"legacy vectorized model regressed further"
        )
    else:
        assert agreement >= _LEGACY_DIRECTION_FLOOR, (
            f"{name}: legacy/real agreement {agreement:.1%} below floor "
            f"{_LEGACY_DIRECTION_FLOOR:.0%} and not in _KNOWN_DIVERGENCE; "
            f"either add to known list or reconcile the vectorized model"
        )
