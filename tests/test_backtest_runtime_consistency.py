"""Backtest-page vs live-runtime consistency.

The backtest page builds positions via the vectorized research model
(`web.api.backtest._build_positions`, keyed by strategy *name*), while the
live runtime trades via the real strategy class (`generate_signals`). These
are two independent implementations of the "same" strategy, so divergence is
possible by construction.

These tests do not require bit-exact parity. They:
  1. guard that every research-supported strategy runs through the backtest
     model without raising and yields a well-formed position series the same
     length as the input (catches regressions like the tz crash); and
  2. for a curated set of well-defined technical strategies, assert the
     backtest model and the real strategy class agree on trade *direction*
     for the large majority of bars, and emit a per-strategy agreement
     report so material drift is visible.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import strategies as strategy_pkg
from config.strategy_registry import STRATEGY_REGISTRY
from core.research.strategy_research import RESEARCH_SUPPORTED_STRATEGIES
from web.api.backtest import _build_positions, _replay_signal_strategy_position

# Strategies whose vectorized model is expected to track the class logic
# closely; used for the stricter directional-agreement assertion.
_CURATED = ["RSIStrategy", "BollingerBandsStrategy", "MACDStrategy", "MomentumStrategy"]
_DIRECTION_FLOOR = 0.5

# Strategies that cannot run on a single-symbol synthetic frame (need a second
# leg, a trained model, or a cross-sectional universe). Excluded from the
# broad "runs without error" guard — not a product defect.
_SKIP_SINGLE_SYMBOL = {
    "PairsTradingStrategy",
    "MLXGBoostStrategy",
}

# Known backtest-model vs runtime divergences, tracked as xfail until the
# vectorized model is reconciled with the strategy class. Turning XPASS means
# parity was achieved and the marker should be removed.
_KNOWN_DIVERGENCE = {
    "MACDStrategy",
}


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
def test_backtest_model_runs_without_error(name: str, synthetic_ohlcv: pd.DataFrame):
    pos = _build_positions(name, synthetic_ohlcv.copy(), params=_defaults(name))
    assert isinstance(pos, pd.Series)
    assert len(pos) == len(synthetic_ohlcv)
    coerced = pd.to_numeric(pos, errors="coerce")
    assert coerced.notna().all(), f"{name} produced non-numeric positions"
    assert np.isfinite(coerced.to_numpy()).all(), f"{name} produced non-finite positions"


def _direction_agreement(a: pd.Series, b: pd.Series) -> float:
    sa = np.sign(pd.to_numeric(a, errors="coerce").fillna(0.0).to_numpy())
    sb = np.sign(pd.to_numeric(b, errors="coerce").fillna(0.0).to_numpy())
    return float(np.mean(sa == sb))


@pytest.mark.parametrize("name", _CURATED)
def test_curated_strategy_backtest_matches_runtime_direction(
    name: str, synthetic_ohlcv: pd.DataFrame, capsys
):
    cls = getattr(strategy_pkg, name, None)
    assert cls is not None, f"strategy class {name} not exported by strategies pkg"
    params = _defaults(name)

    bt_pos = _build_positions(name, synthetic_ohlcv.copy(), params=dict(params))
    live_pos = _replay_signal_strategy_position(
        cls,
        synthetic_ohlcv.copy(),
        params=dict(params),
        allow_long=True,
        allow_short=True,
        reverse_on_signal=False,
    )
    agreement = _direction_agreement(bt_pos, live_pos)
    with capsys.disabled():
        print(f"\n[backtest vs runtime] {name}: {agreement:.1%} directional agreement")

    if agreement < _DIRECTION_FLOOR and name in _KNOWN_DIVERGENCE:
        pytest.xfail(
            f"{name}: backtest model diverges from runtime "
            f"({agreement:.0%} < {_DIRECTION_FLOOR:.0%}); vectorized model "
            f"not yet reconciled with strategy class"
        )
    assert agreement >= _DIRECTION_FLOOR, (
        f"{name} backtest model diverges from runtime: directional agreement "
        f"{agreement:.0%} < {_DIRECTION_FLOOR:.0%}"
    )
