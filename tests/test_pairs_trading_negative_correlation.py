from __future__ import annotations

import numpy as np
import pandas as pd

from core.strategies.strategy_base import SignalType
from strategies.quantitative.pairs_trading import PairsTradingStrategy


def _inverse_pair_frames(prev_spread: float, current_spread: float, rows: int = 80) -> tuple[pd.DataFrame, pd.DataFrame]:
    idx = pd.date_range(start="2025-01-01", periods=rows, freq="h")
    rng = np.random.default_rng(seed=41)
    leg2 = pd.Series(110 + np.cumsum(rng.normal(0, 0.9, rows)), index=idx).clip(lower=20)
    spread_component = pd.Series(rng.normal(0, 0.12, rows), index=idx)
    spread_component.iloc[-2] = prev_spread
    spread_component.iloc[-1] = current_spread
    leg1 = pd.Series(220 - 0.65 * leg2.values + spread_component.values, index=idx).clip(lower=20)

    df1 = pd.DataFrame({"close": leg1.values, "symbol": ["AAA/USDT"] * rows}, index=idx)
    df2 = pd.DataFrame({"close": leg2.values, "symbol": ["BBB/USDT"] * rows}, index=idx)
    return df1, df2


def _strategy() -> PairsTradingStrategy:
    return PairsTradingStrategy(
        name="pairs_inverse_test",
        params={
            "lookback_period": 30,
            "entry_z_score": 1.5,
            "exit_z_score": 0.5,
            "allow_negative_hedge_ratio": True,
            "min_hedge_ratio": -5.0,
            "max_hedge_ratio": 5.0,
        },
    )


def test_negative_correlation_pair_long_spread_buys_both_legs():
    strategy = _strategy()
    data1, data2 = _inverse_pair_frames(prev_spread=-0.2, current_spread=-2.5)

    signals = strategy.generate_signals(data1, data2)

    assert len(signals) == 2
    assert strategy._hedge_ratio is not None and strategy._hedge_ratio < 0
    assert signals[0].symbol == "AAA/USDT"
    assert signals[0].signal_type == SignalType.BUY
    assert signals[1].symbol == "BBB/USDT"
    assert signals[1].signal_type == SignalType.BUY
    assert signals[1].metadata["pair_regime"] == "negative_corr"
    assert signals[0].metadata["pair_group_id"] == signals[1].metadata["pair_group_id"]
    assert signals[0].metadata["pair_leg_notional_fraction"] > 0
    assert signals[1].metadata["pair_leg_notional_fraction"] > 0
    assert signals[0].metadata["pair_quantity_scale"] == 1.0
    assert signals[1].metadata["pair_quantity_scale"] > 0


def test_negative_correlation_pair_short_spread_sells_both_legs():
    strategy = _strategy()
    data1, data2 = _inverse_pair_frames(prev_spread=0.2, current_spread=2.5)

    signals = strategy.generate_signals(data1, data2)

    assert len(signals) == 2
    assert strategy._hedge_ratio is not None and strategy._hedge_ratio < 0
    assert signals[0].symbol == "AAA/USDT"
    assert signals[0].signal_type == SignalType.SELL
    assert signals[1].symbol == "BBB/USDT"
    assert signals[1].signal_type == SignalType.SELL
    assert signals[1].metadata["pair_regime"] == "negative_corr"


def test_exit_guidance_marks_pair_signals_close_only():
    strategy = _strategy()
    data1, data2 = _inverse_pair_frames(prev_spread=-2.5, current_spread=-0.1)

    signals = strategy.generate_signals(data1, data2)

    assert len(signals) == 2
    assert signals[0].metadata["direction"] == "mean_revert_exit"
    assert signals[0].metadata["close_only"] is True
    assert signals[1].metadata["close_only"] is True
