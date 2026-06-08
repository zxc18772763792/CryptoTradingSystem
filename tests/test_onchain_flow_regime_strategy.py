from __future__ import annotations

from datetime import timezone

import pandas as pd

from core.strategies.strategy_base import SignalType
from strategies.macro.onchain_flow_regime import OnChainFlowRegimeStrategy


def _frame(*, accumulation: float, distribution: float) -> pd.DataFrame:
    idx = pd.date_range("2026-01-01", periods=2, freq="h", tz=timezone.utc)
    return pd.DataFrame(
        {
            "symbol": ["BTC/USDT", "BTC/USDT"],
            "close": [100.0, 101.0],
            "accumulation_score": [0.0, accumulation],
            "distribution_score": [0.0, distribution],
            "onchain_as_of": [idx[0], idx[1]],
            "lookahead_risk": ["none", "none"],
        },
        index=idx,
    )


def test_trade_mode_distribution_closes_long_before_optional_short():
    strategy = OnChainFlowRegimeStrategy(
        params={
            "trade_mode": True,
            "emit_regime_hold_signal": False,
            "accumulation_enter": 0.65,
            "distribution_enter": 0.65,
        }
    )

    signals = strategy.generate_signals(_frame(accumulation=0.1, distribution=0.8))

    assert [signal.signal_type for signal in signals] == [
        SignalType.CLOSE_LONG,
        SignalType.SELL,
    ]
    assert signals[0].metadata["structural_role"] == "slow_regime_exit"
    assert signals[0].metadata["reason_codes"] == ["onchain_distribution_close_long"]
    assert signals[1].metadata["reason_codes"] == ["onchain_distribution_optional_short"]


def test_trade_mode_accumulation_closes_short_before_optional_long():
    strategy = OnChainFlowRegimeStrategy(
        params={
            "trade_mode": True,
            "emit_regime_hold_signal": False,
            "accumulation_enter": 0.65,
            "distribution_enter": 0.65,
        }
    )

    signals = strategy.generate_signals(_frame(accumulation=0.8, distribution=0.1))

    assert [signal.signal_type for signal in signals] == [
        SignalType.CLOSE_SHORT,
        SignalType.BUY,
    ]
    assert signals[0].metadata["structural_role"] == "slow_regime_exit"
    assert signals[0].metadata["reason_codes"] == ["onchain_accumulation_close_short"]
    assert signals[1].metadata["reason_codes"] == ["onchain_accumulation_optional_long"]


def test_trade_mode_can_disable_regime_close_signals():
    strategy = OnChainFlowRegimeStrategy(
        params={
            "trade_mode": True,
            "emit_regime_hold_signal": False,
            "emit_regime_close_signal": False,
        }
    )

    signals = strategy.generate_signals(_frame(accumulation=0.1, distribution=0.8))

    assert [signal.signal_type for signal in signals] == [SignalType.SELL]
