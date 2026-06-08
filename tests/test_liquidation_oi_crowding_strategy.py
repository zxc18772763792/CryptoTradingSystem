from datetime import timezone

import numpy as np
import pandas as pd

from core.strategies.strategy_base import SignalType
from core.structural.context import StructuralMarketContext
from core.structural.derivatives_crowding import LiquidationOICrowdingGate
from strategies.quantitative.liquidation_oi_crowding import LiquidationOICrowdingStrategy


def _base_frame(rows: int = 30) -> pd.DataFrame:
    index = pd.date_range("2026-05-19", periods=rows, freq="h", tz=timezone.utc)
    close = pd.Series(np.linspace(100, 100, rows), index=index)
    return pd.DataFrame(
        {
            "open": close.values,
            "high": (close * 1.01).values,
            "low": (close * 0.99).values,
            "close": close.values,
            "volume": np.full(rows, 1000.0),
            "symbol": ["BTC/USDT"] * rows,
            "oi_change_1h": np.zeros(rows),
            "oi_change_z": np.zeros(rows),
            "funding_z": np.zeros(rows),
            "long_short_ratio_z": np.zeros(rows),
            "basis_z": np.zeros(rows),
            "taker_imbalance_z": np.zeros(rows),
            "crowded_long_score": np.zeros(rows),
            "crowded_short_score": np.zeros(rows),
            "long_liquidation_burst_score": np.zeros(rows),
            "short_liquidation_burst_score": np.zeros(rows),
            "price_return_1h": np.zeros(rows),
            "atr_pct": np.full(rows, 0.01),
        },
        index=index,
    )


def test_liquidation_gate_blocks_new_longs_in_crowded_long_extreme():
    ctx = StructuralMarketContext.from_row(
        {
            "symbol": "BTC/USDT",
            "crowded_long_score": 0.82,
            "funding_z": 2.0,
        },
        timestamp=pd.Timestamp("2026-05-20T00:00:00Z"),
    )

    gate = LiquidationOICrowdingGate().evaluate(ctx)

    assert gate.block_new_longs is True
    assert gate.allow_long is False
    assert gate.reduce_long_position_scalar < 1.0
    assert "derivatives_crowded_long_block_new_longs" in gate.reason_codes


def test_liquidation_strategy_emits_long_flush_reversal_trade():
    df = _base_frame()
    df.iloc[-2, df.columns.get_loc("crowded_long_score")] = 0.82
    df.iloc[-2, df.columns.get_loc("funding_z")] = 2.0
    df.iloc[-2, df.columns.get_loc("taker_imbalance_z")] = -1.2
    df.iloc[-1, df.columns.get_loc("crowded_long_score")] = 0.35
    df.iloc[-1, df.columns.get_loc("funding_z")] = 0.35
    df.iloc[-1, df.columns.get_loc("long_liquidation_burst_score")] = 0.90
    df.iloc[-1, df.columns.get_loc("oi_change_1h")] = -0.03
    df.iloc[-1, df.columns.get_loc("price_return_1h")] = -0.025
    df.iloc[-1, df.columns.get_loc("taker_imbalance_z")] = -0.3
    df.iloc[-1, df.columns.get_loc("close")] = 96.0

    strategy = LiquidationOICrowdingStrategy(params={"emit_gate_hold_signal": False})
    signals = strategy.generate_signals(df)

    assert [s.signal_type for s in signals] == [SignalType.BUY]
    assert signals[0].strength >= 0.55
    assert signals[0].metadata["reason_codes"] == ["liquidation_long_flush_reversal"]


def test_liquidation_strategy_closes_long_when_crowded_long_gate_blocks():
    df = _base_frame()
    df.iloc[-1, df.columns.get_loc("crowded_long_score")] = 0.84
    df.iloc[-1, df.columns.get_loc("funding_z")] = 2.0

    strategy = LiquidationOICrowdingStrategy(params={"emit_gate_hold_signal": False})
    signals = strategy.generate_signals(df)

    assert [s.signal_type for s in signals] == [SignalType.CLOSE_LONG]
    assert signals[0].metadata["structural_role"] == "crowding_risk_exit"
    assert signals[0].metadata["reason_codes"] == ["derivatives_crowded_long_close_long"]


def test_liquidation_strategy_closes_short_when_crowded_short_gate_blocks():
    df = _base_frame()
    df.iloc[-1, df.columns.get_loc("crowded_short_score")] = 0.84
    df.iloc[-1, df.columns.get_loc("funding_z")] = -2.0

    strategy = LiquidationOICrowdingStrategy(params={"emit_gate_hold_signal": False})
    signals = strategy.generate_signals(df)

    assert [s.signal_type for s in signals] == [SignalType.CLOSE_SHORT]
    assert signals[0].metadata["structural_role"] == "crowding_risk_exit"
    assert signals[0].metadata["reason_codes"] == ["derivatives_crowded_short_close_short"]


def test_liquidation_strategy_gate_mode_emits_explainable_hold():
    df = _base_frame()
    df.iloc[-1, df.columns.get_loc("crowded_short_score")] = 0.84
    df.iloc[-1, df.columns.get_loc("funding_z")] = -2.0

    strategy = LiquidationOICrowdingStrategy(params={"trade_mode": False})
    signals = strategy.generate_signals(df)

    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.HOLD
    assert "derivatives_crowded_short_block_new_shorts" in signals[0].metadata["reason_codes"]
