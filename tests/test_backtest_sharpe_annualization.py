from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from core.backtest.backtest_engine import BacktestConfig, BacktestEngine
from core.strategies import Signal, SignalType, StrategyBase


def test_backtest_sharpe_annualizes_from_bar_interval() -> None:
    engine = BacktestEngine()
    engine._equity_curve = [100.0, 101.0, 100.5, 102.0]
    engine._equity_index = list(pd.date_range("2026-01-01", periods=4, freq="5min", tz="UTC"))

    result = engine._calculate_result()

    returns = np.diff(np.asarray(engine._equity_curve)) / np.asarray(engine._equity_curve[:-1])
    expected = float(np.mean(returns) / np.std(returns) * np.sqrt(365 * 24 * 12))
    assert result.sharpe_ratio == pytest.approx(expected)


def test_backtest_check_exit_receives_prior_bars_only() -> None:
    class _ExitRecorderStrategy(StrategyBase):
        def __init__(self) -> None:
            super().__init__("exit_recorder", {})
            self.entered = False
            self.exit_seen_lengths: list[int] = []
            self.exit_seen_last_ts: list[pd.Timestamp | None] = []

        def get_required_data(self):
            return {"type": "kline", "min_length": 2}

        def generate_signals(self, data: pd.DataFrame):
            if self.entered or len(data) < 2:
                return []
            self.entered = True
            return [
                Signal(
                    symbol="BTC/USDT",
                    signal_type=SignalType.BUY,
                    price=float(data["close"].iloc[-1]),
                    timestamp=datetime.now(timezone.utc),
                    strategy_name=self.name,
                    strength=1.0,
                )
            ]

        def check_exit(self, data: pd.DataFrame, position):
            self.exit_seen_lengths.append(len(data))
            self.exit_seen_last_ts.append(pd.Timestamp(data.index[-1]) if len(data) else None)
            return None

    idx = pd.date_range("2026-01-01", periods=5, freq="h", tz="UTC")
    df = pd.DataFrame(
        {
            "open": [100.0] * 5,
            "high": [101.0] * 5,
            "low": [99.0] * 5,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0],
            "volume": [1000.0] * 5,
        },
        index=idx,
    )
    strategy = _ExitRecorderStrategy()
    engine = BacktestEngine(
        BacktestConfig(
            commission_rate=0.0,
            slippage=0.0,
            position_size_pct=0.1,
            max_positions=1,
        )
    )

    asyncio.run(engine.run_backtest(strategy, df, symbol="BTC/USDT"))

    assert strategy.exit_seen_lengths
    assert strategy.exit_seen_last_ts == [idx[2], idx[3]]
    assert all(seen_ts < current_ts for seen_ts, current_ts in zip(strategy.exit_seen_last_ts, idx[3:]))
