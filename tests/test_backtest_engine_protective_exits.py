import asyncio
from datetime import datetime, timezone

import pandas as pd
import pytest

from core.backtest.backtest_engine import BacktestConfig, BacktestEngine
from core.strategies import Signal, SignalType, StrategyBase


class OneShotLongStrategy(StrategyBase):
    def __init__(self, *, stop_loss=None, take_profit=None, metadata=None):
        super().__init__("one_shot_long", {})
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.metadata = dict(metadata or {})
        self.emitted = False

    def generate_signals(self, data):
        if self.emitted or len(data) < 2:
            return []
        self.emitted = True
        return [
            Signal(
                symbol="BTC/USDT",
                signal_type=SignalType.BUY,
                price=100.0,
                timestamp=datetime.now(timezone.utc),
                strategy_name=self.name,
                stop_loss=self.stop_loss,
                take_profit=self.take_profit,
                metadata=dict(self.metadata),
            )
        ]

    def get_required_data(self):
        return {"type": "kline", "min_length": 2}


class ShortThenBuyStrategy(StrategyBase):
    def __init__(self):
        super().__init__("short_then_buy", {})
        self._count = 0

    def generate_signals(self, data):
        if len(data) < 2:
            return []
        self._count += 1
        if self._count == 1:
            signal_type = SignalType.SELL
        elif self._count == 2:
            signal_type = SignalType.BUY
        else:
            return []
        return [
            Signal(
                symbol="BTC/USDT",
                signal_type=signal_type,
                price=float(data["close"].iloc[-1]),
                timestamp=datetime.now(timezone.utc),
                strategy_name=self.name,
            )
        ]

    def get_required_data(self):
        return {"type": "kline", "min_length": 2}


def _ohlcv(*, highs, lows, closes, opens=None):
    idx = pd.date_range("2026-01-01", periods=len(closes), freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "open": opens if opens is not None else closes,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [1000.0] * len(closes),
        },
        index=idx,
    )


def _config(**overrides):
    values = {
        "initial_capital": 10000.0,
        "commission_rate": 0.0,
        "slippage": 0.0,
        "position_size_pct": 0.1,
        "max_positions": 1,
        "enable_shorting": True,
    }
    values.update(overrides)
    return BacktestConfig(**values)


def test_backtest_honors_signal_stop_loss_intrabar():
    engine = BacktestEngine(_config())
    data = _ohlcv(
        highs=[100.0, 100.0, 100.0, 101.0],
        lows=[100.0, 100.0, 100.0, 94.0],
        closes=[100.0, 100.0, 100.0, 96.0],
    )

    result = asyncio.run(
        engine.run_backtest(
            OneShotLongStrategy(stop_loss=95.0, take_profit=110.0),
            data,
            symbol="BTC/USDT",
        )
    )

    close = [trade for trade in result.trades if trade.trade_stage == "close"][0]
    assert close.exit_reason == "stop_loss"
    assert close.price == pytest.approx(95.0)


def test_backtest_stop_loss_gap_through_fills_at_open():
    engine = BacktestEngine(_config())
    data = _ohlcv(
        opens=[100.0, 100.0, 100.0, 92.0],
        highs=[100.0, 100.0, 100.0, 93.0],
        lows=[100.0, 100.0, 100.0, 90.0],
        closes=[100.0, 100.0, 100.0, 91.0],
    )

    result = asyncio.run(
        engine.run_backtest(
            OneShotLongStrategy(stop_loss=95.0, take_profit=110.0),
            data,
            symbol="BTC/USDT",
        )
    )

    close = [trade for trade in result.trades if trade.trade_stage == "close"][0]
    assert close.exit_reason == "stop_loss"
    assert close.price == pytest.approx(92.0)


def test_backtest_honors_signal_take_profit_intrabar():
    engine = BacktestEngine(_config())
    data = _ohlcv(
        highs=[100.0, 100.0, 100.0, 111.0],
        lows=[100.0, 100.0, 100.0, 99.0],
        closes=[100.0, 100.0, 100.0, 109.0],
    )

    result = asyncio.run(
        engine.run_backtest(
            OneShotLongStrategy(stop_loss=95.0, take_profit=110.0),
            data,
            symbol="BTC/USDT",
        )
    )

    close = [trade for trade in result.trades if trade.trade_stage == "close"][0]
    assert close.exit_reason == "take_profit"
    assert close.price == pytest.approx(110.0)


def test_backtest_honors_trailing_stop_from_signal_metadata():
    engine = BacktestEngine(_config())
    data = _ohlcv(
        highs=[100.0, 100.0, 100.0, 110.0],
        lows=[100.0, 100.0, 100.0, 104.0],
        closes=[100.0, 100.0, 100.0, 105.0],
    )

    result = asyncio.run(
        engine.run_backtest(
            OneShotLongStrategy(metadata={"trailing_stop_pct": 0.05}),
            data,
            symbol="BTC/USDT",
        )
    )

    close = [trade for trade in result.trades if trade.trade_stage == "close"][0]
    assert close.exit_reason == "trailing_stop"
    assert close.price == pytest.approx(104.5)


def test_backtest_honors_time_stop_from_signal_metadata():
    engine = BacktestEngine(_config())
    data = _ohlcv(
        highs=[100.0, 100.0, 100.0, 101.0],
        lows=[100.0, 100.0, 100.0, 99.0],
        closes=[100.0, 100.0, 100.0, 100.5],
    )

    result = asyncio.run(
        engine.run_backtest(
            OneShotLongStrategy(metadata={"time_stop_enabled": True, "max_bars_in_trade": 2}),
            data,
            symbol="BTC/USDT",
        )
    )

    close = [trade for trade in result.trades if trade.trade_stage == "close"][0]
    assert close.exit_reason == "time_stop"
    assert close.price == pytest.approx(100.5)


def test_backtest_opposite_buy_closes_short_and_opens_long():
    engine = BacktestEngine(_config(position_size_pct=0.1, max_positions=1))
    data = _ohlcv(
        highs=[100.0, 100.0, 101.0, 102.0],
        lows=[100.0, 100.0, 99.0, 100.0],
        closes=[100.0, 100.0, 101.0, 102.0],
    )

    result = asyncio.run(engine.run_backtest(ShortThenBuyStrategy(), data, symbol="BTC/USDT"))

    assert [trade.trade_stage for trade in result.trades] == ["open", "close", "open"]
    assert [trade.side for trade in result.trades] == ["sell", "buy", "buy"]
    assert result.trades[1].exit_reason == "signal_reversal"
