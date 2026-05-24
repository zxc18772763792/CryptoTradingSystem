from types import SimpleNamespace

import numpy as np
import pandas as pd

import strategies as strategy_module
from config.strategy_registry import (
    get_backtest_strategy_info,
    get_strategy_defaults,
    get_strategy_registry_entry,
)
from core.strategies.strategy_base import SignalType
from strategies.quantitative.altcoin_downtrend_bounce_short import (
    AltcoinDowntrendBounceShortStrategy,
)
from web.api.backtest import _run_backtest_core


def _downtrend_bounce_frame(rows: int = 130, symbol: str = "RENDER/USDT") -> pd.DataFrame:
    index = pd.date_range("2026-05-01", periods=rows, freq="h")
    close = np.r_[np.linspace(120.0, 80.0, 110), np.linspace(80.0, 78.0, rows - 111), 86.0]
    close = pd.Series(close, index=index)
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) * 1.002
    low = np.minimum(open_, close) * 0.998
    return pd.DataFrame(
        {
            "open": open_.to_numpy(),
            "high": high.to_numpy(),
            "low": low.to_numpy(),
            "close": close.to_numpy(),
            "volume": np.full(rows, 1000.0),
            "symbol": [symbol] * rows,
        },
        index=index,
    )


def test_altcoin_downtrend_bounce_short_emits_short_entry():
    strategy = AltcoinDowntrendBounceShortStrategy("test_alt_short")
    signals = strategy.generate_signals(_downtrend_bounce_frame())

    assert len(signals) == 1
    signal = signals[0]
    assert signal.signal_type == SignalType.SELL
    assert signal.symbol == "RENDER/USDT"
    assert signal.metadata["setup"] == "downtrend_bounce_failure_short"
    assert signal.metadata["fast_sma"] < signal.metadata["slow_sma"]
    assert signal.metadata["distance"] >= 0.03
    assert signal.metadata["rsi"] >= 60.0
    assert signal.metadata["hold_bars"] == 24
    assert signal.metadata["use_atr_stops"] is False


def test_altcoin_downtrend_bounce_short_exits_after_hold_bars():
    strategy = AltcoinDowntrendBounceShortStrategy("test_alt_short")
    entry_frame = _downtrend_bounce_frame()
    entry_signal = strategy.generate_signals(entry_frame)[0]

    future_index = pd.date_range(entry_frame.index[-1] + pd.Timedelta(hours=1), periods=24, freq="h")
    extension = pd.DataFrame(
        {
            "open": np.full(24, 85.0),
            "high": np.full(24, 86.0),
            "low": np.full(24, 82.0),
            "close": np.linspace(85.0, 83.0, 24),
            "volume": np.full(24, 1000.0),
            "symbol": ["RENDER/USDT"] * 24,
        },
        index=future_index,
    )
    combined = pd.concat([entry_frame, extension])
    position = SimpleNamespace(
        symbol="RENDER/USDT",
        side="short",
        entry_price=entry_signal.price,
        metadata=dict(entry_signal.metadata),
    )

    assert strategy.check_exit(combined.iloc[:-1], position) is None
    exit_signal = strategy.check_exit(combined, position)

    assert exit_signal is not None
    assert exit_signal.signal_type == SignalType.CLOSE_SHORT
    assert exit_signal.metadata["close_reason"] == "downtrend_bounce_time_exit"
    assert exit_signal.metadata["hold_bars"] == 24


def test_altcoin_downtrend_bounce_short_does_not_reenter_same_bar_after_exit():
    strategy = AltcoinDowntrendBounceShortStrategy("test_alt_short")
    entry_frame = _downtrend_bounce_frame()
    entry_signal = strategy.generate_signals(entry_frame)[0]
    future_index = pd.date_range(entry_frame.index[-1] + pd.Timedelta(hours=1), periods=24, freq="h")
    extension = pd.DataFrame(
        {
            "open": np.full(24, 85.0),
            "high": np.full(24, 87.0),
            "low": np.full(24, 83.0),
            "close": np.full(24, 86.0),
            "volume": np.full(24, 1000.0),
            "symbol": ["RENDER/USDT"] * 24,
        },
        index=future_index,
    )
    combined = pd.concat([entry_frame, extension])
    position = SimpleNamespace(
        symbol="RENDER/USDT",
        side="short",
        entry_price=entry_signal.price,
        metadata=dict(entry_signal.metadata),
    )

    exit_signal = strategy.check_exit(combined, position)

    assert exit_signal is not None
    assert strategy.generate_signals(combined) == []


def test_altcoin_downtrend_bounce_short_is_registered_for_backtests():
    assert "AltcoinDowntrendBounceShortStrategy" in strategy_module.ALL_STRATEGIES
    assert getattr(strategy_module, "AltcoinDowntrendBounceShortStrategy") is AltcoinDowntrendBounceShortStrategy

    entry = get_strategy_registry_entry("AltcoinDowntrendBounceShortStrategy")
    defaults = get_strategy_defaults("AltcoinDowntrendBounceShortStrategy")
    info = get_backtest_strategy_info("AltcoinDowntrendBounceShortStrategy")

    assert entry["timeframe"] == "1h"
    assert defaults["allow_long"] is False
    assert defaults["allow_short"] is True
    assert defaults["hold_bars"] == 24
    assert info["backtest_supported"] is True


def test_altcoin_downtrend_bounce_short_runs_through_backtest_core():
    entry_frame = _downtrend_bounce_frame()
    future_index = pd.date_range(entry_frame.index[-1] + pd.Timedelta(hours=1), periods=24, freq="h")
    extension = pd.DataFrame(
        {
            "open": np.full(24, 85.0),
            "high": np.full(24, 86.0),
            "low": np.full(24, 80.0),
            "close": np.linspace(85.0, 82.0, 24),
            "volume": np.full(24, 1000.0),
            "symbol": ["RENDER/USDT"] * 24,
        },
        index=future_index,
    )
    df = pd.concat([entry_frame, extension])

    result = _run_backtest_core(
        strategy="AltcoinDowntrendBounceShortStrategy",
        df=df,
        timeframe="1h",
        initial_capital=10000.0,
        params=get_strategy_defaults("AltcoinDowntrendBounceShortStrategy"),
        include_trade_log=True,
        exit_template=None,
    )

    assert result["allow_long"] is False
    assert result["allow_short"] is True
    assert result["entry_signals"] == 1
    assert result["exit_signals"] == 1
    assert result["total_trades"] == 1
    assert result["completed_trades"][0]["direction"] == "short"
