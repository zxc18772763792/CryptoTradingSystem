import numpy as np
import pandas as pd

from config.strategy_registry import STRATEGY_REGISTRY
from core.strategies.strategy_base import SignalType
from strategies.factor_based.factor_strategies import VaRBreakoutStrategy
from strategies.quantitative.mean_reversion import MeanReversionStrategy
from strategies.technical.bollinger_strategy import BollingerSqueezeStrategy
from strategies.technical.macd_strategy import MACDHistogramStrategy
from strategies.technical.rsi_strategy import RSIStrategy


def _close_frame(rows: int = 40, symbol: str = "BTC/USDT") -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=rows, freq="h")
    close = np.linspace(100.0, 102.0, rows)
    return pd.DataFrame({"close": close, "symbol": [symbol] * rows}, index=index)


def test_macd_histogram_strategy_respects_min_histogram_threshold(monkeypatch):
    strategy = MACDHistogramStrategy(
        "macd_hist_test",
        {"fast_period": 12, "slow_period": 26, "signal_period": 9, "min_histogram": 0.1},
    )
    data = _close_frame(rows=60)

    def _calc_small_cross(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist.iloc[-2] = -0.05
        hist.iloc[-1] = 0.04
        return series, series, hist

    monkeypatch.setattr(strategy, "_calculate_macd", _calc_small_cross)
    assert strategy.generate_signals(data) == []

    def _calc_large_cross(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist = pd.Series(np.zeros(len(_data)), index=_data.index)
        hist.iloc[-2] = -0.15
        hist.iloc[-1] = 0.18
        return series, series, hist

    monkeypatch.setattr(strategy, "_calculate_macd", _calc_large_cross)
    signals = strategy.generate_signals(data)
    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.BUY
    assert signals[0].metadata["min_histogram"] == 0.1


def test_rsi_strategy_uses_exit_thresholds_for_close_signals(monkeypatch):
    strategy = RSIStrategy(
        "rsi_exit_test",
        {"period": 14, "oversold": 30, "overbought": 70, "exit_oversold": 40, "exit_overbought": 60},
    )
    data = _close_frame(rows=30)

    def _entry_rsi(_data, _period):
        series = pd.Series(np.full(len(_data), 50.0), index=_data.index)
        series.iloc[-2] = 25.0
        series.iloc[-1] = 31.0
        return series

    monkeypatch.setattr(strategy, "_calculate_rsi", _entry_rsi)
    entry_signals = strategy.generate_signals(data)
    assert len(entry_signals) == 1
    assert entry_signals[0].signal_type == SignalType.BUY

    def _exit_rsi(_data, _period):
        series = pd.Series(np.full(len(_data), 50.0), index=_data.index)
        series.iloc[-2] = 38.0
        series.iloc[-1] = 41.0
        return series

    monkeypatch.setattr(strategy, "_calculate_rsi", _exit_rsi)
    exit_signals = strategy.generate_signals(data)
    assert len(exit_signals) == 1
    assert exit_signals[0].signal_type == SignalType.CLOSE_LONG
    assert exit_signals[0].metadata["exit_threshold"] == 40


def test_mean_reversion_strategy_uses_exit_zscore_for_close_signals(monkeypatch):
    strategy = MeanReversionStrategy(
        "mr_exit_test",
        {"lookback_period": 20, "entry_z_score": 2.0, "exit_z_score": 0.6},
    )
    data = _close_frame(rows=40)

    def _entry_zscore(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        series.iloc[-2] = -2.3
        series.iloc[-1] = -1.8
        return series

    monkeypatch.setattr(strategy, "_calculate_z_score", _entry_zscore)
    entry_signals = strategy.generate_signals(data)
    assert len(entry_signals) == 1
    assert entry_signals[0].signal_type == SignalType.BUY

    def _exit_zscore(_data):
        series = pd.Series(np.zeros(len(_data)), index=_data.index)
        series.iloc[-2] = -0.9
        series.iloc[-1] = -0.5
        return series

    monkeypatch.setattr(strategy, "_calculate_z_score", _exit_zscore)
    exit_signals = strategy.generate_signals(data)
    assert len(exit_signals) == 1
    assert exit_signals[0].signal_type == SignalType.CLOSE_LONG
    assert exit_signals[0].metadata["exit_z_score"] == 0.6


def test_bollinger_squeeze_strategy_uses_breakout_threshold_and_stop_loss(monkeypatch):
    strategy = BollingerSqueezeStrategy(
        "bb_squeeze_test",
        {
            "period": 20,
            "num_std": 2.0,
            "squeeze_threshold": 0.02,
            "breakout_threshold": 0.01,
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.08,
        },
    )
    data = _close_frame(rows=40)

    def _bands_small_breakout(_data):
        upper = pd.Series(np.full(len(_data), 100.5), index=_data.index)
        middle = pd.Series(np.full(len(_data), 100.0), index=_data.index)
        lower = pd.Series(np.full(len(_data), 99.5), index=_data.index)
        bandwidth = pd.Series(np.full(len(_data), 0.03), index=_data.index)
        bandwidth.iloc[-2] = 0.015
        return upper, middle, lower, bandwidth

    data_small = data.copy()
    data_small.iloc[-1, data_small.columns.get_loc("close")] = 101.0
    monkeypatch.setattr(strategy, "_calculate_bollinger_bands", _bands_small_breakout)
    assert strategy.generate_signals(data_small) == []

    data_large = data.copy()
    data_large.iloc[-1, data_large.columns.get_loc("close")] = 101.8
    signals = strategy.generate_signals(data_large)
    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.BUY
    assert signals[0].stop_loss == data_large["close"].iloc[-1] * (1 - 0.03)
    assert signals[0].metadata["breakout_threshold"] == 0.01


def test_var_breakout_strategy_uses_correct_return_direction():
    strategy = VaRBreakoutStrategy(
        "var_breakout_test",
        {"var_period": 20, "confidence": 0.95, "multiplier": 1.5},
    )

    index = pd.date_range("2025-01-01", periods=30, freq="h")
    base = np.array(
        [
            100.0, 100.4, 99.8, 100.2, 99.9, 100.5, 100.1, 100.7, 100.0, 100.8,
            100.2, 100.6, 100.1, 100.5, 100.0, 100.4, 100.2, 100.7, 100.3, 100.8,
            100.5, 100.9, 100.4, 100.8, 100.3, 100.7, 100.4, 100.8, 101.0, 114.0,
        ]
    )
    positive_df = pd.DataFrame({"close": base, "symbol": ["BTC/USDT"] * len(base)}, index=index)
    positive_signals = strategy.generate_signals(positive_df)
    assert len(positive_signals) == 1
    assert positive_signals[0].signal_type == SignalType.BUY

    negative_base = base.copy()
    negative_base[-1] = 88.0
    negative_df = pd.DataFrame({"close": negative_base, "symbol": ["BTC/USDT"] * len(base)}, index=index)
    negative_signals = strategy.generate_signals(negative_df)
    assert len(negative_signals) == 1
    assert negative_signals[0].signal_type == SignalType.SELL


def test_backtest_optimization_grid_keys_are_declared_in_defaults():
    for name, meta in STRATEGY_REGISTRY.items():
        backtest = dict(meta.get("backtest") or {})
        if not backtest.get("supported"):
            continue
        defaults = set((meta.get("defaults") or {}).keys())
        grid = set((backtest.get("optimization_grid") or {}).keys())
        assert grid.issubset(defaults), f"{name} optimization grid contains undeclared defaults: {sorted(grid - defaults)}"
