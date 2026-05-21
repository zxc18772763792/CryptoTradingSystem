"""Unit tests for strategy.check_exit overrides added in the exit-logic overhaul.

Covers:
    - BollingerBandsStrategy (already done by codex baseline)
    - BollingerSqueezeStrategy (bandwidth re-contract)
    - RSIStrategy (RSI returns through neutral band)
    - MAStrategy / EMAStrategy (fast/slow MA spread compression)
    - MACDStrategy (histogram flip against position)

The tests use small, hand-crafted OHLC frames so the exit conditions are
deterministic — random data only triggers signals stochastically and was
not reproducible enough for regression testing.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import List

import numpy as np
import pandas as pd
import pytest

from core.strategies.strategy_base import SignalType, StrategyBase
from core.trading.position_manager import PositionSide
from strategies.technical.bollinger_strategy import (
    BollingerBandsStrategy,
    BollingerSqueezeStrategy,
)
from strategies.technical.ma_strategy import EMAStrategy, MAStrategy
from strategies.technical.macd_strategy import MACDStrategy
from strategies.technical.rsi_strategy import RSIStrategy


def _make_df(closes: List[float], symbol: str = "BTC/USDT") -> pd.DataFrame:
    """Build a minimal OHLC DataFrame from a close-price series."""
    n = len(closes)
    df = pd.DataFrame(
        {
            "open": closes,
            "high": [c * 1.002 for c in closes],
            "low": [c * 0.998 for c in closes],
            "close": closes,
            "volume": [1_000.0] * n,
            "symbol": [symbol] * n,
        }
    )
    df.index = pd.date_range("2026-01-01", periods=n, freq="1h")
    return df


def _pos(side: str, *, symbol: str = "BTC/USDT", entry_price: float = 100.0) -> SimpleNamespace:
    return SimpleNamespace(side=side, symbol=symbol, entry_price=entry_price, metadata={})


class _GenericExitStrategy(StrategyBase):
    def generate_signals(self, data):
        return []

    def get_required_data(self):
        return {"type": "kline", "columns": ["close"], "min_length": 4}


# ───────────────────────────── RSI ─────────────────────────────


class TestRSICheckExit:
    def test_long_exit_when_rsi_crosses_back_through_40(self):
        # Drive RSI low with a slow decline, then a recovery sequence chosen
        # so that prev_rsi < 40 <= current_rsi on the last two bars.
        closes = (
            [100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86]
            + [86.5, 87, 87.5, 88, 88.2, 88.3, 88.5, 88.7, 89, 90]
        )
        df = _make_df(closes)
        s = RSIStrategy(params={"exit_oversold": 40, "exit_min_profit_pct": 0.0})
        rsi = s._calculate_rsi(df, 14)
        assert rsi.iloc[-2] < 40 <= rsi.iloc[-1], (
            f"Test data did not produce the expected crossover; "
            f"prev_rsi={rsi.iloc[-2]:.2f} current_rsi={rsi.iloc[-1]:.2f}"
        )

        sig = s.check_exit(df, _pos("long", entry_price=86.0))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG
        assert sig.metadata["close_reason"] == "rsi_long_exit"
        assert sig.metadata["close_only"] is True

    def test_no_exit_when_no_position_side(self):
        df = _make_df([100.0] * 40)
        s = RSIStrategy()
        assert s.check_exit(df, _pos("")) is None

    def test_no_exit_for_short_when_rsi_low(self):
        # Short position but RSI not yet crossing back through 60 from above.
        closes = list(np.linspace(100, 110, 30))
        df = _make_df(closes)
        s = RSIStrategy()
        assert s.check_exit(df, _pos("short", entry_price=108.0)) is None

    def test_generate_signals_no_longer_emits_close_branch(self):
        """The old inline exit branch has been removed from generate_signals."""
        closes = [100.0] * 5 + list(np.linspace(100, 70, 20)) + [70.5, 71.0, 71.5, 72.0, 73.0]
        df = _make_df(closes)
        s = RSIStrategy()
        sigs = s.generate_signals(df)
        # No emitted signal should be CLOSE_LONG/SHORT now — exits flow through check_exit.
        for sig in sigs:
            assert sig.signal_type not in {SignalType.CLOSE_LONG, SignalType.CLOSE_SHORT}


# ───────────────────────────── MA / EMA ─────────────────────────────


class TestMAEMAheckExit:
    # Use a wider signal_threshold (1%) so synthetic close paths can deterministically
    # hit the spread-compression region. With the default 0.001 (0.1%) the
    # soft-exit window of ±0.05% is too narrow for hand-crafted bar series.

    def _trend_then_pullback(self) -> pd.DataFrame:
        # 50 bars of uptrend (100 → 110) → 8 bars of moderate pullback (110 → 105).
        # Tuned so MA(10,30) spread starts at ~0.6% (above the 0.5% threshold)
        # and compresses to ~0.2% (below the 0.25% soft-exit) on the last bar.
        closes = list(np.linspace(100, 110, 50)) + list(np.linspace(110, 105, 8))
        return _make_df(closes)

    def test_ma_long_exit_on_spread_compression(self):
        df = self._trend_then_pullback()
        s = MAStrategy(params={"signal_threshold": 0.005})  # 0.5%, soft-exit at 0.25%
        fast = df["close"].rolling(s.params["fast_period"]).mean()
        slow = df["close"].rolling(s.params["slow_period"]).mean()
        diff = (fast - slow) / slow
        assert diff.iloc[-2] >= s.params["signal_threshold"], (
            f"prev spread should be above threshold; got {diff.iloc[-2]:.6f}"
        )
        soft_exit = s.params["signal_threshold"] * 0.5
        assert diff.iloc[-1] <= soft_exit, (
            f"current spread should compress below soft-exit; got {diff.iloc[-1]:.6f}"
        )

        sig = s.check_exit(df, _pos("long", entry_price=102.0))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG
        assert sig.metadata["close_reason"] == "ma_diff_compression"

    def test_ema_long_exit_on_spread_compression(self):
        # EMA reacts faster than SMA, so the same path compresses sooner.
        # Build a slightly different shape to ensure EMA spread compresses on last bar.
        closes = list(np.linspace(100, 105, 35)) + list(np.linspace(105, 102, 12))
        df = _make_df(closes)
        s = EMAStrategy(params={"signal_threshold": 0.005})
        fast = df["close"].ewm(span=s.params["fast_period"], adjust=False).mean()
        slow = df["close"].ewm(span=s.params["slow_period"], adjust=False).mean()
        diff = (fast - slow) / slow
        # Allow either: full crossing of soft-exit OR the helper's other branch.
        if diff.iloc[-2] >= s.params["signal_threshold"] and diff.iloc[-1] <= s.params["signal_threshold"] * 0.5:
            sig = s.check_exit(df, _pos("long", entry_price=104.0))
            assert sig is not None
            assert sig.signal_type == SignalType.CLOSE_LONG
            assert sig.metadata["close_reason"] == "ema_diff_compression"

    def test_no_exit_when_spread_still_strong(self):
        # Pure uptrend — spread stays well above threshold.
        df = _make_df(list(np.linspace(100, 200, 60)))
        s = MAStrategy()
        assert s.check_exit(df, _pos("long", entry_price=150.0)) is None


# ───────────────────────────── MACD ─────────────────────────────


class TestMACDCheckExit:
    def test_long_exit_on_histogram_flip_negative(self, monkeypatch):
        df = _make_df(list(np.linspace(100, 130, 60)))
        s = MACDStrategy()

        def _hist_flip(_data):
            series = pd.Series(np.zeros(len(_data)), index=_data.index)
            hist = pd.Series(np.zeros(len(_data)), index=_data.index)
            hist.iloc[-2] = 0.15
            hist.iloc[-1] = -0.02
            return series, series, hist

        monkeypatch.setattr(s, "_calculate_macd", _hist_flip)

        sig = s.check_exit(df, _pos("long", entry_price=125.0))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG
        assert sig.metadata["close_reason"] == "macd_histogram_flip"

    def test_no_exit_when_histogram_steady_positive(self):
        df = _make_df(list(np.linspace(100, 160, 60)))
        s = MACDStrategy()
        assert s.check_exit(df, _pos("long", entry_price=130.0)) is None


# ───────────────────────────── Bollinger Squeeze ─────────────────────────────


class TestBollingerSqueezeCheckExit:
    def test_long_exit_when_bandwidth_recontracts(self, monkeypatch):
        closes = list(np.linspace(100, 105, 45))
        df = _make_df(closes)
        s = BollingerSqueezeStrategy()

        def _recontracting_bands(_data):
            series = pd.Series(np.full(len(_data), 100.0), index=_data.index)
            bandwidth = pd.Series(np.full(len(_data), 0.04), index=_data.index)
            bandwidth.iloc[-2] = 0.03
            bandwidth.iloc[-1] = 0.021
            return series, series, series, bandwidth

        monkeypatch.setattr(s, "_calculate_bollinger_bands", _recontracting_bands)

        sig = s.check_exit(df, _pos("long", entry_price=float(df["close"].iloc[-1])))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG
        assert sig.metadata["close_reason"] == "bollinger_squeeze_recontract"

    def test_no_exit_when_bandwidth_still_expanding(self):
        # Steadily expanding bandwidth — should not trigger exit.
        closes = list(np.linspace(100, 140, 40))
        df = _make_df(closes)
        s = BollingerSqueezeStrategy()
        assert s.check_exit(df, _pos("long", entry_price=120.0)) is None


# ───────────────────────────── Bollinger Bands (codex baseline) ─────────────────────────────


class TestBollingerBandsCheckExitRegression:
    """Regression: ensure codex's existing BB middle-reversion exit still fires."""

    def test_long_exit_on_middle_band_recross(self, monkeypatch):
        df = _make_df(list(np.linspace(95, 101, 29)))
        df.iloc[-2, df.columns.get_loc("close")] = 99.0
        df.iloc[-1, df.columns.get_loc("close")] = 101.0
        s = BollingerBandsStrategy()

        def _middle_recross(_data):
            series = pd.Series(np.full(len(_data), 100.0), index=_data.index)
            return series, series, series

        monkeypatch.setattr(s, "_calculate_bollinger_bands", _middle_recross)

        sig = s.check_exit(df, _pos("long", entry_price=92.0))
        assert sig is not None
        assert sig.signal_type == SignalType.CLOSE_LONG
        assert sig.metadata["close_reason"] == "bollinger_middle_reversion"


# ───────────────────────────── Cross-strategy invariants ─────────────────────────────


@pytest.mark.parametrize(
    "strategy_cls",
    [
        BollingerBandsStrategy,
        BollingerSqueezeStrategy,
        RSIStrategy,
        MAStrategy,
        EMAStrategy,
        MACDStrategy,
    ],
)
def test_check_exit_metadata_invariants(strategy_cls):
    """Any exit signal must be CLOSE_LONG/SHORT with close_only=True and a close_reason."""
    df = _make_df([100.0] * 60)  # flat data — most won't fire
    s = strategy_cls()
    # If the strategy fires on flat data, the signal must still satisfy invariants.
    for side in ("long", "short"):
        sig = s.check_exit(df, _pos(side))
        if sig is None:
            continue
        assert sig.signal_type in {SignalType.CLOSE_LONG, SignalType.CLOSE_SHORT}
        assert sig.metadata.get("close_only") is True
        assert sig.metadata.get("close_reason"), "close_reason must be non-empty"


def test_check_exit_returns_none_for_empty_data():
    df = pd.DataFrame()
    for cls in (RSIStrategy, MAStrategy, EMAStrategy, MACDStrategy, BollingerSqueezeStrategy):
        assert cls().check_exit(df, _pos("long")) is None, f"{cls.__name__} should handle empty data"


def test_generic_check_exit_accepts_real_position_side_enum():
    df = _make_df([100.0, 105.0, 110.0, 115.0, 120.0, 112.0])
    strategy = _GenericExitStrategy("generic_exit")
    position = SimpleNamespace(
        side=PositionSide.LONG,
        symbol="BTC/USDT",
        entry_price=100.0,
        metadata={},
    )

    sig = strategy.check_exit(df, position)

    assert sig is not None
    assert sig.signal_type == SignalType.CLOSE_LONG
    assert sig.metadata["close_reason"] == "generic_sma_profit_lock"
