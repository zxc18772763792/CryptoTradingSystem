"""Tests for the OI/mcap ambush strategy family (modes A/B/C)."""
from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from config.strategy_registry import get_strategy_registry_entry
from strategies import (
    AccumulationAmbushStrategy,
    IgnitionFastFollowStrategy,
    SqueezeFuelStrategy,
)


def _base_frame(n: int = 900, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2026-01-01", periods=n, freq="1h")
    price = np.abs(1.0 + rng.normal(0, 0.004, n).cumsum() * 0.01) + 0.5
    frame = pd.DataFrame(
        {
            "open": price,
            "high": price * 1.005,
            "low": price * 0.995,
            "close": price,
            "volume": rng.uniform(1e5, 2e5, n),
            "symbol": "TEST/USDT",
        },
        index=idx,
    )
    frame["mcap_usd"] = 100e6
    frame["funding_rate"] = 0.0
    frame["oi_usd"] = 20e6
    return frame


def test_registry_entries_exist_and_support_backtest():
    for name in ("AccumulationAmbushStrategy", "SqueezeFuelStrategy", "IgnitionFastFollowStrategy"):
        entry = get_strategy_registry_entry(name)
        assert entry, name
        assert entry.get("backtest", {}).get("supported") is True
        assert entry.get("defaults", {}).get("use_atr_stops") is False


def test_all_modes_fail_closed_without_enrichment_columns():
    plain = _base_frame().drop(columns=["oi_usd", "funding_rate", "mcap_usd"])
    assert AccumulationAmbushStrategy("A").generate_signals(plain) == []
    assert SqueezeFuelStrategy("B").generate_signals(plain) == []
    assert IgnitionFastFollowStrategy("C").generate_signals(plain) == []


def test_accumulation_ambush_fires_once_on_fingerprint_completion():
    frame = _base_frame()
    frame.loc[frame.index[-260:], "oi_usd"] = np.linspace(20e6, 29e6, 260)
    strategy = AccumulationAmbushStrategy("A")
    fires = []
    for i in range(401, len(frame) + 1):
        for signal in strategy.generate_signals(frame.iloc[:i]):
            fires.append((i, signal))
    assert len(fires) == 1
    _, signal = fires[0]
    assert signal.signal_type.value == "buy"
    assert signal.metadata["mode"] == "A_accumulation_ambush"
    assert signal.stop_loss == pytest.approx(signal.price * (1 - 0.18), rel=1e-6)
    assert signal.metadata["time_stop_enabled"] is True
    assert signal.metadata["generic_check_exit_enabled"] is False


def test_squeeze_fuel_requires_negative_funding_and_breakout():
    frame = _base_frame()
    frame["funding_rate"] = -0.001
    frame.loc[frame.index[-100:], "oi_usd"] = np.linspace(30e6, 40e6, 100)
    frame.loc[frame.index[-30:], "close"] = 0.98
    frame.loc[frame.index[-30:], "high"] = 0.985
    frame.iloc[-1, frame.columns.get_loc("close")] = 1.05
    frame.iloc[-1, frame.columns.get_loc("high")] = 1.06

    signals = SqueezeFuelStrategy("B").generate_signals(frame)
    assert len(signals) == 1
    assert signals[0].metadata["mode"] == "B_squeeze_fuel"

    crowded = frame.copy()
    crowded["funding_rate"] = 0.0008
    assert SqueezeFuelStrategy("B2").generate_signals(crowded) == []


def test_squeeze_fuel_exits_on_funding_flip():
    frame = _base_frame()
    frame["funding_rate"] = 0.0009
    strategy = SqueezeFuelStrategy("B")
    position = SimpleNamespace(symbol="TEST/USDT", side="long", entry_price=1.0, metadata={})
    exit_signal = strategy.check_exit(frame, position)
    assert exit_signal is not None
    assert exit_signal.signal_type.value == "close_long"
    assert exit_signal.metadata["close_reason"] == "funding_flip_positive"


def test_accumulation_exit_on_oi_collapse():
    frame = _base_frame()
    frame["oi_usd"] = 30e6
    frame.loc[frame.index[-72:], "oi_usd"] = np.linspace(30e6, 22e6, 72)
    strategy = AccumulationAmbushStrategy("A")
    position = SimpleNamespace(symbol="TEST/USDT", side="long", entry_price=1.0, metadata={})
    exit_signal = strategy.check_exit(frame, position)
    assert exit_signal is not None
    assert exit_signal.metadata["close_reason"] == "oi_collapse"


def test_ignition_fast_follow_needs_volume_burst_and_oi_jump():
    n = 900
    frame = _base_frame(n)
    frame["close"] = np.linspace(0.9, 1.0, n)
    frame["high"] = frame["close"] * 1.004
    frame["low"] = frame["close"] * 0.996
    frame["open"] = frame["close"]
    frame.iloc[-1, frame.columns.get_loc("close")] = 1.09
    frame.iloc[-1, frame.columns.get_loc("volume")] = 3e6
    frame.loc[frame.index[-5:], "oi_usd"] = [20e6, 20e6, 20e6, 21e6, 23e6]

    signals = IgnitionFastFollowStrategy("C").generate_signals(frame)
    assert len(signals) == 1
    assert signals[0].metadata["mode"] == "C_ignition_fast_follow"

    no_oi_jump = frame.copy()
    no_oi_jump["oi_usd"] = 20e6
    assert IgnitionFastFollowStrategy("C2").generate_signals(no_oi_jump) == []


def test_ignition_skips_already_parabolic_runup():
    n = 900
    frame = _base_frame(n)
    ramp = np.linspace(0.5, 1.0, n)
    ramp[-48:] = np.linspace(0.55, 1.0, 48)  # +82% in 48h
    frame["close"] = ramp
    frame["high"] = frame["close"] * 1.004
    frame["low"] = frame["close"] * 0.996
    frame.iloc[-1, frame.columns.get_loc("volume")] = 3e6
    frame.loc[frame.index[-5:], "oi_usd"] = [20e6, 20e6, 20e6, 21e6, 23e6]
    assert IgnitionFastFollowStrategy("C").generate_signals(frame) == []
