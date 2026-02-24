import numpy as np
import pandas as pd

from core.strategies.strategy_base import SignalType
from strategies.quantitative.multi_factor_hf import MultiFactorHFStrategy


def _df(rows: int = 320, seed: int = 9) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2025-01-01", periods=rows, freq="5min")
    drift = np.r_[np.full(rows // 2, 4.0), np.full(rows - rows // 2, -2.5)]
    close = 40000 + np.cumsum(drift + rng.normal(0, 12, rows))
    close = pd.Series(close, index=idx).abs() + 100
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 6, rows))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 6, rows))
    volume = np.abs(rng.normal(2200, 250, rows)) + 80
    return pd.DataFrame(
        {"open": open_.values, "high": high.values, "low": low.values, "close": close.values, "volume": volume, "symbol": ["BTC/USDT"] * rows},
        index=idx,
    )


def test_multi_factor_hf_strategy_reproducible():
    df = _df()
    s1 = MultiFactorHFStrategy(name="hf_test_1", params={})
    s2 = MultiFactorHFStrategy(name="hf_test_2", params={})
    s1.initialize()
    s2.initialize()
    out1 = []
    out2 = []
    for i in range(180, len(df)):
        out1.extend([x.signal_type.value for x in s1.generate_signals(df.iloc[: i + 1])])
        out2.extend([x.signal_type.value for x in s2.generate_signals(df.iloc[: i + 1])])
    assert out1 == out2


def test_multi_factor_hf_gate_blocks_trades_on_extreme_spread():
    df = _df(rows=260)
    # Inflate ranges to trip spread gate.
    df.loc[df.index[-80:], "high"] = df.loc[df.index[-80:], "close"] * 1.08
    df.loc[df.index[-80:], "low"] = df.loc[df.index[-80:], "close"] * 0.92
    strategy = MultiFactorHFStrategy(
        name="hf_gate_test",
        params={"gates": {"max_spread_proxy": 0.01, "max_rv": 1.0, "max_atr_pct": 1.0, "min_volume_z": -99}},
    )
    strategy.initialize()
    signals = strategy.generate_signals(df)
    # Should only allow close/flat transitions, not fresh entry when gate is blocked.
    assert all(sig.signal_type in {SignalType.CLOSE_LONG, SignalType.CLOSE_SHORT} for sig in signals) or signals == []
    if signals:
        assert all(sig.metadata.get("gate_status", {}).get("all_ok") is False for sig in signals)

