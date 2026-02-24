"""Strategy library and factor library regression tests."""
import numpy as np
import pandas as pd

import strategies as strategy_module
from strategies import ALL_STRATEGIES
from core.data.factor_library import build_factor_library


def _sample_ohlcv(symbol: str, rows: int = 420, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed=seed)
    index = pd.date_range(start="2025-01-01", periods=rows, freq="h")
    close = 50000 + np.cumsum(rng.normal(0, 80, size=rows))
    close = pd.Series(close, index=index).abs() + 1000
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 30, size=rows))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 30, size=rows))
    volume = np.abs(rng.normal(2000, 400, size=rows)) + 100

    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high,
            "low": low,
            "close": close.values,
            "volume": volume,
            "symbol": [symbol] * rows,
        },
        index=index,
    )


def test_strategy_library_sync_signals_do_not_crash():
    base_df = _sample_ohlcv("BTC/USDT", rows=420, seed=42)
    pair_df = _sample_ohlcv("ETH/USDT", rows=420, seed=99)

    for strategy_name in ALL_STRATEGIES:
        klass = getattr(strategy_module, strategy_name, None)
        if klass is None:
            continue

        strategy = klass(name=f"test_{strategy_name}", params={})
        required = strategy.get_required_data() or {}

        if bool(required.get("requires_pair", False)):
            signals = strategy.generate_signals(base_df, pair_df)  # type: ignore[arg-type]
        else:
            signals = strategy.generate_signals(base_df)

        assert isinstance(signals, list), f"{strategy_name} should return list"


def _factor_input(assets: int = 6, rows: int = 600, seed: int = 7):
    rng = np.random.default_rng(seed=seed)
    index = pd.date_range(start="2024-01-01", periods=rows, freq="h")

    close_cols = {}
    volume_cols = {}
    for i in range(assets):
        sym = f"ASSET{i+1}"
        drift = 0.02 + i * 0.01
        noise = rng.normal(0, 1.0 + i * 0.05, size=rows)
        path = 100 + np.cumsum(drift + noise)
        close = pd.Series(path, index=index).abs() + 1
        vol = np.abs(rng.normal(10000 + i * 500, 2000, size=rows)) + 100
        close_cols[sym] = close
        volume_cols[sym] = pd.Series(vol, index=index)

    close_df = pd.DataFrame(close_cols)
    volume_df = pd.DataFrame(volume_cols)
    return close_df, volume_df


def test_factor_library_builds_multi_factor_outputs():
    close_df, volume_df = _factor_input(assets=6, rows=600)
    result = build_factor_library(close_df=close_df, volume_df=volume_df, quantile=0.3)

    assert not result.factors.empty
    for col in ["MKT", "SMB", "MOM", "REV", "VOL", "LIQ", "VAL", "QMJ", "BAB"]:
        assert col in result.factors.columns

    assert not result.asset_scores.empty
    assert "symbol" in result.asset_scores.columns
    assert "score" in result.asset_scores.columns


def test_factor_library_supports_small_universe():
    close_df, volume_df = _factor_input(assets=2, rows=500, seed=11)
    result = build_factor_library(close_df=close_df, volume_df=volume_df, quantile=0.3)

    assert not result.factors.empty
    assert set(["MKT", "SMB", "MOM", "VOL"]).issubset(result.factors.columns)
