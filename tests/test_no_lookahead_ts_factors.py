import numpy as np
import pandas as pd

from core.factors_ts.registry import compute_factor


def _sample_df(rows: int = 240) -> pd.DataFrame:
    rng = np.random.default_rng(123)
    idx = pd.date_range("2025-01-01", periods=rows, freq="5min")
    close = 50000 + np.cumsum(rng.normal(0, 20, rows))
    close = pd.Series(close, index=idx).abs() + 100
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 8, rows))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 8, rows))
    volume = np.abs(rng.normal(2000, 300, rows)) + 100
    return pd.DataFrame(
        {"open": open_.values, "high": high.values, "low": low.values, "close": close.values, "volume": volume},
        index=idx,
    )


def _assert_no_lookahead(name: str, params: dict, warmup: int = 80) -> None:
    df = _sample_df()
    full = compute_factor(name, df, params=params)
    for i in range(warmup, len(df)):
        part = df.iloc[: i + 1]
        part_val = compute_factor(name, part, params=params).iloc[-1]
        full_val = full.iloc[i]
        if pd.isna(part_val) and pd.isna(full_val):
            continue
        assert np.isclose(float(part_val), float(full_val), equal_nan=True)


def test_ts_factors_no_lookahead():
    _assert_no_lookahead("ret_log", {"n": 5}, warmup=20)
    _assert_no_lookahead("ema_slope", {"fast": 8, "slow": 21}, warmup=40)
    _assert_no_lookahead("zscore_price", {"lookback": 30}, warmup=60)
    _assert_no_lookahead("realized_vol", {"lookback": 60}, warmup=80)
    _assert_no_lookahead("atr_pct", {"lookback": 30}, warmup=60)
    _assert_no_lookahead("volume_z", {"lookback": 60}, warmup=80)

