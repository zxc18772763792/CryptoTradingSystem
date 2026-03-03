"""Concrete time-series factors for 5m high-frequency research."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any

import numpy as np
import pandas as pd

from core.factors_ts.base import TimeSeriesFactor


def _safe_std(x: pd.Series, window: int) -> pd.Series:
    return x.rolling(window, min_periods=max(3, min(window, 5))).std(ddof=0)


@dataclass
class RetLogFactor(TimeSeriesFactor):
    def __init__(self, n: int = 1):
        super().__init__(name=f"ret_log_{n}", inputs=("close",), lookback=max(1, int(n)) + 1, params={"n": int(n)})

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params.get("n", 1))
        close = pd.to_numeric(df["close"], errors="coerce")
        return np.log(close / close.shift(n))


@dataclass
class EMASlopeFactor(TimeSeriesFactor):
    def __init__(self, fast: int = 8, slow: int = 21):
        fast = int(fast)
        slow = int(slow)
        super().__init__(
            name=f"ema_slope_{fast}_{slow}",
            inputs=("close",),
            lookback=max(fast, slow) * 3,
            params={"fast": fast, "slow": slow},
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        close = pd.to_numeric(df["close"], errors="coerce")
        fast = int(self.params["fast"])
        slow = int(self.params["slow"])
        ema_fast = close.ewm(span=fast, adjust=False, min_periods=fast).mean()
        ema_slow = close.ewm(span=slow, adjust=False, min_periods=slow).mean()
        denom = close.replace(0, np.nan)
        return ((ema_fast - ema_slow) / denom).replace([np.inf, -np.inf], np.nan)


@dataclass
class ZScorePriceFactor(TimeSeriesFactor):
    def __init__(self, lookback: int = 30):
        lookback = int(lookback)
        super().__init__(name=f"zscore_price_{lookback}", inputs=("close",), lookback=lookback + 1, params={"lookback": lookback})

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["lookback"])
        close = pd.to_numeric(df["close"], errors="coerce")
        mean = close.rolling(n, min_periods=max(5, n // 3)).mean()
        std = _safe_std(close, n).replace(0, np.nan)
        return ((close - mean) / std).replace([np.inf, -np.inf], np.nan)


@dataclass
class RealizedVolFactor(TimeSeriesFactor):
    def __init__(self, lookback: int = 60):
        lookback = int(lookback)
        super().__init__(name=f"realized_vol_{lookback}", inputs=("close",), lookback=lookback + 2, params={"lookback": lookback})

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["lookback"])
        close = pd.to_numeric(df["close"], errors="coerce")
        ret = np.log(close / close.shift(1))
        return _safe_std(ret, n)


@dataclass
class ATRPctFactor(TimeSeriesFactor):
    def __init__(self, lookback: int = 30):
        lookback = int(lookback)
        super().__init__(name=f"atr_pct_{lookback}", inputs=("high", "low", "close"), lookback=lookback + 2, params={"lookback": lookback})

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["lookback"])
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        prev_close = close.shift(1)
        tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        atr = tr.rolling(n, min_periods=max(5, n // 3)).mean()
        return (atr / close.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


@dataclass
class SpreadProxyFactor(TimeSeriesFactor):
    def __init__(self):
        super().__init__(name="spread_proxy", inputs=("high", "low", "close"), lookback=2)

    def compute(self, df: pd.DataFrame) -> pd.Series:
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce").replace(0, np.nan)
        return ((high - low) / close).replace([np.inf, -np.inf], np.nan)


@dataclass
class VolumeZFactor(TimeSeriesFactor):
    def __init__(self, lookback: int = 60):
        lookback = int(lookback)
        super().__init__(name=f"volume_z_{lookback}", inputs=("volume",), lookback=lookback + 1, params={"lookback": lookback})

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["lookback"])
        vol = pd.to_numeric(df["volume"], errors="coerce")
        mean = vol.rolling(n, min_periods=max(5, n // 3)).mean()
        std = _safe_std(vol, n).replace(0, np.nan)
        return ((vol - mean) / std).replace([np.inf, -np.inf], np.nan)


FACTOR_CLASS_MAP = {
    "ret_log": RetLogFactor,
    "ema_slope": EMASlopeFactor,
    "zscore_price": ZScorePriceFactor,
    "realized_vol": RealizedVolFactor,
    "atr_pct": ATRPctFactor,
    "spread_proxy": SpreadProxyFactor,
    "volume_z": VolumeZFactor,
}

# Import extended factors and merge
try:
    from core.factors_ts.extended_factors import EXTENDED_FACTOR_CLASS_MAP
    FACTOR_CLASS_MAP.update(EXTENDED_FACTOR_CLASS_MAP)
except ImportError:
    pass

