"""
未平仓合约 (OI) 因子

基于 OI 数据的交易因子。
"""
from dataclasses import dataclass
from typing import Dict, Any
import numpy as np
import pandas as pd

from core.factors_ts.base import TimeSeriesFactor


# ============================================================
# OI 基础因子
# ============================================================

@dataclass
class OIValueFactor(TimeSeriesFactor):
    """OI 价值"""
    def __init__(self):
        super().__init__(
            name="oi_value",
            inputs=("oi_value",),
            lookback=1,
            params={}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df["oi_value"], errors="coerce")


@dataclass
class OIZscoreFactor(TimeSeriesFactor):
    """OI Z-score"""
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"oi_zscore_{period}",
            inputs=("oi_value",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["oi_value"], errors="coerce")
        
        mean = value.rolling(n, min_periods=7).mean()
        std = value.rolling(n, min_periods=7).std()
        
        return ((value - mean) / std.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


@dataclass
class OIChangeFactor(TimeSeriesFactor):
    """OI 变化率"""
    def __init__(self, period: int = 1):
        period = int(period)
        super().__init__(
            name=f"oi_change_{period}",
            inputs=("oi_value",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["oi_value"], errors="coerce")
        prev = value.shift(n)
        return ((value - prev) / prev.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


@dataclass
class OIMomentumFactor(TimeSeriesFactor):
    """OI 动量"""
    def __init__(self, period: int = 7):
        period = int(period)
        super().__init__(
            name=f"oi_mom_{period}",
            inputs=("oi_value",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["oi_value"], errors="coerce")
        return value.diff(n)


# ============================================================
# OI 信号因子
# ============================================================

@dataclass
class OIExtremeFactor(TimeSeriesFactor):
    """
    OI 极端值检测
    
    检测 OI 是否处于极端高位或低位。
    """
    def __init__(self, threshold: float = 2.0):
        threshold = float(threshold)
        super().__init__(
            name=f"oi_extreme_{int(threshold)}",
            inputs=("oi_value",),
            lookback=31,
            params={"threshold": threshold}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        threshold = float(self.params["threshold"])
        value = pd.to_numeric(df["oi_value"], errors="coerce")
        
        mean = value.rolling(30, min_periods=7).mean()
        std = value.rolling(30, min_periods=7).std()
        zscore = ((value - mean) / std.replace(0, np.nan))
        
        # 1 = 极端高 OI (拥挤做多), -1 = 极端低 OI
        signal = pd.Series(0, index=df.index)
        signal[zscore > threshold] = 1
        signal[zscore < -threshold] = -1
        
        return signal


@dataclass
class OIPriceDivergenceFactor(TimeSeriesFactor):
    """
    OI-价格背离因子
    
    OI 上升但价格下跌 = 空头增加 (看跌)
    OI 下降但价格上升 = 空头平仓 (看涨)
    """
    def __init__(self, period: int = 5):
        period = int(period)
        super().__init__(
            name=f"oi_price_divergence_{period}",
            inputs=("oi_value", "close"),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        oi = pd.to_numeric(df["oi_value"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        
        oi_change = oi.diff(n)
        price_change = close.diff(n)
        
        # 计算背离
        # 1 = 看涨背离 (OI下降+价格上涨 = 空头平仓)
        # -1 = 看跌背离 (OI上升+价格下跌 = 空头增加)
        divergence = pd.Series(0, index=df.index)
        
        # 看涨: OI下降 + 价格上涨
        divergence[(oi_change < 0) & (price_change > 0)] = 1
        
        # 看跌: OI上升 + 价格下跌
        divergence[(oi_change > 0) & (price_change < 0)] = -1
        
        return divergence


@dataclass
class OISurgeFactor(TimeSeriesFactor):
    """
    OI 激增因子
    
    检测 OI 短期内大幅增加。
    """
    def __init__(self, threshold: float = 0.1):
        threshold = float(threshold)
        super().__init__(
            name=f"oi_surge_{int(threshold*100)}",
            inputs=("oi_value",),
            lookback=2,
            params={"threshold": threshold}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        threshold = float(self.params["threshold"])
        value = pd.to_numeric(df["oi_value"], errors="coerce")
        
        change = value.pct_change()
        
        # 1 = OI 激增 (> threshold)
        return (change > threshold).astype(int)


# ============================================================
# 因子注册
# ============================================================

OI_FACTOR_CLASS_MAP = {
    "oi_value": OIValueFactor,
    "oi_zscore": OIZscoreFactor,
    "oi_change": OIChangeFactor,
    "oi_mom": OIMomentumFactor,
    "oi_extreme": OIExtremeFactor,
    "oi_price_divergence": OIPriceDivergenceFactor,
    "oi_surge": OISurgeFactor,
}


def get_oi_factor(name: str, **params) -> TimeSeriesFactor:
    """获取 OI 因子实例"""
    if name not in OI_FACTOR_CLASS_MAP:
        raise ValueError(f"Unknown OI factor: {name}")
    return OI_FACTOR_CLASS_MAP[name](**params)


def list_oi_factors() -> list:
    """列出所有可用的 OI 因子"""
    return list(OI_FACTOR_CLASS_MAP.keys())