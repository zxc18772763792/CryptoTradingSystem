"""
订单簿因子

基于订单簿深度数据的交易因子。
"""
from dataclasses import dataclass
from typing import Dict, Any
import numpy as np
import pandas as pd

from core.factors_ts.base import TimeSeriesFactor


# ============================================================
# 价差因子
# ============================================================

@dataclass
class SpreadPctFactor(TimeSeriesFactor):
    """价差百分比"""
    def __init__(self):
        super().__init__(
            name="spread_pct",
            inputs=("spread_pct",),
            lookback=1,
            params={}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df["spread_pct"], errors="coerce")


@dataclass
class SpreadZscoreFactor(TimeSeriesFactor):
    """价差 Z-score"""
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"spread_zscore_{period}",
            inputs=("spread_pct",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["spread_pct"], errors="coerce")
        
        mean = value.rolling(n, min_periods=7).mean()
        std = value.rolling(n, min_periods=7).std()
        
        return ((value - mean) / std.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


# ============================================================
# 深度不平衡因子
# ============================================================

@dataclass
class DepthImbalanceFactor(TimeSeriesFactor):
    """深度不平衡"""
    def __init__(self):
        super().__init__(
            name="depth_imbalance",
            inputs=("depth_imbalance",),
            lookback=1,
            params={}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df["depth_imbalance"], errors="coerce")


@dataclass
class DepthImbalanceMAFactor(TimeSeriesFactor):
    """深度不平衡移动平均"""
    def __init__(self, period: int = 10):
        period = int(period)
        super().__init__(
            name=f"depth_imbalance_ma_{period}",
            inputs=("depth_imbalance",),
            lookback=period,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["depth_imbalance"], errors="coerce")
        return value.rolling(n, min_periods=3).mean()


@dataclass
class DepthPressureSignalFactor(TimeSeriesFactor):
    """
    深度压力信号
    
    检测显著的买卖压力。
    """
    def __init__(self, threshold: float = 0.3):
        threshold = float(threshold)
        super().__init__(
            name=f"depth_pressure_{int(threshold*100)}",
            inputs=("depth_imbalance",),
            lookback=1,
            params={"threshold": threshold}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        threshold = float(self.params["threshold"])
        value = pd.to_numeric(df["depth_imbalance"], errors="coerce")
        
        # 1 = 显著买压, -1 = 显著卖压, 0 = 中性
        signal = pd.Series(0, index=df.index)
        signal[value > threshold] = 1
        signal[value < -threshold] = -1
        
        return signal


# ============================================================
# 流动性因子
# ============================================================

@dataclass
class BidDepthFactor(TimeSeriesFactor):
    """买单深度"""
    def __init__(self, levels: int = 10):
        levels = int(levels)
        super().__init__(
            name=f"bid_depth_{levels}",
            inputs=(f"bid_depth_{levels}",),
            lookback=1,
            params={"levels": levels}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["levels"])
        col = f"bid_depth_{n}"
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
        return pd.Series(np.nan, index=df.index)


@dataclass
class AskDepthFactor(TimeSeriesFactor):
    """卖单深度"""
    def __init__(self, levels: int = 10):
        levels = int(levels)
        super().__init__(
            name=f"ask_depth_{levels}",
            inputs=(f"ask_depth_{levels}",),
            lookback=1,
            params={"levels": levels}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["levels"])
        col = f"ask_depth_{n}"
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
        return pd.Series(np.nan, index=df.index)


@dataclass
class LiquidityZscoreFactor(TimeSeriesFactor):
    """
    流动性 Z-score
    
    总深度与历史比较。
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"liquidity_zscore_{period}",
            inputs=("total_depth",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["total_depth"], errors="coerce")
        
        mean = value.rolling(n, min_periods=7).mean()
        std = value.rolling(n, min_periods=7).std()
        
        return ((value - mean) / std.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


# ============================================================
# 订单簿倾斜因子
# ============================================================

@dataclass
class OrderBookSlopeFactor(TimeSeriesFactor):
    """
    订单簿倾斜因子
    
    检测大单在哪个价位堆积。
    """
    def __init__(self):
        super().__init__(
            name="ob_slope",
            inputs=("bid_slope", "ask_slope"),
            lookback=1,
            params={}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        bid_slope = pd.to_numeric(df.get("bid_slope", 0), errors="coerce")
        ask_slope = pd.to_numeric(df.get("ask_slope", 0), errors="coerce")
        
        # 正值 = 买单倾斜 (支撑强)
        # 负值 = 卖单倾斜 (阻力强)
        return bid_slope - ask_slope


# ============================================================
# 因子注册
# ============================================================

ORDERBOOK_FACTOR_CLASS_MAP = {
    "spread_pct": SpreadPctFactor,
    "spread_zscore": SpreadZscoreFactor,
    "depth_imbalance": DepthImbalanceFactor,
    "depth_imbalance_ma": DepthImbalanceMAFactor,
    "depth_pressure": DepthPressureSignalFactor,
    "bid_depth": BidDepthFactor,
    "ask_depth": AskDepthFactor,
    "liquidity_zscore": LiquidityZscoreFactor,
    "ob_slope": OrderBookSlopeFactor,
}


def get_orderbook_factor(name: str, **params) -> TimeSeriesFactor:
    """获取订单簿因子实例"""
    if name not in ORDERBOOK_FACTOR_CLASS_MAP:
        raise ValueError(f"Unknown orderbook factor: {name}")
    return ORDERBOOK_FACTOR_CLASS_MAP[name](**params)


def list_orderbook_factors() -> list:
    """列出所有可用的订单簿因子"""
    return list(ORDERBOOK_FACTOR_CLASS_MAP.keys())