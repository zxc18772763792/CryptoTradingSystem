"""
情绪因子

基于恐惧贪婪指数等情绪数据的交易因子。
"""
from dataclasses import dataclass
from typing import Dict, Any
import numpy as np
import pandas as pd

from core.factors_ts.base import TimeSeriesFactor


# ============================================================
# 恐惧贪婪指数因子
# ============================================================

@dataclass
class FearGreedFactor(TimeSeriesFactor):
    """当前恐惧贪婪指数"""
    def __init__(self):
        super().__init__(
            name="fear_greed",
            inputs=("fear_greed_value",),
            lookback=1,
            params={}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df["fear_greed_value"], errors="coerce")


@dataclass
class FearGreedZscoreFactor(TimeSeriesFactor):
    """
    恐惧贪婪指数 Z-score
    
    与历史均值比较，识别极端情绪。
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"fear_greed_zscore_{period}",
            inputs=("fear_greed_value",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["fear_greed_value"], errors="coerce")
        
        mean = value.rolling(n, min_periods=7).mean()
        std = value.rolling(n, min_periods=7).std()
        
        return ((value - mean) / std.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)


@dataclass
class FearGreedMomentumFactor(TimeSeriesFactor):
    """
    恐惧贪婪指数动量
    
    识别情绪变化趋势。
    """
    def __init__(self, period: int = 7):
        period = int(period)
        super().__init__(
            name=f"fear_greed_mom_{period}",
            inputs=("fear_greed_value",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["fear_greed_value"], errors="coerce")
        return value.diff(n)


@dataclass
class ExtremeFearSignalFactor(TimeSeriesFactor):
    """
    极度恐惧信号因子
    
    检测极度恐惧区域 (< 25)，作为买入信号。
    """
    def __init__(self, threshold: int = 25):
        threshold = int(threshold)
        super().__init__(
            name=f"extreme_fear_signal_{threshold}",
            inputs=("fear_greed_value",),
            lookback=1,
            params={"threshold": threshold}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        threshold = int(self.params["threshold"])
        value = pd.to_numeric(df["fear_greed_value"], errors="coerce")
        
        # 1 = 极度恐惧 (买入信号)
        return (value <= threshold).astype(int)


@dataclass
class ExtremeGreedSignalFactor(TimeSeriesFactor):
    """
    极度贪婪信号因子
    
    检测极度贪婪区域 (> 75)，作为卖出信号。
    """
    def __init__(self, threshold: int = 75):
        threshold = int(threshold)
        super().__init__(
            name=f"extreme_greed_signal_{threshold}",
            inputs=("fear_greed_value",),
            lookback=1,
            params={"threshold": threshold}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        threshold = int(self.params["threshold"])
        value = pd.to_numeric(df["fear_greed_value"], errors="coerce")
        
        # 1 = 极度贪婪 (卖出信号)
        return (value >= threshold).astype(int)


@dataclass
class SentimentReversalFactor(TimeSeriesFactor):
    """
    情绪反转因子
    
    检测从极度恐惧/贪婪区域的反转。
    """
    def __init__(self, period: int = 3):
        period = int(period)
        super().__init__(
            name=f"sentiment_reversal_{period}",
            inputs=("fear_greed_value",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        value = pd.to_numeric(df["fear_greed_value"], errors="coerce")
        
        # 从恐惧区反转向上
        was_fear = value.shift(n) <= 30
        is_higher = value > value.shift(n)
        bullish_reversal = (was_fear & is_higher).astype(int)
        
        # 从贪婪区反转向下
        was_greed = value.shift(n) >= 70
        is_lower = value < value.shift(n)
        bearish_reversal = (was_greed & is_lower).astype(int)
        
        # 1 = 看涨反转, -1 = 看跌反转, 0 = 无反转
        return bullish_reversal - bearish_reversal


# ============================================================
# 因子注册
# ============================================================

SENTIMENT_FACTOR_CLASS_MAP = {
    "fear_greed": FearGreedFactor,
    "fear_greed_zscore": FearGreedZscoreFactor,
    "fear_greed_mom": FearGreedMomentumFactor,
    "extreme_fear_signal": ExtremeFearSignalFactor,
    "extreme_greed_signal": ExtremeGreedSignalFactor,
    "sentiment_reversal": SentimentReversalFactor,
}


def get_sentiment_factor(name: str, **params) -> TimeSeriesFactor:
    """获取情绪因子实例"""
    if name not in SENTIMENT_FACTOR_CLASS_MAP:
        raise ValueError(f"Unknown sentiment factor: {name}")
    return SENTIMENT_FACTOR_CLASS_MAP[name](**params)


def list_sentiment_factors() -> list:
    """列出所有可用的情绪因子"""
    return list(SENTIMENT_FACTOR_CLASS_MAP.keys())