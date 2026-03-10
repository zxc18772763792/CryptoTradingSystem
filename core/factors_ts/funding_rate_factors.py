"""
资金费率因子

基于资金费率数据的交易因子，用于生成交易信号。
"""
from dataclasses import dataclass
from typing import Dict, Any
import numpy as np
import pandas as pd

from core.factors_ts.base import TimeSeriesFactor


# ============================================================
# 基础资金费率因子
# ============================================================

@dataclass
class FundingRateFactor(TimeSeriesFactor):
    """
    当前资金费率因子
    
    直接使用资金费率作为因子值。
    正值表示多头付费给空头，负值相反。
    """
    def __init__(self):
        super().__init__(
            name="funding_rate",
            inputs=("funding_rate",),
            lookback=1,
            params={}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(df["funding_rate"], errors="coerce")


@dataclass
class FundingRateAbsFactor(TimeSeriesFactor):
    """
    资金费率绝对值因子
    
    衡量资金费率的强度，不区分方向。
    """
    def __init__(self):
        super().__init__(
            name="funding_rate_abs",
            inputs=("funding_rate",),
            lookback=1,
            params={}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        return rate.abs()


# ============================================================
# 标准化因子
# ============================================================

@dataclass
class FundingRateZscoreFactor(TimeSeriesFactor):
    """
    资金费率 Z-score 因子
    
    将当前资金费率标准化，与历史均值比较。
    Z-score > 2: 费率极端高于历史均值
    Z-score < -2: 费率极端低于历史均值
    
    Args:
        period: 回看窗口期数
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"funding_rate_zscore_{period}",
            inputs=("funding_rate",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        
        mean = rate.rolling(n, min_periods=max(5, n // 2)).mean()
        std = rate.rolling(n, min_periods=max(5, n // 2)).std()
        
        zscore = (rate - mean) / std.replace(0, np.nan)
        return zscore.replace([np.inf, -np.inf], np.nan)


@dataclass
class FundingRatePercentileFactor(TimeSeriesFactor):
    """
    资金费率百分位因子
    
    当前费率在历史中的百分位排名。
    100 = 历史最高
    0 = 历史最低
    
    Args:
        period: 回看窗口期数
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"funding_rate_percentile_{period}",
            inputs=("funding_rate",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        
        def rank_pct(series):
            if len(series.dropna()) < 3:
                return np.nan
            return (series.rank().iloc[-1] - 1) / (len(series.dropna()) - 1) * 100
        
        return rate.rolling(n, min_periods=5).apply(rank_pct, raw=False)


# ============================================================
# 动量因子
# ============================================================

@dataclass
class FundingRateMomentumFactor(TimeSeriesFactor):
    """
    资金费率动量因子
    
    费率的变化趋势，用于识别市场情绪变化。
    正值 = 费率上升 (多头情绪增强)
    负值 = 费率下降 (空头情绪增强)
    
    Args:
        period: 变化周期
    """
    def __init__(self, period: int = 8):
        period = int(period)
        super().__init__(
            name=f"funding_rate_mom_{period}",
            inputs=("funding_rate",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        return rate.diff(n)


@dataclass
class FundingRateAccelerationFactor(TimeSeriesFactor):
    """
    资金费率加速度因子
    
    费率变化的二阶导数，用于识别趋势反转。
    """
    def __init__(self, fast: int = 4, slow: int = 12):
        fast = int(fast)
        slow = int(slow)
        super().__init__(
            name=f"funding_rate_accel_{fast}_{slow}",
            inputs=("funding_rate",),
            lookback=slow + 2,
            params={"fast": fast, "slow": slow}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        fast = int(self.params["fast"])
        slow = int(self.params["slow"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        
        fast_mom = rate.diff(fast)
        slow_mom = rate.diff(slow)
        
        # 加速度 = 快速动量 - 慢速动量
        accel = fast_mom - slow_mom / slow * fast
        return accel


@dataclass
class FundingRateROCFactor(TimeSeriesFactor):
    """
    资金费率变化率因子 (Rate of Change)
    
    Args:
        period: 计算周期
    """
    def __init__(self, period: int = 8):
        period = int(period)
        super().__init__(
            name=f"funding_rate_roc_{period}",
            inputs=("funding_rate",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        prev_rate = rate.shift(n)
        
        # 避免除零
        roc = (rate - prev_rate) / prev_rate.abs().replace(0, np.nan)
        return roc.replace([np.inf, -np.inf], np.nan)


# ============================================================
# 极端值因子
# ============================================================

@dataclass
class FundingRateExtremeFactor(TimeSeriesFactor):
    """
    极端资金费率因子
    
    检测极端正/负费率，用于反向交易。
    
    Args:
        threshold: 极端值阈值 (默认 0.0005 = 0.05%)
    """
    def __init__(self, threshold: float = 0.0005):
        threshold = float(threshold)
        super().__init__(
            name=f"funding_rate_extreme_{threshold}",
            inputs=("funding_rate",),
            lookback=1,
            params={"threshold": threshold}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        threshold = float(self.params["threshold"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        
        # 1 = 极端正 (做空信号)
        # -1 = 极端负 (做多信号)
        # 0 = 中性
        result = pd.Series(0, index=df.index)
        result[rate > threshold] = 1
        result[rate < -threshold] = -1
        
        return result


@dataclass
class FundingRateExtremeCountFactor(TimeSeriesFactor):
    """
    极端费率连续次数因子
    
    统计连续出现极端费率的次数。
    连续多次极端费率表示强烈的市场情绪。
    
    Args:
        threshold: 极端值阈值
        period: 回看期数
    """
    def __init__(self, threshold: float = 0.0005, period: int = 8):
        threshold = float(threshold)
        period = int(period)
        super().__init__(
            name=f"funding_rate_extreme_count_{threshold}_{period}",
            inputs=("funding_rate",),
            lookback=period + 1,
            params={"threshold": threshold, "period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        threshold = float(self.params["threshold"])
        n = int(self.params["period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        
        # 标记极端值
        is_extreme = (rate.abs() > threshold).astype(int)
        
        # 滚动求和
        return is_extreme.rolling(n, min_periods=1).sum()


# ============================================================
# 均值回归因子
# ============================================================

@dataclass
class FundingRateMeanReversionFactor(TimeSeriesFactor):
    """
    资金费率均值回归因子
    
    计算当前费率与长期均值的偏离度。
    假设费率会回归均值，极端偏离时产生交易信号。
    
    Args:
        fast_period: 短期窗口
        slow_period: 长期窗口
    """
    def __init__(self, fast_period: int = 8, slow_period: int = 30):
        fast_period = int(fast_period)
        slow_period = int(slow_period)
        super().__init__(
            name=f"funding_rate_mean_reversion_{fast_period}_{slow_period}",
            inputs=("funding_rate",),
            lookback=slow_period + 1,
            params={"fast_period": fast_period, "slow_period": slow_period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        fast = int(self.params["fast_period"])
        slow = int(self.params["slow_period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        
        fast_mean = rate.rolling(fast, min_periods=3).mean()
        slow_mean = rate.rolling(slow, min_periods=10).mean()
        
        # 短期均值与长期均值的偏离
        deviation = (fast_mean - slow_mean) / slow_mean.abs().replace(0, np.nan)
        return deviation.replace([np.inf, -np.inf], np.nan)


@dataclass
class FundingRateDeviationFactor(TimeSeriesFactor):
    """
    资金费率偏离因子
    
    当前费率与 N 期均值的绝对偏离。
    
    Args:
        period: 均值计算周期
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"funding_rate_deviation_{period}",
            inputs=("funding_rate",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        mean = rate.rolling(n, min_periods=5).mean()
        
        return rate - mean


# ============================================================
# 波动率因子
# ============================================================

@dataclass
class FundingRateVolatilityFactor(TimeSeriesFactor):
    """
    资金费率波动率因子
    
    费率的滚动标准差，衡量市场不确定性。
    高波动率 = 市场分歧大
    
    Args:
        period: 计算周期
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"funding_rate_vol_{period}",
            inputs=("funding_rate",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        return rate.rolling(n, min_periods=5).std()


@dataclass
class FundingRateRangeFactor(TimeSeriesFactor):
    """
    资金费率区间因子
    
    费率在 N 期内的区间 (最高 - 最低)。
    
    Args:
        period: 计算周期
    """
    def __init__(self, period: int = 30):
        period = int(period)
        super().__init__(
            name=f"funding_rate_range_{period}",
            inputs=("funding_rate",),
            lookback=period + 1,
            params={"period": period}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        n = int(self.params["period"])
        rate = pd.to_numeric(df["funding_rate"], errors="coerce")
        
        high = rate.rolling(n, min_periods=5).max()
        low = rate.rolling(n, min_periods=5).min()
        
        return high - low


# ============================================================
# 跨交易所因子
# ============================================================

@dataclass
class FundingRateArbitrageFactor(TimeSeriesFactor):
    """
    资金费率套利因子
    
    计算不同交易所之间费率的差异。
    大差异可能表示套利机会。
    
    注意: 此因子需要多交易所数据作为输入列
    输入列: funding_rate_binance, funding_rate_bybit, etc.
    
    Args:
        exchanges: 交易所列表
    """
    def __init__(self, exchanges: tuple = ("binance", "bybit")):
        exchanges = tuple(exchanges)
        super().__init__(
            name=f"funding_rate_arb_{'_'.join(exchanges)}",
            inputs=tuple(f"funding_rate_{ex}" for ex in exchanges),
            lookback=1,
            params={"exchanges": exchanges}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        exchanges = self.params["exchanges"]
        
        rates = []
        for ex in exchanges:
            col = f"funding_rate_{ex}"
            if col in df.columns:
                rates.append(pd.to_numeric(df[col], errors="coerce"))
        
        if not rates:
            return pd.Series(np.nan, index=df.index)
        
        rates_df = pd.concat(rates, axis=1)
        
        # 最大差异 = 最高费率 - 最低费率
        max_rate = rates_df.max(axis=1)
        min_rate = rates_df.min(axis=1)
        
        return max_rate - min_rate


@dataclass
class FundingRateAvgFactor(TimeSeriesFactor):
    """
    资金费率均值因子 (跨交易所)
    
    计算多个交易所费率的加权平均。
    
    Args:
        exchanges: 交易所列表及其权重
    """
    def __init__(self, exchanges: tuple = ("binance", "bybit", "okx")):
        exchanges = tuple(exchanges)
        super().__init__(
            name=f"funding_rate_avg_{'_'.join(exchanges)}",
            inputs=tuple(f"funding_rate_{ex}" for ex in exchanges),
            lookback=1,
            params={"exchanges": exchanges}
        )

    def compute(self, df: pd.DataFrame) -> pd.Series:
        exchanges = self.params["exchanges"]
        
        rates = []
        for ex in exchanges:
            col = f"funding_rate_{ex}"
            if col in df.columns:
                rates.append(pd.to_numeric(df[col], errors="coerce"))
        
        if not rates:
            return pd.Series(np.nan, index=df.index)
        
        rates_df = pd.concat(rates, axis=1)
        return rates_df.mean(axis=1)


# ============================================================
# 因子注册
# ============================================================

FUNDING_RATE_FACTOR_CLASS_MAP = {
    # 基础
    "funding_rate": FundingRateFactor,
    "funding_rate_abs": FundingRateAbsFactor,
    
    # 标准化
    "funding_rate_zscore": FundingRateZscoreFactor,
    "funding_rate_percentile": FundingRatePercentileFactor,
    
    # 动量
    "funding_rate_mom": FundingRateMomentumFactor,
    "funding_rate_accel": FundingRateAccelerationFactor,
    "funding_rate_roc": FundingRateROCFactor,
    
    # 极端值
    "funding_rate_extreme": FundingRateExtremeFactor,
    "funding_rate_extreme_count": FundingRateExtremeCountFactor,
    
    # 均值回归
    "funding_rate_mean_reversion": FundingRateMeanReversionFactor,
    "funding_rate_deviation": FundingRateDeviationFactor,
    
    # 波动率
    "funding_rate_vol": FundingRateVolatilityFactor,
    "funding_rate_range": FundingRateRangeFactor,
    
    # 跨交易所
    "funding_rate_arb": FundingRateArbitrageFactor,
    "funding_rate_avg": FundingRateAvgFactor,
}


def get_funding_rate_factor(name: str, **params) -> TimeSeriesFactor:
    """
    获取资金费率因子实例
    
    Args:
        name: 因子名称
        **params: 因子参数
        
    Returns:
        TimeSeriesFactor 实例
    """
    if name not in FUNDING_RATE_FACTOR_CLASS_MAP:
        raise ValueError(f"Unknown funding rate factor: {name}")
    
    factor_class = FUNDING_RATE_FACTOR_CLASS_MAP[name]
    return factor_class(**params)


def list_funding_rate_factors() -> list:
    """列出所有可用的资金费率因子"""
    return list(FUNDING_RATE_FACTOR_CLASS_MAP.keys())