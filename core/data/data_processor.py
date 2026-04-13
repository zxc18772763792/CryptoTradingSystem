"""
数据处理器模块
提供数据清洗、特征计算、技术指标计算等功能
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
import numpy as np
import pandas as pd
from loguru import logger

from config.settings import settings


class DataProcessor:
    """数据处理器"""

    def __init__(self):
        pass

    # ==================== 数据清洗 ====================

    def clean_klines(
        self,
        df: pd.DataFrame,
        remove_outliers: bool = True,
        fill_gaps: bool = True,
    ) -> pd.DataFrame:
        """
        清洗K线数据

        Args:
            df: K线DataFrame
            remove_outliers: 是否移除异常值
            fill_gaps: 是否填充缺失数据

        Returns:
            清洗后的DataFrame
        """
        if df.empty:
            return df

        df = df.copy()

        # 移除重复数据
        df = df[~df.index.duplicated(keep="last")]

        # 移除零值或负值
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df = df[df[col] > 0]

        # 检查并修正high/low关系
        if "high" in df.columns and "low" in df.columns:
            mask = df["high"] < df["low"]
            if mask.any():
                logger.warning(f"Found {mask.sum()} rows where high < low, swapping values")
                df.loc[mask, ["high", "low"]] = df.loc[mask, ["low", "high"]].values

        # 移除异常值
        if remove_outliers:
            df = self._remove_price_outliers(df)

        # 填充时间缺口
        if fill_gaps:
            df = self._fill_time_gaps(df)

        return df.sort_index()

    def _remove_price_outliers(
        self,
        df: pd.DataFrame,
        n_std: float = 5.0,
    ) -> pd.DataFrame:
        """移除价格异常值"""
        if len(df) < 10:
            return df

        df = df.copy()

        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                continue

            returns = df[col].pct_change()
            mean = returns.mean()
            std = returns.std()

            if std > 0:
                mask = np.abs(returns - mean) > n_std * std
                if mask.any():
                    logger.info(f"Removing {mask.sum()} outliers from {col}")
                    df = df[~mask]

        return df

    def _fill_time_gaps(self, df: pd.DataFrame) -> pd.DataFrame:
        """填充时间缺口"""
        if len(df) < 2:
            return df

        # 检测时间频率
        freq = pd.infer_freq(df.index)
        if freq is None:
            return df

        # 重新采样填充
        df = df.asfreq(freq)

        # 前向填充价格
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = df[col].ffill()

        # 用0填充成交量
        if "volume" in df.columns:
            df["volume"] = df["volume"].fillna(0)

        return df

    # ==================== 特征计算 ====================

    def calculate_returns(
        self,
        df: pd.DataFrame,
        periods: List[int] = [1, 5, 10, 20],
    ) -> pd.DataFrame:
        """计算收益率"""
        df = df.copy()

        for period in periods:
            df[f"return_{period}"] = df["close"].pct_change(period)

        return df

    def calculate_volatility(
        self,
        df: pd.DataFrame,
        windows: List[int] = [10, 20, 60],
    ) -> pd.DataFrame:
        """计算波动率"""
        df = df.copy()

        returns = df["close"].pct_change()

        for window in windows:
            df[f"volatility_{window}"] = returns.rolling(window).std() * np.sqrt(252)

        return df

    def calculate_volume_features(
        self,
        df: pd.DataFrame,
        windows: List[int] = [5, 10, 20],
    ) -> pd.DataFrame:
        """计算成交量特征"""
        df = df.copy()

        for window in windows:
            df[f"volume_ma_{window}"] = df["volume"].rolling(window).mean()
            df[f"volume_ratio_{window}"] = df["volume"] / df[f"volume_ma_{window}"]

        return df

    def calculate_price_features(
        self,
        df: pd.DataFrame,
        windows: List[int] = [5, 10, 20, 60],
    ) -> pd.DataFrame:
        """计算价格特征"""
        df = df.copy()

        for window in windows:
            # 移动平均
            df[f"ma_{window}"] = df["close"].rolling(window).mean()

            # 价格相对位置
            rolling_max = df["high"].rolling(window).max()
            rolling_min = df["low"].rolling(window).min()
            df[f"price_position_{window}"] = (
                (df["close"] - rolling_min) / (rolling_max - rolling_min)
            )

            # 价格与均线偏离
            df[f"ma_deviation_{window}"] = (
                (df["close"] - df[f"ma_{window}"]) / df[f"ma_{window}"]
            )

        return df

    # ==================== 技术指标 ====================

    def add_ma(
        self,
        df: pd.DataFrame,
        windows: List[int] = [5, 10, 20, 60],
    ) -> pd.DataFrame:
        """添加移动平均线"""
        df = df.copy()

        for window in windows:
            df[f"ma_{window}"] = df["close"].rolling(window).mean()
            df[f"ema_{window}"] = df["close"].ewm(span=window, adjust=False).mean()

        return df

    def add_rsi(
        self,
        df: pd.DataFrame,
        periods: List[int] = [6, 14, 24],
    ) -> pd.DataFrame:
        """添加RSI指标"""
        df = df.copy()

        for period in periods:
            delta = df["close"].diff()
            gain = delta.where(delta > 0, 0)
            loss = (-delta).where(delta < 0, 0)

            avg_gain = gain.rolling(period).mean()
            avg_loss = loss.rolling(period).mean()

            rs = avg_gain / avg_loss.replace(0, np.nan)
            df[f"rsi_{period}"] = 100 - (100 / (1 + rs))

        return df

    def add_macd(
        self,
        df: pd.DataFrame,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> pd.DataFrame:
        """添加MACD指标"""
        df = df.copy()

        ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = df["close"].ewm(span=slow, adjust=False).mean()

        df["macd"] = ema_fast - ema_slow
        df["macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
        df["macd_hist"] = df["macd"] - df["macd_signal"]

        return df

    def add_bollinger_bands(
        self,
        df: pd.DataFrame,
        window: int = 20,
        num_std: float = 2.0,
    ) -> pd.DataFrame:
        """添加布林带"""
        df = df.copy()

        ma = df["close"].rolling(window).mean()
        std = df["close"].rolling(window).std()

        df["bb_middle"] = ma
        df["bb_upper"] = ma + num_std * std
        df["bb_lower"] = ma - num_std * std
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / ma
        df["bb_position"] = (df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])

        return df

    def add_atr(
        self,
        df: pd.DataFrame,
        period: int = 14,
    ) -> pd.DataFrame:
        """添加ATR（平均真实波幅）"""
        df = df.copy()

        high = df["high"]
        low = df["low"]
        close = df["close"].shift(1)

        tr1 = high - low
        tr2 = abs(high - close)
        tr3 = abs(low - close)

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df[f"atr_{period}"] = tr.rolling(period).mean()

        return df

    def add_kdj(
        self,
        df: pd.DataFrame,
        n: int = 9,
        m1: int = 3,
        m2: int = 3,
    ) -> pd.DataFrame:
        """添加KDJ指标"""
        df = df.copy()

        low_n = df["low"].rolling(n).min()
        high_n = df["high"].rolling(n).max()

        rsv = (df["close"] - low_n) / (high_n - low_n) * 100

        df["k"] = rsv.ewm(alpha=1/m1, adjust=False).mean()
        df["d"] = df["k"].ewm(alpha=1/m2, adjust=False).mean()
        df["j"] = 3 * df["k"] - 2 * df["d"]

        return df

    def add_all_indicators(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """添加所有技术指标"""
        df = df.copy()

        df = self.add_ma(df)
        df = self.add_rsi(df)
        df = self.add_macd(df)
        df = self.add_bollinger_bands(df)
        df = self.add_atr(df)
        df = self.add_kdj(df)

        return df

    # ==================== 数据转换 ====================

    def resample_klines(
        self,
        df: pd.DataFrame,
        target_timeframe: str,
    ) -> pd.DataFrame:
        """
        重采样K线数据

        Args:
            df: 原始K线数据
            target_timeframe: 目标时间框架（如 '1h', '4h', '1d'）
        """
        if df.empty:
            return df

        # 解析目标时间框架
        freq_map = {
            "1m": "1min", "3m": "3min", "5m": "5min",
            "15m": "15min", "30m": "30min",
            "1h": "1h", "2h": "2h", "4h": "4h",
            "6h": "6h", "12h": "12h",
            "1d": "1D", "3d": "3D", "1w": "1W",
        }

        freq = freq_map.get(target_timeframe)
        if not freq:
            raise ValueError(f"Unsupported timeframe: {target_timeframe}")

        agg_dict = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }

        resampled = df.resample(freq).agg(agg_dict)
        return resampled.dropna()


# 全局数据处理器实例
data_processor = DataProcessor()
