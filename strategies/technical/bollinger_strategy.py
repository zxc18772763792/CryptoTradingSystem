"""
布林带策略
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger

from core.strategies.strategy_base import (
    StrategyBase,
    Signal,
    SignalType,
)


class BollingerBandsStrategy(StrategyBase):
    """布林带策略"""

    def __init__(
        self,
        name: str = "Bollinger_Bands",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "period": 20,
            "num_std": 2.0,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def _calculate_bollinger_bands(self, data: pd.DataFrame) -> tuple:
        """计算布林带"""
        period = self.params["period"]
        num_std = self.params["num_std"]

        middle = data["close"].rolling(period).mean()
        std = data["close"].rolling(period).std()

        upper = middle + num_std * std
        lower = middle - num_std * std

        return upper, middle, lower

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["period"]:
            return []

        signals = []

        upper, middle, lower = self._calculate_bollinger_bands(data)

        current_close = data["close"].iloc[-1]
        current_upper = upper.iloc[-1]
        current_middle = middle.iloc[-1]
        current_lower = lower.iloc[-1]

        prev_close = data["close"].iloc[-2]
        prev_upper = upper.iloc[-2]
        prev_lower = lower.iloc[-2]

        timestamp = datetime.now()
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        # 计算价格在布林带中的位置
        bb_position = (current_close - current_lower) / (current_upper - current_lower)

        # 价格触及下轨后反弹 - 买入信号
        if prev_close <= prev_lower and current_close > current_lower:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_close,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=1 - bb_position,  # 越靠近下轨，信号越强
                stop_loss=current_close * (1 - self.params["stop_loss_pct"]),
                take_profit=current_middle,  # 止盈到中轨
                metadata={
                    "upper": current_upper,
                    "middle": current_middle,
                    "lower": current_lower,
                    "position": bb_position,
                }
            )
            signals.append(signal)
            logger.info(f"Bollinger lower band bounce for {symbol} at {current_close}")

        # 价格触及上轨后回落 - 卖出信号
        elif prev_close >= prev_upper and current_close < current_upper:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_close,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=bb_position,  # 越靠近上轨，信号越强
                stop_loss=current_close * (1 + self.params["stop_loss_pct"]),
                take_profit=current_middle,  # 止盈到中轨
                metadata={
                    "upper": current_upper,
                    "middle": current_middle,
                    "lower": current_lower,
                    "position": bb_position,
                }
            )
            signals.append(signal)
            logger.info(f"Bollinger upper band decline for {symbol} at {current_close}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["period"] + 5,
        }


class BollingerSqueezeStrategy(StrategyBase):
    """布林带挤压策略（突破策略）"""

    def __init__(
        self,
        name: str = "Bollinger_Squeeze",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "period": 20,
            "num_std": 2.0,
            "squeeze_threshold": 0.02,  # 挤压阈值（带宽比例）
            "breakout_threshold": 0.01,  # 突破阈值
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.08,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def _calculate_bollinger_bands(self, data: pd.DataFrame) -> tuple:
        """计算布林带"""
        period = self.params["period"]
        num_std = self.params["num_std"]

        middle = data["close"].rolling(period).mean()
        std = data["close"].rolling(period).std()

        upper = middle + num_std * std
        lower = middle - num_std * std
        bandwidth = (upper - lower) / middle

        return upper, middle, lower, bandwidth

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["period"] + 5:
            return []

        signals = []

        upper, middle, lower, bandwidth = self._calculate_bollinger_bands(data)

        current_close = data["close"].iloc[-1]
        current_bandwidth = bandwidth.iloc[-1]
        prev_bandwidth = bandwidth.iloc[-2]

        current_upper = upper.iloc[-1]
        current_lower = lower.iloc[-1]
        current_middle = middle.iloc[-1]

        timestamp = datetime.now()
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        # 检测挤压后的突破
        # 首先检查是否处于挤压状态（带宽很窄）
        was_squeezed = prev_bandwidth < self.params["squeeze_threshold"]

        if was_squeezed:
            # 向上突破
            if current_close > current_middle + (current_upper - current_middle) * 0.5:
                signal = Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=current_close,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.8,
                    stop_loss=current_lower * 0.99,
                    take_profit=current_close * (1 + self.params["take_profit_pct"]),
                    metadata={
                        "bandwidth": current_bandwidth,
                        "breakout": "up",
                    }
                )
                signals.append(signal)
                logger.info(f"Bollinger squeeze breakout UP for {symbol}")

            # 向下突破
            elif current_close < current_middle - (current_middle - current_lower) * 0.5:
                signal = Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=current_close,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.8,
                    stop_loss=current_upper * 1.01,
                    take_profit=current_close * (1 - self.params["take_profit_pct"]),
                    metadata={
                        "bandwidth": current_bandwidth,
                        "breakout": "down",
                    }
                )
                signals.append(signal)
                logger.info(f"Bollinger squeeze breakout DOWN for {symbol}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["period"] + 10,
        }
