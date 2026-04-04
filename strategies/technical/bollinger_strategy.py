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
                take_profit=current_close + (current_middle - current_lower),
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
                take_profit=current_close - (current_upper - current_middle),
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
    """Bollinger squeeze breakout strategy."""

    def __init__(
        self,
        name: str = "Bollinger_Squeeze",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "period": 20,
            "num_std": 2.0,
            "squeeze_threshold": 0.02,  # Bandwidth threshold used to define a squeeze.
            "breakout_threshold": 0.01,  # Minimum breakout distance beyond the band.
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.08,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def _calculate_bollinger_bands(self, data: pd.DataFrame) -> tuple:
        """Compute Bollinger Bands and bandwidth."""
        period = self.params["period"]
        num_std = self.params["num_std"]

        middle = data["close"].rolling(period).mean()
        std = data["close"].rolling(period).std()

        upper = middle + num_std * std
        lower = middle - num_std * std
        bandwidth = (upper - lower) / middle

        return upper, middle, lower, bandwidth

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """Generate trading signals."""
        if data.empty or len(data) < self.params["period"] + 5:
            return []

        signals = []

        upper, middle, lower, bandwidth = self._calculate_bollinger_bands(data)

        current_close = float(data["close"].iloc[-1])
        current_bandwidth = float(bandwidth.iloc[-1])
        prev_bandwidth = float(bandwidth.iloc[-2])

        current_upper = float(upper.iloc[-1])
        current_lower = float(lower.iloc[-1])
        current_middle = float(middle.iloc[-1])
        breakout_threshold = max(0.0, float(self.params.get("breakout_threshold", 0.0) or 0.0))
        stop_loss_pct = max(0.0, float(self.params.get("stop_loss_pct", 0.0) or 0.0))

        timestamp = datetime.now()
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        # Detect a breakout only after the previous bar was in a squeeze regime.
        # This keeps the strategy aligned with the configured squeeze threshold.
        was_squeezed = prev_bandwidth <= self.params["squeeze_threshold"] and current_bandwidth >= prev_bandwidth
        breakout_up = 0.0
        breakout_down = 0.0
        if current_middle != 0.0:
            breakout_up = max(0.0, float((current_close - current_upper) / current_middle))
            breakout_down = max(0.0, float((current_lower - current_close) / current_middle))

        if was_squeezed:
            # Upside breakout.
            if breakout_up >= breakout_threshold:
                signal = Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=current_close,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.8,
                    stop_loss=current_close * (1 - stop_loss_pct),
                    take_profit=current_close * (1 + self.params["take_profit_pct"]),
                    metadata={
                        "bandwidth": current_bandwidth,
                        "breakout": "up",
                        "breakout_pct": breakout_up,
                        "breakout_threshold": breakout_threshold,
                        "upper": current_upper,
                        "middle": current_middle,
                        "lower": current_lower,
                    }
                )
                signals.append(signal)
                logger.info(f"Bollinger squeeze breakout UP for {symbol}")

            # Downside breakout.
            elif breakout_down >= breakout_threshold:
                signal = Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=current_close,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.8,
                    stop_loss=current_close * (1 + stop_loss_pct),
                    take_profit=current_close * (1 - self.params["take_profit_pct"]),
                    metadata={
                        "bandwidth": current_bandwidth,
                        "breakout": "down",
                        "breakout_pct": breakout_down,
                        "breakout_threshold": breakout_threshold,
                        "upper": current_upper,
                        "middle": current_middle,
                        "lower": current_lower,
                    }
                )
                signals.append(signal)
                logger.info(f"Bollinger squeeze breakout DOWN for {symbol}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """Describe required market data."""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["period"] + 10,
        }
