"""
移动平均策略
"""
from typing import Optional, List, Dict, Any, Tuple
import pandas as pd
import numpy as np
from loguru import logger

from core.strategies.strategy_base import (
    StrategyBase,
    Signal,
    SignalType,
)


def _latest_bar_context(data: pd.DataFrame) -> Tuple[object, str]:
    """Use the latest completed bar as the signal context for live/backtest parity."""
    timestamp = pd.Timestamp(data.index[-1]).to_pydatetime()
    symbol = "UNKNOWN"
    if "symbol" in data.columns and not data["symbol"].empty:
        symbol = str(data["symbol"].iloc[-1] or "UNKNOWN")
    return timestamp, symbol


class MAStrategy(StrategyBase):
    """移动平均交叉策略"""

    def __init__(
        self,
        name: str = "MA_Cross",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "fast_period": 10,
            "slow_period": 30,
            "signal_threshold": 0.001,  # 信号阈值
            "stop_loss_pct": 0.02,  # 止损比例
            "take_profit_pct": 0.05,  # 止盈比例
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["slow_period"]:
            return []

        signals = []

        # 计算移动平均
        fast_ma = data["close"].rolling(self.params["fast_period"]).mean()
        slow_ma = data["close"].rolling(self.params["slow_period"]).mean()

        # 计算差值
        diff = (fast_ma - slow_ma) / slow_ma

        # 当前和上一个状态
        current_diff = diff.iloc[-1]
        prev_diff = diff.iloc[-2]

        current_price = data["close"].iloc[-1]
        timestamp, symbol = _latest_bar_context(data)

        # 金叉：快线上穿慢线
        if prev_diff < self.params["signal_threshold"] and current_diff >= self.params["signal_threshold"]:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_diff) * 10, 1.0),
                stop_loss=current_price * (1 - self.params["stop_loss_pct"]),
                take_profit=current_price * (1 + self.params["take_profit_pct"]),
                metadata={
                    "fast_ma": fast_ma.iloc[-1],
                    "slow_ma": slow_ma.iloc[-1],
                    "diff": current_diff,
                }
            )
            signals.append(signal)
            logger.info(f"MA Golden Cross detected for {symbol} at {current_price}")

        # 死叉：快线下穿慢线
        elif prev_diff > -self.params["signal_threshold"] and current_diff <= -self.params["signal_threshold"]:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_diff) * 10, 1.0),
                stop_loss=current_price * (1 + self.params["stop_loss_pct"]),
                take_profit=current_price * (1 - self.params["take_profit_pct"]),
                metadata={
                    "fast_ma": fast_ma.iloc[-1],
                    "slow_ma": slow_ma.iloc[-1],
                    "diff": current_diff,
                }
            )
            signals.append(signal)
            logger.info(f"MA Death Cross detected for {symbol} at {current_price}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["slow_period"] + 10,
        }


class EMAStrategy(StrategyBase):
    """EMA策略（使用指数移动平均）"""

    def __init__(
        self,
        name: str = "EMA_Cross",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "fast_period": 12,
            "slow_period": 26,
            "signal_threshold": 0.002,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["slow_period"]:
            return []

        signals = []

        # 计算EMA
        fast_ema = data["close"].ewm(span=self.params["fast_period"], adjust=False).mean()
        slow_ema = data["close"].ewm(span=self.params["slow_period"], adjust=False).mean()

        # 计算差值
        diff = (fast_ema - slow_ema) / slow_ema

        current_diff = diff.iloc[-1]
        prev_diff = diff.iloc[-2]

        current_price = data["close"].iloc[-1]
        timestamp, symbol = _latest_bar_context(data)

        # 金叉
        if prev_diff < self.params["signal_threshold"] and current_diff >= self.params["signal_threshold"]:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_diff) * 10, 1.0),
                stop_loss=current_price * (1 - self.params["stop_loss_pct"]),
                take_profit=current_price * (1 + self.params["take_profit_pct"]),
            )
            signals.append(signal)

        # 死叉
        elif prev_diff > -self.params["signal_threshold"] and current_diff <= -self.params["signal_threshold"]:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_diff) * 10, 1.0),
                stop_loss=current_price * (1 + self.params["stop_loss_pct"]),
                take_profit=current_price * (1 - self.params["take_profit_pct"]),
            )
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["slow_period"] + 10,
        }
