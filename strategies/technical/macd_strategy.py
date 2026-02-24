"""
MACD策略
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


class MACDStrategy(StrategyBase):
    """MACD策略"""

    def __init__(
        self,
        name: str = "MACD",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def _calculate_macd(self, data: pd.DataFrame) -> tuple:
        """计算MACD"""
        fast = self.params["fast_period"]
        slow = self.params["slow_period"]
        signal = self.params["signal_period"]

        ema_fast = data["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = data["close"].ewm(span=slow, adjust=False).mean()

        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        histogram = macd - signal_line

        return macd, signal_line, histogram

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["slow_period"] + self.params["signal_period"]:
            return []

        signals = []

        macd, signal_line, histogram = self._calculate_macd(data)

        current_macd = macd.iloc[-1]
        current_signal = signal_line.iloc[-1]
        current_hist = histogram.iloc[-1]

        prev_macd = macd.iloc[-2]
        prev_signal = signal_line.iloc[-2]
        prev_hist = histogram.iloc[-2]

        current_price = data["close"].iloc[-1]
        timestamp = datetime.now()
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        # MACD金叉：MACD上穿信号线
        if prev_macd <= prev_signal and current_macd > current_signal:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_hist) * 10, 1.0),
                stop_loss=current_price * (1 - self.params["stop_loss_pct"]),
                take_profit=current_price * (1 + self.params["take_profit_pct"]),
                metadata={
                    "macd": current_macd,
                    "signal": current_signal,
                    "histogram": current_hist,
                }
            )
            signals.append(signal)
            logger.info(f"MACD golden cross for {symbol}: MACD={current_macd:.4f}")

        # MACD死叉：MACD下穿信号线
        elif prev_macd >= prev_signal and current_macd < current_signal:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_hist) * 10, 1.0),
                stop_loss=current_price * (1 + self.params["stop_loss_pct"]),
                take_profit=current_price * (1 - self.params["take_profit_pct"]),
                metadata={
                    "macd": current_macd,
                    "signal": current_signal,
                    "histogram": current_hist,
                }
            )
            signals.append(signal)
            logger.info(f"MACD death cross for {symbol}: MACD={current_macd:.4f}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["slow_period"] + self.params["signal_period"] + 10,
        }


class MACDHistogramStrategy(StrategyBase):
    """MACD柱状图策略"""

    def __init__(
        self,
        name: str = "MACD_Histogram",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9,
            "min_histogram": 0.0001,  # 最小柱状图阈值
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def _calculate_macd(self, data: pd.DataFrame) -> tuple:
        """计算MACD"""
        fast = self.params["fast_period"]
        slow = self.params["slow_period"]
        signal = self.params["signal_period"]

        ema_fast = data["close"].ewm(span=fast, adjust=False).mean()
        ema_slow = data["close"].ewm(span=slow, adjust=False).mean()

        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        histogram = macd - signal_line

        return macd, signal_line, histogram

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["slow_period"] + self.params["signal_period"]:
            return []

        signals = []

        macd, signal_line, histogram = self._calculate_macd(data)

        current_hist = histogram.iloc[-1]
        prev_hist = histogram.iloc[-2]

        current_price = data["close"].iloc[-1]
        timestamp = datetime.now()
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        # 柱状图由负转正（多头动能增强）
        if prev_hist < 0 and current_hist > 0:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_hist) * 100, 1.0),
                stop_loss=current_price * (1 - self.params["stop_loss_pct"]),
                take_profit=current_price * (1 + self.params["take_profit_pct"]),
                metadata={"histogram": current_hist}
            )
            signals.append(signal)

        # 柱状图由正转负（空头动能增强）
        elif prev_hist > 0 and current_hist < 0:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_hist) * 100, 1.0),
                stop_loss=current_price * (1 + self.params["stop_loss_pct"]),
                take_profit=current_price * (1 - self.params["take_profit_pct"]),
                metadata={"histogram": current_hist}
            )
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["slow_period"] + self.params["signal_period"] + 10,
        }
