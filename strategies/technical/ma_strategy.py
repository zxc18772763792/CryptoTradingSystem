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
    bar_time,
)


def _latest_bar_context(data: pd.DataFrame) -> Tuple[object, str]:
    """Use the latest completed bar as the signal context for live/backtest parity."""
    timestamp = bar_time(data)
    symbol = "UNKNOWN"
    if "symbol" in data.columns and not data["symbol"].empty:
        symbol = str(data["symbol"].iloc[-1] or "UNKNOWN")
    return timestamp, symbol


def _position_attr(position: Any, name: str, default: Any = None) -> Any:
    if isinstance(position, dict):
        return position.get(name, default)
    return getattr(position, name, default)


class MAStrategy(StrategyBase):
    """移动平均交叉策略"""

    mutates_input = False  # only reads data["close"].rolling(...).mean()

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
        if data.empty or len(data) < self.params["slow_period"] + 1:
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

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        """Exit when fast/slow MA spread compresses toward zero (trend lost).

        - LONG  exit: prev_diff >= +threshold but current_diff <= +threshold/2
        - SHORT exit: prev_diff <= -threshold but current_diff >= -threshold/2

        Half-threshold acts as an "early warning" before full death-cross — gives back
        less profit than waiting for full reverse signal.
        """
        if data.empty or len(data) < self.params["slow_period"] + 1:
            return None
        try:
            fast_ma = data["close"].rolling(self.params["fast_period"]).mean()
            slow_ma = data["close"].rolling(self.params["slow_period"]).mean()
            diff = (fast_ma - slow_ma) / slow_ma
            current_diff = float(diff.iloc[-1])
            prev_diff = float(diff.iloc[-2])
            current_price = float(data["close"].iloc[-1])
        except Exception:
            return None
        if not np.isfinite([current_diff, prev_diff, current_price]).all():
            return None

        threshold = float(self.params["signal_threshold"])
        soft_exit = threshold * 0.5
        raw_side = _position_attr(position, "side")
        side = str(getattr(raw_side, "value", raw_side) or "").lower()
        symbol = str(_position_attr(position, "symbol") or (data["symbol"].iloc[-1] if "symbol" in data else "UNKNOWN"))

        metadata = {
            "ma_diff": current_diff,
            "prev_ma_diff": prev_diff,
            "soft_exit_threshold": soft_exit,
            "close_only": True,
            "close_reason": "ma_diff_compression",
        }
        if side == "long" and prev_diff >= threshold and current_diff <= soft_exit:
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_LONG,
                price=current_price,
                timestamp=_latest_bar_context(data)[0],
                strategy_name=self.name,
                strength=0.7,
                metadata=metadata,
            )
        if side == "short" and prev_diff <= -threshold and current_diff >= -soft_exit:
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_SHORT,
                price=current_price,
                timestamp=_latest_bar_context(data)[0],
                strategy_name=self.name,
                strength=0.7,
                metadata=metadata,
            )
        return None

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["slow_period"] + 10,
        }


class EMAStrategy(StrategyBase):
    """EMA策略（使用指数移动平均）"""

    mutates_input = False  # only reads data["close"]

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
        if data.empty or len(data) < self.params["slow_period"] + 1:
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

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        """Exit when fast/slow EMA spread compresses toward zero."""
        if data.empty or len(data) < self.params["slow_period"] + 1:
            return None
        try:
            fast_ema = data["close"].ewm(span=self.params["fast_period"], adjust=False).mean()
            slow_ema = data["close"].ewm(span=self.params["slow_period"], adjust=False).mean()
            diff = (fast_ema - slow_ema) / slow_ema
            current_diff = float(diff.iloc[-1])
            prev_diff = float(diff.iloc[-2])
            current_price = float(data["close"].iloc[-1])
        except Exception:
            return None
        if not np.isfinite([current_diff, prev_diff, current_price]).all():
            return None

        threshold = float(self.params["signal_threshold"])
        soft_exit = threshold * 0.5
        raw_side = _position_attr(position, "side")
        side = str(getattr(raw_side, "value", raw_side) or "").lower()
        symbol = str(_position_attr(position, "symbol") or (data["symbol"].iloc[-1] if "symbol" in data else "UNKNOWN"))

        metadata = {
            "ema_diff": current_diff,
            "prev_ema_diff": prev_diff,
            "soft_exit_threshold": soft_exit,
            "close_only": True,
            "close_reason": "ema_diff_compression",
        }
        if side == "long" and prev_diff >= threshold and current_diff <= soft_exit:
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_LONG,
                price=current_price,
                timestamp=_latest_bar_context(data)[0],
                strategy_name=self.name,
                strength=0.7,
                metadata=metadata,
            )
        if side == "short" and prev_diff <= -threshold and current_diff >= -soft_exit:
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_SHORT,
                price=current_price,
                timestamp=_latest_bar_context(data)[0],
                strategy_name=self.name,
                strength=0.7,
                metadata=metadata,
            )
        return None

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["slow_period"] + 10,
        }
