"""
布林带策略
"""
from datetime import datetime, timezone
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
            "exit_confirm_pct": 0.001,
            "exit_min_profit_pct": 0.006,
            "exit_min_profit_atr_mult": 0.5,
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
        if data.empty or len(data) < self.params["period"] + 1:
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

        timestamp = self._bar_time(data)
        symbol = str(data["symbol"].iloc[0]) if "symbol" in data and len(data) else "UNKNOWN"

        # 计算价格在布林带中的位置
        band_width = current_upper - current_lower
        if band_width <= 0:
            return []
        bb_position = (current_close - current_lower) / band_width

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

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        if data.empty or len(data) < self.params["period"] + 1:
            return None

        _upper, middle, _lower = self._calculate_bollinger_bands(data)
        current_close = float(data["close"].iloc[-1])
        prev_close = float(data["close"].iloc[-2])
        current_middle = float(middle.iloc[-1])
        prev_middle = float(middle.iloc[-2])
        if not np.isfinite([current_close, prev_close, current_middle, prev_middle]).all():
            return None

        if isinstance(position, dict):
            raw_side = position.get("side")
            symbol = str(position.get("symbol") or (data["symbol"].iloc[-1] if "symbol" in data else "UNKNOWN"))
            entry_price = float(position.get("entry_price") or 0.0)
        else:
            raw_side = getattr(position, "side", None)
            symbol = str(getattr(position, "symbol", None) or (data["symbol"].iloc[-1] if "symbol" in data else "UNKNOWN"))
            entry_price = float(getattr(position, "entry_price", 0.0) or 0.0)
        side = str(getattr(raw_side, "value", raw_side) or "").lower()
        if entry_price <= 0:
            return None

        atr_pct = self.compute_atr_pct(data, default=0.01)
        confirm_pct = max(0.0, float(self.params.get("exit_confirm_pct", 0.001) or 0.0))
        min_profit_pct = max(
            0.0,
            float(self.params.get("exit_min_profit_pct", 0.006) or 0.0),
            float(atr_pct or 0.0) * max(0.0, float(self.params.get("exit_min_profit_atr_mult", 0.5) or 0.0)),
        )
        metadata = {
            "middle": current_middle,
            "close_reason": "bollinger_middle_reversion",
            "close_only": True,
            "atr_pct": float(atr_pct or 0.0),
            "exit_confirm_pct": confirm_pct,
            "exit_min_profit_pct": min_profit_pct,
        }

        if side == "long":
            pnl_pct = (current_close - entry_price) / entry_price
            if (
                pnl_pct >= min_profit_pct
                and prev_close < prev_middle
                and current_close >= current_middle * (1.0 + confirm_pct)
            ):
                metadata["pnl_pct"] = pnl_pct
                return Signal(
                    symbol=symbol,
                    signal_type=SignalType.CLOSE_LONG,
                    price=current_close,
                    timestamp=self._bar_time(data),
                    strategy_name=self.name,
                    strength=0.8,
                    metadata=metadata,
                )
        if side == "short":
            pnl_pct = (entry_price - current_close) / entry_price
            if (
                pnl_pct >= min_profit_pct
                and prev_close > prev_middle
                and current_close <= current_middle * (1.0 - confirm_pct)
            ):
                metadata["pnl_pct"] = pnl_pct
                return Signal(
                    symbol=symbol,
                    signal_type=SignalType.CLOSE_SHORT,
                    price=current_close,
                    timestamp=self._bar_time(data),
                    strategy_name=self.name,
                    strength=0.8,
                    metadata=metadata,
                )
        return None

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

        timestamp = self._bar_time(data)
        symbol = str(data["symbol"].iloc[0]) if "symbol" in data and len(data) else "UNKNOWN"

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

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        """Exit when squeeze re-contracts — breakout has failed.

        After a squeeze breakout, if bandwidth shrinks back below the squeeze
        threshold (or contracts vs prior bar in a way that suggests the move
        is fading), the trend thesis is invalidated. Close to lock whatever
        partial profit exists rather than wait for SL.
        """
        if data.empty or len(data) < self.params["period"] + 5:
            return None
        try:
            _upper, _middle, _lower, bandwidth = self._calculate_bollinger_bands(data)
            current_bandwidth = float(bandwidth.iloc[-1])
            prev_bandwidth = float(bandwidth.iloc[-2])
            current_close = float(data["close"].iloc[-1])
        except Exception:
            return None
        if not np.isfinite([current_bandwidth, prev_bandwidth, current_close]).all():
            return None

        squeeze_threshold = float(self.params.get("squeeze_threshold", 0.02))
        # Trigger when bandwidth contracts AND drops back into squeeze regime.
        recontracting = (
            current_bandwidth < prev_bandwidth
            and current_bandwidth <= squeeze_threshold * 1.1
        )
        if not recontracting:
            return None

        raw_side = position.side if hasattr(position, "side") else (position or {}).get("side")
        side = str(getattr(raw_side, "value", raw_side) or "").lower()
        symbol = str(getattr(position, "symbol", None) or (data["symbol"].iloc[-1] if "symbol" in data else "UNKNOWN"))

        metadata = {
            "bandwidth": current_bandwidth,
            "prev_bandwidth": prev_bandwidth,
            "squeeze_threshold": squeeze_threshold,
            "close_only": True,
            "close_reason": "bollinger_squeeze_recontract",
        }
        if side == "long":
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_LONG,
                price=current_close,
                timestamp=self._bar_time(data),
                strategy_name=self.name,
                strength=0.7,
                metadata=metadata,
            )
        if side == "short":
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_SHORT,
                price=current_close,
                timestamp=self._bar_time(data),
                strategy_name=self.name,
                strength=0.7,
                metadata=metadata,
            )
        return None

    def get_required_data(self) -> Dict[str, Any]:
        """Describe required market data."""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["period"] + 10,
        }
