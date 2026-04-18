"""
均值回归策略
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


class MeanReversionStrategy(StrategyBase):
    """Z-score mean-reversion strategy."""

    def __init__(
        self,
        name: str = "Mean_Reversion",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "lookback_period": 20,
            "entry_z_score": 2.0,  # Entry threshold for the z-score.
            "exit_z_score": 0.5,  # Exit threshold once price normalizes.
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)
        self._regime_bias: Dict[str, int] = {}

    def _calculate_z_score(self, data: pd.DataFrame) -> pd.Series:
        """Compute the rolling z-score."""
        period = self.params["lookback_period"]
        rolling_mean = data["close"].rolling(period).mean()
        rolling_std = data["close"].rolling(period).std()
        z_score = (data["close"] - rolling_mean) / rolling_std
        return z_score

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """Generate trading signals."""
        if data.empty or len(data) < self.params["lookback_period"]:
            return []

        signals = []

        z_score = self._calculate_z_score(data)

        if len(z_score) < 2:
            return []

        current_z = float(z_score.iloc[-1])
        prev_z = float(z_score.iloc[-2])
        entry_z = max(0.0, float(self.params["entry_z_score"]))
        exit_z = max(0.0, float(self.params.get("exit_z_score", 0.0)))

        current_price = data["close"].iloc[-1]
        timestamp = datetime.now(timezone.utc)
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        # Buy when price recovers from an oversold z-score extreme.
        if prev_z < -entry_z and current_z >= -entry_z:
            self._regime_bias[symbol] = 1
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_z) / max(entry_z, 1e-9), 1.0),
                stop_loss=current_price * (1 - self.params["stop_loss_pct"]),
                take_profit=current_price * (1 + self.params["take_profit_pct"]),
                metadata={"z_score": current_z, "entry_z_score": entry_z, "exit_z_score": exit_z}
            )
            signals.append(signal)
            logger.info(f"Mean reversion BUY for {symbol}: Z-score={current_z:.2f}")

        # Sell when price cools off from an overbought z-score extreme.
        elif prev_z > entry_z and current_z <= entry_z:
            self._regime_bias[symbol] = -1
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_z) / max(entry_z, 1e-9), 1.0),
                stop_loss=current_price * (1 + self.params["stop_loss_pct"]),
                take_profit=current_price * (1 - self.params["take_profit_pct"]),
                metadata={"z_score": current_z, "entry_z_score": entry_z, "exit_z_score": exit_z}
            )
            signals.append(signal)
            logger.info(f"Mean reversion SELL for {symbol}: Z-score={current_z:.2f}")
        elif int(self._regime_bias.get(symbol, 0) or 0) > 0 and prev_z < -exit_z <= current_z:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.CLOSE_LONG,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.6,
                    metadata={"z_score": current_z, "exit_z_score": exit_z, "reason": "mean_reversion_long_exit"},
                )
            )
            self._regime_bias.pop(symbol, None)
            logger.info(f"Mean reversion CLOSE_LONG for {symbol}: Z-score={current_z:.2f}")
        elif int(self._regime_bias.get(symbol, 0) or 0) < 0 and prev_z > exit_z >= current_z:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.CLOSE_SHORT,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.6,
                    metadata={"z_score": current_z, "exit_z_score": exit_z, "reason": "mean_reversion_short_exit"},
                )
            )
            self._regime_bias.pop(symbol, None)
            logger.info(f"Mean reversion CLOSE_SHORT for {symbol}: Z-score={current_z:.2f}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """Describe required market data."""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["lookback_period"] + 5,
        }


class BollingerMeanReversionStrategy(StrategyBase):
    """布林带均值回归策略"""

    def __init__(
        self,
        name: str = "BB_Mean_Reversion",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "period": 20,
            "num_std": 2.0,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.04,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        """生成交易信号"""
        if data.empty or len(data) < self.params["period"]:
            return []

        signals = []

        period = self.params["period"]
        num_std = self.params["num_std"]

        middle = data["close"].rolling(period).mean()
        std = data["close"].rolling(period).std()
        upper = middle + num_std * std
        lower = middle - num_std * std

        current_close = data["close"].iloc[-1]
        current_upper = upper.iloc[-1]
        current_lower = lower.iloc[-1]
        current_middle = middle.iloc[-1]

        prev_close = data["close"].iloc[-2]
        prev_upper = upper.iloc[-2]
        prev_lower = lower.iloc[-2]

        timestamp = datetime.now(timezone.utc)
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        # 价格从下轨反弹
        if prev_close <= prev_lower and current_close > current_lower:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_close,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=0.7,
                stop_loss=current_lower * 0.98,
                take_profit=current_middle,
                metadata={"band": "lower"}
            )
            signals.append(signal)

        # 价格从上轨回落
        elif prev_close >= prev_upper and current_close < current_upper:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_close,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=0.7,
                stop_loss=current_upper * 1.02,
                take_profit=current_middle,
                metadata={"band": "upper"}
            )
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """获取所需数据"""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["period"] + 5,
        }
