"""
MACD策略
"""
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

    mutates_input = False  # only reads data["close"]

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
        timestamp = self._bar_time(data)
        symbol = str(data["symbol"].iloc[0]) if "symbol" in data and len(data) else "UNKNOWN"

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

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        """Exit when MACD histogram flips against the open position.

        - LONG  exit: histogram was >= 0 last bar, now <= 0 (momentum lost)
        - SHORT exit: histogram was <= 0 last bar, now >= 0 (downside lost)
        """
        if data.empty or len(data) < self.params["slow_period"] + self.params["signal_period"] + 1:
            return None
        try:
            _macd, _signal_line, histogram = self._calculate_macd(data)
            current_hist = float(histogram.iloc[-1])
            prev_hist = float(histogram.iloc[-2])
            current_price = float(data["close"].iloc[-1])
        except Exception:
            return None
        if not np.isfinite([current_hist, prev_hist, current_price]).all():
            return None

        raw_side = position.side if hasattr(position, "side") else (position or {}).get("side")
        side = str(getattr(raw_side, "value", raw_side) or "").lower()
        symbol = str(getattr(position, "symbol", None) or (data["symbol"].iloc[-1] if "symbol" in data else "UNKNOWN"))

        metadata = {
            "macd_histogram": current_hist,
            "prev_macd_histogram": prev_hist,
            "close_only": True,
            "close_reason": "macd_histogram_flip",
        }
        if side == "long" and prev_hist >= 0 and current_hist <= 0:
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_LONG,
                price=current_price,
                timestamp=self._bar_time(data),
                strategy_name=self.name,
                strength=0.7,
                metadata=metadata,
            )
        if side == "short" and prev_hist <= 0 and current_hist >= 0:
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_SHORT,
                price=current_price,
                timestamp=self._bar_time(data),
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
            "min_length": self.params["slow_period"] + self.params["signal_period"] + 10,
        }


class MACDHistogramStrategy(StrategyBase):
    """MACD histogram crossover strategy."""

    def __init__(
        self,
        name: str = "MACD_Histogram",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9,
            "min_histogram": 0.0001,  # Ignore tiny histogram noise.
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)

        super().__init__(name, default_params)

    def _calculate_macd(self, data: pd.DataFrame) -> tuple:
        """Compute MACD values."""
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
        """Generate trading signals."""
        if data.empty or len(data) < self.params["slow_period"] + self.params["signal_period"]:
            return []

        signals = []

        _, _, histogram = self._calculate_macd(data)

        current_hist = float(histogram.iloc[-1])
        prev_hist = float(histogram.iloc[-2])
        min_histogram = max(0.0, float(self.params.get("min_histogram", 0.0) or 0.0))
        hist_scale = max(min_histogram, 1e-9)

        current_price = data["close"].iloc[-1]
        timestamp = self._bar_time(data)
        symbol = str(data["symbol"].iloc[0]) if "symbol" in data and len(data) else "UNKNOWN"

        # Bullish crossover with enough histogram expansion to avoid noise.
        if prev_hist <= -min_histogram and current_hist >= min_histogram:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.BUY,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_hist) / hist_scale, 1.0),
                stop_loss=current_price * (1 - self.params["stop_loss_pct"]),
                take_profit=current_price * (1 + self.params["take_profit_pct"]),
                metadata={
                    "histogram": current_hist,
                    "prev_histogram": prev_hist,
                    "min_histogram": min_histogram,
                }
            )
            signals.append(signal)

        # Bearish crossover with enough histogram expansion to avoid noise.
        elif prev_hist >= min_histogram and current_hist <= -min_histogram:
            signal = Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=min(abs(current_hist) / hist_scale, 1.0),
                stop_loss=current_price * (1 + self.params["stop_loss_pct"]),
                take_profit=current_price * (1 - self.params["take_profit_pct"]),
                metadata={
                    "histogram": current_hist,
                    "prev_histogram": prev_hist,
                    "min_histogram": min_histogram,
                }
            )
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        """Describe required market data."""
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["slow_period"] + self.params["signal_period"] + 10,
        }
