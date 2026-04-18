from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from core.strategies.strategy_base import Signal, SignalType, StrategyBase


class RSIStrategy(StrategyBase):
    """RSI overbought/oversold reversal strategy."""

    def __init__(self, name: str = "RSI", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 14,
            "oversold": 30,
            "overbought": 70,
            "exit_oversold": 40,
            "exit_overbought": 60,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._regime_bias: Dict[str, int] = {}

    def _calculate_rsi(self, data: pd.DataFrame, period: int) -> pd.Series:
        delta = data["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < int(self.params["period"]) + 5:
            return []

        rsi = self._calculate_rsi(data, int(self.params["period"]))
        current_rsi = float(rsi.iloc[-1])
        prev_rsi = float(rsi.iloc[-2])
        current_price = float(data["close"].iloc[-1])
        timestamp = datetime.now(timezone.utc)
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        oversold = float(self.params["oversold"])
        overbought = float(self.params["overbought"])
        exit_oversold = float(self.params.get("exit_oversold", 40))
        exit_overbought = float(self.params.get("exit_overbought", 60))
        signals: List[Signal] = []

        if prev_rsi < oversold <= current_rsi:
            strength = min(1.0, max(0.1, (oversold - prev_rsi) / max(oversold, 1e-9) * 1.5 + 0.3))
            self._regime_bias[symbol] = 1
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    stop_loss=current_price * (1 - float(self.params["stop_loss_pct"])),
                    take_profit=current_price * (1 + float(self.params["take_profit_pct"])),
                    metadata={"rsi": current_rsi},
                )
            )
            logger.info(f"RSI oversold bounce for {symbol}: RSI={current_rsi:.2f}")
        elif prev_rsi > overbought >= current_rsi:
            strength = min(1.0, max(0.1, (prev_rsi - overbought) / max(100 - overbought, 1e-9) * 1.5 + 0.3))
            self._regime_bias[symbol] = -1
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    stop_loss=current_price * (1 + float(self.params["stop_loss_pct"])),
                    take_profit=current_price * (1 - float(self.params["take_profit_pct"])),
                    metadata={"rsi": current_rsi},
                )
            )
            logger.info(f"RSI overbought decline for {symbol}: RSI={current_rsi:.2f}")
        elif int(self._regime_bias.get(symbol, 0) or 0) > 0 and prev_rsi < exit_oversold <= current_rsi:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.CLOSE_LONG,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.6,
                    metadata={
                        "rsi": current_rsi,
                        "exit_threshold": exit_oversold,
                        "reason": "rsi_long_exit",
                    },
                )
            )
            self._regime_bias.pop(symbol, None)
            logger.info(f"RSI long exit for {symbol}: RSI={current_rsi:.2f}")
        elif int(self._regime_bias.get(symbol, 0) or 0) < 0 and prev_rsi > exit_overbought >= current_rsi:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.CLOSE_SHORT,
                    price=current_price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.6,
                    metadata={
                        "rsi": current_rsi,
                        "exit_threshold": exit_overbought,
                        "reason": "rsi_short_exit",
                    },
                )
            )
            self._regime_bias.pop(symbol, None)
            logger.info(f"RSI short exit for {symbol}: RSI={current_rsi:.2f}")

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": int(self.params["period"]) + 10,
        }


class RSIDivergenceStrategy(StrategyBase):
    """RSI divergence strategy (pure pandas implementation)."""

    def __init__(self, name: str = "RSI_Divergence", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 14,
            "lookback": 20,
            "min_divergence": 0.02,
            "extrema_order": 5,
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.08,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def _calculate_rsi(self, data: pd.DataFrame, period: int) -> pd.Series:
        delta = data["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _find_peaks(series: pd.Series, order: int = 5) -> pd.Series:
        window = int(max(1, order) * 2 + 1)
        vals = pd.to_numeric(series, errors="coerce")
        rolling_max = vals.rolling(window=window, center=True, min_periods=window).max()
        peaks = (vals == rolling_max) & vals.notna()
        border = int(max(1, order))
        if len(peaks) > border:
            peaks.iloc[:border] = False
            peaks.iloc[-border:] = False
        return peaks.fillna(False)

    @staticmethod
    def _find_troughs(series: pd.Series, order: int = 5) -> pd.Series:
        window = int(max(1, order) * 2 + 1)
        vals = pd.to_numeric(series, errors="coerce")
        rolling_min = vals.rolling(window=window, center=True, min_periods=window).min()
        troughs = (vals == rolling_min) & vals.notna()
        border = int(max(1, order))
        if len(troughs) > border:
            troughs.iloc[:border] = False
            troughs.iloc[-border:] = False
        return troughs.fillna(False)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        period = int(self.params["period"])
        lookback = int(self.params["lookback"])
        if data.empty or len(data) < lookback + period:
            return []

        rsi = self._calculate_rsi(data, period)
        order = int(self.params.get("extrema_order", 5))

        price_peaks = self._find_peaks(data["close"], order=order)
        price_troughs = self._find_troughs(data["close"], order=order)
        rsi_peaks = self._find_peaks(rsi, order=order)
        rsi_troughs = self._find_troughs(rsi, order=order)

        current_price = float(data["close"].iloc[-1])
        timestamp = datetime.now(timezone.utc)
        symbol = data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

        signals: List[Signal] = []
        min_div = float(self.params["min_divergence"])

        recent_troughs = data["close"][price_troughs].tail(2)
        recent_rsi_troughs = rsi[rsi_troughs].tail(2)
        if len(recent_troughs) >= 2 and len(recent_rsi_troughs) >= 2:
            price_trend = (float(recent_troughs.iloc[-1]) - float(recent_troughs.iloc[-2])) / max(float(recent_troughs.iloc[-2]), 1e-9)
            rsi_trend = float(recent_rsi_troughs.iloc[-1]) - float(recent_rsi_troughs.iloc[-2])
            if price_trend < -min_div and rsi_trend > 0:
                signals.append(
                    Signal(
                        symbol=symbol,
                        signal_type=SignalType.BUY,
                        price=current_price,
                        timestamp=timestamp,
                        strategy_name=self.name,
                        strength=max(0.1, min(abs(rsi_trend) / 20, 1.0)),
                        stop_loss=current_price * (1 - float(self.params["stop_loss_pct"])),
                        take_profit=current_price * (1 + float(self.params["take_profit_pct"])),
                        metadata={"type": "bullish_divergence", "rsi": float(rsi.iloc[-1])},
                    )
                )

        recent_peaks = data["close"][price_peaks].tail(2)
        recent_rsi_peaks = rsi[rsi_peaks].tail(2)
        if len(recent_peaks) >= 2 and len(recent_rsi_peaks) >= 2:
            price_trend = (float(recent_peaks.iloc[-1]) - float(recent_peaks.iloc[-2])) / max(float(recent_peaks.iloc[-2]), 1e-9)
            rsi_trend = float(recent_rsi_peaks.iloc[-1]) - float(recent_rsi_peaks.iloc[-2])
            if price_trend > min_div and rsi_trend < 0:
                signals.append(
                    Signal(
                        symbol=symbol,
                        signal_type=SignalType.SELL,
                        price=current_price,
                        timestamp=timestamp,
                        strategy_name=self.name,
                        strength=max(0.1, min(abs(rsi_trend) / 20, 1.0)),
                        stop_loss=current_price * (1 + float(self.params["stop_loss_pct"])),
                        take_profit=current_price * (1 - float(self.params["take_profit_pct"])),
                        metadata={"type": "bearish_divergence", "rsi": float(rsi.iloc[-1])},
                    )
                )

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": int(self.params["lookback"]) + int(self.params["period"]) + 10,
        }
