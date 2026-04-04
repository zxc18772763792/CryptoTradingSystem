"""
Factor-based trading strategies.

Each strategy corresponds to a specific time-series factor from the extended factor library.
Strategies generate signals based on factor value thresholds and transitions.
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


class FactorStrategyBase(StrategyBase):
    """Base class for factor-based strategies."""

    def __init__(
        self,
        name: str,
        params: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(name, params or {})

    def _get_symbol(self, data: pd.DataFrame) -> str:
        """Extract symbol from data."""
        return data.get("symbol", ["UNKNOWN"])[0] if "symbol" in data else "UNKNOWN"

    def _create_signal(
        self,
        symbol: str,
        signal_type: SignalType,
        price: float,
        strength: float = 1.0,
        metadata: Optional[Dict] = None
    ) -> Signal:
        """Create a trading signal."""
        return Signal(
            symbol=symbol,
            signal_type=signal_type,
            price=price,
            timestamp=datetime.now(),
            strategy_name=self.name,
            strength=min(max(strength, 0.0), 1.0),
            stop_loss=None,
            take_profit=None,
            metadata=metadata or {}
        )


# ============================================================
# Momentum and Trend Strategies
# ============================================================

class ROCStrategy(FactorStrategyBase):
    """Rate of Change momentum strategy.

    Generates buy signals when ROC crosses above threshold (positive momentum).
    Generates sell signals when ROC crosses below negative threshold.
    """

    def __init__(self, name: str = "ROC", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 14,
            "buy_threshold": 5.0,  # ROC above 5%
            "sell_threshold": -5.0,  # ROC below -5%
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.06,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] + 2:
            return []

        signals = []
        n = self.params["period"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate ROC
        roc = ((close - close.shift(n)) / close.shift(n) * 100)
        current_roc = roc.iloc[-1]
        prev_roc = roc.iloc[-2]

        buy_thresh = self.params["buy_threshold"]
        sell_thresh = self.params["sell_threshold"]

        # Buy signal: ROC crosses above buy threshold
        if prev_roc < buy_thresh and current_roc >= buy_thresh:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(current_roc / buy_thresh, 1.0),
                metadata={"roc": current_roc}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Sell signal: ROC crosses below sell threshold
        elif prev_roc > sell_thresh and current_roc <= sell_thresh:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(abs(current_roc / sell_thresh), 1.0),
                metadata={"roc": current_roc}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["period"] + 5,
        }


class PriceAccelerationStrategy(FactorStrategyBase):
    """Price acceleration strategy.

    Detects momentum acceleration/deceleration for early trend signals.
    """

    def __init__(self, name: str = "PriceAcceleration", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "fast": 5,
            "slow": 15,
            "accel_threshold": 0.1,
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["slow"] * 2 + 5:
            return []

        signals = []
        fast = self.params["fast"]
        slow = self.params["slow"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate acceleration
        fast_mom = close.pct_change(fast)
        slow_mom = close.pct_change(slow)
        accel = (fast_mom - slow_mom) / slow_mom.abs().replace(0, np.nan)

        current_accel = accel.iloc[-1]
        prev_accel = accel.iloc[-2]
        threshold = self.params["accel_threshold"]

        # Positive acceleration crossing threshold
        if prev_accel < threshold and current_accel >= threshold:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(current_accel / threshold, 1.0),
                metadata={"acceleration": current_accel}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Negative acceleration crossing threshold
        elif prev_accel > -threshold and current_accel <= -threshold:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(abs(current_accel / threshold), 1.0),
                metadata={"acceleration": current_accel}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["slow"] * 2 + 10,
        }


class AroonStrategy(FactorStrategyBase):
    """Aroon trend strength strategy.

    Uses Aroon oscillator for trend detection.
    """

    def __init__(self, name: str = "Aroon", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 25,
            "buy_threshold": 50,  # Aroon > 50 = uptrend
            "sell_threshold": -50,  # Aroon < -50 = downtrend
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.06,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] + 3:
            return []

        signals = []
        n = self.params["period"]
        high_arr = data["high"].values
        low_arr = data["low"].values
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Vectorized Aroon calculation (only compute last 2 values needed for signal)
        window = n + 1
        aroon_up_curr = float((n - np.argmax(high_arr[-window:])) / n * 100)
        aroon_down_curr = float((n - np.argmin(low_arr[-window:])) / n * 100)
        aroon_curr = aroon_up_curr - aroon_down_curr

        aroon_up_prev = float((n - np.argmax(high_arr[-window - 1:-1])) / n * 100)
        aroon_down_prev = float((n - np.argmin(low_arr[-window - 1:-1])) / n * 100)
        aroon_prev = aroon_up_prev - aroon_down_prev

        current_aroon = aroon_curr
        prev_aroon = aroon_prev

        # Buy signal: Aroon crosses above threshold
        if prev_aroon < self.params["buy_threshold"] and current_aroon >= self.params["buy_threshold"]:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(current_aroon / 100, 1.0),
                metadata={"aroon": current_aroon, "aroon_up": aroon_up_curr}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Sell signal: Aroon crosses below threshold
        elif prev_aroon > self.params["sell_threshold"] and current_aroon <= self.params["sell_threshold"]:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(abs(current_aroon) / 100, 1.0),
                metadata={"aroon": current_aroon, "aroon_down": aroon_down_curr}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close"],
            "min_length": self.params["period"] + 5,
        }


# ============================================================
# Volatility Strategies
# ============================================================

class ParkinsonVolStrategy(FactorStrategyBase):
    """Parkinson volatility mean reversion strategy.

    Uses high-low range volatility for overbought/oversold detection.
    """

    def __init__(self, name: str = "ParkinsonVol", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 20,
            "vol_percentile_low": 20,  # Low vol = buy
            "vol_percentile_high": 80,  # High vol = sell
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] * 3:
            return []

        signals = []
        n = self.params["period"]
        high = data["high"]
        low = data["low"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate Parkinson volatility
        hl_log = np.log(high / low.replace(0, np.nan))
        variance = (hl_log ** 2) / (4 * np.log(2))
        park_vol = np.sqrt(variance.rolling(n).mean())

        # Rolling percentile
        vol_percentile = park_vol.rolling(n * 2).rank(pct=True) * 100

        current_pct = vol_percentile.iloc[-1]
        prev_pct = vol_percentile.iloc[-2]

        # Low volatility regime - potential breakout
        if prev_pct > self.params["vol_percentile_low"] and current_pct <= self.params["vol_percentile_low"]:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=0.7,
                metadata={"vol_percentile": current_pct, "vol": park_vol.iloc[-1]}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # High volatility regime - potential reversal
        elif prev_pct < self.params["vol_percentile_high"] and current_pct >= self.params["vol_percentile_high"]:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=0.7,
                metadata={"vol_percentile": current_pct, "vol": park_vol.iloc[-1]}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close"],
            "min_length": self.params["period"] * 3 + 5,
        }


class UlcerIndexStrategy(FactorStrategyBase):
    """Ulcer Index risk-based strategy.

    Uses downside risk measure for position timing.
    """

    def __init__(self, name: str = "UlcerIndex", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 14,
            "high_risk_threshold": 10,  # UI > 10 = high risk, sell
            "low_risk_threshold": 3,  # UI < 3 = low risk, buy
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.06,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] + 2:
            return []

        signals = []
        n = self.params["period"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate Ulcer Index
        rolling_max = close.rolling(n).max()
        drawdown_pct = ((close - rolling_max) / rolling_max.replace(0, np.nan)) * 100
        ulcer = np.sqrt((drawdown_pct ** 2).rolling(n).mean())

        current_ulcer = ulcer.iloc[-1]
        prev_ulcer = ulcer.iloc[-2]

        # Low risk: Ulcer falling below threshold
        if prev_ulcer > self.params["low_risk_threshold"] and current_ulcer <= self.params["low_risk_threshold"]:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=0.8,
                metadata={"ulcer_index": current_ulcer}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # High risk: Ulcer rising above threshold
        elif prev_ulcer < self.params["high_risk_threshold"] and current_ulcer >= self.params["high_risk_threshold"]:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=0.8,
                metadata={"ulcer_index": current_ulcer}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["period"] + 5,
        }


# ============================================================
# Liquidity and Volume Strategies
# ============================================================

class MFIStrategy(FactorStrategyBase):
    """Money Flow Index strategy.

    Volume-weighted RSI for overbought/oversold detection.
    """

    def __init__(self, name: str = "MFI", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 14,
            "oversold": 20,  # MFI < 20 = oversold
            "overbought": 80,  # MFI > 80 = overbought
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] + 2:
            return []

        signals = []
        n = self.params["period"]
        high = data["high"]
        low = data["low"]
        close = data["close"]
        volume = data["volume"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate MFI
        tp = (high + low + close) / 3
        mf = tp * volume

        pos_mf = mf.where(tp > tp.shift(1), 0)
        neg_mf = mf.where(tp < tp.shift(1), 0)

        pos_sum = pos_mf.rolling(n).sum()
        neg_sum = neg_mf.rolling(n).sum()
        mf_ratio = pos_sum / neg_sum.replace(0, np.nan)
        mfi = 100 - (100 / (1 + mf_ratio))

        current_mfi = mfi.iloc[-1]
        prev_mfi = mfi.iloc[-2]

        # Oversold: MFI crosses above oversold threshold
        if prev_mfi < self.params["oversold"] and current_mfi >= self.params["oversold"]:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min((current_mfi - self.params["oversold"]) / 20, 1.0),
                metadata={"mfi": current_mfi}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Overbought: MFI crosses below overbought threshold
        elif prev_mfi > self.params["overbought"] and current_mfi <= self.params["overbought"]:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min((100 - current_mfi) / 20, 1.0),
                metadata={"mfi": current_mfi}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close", "volume"],
            "min_length": self.params["period"] + 5,
        }


class VWAPStrategy(FactorStrategyBase):
    """VWAP mean reversion strategy.

    Price deviation from VWAP for mean reversion signals.
    """

    def __init__(self, name: str = "VWAP", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 20,
            "buy_threshold": -0.02,  # 2% below VWAP
            "sell_threshold": 0.02,  # 2% above VWAP
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.03,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] + 1:
            return []

        signals = []
        n = self.params["period"]
        high = data["high"]
        low = data["low"]
        close = data["close"]
        volume = data["volume"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate VWAP
        tp = (high + low + close) / 3
        cum_tp_vol = (tp * volume).rolling(n).sum()
        cum_vol = volume.rolling(n).sum()
        vwap = cum_tp_vol / cum_vol.replace(0, np.nan)

        # Deviation from VWAP
        deviation = (close - vwap) / vwap

        current_dev = deviation.iloc[-1]
        prev_dev = deviation.iloc[-2]

        # Buy: Price below VWAP and recovering
        if prev_dev < self.params["buy_threshold"] and current_dev >= self.params["buy_threshold"]:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(abs(current_dev) / abs(self.params["buy_threshold"]), 1.0),
                metadata={"vwap_deviation": current_dev, "vwap": vwap.iloc[-1]}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Sell: Price above VWAP and falling back
        elif prev_dev > self.params["sell_threshold"] and current_dev <= self.params["sell_threshold"]:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(abs(current_dev) / abs(self.params["sell_threshold"]), 1.0),
                metadata={"vwap_deviation": current_dev, "vwap": vwap.iloc[-1]}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close", "volume"],
            "min_length": self.params["period"] + 5,
        }


class OBVStrategy(FactorStrategyBase):
    """On-Balance Volume strategy.

    Uses OBV divergence with price for signal generation.
    """

    def __init__(self, name: str = "OBV", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "smooth": 20,
            "divergence_threshold": 1.5,  # Z-score threshold
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["smooth"] + 2:
            return []

        signals = []
        n = self.params["smooth"]
        close = data["close"]
        volume = data["volume"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate OBV
        direction = np.sign(close.diff())
        obv = (direction * volume).cumsum()

        # OBV Z-score
        obv_ma = obv.rolling(n).mean()
        obv_std = obv.rolling(n).std().replace(0, np.nan)
        obv_z = (obv - obv_ma) / obv_std

        current_obv_z = obv_z.iloc[-1]
        prev_obv_z = obv_z.iloc[-2]
        threshold = self.params["divergence_threshold"]

        # Price down but OBV strong = bullish divergence
        price_falling = close.iloc[-1] < close.iloc[-5]
        if price_falling and prev_obv_z < threshold and current_obv_z >= threshold:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(current_obv_z / threshold, 1.0),
                metadata={"obv_z": current_obv_z}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Price up but OBV weak = bearish divergence
        price_rising = close.iloc[-1] > close.iloc[-5]
        if price_rising and prev_obv_z > -threshold and current_obv_z <= -threshold:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(abs(current_obv_z) / threshold, 1.0),
                metadata={"obv_z": current_obv_z}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close", "volume"],
            "min_length": self.params["smooth"] + 10,
        }


# ============================================================
# Microstructure Strategies
# ============================================================

class OrderFlowImbalanceStrategy(FactorStrategyBase):
    """Order Flow Imbalance strategy.

    Uses approximate OFI for detecting informed trading.
    """

    def __init__(self, name: str = "OrderFlowImbalance", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 10,
            "imbalance_threshold": 1.0,  # Z-score threshold
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.04,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] * 2 + 2:
            return []

        signals = []
        n = self.params["period"]
        high = data["high"]
        low = data["low"]
        close = data["close"]
        volume = data["volume"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate OFI proxy
        mid = (high + low) / 2
        rng = (high - low).replace(0, np.nan)
        imbalance = ((close - mid) / rng * volume).fillna(0)
        cum_imbalance = imbalance.rolling(n).sum()

        # Normalize
        ofi_z = (cum_imbalance - cum_imbalance.rolling(n).mean()) / cum_imbalance.rolling(n).std().replace(0, np.nan)

        current_ofi = ofi_z.iloc[-1]
        prev_ofi = ofi_z.iloc[-2]
        threshold = self.params["imbalance_threshold"]

        # Strong buying pressure
        if prev_ofi < threshold and current_ofi >= threshold:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(current_ofi / threshold, 1.0),
                metadata={"ofi_z": current_ofi}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Strong selling pressure
        elif prev_ofi > -threshold and current_ofi <= -threshold:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(abs(current_ofi) / threshold, 1.0),
                metadata={"ofi_z": current_ofi}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close", "volume"],
            "min_length": self.params["period"] * 2 + 10,
        }


class TradeIntensityStrategy(FactorStrategyBase):
    """Trade intensity strategy.

    Detects volume surges relative to normal levels.
    """

    def __init__(self, name: str = "TradeIntensity", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "fast": 5,
            "slow": 20,
            "intensity_threshold": 1.5,  # 50% above normal
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["slow"] + 1:
            return []

        signals = []
        fast = self.params["fast"]
        slow = self.params["slow"]
        close = data["close"]
        volume = data["volume"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate intensity ratio
        fast_vol = volume.rolling(fast).mean()
        slow_vol = volume.rolling(slow).mean()
        intensity = fast_vol / slow_vol.replace(0, np.nan) - 1

        current_intensity = intensity.iloc[-1]
        prev_intensity = intensity.iloc[-2]
        threshold = self.params["intensity_threshold"] - 1  # Convert to ratio

        price_change = close.iloc[-1] / close.iloc[-fast] - 1

        # High intensity with price up = bullish
        if prev_intensity < threshold and current_intensity >= threshold and price_change > 0:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(current_intensity / threshold, 1.0),
                metadata={"intensity": current_intensity, "price_change": price_change}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # High intensity with price down = bearish
        elif prev_intensity < threshold and current_intensity >= threshold and price_change < 0:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(current_intensity / threshold, 1.0),
                metadata={"intensity": current_intensity, "price_change": price_change}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close", "volume"],
            "min_length": self.params["slow"] + 10,
        }


# ============================================================
# Statistical Arbitrage Strategies
# ============================================================

class MeanReversionHalfLifeStrategy(FactorStrategyBase):
    """Mean reversion strategy based on half-life estimation.

    Enters positions when price deviates from mean, with expectation
    of reversion within estimated half-life period.
    """

    def __init__(self, name: str = "MeanReversionHalfLife", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "lookback": 60,
            "zscore_entry": 2.0,
            "zscore_exit": 0.5,
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["lookback"] + 2:
            return []

        signals = []
        n = self.params["lookback"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate z-score
        mean = close.rolling(n).mean()
        std = close.rolling(n).std().replace(0, np.nan)
        zscore = (close - mean) / std

        current_z = zscore.iloc[-1]
        prev_z = zscore.iloc[-2]
        entry_z = self.params["zscore_entry"]
        exit_z = self.params["zscore_exit"]

        # Mean reversion buy: price below mean
        if prev_z < -entry_z and current_z >= -entry_z and current_z < -exit_z:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(abs(current_z) / entry_z, 1.0),
                metadata={"zscore": current_z, "mean": mean.iloc[-1]}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = mean.iloc[-1]  # Target the mean
            signals.append(signal)

        # Mean reversion sell: price above mean
        elif prev_z > entry_z and current_z <= entry_z and current_z > exit_z:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(abs(current_z) / entry_z, 1.0),
                metadata={"zscore": current_z, "mean": mean.iloc[-1]}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = mean.iloc[-1]  # Target the mean
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["lookback"] + 10,
        }


class HurstExponentStrategy(FactorStrategyBase):
    """Hurst exponent regime detection strategy.

    Adapts strategy based on whether market is trending or mean-reverting.
    """

    def __init__(self, name: str = "HurstExponent", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "hurst_period": 100,
            "zscore_period": 20,
            "trending_threshold": 0.55,  # H > 0.55 = trending
            "mean_revert_threshold": 0.45,  # H < 0.45 = mean reverting
            "zscore_threshold": 1.5,
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.06,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["hurst_period"] + 2:
            return []

        signals = []
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate simplified Hurst proxy using variance ratio
        n = self.params["hurst_period"]
        returns = close.pct_change()

        # Variance ratio as Hurst proxy
        var_1 = returns.rolling(n).var()
        var_long = returns.rolling(n).apply(lambda x: np.var(x[::5]) * 5, raw=False)
        vr = (var_long / var_1.replace(0, np.nan)).fillna(1)

        # Z-score for mean reversion signals
        mean = close.rolling(self.params["zscore_period"]).mean()
        std = close.rolling(self.params["zscore_period"]).std().replace(0, np.nan)
        zscore = (close - mean) / std

        current_vr = vr.iloc[-1]
        current_z = zscore.iloc[-1]
        prev_z = zscore.iloc[-2]

        # Trending market (VR > 1): momentum strategy
        if current_vr > self.params["trending_threshold"]:
            if prev_z < self.params["zscore_threshold"] and current_z >= self.params["zscore_threshold"]:
                signal = self._create_signal(
                    symbol, SignalType.BUY, current_price,
                    strength=0.7,
                    metadata={"variance_ratio": current_vr, "regime": "trending"}
                )
                signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
                signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
                signals.append(signal)

        # Mean-reverting market (VR < 1): contrarian strategy
        elif current_vr < self.params["mean_revert_threshold"]:
            if prev_z > self.params["zscore_threshold"] and current_z <= self.params["zscore_threshold"]:
                signal = self._create_signal(
                    symbol, SignalType.BUY, current_price,
                    strength=0.7,
                    metadata={"variance_ratio": current_vr, "regime": "mean_reverting"}
                )
                signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
                signal.take_profit = mean.iloc[-1]
                signals.append(signal)

            elif prev_z < -self.params["zscore_threshold"] and current_z >= -self.params["zscore_threshold"]:
                signal = self._create_signal(
                    symbol, SignalType.SELL, current_price,
                    strength=0.7,
                    metadata={"variance_ratio": current_vr, "regime": "mean_reverting"}
                )
                signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
                signal.take_profit = mean.iloc[-1]
                signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["hurst_period"] + 10,
        }


# ============================================================
# Risk-Based Strategies
# ============================================================

class VaRBreakoutStrategy(FactorStrategyBase):
    """Value at Risk breakout strategy.

    Uses VaR to identify abnormal price movements.
    """

    def __init__(self, name: str = "VaRBreakout", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "var_period": 20,
            "confidence": 0.95,
            "multiplier": 1.5,  # Breakout beyond 1.5x VaR
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.04,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["var_period"] + 2:
            return []

        signals = []
        n = self.params["var_period"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        returns = close.pct_change()

        # Calculate rolling VaR
        def calc_var(series):
            r = series.dropna()
            if len(r) < n // 2:
                return np.nan
            return np.percentile(r, (1 - self.params["confidence"]) * 100)

        var = returns.rolling(n).apply(calc_var, raw=False)

        current_ret = float(returns.iloc[-1])
        current_var = float(var.iloc[-1])
        var_threshold = abs(current_var) * float(self.params["multiplier"])

        # Breakout: return exceeds VaR significantly
        if current_var != 0 and not np.isnan(current_var):
            # Positive breakout
            if current_ret >= var_threshold:
                signal = self._create_signal(
                    symbol, SignalType.BUY, current_price,
                    strength=0.8,
                    metadata={
                        "return": current_ret,
                        "var": current_var,
                        "var_threshold": var_threshold,
                        "breakout": "positive",
                    }
                )
                signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
                signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
                signals.append(signal)

            # Negative breakout
            elif current_ret <= -var_threshold:
                signal = self._create_signal(
                    symbol, SignalType.SELL, current_price,
                    strength=0.8,
                    metadata={
                        "return": current_ret,
                        "var": current_var,
                        "var_threshold": var_threshold,
                        "breakout": "negative",
                    }
                )
                signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
                signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
                signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["var_period"] + 10,
        }


class MaxDrawdownStrategy(FactorStrategyBase):
    """Maximum drawdown recovery strategy.

    Buys when recovering from significant drawdowns.
    """

    def __init__(self, name: str = "MaxDrawdown", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "lookback": 30,
            "dd_threshold": -0.10,  # 10% drawdown
            "recovery_threshold": 0.3,  # 30% recovery from bottom
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.08,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["lookback"] + 1:
            return []

        signals = []
        n = self.params["lookback"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate rolling drawdown
        rolling_max = close.rolling(n).max()
        drawdown = (close - rolling_max) / rolling_max

        current_dd = drawdown.iloc[-1]
        prev_dd = drawdown.iloc[-2]

        # Find recent minimum
        rolling_min = close.rolling(n).min()
        bottom_price = rolling_min.iloc[-1]
        top_price = rolling_max.iloc[-1]

        # Recovery percentage from bottom
        recovery = (current_price - bottom_price) / (top_price - bottom_price) if top_price != bottom_price else 0

        # Signal: recovering from significant drawdown
        if (prev_dd <= self.params["dd_threshold"] and
            recovery > self.params["recovery_threshold"] and
            current_price > close.iloc[-2]):  # Uptrend
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(recovery, 1.0),
                metadata={"drawdown": current_dd, "recovery": recovery}
            )
            signal.stop_loss = bottom_price * 0.98  # Just below recent low
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["lookback"] + 10,
        }


class SortinoRatioStrategy(FactorStrategyBase):
    """Sortino ratio trend strategy.

    Follows trends when risk-adjusted returns are favorable.
    """

    def __init__(self, name: str = "SortinoRatio", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 30,
            "sortino_threshold": 1.0,  # Minimum Sortino ratio
            "lookback_trend": 5,
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.06,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] + 2:
            return []

        signals = []
        n = self.params["period"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        returns = close.pct_change()

        def calc_sortino(series):
            r = series.dropna()
            if len(r) < n // 2:
                return np.nan
            mean_ret = r.mean()
            downside = r[r < 0]
            if len(downside) < 2:
                return np.nan
            downside_std = np.sqrt((downside ** 2).mean())
            return mean_ret / downside_std if downside_std > 0 else np.nan

        sortino = returns.rolling(n).apply(calc_sortino, raw=False)

        current_sortino = sortino.iloc[-1]
        prev_sortino = sortino.iloc[-2]
        threshold = self.params["sortino_threshold"]

        trend = close.iloc[-1] / close.iloc[-self.params["lookback_trend"]] - 1

        # Good risk-adjusted returns with positive trend
        if prev_sortino < threshold and current_sortino >= threshold and trend > 0:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(current_sortino / (threshold * 2), 1.0),
                metadata={"sortino": current_sortino, "trend": trend}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Poor risk-adjusted returns
        elif prev_sortino > -threshold and current_sortino <= -threshold:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min(abs(current_sortino) / threshold, 1.0),
                metadata={"sortino": current_sortino, "trend": trend}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["period"] + 10,
        }


# ============================================================
# Technical Analysis Strategies
# ============================================================

class WilliamsRStrategy(FactorStrategyBase):
    """Williams %R strategy.

    Overbought/oversold oscillator strategy.
    """

    def __init__(self, name: str = "WilliamsR", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 14,
            "oversold": -80,
            "overbought": -20,
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] + 1:
            return []

        signals = []
        n = self.params["period"]
        high = data["high"]
        low = data["low"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        highest = high.rolling(n).max()
        lowest = low.rolling(n).min()

        wr = (highest - close) / (highest - lowest).replace(0, np.nan) * -100

        current_wr = wr.iloc[-1]
        prev_wr = wr.iloc[-2]

        # Oversold: crossing above -80
        if prev_wr < self.params["oversold"] and current_wr >= self.params["oversold"]:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min((current_wr - self.params["oversold"]) / 20, 1.0),
                metadata={"williams_r": current_wr}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Overbought: crossing below -20
        elif prev_wr > self.params["overbought"] and current_wr <= self.params["overbought"]:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min((self.params["overbought"] - current_wr) / 20, 1.0),
                metadata={"williams_r": current_wr}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close"],
            "min_length": self.params["period"] + 5,
        }


class CCIStrategy(FactorStrategyBase):
    """Commodity Channel Index strategy.

    CCI for trend identification and overbought/oversold.
    """

    def __init__(self, name: str = "CCI", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 20,
            "constant": 0.015,
            "oversold": -100,
            "overbought": 100,
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["period"] + 1:
            return []

        signals = []
        n = self.params["period"]
        constant = self.params["constant"]
        high = data["high"]
        low = data["low"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        tp = (high + low + close) / 3
        sma = tp.rolling(n).mean()
        mad = tp.rolling(n).apply(lambda x: np.abs(x - x.mean()).mean())

        cci = (tp - sma) / (constant * mad.replace(0, np.nan))

        current_cci = cci.iloc[-1]
        prev_cci = cci.iloc[-2]

        # Oversold: crossing above -100
        if prev_cci < self.params["oversold"] and current_cci >= self.params["oversold"]:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min((current_cci - self.params["oversold"]) / 100, 1.0),
                metadata={"cci": current_cci}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Overbought: crossing below 100
        elif prev_cci > self.params["overbought"] and current_cci <= self.params["overbought"]:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min((self.params["overbought"] - current_cci) / 100, 1.0),
                metadata={"cci": current_cci}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close"],
            "min_length": self.params["period"] + 5,
        }


class StochRSIStrategy(FactorStrategyBase):
    """Stochastic RSI strategy.

    More sensitive oscillator combining Stochastic and RSI.
    """

    def __init__(self, name: str = "StochRSI", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "rsi_period": 14,
            "stoch_period": 14,
            "oversold": 20,
            "overbought": 80,
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < self.params["rsi_period"] + self.params["stoch_period"] + 5:
            return []

        signals = []
        rsi_n = self.params["rsi_period"]
        stoch_n = self.params["stoch_period"]
        close = data["close"]
        symbol = self._get_symbol(data)
        current_price = close.iloc[-1]

        # Calculate RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)

        avg_gain = gain.rolling(rsi_n).mean()
        avg_loss = loss.rolling(rsi_n).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))

        # Stochastic of RSI
        rsi_min = rsi.rolling(stoch_n).min()
        rsi_max = rsi.rolling(stoch_n).max()

        stoch_rsi = (rsi - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan) * 100

        current_srsi = stoch_rsi.iloc[-1]
        prev_srsi = stoch_rsi.iloc[-2]

        # Oversold: crossing above 20
        if prev_srsi < self.params["oversold"] and current_srsi >= self.params["oversold"]:
            signal = self._create_signal(
                symbol, SignalType.BUY, current_price,
                strength=min(current_srsi / 50, 1.0),
                metadata={"stoch_rsi": current_srsi}
            )
            signal.stop_loss = current_price * (1 - self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 + self.params["take_profit_pct"])
            signals.append(signal)

        # Overbought: crossing below 80
        elif prev_srsi > self.params["overbought"] and current_srsi <= self.params["overbought"]:
            signal = self._create_signal(
                symbol, SignalType.SELL, current_price,
                strength=min((100 - current_srsi) / 50, 1.0),
                metadata={"stoch_rsi": current_srsi}
            )
            signal.stop_loss = current_price * (1 + self.params["stop_loss_pct"])
            signal.take_profit = current_price * (1 - self.params["take_profit_pct"])
            signals.append(signal)

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": self.params["rsi_period"] + self.params["stoch_period"] + 10,
        }
