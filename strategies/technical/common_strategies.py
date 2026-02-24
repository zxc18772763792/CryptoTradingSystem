"""Common technical strategies."""
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _symbol_of(data: pd.DataFrame) -> str:
    if "symbol" in data.columns and len(data["symbol"]) > 0:
        return str(data["symbol"].iloc[-1])
    return "UNKNOWN"


class DonchianBreakoutStrategy(StrategyBase):
    """Donchian breakout trend-following strategy."""

    def __init__(self, name: str = "Donchian_Breakout", params: Optional[Dict[str, Any]] = None):
        default = {
            "lookback": 20,
            "exit_lookback": 10,
            "breakout_buffer_pct": 0.0,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.06,
        }
        if params:
            default.update(params)
        super().__init__(name, default)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        lb = int(self.params["lookback"])
        ex_lb = int(self.params["exit_lookback"])
        if data.empty or len(data) < max(lb, ex_lb) + 2:
            return []

        high = data["high"]
        low = data["low"]
        close = data["close"]
        upper = high.rolling(lb).max().shift(1)
        exit_low = low.rolling(ex_lb).min().shift(1)
        buf = float(self.params.get("breakout_buffer_pct", 0.0) or 0.0)

        c = float(close.iloc[-1])
        prev_c = float(close.iloc[-2])
        up = float(upper.iloc[-1]) if pd.notna(upper.iloc[-1]) else np.nan
        prev_up = float(upper.iloc[-2]) if pd.notna(upper.iloc[-2]) else np.nan
        ex = float(exit_low.iloc[-1]) if pd.notna(exit_low.iloc[-1]) else np.nan
        symbol = _symbol_of(data)
        now = datetime.now()
        signals: List[Signal] = []

        if pd.notna(up) and pd.notna(prev_up):
            enter_level = up * (1 + buf)
            prev_enter = prev_up * (1 + buf)
            if prev_c <= prev_enter and c > enter_level:
                signals.append(
                    Signal(
                        symbol=symbol,
                        signal_type=SignalType.BUY,
                        price=c,
                        timestamp=now,
                        strategy_name=self.name,
                        strength=min(max((c - enter_level) / max(enter_level, 1e-9) * 20, 0.1), 1.0),
                        stop_loss=c * (1 - float(self.params["stop_loss_pct"])),
                        take_profit=c * (1 + float(self.params["take_profit_pct"])),
                        metadata={"upper": up, "exit_low": ex},
                    )
                )

        if pd.notna(ex) and prev_c >= ex and c < ex:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=c,
                    timestamp=now,
                    strategy_name=self.name,
                    strength=0.8,
                    metadata={"upper": up, "exit_low": ex},
                )
            )
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close"],
            "min_length": max(int(self.params["lookback"]), int(self.params["exit_lookback"])) + 5,
        }


class StochasticStrategy(StrategyBase):
    """Stochastic oscillator cross strategy."""

    def __init__(self, name: str = "Stochastic", params: Optional[Dict[str, Any]] = None):
        default = {
            "k_period": 14,
            "d_period": 3,
            "smooth_k": 3,
            "oversold": 20.0,
            "overbought": 80.0,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.04,
        }
        if params:
            default.update(params)
        super().__init__(name, default)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        k_period = int(self.params["k_period"])
        d_period = int(self.params["d_period"])
        smooth_k = int(self.params["smooth_k"])
        if data.empty or len(data) < k_period + d_period + smooth_k + 2:
            return []

        high = data["high"]
        low = data["low"]
        close = data["close"]
        lowest = low.rolling(k_period).min()
        highest = high.rolling(k_period).max()
        raw_k = (close - lowest) / (highest - lowest).replace(0, np.nan) * 100
        k = raw_k.rolling(smooth_k).mean()
        d = k.rolling(d_period).mean()

        k_now, k_prev = float(k.iloc[-1]), float(k.iloc[-2])
        d_now, d_prev = float(d.iloc[-1]), float(d.iloc[-2])
        if np.isnan([k_now, k_prev, d_now, d_prev]).any():
            return []

        c = float(close.iloc[-1])
        symbol = _symbol_of(data)
        now = datetime.now()
        oversold = float(self.params["oversold"])
        overbought = float(self.params["overbought"])
        signals: List[Signal] = []

        cross_up = k_prev <= d_prev and k_now > d_now
        cross_down = k_prev >= d_prev and k_now < d_now

        if cross_up and k_now <= oversold:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=c,
                    timestamp=now,
                    strategy_name=self.name,
                    strength=min(max((oversold - k_now) / max(oversold, 1e-9), 0.1), 1.0),
                    stop_loss=c * (1 - float(self.params["stop_loss_pct"])),
                    take_profit=c * (1 + float(self.params["take_profit_pct"])),
                    metadata={"k": k_now, "d": d_now},
                )
            )

        if cross_down and k_now >= overbought:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=c,
                    timestamp=now,
                    strategy_name=self.name,
                    strength=min(max((k_now - overbought) / max(100 - overbought, 1e-9), 0.1), 1.0),
                    metadata={"k": k_now, "d": d_now},
                )
            )
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close"],
            "min_length": int(self.params["k_period"]) + int(self.params["d_period"]) + int(self.params["smooth_k"]) + 10,
        }


class ADXTrendStrategy(StrategyBase):
    """ADX + DI trend strategy."""

    def __init__(self, name: str = "ADX_Trend", params: Optional[Dict[str, Any]] = None):
        default = {
            "period": 14,
            "adx_threshold": 25.0,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        }
        if params:
            default.update(params)
        super().__init__(name, default)

    def _adx(self, data: pd.DataFrame, period: int) -> Dict[str, pd.Series]:
        high = data["high"]
        low = data["low"]
        close = data["close"]

        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=data.index)
        minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=data.index)

        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / period, adjust=False).mean()

        plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, np.nan))
        minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, np.nan))
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
        adx = dx.ewm(alpha=1 / period, adjust=False).mean()
        return {"plus_di": plus_di, "minus_di": minus_di, "adx": adx}

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        period = int(self.params["period"])
        if data.empty or len(data) < period * 3:
            return []

        vals = self._adx(data, period)
        plus_di = vals["plus_di"]
        minus_di = vals["minus_di"]
        adx = vals["adx"]

        p_now, p_prev = float(plus_di.iloc[-1]), float(plus_di.iloc[-2])
        m_now, m_prev = float(minus_di.iloc[-1]), float(minus_di.iloc[-2])
        a_now = float(adx.iloc[-1])
        if np.isnan([p_now, p_prev, m_now, m_prev, a_now]).any():
            return []

        strong = a_now >= float(self.params["adx_threshold"])
        cross_up = p_prev <= m_prev and p_now > m_now
        cross_down = p_prev >= m_prev and p_now < m_now

        c = float(data["close"].iloc[-1])
        symbol = _symbol_of(data)
        now = datetime.now()
        signals: List[Signal] = []

        if strong and cross_up:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=c,
                    timestamp=now,
                    strategy_name=self.name,
                    strength=min(max((a_now - float(self.params["adx_threshold"])) / 25, 0.2), 1.0),
                    stop_loss=c * (1 - float(self.params["stop_loss_pct"])),
                    take_profit=c * (1 + float(self.params["take_profit_pct"])),
                    metadata={"adx": a_now, "plus_di": p_now, "minus_di": m_now},
                )
            )
        elif strong and cross_down:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=c,
                    timestamp=now,
                    strategy_name=self.name,
                    strength=min(max((a_now - float(self.params["adx_threshold"])) / 25, 0.2), 1.0),
                    metadata={"adx": a_now, "plus_di": p_now, "minus_di": m_now},
                )
            )
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close"],
            "min_length": int(self.params["period"]) * 3 + 10,
        }


class VWAPReversionStrategy(StrategyBase):
    """Rolling VWAP mean-reversion strategy."""

    def __init__(self, name: str = "VWAP_Reversion", params: Optional[Dict[str, Any]] = None):
        default = {
            "window": 48,
            "entry_deviation_pct": 0.01,
            "exit_deviation_pct": 0.002,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.03,
        }
        if params:
            default.update(params)
        super().__init__(name, default)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        window = int(self.params["window"])
        if data.empty or len(data) < window + 2:
            return []

        price = (data["high"] + data["low"] + data["close"]) / 3.0
        volume = data["volume"].replace(0, np.nan)
        pv = price * volume
        vwap = pv.rolling(window).sum() / volume.rolling(window).sum()
        deviation = (data["close"] - vwap) / vwap

        d_now = float(deviation.iloc[-1]) if pd.notna(deviation.iloc[-1]) else np.nan
        d_prev = float(deviation.iloc[-2]) if pd.notna(deviation.iloc[-2]) else np.nan
        if np.isnan([d_now, d_prev]).any():
            return []

        entry = float(self.params["entry_deviation_pct"])
        exit_dev = float(self.params["exit_deviation_pct"])
        c = float(data["close"].iloc[-1])
        symbol = _symbol_of(data)
        now = datetime.now()
        signals: List[Signal] = []

        if d_prev >= -entry and d_now < -entry:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.BUY,
                    price=c,
                    timestamp=now,
                    strategy_name=self.name,
                    strength=min(max(abs(d_now) / max(entry * 2, 1e-9), 0.1), 1.0),
                    stop_loss=c * (1 - float(self.params["stop_loss_pct"])),
                    take_profit=c * (1 + float(self.params["take_profit_pct"])),
                    metadata={"vwap": float(vwap.iloc[-1]), "deviation": d_now},
                )
            )
        elif d_prev <= -exit_dev and d_now > -exit_dev:
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.SELL,
                    price=c,
                    timestamp=now,
                    strategy_name=self.name,
                    strength=0.7,
                    metadata={"vwap": float(vwap.iloc[-1]), "deviation": d_now},
                )
            )
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["high", "low", "close", "volume"],
            "min_length": int(self.params["window"]) + 10,
        }

