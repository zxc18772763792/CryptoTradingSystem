from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _calculate_rsi_from_close(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    rsi = rsi.mask((avg_gain == 0) & (avg_loss > 0), 0.0)
    rsi = rsi.mask((avg_gain == 0) & (avg_loss == 0), 50.0)
    return rsi


class RSIStrategy(StrategyBase):
    """RSI overbought/oversold reversal strategy."""

    mutates_input = False  # only reads data["close"]

    def __init__(self, name: str = "RSI", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "period": 14,
            "oversold": 30,
            "overbought": 70,
            "exit_oversold": 80,
            "exit_overbought": 20,
            "exit_min_profit_pct": 0.002,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._regime_bias: Dict[str, int] = {}

    def _calculate_rsi(self, data: pd.DataFrame, period: int) -> pd.Series:
        return _calculate_rsi_from_close(data["close"], period)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data.empty or len(data) < int(self.params["period"]) + 5:
            return []

        rsi = self._calculate_rsi(data, int(self.params["period"]))
        current_rsi = float(rsi.iloc[-1])
        prev_rsi = float(rsi.iloc[-2])
        current_price = float(data["close"].iloc[-1])
        timestamp = self._bar_time(data)
        symbol = str(data["symbol"].iloc[0]) if "symbol" in data and len(data) else "UNKNOWN"

        oversold = float(self.params["oversold"])
        overbought = float(self.params["overbought"])
        # NOTE: exit_oversold / exit_overbought are read by check_exit, not here.
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

        return signals

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        """Exit when RSI returns through a stricter neutral band with profit.

        Refactored from the old inline exit branch in ``generate_signals`` so that
        position-driven exits run through the unified ``check_exit`` channel —
        this lets the framework's Tier 2/3 lifecycle reason about exits without
        depending on a stale per-symbol ``_regime_bias`` cache (which was lost
        across restarts and reseeds and went out of sync with the actual
        positions tracked by ``position_manager``).
        """
        if data.empty or len(data) < int(self.params["period"]) + 5:
            return None
        try:
            rsi = self._calculate_rsi(data, int(self.params["period"]))
            current_rsi = float(rsi.iloc[-1])
            prev_rsi = float(rsi.iloc[-2])
            current_price = float(data["close"].iloc[-1])
        except Exception:
            return None
        if not np.isfinite([current_rsi, prev_rsi, current_price]).all():
            return None

        exit_oversold = float(self.params.get("exit_oversold", 80))
        exit_overbought = float(self.params.get("exit_overbought", 20))
        min_profit_pct = max(0.0, float(self.params.get("exit_min_profit_pct", 0.002) or 0.0))
        if isinstance(position, dict):
            raw_side = position.get("side")
            symbol = str(position.get("symbol") or (data["symbol"].iloc[-1] if "symbol" in data else "UNKNOWN"))
            entry_price = float(position.get("entry_price") or 0.0)
        else:
            raw_side = getattr(position, "side", None)
            symbol = str(getattr(position, "symbol", None) or (data["symbol"].iloc[-1] if "symbol" in data else "UNKNOWN"))
            entry_price = float(getattr(position, "entry_price", 0.0) or 0.0)
        side = str(getattr(raw_side, "value", raw_side) or "").lower()
        timestamp = self._bar_time(data)
        if entry_price <= 0:
            return None

        if side == "long" and prev_rsi < exit_oversold <= current_rsi:
            pnl_pct = (current_price - entry_price) / entry_price
            if pnl_pct < min_profit_pct:
                return None
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_LONG,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=0.6,
                metadata={
                    "rsi": current_rsi,
                    "exit_threshold": exit_oversold,
                    "exit_min_profit_pct": min_profit_pct,
                    "pnl_pct": pnl_pct,
                    "close_only": True,
                    "close_reason": "rsi_long_exit",
                },
            )
        if side == "short" and prev_rsi > exit_overbought >= current_rsi:
            pnl_pct = (entry_price - current_price) / entry_price
            if pnl_pct < min_profit_pct:
                return None
            return Signal(
                symbol=symbol,
                signal_type=SignalType.CLOSE_SHORT,
                price=current_price,
                timestamp=timestamp,
                strategy_name=self.name,
                strength=0.6,
                metadata={
                    "rsi": current_rsi,
                    "exit_threshold": exit_overbought,
                    "exit_min_profit_pct": min_profit_pct,
                    "pnl_pct": pnl_pct,
                    "close_only": True,
                    "close_reason": "rsi_short_exit",
                },
            )
        return None

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
        return _calculate_rsi_from_close(data["close"], period)

    @staticmethod
    def _paired_extrema(
        values: pd.Series,
        value_extrema: pd.Series,
        rsi: pd.Series,
        rsi_extrema: pd.Series,
        max_distance: int,
    ) -> List[tuple[int, float, float]]:
        value_positions = np.flatnonzero(value_extrema.to_numpy(dtype=bool))
        rsi_positions = np.flatnonzero(rsi_extrema.to_numpy(dtype=bool))
        pairs: List[tuple[int, float, float]] = []
        used_rsi: set[int] = set()
        for pos in value_positions:
            candidates = [rpos for rpos in rsi_positions if rpos not in used_rsi and abs(rpos - pos) <= max_distance]
            if not candidates:
                continue
            rsi_pos = min(candidates, key=lambda rpos: (abs(rpos - pos), rpos))
            value_at_pos = float(values.iloc[pos])
            rsi_at_pos = float(rsi.iloc[rsi_pos])
            if np.isfinite([value_at_pos, rsi_at_pos]).all():
                pairs.append((pos, value_at_pos, rsi_at_pos))
                used_rsi.add(rsi_pos)
        return pairs

    @staticmethod
    def _find_peaks(series: pd.Series, order: int = 5) -> pd.Series:
        """Non-centered peak detector: a peak at index t is confirmed only after
        ``order`` subsequent bars. This avoids future-data leakage that centered
        rolling windows introduce — the resulting series is causal and consistent
        between backtest and live."""
        order = int(max(1, order))
        vals = pd.to_numeric(series, errors="coerce")
        n = len(vals)
        peaks = pd.Series(False, index=vals.index)
        if n <= 2 * order:
            return peaks
        arr = vals.values
        for i in range(order, n - order):
            v = arr[i]
            if not np.isfinite(v):
                continue
            left = arr[i - order:i]
            right = arr[i + 1:i + 1 + order]
            if np.all(np.isfinite(left)) and np.all(np.isfinite(right)):
                if v >= left.max() and v >= right.max():
                    peaks.iloc[i] = True
        return peaks

    @staticmethod
    def _find_troughs(series: pd.Series, order: int = 5) -> pd.Series:
        """Non-centered trough detector: a trough at index t is confirmed only after
        ``order`` subsequent bars (causal, no look-ahead bias)."""
        order = int(max(1, order))
        vals = pd.to_numeric(series, errors="coerce")
        n = len(vals)
        troughs = pd.Series(False, index=vals.index)
        if n <= 2 * order:
            return troughs
        arr = vals.values
        for i in range(order, n - order):
            v = arr[i]
            if not np.isfinite(v):
                continue
            left = arr[i - order:i]
            right = arr[i + 1:i + 1 + order]
            if np.all(np.isfinite(left)) and np.all(np.isfinite(right)):
                if v <= left.min() and v <= right.min():
                    troughs.iloc[i] = True
        return troughs

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
        timestamp = self._bar_time(data)
        symbol = str(data["symbol"].iloc[0]) if "symbol" in data and len(data) else "UNKNOWN"

        signals: List[Signal] = []
        min_div = float(self.params["min_divergence"])

        max_pair_distance = int(self.params.get("extrema_pair_max_distance", max(1, order)))
        trough_pairs = self._paired_extrema(
            data["close"], price_troughs, rsi, rsi_troughs, max_pair_distance
        )
        recent_trough_pairs = [pair for pair in trough_pairs if pair[0] >= len(data) - lookback][-2:]
        if len(recent_trough_pairs) >= 2:
            _, prev_price, prev_rsi = recent_trough_pairs[-2]
            _, latest_price, latest_rsi = recent_trough_pairs[-1]
            price_trend = (latest_price - prev_price) / max(prev_price, 1e-9)
            rsi_trend = latest_rsi - prev_rsi
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

        peak_pairs = self._paired_extrema(
            data["close"], price_peaks, rsi, rsi_peaks, max_pair_distance
        )
        recent_peak_pairs = [pair for pair in peak_pairs if pair[0] >= len(data) - lookback][-2:]
        if len(recent_peak_pairs) >= 2:
            _, prev_price, prev_rsi = recent_peak_pairs[-2]
            _, latest_price, latest_rsi = recent_peak_pairs[-1]
            price_trend = (latest_price - prev_price) / max(prev_price, 1e-9)
            rsi_trend = latest_rsi - prev_rsi
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
