"""Short weak-altcoin bounce failures inside a downtrend."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _calculate_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    rsi = rsi.mask((avg_gain == 0) & (avg_loss > 0), 0.0)
    rsi = rsi.mask((avg_gain == 0) & (avg_loss == 0), 50.0)
    return rsi


def _symbol_of(data: pd.DataFrame) -> str:
    if "symbol" in data.columns and len(data["symbol"]) > 0:
        return str(data["symbol"].iloc[-1])
    return "UNKNOWN"


def _timestamp_to_utc_naive(value: Any) -> Optional[pd.Timestamp]:
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


class AltcoinDowntrendBounceShortStrategy(StrategyBase):
    """Fade extended 1h bounces while the medium-term trend remains down.

    The mined rule is intentionally short-only:
    SMA20 < SMA100, close is at least 3% above SMA20, and RSI14 >= 60.
    It exits on a fixed 24-bar time stop by default.
    """

    mutates_input = False

    def __init__(
        self,
        name: str = "Altcoin_Downtrend_Bounce_Short",
        params: Optional[Dict[str, Any]] = None,
    ):
        default_params = {
            "fast_sma_period": 20,
            "slow_sma_period": 100,
            "distance_threshold": 0.03,
            "rsi_period": 14,
            "rsi_threshold": 60.0,
            "hold_bars": 24,
            "position_exposure": 0.05,
            "max_positions": 10,
            "allow_long": False,
            "allow_short": True,
            "reverse_on_signal": False,
            "use_atr_stops": False,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._active_entry_at: Dict[str, pd.Timestamp] = {}
        self._last_exit_at: Dict[str, pd.Timestamp] = {}

    def _entry_context(self, data: pd.DataFrame) -> Optional[Dict[str, float]]:
        fast_period = int(self.params.get("fast_sma_period", 20))
        slow_period = int(self.params.get("slow_sma_period", 100))
        rsi_period = int(self.params.get("rsi_period", 14))
        min_rows = max(fast_period, slow_period, rsi_period) + 2
        if data is None or data.empty or len(data) < min_rows or "close" not in data.columns:
            return None

        close = pd.to_numeric(data["close"], errors="coerce")
        fast_sma = close.rolling(fast_period, min_periods=fast_period).mean()
        slow_sma = close.rolling(slow_period, min_periods=slow_period).mean()
        rsi = _calculate_rsi(close, rsi_period)

        price = float(close.iloc[-1])
        fast = float(fast_sma.iloc[-1])
        slow = float(slow_sma.iloc[-1])
        rsi_now = float(rsi.iloc[-1])
        if not np.isfinite([price, fast, slow, rsi_now]).all() or fast <= 0 or price <= 0:
            return None

        distance = price / fast - 1.0
        return {
            "price": price,
            "fast_sma": fast,
            "slow_sma": slow,
            "distance": float(distance),
            "rsi": rsi_now,
        }

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        ctx = self._entry_context(data)
        if ctx is None:
            return []

        symbol = _symbol_of(data)
        if symbol in self._active_entry_at:
            return []

        trend_down = ctx["fast_sma"] < ctx["slow_sma"]
        bounce_extended = ctx["distance"] >= float(self.params.get("distance_threshold", 0.03))
        momentum_repaired = ctx["rsi"] >= float(self.params.get("rsi_threshold", 60.0))
        if not (trend_down and bounce_extended and momentum_repaired):
            return []

        timestamp = self._bar_time(data)
        entry_at = _timestamp_to_utc_naive(timestamp)
        last_exit_at = self._last_exit_at.get(symbol)
        if entry_at is not None and last_exit_at is not None and entry_at <= last_exit_at:
            return []

        strength = min(1.0, max(0.2, ctx["distance"] / max(float(self.params["distance_threshold"]), 1e-9)))
        return [
            Signal(
                symbol=symbol,
                signal_type=SignalType.SELL,
                price=ctx["price"],
                timestamp=timestamp,
                strategy_name=self.name,
                strength=float(strength),
                metadata={
                    "setup": "downtrend_bounce_failure_short",
                    "fast_sma": ctx["fast_sma"],
                    "slow_sma": ctx["slow_sma"],
                    "distance": ctx["distance"],
                    "rsi": ctx["rsi"],
                    "hold_bars": int(self.params.get("hold_bars", 24)),
                    "position_exposure": float(self.params.get("position_exposure", 0.05)),
                    "use_atr_stops": False,
                    "generic_check_exit_enabled": False,
                    **({"entry_signal_at": entry_at.isoformat()} if entry_at is not None else {}),
                },
            )
        ]

    def check_exit(self, data: pd.DataFrame, position: Any) -> Optional[Signal]:
        raw_side = position.get("side") if isinstance(position, dict) else getattr(position, "side", "")
        side = str(getattr(raw_side, "value", raw_side) or "").lower()
        if side != "short":
            return None

        symbol = (
            str(position.get("symbol") or _symbol_of(data))
            if isinstance(position, dict)
            else str(getattr(position, "symbol", None) or _symbol_of(data))
        )
        metadata = dict((position.get("metadata") if isinstance(position, dict) else getattr(position, "metadata", {})) or {})

        current_at = _timestamp_to_utc_naive(self._bar_time(data))
        if current_at is None:
            return None

        entry_at = self._active_entry_at.get(symbol)
        if entry_at is None:
            entry_at = _timestamp_to_utc_naive(metadata.get("entry_signal_at"))
            if entry_at is not None:
                self._active_entry_at[symbol] = entry_at
        if entry_at is None:
            self._active_entry_at[symbol] = current_at
            return None

        hold_bars = max(1, int(self.params.get("hold_bars", metadata.get("hold_bars", 24)) or 24))
        elapsed_hours = max(0.0, (current_at - entry_at).total_seconds() / 3600.0)
        if elapsed_hours + 1e-9 < float(hold_bars):
            return None

        try:
            price = float(pd.to_numeric(data["close"].iloc[-1], errors="coerce"))
        except Exception:
            return None
        if not np.isfinite(price) or price <= 0:
            return None

        self._active_entry_at.pop(symbol, None)
        self._last_exit_at[symbol] = current_at
        return Signal(
            symbol=symbol,
            signal_type=SignalType.CLOSE_SHORT,
            price=price,
            timestamp=self._bar_time(data),
            strategy_name=self.name,
            strength=1.0,
            metadata={
                "close_only": True,
                "close_reason": "downtrend_bounce_time_exit",
                "hold_bars": hold_bars,
                "hours_held": elapsed_hours,
                "generic_check_exit_enabled": False,
            },
        )

    def get_required_data(self) -> Dict[str, Any]:
        fast_period = int(self.params.get("fast_sma_period", 20))
        slow_period = int(self.params.get("slow_sma_period", 100))
        rsi_period = int(self.params.get("rsi_period", 14))
        return {
            "type": "kline",
            "columns": ["open", "high", "low", "close", "volume"],
            "min_length": max(fast_period, slow_period, rsi_period) + 5,
        }
