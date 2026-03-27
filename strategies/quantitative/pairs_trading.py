from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

from core.strategies.strategy_base import Signal, SignalType, StrategyBase


class PairsTradingStrategy(StrategyBase):
    """Pairs trading strategy based on rolling spread Z-score."""

    def __init__(self, name: str = "Pairs_Trading", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "lookback_period": 30,
            "entry_z_score": 2.0,
            "exit_z_score": 0.5,
            "hedge_ratio_method": "ols",
            "allow_negative_hedge_ratio": True,
            "min_hedge_ratio": -5.0,
            "max_hedge_ratio": 5.0,
            "stop_loss_pct": 0.05,
            "pair_symbol": "ETH/USDT",
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._hedge_ratio: Optional[float] = None
        self._spread: Optional[pd.Series] = None

    @staticmethod
    def _safe_symbol(data: pd.DataFrame) -> str:
        if "symbol" in data.columns and len(data["symbol"]) > 0:
            return str(data["symbol"].iloc[-1])
        return "UNKNOWN"

    @staticmethod
    def _calculate_hedge_ratio_ols(price1: pd.Series, price2: pd.Series) -> float:
        x = pd.to_numeric(price2, errors="coerce").ffill().dropna()
        y = pd.to_numeric(price1, errors="coerce").ffill().dropna()
        aligned = pd.concat([y, x], axis=1).dropna()
        if aligned.empty:
            return 1.0
        yv = aligned.iloc[:, 0].values
        xv = aligned.iloc[:, 1].values
        xv = xv - float(np.mean(xv))
        yv = yv - float(np.mean(yv))
        if np.nanstd(xv) <= 1e-12:
            return 1.0
        xv = xv.reshape(-1, 1)
        coef, *_ = np.linalg.lstsq(xv, yv, rcond=None)
        return float(coef[0]) if len(coef) else 1.0

    def _hedge_ratio_bounds(self) -> Tuple[float, float]:
        allow_negative = bool(self.params.get("allow_negative_hedge_ratio", True))
        min_hr = float(self.params.get("min_hedge_ratio", -5.0))
        max_hr = float(self.params.get("max_hedge_ratio", 5.0))
        if min_hr > max_hr:
            min_hr, max_hr = max_hr, min_hr
        if allow_negative:
            if min_hr >= 0 < max_hr:
                min_hr = -abs(max_hr)
        else:
            min_hr = max(0.0, min_hr)
            max_hr = max(max_hr, min_hr)
        return min_hr, max_hr

    @staticmethod
    def _secondary_entry_side(direction: str, hedge_ratio: float) -> SignalType:
        positive_hedge = hedge_ratio >= 0
        if direction == "long_spread":
            return SignalType.SELL if positive_hedge else SignalType.BUY
        return SignalType.BUY if positive_hedge else SignalType.SELL

    @staticmethod
    def _opposite_side(side: SignalType) -> SignalType:
        return SignalType.BUY if side == SignalType.SELL else SignalType.SELL

    def _secondary_quantity(self, hedge_ratio: float) -> float:
        min_hr, max_hr = self._hedge_ratio_bounds()
        max_abs = max(abs(min_hr), abs(max_hr), 0.001)
        return max(0.001, min(abs(float(hedge_ratio)), max_abs))

    def _calculate_spread(self, data1: pd.DataFrame, data2: pd.DataFrame) -> Tuple[pd.Series, float]:
        price1 = pd.to_numeric(data1["close"], errors="coerce")
        price2 = pd.to_numeric(data2["close"], errors="coerce")

        hedge_ratio = self._calculate_hedge_ratio_ols(price1, price2)
        min_hr, max_hr = self._hedge_ratio_bounds()
        hedge_ratio = max(min_hr, min(hedge_ratio, max_hr))
        spread = price1 - hedge_ratio * price2
        return spread, hedge_ratio

    def _fallback_pair_df(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        # Optional fallback for pre-joined dataset.
        if {"pair_close"}.issubset(data.columns):
            out = pd.DataFrame(index=data.index)
            out["close"] = pd.to_numeric(data["pair_close"], errors="coerce")
            out["symbol"] = str(self.params.get("pair_symbol", "PAIR"))
            return out
        return None

    def generate_signals(self, data: pd.DataFrame, data2: Optional[pd.DataFrame] = None) -> List[Signal]:
        if data.empty:
            return []

        if data2 is None:
            data2 = self._fallback_pair_df(data)
        if data2 is None or data2.empty:
            return []

        lookback = int(self.params["lookback_period"])
        if len(data) < lookback + 5 or len(data2) < lookback + 5:
            return []

        spread, hedge_ratio = self._calculate_spread(data, data2)
        self._hedge_ratio = hedge_ratio
        self._spread = spread

        spread_mean = spread.rolling(lookback, min_periods=lookback).mean()
        spread_std = spread.rolling(lookback, min_periods=lookback).std().replace(0, np.nan)
        z_score = (spread - spread_mean) / spread_std

        current_z = float(z_score.iloc[-1]) if pd.notna(z_score.iloc[-1]) else np.nan
        prev_z = float(z_score.iloc[-2]) if pd.notna(z_score.iloc[-2]) else np.nan
        if np.isnan([current_z, prev_z]).any():
            return []

        entry_z = float(self.params["entry_z_score"])
        exit_z = float(self.params["exit_z_score"])

        current_price1 = float(data["close"].iloc[-1])
        current_price2 = float(data2["close"].iloc[-1])
        timestamp = datetime.now()
        symbol1 = self._safe_symbol(data)
        symbol2 = self._safe_symbol(data2)
        pair_regime = "positive_corr" if hedge_ratio >= 0 else "negative_corr"

        signals: List[Signal] = []

        # Long spread: long d(spread), with leg-2 side adapting to hedge-ratio sign.
        if prev_z > -entry_z >= current_z:
            strength = max(0.1, min(abs(current_z) / max(entry_z, 1e-9), 1.0))
            side2 = self._secondary_entry_side("long_spread", hedge_ratio)
            signals.append(
                Signal(
                    symbol=symbol1,
                    signal_type=SignalType.BUY,
                    price=current_price1,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    stop_loss=current_price1 * (1 - float(self.params["stop_loss_pct"])),
                    metadata={
                        "pair": symbol2,
                        "hedge_ratio": hedge_ratio,
                        "z_score": current_z,
                        "direction": "long_spread",
                        "pair_regime": pair_regime,
                    },
                )
            )
            signals.append(
                Signal(
                    symbol=symbol2,
                    signal_type=side2,
                    price=current_price2,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    quantity=self._secondary_quantity(hedge_ratio),
                    metadata={
                        "pair": symbol1,
                        "hedge_ratio": hedge_ratio,
                        "z_score": current_z,
                        "direction": "long_spread",
                        "pair_regime": pair_regime,
                    },
                )
            )
            logger.info(f"{self.name} LONG spread {symbol1}/{symbol2}, z={current_z:.2f}, hr={hedge_ratio:.3f}")

        # Short spread: short d(spread), with leg-2 side adapting to hedge-ratio sign.
        elif prev_z < entry_z <= current_z:
            strength = max(0.1, min(abs(current_z) / max(entry_z, 1e-9), 1.0))
            side2 = self._secondary_entry_side("short_spread", hedge_ratio)
            signals.append(
                Signal(
                    symbol=symbol1,
                    signal_type=SignalType.SELL,
                    price=current_price1,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    stop_loss=current_price1 * (1 + float(self.params["stop_loss_pct"])),
                    metadata={
                        "pair": symbol2,
                        "hedge_ratio": hedge_ratio,
                        "z_score": current_z,
                        "direction": "short_spread",
                        "pair_regime": pair_regime,
                    },
                )
            )
            signals.append(
                Signal(
                    symbol=symbol2,
                    signal_type=side2,
                    price=current_price2,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=strength,
                    quantity=self._secondary_quantity(hedge_ratio),
                    metadata={
                        "pair": symbol1,
                        "hedge_ratio": hedge_ratio,
                        "z_score": current_z,
                        "direction": "short_spread",
                        "pair_regime": pair_regime,
                    },
                )
            )
            logger.info(f"{self.name} SHORT spread {symbol1}/{symbol2}, z={current_z:.2f}, hr={hedge_ratio:.3f}")

        # Exit zone guidance (optional close signals).
        elif abs(current_z) <= exit_z < abs(prev_z):
            side1 = SignalType.SELL if prev_z < 0 else SignalType.BUY
            active_direction = "long_spread" if prev_z < 0 else "short_spread"
            side2 = self._opposite_side(self._secondary_entry_side(active_direction, hedge_ratio))
            signals.append(
                Signal(
                    symbol=symbol1,
                    signal_type=side1,
                    price=current_price1,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.3,
                    metadata={
                        "pair": symbol2,
                        "hedge_ratio": hedge_ratio,
                        "z_score": current_z,
                        "direction": "mean_revert_exit",
                        "pair_regime": pair_regime,
                    },
                )
            )
            signals.append(
                Signal(
                    symbol=symbol2,
                    signal_type=side2,
                    price=current_price2,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=0.3,
                    quantity=self._secondary_quantity(hedge_ratio),
                    metadata={
                        "pair": symbol1,
                        "hedge_ratio": hedge_ratio,
                        "z_score": current_z,
                        "direction": "mean_revert_exit",
                        "pair_regime": pair_regime,
                    },
                )
            )

        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "kline",
            "columns": ["close"],
            "min_length": int(self.params["lookback_period"]) + 10,
            "requires_pair": True,
        }
