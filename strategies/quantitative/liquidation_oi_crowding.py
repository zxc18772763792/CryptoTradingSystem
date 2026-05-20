"""Liquidation/OI crowding strategy.

This is deliberately conservative: gate mode is the primary output, and trade
mode only fires after a liquidation reset is confirmed.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from core.strategies.strategy_base import Signal, SignalType, StrategyBase
from core.structural.context import StructuralMarketContext, clamp, safe_float
from core.structural.derivatives_crowding import (
    DerivativesCrowdingConfig,
    LiquidationOICrowdingGate,
    detect_flush_reversal,
    prepare_derivatives_features,
    update_derivatives_scores,
)


class LiquidationOICrowdingStrategy(StrategyBase):
    mutates_input = False

    def __init__(self, name: str = "LiquidationOI_Crowding", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "gate_mode": True,
            "trade_mode": True,
            "crowded_score_enter": 0.75,
            "crowded_score_exit": 0.55,
            "liquidation_burst_enter": 0.80,
            "base_position_pct": 0.04,
            "max_position_pct": 0.08,
            "min_signal_strength": 0.55,
            "hard_stop_atr_mult": 1.8,
            "take_profit_atr_mult": 2.4,
            "max_holding_hours": 36,
            "cooldown_hours_after_loss": 12,
            "max_trades_per_symbol_per_day": 2,
            "lookback_bars": 120,
            "emit_gate_hold_signal": True,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._config = DerivativesCrowdingConfig(
            crowded_score_enter=float(self.params["crowded_score_enter"]),
            crowded_score_exit=float(self.params["crowded_score_exit"]),
            liquidation_burst_enter=float(self.params["liquidation_burst_enter"]),
            base_position_pct=float(self.params["base_position_pct"]),
            max_position_pct=float(self.params["max_position_pct"]),
            min_signal_strength=float(self.params["min_signal_strength"]),
        )
        self._gate = LiquidationOICrowdingGate(self._config)

    def _context_from_prepared(self, data: pd.DataFrame) -> StructuralMarketContext:
        row = data.iloc[-1].to_dict()
        symbol = str(row.get("symbol", "UNKNOWN"))
        timestamp = self._bar_time(data)
        ctx = StructuralMarketContext.from_row(row, symbol=symbol, timestamp=timestamp)
        return update_derivatives_scores(ctx)

    def _protective_levels(self, price: float, direction: str, atr_pct: float) -> tuple[float, float]:
        stop_mult = float(self.params.get("hard_stop_atr_mult", 1.8))
        take_mult = float(self.params.get("take_profit_atr_mult", 2.4))
        atr_pct = max(safe_float(atr_pct), 0.002)
        if direction == "buy":
            return price * (1.0 - stop_mult * atr_pct), price * (1.0 + take_mult * atr_pct)
        return price * (1.0 + stop_mult * atr_pct), price * (1.0 - take_mult * atr_pct)

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data is None or data.empty or "close" not in data:
            return []
        min_rows = min(20, max(3, int(self.params.get("lookback_bars", 120)) // 4))
        if len(data) < min_rows:
            return []

        prepared = prepare_derivatives_features(data, lookback=int(self.params.get("lookback_bars", 120)))
        ctx = self._context_from_prepared(prepared)
        gate = self._gate.evaluate(ctx)
        row = prepared.iloc[-1]
        price = float(row["close"])
        signals: List[Signal] = []

        if bool(self.params.get("gate_mode", True)) and bool(self.params.get("emit_gate_hold_signal", True)):
            gate_reasons = set(gate.reason_codes or [])
            if gate.block_new_longs or gate.block_new_shorts or gate.gate_scalar < 1.0:
                signals.append(
                    Signal(
                        symbol=ctx.symbol,
                        signal_type=SignalType.HOLD,
                        price=price,
                        timestamp=ctx.timestamp,
                        strategy_name=self.name,
                        strength=max(0.1, clamp(gate.severity)),
                        metadata={
                            "structural_role": "risk_gate",
                            "risk_gate": gate.to_dict(),
                            "reason_codes": sorted(gate_reasons),
                        },
                    )
                )

        if not bool(self.params.get("trade_mode", True)):
            return signals

        long_reversal = detect_flush_reversal(prepared, side="long_flush", config=self._config)
        short_reversal = detect_flush_reversal(prepared, side="short_squeeze", config=self._config)
        candidates = [item for item in (long_reversal, short_reversal) if item.direction in {"buy", "sell"}]
        if not candidates:
            return signals
        decision = max(candidates, key=lambda item: item.strength)
        if decision.strength < float(self.params.get("min_signal_strength", 0.55)):
            return signals
        if decision.direction == "buy" and gate.block_new_longs:
            return signals
        if decision.direction == "sell" and gate.block_new_shorts:
            return signals

        atr_pct = safe_float(row.get("atr_pct"), 0.0)
        stop_loss, take_profit = self._protective_levels(price, decision.direction, atr_pct)
        signal_type = SignalType.BUY if decision.direction == "buy" else SignalType.SELL
        signal = Signal(
            symbol=ctx.symbol,
            signal_type=signal_type,
            price=price,
            timestamp=ctx.timestamp,
            strategy_name=self.name,
            strength=decision.strength,
            stop_loss=stop_loss,
            take_profit=take_profit,
            metadata={
                "structural_role": "trade_signal",
                "decision": decision.to_dict(),
                "risk_gate": gate.to_dict(),
                "base_position_pct": float(self.params.get("base_position_pct", 0.04)),
                "max_position_pct": float(self.params.get("max_position_pct", 0.08)),
                "max_holding_hours": int(self.params.get("max_holding_hours", 36)),
                "reason_codes": list(decision.reason_codes or []),
            },
        )
        signals.append(signal)
        logger.info(
            f"{self.name} {signal_type.value.upper()} {ctx.symbol} "
            f"strength={decision.strength:.3f} reasons={decision.reason_codes}"
        )
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "structural_derivatives",
            "columns": [
                "open",
                "high",
                "low",
                "close",
                "oi",
                "funding_rate",
                "global_long_short_ratio",
                "taker_imbalance",
                "liquidation_long_usd",
                "liquidation_short_usd",
                "spread_bps",
                "depth_score",
            ],
            "min_length": int(self.params.get("lookback_bars", 120)),
        }
