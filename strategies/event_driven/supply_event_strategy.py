"""Supply-event strategy with point-in-time event visibility."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from core.events.supply_event_store import SupplyEventStore
from core.strategies.strategy_base import Signal, SignalType, StrategyBase
from core.structural.context import EventContext, StructuralMarketContext, clamp, safe_float
from core.structural.supply_events import (
    SupplyEventConfig,
    SupplyEventGate,
    evaluate_supply_event_trade,
)


class SupplyEventStrategy(StrategyBase):
    mutates_input = False

    def __init__(self, name: str = "Supply_Event", params: Optional[Dict[str, Any]] = None):
        default_params: Dict[str, Any] = {
            "gate_mode": True,
            "trade_mode": True,
            "event_csv_path": "",
            "events": [],
            "pre_event_window_days": 14,
            "post_event_window_days": 14,
            "supply_pressure_enter": 0.70,
            "absorption_enter": 0.70,
            "event_blackout_hours_before": 2,
            "event_blackout_hours_after": 6,
            "base_position_pct": 0.03,
            "max_position_pct": 0.06,
            "emit_gate_hold_signal": True,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)
        self._config = SupplyEventConfig(
            pre_event_window_days=int(self.params["pre_event_window_days"]),
            post_event_window_days=int(self.params["post_event_window_days"]),
            supply_pressure_enter=float(self.params["supply_pressure_enter"]),
            absorption_enter=float(self.params["absorption_enter"]),
            event_blackout_hours_before=float(self.params["event_blackout_hours_before"]),
            event_blackout_hours_after=float(self.params["event_blackout_hours_after"]),
            base_position_pct=float(self.params["base_position_pct"]),
        )
        self._store = SupplyEventStore(self.params.get("events") or [])
        csv_path = str(self.params.get("event_csv_path") or "").strip()
        if csv_path:
            self._store.load_csv(csv_path)
        self._gate = SupplyEventGate(self._config)

    def upsert_event(self, event: Dict[str, Any]) -> None:
        self._store.upsert(event)

    def _market_snapshot(self, row: Dict[str, Any]) -> Dict[str, Any]:
        keys = [
            "avg_daily_volume_30d",
            "low_liquidity_score",
            "weak_market_regime_score",
            "pre_event_price_move_z",
            "pre_event_crowding",
            "news_mentions_z",
            "liquidity_score",
            "borrow_or_perp_available",
            "market_regime",
            "short_crowding_score",
            "price_recovers_event_vwap",
            "funding_reset",
            "price_holds_above_event_low",
            "volume_above_average_without_new_low",
            "funding_normalizes",
            "exchange_netflow_not_worsening",
        ]
        return {key: row[key] for key in keys if key in row}

    def _context(self, data: pd.DataFrame) -> tuple[StructuralMarketContext, Dict[str, Any]]:
        row = data.iloc[-1].to_dict()
        symbol = str(row.get("symbol", "UNKNOWN")).upper()
        timestamp = self._bar_time(data)
        active_events = [
            event.to_dict()
            for event in self._store.active_events(
                symbol=symbol,
                as_of=timestamp,
                before_days=int(self.params["pre_event_window_days"]),
                after_days=int(self.params["post_event_window_days"]),
                require_visible=True,
            )
        ]
        market = self._market_snapshot(row)
        ctx = StructuralMarketContext.from_row(row, symbol=symbol, timestamp=timestamp)
        ctx.events = EventContext(
            active_events=active_events,
            event_risk_score=safe_float(row.get("event_risk_score", 0.0)),
            supply_pressure_score=safe_float(row.get("supply_pressure_score", 0.0)),
            priced_in_score=safe_float(row.get("priced_in_score", 0.0)),
            absorption_score=safe_float(row.get("absorption_score", 0.0)),
            source="supply_event_store",
            as_of=timestamp,
        )
        return ctx, market

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data is None or data.empty or "close" not in data:
            return []
        ctx, market = self._context(data)
        if not ctx.events.active_events:
            return []
        row = data.iloc[-1]
        price = float(row["close"])
        signals: List[Signal] = []
        gate = self._gate.evaluate(ctx, market=market)

        if bool(self.params.get("gate_mode", True)) and bool(self.params.get("emit_gate_hold_signal", True)):
            if gate.block_new_longs or gate.gate_scalar < 1.0:
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
                            "reason_codes": list(gate.reason_codes or []),
                        },
                    )
                )

        if not bool(self.params.get("trade_mode", True)):
            return signals

        decision = evaluate_supply_event_trade(ctx, market=market, config=self._config)
        if decision.direction not in {"buy", "sell"}:
            return signals
        if decision.direction == "buy" and gate.block_new_longs:
            return signals

        signal_type = SignalType.BUY if decision.direction == "buy" else SignalType.SELL
        stop_loss = None
        take_profit = None
        if signal_type == SignalType.BUY and "event_low" in row:
            stop_loss = float(row["event_low"])
            take_profit = price * 1.06
        elif signal_type == SignalType.SELL:
            stop_loss = price * 1.04
            take_profit = price * 0.94

        signals.append(
            Signal(
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
                    "base_position_pct": float(self.params.get("base_position_pct", 0.03)),
                    "max_position_pct": float(self.params.get("max_position_pct", 0.06)),
                    "reason_codes": list(decision.reason_codes or []),
                },
            )
        )
        logger.info(f"{self.name} {signal_type.value.upper()} {ctx.symbol}: {decision.reason_codes}")
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "event_context",
            "columns": ["close", "symbol", "supply_pressure_score", "priced_in_score", "absorption_score"],
            "requires_event_store": True,
        }
