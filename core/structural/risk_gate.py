"""Execution-facing structural risk gate."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from core.strategies.strategy_base import Signal, SignalType
from core.structural.context import RiskGateDecision, StructuralMarketContext, clamp, parse_utc_datetime
from core.structural.derivatives_crowding import LiquidationOICrowdingGate
from core.structural.supply_events import SupplyEventGate


@dataclass
class StructuralRiskGateResult:
    allowed: bool
    reason_codes: List[str] = field(default_factory=list)
    adjusted_strength: float = 1.0
    gate: Optional[RiskGateDecision] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": bool(self.allowed),
            "reason_codes": list(self.reason_codes or []),
            "adjusted_strength": float(self.adjusted_strength),
            "gate": self.gate.to_dict() if self.gate else None,
            "metadata": dict(self.metadata or {}),
        }


def _gate_from_metadata(signal: Signal, meta: Mapping[str, Any]) -> Optional[RiskGateDecision]:
    raw = meta.get("risk_gate") or meta.get("structural_risk_gate")
    if not isinstance(raw, Mapping):
        return None
    return RiskGateDecision(
        symbol=str(raw.get("symbol") or signal.symbol),
        timestamp=parse_utc_datetime(raw.get("timestamp") or signal.timestamp),
        allow_long=bool(raw.get("allow_long", True)),
        allow_short=bool(raw.get("allow_short", True)),
        block_new_longs=bool(raw.get("block_new_longs", False)),
        block_new_shorts=bool(raw.get("block_new_shorts", False)),
        reduce_long_position_scalar=float(raw.get("reduce_long_position_scalar", 1.0) or 1.0),
        reduce_short_position_scalar=float(raw.get("reduce_short_position_scalar", 1.0) or 1.0),
        gate_scalar=float(raw.get("gate_scalar", 1.0) or 1.0),
        severity=float(raw.get("severity", 0.0) or 0.0),
        reason_codes=list(raw.get("reason_codes") or ["structural_gate_from_signal_metadata"]),
        metadata=dict(raw.get("metadata") or {}),
    )


class StructuralRiskGate:
    def __init__(self):
        self._derivatives_gate = LiquidationOICrowdingGate()
        self._supply_event_gate = SupplyEventGate()

    def _gate_from_context(self, signal: Signal, meta: Mapping[str, Any]) -> Optional[RiskGateDecision]:
        raw_context = meta.get("structural_context")
        if not isinstance(raw_context, Mapping):
            return None
        timestamp = raw_context.get("timestamp") or signal.timestamp or datetime.now(timezone.utc)
        symbol = str(raw_context.get("symbol") or signal.symbol)
        flat_context = dict(raw_context)
        for section in ("derivatives", "events", "onchain", "execution"):
            nested = raw_context.get(section)
            if isinstance(nested, Mapping):
                flat_context.update(dict(nested))
        ctx = StructuralMarketContext.from_row(flat_context, symbol=symbol, timestamp=timestamp)
        derivative_gate = self._derivatives_gate.evaluate(ctx)
        event_gate = self._supply_event_gate.evaluate(ctx)
        return RiskGateDecision.combine(symbol, ctx.timestamp, [derivative_gate, event_gate])

    def evaluate_signal(self, signal: Signal) -> StructuralRiskGateResult:
        if signal.signal_type in {SignalType.CLOSE_LONG, SignalType.CLOSE_SHORT}:
            return StructuralRiskGateResult(
                allowed=True,
                adjusted_strength=clamp(signal.strength, 0.0, 1.0),
                reason_codes=["structural_gate_reduce_only_bypass"],
            )
        if signal.signal_type not in {SignalType.BUY, SignalType.SELL}:
            return StructuralRiskGateResult(
                allowed=True,
                adjusted_strength=clamp(signal.strength, 0.0, 1.0),
                reason_codes=["structural_gate_non_entry_bypass"],
            )

        meta = dict(signal.metadata or {})
        gate = _gate_from_metadata(signal, meta) or self._gate_from_context(signal, meta)
        position_scalar = meta.get("position_scalar")
        scalar_meta = dict(position_scalar or {}) if isinstance(position_scalar, Mapping) else {}
        if gate is None:
            return StructuralRiskGateResult(
                allowed=True,
                adjusted_strength=clamp(signal.strength, 0.0, 1.0),
                reason_codes=["structural_gate_no_context_bypass"],
            )

        side = signal.signal_type
        if side == SignalType.BUY and (gate.block_new_longs or not gate.allow_long):
            return StructuralRiskGateResult(
                allowed=False,
                adjusted_strength=0.0,
                gate=gate,
                reason_codes=list(gate.reason_codes or []) + ["structural_gate_blocked_buy"],
            )
        if side == SignalType.SELL and (gate.block_new_shorts or not gate.allow_short):
            return StructuralRiskGateResult(
                allowed=False,
                adjusted_strength=0.0,
                gate=gate,
                reason_codes=list(gate.reason_codes or []) + ["structural_gate_blocked_sell"],
            )

        gate_scalar = gate.reduce_long_position_scalar if side == SignalType.BUY else gate.reduce_short_position_scalar
        scalar = float(scalar_meta.get("long_scalar" if side == SignalType.BUY else "short_scalar", 1.0) or 1.0)
        adjusted = clamp(float(signal.strength or 0.0) * clamp(gate_scalar, 0.0, 1.0) * clamp(scalar, 0.0, 1.2), 0.0, 1.0)
        return StructuralRiskGateResult(
            allowed=True,
            adjusted_strength=adjusted,
            gate=gate,
            reason_codes=list(gate.reason_codes or []) + ["structural_gate_allowed"],
            metadata={"position_scalar": scalar_meta},
        )


structural_risk_gate = StructuralRiskGate()
