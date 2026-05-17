"""Structured decision traces for gates, downgrades, and degraded modes."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


GateStatus = Literal["pass", "warn", "block", "downgrade", "skip", "shadow", "degraded"]

_STATUS_PRIORITY: Dict[str, int] = {
    "block": 0,
    "downgrade": 1,
    "degraded": 2,
    "warn": 3,
    "shadow": 4,
    "skip": 8,
    "pass": 9,
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class DecisionGateTrace(BaseModel):
    code: str
    label: str = ""
    status: GateStatus = "pass"
    severity: int = 0
    input_value: Any = None
    threshold: Any = None
    margin: Optional[float] = None
    decision_before: str = ""
    decision_after: str = ""
    reason: str = ""
    counterfactual_decision: str = ""
    source: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")


class DecisionTrace(BaseModel):
    trace_id: str = Field(default_factory=lambda: f"trace-{uuid4().hex[:16]}")
    subject_type: str = ""
    subject_id: str = ""
    stage: str = ""
    input_summary: Dict[str, Any] = Field(default_factory=dict)
    final_decision: str = ""
    effective_score: Optional[float] = None
    root_blocker_code: str = ""
    root_blocker_label: str = ""
    gates: List[DecisionGateTrace] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=_now_utc)

    def refresh_root_blocker(self) -> "DecisionTrace":
        root = build_root_blocker(self.gates)
        if root is None:
            self.root_blocker_code = ""
            self.root_blocker_label = ""
        else:
            self.root_blocker_code = root.code
            self.root_blocker_label = root.label or root.reason or root.code
        return self

    def to_dict(self) -> Dict[str, Any]:
        self.refresh_root_blocker()
        return self.model_dump(mode="json")


def build_root_blocker(gates: List[DecisionGateTrace]) -> Optional[DecisionGateTrace]:
    actionable = [
        gate
        for gate in gates or []
        if str(gate.status or "") not in {"pass", "skip"} and str(gate.code or "").strip()
    ]
    if not actionable:
        return None
    return sorted(
        actionable,
        key=lambda gate: (
            _STATUS_PRIORITY.get(str(gate.status or "pass"), 9),
            -int(gate.severity or 0),
            str(gate.code or ""),
        ),
    )[0]


def append_gate(
    trace: DecisionTrace,
    *,
    code: str,
    label: str = "",
    status: GateStatus = "pass",
    severity: int = 0,
    input_value: Any = None,
    threshold: Any = None,
    margin: Optional[float] = None,
    decision_before: str = "",
    decision_after: str = "",
    reason: str = "",
    counterfactual_decision: str = "",
    source: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> DecisionGateTrace:
    gate = DecisionGateTrace(
        code=str(code or "").strip(),
        label=str(label or code or "").strip(),
        status=status,
        severity=int(severity or 0),
        input_value=input_value,
        threshold=threshold,
        margin=margin,
        decision_before=str(decision_before or ""),
        decision_after=str(decision_after or ""),
        reason=str(reason or ""),
        counterfactual_decision=str(counterfactual_decision or ""),
        source=str(source or ""),
        metadata=dict(metadata or {}),
    )
    trace.gates.append(gate)
    trace.refresh_root_blocker()
    return gate


def coerce_trace_dict(trace: DecisionTrace | Dict[str, Any] | None) -> Dict[str, Any]:
    if trace is None:
        return {}
    if isinstance(trace, DecisionTrace):
        return trace.to_dict()
    if isinstance(trace, dict):
        return dict(trace)
    return {}

