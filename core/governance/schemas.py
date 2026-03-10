from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field, model_validator


Role = Literal["RESEARCH_LEAD", "RISK_OWNER", "OPERATOR", "AUDITOR", "ENGINEER", "SYSTEM"]
StrategyLifecycleState = Literal["proposed", "approved", "paper", "live", "retired"]
RiskChangeStatus = Literal["pending", "approved", "rejected", "applied"]


class StrategySpecPayload(BaseModel):
    strategy_id: str
    version: int = 1
    name: str
    strategy_class: str
    status: StrategyLifecycleState = "proposed"
    params: Dict[str, Any] = Field(default_factory=dict)
    guardrails: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    regime: str = "mixed"


class RiskConfigPayload(BaseModel):
    max_leverage: float = 3.0
    max_position_notional_pct: float = 0.1
    max_trade_risk_pct: float = 0.02
    max_daily_drawdown_pct: float = 0.02
    spread_limit_bps: float = 25.0
    data_staleness_limit_ms: int = 60_000
    allowed_symbols: List[str] = Field(default_factory=list)
    allowed_timeframes: List[str] = Field(default_factory=list)
    reduce_only: bool = False
    kill_switch: bool = False


class RiskChangeRequestPayload(BaseModel):
    reason: str = ""
    proposed_config: RiskConfigPayload


_FORBIDDEN_TRADE_TERMS = [
    "买入",
    "卖出",
    "开多",
    "开空",
    "下单",
    "市价",
    "限价",
    "杠杆",
    "long",
    "short",
    "market order",
    "limit order",
]


def _contains_forbidden_trade_instruction(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return any(_contains_forbidden_trade_instruction(v) for v in value.values())
    if isinstance(value, list):
        return any(_contains_forbidden_trade_instruction(v) for v in value)
    text = str(value).lower()
    return any(term in text for term in _FORBIDDEN_TRADE_TERMS)


class LLMResearchOutput(BaseModel):
    hypothesis: str
    experiment_plan: List[str] = Field(default_factory=list)
    metrics_to_check: List[str] = Field(default_factory=list)
    expected_failure_modes: List[str] = Field(default_factory=list)
    proposed_strategy_changes: List[Dict[str, Any]] = Field(default_factory=list)
    uncertainty: str = ""
    evidence_refs: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def validate_no_direct_trade_instruction(self) -> "LLMResearchOutput":
        fields = [
            self.hypothesis,
            self.experiment_plan,
            self.metrics_to_check,
            self.expected_failure_modes,
            self.proposed_strategy_changes,
            self.uncertainty,
            self.evidence_refs,
        ]
        if _contains_forbidden_trade_instruction(fields):
            raise ValueError("LLM output contains direct trading instruction terms")
        return self

