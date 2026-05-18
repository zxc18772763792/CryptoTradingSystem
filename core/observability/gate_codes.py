"""Canonical gate codes shared by validation, aggregation, autonomy, and audit."""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict


class GateCode(str, Enum):
    """str-mixin enum (Python 3.9 compatible; StrEnum is 3.11+)."""

    # Validation / promotion
    NO_VALID_RESEARCH_RUNS = "no_valid_research_runs"
    EFFECTIVE_SHARPE_SOURCE = "effective_sharpe_source"
    DEPLOYMENT_TIER = "deployment_tier"
    NO_OOS_LIVE_CAP = "no_oos_live_cap"
    DSR_REJECT = "dsr_reject"
    DSR_DOWNGRADE = "dsr_downgrade"
    DSR_GATE = "dsr_gate"
    TRADE_COUNT_MIN_SHADOW = "trade_count_min_shadow"
    TRADE_COUNT_LIVE_CANDIDATE = "trade_count_live_candidate"
    TRADE_COUNT_PAPER = "trade_count_paper"
    TRADE_COUNT_GATE = "trade_count_gate"
    CORRELATION_REDUNDANT = "correlation_redundant"

    # Signal aggregation
    COMPONENT_LLM = "component_llm"
    COMPONENT_ML = "component_ml"
    COMPONENT_FACTOR = "component_factor"
    COMPONENT_DERIVATIVES = "component_derivatives"
    RISK_GATE = "risk_gate"
    APPROVAL_THRESHOLD = "approval_threshold"

    # Autonomous agent diagnostics
    DECISION_COMPLETED = "decision_completed"
    MODEL_ERROR = "model_error"
    NO_PRICE = "no_price"
    BELOW_MIN_CONFIDENCE = "below_min_confidence"
    COOLDOWN = "cooldown"
    REVIEW_COOLDOWN = "review_cooldown"
    REVIEW_RISK_HALT = "review_risk_halt"
    RISK_HALT = "risk_halt"
    AGGREGATED_RISK_BLOCKED = "aggregated_risk_blocked"
    SHADOW_MODE = "shadow_mode"
    LIVE_MODE_BLOCKED = "live_mode_blocked"
    SUBMIT_REJECTED = "submit_rejected"

    # Monitoring / audit
    CUSUM_DECAY = "cusum_decay"

    def __str__(self) -> str:  # match StrEnum: str()/f-string yield the value
        return str(self.value)


GATE_CODE_REGISTRY: Dict[str, Dict[str, Any]] = {
    code.value: {
        "code": code.value,
        "name": code.name.lower(),
    }
    for code in GateCode
}


def normalize_gate_code(code: Any) -> str:
    if isinstance(code, GateCode):
        return code.value
    text = str(code or "").strip()
    if not text:
        return ""
    return text.lower().replace(" ", "_").replace("-", "_")


def is_registered_gate_code(code: Any) -> bool:
    return normalize_gate_code(code) in GATE_CODE_REGISTRY


def describe_gate_code(code: Any) -> Dict[str, Any]:
    normalized = normalize_gate_code(code)
    return dict(GATE_CODE_REGISTRY.get(normalized) or {"code": normalized, "unregistered": True})
