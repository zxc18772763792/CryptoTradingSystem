"""Authoritative operating-mode snapshot for AI/runtime surfaces."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from config.settings import settings


class OperatingModeSnapshot(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    trading_mode: str = "paper"
    decision_mode: str = "shadow"
    governance_enabled: bool = False
    ai_live_decision: Dict[str, Any] = Field(default_factory=dict)
    autonomous_agent: Dict[str, Any] = Field(default_factory=dict)
    coinglass: Dict[str, Any] = Field(default_factory=dict)
    source_health: Dict[str, Any] = Field(default_factory=dict)
    runtime_state: Dict[str, Any] = Field(default_factory=dict)
    degradations: List[Dict[str, Any]] = Field(default_factory=list)
    safety: Dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")


def _degradation(code: str, label: str, detail: str = "", severity: str = "warn") -> Dict[str, Any]:
    return {
        "code": code,
        "label": label,
        "detail": detail,
        "severity": severity,
    }


def validate_operating_mode(
    *,
    live_decision_config: Dict[str, Any] | None = None,
    agent_config: Dict[str, Any] | None = None,
    runtime_state_snapshot: Dict[str, Any] | None = None,
    source_health: Dict[str, Any] | None = None,
) -> OperatingModeSnapshot:
    live_cfg = dict(live_decision_config or {})
    agent_cfg = dict(agent_config or {})
    sources = dict(source_health or {})
    runtime_snapshot = dict(runtime_state_snapshot or {})
    trading_mode = str(getattr(settings, "TRADING_MODE", "paper") or "paper").lower()
    decision_mode = str(getattr(settings, "DECISION_MODE", "shadow") or "shadow").lower()
    degradations: List[Dict[str, Any]] = []

    if bool(live_cfg.get("provider_fallback")):
        degradations.append(
            _degradation(
                "provider_fallback",
                "AI provider fallback active",
                f"requested={live_cfg.get('provider_requested')} resolved={live_cfg.get('provider')}",
            )
        )
    providers = dict(live_cfg.get("providers") or {})
    provider_name = str(live_cfg.get("provider") or "")
    if provider_name and not bool((providers.get(provider_name) or {}).get("available", True)):
        degradations.append(_degradation("provider_unavailable", "Configured AI provider unavailable", provider_name, "danger"))
    if bool(live_cfg.get("enabled")) and bool(live_cfg.get("fail_open")):
        degradations.append(_degradation("ai_live_fail_open", "AI live decision fail-open", "model failure may allow fallback behavior"))
    if not bool(agent_cfg.get("allow_live")):
        degradations.append(_degradation("autonomous_allow_live_false", "Autonomous agent is not live-enabled", "agent can only paper/shadow unless allow_live is enabled", "info"))
    if trading_mode == "live" and not bool(agent_cfg.get("allow_live")):
        degradations.append(_degradation("live_mode_agent_blocked", "Trading live but agent live blocked", "TRADING_MODE=live while autonomous allow_live=false", "danger"))
    if not bool(getattr(settings, "COINGLASS_LIVE_GATING_ENABLED", False)):
        degradations.append(_degradation("derivatives_shadow_only", "Derivatives are shadow-only", "COINGLASS_LIVE_GATING_ENABLED=false", "info"))

    source_items = list(sources.get("items") or sources.get("sources") or [])
    for category_key, category in dict(sources.get("categories") or {}).items():
        for source_key, source in dict((category or {}).get("sources") or {}).items():
            if isinstance(source, dict):
                source_items.append({"key": f"{category_key}.{source_key}", **source})
    for item in source_items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or item.get("health") or "").lower()
        ready = item.get("ready")
        stale = bool(item.get("stale"))
        if status in {"missing", "failed", "degraded", "stale"} or stale or ready is False:
            degradations.append(
                _degradation(
                    f"source_{str(item.get('key') or item.get('source') or 'unknown')}",
                    "Source degraded",
                    str("; ".join([str(x) for x in item.get("issues") or []]) or item.get("issue") or item.get("stale_reason") or item.get("error") or status or "not ready"),
                    "warn" if status != "failed" else "danger",
                )
            )

    if bool(getattr(settings, "AI_MARKET_STATE_RISK_POSTURE_ENABLED", True)) and not bool(
        getattr(settings, "AI_MARKET_STATE_RISK_POSTURE_LIVE_ENFORCE", False)
    ):
        degradations.append(
            _degradation(
                "market_state_risk_posture_advisory_live",
                "Market-state risk posture is advisory for live",
                "paper/shadow may tighten posture; live enforcement requires explicit opt-in",
                "info",
            )
        )

    return OperatingModeSnapshot(
        trading_mode=trading_mode,
        decision_mode=decision_mode,
        governance_enabled=bool(getattr(settings, "GOVERNANCE_ENABLED", False)),
        ai_live_decision=live_cfg,
        autonomous_agent=agent_cfg,
        coinglass={
            "live_gating_enabled": bool(getattr(settings, "COINGLASS_LIVE_GATING_ENABLED", False)),
            "enabled": bool(getattr(settings, "COINGLASS_ENABLED", False)),
        },
        source_health=sources,
        runtime_state=runtime_snapshot,
        degradations=degradations,
        safety={
            "market_state_risk_posture_enabled": bool(getattr(settings, "AI_MARKET_STATE_RISK_POSTURE_ENABLED", True)),
            "market_state_risk_posture_live_enforce": bool(getattr(settings, "AI_MARKET_STATE_RISK_POSTURE_LIVE_ENFORCE", False)),
        },
    )
