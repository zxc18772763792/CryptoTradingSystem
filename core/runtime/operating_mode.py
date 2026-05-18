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


def _iter_source_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        items: List[Dict[str, Any]] = []
        for key, value in payload.items():
            if isinstance(value, dict):
                item = dict(value)
                item.setdefault("key", str(key))
            else:
                item = {"key": str(key), "status": value}
            items.append(item)
        return items
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    return []


def _source_issue_detail(item: Dict[str, Any], status: str) -> str:
    issues = item.get("issues")
    if isinstance(issues, (list, tuple)):
        issue_text = "; ".join(str(issue) for issue in issues if str(issue).strip())
    elif issues is not None:
        issue_text = str(issues)
    else:
        issue_text = ""
    return str(
        issue_text
        or item.get("issue")
        or item.get("stale_reason")
        or item.get("error")
        or status
        or "not ready"
    )


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "stale"}
    return bool(value)


def _explicit_false(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    if isinstance(value, str):
        return value.strip().lower() in {"0", "false", "no", "n", "not_ready"}
    return False


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

    # Calibration maturity: be honest that the self-feedback loop may be cold.
    try:
        from pathlib import Path

        from core.observability.score_calibration import load_family_regime_priors, prior_graduation_policy

        prior_data = load_family_regime_priors()
        priors_map = dict(prior_data.get("priors") or {})
        if not priors_map or not prior_data.get("updated_at"):
            degradations.append(
                _degradation(
                    "feedback_priors_cold",
                    "Calibration priors are cold",
                    "no realized-performance feedback has been folded into family/regime priors yet",
                    "info",
                )
            )
        elif not any(prior_graduation_policy(dict(value or {})).get("binding_eligible") for value in priors_map.values()):
            degradations.append(
                _degradation(
                    "feedback_priors_advisory_only",
                    "Calibration priors have not graduated",
                    "priors need enough samples plus linked counterfactual outcomes before capped binding use is allowed",
                    "info",
                )
            )
        from core.audit.gate_counterfactuals import _resolve_audit_path

        if not Path(_resolve_audit_path()).exists():
            degradations.append(
                _degradation(
                    "cusum_feedback_inactive",
                    "Gate counterfactual audit is empty",
                    "no decay/gate counterfactual rows recorded yet — feedback loop unproven",
                    "info",
                )
            )
    except Exception:
        pass

    source_items: List[Dict[str, Any]] = []
    source_items.extend(_iter_source_items(sources.get("items")))
    source_items.extend(_iter_source_items(sources.get("sources")))
    categories = sources.get("categories") if isinstance(sources.get("categories"), dict) else {}
    for category_key, category in categories.items():
        if not isinstance(category, dict):
            continue
        for source in _iter_source_items(category.get("sources")):
            source_key = str(source.get("key") or source.get("source") or "unknown")
            source["key"] = f"{category_key}.{source_key}"
            source_items.append(source)
    for item in source_items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or item.get("health") or "").lower()
        ready = item.get("ready")
        stale = _truthy(item.get("stale"))
        if status in {"missing", "failed", "degraded", "stale"} or stale or _explicit_false(ready):
            degradations.append(
                _degradation(
                    f"source_{str(item.get('key') or item.get('source') or 'unknown')}",
                    "Source degraded",
                    _source_issue_detail(item, status),
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
