from __future__ import annotations

from typing import Any, Dict, Mapping


_CAPABILITY_LIVE_DECISION_REVIEW = "live_decision_review"
_CAPABILITY_AUTONOMOUS_LIVE_EXECUTION = "autonomous_live_execution"

_PROVIDER_RUNTIME_RULES = {
    "codex": {
        _CAPABILITY_LIVE_DECISION_REVIEW: {
            "status": "restricted",
            "reason_code": "provider_live_decision_restricted",
            "reason": (
                "Current codex/OpenAI-compatible provider chain should not be used to "
                "automatically approve or deny live trading execution."
            ),
        },
        _CAPABILITY_AUTONOMOUS_LIVE_EXECUTION: {
            "status": "restricted",
            "reason_code": "provider_live_execution_restricted",
            "reason": (
                "Current codex/OpenAI-compatible provider chain should not be used for "
                "autonomous live trading execution."
            ),
        },
    },
}


def provider_runtime_policy(provider: Any, capability: str) -> Dict[str, Any]:
    provider_text = str(provider or "").strip().lower() or "unknown"
    capability_text = str(capability or "").strip().lower()
    raw = dict((_PROVIDER_RUNTIME_RULES.get(provider_text, {}) or {}).get(capability_text, {}))
    status = str(raw.get("status") or "unknown").strip().lower() or "unknown"
    return {
        "provider": provider_text,
        "capability": capability_text,
        "status": status,
        "restricted": status == "restricted",
        "reason_code": str(raw.get("reason_code") or "").strip(),
        "reason": str(raw.get("reason") or "").strip(),
    }


def provider_runtime_capability_catalog(provider: Any) -> Dict[str, Dict[str, Any]]:
    return {
        _CAPABILITY_LIVE_DECISION_REVIEW: provider_runtime_policy(provider, _CAPABILITY_LIVE_DECISION_REVIEW),
        _CAPABILITY_AUTONOMOUS_LIVE_EXECUTION: provider_runtime_policy(provider, _CAPABILITY_AUTONOMOUS_LIVE_EXECUTION),
    }


def resolve_provider_for_runtime_capability(
    *,
    requested_provider: Any,
    providers: Mapping[str, Mapping[str, Any]] | None,
    capability: str,
) -> Dict[str, Any]:
    requested = str(requested_provider or "").strip().lower()
    providers_map = dict(providers or {})
    ordered = []
    if requested:
        ordered.append(requested)
    for name in providers_map:
        text = str(name or "").strip().lower()
        if text and text not in ordered:
            ordered.append(text)

    first_policy = provider_runtime_policy(requested, capability)
    for candidate in ordered:
        meta = providers_map.get(candidate) or {}
        if not bool(meta.get("available")):
            continue
        policy = provider_runtime_policy(candidate, capability)
        if not policy.get("restricted"):
            return {
                "provider": candidate,
                "fallback": candidate != requested,
                "policy": policy,
                "requested_policy": first_policy,
                "restricted": False,
            }

    return {
        "provider": requested,
        "fallback": False,
        "policy": first_policy,
        "requested_policy": first_policy,
        "restricted": bool(first_policy.get("restricted")),
    }
