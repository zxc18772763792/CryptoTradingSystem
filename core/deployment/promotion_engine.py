"""Promotion engine for validated AI research candidates."""
from __future__ import annotations

from datetime import datetime, timezone
import secrets
from typing import Any, Dict, Optional

from fastapi import FastAPI

import strategies as strategy_module
from config.settings import settings
from config.strategy_registry import get_strategy_defaults
from core.ai.proposal_schemas import ResearchProposal
from core.research.experiment_registry import LifecycleRegistry
from core.research.experiment_schemas import LifecycleRecord, PromotionDecision, StrategyCandidate
from core.strategies.runtime_policy import build_runtime_limit_policy
from core.strategies import strategy_manager
from core.strategies.persistence import persist_strategy_snapshot
from core.trading.execution_engine import execution_engine


_PROPOSAL_TRANSITIONS = {
    "draft": {"research_queued", "research_running"},
    "research_queued": {"research_running"},
    "research_running": {"validated", "rejected"},
    "validated": {"paper_running", "shadow_running", "live_candidate", "retired", "research_queued", "research_running"},
    "paper_running": {"retired"},
    "shadow_running": {"retired"},
    "live_candidate": {"retired"},
    "rejected": {"retired", "research_queued", "research_running"},
}
_CANDIDATE_TRANSITIONS = {
    "new": {"paper_running", "shadow_running", "live_candidate", "retired"},
    "paper_running": {"retired"},
    "shadow_running": {"retired"},
    "live_candidate": {"retired"},
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def record_lifecycle(
    lifecycle_registry: LifecycleRegistry,
    *,
    object_type: str,
    object_id: str,
    from_state: Optional[str],
    to_state: str,
    actor: str,
    reason: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> LifecycleRecord:
    record = LifecycleRecord(
        object_type=str(object_type),
        object_id=str(object_id),
        from_state=str(from_state) if from_state is not None else None,
        to_state=str(to_state),
        actor=str(actor or "system"),
        ts=_now_utc(),
        reason=str(reason or ""),
        metadata=dict(metadata or {}),
    )
    lifecycle_registry.append(record)
    return record


def transition_proposal(
    proposal: ResearchProposal,
    *,
    to_state: str,
    lifecycle_registry: LifecycleRegistry,
    actor: str,
    reason: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> ResearchProposal:
    from_state = str(proposal.status)
    allowed = _PROPOSAL_TRANSITIONS.get(from_state, set())
    if str(to_state) not in allowed and from_state != str(to_state):
        raise ValueError(f"invalid proposal transition: {from_state} -> {to_state}")
    proposal.status = str(to_state)
    proposal.updated_at = _now_utc()
    record_lifecycle(
        lifecycle_registry,
        object_type="proposal",
        object_id=proposal.proposal_id,
        from_state=from_state,
        to_state=str(to_state),
        actor=actor,
        reason=reason,
        metadata=metadata,
    )
    return proposal


def transition_candidate(
    candidate: StrategyCandidate,
    *,
    to_state: str,
    lifecycle_registry: LifecycleRegistry,
    actor: str,
    reason: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> StrategyCandidate:
    from_state = str(candidate.status)
    allowed = _CANDIDATE_TRANSITIONS.get(from_state, set())
    if str(to_state) not in allowed and from_state != str(to_state):
        raise ValueError(f"invalid candidate transition: {from_state} -> {to_state}")
    candidate.status = str(to_state)
    record_lifecycle(
        lifecycle_registry,
        object_type="candidate",
        object_id=candidate.candidate_id,
        from_state=from_state,
        to_state=str(to_state),
        actor=actor,
        reason=reason,
        metadata=metadata,
    )
    return candidate


def _resolve_strategy_class(strategy_type: str):
    return getattr(strategy_module, str(strategy_type), None)


def _resolve_observed_trades_per_day(app: FastAPI, candidate: StrategyCandidate) -> Optional[float]:
    best = dict(candidate.metadata.get("best") or {})
    trade_count = float(best.get("total_trades") or best.get("trade_count") or 0.0)
    if trade_count <= 0 and candidate.validation_summary is not None:
        try:
            metrics_best = dict((candidate.validation_summary.metrics or {}).get("best") or {})
            trade_count = float(metrics_best.get("total_trades") or metrics_best.get("trade_count") or 0.0)
        except Exception:
            trade_count = 0.0
    if trade_count <= 0:
        return None

    days = None
    try:
        registry = getattr(app.state, "ai_experiment_registry", None)
        if registry is not None:
            exp = registry.get(candidate.experiment_id)
            if exp is not None:
                days = float(getattr(exp, "days", 0.0) or 0.0)
    except Exception:
        days = None
    if not days or days <= 0:
        try:
            days = float(candidate.metadata.get("research_days") or 0.0)
        except Exception:
            days = 0.0
    if not days or days <= 0:
        return None
    return float(trade_count / days)


async def promote_candidate(
    app: FastAPI,
    *,
    proposal: ResearchProposal,
    candidate: StrategyCandidate,
    promotion: PromotionDecision,
    actor: str,
) -> Dict[str, Any]:
    lifecycle_registry: LifecycleRegistry = app.state.ai_lifecycle_registry
    decision = str(promotion.decision or "reject")
    candidate.promotion = promotion
    candidate.promotion_target = decision if decision in {"paper", "shadow", "live_candidate"} else None

    if decision == "reject":
        return {"candidate": candidate, "proposal": proposal, "promotion": promotion, "runtime_status": "rejected"}

    if decision == "live_candidate":
        transition_candidate(candidate, to_state="live_candidate", lifecycle_registry=lifecycle_registry, actor=actor, reason=promotion.reason)
        transition_proposal(proposal, to_state="live_candidate", lifecycle_registry=lifecycle_registry, actor=actor, reason=promotion.reason)
        candidate.metadata["promotion_runtime"] = {
            "mode": "candidate_only",
            "approval_required": True,
            "promoted_at": _now_utc().isoformat(),
        }
        return {
            "candidate": candidate,
            "proposal": proposal,
            "promotion": promotion,
            "runtime_status": "live_candidate",
            "registered_strategy_name": None,
        }

    strategy_name = f"{candidate.strategy}_ai_{int(_now_utc().timestamp())}_{secrets.token_hex(2)}"
    strategy_class = _resolve_strategy_class(candidate.strategy)
    if strategy_class is None:
        raise ValueError(f"unknown strategy class for promotion: {candidate.strategy}")

    params = dict(get_strategy_defaults(candidate.strategy))
    params.update(dict(candidate.params or {}))
    params.setdefault("exchange", str(candidate.metadata.get("exchange") or "binance"))
    params.setdefault("account_id", f"ai_{strategy_name.lower()}")

    if decision == "paper" and execution_engine.get_trading_mode() != "paper":
        raise RuntimeError("current system trading mode is not paper; refusing automatic paper promotion")

    if decision == "paper":
        default_allocation = max(0.0, min(1.0, float(getattr(settings, "DEFAULT_STRATEGY_ALLOCATION", 0.15) or 0.15)))
        constraints = dict(promotion.constraints or {})
        runtime_limit_minutes: Optional[int]
        runtime_policy: Dict[str, Any]
        runtime_override = constraints.get("runtime_limit_minutes")
        if runtime_override is not None:
            runtime_limit_minutes = max(0, int(float(runtime_override)))
            runtime_limit_minutes = runtime_limit_minutes or None
            runtime_policy = {
                "runtime_limit_minutes": runtime_limit_minutes,
                "source": "promotion_constraint",
            }
        else:
            observed_tpd = _resolve_observed_trades_per_day(app, candidate)
            runtime_policy = build_runtime_limit_policy(
                timeframe=str(candidate.timeframe or "1h"),
                params=params,
                observed_trades_per_day=observed_tpd,
            )
            runtime_limit_minutes = int(runtime_policy["runtime_limit_minutes"])
        ok = strategy_manager.register_strategy(
            name=strategy_name,
            strategy_class=strategy_class,
            params=params,
            symbols=[candidate.symbol],
            timeframe=candidate.timeframe,
            allocation=float(constraints.get("allocation_cap", default_allocation) or default_allocation),
            runtime_limit_minutes=runtime_limit_minutes,
        )
        if not ok:
            raise RuntimeError("strategy registration failed during paper promotion")
        started = await strategy_manager.start_strategy(strategy_name)
        if not started:
            raise RuntimeError("strategy start failed during paper promotion")
        await persist_strategy_snapshot(strategy_name, state_override="running")
        transition_candidate(candidate, to_state="paper_running", lifecycle_registry=lifecycle_registry, actor=actor, reason=promotion.reason, metadata={"strategy_name": strategy_name})
        transition_proposal(proposal, to_state="paper_running", lifecycle_registry=lifecycle_registry, actor=actor, reason=promotion.reason, metadata={"strategy_name": strategy_name})
        candidate.metadata["promotion_runtime"] = {
            "mode": "paper",
            "registered_strategy_name": strategy_name,
            "started": True,
            "runtime_limit_minutes": runtime_limit_minutes,
            "runtime_policy": runtime_policy,
            "promoted_at": _now_utc().isoformat(),
        }
        candidate.metadata["registered_strategy_name"] = strategy_name
        return {
            "candidate": candidate,
            "proposal": proposal,
            "promotion": promotion,
            "runtime_status": "paper_running",
            "registered_strategy_name": strategy_name,
        }

    transition_candidate(candidate, to_state="shadow_running", lifecycle_registry=lifecycle_registry, actor=actor, reason=promotion.reason)
    transition_proposal(proposal, to_state="shadow_running", lifecycle_registry=lifecycle_registry, actor=actor, reason=promotion.reason)
    candidate.metadata["promotion_runtime"] = {
        "mode": "shadow_virtual",
        "registered_strategy_name": strategy_name,
        "started": False,
        "promoted_at": _now_utc().isoformat(),
    }
    return {
        "candidate": candidate,
        "proposal": proposal,
        "promotion": promotion,
        "runtime_status": "shadow_running",
        "registered_strategy_name": strategy_name,
    }
