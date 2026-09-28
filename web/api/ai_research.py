"""Web API for AI research workbench."""
from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from config.database import StrategyPerformanceSnapshot, async_session_maker
from config.settings import settings
from core.ai.autonomous_agent import autonomous_trading_agent
from core.ai.live_decision_router import live_decision_router
from core.ai.research_runtime_context import resolve_runtime_research_context
from core.ai.runtime_strategy_metadata import (
    ai_research_runtime_fingerprint_from_strategy_info,
    ai_research_runtime_fingerprint_for_candidate,
    build_ai_research_strategy_metadata,
)
from core.backtest.funding_provider import FundingProviderConfig, FundingRateProvider
from core.governance.audit import GovernanceAuditEvent, write_audit
from core.deployment.promotion_engine import transition_candidate, transition_proposal
from core.news.storage import db as news_db
from core.trading import execution_engine, order_manager, position_manager
from web.api.auth import require_sensitive_ops_permissions
from core.research.orchestrator import (
    cancel_proposal_job,
    create_manual_proposal,
    delete_orphan_candidate,
    delete_proposal,
    ensure_ai_research_runtime_state,
    generate_planned_proposal,
    get_candidate,
    get_deployment_status,
    get_experiment,
    get_proposal,
    list_candidates,
    list_experiment_runs,
    list_experiments,
    list_lifecycle,
    list_promotions,
    list_proposals,
    promote_existing_candidate,
    run_proposal,
    save_proposal,
)


router = APIRouter(
    dependencies=[Depends(require_sensitive_ops_permissions("read_trading_state"))]
)
# Market-data parquet/live indexes are tz-naive UTC (data_storage coerces to UTC
# then drops tzinfo). Naive bar timestamps must therefore be interpreted as UTC —
# localizing them as an Asia/Shanghai wall clock shifts each bar ~8h earlier and
# inflates the freshness age by ~8h, marking every fresh signal "数据旧".
SIGNAL_MARKET_DATA_TIMEZONE = timezone.utc
_OPERATING_MODE_CACHE_TTL_SEC = 20.0
_OPERATING_MODE_BUILD_TIMEOUT_SEC = 2.5
_OPERATING_MODE_CACHE: Dict[str, Any] = {"payload": None, "expires_at": None}
_OPERATING_MODE_CACHE_TASK: Optional[asyncio.Task] = None
_SOURCES_HEALTH_CACHE_TTL_SEC = 30.0
_SOURCES_HEALTH_BUILD_TIMEOUT_SEC = 8.0
_SOURCES_HEALTH_CACHE: Dict[str, Any] = {"payload": None, "expires_at": None}
_SOURCES_HEALTH_CACHE_TASK: Optional[asyncio.Task] = None


class AIPlannerGenerateRequest(BaseModel):
    goal: str = Field(..., min_length=8, max_length=600)
    market_regime: str = "mixed"
    symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT"])
    timeframes: List[str] = Field(default_factory=lambda: ["15m", "1h"])
    constraints: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    origin_context: Dict[str, Any] = Field(default_factory=dict)
    # E: market context for strategy selection influence
    market_context: Dict[str, Any] = Field(default_factory=dict)
    llm_research_output: Dict[str, Any] = Field(default_factory=dict)


class AIProposalCreateRequest(BaseModel):
    thesis: str = Field(..., min_length=8, max_length=600)
    symbols: List[str] = Field(default_factory=lambda: ["BTCUSDT"])
    timeframes: List[str] = Field(default_factory=lambda: ["5m", "15m", "1h"])
    market_regime: str = "mixed"
    strategy_templates: List[str] = Field(default_factory=list)
    source: str = Field(default="human")
    expected_holding_period: str = "1d"
    risk_hypothesis: str = ""
    invalidation_rules: List[str] = Field(default_factory=list)
    required_features: List[str] = Field(default_factory=list)
    parameter_space: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    notes: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AIProposalRunRequest(BaseModel):
    exchange: str = "binance"
    symbol: Optional[str] = None
    days: int = Field(default=30, ge=1, le=3650)
    commission_rate: float = Field(default=0.0004, ge=0.0, le=1.0)
    slippage_bps: float = Field(default=2.0, ge=0.0, le=10000.0)
    initial_capital: float = Field(default=10000.0, gt=0.0)
    background: bool = True
    timeframes: List[str] = Field(default_factory=list)
    strategies: List[str] = Field(default_factory=list)


class AIOneClickResearchDeployRequest(BaseModel):
    goal: str = Field(..., min_length=8, max_length=600)
    market_regime: str = "mixed"
    symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT"])
    timeframes: List[str] = Field(default_factory=lambda: ["15m", "1h"])
    constraints: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    origin_context: Dict[str, Any] = Field(default_factory=dict)
    market_context: Dict[str, Any] = Field(default_factory=dict)
    llm_research_output: Dict[str, Any] = Field(default_factory=dict)
    exchange: str = "binance"
    symbol: Optional[str] = None
    days: int = Field(default=30, ge=1, le=3650)
    commission_rate: float = Field(default=0.0004, ge=0.0, le=1.0)
    slippage_bps: float = Field(default=2.0, ge=0.0, le=10000.0)
    initial_capital: float = Field(default=10000.0, gt=0.0)
    strategies: List[str] = Field(default_factory=list)
    target: str = "auto"  # auto | paper | live_candidate
    allocation_pct: float = Field(default=0.05, ge=0.001, le=1.0)
    strategy_name: str = ""
    approval_notes: str = "oneclick approve"
    skip_deploy: bool = False


class AIOneClickDeployRequest(BaseModel):
    candidate_id: str
    target: str = "auto"  # auto | paper | live_candidate
    allocation_pct: float = Field(default=0.05, ge=0.001, le=1.0)
    strategy_name: str = ""
    approval_notes: str = "oneclick approve"
    skip_deploy: bool = False


class AICandidatePromotionRequest(BaseModel):
    target: Optional[str] = None


class AICandidateRegisterRequest(BaseModel):
    mode: str = "paper"       # paper | live_candidate
    name: Optional[str] = None  # optional custom strategy name (stored in metadata)
    allocation_pct: Optional[float] = Field(default=None, ge=0.001, le=1.0)


class AIHumanApprovalRequest(BaseModel):
    target: Optional[str] = None   # paper | live_candidate
    notes: str = ""


class AIResearchContextRequest(BaseModel):
    market_summary: Dict[str, Any] = Field(default_factory=dict)
    goals: str = ""
    timeout: int = Field(default=180, ge=5, le=1800)


class AIRetireRequest(BaseModel):
    notes: str = ""


PROPOSAL_CANCEL_ONLY_STATUSES = {"research_queued", "research_running"}
PROPOSAL_EXITABLE_STATUSES = {"shadow_running", "live_candidate", "paper_running", "live_running"}
CANDIDATE_EXITABLE_STATUSES = {"shadow_running", "live_candidate", "paper_running", "live_running"}


class AICandidateActivateLiveRequest(BaseModel):
    notes: str = ""


class AIFundingWarmRequest(BaseModel):
    exchange: str = "binance"
    symbol: str = "BTC/USDT"
    days: int = Field(default=60, ge=1, le=3650)
    source: str = "coinglass"


class AILiveDecisionConfigUpdateRequest(BaseModel):
    enabled: Optional[bool] = None
    mode: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    timeout_ms: Optional[int] = Field(default=None, ge=1000, le=60000)
    max_tokens: Optional[int] = Field(default=None, ge=32, le=4096)
    temperature: Optional[float] = Field(default=None, ge=0.0, le=1.5)
    fail_open: Optional[bool] = None
    apply_in_paper: Optional[bool] = None


class AIAutonomousAgentConfigUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile: Optional[str] = None
    enabled: Optional[bool] = None
    mode: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    symbol_mode: Optional[str] = None
    universe_symbols: Optional[Union[List[str], str]] = None
    selection_top_n: Optional[int] = Field(default=None, ge=3, le=20)
    timeframe: Optional[str] = None
    interval_sec: Optional[int] = Field(default=None, ge=15, le=7200)
    lookback_bars: Optional[int] = Field(default=None, ge=30, le=4000)
    min_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    default_leverage: Optional[float] = Field(default=None, ge=1.0, le=1.0)
    max_leverage: Optional[float] = Field(default=None, ge=1.0, le=1.0)
    default_stop_loss_pct: Optional[float] = Field(default=None, ge=0.001, le=0.5)
    default_take_profit_pct: Optional[float] = Field(default=None, ge=0.001, le=2.0)
    timeout_ms: Optional[int] = Field(default=None, ge=1000, le=120000)
    max_tokens: Optional[int] = Field(default=None, ge=32, le=4096)
    temperature: Optional[float] = Field(default=None, ge=0.0, le=1.5)
    cooldown_sec: Optional[int] = Field(default=None, ge=0, le=86400)
    max_total_exposure_ratio: Optional[float] = Field(default=None, ge=0.05, le=0.4)
    max_total_exposure_usdt: Optional[float] = Field(default=None, gt=0)
    allow_live: Optional[bool] = None
    runtime_profile: Optional[str] = None
    account_id: Optional[str] = None
    strategy_name: Optional[str] = None


class AIAutonomousAgentRiskConfigUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    autonomy_daily_stop_buffer_ratio: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    autonomy_max_drawdown_reduce_only: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    autonomy_rolling_3d_drawdown_reduce_only: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    autonomy_rolling_7d_drawdown_reduce_only: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class AIAutonomousAgentStartRequest(BaseModel):
    enable: bool = True
    profile: Optional[str] = None
    runtime_profile: Optional[str] = None


class AIAutonomousAgentRunOnceRequest(BaseModel):
    force: bool = False


def _proposal_job_summary(app: Request | Any, proposal_id: str, preferred_job_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    state_owner = getattr(app, "app", app)
    jobs = dict(getattr(getattr(state_owner, "state", None), "research_jobs", {}) or {})
    job_id = str(preferred_job_id or "").strip()
    chosen: Optional[Dict[str, Any]] = None
    if job_id:
        raw = jobs.get(job_id)
        if isinstance(raw, dict):
            chosen = dict(raw)
    if chosen is None:
        matches = [
            dict(job)
            for job in jobs.values()
            if str((job or {}).get("proposal_id") or "") == str(proposal_id)
        ]
        if matches:
            matches.sort(key=lambda job: str(job.get("started_at") or job.get("created_at") or ""))
            chosen = matches[-1]
    if not chosen:
        return None
    return {
        "job_id": chosen.get("job_id"),
        "status": chosen.get("status"),
        "created_at": chosen.get("created_at"),
        "started_at": chosen.get("started_at"),
        "finished_at": chosen.get("finished_at"),
        "error": chosen.get("error"),
        "progress": chosen.get("progress"),
    }


def _serialize_proposal(app: Request | Any, proposal: Any) -> Dict[str, Any]:
    data = proposal.model_dump(mode="json")
    data["job"] = _proposal_job_summary(
        app,
        proposal_id=str(proposal.proposal_id),
        preferred_job_id=str((proposal.metadata or {}).get("last_research_job_id") or ""),
    )
    return data


def _normalize_exchange(value: str) -> str:
    text = str(value or "binance").strip().lower()
    return text or "binance"


def _normalize_symbol(value: str) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return "BTC/USDT"
    if "/" in raw:
        return raw
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}/USDT"
    return raw


def _normalize_deploy_target(value: str) -> str:
    text = str(value or "auto").strip().lower()
    if text not in {"auto", "paper", "live_candidate"}:
        return "auto"
    return text


def _extract_first_validation_reason(item: Any) -> Optional[str]:
    summary = getattr(item, "validation_summary", None)
    reasons = list(getattr(summary, "reasons", []) or [])
    if reasons:
        return str(reasons[0] or "").strip() or None
    metadata = getattr(item, "metadata", None) or {}
    if isinstance(metadata, dict):
        return str(metadata.get("last_research_error") or "").strip() or None
    return None


def _serialize_research_job_result(raw_result: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_result, dict):
        return None
    proposal = raw_result.get("proposal")
    candidate = raw_result.get("candidate")
    promotion = raw_result.get("promotion")
    payload: Dict[str, Any] = {
        "proposal": proposal.model_dump(mode="json") if hasattr(proposal, "model_dump") else proposal,
        "candidate": candidate.model_dump(mode="json") if hasattr(candidate, "model_dump") else candidate,
        "promotion": promotion.model_dump(mode="json") if hasattr(promotion, "model_dump") else promotion,
        "proposal_reason": None,
    }
    if payload["candidate"] is None:
        payload["proposal_reason"] = _extract_first_validation_reason(proposal)
    return payload


def _resolve_oneclick_target(raw_target: str, promotion_decision: str) -> Optional[str]:
    resolved_target: Optional[str] = raw_target
    normalized_decision = str(promotion_decision or "").strip().lower()
    if raw_target == "auto":
        if normalized_decision == "shadow":
            normalized_decision = "paper"
        resolved_target = (
            normalized_decision if normalized_decision in {"paper", "live_candidate"} else None
        )
    return resolved_target


def _oneclick_generated_payload(request: Request, proposal: Any, generated: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "proposal": _serialize_proposal(request, proposal),
        "planner_notes": generated.get("planner_notes", []),
        "filtered_templates": generated.get("filtered_templates", []),
    }


async def _execute_oneclick_candidate_deploy(
    request: Request,
    *,
    payload: AIOneClickDeployRequest,
) -> Dict[str, Any]:
    ensure_ai_research_runtime_state(request.app)
    candidate = get_candidate(request.app, payload.candidate_id)
    governance_enabled = bool(getattr(settings, "GOVERNANCE_ENABLED", True))
    raw_target = _normalize_deploy_target(payload.target)
    promotion_decision = str(
        getattr(candidate.promotion, "decision", "")
        or (getattr(candidate, "metadata", {}) or {}).get("recommended_runtime_target")
        or ""
    ).strip().lower()
    resolved_target = _resolve_oneclick_target(raw_target, promotion_decision)
    current_mode = str(execution_engine.get_trading_mode() or "").strip().lower() or "paper"
    if current_mode not in {"paper", "live"}:
        current_mode = str(getattr(settings, "TRADING_MODE", "paper") or "paper").strip().lower() or "paper"
    auto_mode_conflict_detail: Optional[str] = None
    if raw_target == "auto" and resolved_target == "paper" and current_mode != "paper":
        auto_mode_conflict_detail = _register_mode_conflict_detail(current_mode)

    if payload.skip_deploy or resolved_target is None or auto_mode_conflict_detail:
        deploy_payload: Dict[str, Any] = {
            "performed": False,
            "action": None,
            "result": None,
            "runtime_status": None,
        }
        outcome = "completed_no_deploy" if payload.skip_deploy else "completed_without_deployable_candidate"
        manual_action_required = False
        manual_target_options: List[str] = []
        if auto_mode_conflict_detail:
            deploy_payload["reasons"] = [auto_mode_conflict_detail]
            deploy_payload["blockers"] = [
                {
                    "code": "paper_target_conflicts_with_runtime_mode",
                    "detail": auto_mode_conflict_detail,
                    "resolved_target": resolved_target,
                    "current_trading_mode": current_mode,
                }
            ]
            outcome = "completed_without_compatible_runtime_target"
            manual_action_required = True
            if current_mode == "live":
                manual_target_options = ["live_candidate"]
        return {
            "candidate_id": payload.candidate_id,
            "governance_enabled": governance_enabled,
            "target": resolved_target,
            "deploy": deploy_payload,
            "outcome": outcome,
            "runtime_status": None,
            "registered_strategy_name": None,
            "current_trading_mode": current_mode,
            "manual_action_required": manual_action_required,
            "manual_target_options": manual_target_options,
            "candidate": candidate.model_dump(mode="json"),
        }

    deploy_action: Optional[str] = None
    deploy_result: Optional[Dict[str, Any]] = None
    if governance_enabled:
        if resolved_target == "paper":
            deploy_action = "quick_register"
            deploy_result = await quick_register_candidate(
                request,
                payload.candidate_id,
                AIQuickRegisterRequest(allocation_pct=float(payload.allocation_pct)),
            )
        else:
            deploy_action = "human_approve"
            deploy_result = await human_approve_candidate(
                request,
                payload.candidate_id,
                AIHumanApprovalRequest(target="live_candidate", notes=payload.approval_notes),
            )
    else:
        deploy_action = "register"
        deploy_result = await register_ai_candidate(
            request,
            payload.candidate_id,
            AICandidateRegisterRequest(mode=resolved_target, name=(payload.strategy_name or None)),
        )

    return {
        "candidate_id": payload.candidate_id,
        "governance_enabled": governance_enabled,
        "target": resolved_target,
        "deploy": {
            "performed": True,
            "action": deploy_action,
            "result": deploy_result,
            "runtime_status": (deploy_result or {}).get("runtime_status"),
        },
        "outcome": f"deployed_{resolved_target}",
        "runtime_status": (deploy_result or {}).get("runtime_status"),
        "registered_strategy_name": (deploy_result or {}).get("registered_strategy_name"),
        "candidate": candidate.model_dump(mode="json"),
    }


def _register_mode_conflict_detail(current_mode: str) -> str:
    mode = str(current_mode or "unknown").strip().lower() or "unknown"
    return (
        f"当前系统处于 {mode} 模式，不能直接注册为纸盘。"
        "请先切换到 paper 模式，或改选“实盘候选（live_candidate）”。"
    )


def _news_key(symbol: str) -> str:
    raw = _normalize_symbol(symbol).split(":")[0]
    if "/" in raw:
        return raw.split("/", 1)[0]
    return raw.replace("USDT", "") or raw


def _candidate_primary_symbol(candidate: Any, default: str = "BTC/USDT") -> str:
    symbol_value = getattr(candidate, "symbol", None)
    if isinstance(symbol_value, str) and symbol_value.strip():
        return _normalize_symbol(symbol_value)
    symbols_value = getattr(candidate, "symbols", None)
    if isinstance(symbols_value, (list, tuple)) and symbols_value:
        first = symbols_value[0]
        if isinstance(first, str) and first.strip():
            return _normalize_symbol(first)
    return _normalize_symbol(default)


def _candidate_strategy_name(candidate: Any, default: str = "unknown") -> str:
    strategy_value = getattr(candidate, "strategy", None)
    if isinstance(strategy_value, str) and strategy_value.strip():
        return strategy_value.strip()
    strategy_name = getattr(candidate, "strategy_name", None)
    if isinstance(strategy_name, str) and strategy_name.strip():
        return strategy_name.strip()
    return default


def _candidate_exchange(candidate: Any, default: str = "binance") -> str:
    """Extract exchange from candidate, falling back to default."""
    exchange = getattr(candidate, "exchange", None)
    if isinstance(exchange, str) and exchange.strip():
        return _normalize_exchange(exchange)
    meta = getattr(candidate, "metadata", None) or {}
    meta_exchange = meta.get("exchange") if isinstance(meta, dict) else None
    if isinstance(meta_exchange, str) and meta_exchange.strip():
        return _normalize_exchange(meta_exchange)
    return default


def _candidate_registered_strategy_name(candidate: Any) -> Optional[str]:
    meta = getattr(candidate, "metadata", None) or {}
    runtime_meta = meta.get("promotion_runtime") if isinstance(meta, dict) else None
    if not isinstance(runtime_meta, dict):
        runtime_meta = {}
    for value in (
        meta.get("registered_strategy_name") if isinstance(meta, dict) else None,
        runtime_meta.get("registered_strategy_name"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return None


def _ensure_candidate_metadata(candidate: Any) -> Dict[str, Any]:
    meta = getattr(candidate, "metadata", None)
    if not isinstance(meta, dict):
        meta = {}
        candidate.metadata = meta
    return meta


def _ensure_proposal_metadata(proposal: Any) -> Dict[str, Any]:
    meta = getattr(proposal, "metadata", None)
    if not isinstance(meta, dict):
        meta = {}
        proposal.metadata = meta
    return meta


async def _exit_candidate_runtime_strategy(candidate: Any) -> Dict[str, Any]:
    from core.strategies import strategy_manager  # noqa: PLC0415
    from core.strategies.persistence import delete_strategy_snapshot  # noqa: PLC0415

    strategy_name = _candidate_registered_strategy_name(candidate)
    result = {
        "strategy_name": strategy_name,
        "stopped": False,
        "unregistered": False,
        "snapshot_deleted": False,
    }
    if not strategy_name:
        return result

    strategy_exists = (
        strategy_manager.get_strategy(strategy_name) is not None
        or strategy_manager.get_strategy_info(strategy_name) is not None
    )
    if strategy_exists:
        stopped = await strategy_manager.stop_strategy(strategy_name)
        if not stopped:
            raise RuntimeError(f"failed to stop runtime strategy: {strategy_name}")
        result["stopped"] = True

        unregistered = strategy_manager.unregister_strategy(strategy_name)
        if not unregistered:
            raise RuntimeError(f"failed to unregister runtime strategy: {strategy_name}")
        result["unregistered"] = True

    result["snapshot_deleted"] = await delete_strategy_snapshot(strategy_name)
    return result


async def _exit_candidate_record(
    app: Any,
    candidate: Any,
    *,
    notes: str,
    actor: str,
) -> Dict[str, Any]:
    status = str(getattr(candidate, "status", "") or "")
    if status not in CANDIDATE_EXITABLE_STATUSES and status != "retired":
        raise HTTPException(status_code=409, detail=f"candidate in state {status}, exit is not allowed")

    runtime_result = await _exit_candidate_runtime_strategy(candidate)
    now_iso = datetime.now(timezone.utc).isoformat()
    metadata = _ensure_candidate_metadata(candidate)
    runtime_meta = metadata.get("promotion_runtime")
    if not isinstance(runtime_meta, dict):
        runtime_meta = {}
    runtime_meta["started"] = False
    runtime_meta["exit_requested_at"] = now_iso
    runtime_meta["exit_source"] = actor
    runtime_meta["exit_notes"] = notes
    runtime_meta["stopped"] = bool(runtime_result.get("stopped"))
    runtime_meta["unregistered"] = bool(runtime_result.get("unregistered"))
    runtime_meta["snapshot_deleted"] = bool(runtime_result.get("snapshot_deleted"))
    if runtime_result.get("strategy_name"):
        runtime_meta["registered_strategy_name"] = runtime_result["strategy_name"]
    metadata["promotion_runtime"] = runtime_meta
    metadata["retired_manually_at"] = now_iso
    metadata["retired_manually_reason"] = notes
    if runtime_result.get("strategy_name"):
        metadata["retired_strategy_name"] = runtime_result["strategy_name"]
    candidate.promotion_target = None

    if status != "retired":
        transition_candidate(
            candidate,
            to_state="retired",
            lifecycle_registry=app.state.ai_lifecycle_registry,
            actor=actor,
            reason=notes or "exited manually from AI research queue",
            metadata={
                "source": "ai_research_queue",
                "strategy_name": runtime_result.get("strategy_name"),
                "strategy_stopped": bool(runtime_result.get("stopped")),
                "strategy_unregistered": bool(runtime_result.get("unregistered")),
            },
        )

    app.state.ai_candidate_registry.save(candidate)
    return {
        "candidate_id": str(getattr(candidate, "candidate_id", "") or ""),
        "status": "retired",
        "previous_status": status,
        "strategy_name": runtime_result.get("strategy_name"),
        "strategy_stopped": bool(runtime_result.get("stopped")),
        "strategy_unregistered": bool(runtime_result.get("unregistered")),
        "snapshot_deleted": bool(runtime_result.get("snapshot_deleted")),
    }


def _candidate_timeframe(candidate: Any, default: str = "1h") -> str:
    timeframe_value = getattr(candidate, "timeframe", None)
    if isinstance(timeframe_value, str) and timeframe_value.strip():
        return timeframe_value.strip()
    timeframes_value = getattr(candidate, "timeframes", None)
    if isinstance(timeframes_value, (list, tuple)) and timeframes_value:
        for item in timeframes_value:
            if isinstance(item, str) and item.strip():
                return item.strip()
    meta = getattr(candidate, "metadata", None) or {}
    meta_timeframe = meta.get("timeframe") if isinstance(meta, dict) else None
    if isinstance(meta_timeframe, str) and meta_timeframe.strip():
        return meta_timeframe.strip()
    meta_timeframes = meta.get("timeframes") if isinstance(meta, dict) else None
    if isinstance(meta_timeframes, (list, tuple)) and meta_timeframes:
        for item in meta_timeframes:
            if isinstance(item, str) and item.strip():
                return item.strip()
    return str(default or "1h").strip() or "1h"


def _params_fingerprint(params: Any) -> str:
    try:
        payload = json.dumps(
            dict(params or {}),
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
            default=str,
        )
    except Exception:
        payload = str(params or "")
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def _candidate_id_suffix(candidate_id: Any, size: int = 6) -> str:
    text = str(candidate_id or "").strip()
    if not text:
        return ""
    return text[-max(1, int(size or 6)) :]


def _live_signal_status_rank(status: str) -> int:
    return {
        "live_running": 40,
        "live_candidate": 30,
        "paper_running": 20,
        "shadow_running": 10,
    }.get(str(status or "").strip().lower(), 0)


def _candidate_created_sort_value(candidate: Any) -> float:
    value = getattr(candidate, "created_at", None)
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return float(dt.timestamp())
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize(timezone.utc)
        else:
            ts = ts.tz_convert(timezone.utc)
        return float(ts.timestamp())
    except Exception:
        return 0.0


def _normalize_signal_market_timestamp(value: Any) -> Optional[pd.Timestamp]:
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize(SIGNAL_MARKET_DATA_TIMEZONE)
    return ts


def _candidate_live_signal_group_key(candidate: Any) -> tuple[str, str, str, str]:
    return (
        _candidate_primary_symbol(candidate),
        _candidate_timeframe(candidate),
        _candidate_strategy_name(candidate),
        _params_fingerprint(getattr(candidate, "params", {}) or {}),
    )


def _pick_preferred_candidate(candidates: List[Any]) -> Any:
    ranked = sorted(
        list(candidates or []),
        key=lambda cand: (
            _live_signal_status_rank(str(getattr(cand, "status", "") or "")),
            _candidate_created_sort_value(cand),
            str(getattr(cand, "candidate_id", "") or ""),
        ),
        reverse=True,
    )
    return ranked[0] if ranked else None


def _group_candidates_for_live_signals(candidates: List[Any]) -> List[Dict[str, Any]]:
    groups: Dict[tuple[str, str, str, str], List[Any]] = {}
    order: List[tuple[str, str, str, str]] = []
    for cand in candidates or []:
        key = _candidate_live_signal_group_key(cand)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(cand)
    return [
        {
            "key": key,
            "candidates": groups[key],
            "preferred": _pick_preferred_candidate(groups[key]),
        }
        for key in order
        if groups.get(key)
    ]


def _build_live_signal_candidate_item(
    *,
    preferred: Any,
    grouped_candidates: List[Any],
    signal_payload: Optional[Dict[str, Any]],
    error: str = "",
) -> Dict[str, Any]:
    preferred_candidate = preferred or _pick_preferred_candidate(grouped_candidates)
    candidate_ids = [
        str(getattr(cand, "candidate_id", "") or "").strip()
        for cand in grouped_candidates or []
        if str(getattr(cand, "candidate_id", "") or "").strip()
    ]
    suffixes = [_candidate_id_suffix(candidate_id) for candidate_id in candidate_ids if candidate_id]
    statuses: List[str] = []
    for cand in grouped_candidates or []:
        status = str(getattr(cand, "status", "") or "").strip()
        if status and status not in statuses:
            statuses.append(status)

    return {
        "source": "candidate",
        "group_type": "candidate",
        "candidate_id": str(getattr(preferred_candidate, "candidate_id", "") or "").strip(),
        "candidate_id_suffix": _candidate_id_suffix(getattr(preferred_candidate, "candidate_id", "")),
        "candidate_ids": candidate_ids,
        "candidate_suffixes": suffixes,
        "duplicate_count": len(grouped_candidates or []),
        "strategy": _candidate_strategy_name(preferred_candidate),
        "symbol": _candidate_primary_symbol(preferred_candidate),
        "timeframe": _candidate_timeframe(preferred_candidate),
        "status": str(getattr(preferred_candidate, "status", "unknown") or "unknown"),
        "statuses": statuses,
        "params_fingerprint": _params_fingerprint(getattr(preferred_candidate, "params", {}) or {}),
        "signal": signal_payload,
        "error": str(error or "").strip(),
    }


def _build_live_signal_watchlist_symbols(
    *,
    runtime_cfg: Dict[str, Any],
    selection: Dict[str, Any],
) -> List[str]:
    symbols: List[str] = []
    selected_symbol = _normalize_symbol(str(selection.get("selected_symbol") or runtime_cfg.get("symbol") or ""))
    if selected_symbol:
        symbols.append(selected_symbol)

    top_candidates = list(selection.get("top_candidates") or [])
    for row in top_candidates:
        symbol = _normalize_symbol(str((row or {}).get("symbol") or ""))
        if symbol and symbol not in symbols:
            symbols.append(symbol)

    configured_symbols = list((selection.get("scan_config") or {}).get("universe_symbols") or runtime_cfg.get("universe_symbols") or [])
    for raw in configured_symbols:
        symbol = _normalize_symbol(str(raw or ""))
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _build_live_signal_watchlist_item(
    *,
    symbol: str,
    runtime_cfg: Dict[str, Any],
    selection: Dict[str, Any],
    signal_payload: Optional[Dict[str, Any]],
    error: str = "",
) -> Dict[str, Any]:
    top_candidates = list(selection.get("top_candidates") or [])
    matched_row = next(
        (row for row in top_candidates if _normalize_symbol(str((row or {}).get("symbol") or "")) == _normalize_symbol(symbol)),
        {},
    )
    selected_symbol = _normalize_symbol(str(selection.get("selected_symbol") or runtime_cfg.get("symbol") or ""))
    is_selected = bool(selected_symbol and _normalize_symbol(symbol) == selected_symbol)
    watch_status = "selected" if is_selected else "watchlist"
    watch_status_display = "当前关注" if is_selected else (
        f"候选 #{int((matched_row or {}).get('rank') or 0)}" if int((matched_row or {}).get("rank") or 0) > 0 else "观察列表"
    )

    return {
        "source": "agent_watchlist",
        "group_type": "watchlist",
        "strategy": str(runtime_cfg.get("strategy_name") or "AI_AutonomousAgent"),
        "symbol": _normalize_symbol(symbol),
        "timeframe": str((selection.get("scan_config") or {}).get("timeframe") or runtime_cfg.get("timeframe") or "15m").strip() or "15m",
        "status": watch_status,
        "status_display": watch_status_display,
        "selected": is_selected,
        "rank": int((matched_row or {}).get("rank") or 0),
        "params_fingerprint": "",
        "signal": signal_payload,
        "error": str(error or "").strip(),
    }


async def _load_live_signal_snapshot(
    *,
    symbol: str,
    exchange: str,
    timeframe: str,
    signal_aggregator: Any,
    limit: int = 120,
    timeout_sec: float = 10.0,
    log_label: str,
) -> tuple[Optional[Dict[str, Any]], str]:
    async def _work() -> Dict[str, Any]:
        df, market_meta = await _load_signal_market_data(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
        )
        sig = await signal_aggregator.aggregate(
            symbol,
            df,
            include_llm=False,
        )
        signal_payload = sig.to_dict() if callable(getattr(sig, "to_dict", None)) else dict(sig or {})
        return {
            **signal_payload,
            **market_meta,
            "aggregated_at": signal_payload.get("timestamp"),
        }

    try:
        payload = await asyncio.wait_for(_work(), timeout=timeout_sec)
        return payload, ""
    except Exception as exc:
        logger.debug(f"live-signals: {log_label}: {exc}")
        return None, str(exc)


def _live_signal_ml_model_loaded(signal_aggregator: Any) -> bool:
    try:
        ml_model = getattr(signal_aggregator, "_ml_model", None)
        if ml_model is not None and callable(getattr(ml_model, "is_loaded", None)):
            return bool(ml_model.is_loaded())
    except Exception:
        return False
    return False


async def _load_autonomous_watchlist_runtime() -> tuple[Dict[str, Any], Dict[str, Any]]:
    runtime_cfg: Dict[str, Any] = {}
    selection: Dict[str, Any] = {}
    try:
        runtime_cfg = dict(autonomous_trading_agent.get_runtime_config() or {})
    except Exception as exc:
        logger.debug(f"live-signals: runtime config unavailable: {exc}")
        runtime_cfg = {}

    try:
        selection_limit = max(3, min(int(runtime_cfg.get("selection_top_n") or 8), 20))
    except Exception:
        selection_limit = 8

    def _coerce_selection(payload: Any) -> Dict[str, Any]:
        return dict(payload or {}) if isinstance(payload, dict) else {}

    def _selection_from_status(payload: Any) -> Dict[str, Any]:
        status_payload = _coerce_selection(payload)
        for key in ("preview_symbol_scan", "last_symbol_scan"):
            selection_payload = _coerce_selection(status_payload.get(key))
            if selection_payload:
                return selection_payload
        return {}

    snapshot_method = getattr(autonomous_trading_agent, "get_symbol_scan_preview_snapshot", None)
    if callable(snapshot_method):
        try:
            selection = _coerce_selection(snapshot_method(limit=selection_limit))
        except Exception as exc:
            logger.debug(f"live-signals: get_symbol_scan_preview_snapshot unavailable: {exc}")
            selection = {}
        if selection:
            return runtime_cfg, selection

    status_method = getattr(autonomous_trading_agent, "get_status", None)
    if callable(status_method):
        try:
            selection = _selection_from_status(status_method())
        except Exception as exc:
            logger.debug(f"live-signals: get_status unavailable: {exc}")
            selection = {}
        if selection:
            return runtime_cfg, selection

    async def _load_selection(method_name: str, *, timeout_sec: float) -> Dict[str, Any]:
        method = getattr(autonomous_trading_agent, method_name, None)
        if not callable(method):
            return {}
        payload = await asyncio.wait_for(
            asyncio.shield(method(limit=selection_limit, force=False)),
            timeout=timeout_sec,
        )
        return _coerce_selection(payload)

    preview_method = getattr(autonomous_trading_agent, "get_symbol_scan_preview", None)
    if callable(preview_method):
        try:
            selection = await _load_selection("get_symbol_scan_preview", timeout_sec=4.0)
        except Exception as exc:
            logger.debug(f"live-signals: get_symbol_scan_preview unavailable: {exc}")
            selection = {}
        if selection:
            return runtime_cfg, selection

    full_scan_method = getattr(autonomous_trading_agent, "get_symbol_scan", None)
    if callable(full_scan_method):
        try:
            selection = await _load_selection("get_symbol_scan", timeout_sec=4.0)
        except Exception as exc:
            logger.debug(f"live-signals: get_symbol_scan unavailable: {exc}")
            selection = {}
    return runtime_cfg, selection


async def _build_candidate_live_signals_payload(
    request: Request,
    *,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    from core.ai.signal_aggregator import signal_aggregator

    all_candidates = list_candidates(request.app, limit=200)
    active = [
        c for c in all_candidates
        if str(c.status) in {"paper_running", "shadow_running", "live_running", "live_candidate"}
    ]
    if symbol:
        sym_norm = _normalize_symbol(symbol)
        active = [c for c in active if _candidate_primary_symbol(c) == sym_norm]

    candidate_specs: List[Dict[str, Any]] = []
    candidate_tasks: List[Any] = []
    for grouped in _group_candidates_for_live_signals(active):
        grouped_candidates = list(grouped.get("candidates") or [])
        preferred = grouped.get("preferred") or _pick_preferred_candidate(grouped_candidates)
        if preferred is None:
            continue
        cand_symbol = _candidate_primary_symbol(preferred)
        cand_timeframe = _candidate_timeframe(preferred)
        cand_exchange = _candidate_exchange(preferred)
        candidate_id = str(getattr(preferred, "candidate_id", "") or "unknown")
        candidate_specs.append(
            {
                "preferred": preferred,
                "grouped_candidates": grouped_candidates,
            }
        )
        candidate_tasks.append(
            _load_live_signal_snapshot(
                symbol=cand_symbol,
                exchange=cand_exchange,
                timeframe=cand_timeframe,
                signal_aggregator=signal_aggregator,
                limit=120,
                timeout_sec=10.0,
                log_label=f"candidate error for {candidate_id}",
            )
        )

    candidate_items: List[Dict[str, Any]] = []
    candidate_results = await asyncio.gather(*candidate_tasks) if candidate_tasks else []
    for spec, result in zip(candidate_specs, candidate_results):
        signal_payload, error = result
        candidate_items.append(
            _build_live_signal_candidate_item(
                preferred=spec["preferred"],
                grouped_candidates=spec["grouped_candidates"],
                signal_payload=signal_payload,
                error=error,
            )
        )

    return {
        "sections": [
            {
                "id": "candidates",
                "title": "运行中候选",
                "count": len(candidate_items),
                "empty_text": "暂无运行中候选",
                "items": candidate_items,
            }
        ],
        "candidate_items": candidate_items,
        "watchlist_items": [],
        "items": candidate_items,
        "candidate_count": len(candidate_items),
        "watchlist_count": 0,
        "count": len(candidate_items),
        "ml_model_loaded": _live_signal_ml_model_loaded(signal_aggregator),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


async def _build_autonomous_watchlist_live_signals_payload(
    *,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    from core.ai.signal_aggregator import signal_aggregator

    runtime_cfg, selection = await _load_autonomous_watchlist_runtime()
    watchlist_symbols = _build_live_signal_watchlist_symbols(
        runtime_cfg=runtime_cfg,
        selection=selection,
    )
    if symbol:
        sym_norm = _normalize_symbol(symbol)
        watchlist_symbols = [sym for sym in watchlist_symbols if _normalize_symbol(sym) == sym_norm]
    else:
        watchlist_limit = max(
            1,
            min(
                int((selection.get("top_n") or runtime_cfg.get("selection_top_n") or 8)),
                8,
            ),
        )
        watchlist_symbols = watchlist_symbols[:watchlist_limit]

    watchlist_timeframe = str(
        (selection.get("scan_config") or {}).get("timeframe")
        or runtime_cfg.get("timeframe")
        or "15m"
    ).strip() or "15m"
    watchlist_exchange = _normalize_exchange(
        str(
            (selection.get("scan_config") or {}).get("exchange")
            or runtime_cfg.get("exchange")
            or "binance"
        )
    )
    watchlist_tasks = [
        _load_live_signal_snapshot(
            symbol=watch_symbol,
            exchange=watchlist_exchange,
            timeframe=watchlist_timeframe,
            signal_aggregator=signal_aggregator,
            limit=120,
            timeout_sec=10.0,
            log_label=f"watchlist error for {watch_symbol}",
        )
        for watch_symbol in watchlist_symbols
    ]
    watchlist_results = await asyncio.gather(*watchlist_tasks) if watchlist_tasks else []

    watchlist_items: List[Dict[str, Any]] = []
    for watch_symbol, result in zip(watchlist_symbols, watchlist_results):
        signal_payload, error = result
        watchlist_items.append(
            _build_live_signal_watchlist_item(
                symbol=watch_symbol,
                runtime_cfg=runtime_cfg,
                selection=selection,
                signal_payload=signal_payload,
                error=error,
            )
        )

    return {
        "sections": [
            {
                "id": "watchlist",
                "title": "自治代理 watchlist",
                "count": len(watchlist_items),
                "empty_text": "暂无自治代理 watchlist",
                "items": watchlist_items,
            }
        ],
        "candidate_items": [],
        "watchlist_items": watchlist_items,
        "items": watchlist_items,
        "candidate_count": 0,
        "watchlist_count": len(watchlist_items),
        "count": len(watchlist_items),
        "ml_model_loaded": _live_signal_ml_model_loaded(signal_aggregator),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def _safe_strategy_name_fragment(value: str, default: str = "strategy") -> str:
    text = "".join(
        ch if (str(ch).isascii() and str(ch).isalnum()) else "_"
        for ch in str(value or "").strip()
    )
    while "__" in text:
        text = text.replace("__", "_")
    text = text.strip("_")
    return text or default


def _build_candidate_strategy_name(candidate: Any, target_mode: str = "paper") -> str:
    meta = getattr(candidate, "metadata", None) or {}
    custom_name = meta.get("display_name") if isinstance(meta, dict) else None
    symbol = _candidate_primary_symbol(candidate).replace("/", "")
    timeframe = str(getattr(candidate, "timeframe", "") or "1h").strip() or "1h"
    mode_fragment = "live" if str(target_mode or "").strip().lower() == "live" else "paper"
    hint = (
        str(custom_name).strip()
        if isinstance(custom_name, str) and custom_name.strip()
        else f"{_candidate_strategy_name(candidate)}_{symbol}_{timeframe}"
    )
    safe_hint = _safe_strategy_name_fragment(hint, default="ai_strategy")
    candidate_suffix = _safe_strategy_name_fragment(str(getattr(candidate, "candidate_id", "") or "")[:6], default="cand")
    return f"{safe_hint}_{mode_fragment}_{int(datetime.now(timezone.utc).timestamp())}_{candidate_suffix}"


async def _ensure_candidate_runtime_strategy(
    app: Any,
    candidate: Any,
    *,
    target_mode: str = "live",
) -> Dict[str, Any]:
    from config.strategy_registry import get_strategy_defaults  # noqa: PLC0415
    from core.deployment.promotion_engine import (  # noqa: PLC0415
        _resolve_observed_trades_per_day,
        _resolve_strategy_class,
    )
    from core.strategies import strategy_manager  # noqa: PLC0415
    from core.strategies.persistence import persist_strategy_snapshot  # noqa: PLC0415
    from core.strategies.runtime_policy import build_runtime_limit_policy  # noqa: PLC0415

    resolved_mode = str(target_mode or "live").strip().lower() or "live"
    if resolved_mode not in {"live", "paper"}:
        raise RuntimeError(f"unsupported runtime mode: {resolved_mode}")

    runtime_fingerprint = ai_research_runtime_fingerprint_for_candidate(
        candidate,
        target_mode=resolved_mode,
    )
    strategy_name = _candidate_registered_strategy_name(candidate)
    if not strategy_name and runtime_fingerprint:
        for item in sorted(strategy_manager.list_strategies(), key=lambda row: str(row.get("name") or "")):
            metadata = dict(item.get("metadata") or {}) if isinstance(item.get("metadata"), dict) else {}
            inferred_fingerprint = (
                str(metadata.get("runtime_fingerprint") or "").strip()
                or ai_research_runtime_fingerprint_from_strategy_info(item)
            )
            if inferred_fingerprint == runtime_fingerprint:
                strategy_name = str(item.get("name") or "").strip()
    strategy_name = strategy_name or _build_candidate_strategy_name(candidate, resolved_mode)
    strategy_class = _resolve_strategy_class(_candidate_strategy_name(candidate))
    if strategy_class is None:
        raise RuntimeError(f"unknown strategy class for candidate: {_candidate_strategy_name(candidate)}")

    params = dict(get_strategy_defaults(_candidate_strategy_name(candidate)))
    params.update(dict(getattr(candidate, "params", {}) or {}))
    params.setdefault("exchange", _candidate_exchange(candidate))
    params["runtime_mode"] = resolved_mode
    params.setdefault("account_id", f"ai_{_safe_strategy_name_fragment(strategy_name.lower(), default='strategy')}")

    promotion = getattr(candidate, "promotion", None)
    constraints = dict(getattr(promotion, "constraints", {}) or {})
    default_allocation = max(
        0.0,
        min(float(getattr(settings, "DEFAULT_STRATEGY_ALLOCATION", 0.15) or 0.15), 1.0),
    )
    allocation = constraints.get("allocation_cap")
    if allocation is None:
        allocation = (getattr(candidate, "metadata", {}) or {}).get("allocation_pct")
    allocation = max(0.0, min(float(allocation or default_allocation), 1.0))

    runtime_override = constraints.get("runtime_limit_minutes")
    if runtime_override is None:
        runtime_override = ((getattr(candidate, "metadata", {}) or {}).get("promotion_runtime") or {}).get("runtime_limit_minutes")
    runtime_limit_minutes: Optional[int]
    runtime_policy: Dict[str, Any]
    if runtime_override is not None:
        runtime_limit_minutes = max(0, int(float(runtime_override))) or None
        runtime_policy = {
            "runtime_limit_minutes": runtime_limit_minutes,
            "source": "promotion_constraint",
        }
    else:
        observed_tpd = _resolve_observed_trades_per_day(app, candidate)
        runtime_policy = build_runtime_limit_policy(
            timeframe=str(getattr(candidate, "timeframe", "") or "1h"),
            params=params,
            observed_trades_per_day=observed_tpd,
        )
        runtime_limit_minutes = int(runtime_policy["runtime_limit_minutes"])

    strategy = strategy_manager.get_strategy(strategy_name)
    strategy_metadata = build_ai_research_strategy_metadata(
        candidate,
        strategy_name=strategy_name,
        target_mode=resolved_mode,
    )
    if strategy is None:
        ok = strategy_manager.register_strategy(
            name=strategy_name,
            strategy_class=strategy_class,
            params=params,
            symbols=[_candidate_primary_symbol(candidate)],
            timeframe=str(getattr(candidate, "timeframe", "") or "1h"),
            allocation=allocation,
            runtime_limit_minutes=runtime_limit_minutes,
            metadata=strategy_metadata,
        )
        if not ok:
            raise RuntimeError("strategy registration failed during live activation")
    else:
        strategy_manager.update_strategy_params(strategy_name, params)
        strategy_manager.update_strategy_allocation(strategy_name, allocation)
        strategy_manager.update_strategy_metadata(strategy_name, strategy_metadata)
        strategy_manager.update_strategy_runtime_config(
            strategy_name,
            timeframe=str(getattr(candidate, "timeframe", "") or "1h"),
            symbols=[_candidate_primary_symbol(candidate)],
            runtime_limit_minutes=runtime_limit_minutes,
        )

    started = await strategy_manager.start_strategy(strategy_name)
    if not started:
        raise RuntimeError("strategy start failed during live activation")
    stopped_duplicates: list[str] = []
    if runtime_fingerprint:
        for item in list(strategy_manager.list_strategies()):
            duplicate_name = str(item.get("name") or "").strip()
            if not duplicate_name or duplicate_name == strategy_name:
                continue
            metadata = dict(item.get("metadata") or {}) if isinstance(item.get("metadata"), dict) else {}
            if metadata.get("runtime_fingerprint") != runtime_fingerprint:
                continue
            strategy = strategy_manager.get_strategy(duplicate_name)
            if strategy is not None and getattr(strategy, "is_running", False):
                await strategy_manager.stop_strategy(duplicate_name)
            await persist_strategy_snapshot(duplicate_name, state_override="stopped")
            stopped_duplicates.append(duplicate_name)
    await persist_strategy_snapshot(strategy_name, state_override="running")

    meta = getattr(candidate, "metadata", None)
    if not isinstance(meta, dict):
        meta = {}
        candidate.metadata = meta
    meta["registered_strategy_name"] = strategy_name
    meta["promotion_runtime"] = {
        "mode": resolved_mode,
        "registered_strategy_name": strategy_name,
        "started": True,
        "runtime_limit_minutes": runtime_limit_minutes,
        "runtime_policy": runtime_policy,
        "dedupe_stopped_strategy_names": stopped_duplicates,
        "promoted_at": datetime.now(timezone.utc).isoformat(),
    }
    return {
        "registered_strategy_name": strategy_name,
        "allocation": allocation,
        "runtime_limit_minutes": runtime_limit_minutes,
        "runtime_policy": runtime_policy,
        "dedupe_stopped_strategy_names": stopped_duplicates,
    }


def _signal_timeframe_seconds(timeframe: str) -> int:
    text = str(timeframe or "1h").strip()
    if not text:
        return 3600
    try:
        unit = text[-1]
        value = int(text[:-1])
    except Exception:
        return 3600
    if unit == "s":
        return max(1, value)
    if unit == "m":
        return max(60, value * 60)
    if unit == "h":
        return max(3600, value * 3600)
    if unit == "d":
        return max(86400, value * 86400)
    return 3600


def _signal_stale_threshold_seconds(timeframe: str) -> int:
    tf_seconds = max(1, _signal_timeframe_seconds(timeframe))
    if tf_seconds < 60:
        return max(180, tf_seconds * 12)
    if tf_seconds <= 15 * 60:
        return max(3 * tf_seconds, 45 * 60)
    if tf_seconds <= 3600:
        return max(2 * tf_seconds, 90 * 60)
    return max(2 * tf_seconds, 3 * 3600)


async def _load_signal_market_data(
    *,
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
    limit: int = 120,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """Load the best available market data for signal generation.

    Prefer the strategy manager loader because it can reuse shared cache and
    opportunistically refresh from the exchange when local parquet is stale.
    """
    from core.data import data_storage

    resolved_exchange = _normalize_exchange(exchange or "binance")
    resolved_symbol = _normalize_symbol(symbol or "BTC/USDT")
    resolved_timeframe = str(timeframe or "1h").strip() or "1h"
    source = "empty"
    load_error: Optional[str] = None
    df = pd.DataFrame()

    try:
        from core.strategies import strategy_manager as sm  # noqa: PLC0415

        df = await asyncio.wait_for(
            sm._load_market_data(
                resolved_exchange,
                resolved_symbol,
                resolved_timeframe,
                limit=max(60, int(limit or 120)),
            ),
            timeout=5.0,
        )
        source = "strategy_manager"
    except Exception as exc:
        load_error = str(exc)
        logger.debug(
            f"signal market-data load via strategy_manager failed for "
            f"{resolved_exchange} {resolved_symbol} {resolved_timeframe}: {exc}"
        )

    if df is None or df.empty:
        try:
            df = await data_storage.load_klines_from_parquet(
                exchange=resolved_exchange,
                symbol=resolved_symbol,
                timeframe=resolved_timeframe,
            )
            source = "parquet"
        except Exception as exc:
            if not load_error:
                load_error = str(exc)
            logger.debug(
                f"signal market-data parquet fallback failed for "
                f"{resolved_exchange} {resolved_symbol} {resolved_timeframe}: {exc}"
            )
            df = pd.DataFrame()

    rows = int(len(df)) if df is not None else 0
    last_bar_at: Optional[str] = None
    age_sec: Optional[float] = None
    stale = rows <= 0
    if rows > 0:
        try:
            last_ts = _normalize_signal_market_timestamp(df.index[-1])
            if last_ts is None:
                raise ValueError("invalid_last_bar_timestamp")
            last_bar_at = last_ts.isoformat()
            last_ts_utc = last_ts.tz_convert(timezone.utc)
            age_sec = max(
                0.0,
                (pd.Timestamp.now(tz=timezone.utc) - last_ts_utc).total_seconds(),
            )
            stale = age_sec > _signal_stale_threshold_seconds(resolved_timeframe)
        except Exception as exc:
            stale = True
            if not load_error:
                load_error = f"freshness_check_failed: {exc}"

    meta = {
        "market_data_exchange": resolved_exchange,
        "market_data_symbol": resolved_symbol,
        "market_data_timeframe": resolved_timeframe,
        "market_data_source": source,
        "market_data_rows": rows,
        "market_data_last_bar_at": last_bar_at,
        "market_data_age_sec": round(float(age_sec), 3) if age_sec is not None else None,
        "market_data_stale": bool(stale),
        "market_data_stale_threshold_sec": _signal_stale_threshold_seconds(resolved_timeframe),
        "market_data_load_error": load_error,
    }
    return (df.copy() if rows > 0 else pd.DataFrame()), meta


def _serialize_funding_cache(provider: FundingRateProvider, *, exchange: str, symbol: str, series) -> Dict[str, Any]:
    path = provider._cache_path(symbol, exchange=exchange)  # noqa: SLF001
    rows = int(len(series)) if series is not None else 0
    latest_rate = None
    age_sec = None
    coverage = {"start": None, "end": None}
    if rows > 0:
        try:
            coverage = {
                "start": pd.Timestamp(series.index.min()).isoformat(),
                "end": pd.Timestamp(series.index.max()).isoformat(),
            }
            latest_rate = float(series.iloc[-1])
            latest_ts = pd.Timestamp(series.index.max())
            latest_ts = latest_ts.tz_localize("UTC") if latest_ts.tzinfo is None else latest_ts.tz_convert("UTC")
            age_sec = max(0.0, (pd.Timestamp.now(tz="UTC") - latest_ts).total_seconds())
        except Exception:
            coverage = {"start": None, "end": None}
            latest_rate = None
    max_age_sec = 36 * 3600
    health = "missing" if rows <= 0 else ("unknown" if age_sec is None else ("stale" if age_sec > max_age_sec else "healthy"))
    return {
        "exchange": exchange,
        "symbol": symbol,
        "cache_path": str(path),
        "cache_exists": bool(path.exists()),
        "rows": rows,
        "latest_rate": latest_rate,
        "coverage": coverage,
        "health": health,
        "ready": health == "healthy",
        "age_sec": age_sec,
        "max_age_sec": max_age_sec,
        "storage": {
            "mode": "parquet",
            "description": "资金费率历史缓存写入本地 Parquet，研究回测直接读取",
        },
    }


_AUTONOMOUS_AGENT_STRATEGY = "AI_AutonomousAgent"
_AUTONOMOUS_ENTRY_ACTIONS = {"buy", "sell"}
_AUTONOMOUS_EXIT_ACTIONS = {"close_long", "close_short"}


def _review_safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _review_safe_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _normalize_review_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if ":" in text:
        text = text.split(":", 1)[0].strip()
    return text


def _review_symbol_matches(left: Any, right: Any) -> bool:
    normalized_left = _normalize_review_symbol(left)
    normalized_right = _normalize_review_symbol(right)
    return bool(normalized_left and normalized_right and normalized_left == normalized_right)


def _review_action_side(action: Any) -> str:
    value = str(action or "").strip().lower()
    if value in {"buy", "close_long"}:
        return "long"
    if value in {"sell", "close_short"}:
        return "short"
    return ""


def _review_action_order_side(action: Any) -> str:
    value = str(action or "").strip().lower()
    if value in {"buy", "close_short"}:
        return "buy"
    if value in {"sell", "close_long"}:
        return "sell"
    return ""


def _review_action_label(action: Any) -> str:
    mapping = {
        "buy": "开多",
        "sell": "开空",
        "close_long": "平多",
        "close_short": "平空",
        "hold": "观望",
    }
    value = str(action or "").strip().lower()
    return mapping.get(value, str(action or "--"))


def _review_status_label(status: str) -> str:
    return {
        "open_gain": "持仓浮盈",
        "open_loss": "持仓浮亏",
        "closed_gain": "平仓盈利",
        "closed_loss": "平仓亏损",
        "closed_flat": "平仓打平",
        "stacked_entry": "同向连续加码",
        "pending": "等待后续",
        "profit_exit": "盈利离场",
        "loss_exit": "亏损离场",
        "flat_exit": "中性离场",
        "orphan_exit": "缺少对应开仓",
    }.get(str(status or "").strip().lower(), "待观察")


def _review_status_tone(status: str) -> str:
    value = str(status or "").strip().lower()
    if value in {"open_gain", "closed_gain", "profit_exit"}:
        return "good"
    if value in {"open_loss", "closed_loss", "loss_exit", "stacked_entry", "orphan_exit"}:
        return "danger"
    if value in {"pending", "closed_flat", "flat_exit"}:
        return "warn"
    return "info"


def _review_position_payload(position: Any) -> Optional[Dict[str, Any]]:
    if position is None:
        return None
    try:
        payload = position.to_dict() if hasattr(position, "to_dict") else dict(position)
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        return None
    side_value = payload.get("side")
    if hasattr(side_value, "value"):
        side_value = side_value.value
    return {
        "symbol": payload.get("symbol"),
        "symbol_norm": _normalize_review_symbol(payload.get("symbol")),
        "exchange": payload.get("exchange"),
        "side": str(side_value or "").strip().lower(),
        "entry_price": _review_safe_float(payload.get("entry_price")),
        "current_price": _review_safe_float(payload.get("current_price")),
        "quantity": _review_safe_float(payload.get("quantity")),
        "value": _review_safe_float(payload.get("value")),
        "unrealized_pnl": _review_safe_float(payload.get("unrealized_pnl")),
        "unrealized_pnl_pct": _review_safe_float(payload.get("unrealized_pnl_pct")),
        "realized_pnl": _review_safe_float(payload.get("realized_pnl")),
        "opened_at": payload.get("opened_at"),
        "updated_at": payload.get("updated_at"),
        "strategy": payload.get("strategy"),
        "account_id": payload.get("account_id"),
        "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
        "stop_loss": _review_safe_float(payload.get("stop_loss")),
        "take_profit": _review_safe_float(payload.get("take_profit")),
    }


def _review_price_markout_bps(entry_price: Any, current_price: Any, side: str) -> Optional[float]:
    entry = _review_safe_float(entry_price)
    current = _review_safe_float(current_price)
    if entry is None or current is None or entry <= 0:
        return None
    side_value = str(side or "").strip().lower()
    if side_value == "short":
        return (entry - current) / entry * 10000.0
    if side_value == "long":
        return (current - entry) / entry * 10000.0
    return None


def _review_curve_row_symbol(row: Dict[str, Any]) -> str:
    config = row.get("config") if isinstance(row.get("config"), dict) else {}
    if config.get("symbol"):
        return str(config.get("symbol") or "")
    execution = row.get("execution") if isinstance(row.get("execution"), dict) else {}
    signal = execution.get("signal") if isinstance(execution.get("signal"), dict) else {}
    return str(signal.get("symbol") or "")


def _review_append_curve_point(
    points: List[Dict[str, Any]],
    *,
    timestamp: Any,
    pnl: Any,
    price: Any = None,
    kind: str = "mark",
    label: Optional[str] = None,
) -> None:
    ts = str(timestamp or "").strip()
    pnl_value = _review_safe_float(pnl)
    if not ts or pnl_value is None:
        return
    point: Dict[str, Any] = {
        "timestamp": ts,
        "pnl": round(float(pnl_value), 6),
        "kind": str(kind or "mark").strip().lower() or "mark",
    }
    price_value = _review_safe_float(price)
    if price_value is not None:
        point["price"] = round(float(price_value), 8)
    if label:
        point["label"] = str(label)
    if points and str(points[-1].get("timestamp") or "") == ts:
        points[-1].update(point)
    else:
        points.append(point)


def _build_autonomous_review_profit_curve(
    *,
    entry_event: Dict[str, Any],
    journal_rows: List[Dict[str, Any]],
    exit_event: Optional[Dict[str, Any]] = None,
    current_position: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(entry_event, dict) or not journal_rows:
        return None
    entry_row_index = entry_event.get("_row_index")
    if not isinstance(entry_row_index, int) or entry_row_index < 0 or entry_row_index >= len(journal_rows):
        return None
    entry_symbol = entry_event.get("symbol")
    entry_side = str(entry_event.get("position_side") or "").strip().lower()
    if entry_side not in {"long", "short"}:
        return None

    points: List[Dict[str, Any]] = []
    _review_append_curve_point(
        points,
        timestamp=entry_event.get("timestamp"),
        pnl=0.0,
        price=entry_event.get("price"),
        kind="entry",
        label="entry",
    )

    end_row_index = len(journal_rows) - 1
    if isinstance(exit_event, dict) and isinstance(exit_event.get("_row_index"), int):
        end_row_index = min(end_row_index, int(exit_event["_row_index"]))

    for row_index in range(entry_row_index + 1, end_row_index + 1):
        row = journal_rows[row_index]
        if row_index == end_row_index and isinstance(exit_event, dict):
            exit_context = row.get("context") if isinstance(row.get("context"), dict) else {}
            exit_position = exit_context.get("position") if isinstance(exit_context.get("position"), dict) else {}
            close_price = _review_safe_float(exit_event.get("price"))
            if close_price is None:
                close_price = _review_safe_float((exit_position or {}).get("current_price"))
            if close_price is None:
                close_price = _review_safe_float(exit_context.get("price"))
            _review_append_curve_point(
                points,
                timestamp=exit_event.get("timestamp"),
                pnl=(exit_event.get("position_before") or {}).get("unrealized_pnl"),
                price=close_price,
                kind="exit",
                label="exit",
            )
            continue

        row_symbol = _review_curve_row_symbol(row)
        if not _review_symbol_matches(row_symbol, entry_symbol):
            continue
        context = row.get("context") if isinstance(row.get("context"), dict) else {}
        position = context.get("position") if isinstance(context.get("position"), dict) else {}
        if not position:
            continue
        if str(position.get("side") or "").strip().lower() != entry_side:
            continue
        _review_append_curve_point(
            points,
            timestamp=row.get("timestamp"),
            pnl=position.get("unrealized_pnl"),
            price=position.get("current_price") or context.get("price"),
            kind="mark",
        )

    if exit_event is None and isinstance(current_position, dict):
        _review_append_curve_point(
            points,
            timestamp=current_position.get("updated_at") or current_position.get("opened_at") or datetime.now(timezone.utc).isoformat(),
            pnl=current_position.get("unrealized_pnl"),
            price=current_position.get("current_price"),
            kind="current",
            label="current",
        )

    if len(points) < 2:
        return None

    pnl_series = [float(point.get("pnl") or 0.0) for point in points]
    return {
        "points": points,
        "closed": bool(exit_event),
        "entry_timestamp": entry_event.get("timestamp"),
        "close_timestamp": (exit_event or {}).get("timestamp") if isinstance(exit_event, dict) else None,
        "point_count": len(points),
        "final_pnl": round(float(pnl_series[-1]), 6),
        "max_pnl": round(float(max(pnl_series)), 6),
        "min_pnl": round(float(min(pnl_series)), 6),
    }


def _review_is_model_issue(row: Dict[str, Any]) -> bool:
    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
    primary = diagnostics.get("primary") if isinstance(diagnostics.get("primary"), dict) else {}
    code = str(primary.get("code") or "").strip().lower()
    label = str(primary.get("label") or "").strip().lower()
    reason = str(((row.get("decision") or {}) if isinstance(row.get("decision"), dict) else {}).get("reason") or "").strip().lower()
    return (
        code.startswith("model_")
        or "503" in label
        or "超时" in label
        or reason.startswith("model_error:")
    )


def _serialize_autonomy_order(order: Any) -> Dict[str, Any]:
    meta = order_manager.get_order_metadata(order.id)
    order_type = getattr(getattr(order, "type", None), "value", getattr(order, "type", "")) or ""
    order_side = getattr(getattr(order, "side", None), "value", getattr(order, "side", "")) or ""
    order_status = getattr(getattr(order, "status", None), "value", getattr(order, "status", "")) or ""
    return {
        "id": order.id,
        "exchange": getattr(order, "exchange", ""),
        "symbol": getattr(order, "symbol", ""),
        "symbol_norm": _normalize_review_symbol(getattr(order, "symbol", "")),
        "side": str(order_side).strip().lower(),
        "type": str(order_type).strip().lower(),
        "status": str(order_status).strip().lower(),
        "price": _review_safe_float(getattr(order, "price", None)),
        "amount": _review_safe_float(getattr(order, "amount", None)),
        "filled": _review_safe_float(getattr(order, "filled", None)),
        "timestamp": getattr(getattr(order, "timestamp", None), "isoformat", lambda: None)(),
        "strategy": meta.get("strategy"),
        "account_id": meta.get("account_id"),
        "stop_loss": _review_safe_float(meta.get("stop_loss")),
        "take_profit": _review_safe_float(meta.get("take_profit")),
        "reduce_only": bool(meta.get("reduce_only", False)),
        "match_source": "order_record",
        "match_label": "",
    }


def _match_autonomy_order(
    *,
    timestamp: Any,
    symbol: Any,
    action: Any,
    orders: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    target_dt = _review_safe_dt(timestamp)
    desired_order_side = _review_action_order_side(action)
    candidates: List[tuple[int, float, Dict[str, Any]]] = []
    for order in orders:
        if not _review_symbol_matches(order.get("symbol"), symbol):
            continue
        if desired_order_side and str(order.get("side") or "").strip().lower() != desired_order_side:
            continue
        order_dt = _review_safe_dt(order.get("timestamp"))
        if target_dt and order_dt:
            delta_seconds = abs((order_dt - target_dt).total_seconds())
        else:
            delta_seconds = 0.0
        strategy_rank = 0 if str(order.get("strategy") or "").strip() == _AUTONOMOUS_AGENT_STRATEGY else 1
        if target_dt and delta_seconds > 6 * 3600:
            continue
        candidates.append((strategy_rank, delta_seconds, order))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def _build_autonomy_order_fallback(
    *,
    timestamp: Any,
    symbol: Any,
    action: Any,
    execution_signal: Dict[str, Any],
    config: Dict[str, Any],
    position_before: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not isinstance(execution_signal, dict) or not execution_signal:
        return None
    signal_meta = execution_signal.get("metadata") if isinstance(execution_signal.get("metadata"), dict) else {}
    strategy_name = str(execution_signal.get("strategy_name") or signal_meta.get("strategy") or "").strip()
    signal_source = str(signal_meta.get("source") or "").strip().lower()
    if strategy_name != _AUTONOMOUS_AGENT_STRATEGY and signal_source != "ai_autonomous_agent":
        return None

    action_value = str(action or execution_signal.get("signal_type") or "").strip().lower()
    order_side = _review_action_order_side(action_value)
    order_price = _review_safe_float(execution_signal.get("price"))
    if order_price is None:
        order_price = _review_safe_float((position_before or {}).get("current_price"))
    if order_price is None:
        order_price = _review_safe_float((position_before or {}).get("entry_price"))
    order_amount = _review_safe_float(execution_signal.get("quantity"))
    if order_amount is None and action_value in _AUTONOMOUS_EXIT_ACTIONS:
        order_amount = _review_safe_float((position_before or {}).get("quantity"))
    if not order_side and order_price is None and order_amount is None:
        return None

    exchange = str(signal_meta.get("exchange") or config.get("exchange") or "").strip().lower()
    position_source = str((position_before or {}).get("source") or "").strip().lower()
    match_source = "journal_signal"
    match_label = "journal signal"
    if exchange == "binance" and position_source == "exchange_live" and action_value in _AUTONOMOUS_EXIT_ACTIONS:
        match_source = "merged_position"
        match_label = "binance merged position"

    fallback_symbol = execution_signal.get("symbol") or symbol or config.get("symbol") or ""
    fallback_timestamp = execution_signal.get("timestamp") or timestamp
    fallback_key = f"{fallback_timestamp}|{fallback_symbol}|{action_value}|{match_source}"
    fallback_id = f"review-fallback-{hashlib.sha1(fallback_key.encode('utf-8')).hexdigest()[:12]}"

    return {
        "id": fallback_id,
        "exchange": exchange,
        "symbol": fallback_symbol,
        "symbol_norm": _normalize_review_symbol(fallback_symbol),
        "side": order_side,
        "type": "market",
        "status": "submitted" if match_source == "journal_signal" else "merged_position",
        "price": order_price,
        "amount": order_amount,
        "filled": order_amount,
        "timestamp": fallback_timestamp,
        "strategy": strategy_name or _AUTONOMOUS_AGENT_STRATEGY,
        "account_id": signal_meta.get("account_id"),
        "stop_loss": _review_safe_float(execution_signal.get("stop_loss")),
        "take_profit": _review_safe_float(execution_signal.get("take_profit")),
        "reduce_only": bool(signal_meta.get("reduce_only")) or action_value in _AUTONOMOUS_EXIT_ACTIONS,
        "match_source": match_source,
        "match_label": match_label,
    }


# Rows are ~40KB each, so the review works on a recent tail (a few days of
# ticks) instead of parsing the whole journal on every request.
_AUTONOMOUS_REVIEW_JOURNAL_ROWS = 2000


def _load_autonomous_agent_review_journal() -> List[Dict[str, Any]]:
    return autonomous_trading_agent.read_journal(
        limit=_AUTONOMOUS_REVIEW_JOURNAL_ROWS,
        max_limit=_AUTONOMOUS_REVIEW_JOURNAL_ROWS,
    )


def _build_autonomous_agent_review(
    limit: int = 12,
    *,
    journal_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    review_limit = max(1, min(int(limit or 12), 30))
    if journal_rows is None:
        journal_rows = _load_autonomous_agent_review_journal()
    journal_rows = [row for row in journal_rows if isinstance(row, dict)]
    if not journal_rows:
        return {
            "summary": {
                "submitted_count": 0,
                "entry_count": 0,
                "close_count": 0,
                "losing_close_count": 0,
                "repeated_same_direction_entries": 0,
                "outage_after_entry_count": 0,
                "unmatched_entry_count": 0,
                "current_open_count": 0,
                "current_open_unrealized_pnl": 0.0,
                "dominant_symbol": None,
                "dominant_entry_side": None,
                "latest_entry_symbol": None,
                "latest_entry_at": None,
            },
            "insights": ["当前还没有自治代理放行记录，先运行几轮后这里会自动生成复盘摘要。"],
            "items": [],
        }

    serialized_orders = [
        _serialize_autonomy_order(order)
        for order in order_manager.get_recent_orders(limit=5000)
    ]

    submitted_indices: List[int] = []
    events_by_row_index: Dict[int, Dict[str, Any]] = {}
    events: List[Dict[str, Any]] = []
    entry_events: List[Dict[str, Any]] = []
    exit_events: List[Dict[str, Any]] = []
    symbol_counter: Counter[str] = Counter()
    entry_side_counter: Counter[str] = Counter()
    primary_counter: Counter[str] = Counter()
    repeated_same_direction_entries = 0
    losing_close_count = 0

    for row_index, row in enumerate(journal_rows):
        execution = row.get("execution") if isinstance(row.get("execution"), dict) else {}
        if not bool(execution.get("submitted")):
            diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
            primary = diagnostics.get("primary") if isinstance(diagnostics.get("primary"), dict) else {}
            primary_label = str(primary.get("label") or "").strip()
            if primary_label:
                primary_counter[primary_label] += 1
            continue

        decision = row.get("decision") if isinstance(row.get("decision"), dict) else {}
        diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
        primary = diagnostics.get("primary") if isinstance(diagnostics.get("primary"), dict) else {}
        selection = row.get("selection") if isinstance(row.get("selection"), dict) else {}
        config = row.get("config") if isinstance(row.get("config"), dict) else {}
        context = row.get("context") if isinstance(row.get("context"), dict) else {}
        execution_signal = execution.get("signal") if isinstance(execution.get("signal"), dict) else {}
        position_before = context.get("position") if isinstance(context.get("position"), dict) else {}
        cost = context.get("execution_cost") if isinstance(context.get("execution_cost"), dict) else {}
        aggregated_signal = context.get("aggregated_signal") if isinstance(context.get("aggregated_signal"), dict) else {}
        action = str(decision.get("action") or execution_signal.get("signal_type") or "").strip().lower()
        phase = "entry" if action in _AUTONOMOUS_ENTRY_ACTIONS else ("exit" if action in _AUTONOMOUS_EXIT_ACTIONS else "other")
        symbol = (
            execution_signal.get("symbol")
            or config.get("symbol")
            or selection.get("selected_symbol")
            or ""
        )
        position_side = _review_action_side(action)
        repeat_open_rank = 1
        if phase == "entry":
            repeat_open_rank = 1
        matched_order = _match_autonomy_order(
            timestamp=row.get("timestamp"),
            symbol=symbol,
            action=action,
            orders=serialized_orders,
        )
        if matched_order is None:
            matched_order = _build_autonomy_order_fallback(
                timestamp=row.get("timestamp"),
                symbol=symbol,
                action=action,
                execution_signal=execution_signal,
                config=config,
                position_before=position_before,
            )

        event = {
            "id": f"agent-review-{len(events) + 1}",
            "timestamp": row.get("timestamp"),
            "phase": phase,
            "symbol": symbol,
            "symbol_norm": _normalize_review_symbol(symbol),
            "action": action,
            "action_label": _review_action_label(action),
            "position_side": position_side,
            "price": _review_safe_float(execution_signal.get("price") or context.get("price")),
            "decision_confidence": _review_safe_float(decision.get("confidence")),
            "reason": str(decision.get("reason") or execution.get("reason") or "").strip(),
            "primary": {
                "label": str(primary.get("label") or "无结构化原因"),
                "detail": str(primary.get("detail") or "").strip(),
                "tone": str(primary.get("tone") or "info").strip().lower(),
            },
            "aggregated_signal": {
                "direction": aggregated_signal.get("direction"),
                "confidence": _review_safe_float(aggregated_signal.get("confidence")),
            },
            "cost": {
                "one_way_bps": _review_safe_float(cost.get("estimated_one_way_cost_bps")),
                "round_trip_bps": _review_safe_float(cost.get("estimated_round_trip_cost_bps")),
                "fee_bps": _review_safe_float(cost.get("fee_bps")),
                "slippage_bps": _review_safe_float(cost.get("estimated_slippage_bps")),
            },
            "position_before": {
                "side": str(position_before.get("side") or "").strip().lower(),
                "quantity": _review_safe_float(position_before.get("quantity")),
                "entry_price": _review_safe_float(position_before.get("entry_price")),
                "current_price": _review_safe_float(position_before.get("current_price")),
                "unrealized_pnl": _review_safe_float(position_before.get("unrealized_pnl")),
                "source": position_before.get("source"),
            },
            "order": matched_order,
            "pair": {
                "matched": False,
                "entry_id": None,
                "exit_id": None,
                "holding_minutes": None,
                "repeat_open_rank": repeat_open_rank,
                "close_unrealized_pnl": None,
            },
            "follow_up": {
                "observed_count": 0,
                "latest_timestamp": None,
                "latest_price": None,
                "latest_unrealized_pnl": None,
                "latest_primary_label": None,
                "favorable_markout_bps": None,
                "adverse_markout_bps": None,
                "outage_hold_count": 0,
                "blockers": [],
            },
            "review_status": "pending",
            "review_status_text": _review_status_label("pending"),
            "review_status_tone": _review_status_tone("pending"),
            "profit_curve": None,
            "summary_lines": [],
            "_row_index": row_index,
        }

        if phase == "entry":
            entry_events.append(event)
            symbol_counter[event["symbol"]] += 1
            entry_side_counter[position_side] += 1
        elif phase == "exit":
            exit_events.append(event)
            pre_close_pnl = _review_safe_float(event["position_before"].get("unrealized_pnl"))
            if pre_close_pnl is not None and pre_close_pnl < 0:
                losing_close_count += 1

        events.append(event)
        events_by_row_index[row_index] = event
        submitted_indices.append(row_index)

    for idx, row_index in enumerate(submitted_indices):
        event = events_by_row_index[row_index]
        next_row_index = submitted_indices[idx + 1] if idx + 1 < len(submitted_indices) else len(journal_rows)
        follow_rows = journal_rows[row_index + 1:next_row_index]
        blocker_counter: Counter[str] = Counter()
        markouts: List[float] = []
        latest_price: Optional[float] = None
        latest_unrealized_pnl: Optional[float] = None
        latest_primary_label: Optional[str] = None
        latest_timestamp: Optional[str] = None
        outage_hold_count = 0
        for follow_row in follow_rows:
            follow_context = follow_row.get("context") if isinstance(follow_row.get("context"), dict) else {}
            follow_position = follow_context.get("position") if isinstance(follow_context.get("position"), dict) else {}
            follow_diagnostics = follow_row.get("diagnostics") if isinstance(follow_row.get("diagnostics"), dict) else {}
            follow_primary = follow_diagnostics.get("primary") if isinstance(follow_diagnostics.get("primary"), dict) else {}
            latest_timestamp = follow_row.get("timestamp")
            latest_price = _review_safe_float(follow_context.get("price"), latest_price)
            latest_unrealized_pnl = _review_safe_float(follow_position.get("unrealized_pnl"), latest_unrealized_pnl)
            latest_primary_label = str(follow_primary.get("label") or "").strip() or latest_primary_label
            if _review_is_model_issue(follow_row):
                outage_hold_count += 1
            blocker_label = str(follow_primary.get("label") or "").strip()
            if blocker_label and blocker_label != "无结构化原因":
                blocker_counter[blocker_label] += 1
            if event["phase"] == "entry":
                markout_bps = _review_price_markout_bps(event.get("price"), follow_context.get("price"), event.get("position_side"))
                if markout_bps is not None:
                    markouts.append(markout_bps)
        event["follow_up"] = {
            "observed_count": len(follow_rows),
            "latest_timestamp": latest_timestamp,
            "latest_price": latest_price,
            "latest_unrealized_pnl": latest_unrealized_pnl,
            "latest_primary_label": latest_primary_label,
            "favorable_markout_bps": max(markouts) if markouts else None,
            "adverse_markout_bps": min(markouts) if markouts else None,
            "outage_hold_count": outage_hold_count,
            "blockers": [
                {"label": label, "count": count}
                for label, count in blocker_counter.most_common(3)
            ],
        }

    open_stacks: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for event in events:
        if event["phase"] == "entry":
            key = (event["symbol_norm"], event["position_side"])
            stack = open_stacks.setdefault(key, [])
            repeat_rank = len(stack) + 1
            event["pair"]["repeat_open_rank"] = repeat_rank
            if repeat_rank > 1:
                repeated_same_direction_entries += 1
            stack.append(event)
            continue

        if event["phase"] != "exit":
            continue

        key = (event["symbol_norm"], event["position_side"])
        stack = open_stacks.setdefault(key, [])
        if not stack:
            event["review_status"] = "orphan_exit"
            event["review_status_text"] = _review_status_label("orphan_exit")
            event["review_status_tone"] = _review_status_tone("orphan_exit")
            continue

        entry_event = stack.pop()
        opened_at = _review_safe_dt(entry_event.get("timestamp"))
        closed_at = _review_safe_dt(event.get("timestamp"))
        holding_minutes = None
        if opened_at and closed_at:
            holding_minutes = round(max(0.0, (closed_at - opened_at).total_seconds()) / 60.0, 1)
        close_unrealized_pnl = _review_safe_float(event["position_before"].get("unrealized_pnl"))
        entry_event["pair"].update(
            {
                "matched": True,
                "entry_id": entry_event["id"],
                "exit_id": event["id"],
                "holding_minutes": holding_minutes,
                "close_unrealized_pnl": close_unrealized_pnl,
                "closed_at": event.get("timestamp"),
                "exit_action": event.get("action"),
                "exit_action_label": event.get("action_label"),
                "exit_row_index": event.get("_row_index"),
            }
        )
        event["pair"].update(
            {
                "matched": True,
                "entry_id": entry_event["id"],
                "exit_id": event["id"],
                "holding_minutes": holding_minutes,
                "opened_at": entry_event.get("timestamp"),
                "entry_action": entry_event.get("action"),
                "entry_action_label": entry_event.get("action_label"),
                "entry_row_index": entry_event.get("_row_index"),
            }
        )

    unmatched_entries: List[Dict[str, Any]] = []
    for stack in open_stacks.values():
        unmatched_entries.extend(stack)

    current_positions = [_review_position_payload(position) for position in position_manager.get_all_positions()]
    current_positions = [position for position in current_positions if position]
    matched_current_positions: List[Dict[str, Any]] = []
    for position in current_positions:
        if not isinstance(position, dict):
            continue
        matching_unmatched = [
            entry
            for entry in unmatched_entries
            if _review_symbol_matches(entry.get("symbol"), position.get("symbol"))
            and str(entry.get("position_side") or "").strip().lower() == str(position.get("side") or "").strip().lower()
        ]
        strategy_name = str(position.get("strategy") or "").strip()
        if not matching_unmatched and strategy_name != _AUTONOMOUS_AGENT_STRATEGY:
            continue
        matched_current_positions.append(position)
        if matching_unmatched:
            latest_entry = matching_unmatched[-1]
            latest_entry["current_position"] = position

    events_by_id = {str(event.get("id") or ""): event for event in events}
    for event in entry_events:
        pair = event.get("pair") if isinstance(event.get("pair"), dict) else {}
        exit_event = events_by_id.get(str(pair.get("exit_id") or "")) if pair.get("matched") else None
        curve = _build_autonomous_review_profit_curve(
            entry_event=event,
            exit_event=exit_event if isinstance(exit_event, dict) else None,
            current_position=event.get("current_position") if isinstance(event.get("current_position"), dict) else None,
            journal_rows=journal_rows,
        )
        if curve is not None:
            event["profit_curve"] = curve
            if isinstance(exit_event, dict):
                exit_event["profit_curve"] = curve

    outage_after_entry_count = sum(
        1
        for event in entry_events
        if int((event.get("follow_up") or {}).get("outage_hold_count") or 0) > 0
    )

    total_current_unrealized = sum(
        float(position.get("unrealized_pnl") or 0.0)
        for position in matched_current_positions
    )

    for event in events:
        current_position = event.get("current_position") if isinstance(event.get("current_position"), dict) else None
        close_unrealized_pnl = _review_safe_float((event.get("pair") or {}).get("close_unrealized_pnl"))
        latest_unrealized = _review_safe_float((event.get("follow_up") or {}).get("latest_unrealized_pnl"))
        summary_lines: List[str] = []
        if event["phase"] == "entry":
            if current_position:
                live_unrealized = _review_safe_float(current_position.get("unrealized_pnl"), 0.0) or 0.0
                event["review_status"] = "open_gain" if live_unrealized > 0 else ("open_loss" if live_unrealized < 0 else "pending")
                summary_lines.append(
                    f"当前仍持有 {event['symbol']} {event['action_label']}，最新浮盈亏约 {live_unrealized:.4f} USDT。"
                )
            elif close_unrealized_pnl is not None:
                if close_unrealized_pnl > 0:
                    event["review_status"] = "closed_gain"
                elif close_unrealized_pnl < 0:
                    event["review_status"] = "closed_loss"
                else:
                    event["review_status"] = "closed_flat"
                action_text = "盈利" if close_unrealized_pnl > 0 else ("亏损" if close_unrealized_pnl < 0 else "打平")
                summary_lines.append(
                    f"后续已通过 {event['pair'].get('exit_action_label') or '平仓'} 结束，平仓前浮盈亏约 {close_unrealized_pnl:.4f} USDT（{action_text}）。"
                )
            elif latest_unrealized is not None:
                event["review_status"] = "open_gain" if latest_unrealized > 0 else ("open_loss" if latest_unrealized < 0 else "pending")
                summary_lines.append(
                    f"日志最近一次跟踪显示，这笔 {event['symbol']} {event['action_label']} 浮盈亏约 {latest_unrealized:.4f} USDT。"
                )
            elif int((event.get("pair") or {}).get("repeat_open_rank") or 1) > 1:
                event["review_status"] = "stacked_entry"
                summary_lines.append(
                    f"这是同币种同方向第 {int(event['pair']['repeat_open_rank'])} 次连续放行，仓位连续叠加风险偏高。"
                )
            else:
                event["review_status"] = "pending"
                summary_lines.append("这笔开仓之后还没有看到明确的平仓动作，继续观察后续管理质量。")
            adverse_markout = _review_safe_float((event.get("follow_up") or {}).get("adverse_markout_bps"))
            favorable_markout = _review_safe_float((event.get("follow_up") or {}).get("favorable_markout_bps"))
            if favorable_markout is not None or adverse_markout is not None:
                pieces = []
                if favorable_markout is not None:
                    pieces.append(f"最好走出 {favorable_markout:.1f} bps")
                if adverse_markout is not None:
                    pieces.append(f"最差回撤到 {adverse_markout:.1f} bps")
                summary_lines.append("后续观测里，" + "，".join(pieces) + "。")
            outage_hold_count = int((event.get("follow_up") or {}).get("outage_hold_count") or 0)
            if outage_hold_count > 0:
                summary_lines.append(f"开仓后的管理阶段又遇到 {outage_hold_count} 次模型 503/超时回退为 hold。")
            if (
                latest_unrealized is not None
                and current_position is None
                and close_unrealized_pnl is None
                and event["review_status"] == "pending"
            ):
                summary_lines.append(f"最近一次跟踪时，这笔仓位相关浮盈亏约 {latest_unrealized:.4f} USDT。")
        elif event["phase"] == "exit":
            pre_close_pnl = _review_safe_float(event["position_before"].get("unrealized_pnl"))
            if pre_close_pnl is None or abs(pre_close_pnl) < 1e-9:
                event["review_status"] = "flat_exit"
                summary_lines.append("平仓前盈亏接近打平，这更像一次中性降风险处理。")
            elif pre_close_pnl > 0:
                event["review_status"] = "profit_exit"
                summary_lines.append(f"平仓前这笔仓位处于浮盈，约 {pre_close_pnl:.4f} USDT。")
            else:
                event["review_status"] = "loss_exit"
                summary_lines.append(f"平仓前这笔仓位处于浮亏，约 {pre_close_pnl:.4f} USDT。")
            holding_minutes = _review_safe_float((event.get("pair") or {}).get("holding_minutes"))
            if holding_minutes is not None:
                summary_lines.append(f"对应开仓大约持有了 {holding_minutes:.1f} 分钟。")
        else:
            event["review_status"] = "pending"
            summary_lines.append("这条记录暂时无法归类到标准开仓或平仓动作。")

        blockers = (event.get("follow_up") or {}).get("blockers") or []
        if blockers:
            blocker_text = "，".join(
                f"{item.get('label')} x{int(item.get('count') or 0)}"
                for item in blockers[:3]
            )
            summary_lines.append(f"后续阻塞原因主要是：{blocker_text}。")
        if event.get("cost", {}).get("one_way_bps") is not None:
            summary_lines.append(f"估算单边执行成本约 {float(event['cost']['one_way_bps']):.2f} bps。")

        event["review_status_text"] = _review_status_label(event["review_status"])
        event["review_status_tone"] = _review_status_tone(event["review_status"])
        event["summary_lines"] = summary_lines[:5]
        event.pop("_row_index", None)

    dominant_symbol = symbol_counter.most_common(1)[0][0] if symbol_counter else None
    dominant_entry_side = entry_side_counter.most_common(1)[0][0] if entry_side_counter else None
    latest_entry = entry_events[-1] if entry_events else None

    insights: List[str] = []
    if matched_current_positions:
        if total_current_unrealized < 0:
            insights.append(
                f"当前仍有 {len(matched_current_positions)} 笔自治仓位处于浮亏，合计约 {total_current_unrealized:.4f} USDT。"
            )
        else:
            insights.append(
                f"当前仍有 {len(matched_current_positions)} 笔自治仓位在跟踪中，合计浮盈亏约 {total_current_unrealized:.4f} USDT。"
            )
    elif latest_entry:
        latest_entry_unrealized = _review_safe_float((latest_entry.get("follow_up") or {}).get("latest_unrealized_pnl"))
        if latest_entry_unrealized is not None:
            insights.append(
                f"最近一笔放行是 {latest_entry.get('symbol') or '--'} {latest_entry.get('action_label') or ''}，日志最新跟踪浮盈亏约 {latest_entry_unrealized:.4f} USDT。"
            )
    if exit_events:
        insights.append(
            f"最近 {len(exit_events)} 次平仓里，有 {losing_close_count} 次是在浮亏状态下触发的。"
        )
    if repeated_same_direction_entries > 0:
        symbol_text = dominant_symbol or "同一币种"
        insights.append(
            f"最近出现 {repeated_same_direction_entries} 次同币种同方向连续放行，{symbol_text} 最明显，需要重点检查仓位感知与事件配对。"
        )
    if outage_after_entry_count > 0:
        insights.append(
            f"最近有 {outage_after_entry_count} 次开仓后，后续管理阶段又遇到模型 503/超时回退为 hold。"
        )
    if dominant_entry_side == "short" and entry_events:
        insights.append(f"最近开仓以做空为主，占 {entry_side_counter['short']}/{len(entry_events)}。")
    if not insights:
        insights.append("最近没有足够多的放行记录，复盘结果偏样本不足，建议继续积累交易样本。")

    return {
        "summary": {
            "submitted_count": len(events),
            "entry_count": len(entry_events),
            "close_count": len(exit_events),
            "losing_close_count": losing_close_count,
            "repeated_same_direction_entries": repeated_same_direction_entries,
            "outage_after_entry_count": outage_after_entry_count,
            "unmatched_entry_count": len(unmatched_entries),
            "current_open_count": len(matched_current_positions),
            "current_open_unrealized_pnl": round(total_current_unrealized, 6),
            "dominant_symbol": dominant_symbol,
            "dominant_entry_side": dominant_entry_side,
            "latest_entry_symbol": latest_entry.get("symbol") if latest_entry else None,
            "latest_entry_at": latest_entry.get("timestamp") if latest_entry else None,
            "top_rejection_reasons": [
                {"label": label, "count": count}
                for label, count in primary_counter.most_common(5)
            ],
        },
        "insights": insights[:6],
        "items": list(reversed(events[-review_limit:])),
    }


def _get_autonomous_agent_learning_memory() -> Dict[str, Any]:
    try:
        payload = autonomous_trading_agent.get_learning_memory(force=False)
    except Exception as exc:
        logger.debug(f"autonomous-agent scorecard learning memory unavailable: {exc}")
        return {}
    return payload if isinstance(payload, dict) else {}


def _get_autonomous_agent_risk_report() -> Dict[str, Any]:
    try:
        from core.risk.risk_manager import risk_manager  # noqa: PLC0415

        payload = risk_manager.get_risk_report()
    except Exception as exc:
        logger.debug(f"autonomous-agent scorecard risk report unavailable: {exc}")
        return {}
    return payload if isinstance(payload, dict) else {}


def _autonomous_agent_selected_symbol(runtime_config: Dict[str, Any], agent_status: Dict[str, Any]) -> str:
    status_payload = dict(agent_status or {})
    for key in ("last_symbol_scan", "preview_symbol_scan"):
        selection = status_payload.get(key)
        if not isinstance(selection, dict):
            continue
        selected_symbol = _normalize_symbol(str(selection.get("selected_symbol") or ""))
        if selected_symbol:
            return selected_symbol
    return _normalize_symbol(str((runtime_config or {}).get("symbol") or ""))


def _build_autonomous_agent_eligibility_summary(
    *,
    runtime_config: Optional[Dict[str, Any]] = None,
    agent_status: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    runtime_cfg = dict(runtime_config or {})
    status_payload = dict(agent_status or {})
    exchange = str(runtime_cfg.get("exchange") or "binance").strip().lower() or "binance"
    symbol = _autonomous_agent_selected_symbol(runtime_cfg, status_payload)

    selection = {}
    for key in ("last_symbol_scan", "preview_symbol_scan"):
        payload = status_payload.get(key)
        if isinstance(payload, dict) and payload:
            selection = dict(payload)
            break

    selection_config = dict(selection.get("scan_config") or {}) if isinstance(selection.get("scan_config"), dict) else {}
    timeframe = str(selection_config.get("timeframe") or runtime_cfg.get("timeframe") or "").strip()
    strategy_name = (
        str(runtime_cfg.get("strategy_name") or _AUTONOMOUS_AGENT_STRATEGY).strip() or _AUTONOMOUS_AGENT_STRATEGY
    )
    try:
        context = resolve_runtime_research_context(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            strategy_name=strategy_name,
        )
    except Exception as exc:
        logger.debug(f"autonomous-agent eligibility summary unavailable: {exc}")
        context = {}

    context_payload = dict(context or {})
    contract = (
        dict(context_payload.get("eligibility_contract") or {})
        if isinstance(context_payload.get("eligibility_contract"), dict)
        else {}
    )
    selected = (
        dict(context_payload.get("selected_eligibility") or {})
        if isinstance(context_payload.get("selected_eligibility"), dict)
        else {}
    )
    generated_at = (
        context_payload.get("snapshot_generated_at")
        or contract.get("generated_at")
        or selected.get("generated_at")
    )
    refresh_age_sec: Optional[float] = None
    generated_dt = _review_safe_dt(generated_at)
    if generated_dt is not None:
        refresh_age_sec = max(0.0, (datetime.now(timezone.utc) - generated_dt).total_seconds())

    reason_codes = [
        str(item).strip()
        for item in list(context_payload.get("reason_codes") or [])
        if str(item or "").strip()
    ]
    selected_reason_codes = [
        str(item).strip()
        for item in list(selected.get("reason_codes") or [])
        if str(item or "").strip()
    ]
    return {
        "available": bool(context_payload.get("available")),
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe or None,
        "strategy_name": strategy_name,
        "candidate_count": int(context_payload.get("candidate_count") or 0),
        "selection_reason": str(context_payload.get("selection_reason") or "").strip() or None,
        "data_source": str(context_payload.get("data_source") or contract.get("source") or "").strip() or None,
        "generated_at": generated_at,
        "refresh_age_sec": round(float(refresh_age_sec), 3) if refresh_age_sec is not None else None,
        "reason_codes": reason_codes,
        "snapshot_path": str(context_payload.get("snapshot_path") or "").strip() or None,
        "contract": {
            "schema_version": str(contract.get("schema_version") or "").strip() or None,
            "source": str(contract.get("source") or "").strip() or None,
            "generated_at": contract.get("generated_at"),
        },
        "selected": {
            "candidate_id": str(selected.get("candidate_id") or "").strip() or None,
            "proposal_id": str(selected.get("proposal_id") or "").strip() or None,
            "strategy": str(selected.get("strategy") or "").strip() or None,
            "status": str(selected.get("status") or "").strip() or None,
            "promotion_target": str(selected.get("promotion_target") or "").strip() or None,
            "runtime_mode_cap": str(selected.get("runtime_mode_cap") or "").strip() or None,
            "eligible_for_autonomy": bool(selected.get("eligible_for_autonomy")),
            "is_expired": bool(selected.get("is_expired")),
            "expires_at": selected.get("expires_at"),
            "reason_codes": selected_reason_codes,
        },
    }


def _build_autonomous_agent_risk_view(risk_report: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(risk_report or {})
    risk_equity = dict(payload.get("equity") or {})
    risk_discipline = dict(payload.get("discipline") or {})
    risk_drawdown = dict(payload.get("drawdown") or {})
    rolling_3d = dict(risk_drawdown.get("rolling_3d") or {})
    rolling_7d = dict(risk_drawdown.get("rolling_7d") or {})

    discipline_view = {
        "fresh_entry_allowed": bool(risk_discipline.get("fresh_entry_allowed", True)),
        "reduce_only": bool(risk_discipline.get("reduce_only", False)),
        "degrade_mode": str(risk_discipline.get("degrade_mode") or "").strip() or None,
        "reasons": [
            str(item).strip()
            for item in list(risk_discipline.get("reasons") or [])
            if str(item or "").strip()
        ],
        "thresholds": dict(risk_discipline.get("thresholds") or {})
        if isinstance(risk_discipline.get("thresholds"), dict)
        else {},
    }
    return {
        "trading_halted": bool(
            payload.get("trading_halted")
            if payload.get("trading_halted") is not None
            else payload.get("risk_trading_halted")
        ),
        "halt_reason": str(
            payload.get("halt_reason")
            or payload.get("risk_halt_reason")
            or ""
        ).strip() or None,
        "risk_level": str(payload.get("risk_level") or "").strip() or None,
        "daily_pnl_ratio": _review_safe_float(
            payload.get("daily_pnl_ratio", risk_equity.get("daily_pnl_ratio")),
            _review_safe_float(payload.get("risk_daily_pnl_ratio")),
        ),
        "daily_stop_basis_ratio": _review_safe_float(
            payload.get("daily_stop_basis_ratio", risk_equity.get("daily_stop_basis_ratio")),
            _review_safe_float(payload.get("risk_daily_stop_basis_ratio")),
        ),
        "max_drawdown": _review_safe_float(
            payload.get("max_drawdown", risk_equity.get("max_drawdown", risk_drawdown.get("max_drawdown"))),
            _review_safe_float(payload.get("risk_max_drawdown")),
        ),
        "rolling_3d_drawdown": _review_safe_float(
            risk_equity.get("rolling_3d_drawdown", rolling_3d.get("drawdown")),
            0.0,
        ),
        "rolling_7d_drawdown": _review_safe_float(
            risk_equity.get("rolling_7d_drawdown", rolling_7d.get("drawdown")),
            0.0,
        ),
        "fresh_entry_allowed": bool(discipline_view.get("fresh_entry_allowed")),
        "reduce_only": bool(discipline_view.get("reduce_only")),
        "degrade_mode": discipline_view.get("degrade_mode"),
        "reasons": list(discipline_view.get("reasons") or []),
        "thresholds": dict(discipline_view.get("thresholds") or {}),
        "discipline": discipline_view,
        "equity": {
            "current": _review_safe_float(risk_equity.get("current")),
            "day_start": _review_safe_float(risk_equity.get("day_start")),
            "daily_total_pnl_usd": _review_safe_float(risk_equity.get("daily_total_pnl_usd")),
            "daily_realized_pnl_usd": _review_safe_float(risk_equity.get("daily_realized_pnl_usd")),
            "current_unrealized_pnl_usd": _review_safe_float(risk_equity.get("current_unrealized_pnl_usd")),
        },
        "drawdown": {
            "max_drawdown": _review_safe_float(risk_drawdown.get("max_drawdown"), _review_safe_float(risk_equity.get("max_drawdown"))),
            "rolling_3d": rolling_3d,
            "rolling_7d": rolling_7d,
        },
    }


def _build_autonomous_agent_risk_config() -> Dict[str, Any]:
    risk_report = _get_autonomous_agent_risk_report()
    risk_view = _build_autonomous_agent_risk_view(risk_report)
    limits = dict(risk_report.get("limits") or {}) if isinstance(risk_report.get("limits"), dict) else {}
    try:
        from core.risk.risk_manager import risk_manager  # noqa: PLC0415

        config = risk_manager.get_autonomy_risk_config()
    except Exception as exc:
        logger.debug(f"autonomous-agent risk config unavailable: {exc}")
        config = {}

    if not config:
        for key in (
            "autonomy_daily_stop_buffer_ratio",
            "autonomy_max_drawdown_reduce_only",
            "autonomy_rolling_3d_drawdown_reduce_only",
            "autonomy_rolling_7d_drawdown_reduce_only",
        ):
            if key in limits:
                config[key] = limits.get(key)

    return {
        "config": config,
        "effective_thresholds": dict((risk_view.get("discipline") or {}).get("thresholds") or {}),
        "base_limits": limits,
        "risk": risk_view,
        "updated_at": risk_report.get("timestamp"),
    }


def _build_autonomous_agent_execution_gate(
    *,
    runtime_config: Dict[str, Any],
    agent_status: Dict[str, Any],
) -> Dict[str, Any]:
    cfg = dict(runtime_config or {})
    status = dict(agent_status or {})
    trading_mode = _current_trading_mode()
    agent_mode = str(cfg.get("mode") or "shadow").strip().lower() or "shadow"
    symbol_mode = str(cfg.get("symbol_mode") or "").strip().lower() or None
    enabled = bool(cfg.get("enabled"))
    running = bool(status.get("running"))
    allow_live = bool(cfg.get("allow_live"))
    armed_for_execution = agent_mode == "execute"

    safety_payload = _build_autonomous_agent_paper_longrun_safety(
        runtime_cfg=cfg,
        status_payload=status,
        trading_mode=trading_mode,
    )
    safety_candidates = [
        safety_payload,
        status.get("safety"),
        cfg.get("safety"),
        status.get("paper_longrun_safety"),
        cfg.get("paper_longrun_safety"),
    ]
    reason_codes: List[str] = []
    recommendations: List[str] = []
    raw_provider_live_restricted = False
    for candidate in safety_candidates:
        if not isinstance(candidate, dict):
            continue
        reason_codes.extend(str(item) for item in (candidate.get("reason_codes") or []) if item)
        recommendations.extend(str(item) for item in (candidate.get("recommendations") or []) if item)
        policy = candidate.get("provider_live_policy")
        if isinstance(policy, dict):
            status_text = str(policy.get("status") or "").strip().lower()
            if bool(policy.get("restricted")) or status_text == "restricted":
                raw_provider_live_restricted = True
            if policy.get("reason_code"):
                reason_codes.append(str(policy.get("reason_code")))
    reason_codes = list(dict.fromkeys(code for code in reason_codes if code))
    recommendations = list(dict.fromkeys(item for item in recommendations if item))
    provider_live_restricted = bool(raw_provider_live_restricted and allow_live)

    blocked = False
    code = "execution_gate_open"
    source = "execution_safety"
    tone = "good"
    label = "执行安全门禁允许提交"
    detail = "交易引擎和 agent 执行配置一致；是否开新单继续由风险纪律和学习保护决定。"

    if not enabled:
        blocked = True
        code = "agent_disabled"
        source = "runtime"
        tone = "warn"
        label = "代理未启用"
        detail = "enabled=false，自治代理不会持续决策，也不会自动提交订单。"
        reason_codes = list(dict.fromkeys([*reason_codes, "agent_disabled"]))
    elif not armed_for_execution:
        blocked = True
        code = "mode_not_execute"
        source = "runtime"
        tone = "info"
        label = "当前只提示不执行"
        detail = f"agent mode={agent_mode}，需要 execute 模式才会提交订单。"
        reason_codes = list(dict.fromkeys([*reason_codes, "mode_not_execute"]))
    elif trading_mode == "live" and not allow_live:
        blocked = True
        code = "live_mode_blocked"
        source = "execution_safety"
        tone = "warn"
        label = "实盘安全门禁阻止提交"
        detail = "当前交易引擎是 live，但 AI 自治代理 allow_live=false；这不是风险纪律挡单，而是实盘安全门禁在阻止提交。"
        reason_codes = list(dict.fromkeys([*reason_codes, "trading_mode_live", "live_mode_blocked"]))
        recommendations = list(dict.fromkeys([*recommendations, "switch_trading_mode_to_paper"]))
    elif trading_mode == "live" and provider_live_restricted:
        blocked = True
        code = "provider_live_execution_restricted"
        source = "execution_safety"
        tone = "warn"
        label = "模型供应商不允许实盘执行"
        detail = "当前 provider 的 autonomous_live_execution 策略受限；需要切换到允许实盘执行的 provider，或回到 paper。"
        reason_codes = list(dict.fromkeys([*reason_codes, "provider_live_execution_restricted"]))
    elif isinstance(status.get("circuit_breaker"), dict):
        breaker = status["circuit_breaker"]
        scope_label = "组合" if breaker.get("scope") == "portfolio" else "策略"
        blocked = True
        code = "circuit_breaker_blocked"
        source = "circuit_breaker"
        tone = "danger"
        label = f"{scope_label}熔断：仅允许平仓"
        detail = (
            f"{breaker.get('reason') or 'tripped'}（触发于 {breaker.get('tripped_at') or '--'}）。"
            "新开仓信号会被执行引擎丢弃，熔断不会自动恢复；确认回撤原因后在风控页手动复位。"
        )
        reason_codes = list(dict.fromkeys([*reason_codes, "circuit_breaker_blocked"]))
        recommendations = list(dict.fromkeys([*recommendations, "review_and_reset_circuit_breaker"]))
    elif not running:
        tone = "info"
        label = "提交门禁允许，代理未持续运行"
        detail = "安全门禁未阻止信号提交；当前只是没有启动持续轮询，手动试跑仍会按相同门禁检查。"
        reason_codes = []
        recommendations = []
    else:
        reason_codes = []
        recommendations = []

    return {
        "blocked": bool(blocked),
        "submission_allowed": not bool(blocked),
        "code": code,
        "source": source,
        "tone": tone,
        "label": label,
        "detail": detail,
        "trading_mode": trading_mode,
        "agent_mode": agent_mode,
        "symbol_mode": symbol_mode,
        "enabled": enabled,
        "running": running,
        "allow_live": allow_live,
        "armed_for_execution": armed_for_execution,
        "provider_live_restricted": bool(provider_live_restricted),
        "safe_for_paper_longrun": bool(safety_payload.get("safe_for_paper_longrun")),
        "paper_longrun_profile_ready": bool(safety_payload.get("paper_longrun_profile_ready")),
        "reason_codes": reason_codes,
        "recommendations": recommendations,
    }


def _build_autonomous_agent_risk_status() -> Dict[str, Any]:
    learning_memory = _get_autonomous_agent_learning_memory()
    learning_summary = dict(learning_memory.get("summary") or {}) if isinstance(learning_memory.get("summary"), dict) else {}
    adaptive_risk = dict(learning_memory.get("adaptive_risk") or {}) if isinstance(learning_memory.get("adaptive_risk"), dict) else {}
    risk_report = _get_autonomous_agent_risk_report()
    risk_view = _build_autonomous_agent_risk_view(risk_report)

    runtime_config = autonomous_trading_agent.get_runtime_config()
    try:
        agent_status = autonomous_trading_agent.get_status()
    except Exception as exc:
        logger.debug(f"autonomous-agent risk status get_status failed: {exc}")
        agent_status = {}
    eligibility = _build_autonomous_agent_eligibility_summary(
        runtime_config=runtime_config,
        agent_status=agent_status if isinstance(agent_status, dict) else {},
    )

    blockers: List[Dict[str, Any]] = []
    discipline = dict(risk_view.get("discipline") or {})
    if not bool(discipline.get("fresh_entry_allowed", True)) or bool(risk_view.get("trading_halted")):
        reasons = list(discipline.get("reasons") or [])
        blockers.append(
            {
                "code": str(discipline.get("degrade_mode") or "risk_discipline_active"),
                "source": "risk_discipline",
                "detail": reasons[0] if reasons else (risk_view.get("halt_reason") or "risk discipline active"),
            }
        )

    loss_streak = int(
        learning_summary.get(
            "recent_close_loss_streak_count",
            adaptive_risk.get("recent_close_loss_streak_count"),
        )
        or 0
    )
    if bool(adaptive_risk.get("avoid_new_entries_during_service_instability")):
        blockers.append(
            {
                "code": "learning_service_instability_guard",
                "source": "learning_memory",
                "detail": "avoid_new_entries_during_service_instability",
            }
        )
    if bool(adaptive_risk.get("avoid_new_entries_during_loss_streak")):
        blockers.append(
            {
                "code": "learning_loss_streak_guard",
                "source": "learning_memory",
                "detail": f"recent_close_loss_streak_count={loss_streak}",
            }
        )

    effective_fresh_entry_allowed = bool(discipline.get("fresh_entry_allowed", True)) and not any(
        str(item.get("source") or "") == "learning_memory" for item in blockers
    )
    if bool(risk_view.get("trading_halted")):
        effective_fresh_entry_allowed = False

    close_only_effective = bool(
        risk_view.get("trading_halted")
        or ((risk_view.get("discipline") or {}).get("reduce_only"))
    )
    execution_gate = _build_autonomous_agent_execution_gate(
        runtime_config=runtime_config,
        agent_status=agent_status if isinstance(agent_status, dict) else {},
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "enabled": bool(runtime_config.get("enabled")),
            "mode": str(runtime_config.get("mode") or "").strip() or None,
            "allow_live": bool(runtime_config.get("allow_live")),
            "symbol_mode": str(runtime_config.get("symbol_mode") or "").strip() or None,
            "status": agent_status if isinstance(agent_status, dict) else {},
        },
        "risk": risk_view,
        "learning": {
            "effective_min_confidence": _review_safe_float(adaptive_risk.get("effective_min_confidence")),
            "avoid_new_entries_during_service_instability": bool(
                adaptive_risk.get("avoid_new_entries_during_service_instability")
            ),
            "avoid_new_entries_during_loss_streak": bool(
                adaptive_risk.get("avoid_new_entries_during_loss_streak")
            ),
            "recent_close_loss_streak_count": loss_streak,
            "lesson_count": len(learning_memory.get("lessons") or [])
            if isinstance(learning_memory.get("lessons"), list)
            else 0,
        },
        "eligibility": eligibility,
        "effective_fresh_entry_allowed": bool(effective_fresh_entry_allowed),
        "fresh_entry_submission_allowed": bool(
            effective_fresh_entry_allowed
            and not close_only_effective
            and bool(execution_gate.get("submission_allowed"))
        ),
        "close_only_effective": close_only_effective,
        "fresh_entry_blockers": blockers,
        "execution_gate": execution_gate,
    }


def _autonomous_scorecard_trade_phase(row: Dict[str, Any]) -> str:
    signal_payload = row.get("signal") if isinstance(row.get("signal"), dict) else {}
    signal_type = str(
        row.get("signal_type")
        or signal_payload.get("signal_type")
        or ""
    ).strip().lower()
    action = str(row.get("action") or "").strip().lower()
    if signal_type in _AUTONOMOUS_EXIT_ACTIONS or action == "close":
        return "close"
    if signal_type in _AUTONOMOUS_ENTRY_ACTIONS or action == "open_or_add":
        return "entry"
    return "other"


def _autonomous_scorecard_row_net_pnl_usd(row: Dict[str, Any]) -> float:
    explicit_net = _review_safe_float(row.get("net_pnl_usd"))
    if explicit_net is not None:
        return float(explicit_net)
    recorded_pnl = float(_review_safe_float(row.get("pnl"), 0.0) or 0.0)
    slippage_cost_usd = float(_review_safe_float(row.get("slippage_cost_usd"), 0.0) or 0.0)
    return recorded_pnl - slippage_cost_usd


def _autonomous_scorecard_row_gross_pnl_usd(row: Dict[str, Any]) -> float:
    explicit_gross = _review_safe_float(row.get("gross_pnl_usd"))
    if explicit_gross is not None:
        return float(explicit_gross)
    explicit_net = _review_safe_float(row.get("net_pnl_usd"))
    fee_usd = float(_review_safe_float(row.get("fee_usd"), 0.0) or 0.0)
    slippage_cost_usd = float(_review_safe_float(row.get("slippage_cost_usd"), 0.0) or 0.0)
    if explicit_net is not None:
        return float(explicit_net) + fee_usd + slippage_cost_usd
    recorded_pnl = float(_review_safe_float(row.get("pnl"), 0.0) or 0.0)
    return recorded_pnl + fee_usd


def _build_autonomous_agent_scorecard(
    limit: int = 200,
    hours: int = 24 * 7,
    *,
    journal_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    trade_limit = max(1, min(int(limit or 200), 2000))
    lookback_hours = max(1, min(int(hours or 24 * 7), 24 * 365))

    live_review = execution_engine.get_live_trade_review(
        limit=trade_limit,
        strategy=_AUTONOMOUS_AGENT_STRATEGY,
        hours=lookback_hours,
    )
    trade_rows = list(live_review.get("items") or []) if isinstance(live_review, dict) else []
    trade_rows = [row for row in trade_rows if isinstance(row, dict)]
    live_review_summary = (
        dict(live_review.get("summary") or {})
        if isinstance(live_review, dict)
        else {}
    )

    review_payload = _build_autonomous_agent_review(
        limit=min(max(trade_limit, 12), 30),
        journal_rows=journal_rows,
    )
    review_summary = (
        dict(review_payload.get("summary") or {})
        if isinstance(review_payload, dict)
        else {}
    )
    review_items = list(review_payload.get("items") or []) if isinstance(review_payload, dict) else []

    learning_memory = _get_autonomous_agent_learning_memory()
    adaptive_risk = (
        dict(learning_memory.get("adaptive_risk") or {})
        if isinstance(learning_memory.get("adaptive_risk"), dict)
        else {}
    )
    risk_report = _get_autonomous_agent_risk_report()
    risk_view = _build_autonomous_agent_risk_view(risk_report)
    runtime_config = autonomous_trading_agent.get_runtime_config()
    try:
        agent_status = autonomous_trading_agent.get_status()
    except Exception as exc:
        logger.debug(f"autonomous-agent scorecard get_status failed: {exc}")
        agent_status = {}
    eligibility = _build_autonomous_agent_eligibility_summary(
        runtime_config=runtime_config,
        agent_status=agent_status if isinstance(agent_status, dict) else {},
    )

    entry_count = 0
    close_count = 0
    gross_pnl_usd = 0.0
    fee_usd_total = 0.0
    slippage_cost_usd_total = 0.0
    net_pnl_usd = 0.0
    close_net_pnls: List[float] = []

    for row in trade_rows:
        fee_usd = float(_review_safe_float(row.get("fee_usd"), 0.0) or 0.0)
        slippage_cost_usd = float(_review_safe_float(row.get("slippage_cost_usd"), 0.0) or 0.0)
        row_gross_pnl = _autonomous_scorecard_row_gross_pnl_usd(row)
        row_net_pnl = _autonomous_scorecard_row_net_pnl_usd(row)

        phase = _autonomous_scorecard_trade_phase(row)
        if phase == "entry":
            entry_count += 1
        elif phase == "close":
            close_count += 1
            close_net_pnls.append(row_net_pnl)

        gross_pnl_usd += row_gross_pnl
        fee_usd_total += fee_usd
        slippage_cost_usd_total += slippage_cost_usd
        net_pnl_usd += row_net_pnl

    avg_holding_minutes: Optional[float] = None
    if _review_safe_float(live_review_summary.get("avg_holding_minutes")) is not None:
        avg_holding_minutes = float(_review_safe_float(live_review_summary.get("avg_holding_minutes"), 0.0) or 0.0)
    holding_values = [
        float(value)
        for value in (
            _review_safe_float((item.get("pair") or {}).get("holding_minutes"))
            for item in review_items
            if isinstance(item, dict) and str(item.get("phase") or "").strip().lower() == "exit"
        )
        if value is not None
    ]
    if avg_holding_minutes is None and holding_values:
        avg_holding_minutes = sum(holding_values) / len(holding_values)

    win_rate: Optional[float] = None
    profit_factor: Optional[float] = None
    if close_net_pnls:
        win_count = sum(1 for value in close_net_pnls if value > 0)
        gross_wins = sum(value for value in close_net_pnls if value > 0)
        gross_losses = abs(sum(value for value in close_net_pnls if value < 0))
        win_rate = win_count / len(close_net_pnls)
        if gross_losses > 1e-12:
            profit_factor = gross_wins / gross_losses

    current_open_unrealized_pnl = _review_safe_float(review_summary.get("current_open_unrealized_pnl"), 0.0)
    submitted_count = int(review_summary.get("submitted_count") or 0)
    review_entry_count = int(review_summary.get("entry_count") or 0)
    review_close_count = int(review_summary.get("close_count") or 0)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy": _AUTONOMOUS_AGENT_STRATEGY,
        "mode": "live",
        "window": {
            "hours": lookback_hours,
            "trade_limit": trade_limit,
            "review_limit": min(max(trade_limit, 12), 30),
        },
        "metrics": {
            "trades": int(live_review_summary.get("trade_count") or len(trade_rows)),
            "entries": int(live_review_summary.get("entry_count") or entry_count),
            "closes": int(live_review_summary.get("close_count") or close_count),
            "gross_pnl_usd": round(gross_pnl_usd, 6),
            "fee_usd": round(fee_usd_total, 6),
            "slippage_cost_usd": round(slippage_cost_usd_total, 6),
            "cost_drag_usd": round(fee_usd_total + slippage_cost_usd_total, 6),
            "net_pnl_usd": round(net_pnl_usd, 6),
            "win_rate": round(win_rate, 6) if win_rate is not None else None,
            "profit_factor": round(profit_factor, 6) if profit_factor is not None else None,
            "avg_holding_minutes": round(avg_holding_minutes, 3) if avg_holding_minutes is not None else None,
            "current_open_unrealized_pnl": round(float(current_open_unrealized_pnl or 0.0), 6),
        },
        "review_summary": {
            "submitted_count": submitted_count,
            "entry_count": review_entry_count,
            "close_count": review_close_count,
            "losing_close_count": int(review_summary.get("losing_close_count") or 0),
            "repeated_same_direction_entries": int(review_summary.get("repeated_same_direction_entries") or 0),
            "outage_after_entry_count": int(review_summary.get("outage_after_entry_count") or 0),
            "unmatched_entry_count": int(review_summary.get("unmatched_entry_count") or 0),
            "current_open_count": int(review_summary.get("current_open_count") or 0),
        },
        "consistency": {
            "live_trade_count": len(trade_rows),
            "review_submitted_count": submitted_count,
            "entry_count_delta": entry_count - review_entry_count,
            "close_count_delta": close_count - review_close_count,
        },
        "learning_memory": learning_memory,
        "learning_summary": {
            "effective_min_confidence": _review_safe_float(adaptive_risk.get("effective_min_confidence")),
            "recent_close_loss_streak_count": int(
                ((learning_memory.get("summary") or {}) if isinstance(learning_memory.get("summary"), dict) else {}).get(
                    "recent_close_loss_streak_count",
                    adaptive_risk.get("recent_close_loss_streak_count"),
                )
                or 0
            ),
            "avoid_new_entries_during_loss_streak": bool(
                adaptive_risk.get("avoid_new_entries_during_loss_streak", False)
            ),
            "lesson_count": len(learning_memory.get("lessons") or [])
            if isinstance(learning_memory.get("lessons"), list)
            else 0,
        },
        "risk": risk_view,
        "eligibility": eligibility,
        "metric_notes": {
            "journal_pnl_basis": "prefer explicit gross_pnl_usd/net_pnl_usd; fallback to legacy pnl semantics when absent",
            "gross_pnl_formula": "sum(gross_pnl_usd) or fallback sum(pnl + fee_usd)",
            "net_pnl_formula": "sum(net_pnl_usd) or fallback sum(pnl - slippage_cost_usd)",
        },
    }

def _current_trading_mode() -> str:
    trading_mode = str(execution_engine.get_trading_mode() or "").strip().lower() or "paper"
    if trading_mode not in {"paper", "live"}:
        fallback = str(getattr(settings, "TRADING_MODE", "paper") or "paper").strip().lower() or "paper"
        trading_mode = fallback if fallback in {"paper", "live"} else "paper"
    return trading_mode


def _build_autonomous_agent_paper_longrun_safety(
    runtime_cfg: Optional[Dict[str, Any]] = None,
    status_payload: Optional[Dict[str, Any]] = None,
    trading_mode: Optional[str] = None,
) -> Dict[str, Any]:
    cfg = dict(runtime_cfg or {})
    status = dict(status_payload or {})

    candidates = [
        status.get("paper_longrun_safety"),
        cfg.get("paper_longrun_safety"),
        status.get("runtime_safety"),
        cfg.get("runtime_safety"),
        status.get("safety"),
        cfg.get("safety"),
        status.get("diagnostics"),
        cfg.get("diagnostics"),
    ]
    merged_payload: Dict[str, Any] = {}
    matched_candidate = False
    for candidate in reversed(candidates):
        if isinstance(candidate, dict) and (
            "safe_for_paper_longrun" in candidate
            or "paper_longrun_profile_ready" in candidate
            or "reason_codes" in candidate
        ):
            merged_payload.update(dict(candidate))
            matched_candidate = True
    if matched_candidate:
        if not isinstance(merged_payload.get("reason_codes"), list):
            merged_payload["reason_codes"] = []
        merged_payload["trading_mode"] = str(trading_mode or _current_trading_mode())
        merged_payload.setdefault("safe_for_paper_longrun", False)
        merged_payload.setdefault("paper_longrun_profile_ready", False)
        merged_payload.setdefault("armed_for_execution", False)
        merged_payload.setdefault("live_capable", False)
        merged_payload.setdefault("allow_live", False)
        return merged_payload

    trading_mode_value = str(trading_mode or _current_trading_mode())
    mode = str(cfg.get("mode") or "").strip().lower()
    symbol_mode = str(cfg.get("symbol_mode") or "").strip().lower()
    allow_live = bool(cfg.get("allow_live", False))
    armed_for_execution = mode == "execute"
    profile_ready = armed_for_execution and symbol_mode == "auto" and (not allow_live)
    safe_for_paper_longrun = trading_mode_value == "paper" and profile_ready
    reason_codes: List[str] = []
    if trading_mode_value != "paper":
        reason_codes.append("trading_mode_not_paper")
    if mode != "execute":
        reason_codes.append("mode_not_execute")
    if symbol_mode != "auto":
        reason_codes.append("symbol_mode_not_auto")
    if allow_live:
        reason_codes.append("allow_live_enabled")
    return {
        "safe_for_paper_longrun": bool(safe_for_paper_longrun),
        "paper_longrun_profile_ready": bool(profile_ready),
        "armed_for_execution": bool(armed_for_execution),
        "live_capable": bool(allow_live or trading_mode_value == "live"),
        "allow_live": bool(allow_live),
        "trading_mode": trading_mode_value,
        "reason_codes": reason_codes,
    }


@router.get("/runtime-config")
async def get_ai_runtime_config(request: Request):
    """Expose lightweight runtime switches for the AI research UI."""
    ensure_ai_research_runtime_state(request.app)
    trading_mode = _current_trading_mode()
    agent_cfg = autonomous_trading_agent.get_runtime_config()
    if isinstance(agent_cfg, dict):
        agent_cfg = dict(agent_cfg)
    else:
        agent_cfg = {}
    agent_cfg["paper_longrun_safety"] = _build_autonomous_agent_paper_longrun_safety(
        runtime_cfg=agent_cfg,
        trading_mode=trading_mode,
    )
    agent_cfg["trading_mode"] = trading_mode
    return {
        "governance_enabled": bool(getattr(settings, "GOVERNANCE_ENABLED", True)),
        "decision_mode": str(getattr(settings, "DECISION_MODE", "shadow") or "shadow"),
        "trading_mode": trading_mode,
        "ai_live_decision": live_decision_router.get_runtime_config(),
        "ai_autonomous_agent": agent_cfg,
    }


@router.get("/runtime-config/live-decision")
async def get_ai_live_decision_runtime_config(request: Request):
    ensure_ai_research_runtime_state(request.app)
    return live_decision_router.get_runtime_config()


@router.post("/runtime-config/live-decision", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def update_ai_live_decision_runtime_config(
    request: Request,
    payload: AILiveDecisionConfigUpdateRequest,
):
    ensure_ai_research_runtime_state(request.app)
    try:
        updated = await live_decision_router.update_runtime_config(**payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": True, "config": updated}


@router.get("/runtime-config/live-decision/summary")
async def get_ai_live_decision_summary(request: Request):
    ensure_ai_research_runtime_state(request.app)
    diagnostics = execution_engine.get_signal_diagnostics()
    block_count = max(0, int(diagnostics.get("ai_rejected", 0) or 0))
    reduce_only_count = max(0, int(diagnostics.get("ai_reduce_only_rejected", 0) or 0))
    bypass_count = max(0, int(diagnostics.get("ai_review_bypassed", 0) or 0))
    last_hit = diagnostics.get("last_ai_review_result")
    if not isinstance(last_hit, dict):
        fallback = diagnostics.get("last_result")
        if isinstance(fallback, dict) and str(fallback.get("status") or "") in {"ai_rejected", "ai_reduce_only_rejected"}:
            last_hit = fallback
        else:
            last_hit = None
    return {
        "hit_count": block_count + reduce_only_count,
        "block_count": block_count,
        "reduce_only_count": reduce_only_count,
        "bypass_count": bypass_count,
        "last_hit": last_hit if isinstance(last_hit, dict) else None,
        "last_updated_at": diagnostics.get("last_updated_at"),
        "scope_note": "仅统计策略库/候选执行链，AI自动交易直连执行，不计入这里。",
    }


async def get_ai_autonomous_agent_runtime_config(request: Request):
    trading_mode = _current_trading_mode()
    cfg = autonomous_trading_agent.get_runtime_config()
    if isinstance(cfg, dict):
        cfg = dict(cfg)
    else:
        cfg = {}
    cfg["paper_longrun_safety"] = _build_autonomous_agent_paper_longrun_safety(
        runtime_cfg=cfg,
        trading_mode=trading_mode,
    )
    cfg["trading_mode"] = trading_mode
    return cfg


async def update_ai_autonomous_agent_runtime_config(
    request: Request,
    payload: AIAutonomousAgentConfigUpdateRequest,
):
    try:
        updated = await autonomous_trading_agent.update_runtime_config(**payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": True, "config": updated}


async def get_ai_autonomous_agent_risk_config(request: Request):
    return _build_autonomous_agent_risk_config()


async def update_ai_autonomous_agent_risk_config(
    request: Request,
    payload: AIAutonomousAgentRiskConfigUpdateRequest,
):
    try:
        from core.risk.risk_manager import risk_manager  # noqa: PLC0415

        risk_manager.update_parameters(payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": True, "config": _build_autonomous_agent_risk_config()}


async def get_ai_autonomous_agent_status(request: Request, warm_preview: bool = False):
    trading_mode = _current_trading_mode()
    cfg = autonomous_trading_agent.get_runtime_config()
    if isinstance(cfg, dict):
        cfg = dict(cfg)
    else:
        cfg = {}
    if warm_preview and str(cfg.get("symbol_mode") or "manual").strip().lower() == "auto":
        autonomous_trading_agent.ensure_symbol_scan_preview_warm(
            limit=int(cfg.get("selection_top_n") or 10),
            force=False,
        )
    status_payload = autonomous_trading_agent.get_status()
    if isinstance(status_payload, dict):
        status_payload = dict(status_payload)
    else:
        status_payload = {}
    safety_payload = _build_autonomous_agent_paper_longrun_safety(
        runtime_cfg=cfg,
        status_payload=status_payload,
        trading_mode=trading_mode,
    )
    cfg["paper_longrun_safety"] = safety_payload
    cfg["trading_mode"] = trading_mode
    status_payload["trading_mode"] = trading_mode
    return {
        "status": status_payload,
        "config": cfg,
        "trading_mode": trading_mode,
        "paper_longrun_safety": safety_payload,
    }


async def start_ai_autonomous_agent(
    request: Request,
    payload: AIAutonomousAgentStartRequest = AIAutonomousAgentStartRequest(),
):
    trading_mode = _current_trading_mode()
    update_kwargs: Dict[str, Any] = {}
    if payload.enable:
        update_kwargs["enabled"] = True
    if payload.runtime_profile is not None:
        update_kwargs["runtime_profile"] = payload.runtime_profile
    elif payload.profile is not None:
        update_kwargs["profile"] = payload.profile
    if update_kwargs:
        try:
            await autonomous_trading_agent.update_runtime_config(**update_kwargs)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    status = await autonomous_trading_agent.start()
    cfg = autonomous_trading_agent.get_runtime_config()
    if isinstance(cfg, dict):
        cfg = dict(cfg)
    else:
        cfg = {}
    safety_payload = _build_autonomous_agent_paper_longrun_safety(
        runtime_cfg=cfg,
        status_payload=status if isinstance(status, dict) else {},
        trading_mode=trading_mode,
    )
    cfg["paper_longrun_safety"] = safety_payload
    cfg["trading_mode"] = trading_mode
    if isinstance(status, dict):
        status["trading_mode"] = trading_mode
    return {
        "started": True,
        "status": status,
        "config": cfg,
        "trading_mode": trading_mode,
        "paper_longrun_safety": safety_payload,
    }


async def stop_ai_autonomous_agent(request: Request):
    trading_mode = _current_trading_mode()
    status = await autonomous_trading_agent.stop()
    cfg = autonomous_trading_agent.get_runtime_config()
    if isinstance(cfg, dict):
        cfg = dict(cfg)
    else:
        cfg = {}
    safety_payload = _build_autonomous_agent_paper_longrun_safety(
        runtime_cfg=cfg,
        status_payload=status if isinstance(status, dict) else {},
        trading_mode=trading_mode,
    )
    cfg["paper_longrun_safety"] = safety_payload
    cfg["trading_mode"] = trading_mode
    if isinstance(status, dict):
        status["trading_mode"] = trading_mode
    return {
        "stopped": True,
        "status": status,
        "config": cfg,
        "trading_mode": trading_mode,
        "paper_longrun_safety": safety_payload,
    }


async def run_ai_autonomous_agent_once(
    request: Request,
    payload: AIAutonomousAgentRunOnceRequest = AIAutonomousAgentRunOnceRequest(),
):
    return await autonomous_trading_agent.trigger_run_once(
        trigger="api_manual",
        force=bool(payload.force),
    )


async def get_ai_autonomous_agent_journal(request: Request, limit: int = 50):
    rows = autonomous_trading_agent.read_journal(limit=limit)
    return {"items": rows, "count": len(rows)}


async def get_ai_autonomous_agent_review(request: Request, limit: int = 12):
    journal_rows = await asyncio.to_thread(_load_autonomous_agent_review_journal)
    payload = _build_autonomous_agent_review(limit=limit, journal_rows=journal_rows)
    payload["learning_memory"] = _get_autonomous_agent_learning_memory()
    return payload


async def get_ai_autonomous_agent_scorecard(request: Request, limit: int = 200, hours: int = 24 * 7):
    journal_rows = await asyncio.to_thread(_load_autonomous_agent_review_journal)
    return _build_autonomous_agent_scorecard(limit=limit, hours=hours, journal_rows=journal_rows)


async def get_ai_autonomous_agent_risk_status(request: Request):
    return _build_autonomous_agent_risk_status()


async def get_ai_autonomous_agent_symbol_ranking(request: Request, limit: int = 10, refresh: bool = False):
    try:
        payload = await asyncio.wait_for(
            asyncio.shield(autonomous_trading_agent.get_symbol_scan_preview(limit=limit, force=bool(refresh))),
            timeout=4.0 if bool(refresh) else 2.0,
        )
        return payload
    except Exception as exc:
        fallback = autonomous_trading_agent.get_symbol_scan_preview_snapshot(limit=limit)
        if fallback is not None:
            meta = dict(fallback.get("scan_meta") or {})
            meta["fallback_reason"] = str(exc)
            meta["fallback_used"] = True
            fallback["scan_meta"] = meta
            return fallback
        autonomous_trading_agent.ensure_symbol_scan_preview_warm(limit=limit, force=False)
        return autonomous_trading_agent.build_symbol_scan_preview_pending_payload(limit=limit, reason=str(exc))


@router.post("/proposals/generate", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def generate_ai_proposal(request: Request, payload: AIPlannerGenerateRequest):
    ensure_ai_research_runtime_state(request.app)
    try:
        result = generate_planned_proposal(
            request.app,
            actor="web_ui",
            goal=payload.goal,
            market_regime=payload.market_regime,
            symbols=payload.symbols,
            timeframes=payload.timeframes,
            constraints=payload.constraints,
            metadata=payload.metadata,
            origin_context=payload.origin_context,
            market_context=payload.market_context,
            llm_research_output=payload.llm_research_output,
        )
    except ValueError as exc:
        await write_audit(
            GovernanceAuditEvent(
                module="ai.research",
                action="planner_generate_rejected",
                status="denied",
                actor="web_ui",
                role="SYSTEM",
                input_payload={"goal": payload.goal, "llm_research_output": payload.llm_research_output},
                output_payload={"error": str(exc)},
            )
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "proposal": _serialize_proposal(request, result["proposal"]),
        "planner_notes": result["planner_notes"],
        "filtered_templates": result.get("filtered_templates", []),
        "filtered_reasons": result.get("filtered_reasons", {}),
    }


@router.post("/proposals", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def create_ai_proposal(request: Request, payload: AIProposalCreateRequest):
    ensure_ai_research_runtime_state(request.app)
    proposal = create_manual_proposal(
        request.app,
        actor="web_ui",
        thesis=payload.thesis,
        symbols=payload.symbols,
        timeframes=payload.timeframes,
        market_regime=payload.market_regime,
        strategy_templates=payload.strategy_templates,
        source=payload.source,
        expected_holding_period=payload.expected_holding_period,
        risk_hypothesis=payload.risk_hypothesis,
        invalidation_rules=payload.invalidation_rules,
        required_features=payload.required_features,
        parameter_space=payload.parameter_space,
        notes=payload.notes,
        metadata=payload.metadata,
    )
    return {"proposal": _serialize_proposal(request, proposal)}


@router.get("/proposals")
async def get_ai_proposals(request: Request, limit: int = 20):
    ensure_ai_research_runtime_state(request.app)
    items = list_proposals(request.app, limit=limit)
    return {"items": [_serialize_proposal(request, item) for item in items], "count": len(items)}


@router.get("/proposals/{proposal_id}")
async def get_ai_proposal(request: Request, proposal_id: str):
    ensure_ai_research_runtime_state(request.app)
    item = get_proposal(request.app, proposal_id)
    return {"proposal": _serialize_proposal(request, item)}


@router.delete("/proposals/{proposal_id}", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def delete_ai_proposal(request: Request, proposal_id: str):
    ensure_ai_research_runtime_state(request.app)
    result = delete_proposal(
        request.app,
        proposal_id=proposal_id,
        actor="web_ui",
    )
    return result


@router.post("/proposals/{proposal_id}/retire", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def retire_ai_proposal(request: Request, proposal_id: str, payload: AIRetireRequest = AIRetireRequest()):
    ensure_ai_research_runtime_state(request.app)
    proposal = get_proposal(request.app, proposal_id)
    proposal_status = str(proposal.status or "")
    if proposal_status in {"research_queued", "research_running", "paper_running", "live_running"}:
        raise HTTPException(status_code=409, detail=f"proposal in state {proposal_status}, retire is not allowed")

    retired_candidates = 0
    all_candidates = request.app.state.ai_candidate_registry.list(limit=None)
    for cand in all_candidates:
        if str(cand.proposal_id) != str(proposal_id):
            continue
        status = str(cand.status or "")
        if status in {"paper_running", "live_running"}:
            raise HTTPException(status_code=409, detail=f"{status} candidate must be stopped before retire")
        if status != "retired":
            transition_candidate(
                cand,
                to_state="retired",
                lifecycle_registry=request.app.state.ai_lifecycle_registry,
                actor="web_ui",
                reason=payload.notes or "retired manually from AI research queue",
                metadata={"source": "ai_research_queue"},
            )
            cand.promotion_target = None
            cand.metadata["retired_manually_at"] = datetime.now(timezone.utc).isoformat()
            cand.metadata["retired_manually_reason"] = payload.notes
            request.app.state.ai_candidate_registry.save(cand)
            retired_candidates += 1

    if proposal_status != "retired":
        transition_proposal(
            proposal,
            to_state="retired",
            lifecycle_registry=request.app.state.ai_lifecycle_registry,
            actor="web_ui",
            reason=payload.notes or "retired manually from AI research queue",
            metadata={"retired_candidates": retired_candidates},
        )
        proposal.metadata["retired_manually_at"] = datetime.now(timezone.utc).isoformat()
        proposal.metadata["retired_manually_reason"] = payload.notes
        save_proposal(request.app, proposal)

    return {
        "proposal_id": proposal_id,
        "status": "retired",
        "retired_candidates": retired_candidates,
        "proposal": _serialize_proposal(request, proposal),
    }


@router.post("/proposals/{proposal_id}/exit", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def exit_ai_proposal(request: Request, proposal_id: str, payload: AIRetireRequest = AIRetireRequest()):
    ensure_ai_research_runtime_state(request.app)
    proposal = get_proposal(request.app, proposal_id)
    proposal_status = str(proposal.status or "")
    if proposal_status in PROPOSAL_CANCEL_ONLY_STATUSES:
        raise HTTPException(status_code=409, detail=f"proposal in state {proposal_status}, cancel it before exit")
    if proposal_status not in PROPOSAL_EXITABLE_STATUSES and proposal_status != "retired":
        raise HTTPException(status_code=409, detail=f"proposal in state {proposal_status}, exit is not allowed")

    notes = payload.notes or "exited manually from AI research queue"
    candidate_results: List[Dict[str, Any]] = []
    all_candidates = request.app.state.ai_candidate_registry.list(limit=None)
    for cand in all_candidates:
        if str(getattr(cand, "proposal_id", "") or "") != str(proposal_id):
            continue
        if str(getattr(cand, "status", "") or "") == "retired":
            continue
        candidate_results.append(
            await _exit_candidate_record(
                request.app,
                cand,
                notes=notes,
                actor="web_ui",
            )
        )

    if proposal_status != "retired":
        transition_proposal(
            proposal,
            to_state="retired",
            lifecycle_registry=request.app.state.ai_lifecycle_registry,
            actor="web_ui",
            reason=notes,
            metadata={
                "retired_candidates": len(candidate_results),
                "stopped_strategies": [
                    item["strategy_name"]
                    for item in candidate_results
                    if item.get("strategy_stopped") and item.get("strategy_name")
                ],
            },
        )
    proposal_meta = _ensure_proposal_metadata(proposal)
    proposal_meta["retired_manually_at"] = datetime.now(timezone.utc).isoformat()
    proposal_meta["retired_manually_reason"] = notes
    proposal_meta["runtime_exit_candidates"] = [item["candidate_id"] for item in candidate_results if item.get("candidate_id")]
    save_proposal(request.app, proposal)

    return {
        "proposal_id": proposal_id,
        "status": "retired",
        "retired_candidates": len(candidate_results),
        "stopped_strategies": [
            item["strategy_name"]
            for item in candidate_results
            if item.get("strategy_stopped") and item.get("strategy_name")
        ],
        "proposal": _serialize_proposal(request, proposal),
    }


@router.post("/proposals/{proposal_id}/run", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def run_ai_proposal_endpoint(request: Request, proposal_id: str, payload: AIProposalRunRequest):
    ensure_ai_research_runtime_state(request.app)
    try:
        result = await run_proposal(
            request.app,
            proposal_id=proposal_id,
            actor="web_ui",
            exchange=payload.exchange,
            symbol=payload.symbol,
            days=payload.days,
            commission_rate=payload.commission_rate,
            slippage_bps=payload.slippage_bps,
            initial_capital=payload.initial_capital,
            background=payload.background,
            timeframes=payload.timeframes,
            strategies=payload.strategies,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if payload.background:
        return {
            "job": result["job"],
            "proposal": _serialize_proposal(request, result["proposal"]),
            "experiment": result["experiment"].model_dump(mode="json"),
            "run": result["run"].model_dump(mode="json"),
        }
    return {
        "proposal": _serialize_proposal(request, result["proposal"]),
        "experiment": result["experiment"].model_dump(mode="json"),
        "run": result["run"].model_dump(mode="json"),
        "candidate": result["candidate"].model_dump(mode="json") if result.get("candidate") else None,
        "promotion": result["promotion"].model_dump(mode="json") if result.get("promotion") else None,
        "research_result": result["research_result"],
    }


@router.post("/oneclick/research-deploy", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def oneclick_ai_research_deploy(request: Request, payload: AIOneClickResearchDeployRequest):
    """One-click orchestration stage 1: generate -> queue research job."""
    ensure_ai_research_runtime_state(request.app)

    generated = generate_planned_proposal(
        request.app,
        actor="web_ui_oneclick",
        goal=payload.goal,
        market_regime=payload.market_regime,
        symbols=payload.symbols,
        timeframes=payload.timeframes,
        constraints=payload.constraints,
        metadata=payload.metadata,
        origin_context=payload.origin_context,
        market_context=payload.market_context,
        llm_research_output=payload.llm_research_output,
    )
    proposal = generated["proposal"]
    generated_payload = _oneclick_generated_payload(request, proposal, generated)

    try:
        run_result = await run_proposal(
            request.app,
            proposal_id=str(proposal.proposal_id),
            actor="web_ui_oneclick",
            exchange=payload.exchange,
            symbol=payload.symbol,
            days=payload.days,
            commission_rate=payload.commission_rate,
            slippage_bps=payload.slippage_bps,
            initial_capital=payload.initial_capital,
            background=True,
            timeframes=payload.timeframes,
            strategies=payload.strategies,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException as exc:
        if exc.status_code != 409:
            raise
        existing_job = _proposal_job_summary(
            request,
            proposal_id=str(proposal.proposal_id),
            preferred_job_id=str((proposal.metadata or {}).get("last_research_job_id") or ""),
        ) or {}
        return {
            "proposal_id": str(proposal.proposal_id),
            "job_id": existing_job.get("job_id"),
            "job": existing_job or None,
            "status": "already_running",
            "generated": generated_payload,
            "outcome": "queued",
            "message": "该提案已有研究任务在运行，请等待完成后查看候选结果。",
        }

    job = dict(run_result.get("job") or {})
    return {
        "proposal_id": str(proposal.proposal_id),
        "job_id": job.get("job_id"),
        "job": job or None,
        "status": str(job.get("status") or "queued"),
        "generated": generated_payload,
        "outcome": "queued",
    }


@router.post("/oneclick/deploy-candidate", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def oneclick_deploy_candidate(request: Request, payload: AIOneClickDeployRequest):
    """One-click orchestration stage 2: deploy a completed research candidate."""
    result = await _execute_oneclick_candidate_deploy(request, payload=payload)
    return result


@router.get("/diagnostics/funding-cache")
async def get_ai_funding_cache_diagnostics(
    request: Request,
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    days: int = 60,
):
    ensure_ai_research_runtime_state(request.app)
    exchange_norm = _normalize_exchange(exchange)
    symbol_norm = _normalize_symbol(symbol)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=max(1, min(int(days or 60), 3650)))
    provider = FundingRateProvider(FundingProviderConfig(exchange=exchange_norm, source="local"))
    series = pd.Series(dtype=float)
    cache_error = None
    try:
        series = provider.load_local_cache(symbol_norm, exchange=exchange_norm)
        series = provider.get_series(symbol_norm, start_time=start_time, end_time=end_time)
    except Exception as exc:
        cache_error = str(exc)
    return {
        "funding": {
            **_serialize_funding_cache(provider, exchange=exchange_norm, symbol=symbol_norm, series=series),
            "requested_days": int(days),
            "source": "local_or_coinglass_cache",
            "error": cache_error,
        },
        "how_to_enable": [
            "CoinGlass funding_rate_history is the preferred funding source.",
            "Local cache files are stored under data/funding/<exchange>/<symbol>_funding.parquet.",
            "Exchange HTTP funding is used only when explicitly requested as binance_http or exchange_http.",
        ],
    }


@router.post("/diagnostics/funding-cache/warm", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def warm_ai_funding_cache(request: Request, payload: AIFundingWarmRequest):
    ensure_ai_research_runtime_state(request.app)
    exchange_norm = _normalize_exchange(payload.exchange)
    symbol_norm = _normalize_symbol(payload.symbol)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=max(1, min(int(payload.days or 60), 3650)))
    requested_source = str(payload.source or "coinglass")
    provider = FundingRateProvider(FundingProviderConfig(exchange=exchange_norm, source=requested_source))
    try:
        series = await asyncio.to_thread(
            provider.ensure_history,
            symbol_norm,
            start_time=start_time,
            end_time=end_time,
            source=requested_source,
            save=True,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"funding warm failed: {exc}") from exc
    funding = _serialize_funding_cache(provider, exchange=exchange_norm, symbol=symbol_norm, series=series)
    return {
        "warmed": bool(funding.get("ready")),
        "funding": {
            **funding,
            "requested_days": int(payload.days),
            "source": requested_source,
        },
    }


@router.post("/diagnostics/macro-cache/warm", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def warm_ai_macro_cache(request: Request):
    ensure_ai_research_runtime_state(request.app)
    try:
        from pathlib import Path as _Path  # noqa: PLC0415

        from core.data.macro_collector import group_macro_snapshot, load_macro_snapshot, update_macro_cache  # noqa: PLC0415

        updated = await update_macro_cache()
        snapshot = dict(load_macro_snapshot() or {})
        active_series = [name for name, value in snapshot.items() if value is not None]
        latest_mtime = None
        for name in active_series or list(snapshot.keys()):
            try:
                mtime = (_Path("data/macro") / f"{name}.parquet").stat().st_mtime
            except Exception:
                continue
            if latest_mtime is None or mtime > latest_mtime:
                latest_mtime = mtime
        return {
            "warmed": True,
            "macro": {
                "updated": dict(updated or {}),
                "updated_count": int(sum(int(value or 0) for value in (updated or {}).values())),
                "active_series": active_series,
                "active_series_count": len(active_series),
                "regions": group_macro_snapshot(snapshot),
                "snapshot": snapshot,
                "last_updated": datetime.fromtimestamp(latest_mtime, tz=timezone.utc).isoformat() if latest_mtime is not None else None,
            },
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"macro warm failed: {exc}") from exc


@router.post("/proposals/{proposal_id}/cancel", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def cancel_ai_proposal_job(request: Request, proposal_id: str):
    ensure_ai_research_runtime_state(request.app)
    result = await cancel_proposal_job(
        request.app,
        proposal_id=proposal_id,
        actor="web_ui",
        reason="research cancelled by user",
    )
    return result


@router.get("/proposals/{proposal_id}/job-status")
async def get_ai_proposal_job_status(request: Request, proposal_id: str):
    """Poll the background research job status for a proposal."""
    ensure_ai_research_runtime_state(request.app)
    item = get_proposal(request.app, proposal_id)
    job_id = item.metadata.get("last_research_job_id")
    job: Dict[str, Any] = {}
    if job_id:
        raw = request.app.state.research_jobs.get(job_id) or {}
        job = {
            "job_id": raw.get("job_id"),
            "proposal_id": raw.get("proposal_id"),
            "experiment_id": raw.get("experiment_id"),
            "run_id": raw.get("run_id"),
            "status": raw.get("status"),
            "created_at": raw.get("created_at"),
            "started_at": raw.get("started_at"),
            "finished_at": raw.get("finished_at"),
            "error": raw.get("error"),
            "progress": raw.get("progress"),
            "result": _serialize_research_job_result(raw.get("result")),
        }
    proposal_reason = None
    if str(item.status) == "rejected":
        proposal_reason = _extract_first_validation_reason(item)
    return {
        "proposal_id": proposal_id,
        "proposal_status": item.status,
        "proposal_reason": proposal_reason,
        "job_id": job_id,
        "job": job or None,
        "job_status": job.get("status"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "error": job.get("error"),
    }


@router.get("/proposals/{proposal_id}/lifecycle")
async def get_ai_proposal_lifecycle(request: Request, proposal_id: str, limit: int = 200):
    ensure_ai_research_runtime_state(request.app)
    _ = get_proposal(request.app, proposal_id)
    rows = list_lifecycle(request.app, "proposal", proposal_id, limit=limit)
    return {"proposal_id": proposal_id, "items": rows, "count": len(rows)}


@router.get("/experiments")
async def get_ai_experiments(request: Request, limit: int = 50):
    ensure_ai_research_runtime_state(request.app)
    rows = list_experiments(request.app, limit=limit)
    return {"items": [row.model_dump(mode="json") for row in rows], "count": len(rows)}


@router.get("/experiments/{experiment_id}")
async def get_ai_experiment(request: Request, experiment_id: str):
    ensure_ai_research_runtime_state(request.app)
    row = get_experiment(request.app, experiment_id)
    return {"experiment": row.model_dump(mode="json")}


@router.get("/experiments/{experiment_id}/runs")
async def get_ai_experiment_runs(request: Request, experiment_id: str, limit: int = 100):
    ensure_ai_research_runtime_state(request.app)
    _ = get_experiment(request.app, experiment_id)
    rows = list_experiment_runs(request.app, experiment_id, limit=limit)
    return {"experiment_id": experiment_id, "items": [row.model_dump(mode="json") for row in rows], "count": len(rows)}


@router.get("/candidates")
async def get_ai_candidates(request: Request, limit: int = 50):
    ensure_ai_research_runtime_state(request.app)
    rows = list_candidates(request.app, limit=limit)
    return {"items": [row.model_dump(mode="json") for row in rows], "count": len(rows)}


@router.get("/reserve-candidates")
async def get_reserve_candidates(request: Request, limit: int = 100):
    ensure_ai_research_runtime_state(request.app)
    rows = list_candidates(request.app, limit=max(1, min(int(limit), 500)))
    reserve = []
    for cand in rows:
        meta = dict(getattr(cand, "metadata", {}) or {})
        validation = getattr(cand, "validation_summary", None)
        reserve_eligible = bool(meta.get("reserve_eligible") or getattr(validation, "reserve_eligible", False))
        outcome_type = str(meta.get("outcome_type") or getattr(validation, "outcome_type", "") or "")
        if reserve_eligible or outcome_type in {"redundant_correlated", "borderline"}:
            payload = cand.model_dump(mode="json")
            payload["reserve_reason"] = outcome_type or "reserve_eligible"
            reserve.append(payload)
    return {"items": reserve, "count": len(reserve)}


@router.get("/work-queue")
async def get_ai_work_queue(request: Request, limit: int = 100):
    ensure_ai_research_runtime_state(request.app)
    items: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()
    candidates = list_candidates(request.app, limit=300)
    proposals = list_proposals(request.app, limit=200)

    for proposal in proposals:
        if str(getattr(proposal, "status", "") or "") == "draft":
            items.append(
                {
                    "type": "research_draft",
                    "id": getattr(proposal, "proposal_id", ""),
                    "title": getattr(proposal, "thesis", "")[:120],
                    "severity": "info",
                    "created_at": getattr(proposal, "created_at", now),
                    "next_action": "run_research",
                    "source": "ai_research",
                }
            )

    for cand in candidates:
        meta = dict(getattr(cand, "metadata", {}) or {})
        validation = getattr(cand, "validation_summary", None)
        payload_base = {
            "id": getattr(cand, "candidate_id", ""),
            "candidate_id": getattr(cand, "candidate_id", ""),
            "title": f"{getattr(cand, 'strategy', '')} {getattr(cand, 'symbol', '')}",
            "created_at": getattr(cand, "created_at", now),
            "source": "ai_research",
            "risk_adjusted_edge": getattr(validation, "risk_adjusted_edge", None),
        }
        if meta.get("promotion_pending_human_gate"):
            items.append({**payload_base, "type": "human_approval", "severity": "warn", "next_action": "human_approve"})
        if str(getattr(cand, "status", "") or "") == "live_candidate":
            items.append({**payload_base, "type": "live_activation", "severity": "warn", "next_action": "activate_live"})
        outcome_type = str(meta.get("outcome_type") or getattr(validation, "outcome_type", "") or "")
        if outcome_type in {"redundant_correlated", "borderline"} or bool(meta.get("reserve_eligible") or getattr(validation, "reserve_eligible", False)):
            items.append({**payload_base, "type": "reserve_candidate", "severity": "info", "next_action": "watch_reserve", "outcome_type": outcome_type})
        if validation is not None and str(getattr(validation, "decision", "") or "") == "reject" and outcome_type not in {"redundant_correlated"}:
            trace = dict(getattr(validation, "decision_trace", {}) or {})
            items.append(
                {
                    **payload_base,
                    "type": "validation_blocked",
                    "severity": "warn",
                    "next_action": "inspect_root_blocker",
                    "root_blocker_code": trace.get("root_blocker_code"),
                    "root_blocker_label": trace.get("root_blocker_label"),
                    "outcome_type": outcome_type,
                }
            )

    try:
        operating = await _get_operating_mode_payload()
        for degradation in operating.get("degradations") or []:
            items.append(
                {
                    "type": "runtime_degradation",
                    "id": degradation.get("code"),
                    "title": degradation.get("label"),
                    "severity": degradation.get("severity") or "warn",
                    "created_at": operating.get("generated_at") or now,
                    "next_action": "inspect_operating_mode",
                    "source": "operating_mode",
                    "detail": degradation.get("detail"),
                }
            )
    except Exception as exc:
        logger.debug(f"work queue operating mode build failed: {exc}")

    try:
        diagnostics = dict((autonomous_trading_agent.get_status() or {}).get("last_diagnostics") or {})
    except Exception:
        diagnostics = {}
    trace = dict(diagnostics.get("decision_trace") or {})
    if trace.get("root_blocker_code"):
        items.append(
            {
                "type": "agent_hold_root_blocker",
                "id": trace.get("trace_id"),
                "title": trace.get("root_blocker_label") or trace.get("root_blocker_code"),
                "severity": "warn",
                "created_at": trace.get("created_at") or now,
                "next_action": "inspect_agent",
                "source": "autonomous_agent",
                "root_blocker_code": trace.get("root_blocker_code"),
            }
        )

    severity_order = {"danger": 0, "warn": 1, "info": 2}
    items = sorted(
        items,
        key=lambda item: (
            severity_order.get(str(item.get("severity") or "info"), 3),
            -float(item.get("risk_adjusted_edge") or 0.0),
            str(item.get("created_at") or ""),
        ),
    )
    return {"items": items[: max(1, min(int(limit), 500))], "count": len(items), "generated_at": now}


@router.get("/candidates/pending-approvals")
async def get_pending_approvals(request: Request):
    """Return all candidates with promotion_pending_human_gate=True."""
    ensure_ai_research_runtime_state(request.app)
    all_candidates = list_candidates(request.app, limit=200)
    pending = [c for c in all_candidates if c.metadata.get("promotion_pending_human_gate")]
    return {"items": [c.model_dump(mode="json") for c in pending], "count": len(pending)}


@router.get("/candidates/{candidate_id}")
async def get_ai_candidate_endpoint(request: Request, candidate_id: str):
    ensure_ai_research_runtime_state(request.app)
    row = get_candidate(request.app, candidate_id)
    return {"candidate": row.model_dump(mode="json")}


@router.get("/candidates/{candidate_id}/performance-divergence")
async def get_candidate_performance_divergence(
    request: Request,
    candidate_id: str,
    limit: int = 100,
    record_feedback: bool = True,
):
    ensure_ai_research_runtime_state(request.app)
    candidate = get_candidate(request.app, candidate_id)
    from core.research.performance_feedback import (
        build_performance_divergence_report,
        build_prior_update_from_divergence,
    )
    from sqlalchemy import select as sa_select

    async with async_session_maker() as session:
        q = (
            sa_select(StrategyPerformanceSnapshot)
            .where(StrategyPerformanceSnapshot.candidate_id == candidate_id)
            .order_by(StrategyPerformanceSnapshot.snapshot_at.desc())
            .limit(max(1, min(int(limit), 500)))
        )
        rows = (await session.execute(q)).scalars().all()
    report = build_performance_divergence_report(candidate=candidate, snapshots=rows)
    feedback_update: Dict[str, Any] = {}
    if bool(record_feedback) and report.status != "insufficient_sample":
        try:
            from core.observability.score_calibration import update_family_regime_priors
            from core.audit.gate_counterfactuals import update_gate_counterfactual_outcomes

            update = build_prior_update_from_divergence(candidate=candidate, report=report)
            audit_update = update_gate_counterfactual_outcomes(
                [
                    {
                        "subject_type": "candidate",
                        "subject_id": candidate_id,
                        "later_outcome_ref": f"performance_divergence:{report.status}",
                        "status": report.status,
                        "edge_delta": report.edge_delta,
                        "divergence_score": report.divergence_score,
                        "sample_size": report.sample_size,
                    }
                ]
            )
            if int(audit_update.get("updated") or 0) > 0:
                update["counterfactual_outcome_count"] = int(audit_update.get("updated") or 0)
            feedback_update = update_family_regime_priors([update])
            feedback_update["counterfactual_outcomes"] = audit_update
            meta = dict(candidate.metadata or {})
            meta["last_performance_divergence"] = report.to_dict()
            meta["last_performance_feedback_update"] = update
            candidate.metadata = meta
            registry = getattr(request.app.state, "ai_candidate_registry", None)
            if registry is not None and hasattr(registry, "save"):
                registry.save(candidate)
        except Exception as exc:
            feedback_update = {"applied": 0, "error": str(exc)}
    return {"candidate_id": candidate_id, "report": report.to_dict(), "feedback_update": feedback_update}


@router.post("/candidates/{candidate_id}/autonomy-handoff", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def handoff_candidate_to_autonomy(request: Request, candidate_id: str):
    ensure_ai_research_runtime_state(request.app)
    candidate = get_candidate(request.app, candidate_id)
    meta = dict(candidate.metadata or {})
    symbol = _candidate_primary_symbol(candidate)
    timeframe = _candidate_timeframe(candidate)
    exchange = _candidate_exchange(candidate)
    current_runtime = {}
    updated_runtime = {}
    watch_registered = False
    preview_warmed = False
    agent_running = False
    try:
        current_runtime = dict(autonomous_trading_agent.get_runtime_config() or {})
        existing = [
            _normalize_symbol(str(item or ""))
            for item in list(current_runtime.get("universe_symbols") or [])
            if _normalize_symbol(str(item or ""))
        ]
        merged_universe = []
        for item in [symbol, *existing]:
            norm = _normalize_symbol(str(item or ""))
            if norm and norm not in merged_universe:
                merged_universe.append(norm)
        update_kwargs = {
            "symbol_mode": "auto",
            "universe_symbols": merged_universe,
            "timeframe": timeframe,
            "exchange": exchange,
        }
        if not _normalize_symbol(str(current_runtime.get("symbol") or "")):
            update_kwargs["symbol"] = symbol
        updated_runtime = await autonomous_trading_agent.update_runtime_config(**update_kwargs)
        watch_registered = symbol in set(_normalize_symbol(str(item or "")) for item in updated_runtime.get("universe_symbols") or [])
        warm_method = getattr(autonomous_trading_agent, "ensure_symbol_scan_preview_warm", None)
        if callable(warm_method):
            preview_warmed = bool(warm_method(limit=int(updated_runtime.get("selection_top_n") or 10), force=True))
        try:
            agent_running = bool((autonomous_trading_agent.get_status() or {}).get("running"))
        except Exception:
            agent_running = False
    except Exception as exc:
        updated_runtime = {"error": str(exc), **(current_runtime or {})}
    meta["autonomy_handoff_requested"] = True
    meta["autonomy_mode"] = "watch"
    meta["autonomy_handoff_at"] = datetime.now(timezone.utc).isoformat()
    meta["autonomy_watch_scope"] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "exchange": exchange,
        "registered": bool(watch_registered),
        "preview_warmed": bool(preview_warmed),
        "active": bool(watch_registered and (preview_warmed or agent_running)),
    }
    candidate.metadata = meta
    registry = getattr(request.app.state, "ai_candidate_registry", None)
    if registry is not None and hasattr(registry, "save"):
        registry.save(candidate)
    try:
        from core.research.orchestrator import _refresh_runtime_eligibility_snapshot_safe  # noqa: PLC0415

        _refresh_runtime_eligibility_snapshot_safe(reason="autonomy_handoff", app=request.app)
    except Exception:
        pass
    return {
        "candidate_id": candidate_id,
        "candidate": candidate.model_dump(mode="json"),
        "watch_registered": bool(watch_registered),
        "autonomy_watch_scope": meta["autonomy_watch_scope"],
        "preview_warmed": bool(preview_warmed),
        "watch_active": bool(meta["autonomy_watch_scope"]["active"]),
        "runtime_config": updated_runtime,
        "next_actions": ["watch", "promote_paper", "request_live_approval"],
    }


@router.delete("/candidates/{candidate_id}", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def delete_ai_candidate_endpoint(request: Request, candidate_id: str):
    ensure_ai_research_runtime_state(request.app)
    result = delete_orphan_candidate(
        request.app,
        candidate_id=candidate_id,
        actor="web_ui",
    )
    return result


@router.post("/candidates/{candidate_id}/exit", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def exit_ai_candidate_endpoint(request: Request, candidate_id: str, payload: AIRetireRequest = AIRetireRequest()):
    ensure_ai_research_runtime_state(request.app)
    candidate = get_candidate(request.app, candidate_id)
    proposal_id = str(getattr(candidate, "proposal_id", "") or "").strip()
    if proposal_id:
        try:
            get_proposal(request.app, proposal_id)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
        else:
            raise HTTPException(status_code=409, detail="candidate belongs to an existing proposal, exit the proposal instead")

    return await _exit_candidate_record(
        request.app,
        candidate,
        notes=payload.notes or "exited manually from AI research queue",
        actor="web_ui",
    )


@router.get("/candidates/{candidate_id}/lifecycle")
async def get_ai_candidate_lifecycle(request: Request, candidate_id: str, limit: int = 200):
    ensure_ai_research_runtime_state(request.app)
    _ = get_candidate(request.app, candidate_id)
    rows = list_lifecycle(request.app, "candidate", candidate_id, limit=limit)
    return {"candidate_id": candidate_id, "items": rows, "count": len(rows)}


@router.get(
    "/candidates/{candidate_id}/param-sensitivity",
    dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))],
)
async def get_candidate_param_sensitivity(
    request: Request,
    candidate_id: str,
    max_params: int = 5,
):
    """Single-parameter sensitivity scan using [-20%, base, +20%] perturbation."""
    ensure_ai_research_runtime_state(request.app)
    candidate = get_candidate(request.app, candidate_id)
    params = dict(candidate.params or {})
    if not params:
        return {"candidate_id": candidate_id, "items": [], "note": "candidate has no params"}

    numeric_rows: List[tuple[str, float, bool]] = []
    for key, value in params.items():
        if isinstance(value, bool):
            continue
        try:
            val = float(value)
        except Exception:
            continue
        if not pd.notna(val):
            continue
        numeric_rows.append((str(key), float(val), isinstance(value, int)))
    if not numeric_rows:
        return {"candidate_id": candidate_id, "items": [], "note": "candidate has no numeric params"}

    max_params = max(1, min(int(max_params or 5), 8))
    numeric_rows = numeric_rows[:max_params]
    timeframe = str(candidate.timeframe or "1h")
    symbol = _normalize_symbol(candidate.symbol or "BTC/USDT")

    from web.api.backtest import (  # noqa: PLC0415
        _attach_backtest_enrichment_if_needed,
        _load_backtest_inputs,
        _run_backtest_core,
    )

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=30)
    df, bundle, resolved_symbol = await _load_backtest_inputs(
        strategy=candidate.strategy,
        symbol=symbol,
        timeframe=timeframe,
        params=params,
        start_time=start_time,
        end_time=end_time,
    )
    df = await _attach_backtest_enrichment_if_needed(
        strategy=candidate.strategy,
        df=df,
        symbol=resolved_symbol,
        start_time=start_time,
        end_time=end_time,
    )
    if df.empty:
        return {"candidate_id": candidate_id, "items": [], "note": "no market data for sensitivity"}

    try:
        base_result = await asyncio.to_thread(
            _run_backtest_core,
            strategy=candidate.strategy,
            df=df,
            timeframe=timeframe,
            initial_capital=10000.0,
            params=params,
            commission_rate=0.0004,
            slippage_bps=2.0,
            market_bundle=bundle,
        )
        base_sharpe = float(base_result.get("sharpe_ratio") or 0.0)
    except Exception:
        base_sharpe = 0.0

    items: List[Dict[str, Any]] = []
    for key, base_value, is_int in numeric_rows:
        row: Dict[str, Any] = {
            "param": key,
            "base_val": int(base_value) if is_int else round(base_value, 8),
            "low_val": None,
            "high_val": None,
            "sharpe_low": None,
            "sharpe_base": round(base_sharpe, 4),
            "sharpe_high": None,
        }
        for label, multiplier in (("low", 0.8), ("high", 1.2)):
            shifted = base_value * multiplier
            if is_int:
                shifted = max(1.0, round(shifted))
                shifted_val: Any = int(shifted)
            else:
                shifted_val = round(float(shifted), 8)
            row[f"{label}_val"] = shifted_val

            trial_params = dict(params)
            trial_params[key] = shifted_val
            try:
                result = await asyncio.to_thread(
                    _run_backtest_core,
                    strategy=candidate.strategy,
                    df=df,
                    timeframe=timeframe,
                    initial_capital=10000.0,
                    params=trial_params,
                    commission_rate=0.0004,
                    slippage_bps=2.0,
                    market_bundle=bundle,
                )
                row[f"sharpe_{label}"] = round(float(result.get("sharpe_ratio") or 0.0), 4)
            except Exception:
                row[f"sharpe_{label}"] = None
        items.append(row)

    return {
        "candidate_id": candidate_id,
        "symbol": resolved_symbol,
        "timeframe": timeframe,
        "items": items,
        "param_count": len(items),
    }


@router.post("/candidates/{candidate_id}/promote", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def promote_ai_candidate(request: Request, candidate_id: str, payload: AICandidatePromotionRequest):
    ensure_ai_research_runtime_state(request.app)
    result = await promote_existing_candidate(request.app, candidate_id=candidate_id, actor="web_ui", target=payload.target)
    return {
        "candidate_id": candidate_id,
        "candidate": result["candidate"].model_dump(mode="json"),
        "proposal": result["proposal"].model_dump(mode="json"),
        "promotion": result["promotion"].model_dump(mode="json"),
        "runtime_status": result.get("runtime_status"),
        "registered_strategy_name": result.get("registered_strategy_name"),
    }


@router.post("/candidates/{candidate_id}/register", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def register_ai_candidate(request: Request, candidate_id: str, payload: AICandidateRegisterRequest):
    """One-click: promote + register a validated candidate as a live strategy instance."""
    ensure_ai_research_runtime_state(request.app)
    if bool(getattr(settings, "GOVERNANCE_ENABLED", True)):
        raise HTTPException(
            status_code=409,
            detail="governance enabled: one-click register disabled; use pending-approval + human-approve workflow",
        )
    # Persist optional UI overrides into candidate metadata before promoting.
    if payload.name or payload.allocation_pct is not None:
        try:
            cand = get_candidate(request.app, candidate_id)
            if payload.name:
                cand.metadata["display_name"] = payload.name
            if payload.allocation_pct is not None:
                cand.metadata["allocation_pct"] = float(payload.allocation_pct)
            request.app.state.ai_candidate_registry.save(cand)
        except Exception:
            pass
    target = "paper" if payload.mode == "shadow" else payload.mode
    valid_modes = {"paper", "live_candidate"}
    target = target if target in valid_modes else "paper"
    current_mode = str(execution_engine.get_trading_mode() or "").strip().lower() or "paper"
    if target == "paper" and current_mode != "paper":
        raise HTTPException(status_code=400, detail=_register_mode_conflict_detail(current_mode))
    try:
        result = await promote_existing_candidate(
            request.app, candidate_id=candidate_id, actor="web_ui_register", target=target
        )
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "candidate registration failed") from exc
    return {
        "candidate_id": candidate_id,
        "candidate": result["candidate"].model_dump(mode="json"),
        "proposal": result["proposal"].model_dump(mode="json"),
        "promotion": result["promotion"].model_dump(mode="json"),
        "runtime_status": result.get("runtime_status"),
        "registered_strategy_name": result.get("registered_strategy_name"),
    }


@router.post("/candidates/{candidate_id}/human-approve", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def human_approve_candidate(request: Request, candidate_id: str, payload: AIHumanApprovalRequest):
    """Human approval: bypass the governance gate and promote the candidate."""
    from core.deployment.promotion_engine import promote_candidate
    from core.research.validation_gate import build_promotion_decision

    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)

    if not cand.metadata.get("promotion_pending_human_gate"):
        raise HTTPException(status_code=400, detail="candidate is not pending human approval")

    proposal = get_proposal(request.app, cand.proposal_id)

    # Determine promotion target
    target = (
        payload.target
        or cand.metadata.get("recommended_runtime_target")
        or (cand.promotion.decision if cand.promotion else None)
        or "paper"
    )
    target = "paper" if target == "shadow" else target
    valid_targets = {"paper", "live_candidate"}
    if target not in valid_targets:
        target = "paper"

    # Build/update promotion decision
    if cand.promotion is None:
        if cand.validation_summary is None:
            raise HTTPException(status_code=400, detail="candidate has no validation summary")
        cand.promotion = build_promotion_decision(cand.candidate_id, cand.validation_summary)
    current_decision = str(cand.promotion.decision or "").strip()
    if current_decision == "shadow":
        current_decision = "paper"
    if target == "live_candidate" and current_decision != "live_candidate":
        raise HTTPException(
            status_code=400,
            detail="approval target live_candidate exceeds validation decision",
        )
    cand.promotion.decision = target

    # Clear the gate; record approval metadata
    cand.metadata.pop("promotion_pending_human_gate", None)
    proposal.metadata.pop("promotion_pending_human_gate", None)
    cand.metadata["human_approved_at"] = datetime.now(timezone.utc).isoformat()
    cand.metadata["human_approved_target"] = target
    cand.metadata["human_approval_notes"] = payload.notes

    # Promote (bypasses the 409 guard in promote_existing_candidate)
    try:
        result = await promote_candidate(
            request.app, proposal=proposal, candidate=cand, promotion=cand.promotion, actor="human_approver"
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    request.app.state.ai_candidate_registry.save(cand)
    save_proposal(request.app, proposal)

    await write_audit(
        GovernanceAuditEvent(
            module="ai.research",
            action="human_approve",
            status="approved",
            actor="web_ui",
            role="HUMAN",
            input_payload={"candidate_id": candidate_id, "target": target, "notes": payload.notes},
            output_payload={"runtime_status": result.get("runtime_status")},
        )
    )
    return {
        "candidate_id": candidate_id,
        "candidate": result["candidate"].model_dump(mode="json"),
        "promotion": result["promotion"].model_dump(mode="json"),
        "runtime_status": result.get("runtime_status"),
        "registered_strategy_name": result.get("registered_strategy_name"),
    }


@router.post("/candidates/{candidate_id}/human-reject", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def human_reject_candidate(request: Request, candidate_id: str, payload: AIHumanApprovalRequest):
    """Human rejection: clear the governance gate and mark candidate as retired."""
    from core.research.experiment_schemas import PromotionDecision as _PD

    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)

    if not cand.metadata.get("promotion_pending_human_gate"):
        raise HTTPException(status_code=400, detail="candidate is not pending human approval")

    proposal = get_proposal(request.app, cand.proposal_id)

    # Clear gate; record rejection
    cand.metadata.pop("promotion_pending_human_gate", None)
    proposal.metadata.pop("promotion_pending_human_gate", None)
    cand.metadata["human_rejected_at"] = datetime.now(timezone.utc).isoformat()
    cand.metadata["human_rejection_reason"] = payload.notes

    # Create a reject promotion decision
    cand.promotion = _PD(
        candidate_id=candidate_id,
        decision="reject",
        reason=f"Human rejected: {payload.notes}" if payload.notes else "Human rejected",
        constraints={},
        created_at=datetime.now(timezone.utc),
    )
    cand.status = "retired"

    request.app.state.ai_candidate_registry.save(cand)
    save_proposal(request.app, proposal)

    await write_audit(
        GovernanceAuditEvent(
            module="ai.research",
            action="human_reject",
            status="denied",
            actor="web_ui",
            role="HUMAN",
            input_payload={"candidate_id": candidate_id, "reason": payload.notes},
            output_payload={"status": "retired"},
        )
    )
    return {
        "candidate_id": candidate_id,
        "status": "retired",
        "candidate": cand.model_dump(mode="json"),
    }


@router.post("/candidates/{candidate_id}/activate-live", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def activate_ai_candidate_live(
    request: Request,
    candidate_id: str,
    payload: AICandidateActivateLiveRequest = AICandidateActivateLiveRequest(),
):
    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)
    proposal = get_proposal(request.app, cand.proposal_id)
    cand_meta = cand.metadata if isinstance(getattr(cand, "metadata", None), dict) else {}
    if not isinstance(getattr(cand, "metadata", None), dict):
        cand.metadata = cand_meta
    proposal_meta = proposal.metadata if isinstance(getattr(proposal, "metadata", None), dict) else {}
    if not isinstance(getattr(proposal, "metadata", None), dict):
        proposal.metadata = proposal_meta

    governance_enabled = bool(getattr(settings, "GOVERNANCE_ENABLED", True))
    cand_status = str(getattr(cand, "status", "") or "")
    pending_human_gate = bool(
        cand_meta.get("promotion_pending_human_gate")
        or proposal_meta.get("promotion_pending_human_gate")
    )

    async def _audit_denied(reason: str) -> None:
        await write_audit(
            GovernanceAuditEvent(
                module="ai.research",
                action="activate_live",
                status="denied",
                actor="web_ui",
                role="HUMAN",
                input_payload={
                    "candidate_id": candidate_id,
                    "notes": payload.notes,
                    "candidate_status": cand_status,
                    "governance_enabled": governance_enabled,
                },
                output_payload={"reason": reason},
            )
        )

    if pending_human_gate:
        detail = "candidate is pending human approval; complete /human-approve before live activation"
        await _audit_denied(detail)
        raise HTTPException(status_code=400, detail=detail)

    current_mode = str(execution_engine.get_trading_mode() or "").strip().lower() or "paper"
    if current_mode != "live":
        await _audit_denied("trading mode is not live; switch to live mode first")
        raise HTTPException(status_code=400, detail="trading mode is not live; switch to live mode first")

    allowed_statuses = {"paper_running", "live_candidate", "live_running"}
    if cand_status not in allowed_statuses:
        detail = f"candidate in state {cand_status or 'unknown'}, live activation is not allowed"
        await _audit_denied(detail)
        raise HTTPException(
            status_code=400,
            detail=detail,
        )

    if governance_enabled and cand_status != "live_running":
        approved_target = str(cand_meta.get("human_approved_target") or "").strip().lower()
        approved_at = str(cand_meta.get("human_approved_at") or "").strip()
        if cand_status != "live_candidate" or approved_target != "live_candidate" or not approved_at:
            detail = (
                "governance enabled: candidate is not human-approved for live activation; "
                "use pending-approval + /human-approve target=live_candidate first"
            )
            await _audit_denied(detail)
            raise HTTPException(status_code=400, detail=detail)

    try:
        strategy_result = await _ensure_candidate_runtime_strategy(
            request.app,
            cand,
            target_mode="live",
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    transition_reason = payload.notes or "activated live from AI research page"
    if cand_status != "live_running":
        transition_candidate(
            cand,
            to_state="live_running",
            lifecycle_registry=request.app.state.ai_lifecycle_registry,
            actor="web_ui_live_activate",
            reason=transition_reason,
            metadata={
                "strategy_name": strategy_result["registered_strategy_name"],
                "source": "ai_research_live_activate",
            },
        )
    if str(getattr(proposal, "status", "") or "") != "live_running":
        transition_proposal(
            proposal,
            to_state="live_running",
            lifecycle_registry=request.app.state.ai_lifecycle_registry,
            actor="web_ui_live_activate",
            reason=transition_reason,
            metadata={
                "strategy_name": strategy_result["registered_strategy_name"],
                "source": "ai_research_live_activate",
            },
        )

    cand_meta["live_activated_at"] = datetime.now(timezone.utc).isoformat()
    cand_meta["live_activation_notes"] = payload.notes
    cand_meta["live_activation_source"] = "ai_research"

    request.app.state.ai_candidate_registry.save(cand)
    save_proposal(request.app, proposal)

    await write_audit(
        GovernanceAuditEvent(
            module="ai.research",
            action="activate_live",
            status="approved",
            actor="web_ui",
            role="HUMAN",
            input_payload={"candidate_id": candidate_id, "notes": payload.notes},
            output_payload={
                "runtime_status": "live_running",
                "registered_strategy_name": strategy_result["registered_strategy_name"],
            },
        )
    )

    return {
        "candidate_id": candidate_id,
        "candidate": cand.model_dump(mode="json"),
        "proposal": proposal.model_dump(mode="json"),
        "runtime_status": "live_running",
        "registered_strategy_name": strategy_result["registered_strategy_name"],
        "activation": strategy_result,
    }


def _trades_to_returns(trades: list) -> List[float]:
    """Convert trade dicts to a per-trade return fraction for CUSUM analysis."""
    returns: List[float] = []
    for t in trades:
        pnl_pct = t.get("pnl_pct") or t.get("return_pct")
        if pnl_pct is not None:
            returns.append(float(pnl_pct))
        else:
            pnl = float(t.get("pnl", 0.0) or 0.0)
            capital = float(t.get("capital") or t.get("initial_capital") or 0.0)
            if capital > 0:
                returns.append(pnl / capital)
            elif pnl != 0.0:
                returns.append(pnl / 10000.0)  # fallback: assume 10k capital
    return returns


def _build_candidate_decay_payload(
    request: Request,
    candidate_id: str,
    *,
    persist: bool,
) -> Dict[str, Any]:
    """Run CUSUM decay detection on the candidate's live/paper trade history.

    Reads the risk_manager's in-memory trade history, filters by strategy name,
    converts to a return series, runs CUSUM, and optionally persists summary to
    candidate metadata. GET callers use the read-only compatibility path; POST
    callers persist the metadata update.
    """
    from core.monitoring.strategy_monitor import detect_strategy_decay
    from core.risk.risk_manager import risk_manager

    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)

    # Resolve strategy name used when the candidate was registered
    strat_name: Optional[str] = (
        cand.metadata.get("registered_strategy_name")
        or cand.metadata.get("display_name")
        or cand.strategy
        or None
    )

    all_trades: List[Dict[str, Any]] = list(getattr(risk_manager, "_trade_history", []) or [])
    if strat_name:
        filtered = [
            t for t in all_trades
            if t.get("strategy") == strat_name or t.get("strategy_name") == strat_name
        ]
    else:
        filtered = all_trades

    returns = _trades_to_returns(filtered)
    result = detect_strategy_decay(returns)

    status_summary: Dict[str, Any] = {
        "triggered": result["triggered"],
        "n_bars": result["n_bars"],
        "decay_pct": result["decay_pct"],
        "threshold": result["threshold"],
        "message": result["message"],
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "strategy_name_used": strat_name,
    }
    if persist:
        cand.metadata["cusum_status"] = status_summary
        request.app.state.ai_candidate_registry.save(cand)

    return {
        "candidate_id": candidate_id,
        "cusum": result,
        "strategy_name": strat_name,
        "n_trades": len(filtered),
        "persisted": bool(persist),
        "cusum_status": status_summary,
    }


@router.post(
    "/candidates/{candidate_id}/decay-check",
    dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))],
)
async def post_candidate_decay_check(request: Request, candidate_id: str):
    return _build_candidate_decay_payload(request, candidate_id, persist=True)


@router.get("/candidates/{candidate_id}/decay-check")
async def get_candidate_decay_check(request: Request, candidate_id: str):
    return _build_candidate_decay_payload(request, candidate_id, persist=False)


@router.get("/promotions")
async def get_ai_promotions(request: Request, limit: int = 50):
    ensure_ai_research_runtime_state(request.app)
    rows = list_promotions(request.app, limit=limit)
    return {"items": rows, "count": len(rows)}


@router.get("/deployments/status")
async def get_ai_deployments_status(request: Request):
    ensure_ai_research_runtime_state(request.app)
    return get_deployment_status(request.app)


@router.post("/research/generate-context", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def generate_research_context_endpoint(request: Request, payload: AIResearchContextRequest):
    """Call the configured OpenAI-compatible model to generate a research hypothesis and experiment plan."""
    from core.ai.research_context_generator import generate_research_context

    try:
        ctx = await generate_research_context(
            market_summary=payload.market_summary,
            goals=payload.goals,
            timeout=payload.timeout,
        )
    except Exception as exc:
        ctx = None
        logger.warning(f"generate_research_context failed: {exc}")

    if ctx is not None:
        return {"llm_research_output": ctx}
    return {"llm_research_output": None, "error": "LLM unavailable or returned invalid response"}


@router.get("/signals/latest")
async def get_latest_signals(
    request: Request,
    symbol: str = "BTC/USDT",
    since_minutes: int = 240,
):
    """Return the latest aggregated signal snapshot for *symbol*.

    Combines LLM news signal, ML model prediction, and rule-based factor
    signal into a single weighted vote.  Falls back gracefully when the ML
    model is not yet trained or the news DB is empty.
    """
    from core.ai.signal_aggregator import signal_aggregator
    market_data, market_meta = await _load_signal_market_data(
        exchange="binance",
        symbol=symbol,
        timeframe="1h",
        limit=max(120, int(since_minutes / 60) * 6 if since_minutes > 0 else 120),
    )

    try:
        agg = await signal_aggregator.aggregate(symbol=symbol, market_data=market_data)
        return {
            **agg.to_dict(),
            **market_meta,
        }
    except Exception as exc:
        return {
            "symbol": symbol,
            "direction": "FLAT",
            "confidence": 0.0,
            "requires_approval": False,
            "blocked_by_risk": False,
            "risk_reason": "",
            "components": {},
            "error": str(exc),
            **market_meta,
        }


# ── Strategy Performance Snapshot endpoints ──────────────────────────────────

class PerformanceSnapshotRequest(BaseModel):
    strategy_name: str
    symbol: str
    timeframe: str
    mode: str = "paper"
    candidate_id: Optional[str] = None
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    win_rate: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    calmar_ratio: Optional[float] = None
    cusum_triggered: bool = False
    cusum_low: Optional[float] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


async def _record_performance_snapshot_outcome_feedback(
    app: Any,
    *,
    candidate_id: str,
    snapshots: List[Any],
) -> Dict[str, Any]:
    """Backfill gate-audit outcomes when real paper/live performance arrives."""
    candidate_id_text = str(candidate_id or "").strip()
    if not candidate_id_text or not snapshots:
        return {"applied": 0, "reason": "missing_candidate_or_snapshot"}

    latest = snapshots[0]
    candidate = None
    report_payload: Dict[str, Any] = {}
    prior_update: Dict[str, Any] = {}
    feedback_update: Dict[str, Any] = {}
    try:
        candidate = get_candidate(app, candidate_id_text)
    except Exception:
        candidate = None

    edge_delta = 0.0
    divergence_score = 0.0
    status = "performance_snapshot"
    sample_size = int(getattr(latest, "trade_count", 0) or 0)
    if candidate is not None:
        try:
            from core.research.performance_feedback import (
                build_performance_divergence_report,
                build_prior_update_from_divergence,
            )

            report = build_performance_divergence_report(candidate=candidate, snapshots=snapshots)
            report_payload = report.to_dict()
            edge_delta = float(report.edge_delta or 0.0)
            divergence_score = float(report.divergence_score or 0.0)
            status = str(report.status or status)
            sample_size = int(report.sample_size or sample_size)
            if status != "insufficient_sample":
                prior_update = build_prior_update_from_divergence(candidate=candidate, report=report)
        except Exception as exc:
            report_payload = {"error": str(exc)}
    else:
        try:
            edge_delta = float(getattr(latest, "sharpe_ratio", 0.0) or getattr(latest, "total_pnl_pct", 0.0) or 0.0)
        except Exception:
            edge_delta = 0.0

    audit_update: Dict[str, Any] = {}
    try:
        from core.audit.gate_counterfactuals import update_gate_counterfactual_outcomes

        audit_update = update_gate_counterfactual_outcomes(
            [
                {
                    "subject_type": "candidate",
                    "subject_id": candidate_id_text,
                    "later_outcome_ref": f"performance_snapshot:{getattr(latest, 'id', '') or getattr(latest, 'snapshot_at', '')}",
                    "status": status,
                    "edge_delta": edge_delta,
                    "divergence_score": divergence_score,
                    "sample_size": sample_size,
                    "snapshot_at": (
                        latest.snapshot_at.isoformat()
                        if getattr(latest, "snapshot_at", None) is not None
                        else None
                    ),
                    "mode": str(getattr(latest, "mode", "") or ""),
                    "strategy_name": str(getattr(latest, "strategy_name", "") or ""),
                    "symbol": str(getattr(latest, "symbol", "") or ""),
                }
            ]
        )
    except Exception as exc:
        audit_update = {"updated": 0, "error": str(exc)}

    if prior_update:
        try:
            from core.observability.score_calibration import update_family_regime_priors

            if int(audit_update.get("updated") or 0) > 0:
                prior_update["counterfactual_outcome_count"] = int(audit_update.get("updated") or 0)
            feedback_update = update_family_regime_priors([prior_update])
        except Exception as exc:
            feedback_update = {"applied": 0, "error": str(exc)}

    try:
        if candidate is not None:
            meta = dict(candidate.metadata or {})
            meta["last_performance_outcome_feedback"] = {
                "status": status,
                "edge_delta": edge_delta,
                "audit_update": audit_update,
                "prior_update": prior_update,
            }
            candidate.metadata = meta
            registry = getattr(app.state, "ai_candidate_registry", None)
            if registry is not None and hasattr(registry, "save"):
                registry.save(candidate)
    except Exception:
        pass

    return {
        "applied": int(audit_update.get("updated") or 0) + int(feedback_update.get("applied") or 0),
        "status": status,
        "audit_update": audit_update,
        "feedback_update": feedback_update,
        "report": report_payload,
    }


@router.post("/performance/snapshots", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def save_performance_snapshot(request: Request, body: PerformanceSnapshotRequest):
    """Persist a strategy performance snapshot to the DB."""

    row = StrategyPerformanceSnapshot(
        snapshot_at=datetime.now(timezone.utc),
        candidate_id=body.candidate_id,
        strategy_name=body.strategy_name,
        symbol=body.symbol,
        timeframe=body.timeframe,
        mode=body.mode,
        total_pnl=body.total_pnl,
        total_pnl_pct=body.total_pnl_pct,
        unrealized_pnl=body.unrealized_pnl,
        realized_pnl=body.realized_pnl,
        trade_count=body.trade_count,
        win_count=body.win_count,
        loss_count=body.loss_count,
        win_rate=body.win_rate,
        sharpe_ratio=body.sharpe_ratio,
        max_drawdown=body.max_drawdown,
        calmar_ratio=body.calmar_ratio,
        cusum_triggered=body.cusum_triggered,
        cusum_low=body.cusum_low,
        payload=body.payload or {},
    )
    try:
        async with async_session_maker() as session:
            session.add(row)
            await session.commit()
    except Exception as exc:
        logger.warning(f"save_performance_snapshot failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    outcome_feedback: Dict[str, Any] = {}
    if body.candidate_id:
        outcome_feedback = await _record_performance_snapshot_outcome_feedback(
            request.app,
            candidate_id=str(body.candidate_id),
            snapshots=[row],
        )
    return {"ok": True, "snapshot_at": row.snapshot_at.isoformat(), "outcome_feedback": outcome_feedback}


@router.get("/performance/snapshots")
async def list_performance_snapshots(
    strategy_name: Optional[str] = None,
    candidate_id: Optional[str] = None,
    symbol: Optional[str] = None,
    mode: Optional[str] = None,
    days: int = 30,
    limit: int = 200,
):
    """List recent performance snapshots with optional filters."""
    from sqlalchemy import select as sa_select

    since = datetime.now(timezone.utc) - timedelta(days=max(1, min(int(days), 3650)))
    try:
        async with async_session_maker() as session:
            q = sa_select(StrategyPerformanceSnapshot).where(
                StrategyPerformanceSnapshot.snapshot_at >= since
            )
            if strategy_name:
                q = q.where(StrategyPerformanceSnapshot.strategy_name == strategy_name)
            if candidate_id:
                q = q.where(StrategyPerformanceSnapshot.candidate_id == candidate_id)
            if symbol:
                q = q.where(StrategyPerformanceSnapshot.symbol == symbol)
            if mode:
                q = q.where(StrategyPerformanceSnapshot.mode == mode)
            q = q.order_by(StrategyPerformanceSnapshot.snapshot_at.desc()).limit(max(1, min(int(limit), 2000)))
            result = await session.execute(q)
            rows = result.scalars().all()
    except Exception as exc:
        logger.warning(f"list_performance_snapshots failed: {exc}")
        return {"snapshots": [], "error": str(exc)}

    snapshots = []
    for row in rows:
        snapshots.append({
            "id": row.id,
            "snapshot_at": row.snapshot_at.isoformat() if row.snapshot_at else None,
            "candidate_id": row.candidate_id,
            "strategy_name": row.strategy_name,
            "symbol": row.symbol,
            "timeframe": row.timeframe,
            "mode": row.mode,
            "total_pnl": row.total_pnl,
            "total_pnl_pct": row.total_pnl_pct,
            "unrealized_pnl": row.unrealized_pnl,
            "realized_pnl": row.realized_pnl,
            "trade_count": row.trade_count,
            "win_count": row.win_count,
            "loss_count": row.loss_count,
            "win_rate": row.win_rate,
            "sharpe_ratio": row.sharpe_ratio,
            "max_drawdown": row.max_drawdown,
            "calmar_ratio": row.calmar_ratio,
            "cusum_triggered": row.cusum_triggered,
            "cusum_low": row.cusum_low,
            "payload": row.payload or {},
        })
    return {"snapshots": snapshots, "count": len(snapshots)}


# ── Phase A: Live signals for all active candidates ───────────────────────────

@router.get("/live-signals")
@router.get("/candidates/live-signals")
async def get_live_signals(request: Request, symbol: Optional[str] = None):
    """Return live signal snapshots for active AI research candidates only."""
    ensure_ai_research_runtime_state(request.app)
    return await _build_candidate_live_signals_payload(request, symbol=symbol)


async def get_autonomous_agent_live_signals(request: Request, symbol: Optional[str] = None):
    """Return live signal snapshots for the autonomous agent watchlist only."""
    return await _build_autonomous_watchlist_live_signals_payload(symbol=symbol)


# ── Phase B: Quick register (human-approve shortcut with allocation_pct) ─────

class AIQuickRegisterRequest(BaseModel):
    allocation_pct: float = Field(default=0.05, ge=0.001, le=1.0)


@router.post("/candidates/{candidate_id}/quick-register", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def quick_register_candidate(
    request: Request,
    candidate_id: str,
    payload: AIQuickRegisterRequest,
):
    """Quick register: store allocation_pct then human-approve to paper.

    Requires the candidate to be pending human approval (promotion_pending_human_gate).
    Thin wrapper around the human-approve logic — no duplicate code paths.
    """
    from core.deployment.promotion_engine import promote_candidate
    from core.research.validation_gate import build_promotion_decision

    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)

    has_gate = bool(cand.metadata.get("promotion_pending_human_gate"))
    governance_enabled = bool(getattr(settings, "GOVERNANCE_ENABLED", True))
    if not has_gate:
        if governance_enabled:
            raise HTTPException(
                status_code=400,
                detail="候选未在审批队列中；治理模式下需先完成研究流程后自动进入待审批",
            )
        # 非治理模式：无门控，直接走 promote_existing_candidate（与 /register 等价），附带 allocation_pct
        cand.metadata["allocation_pct"] = float(payload.allocation_pct)
        request.app.state.ai_candidate_registry.save(cand)
        result = await promote_existing_candidate(
            request.app, candidate_id=candidate_id, actor="quick_register", target="paper"
        )
        await write_audit(
            GovernanceAuditEvent(
                module="ai.research",
                action="quick_register_no_gate",
                status="approved",
                actor="web_ui",
                role="HUMAN",
                input_payload={"candidate_id": candidate_id, "allocation_pct": payload.allocation_pct},
                output_payload={"runtime_status": result.get("runtime_status")},
            )
        )
        return {
            "candidate_id": candidate_id,
            "allocation_pct": payload.allocation_pct,
            "candidate": result["candidate"].model_dump(mode="json"),
            "promotion": result["promotion"].model_dump(mode="json"),
            "runtime_status": result.get("runtime_status"),
            "registered_strategy_name": result.get("registered_strategy_name"),
        }

    proposal = get_proposal(request.app, cand.proposal_id)

    # Store allocation preference in metadata
    cand.metadata["allocation_pct"] = float(payload.allocation_pct)

    # Build or update promotion decision → paper
    if cand.promotion is None:
        if cand.validation_summary is None:
            raise HTTPException(status_code=400, detail="candidate has no validation summary")
        cand.promotion = build_promotion_decision(cand.candidate_id, cand.validation_summary)
    cand.promotion.decision = "paper"
    cand.promotion.constraints["allocation_cap"] = float(payload.allocation_pct)
    cand.promotion.constraints["runtime_mode"] = "paper"

    # Clear governance gate
    cand.metadata.pop("promotion_pending_human_gate", None)
    proposal.metadata.pop("promotion_pending_human_gate", None)
    cand.metadata["human_approved_at"] = datetime.now(timezone.utc).isoformat()
    cand.metadata["human_approved_by"] = "quick_register"
    cand.metadata["human_approval_notes"] = f"Quick register: allocation {payload.allocation_pct:.0%}"

    try:
        result = await promote_candidate(
            request.app,
            proposal=proposal,
            candidate=cand,
            promotion=cand.promotion,
            actor="quick_register",
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    request.app.state.ai_candidate_registry.save(result["candidate"])
    save_proposal(request.app, result["proposal"])

    await write_audit(
        GovernanceAuditEvent(
            module="ai.research",
            action="quick_register",
            status="approved",
            actor="web_ui",
            role="HUMAN",
            input_payload={"candidate_id": candidate_id, "allocation_pct": payload.allocation_pct},
            output_payload={"runtime_status": result.get("runtime_status")},
        )
    )
    return {
        "candidate_id": candidate_id,
        "allocation_pct": payload.allocation_pct,
        "candidate": result["candidate"].model_dump(mode="json"),
        "promotion": result["promotion"].model_dump(mode="json"),
        "runtime_status": result.get("runtime_status"),
        "registered_strategy_name": result.get("registered_strategy_name"),
    }


# ── Phase D — Order Preview (read-only, no order placed) ──────────────────────

@router.post("/candidates/{candidate_id}/order-preview", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def generate_order_preview(request: Request, candidate_id: str):
    """Generate a suggested order preview from SignalAggregator. Does NOT place any order.

    Returns direction, estimated size, stop/take levels, and component breakdown.
    The candidate must be in validated/paper_running/shadow_running/live_candidate/live_running.
    """
    ensure_ai_research_runtime_state(request.app)
    cand = get_candidate(request.app, candidate_id)

    allowed_statuses = {"validated", "paper_running", "shadow_running", "live_candidate", "live_running"}
    if str(cand.status) not in allowed_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"候选状态 {cand.status} 不支持订单预览（需为 {'/'.join(sorted(allowed_statuses))}）",
        )

    cand_symbol = _candidate_primary_symbol(cand)
    allocation_pct = float(cand.metadata.get("allocation_pct") or 0.05)

    # Lazy singleton aggregator (shared with live-signals endpoint)
    if not hasattr(request.app.state, "_signal_aggregator"):
        from core.ai.signal_aggregator import SignalAggregator  # noqa: PLC0415
        request.app.state._signal_aggregator = SignalAggregator()
    aggregator = request.app.state._signal_aggregator

    df, market_meta = await _load_signal_market_data(
        exchange=_candidate_exchange(cand),
        symbol=cand_symbol,
        timeframe=_candidate_timeframe(cand),
        limit=120,
    )

    sig = await aggregator.aggregate(cand_symbol, df)

    # Estimate portfolio capital (read-only)
    total_capital = 10000.0
    try:
        from core.risk.risk_manager import risk_manager  # noqa: PLC0415
        total_capital = float(getattr(risk_manager, "_cached_equity", None) or 10000.0)
    except Exception:
        pass

    size_usdt = round(total_capital * allocation_pct, 2)

    # ATR-based stop/take; fall back to fixed 3%/6% if unavailable
    stop_loss_pct = 0.03
    take_profit_pct = 0.06
    if not df.empty and "atr" in df.columns:
        try:
            atr_raw = float(df["atr"].iloc[-1])
            close = float(df["close"].iloc[-1])
            if close > 0 and atr_raw > 0:
                atr_pct = atr_raw / close
                stop_loss_pct = round(min(max(atr_pct * 1.4, 0.01), 0.15), 4)
                take_profit_pct = round(stop_loss_pct * 2.0, 4)
        except Exception:
            pass

    return {
        "candidate_id": candidate_id,
        "symbol": cand_symbol,
        "direction": sig.direction,
        "confidence": sig.confidence,
        "requires_approval": sig.requires_approval,
        "blocked_by_risk": sig.blocked_by_risk,
        "risk_reason": getattr(sig, "risk_reason", None),
        "size_usdt": size_usdt,
        "allocation_pct": allocation_pct,
        "stop_loss_pct": stop_loss_pct,
        "take_profit_pct": take_profit_pct,
        "components": sig.components,
        "market_data": market_meta,
        "note": "此为预览，不会自动下单。确认后请在交易面板手动执行。",
        "ts": sig.timestamp.isoformat(),
    }


# ── Source Health / Premium Data Status ──────────────────────────────────────

def _source_cache_mtime(cache_dir: str, name: str) -> Optional[str]:
    path = Path(cache_dir) / f"{name}.parquet"
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _source_latest_cache_mtime(cache_dir: str, names: List[str]) -> Optional[str]:
    latest: Optional[float] = None
    for name in names:
        path = Path(cache_dir) / f"{name}.parquet"
        try:
            mtime = path.stat().st_mtime
        except Exception:
            continue
        if latest is None or mtime > latest:
            latest = mtime
    if latest is None:
        return None
    return datetime.fromtimestamp(latest, tz=timezone.utc).isoformat()


def _parse_timestamp_to_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def _age_seconds(value: Any) -> Optional[float]:
    parsed = _parse_timestamp_to_utc(value)
    if parsed is None:
        return None
    return round(max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds()), 3)


def _dedupe_text_items(values: List[Any]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in values or []:
        text = str(raw or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _build_source_entry(
    *,
    label: str,
    available: bool,
    configured: Optional[bool] = None,
    key_configured: Optional[bool] = None,
    has_cached_data: bool = False,
    ready: Optional[bool] = None,
    last_updated: Optional[str] = None,
    max_age_sec: Optional[float] = None,
    support_level: str = "optional",
    issues: Optional[List[Any]] = None,
    recommendation: str = "",
    snapshot: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    age_sec = _age_seconds(last_updated)
    stale = bool(age_sec is not None and max_age_sec is not None and age_sec > max_age_sec)
    issue_list = _dedupe_text_items([*(issues or []), *(["cache/state freshness threshold exceeded"] if stale else []), error])
    configured_value = bool(configured) if configured is not None else bool(available)

    if ready is not None:
        if stale:
            health = "stale"
        elif ready:
            health = "healthy" if not issue_list else "degraded"
        elif available or has_cached_data or configured_value:
            health = "degraded"
        else:
            health = "missing"
    else:
        if stale:
            health = "stale"
        elif has_cached_data and not issue_list:
            health = "healthy"
        elif available or configured_value:
            health = "degraded" if issue_list or not has_cached_data else "healthy"
        else:
            health = "missing"

    payload = {
        "label": label,
        "available": bool(available),
        "configured": configured_value,
        "has_cached_data": bool(has_cached_data),
        "last_updated": last_updated,
        "age_sec": age_sec,
        "stale": stale,
        "health": health,
        "support_level": support_level,
        "issues": issue_list,
        "recommendation": str(recommendation or "").strip() or None,
    }
    if key_configured is not None:
        payload["key_configured"] = bool(key_configured)
    if snapshot is not None:
        payload["snapshot"] = snapshot
    if error:
        payload["error"] = str(error)
    if extra:
        payload.update(extra)
    return payload


def _configured_news_source_flags() -> Dict[str, bool]:
    def _enabled(name: str, default: bool = True) -> bool:
        raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
        return raw not in {"0", "false", "no", "off"}

    try:
        from core.data.coinglass_client import coinglass_enabled as _coinglass_enabled  # noqa: PLC0415

        coinglass_news_enabled = bool(_coinglass_enabled())
    except Exception:
        coinglass_news_enabled = False

    newsapi_enabled = _enabled("NEWS_ENABLE_NEWSAPI", True) and bool(str(os.getenv("NEWSAPI_KEY") or "").strip())
    cryptopanic_enabled = _enabled("NEWS_ENABLE_CRYPTOPANIC", True) and bool(
        str(os.getenv("CRYPTOPANIC_TOKEN") or os.getenv("CRYPTOPANIC_API_KEY") or "").strip()
    )
    return {
        "jin10": _enabled("NEWS_ENABLE_JIN10", True),
        "rss": _enabled("NEWS_ENABLE_RSS", True),
        "gdelt": _enabled("NEWS_ENABLE_GDELT", True),
        "newsapi": newsapi_enabled,
        "cryptopanic": cryptopanic_enabled,
        "chaincatcher_flash": _enabled("NEWS_ENABLE_CHAINCATCHER_FLASH", True),
        "okx_announcements": _enabled("NEWS_ENABLE_OKX_ANNOUNCEMENTS", True),
        "bybit_announcements": _enabled("NEWS_ENABLE_BYBIT_ANNOUNCEMENTS", True),
        "binance_announcements": _enabled("NEWS_ENABLE_BINANCE_ANNOUNCEMENTS", True),
        "cryptocompare_news": _enabled("NEWS_ENABLE_CRYPTOCOMPARE_NEWS", True) and bool(str(os.getenv("CRYPTOCOMPARE_API_KEY") or "").strip()),
        "coinglass_newsflash": coinglass_news_enabled and _enabled("NEWS_ENABLE_COINGLASS_NEWSFLASH", True),
        "coinglass_articles": coinglass_news_enabled and _enabled("NEWS_ENABLE_COINGLASS_ARTICLES", True),
        "coinglass_economic_data": coinglass_news_enabled and _enabled("NEWS_ENABLE_COINGLASS_ECONOMIC_DATA", True),
        "coinglass_financial_events": coinglass_news_enabled and _enabled("NEWS_ENABLE_COINGLASS_FINANCIAL_EVENTS", True),
        "coinglass_central_bank": coinglass_news_enabled and _enabled("NEWS_ENABLE_COINGLASS_CENTRAL_BANK", True),
    }


def _news_source_catalog() -> Dict[str, Dict[str, Any]]:
    return {
        "jin10": {"label": "Jin10 Macro Flash", "support_level": "core", "requires_key": False, "source_type": "macro_flash"},
        "rss": {"label": "RSS Aggregation", "support_level": "core", "requires_key": False, "source_type": "aggregated"},
        "gdelt": {"label": "GDELT", "support_level": "core", "requires_key": False, "source_type": "global_news"},
        "binance_announcements": {"label": "Binance Announcements", "support_level": "core", "requires_key": False, "source_type": "official_exchange"},
        "okx_announcements": {"label": "OKX Announcements", "support_level": "core", "requires_key": False, "source_type": "official_exchange"},
        "bybit_announcements": {"label": "Bybit Announcements", "support_level": "core", "requires_key": False, "source_type": "official_exchange"},
        "chaincatcher_flash": {"label": "ChainCatcher Flash", "support_level": "enhancement", "requires_key": False, "source_type": "crypto_flash"},
        "newsapi": {"label": "NewsAPI", "support_level": "enhancement", "requires_key": True, "source_type": "media_api"},
        "cryptopanic": {"label": "CryptoPanic", "support_level": "enhancement", "requires_key": True, "source_type": "crypto_api"},
        "cryptocompare_news": {"label": "CryptoCompare News", "support_level": "enhancement", "requires_key": True, "source_type": "crypto_api"},
        "coinglass_newsflash": {"label": "CoinGlass Newsflash", "support_level": "enhancement", "requires_key": True, "source_type": "crypto_api"},
        "coinglass_articles": {"label": "CoinGlass Articles", "support_level": "enhancement", "requires_key": True, "source_type": "crypto_api"},
        "coinglass_economic_data": {"label": "CoinGlass Economic Data", "support_level": "enhancement", "requires_key": True, "source_type": "macro_calendar"},
        "coinglass_financial_events": {"label": "CoinGlass Financial Events", "support_level": "enhancement", "requires_key": True, "source_type": "macro_calendar"},
        "coinglass_central_bank": {"label": "CoinGlass Central Bank", "support_level": "enhancement", "requires_key": True, "source_type": "macro_calendar"},
    }


def _is_future_timestamp(value: Any) -> bool:
    dt = _parse_timestamp_to_utc(value)
    if dt is None:
        return False
    return dt > datetime.now(timezone.utc)


def _collect_source_entries(categories: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for category_name, category in (categories or {}).items():
        sources = category.get("sources") or {}
        for source_name, payload in sources.items():
            item = dict(payload or {})
            item["name"] = source_name
            item["category"] = category_name
            rows.append(item)
    return rows


def _agent_model_runtime_issue(status: Dict[str, Any]) -> Optional[str]:
    """Use recent execution evidence without exposing upstream account details."""
    if not status.get("running"):
        return None
    age = _age_seconds(status.get("last_run_at"))
    if age is None or age > 3600:
        return None
    diagnostics = status.get("last_diagnostics") or {}
    feedback = diagnostics.get("model_feedback") or {}
    raw_error = str(feedback.get("raw_error") or "")
    if not raw_error:
        return None
    from core.ai.model_feedback_errors import describe_model_feedback_issue
    return str(describe_model_feedback_issue(raw_error)["label"])


def _build_source_health_summary(categories: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    rows = _collect_source_entries(categories)
    health_counts = Counter(str(row.get("health") or "unknown") for row in rows)
    core_rows = [row for row in rows if str(row.get("support_level") or "") == "core"]
    core_blockers = [row for row in core_rows if str(row.get("health") or "") in {"missing", "stale"}]
    core_degraded = [row for row in core_rows if str(row.get("health") or "") == "degraded"]
    if core_blockers:
        support_assessment = "insufficient"
        headline = "美国/中国宏观研究主链路仍有缺口，当前支持度不足。"
    elif core_degraded:
        support_assessment = "partial"
        headline = "核心研究链路基本可用，但仍有退化项需要补强。"
    else:
        support_assessment = "sufficient"
        headline = "核心研究链路已足够支持美国与中国宏观研究，增强源仍可继续完善。"

    recommendations = _dedupe_text_items(
        [row.get("recommendation") for row in rows if str(row.get("health") or "") != "healthy"]
    )[:8]
    problematic_sources = [
        f"{row.get('category')}:{row.get('name')}"
        for row in rows
        if str(row.get("health") or "") in {"missing", "stale", "degraded"}
    ]
    category_counts: Dict[str, Dict[str, int]] = {}
    for category_name, category in (categories or {}).items():
        source_rows = list((category.get("sources") or {}).values())
        counts = Counter(str(row.get("health") or "unknown") for row in source_rows)
        category_counts[category_name] = {
            "total": len(source_rows),
            "healthy": int(counts.get("healthy", 0)),
            "degraded": int(counts.get("degraded", 0)),
            "stale": int(counts.get("stale", 0)),
            "missing": int(counts.get("missing", 0)),
        }

    return {
        "support_assessment": support_assessment,
        "headline": headline,
        "total_sources": len(rows),
        "healthy_count": int(health_counts.get("healthy", 0)),
        "degraded_count": int(health_counts.get("degraded", 0)),
        "stale_count": int(health_counts.get("stale", 0)),
        "missing_count": int(health_counts.get("missing", 0)),
        "core_sources": len(core_rows),
        "core_healthy": len([row for row in core_rows if str(row.get("health") or "") == "healthy"]),
        "core_ready": support_assessment == "sufficient",
        "problematic_sources": problematic_sources[:12],
        "recommendations": recommendations,
        "by_category": category_counts,
    }


async def _build_sources_health_payload() -> Dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    categories: Dict[str, Dict[str, Any]] = {
        "macro": {"label": "US + China Macro", "support_level": "core", "sources": {}},
        "funding": {"label": "Funding Cache", "support_level": "core", "sources": {}},
        "news": {"label": "News & Event Sources", "support_level": "core", "sources": {}},
        "search_interest": {"label": "Search Interest", "support_level": "enhancement", "sources": {}},
        "options": {"label": "Options Structure", "support_level": "enhancement", "sources": {}},
        "premium_onchain": {"label": "Premium / On-chain", "support_level": "optional", "sources": {}},
        "premium_derivatives": {"label": "Premium / Derivatives", "support_level": "optional", "sources": {}},
        "ai_sources": {"label": "AI Sources", "support_level": "core", "sources": {}},
    }

    try:
        from core.data.macro_collector import group_macro_snapshot, load_macro_snapshot, _api_key as _fred_key  # noqa: PLC0415

        snapshot = load_macro_snapshot()
        grouped = group_macro_snapshot(snapshot)
        has_data = any(value is not None for value in snapshot.values())
        key_configured = bool(_fred_key())
        active_series = [name for name, value in snapshot.items() if value is not None]
        issues: List[str] = []
        if not any(snapshot.get(key) is not None for key in ("fed_rate", "cpi_yoy", "ppi_yoy")):
            issues.append("US macro series missing")
        if snapshot.get("ppi_cpi_gap") is None:
            issues.append("US scissors spread missing")
        if not any(snapshot.get(key) is not None for key in ("cn_cpi_yoy", "cn_ppi_yoy")):
            issues.append("China macro series missing")
        if snapshot.get("cn_ppi_cpi_gap") is None:
            issues.append("China scissors spread missing")
        if snapshot.get("cn_m1_m2_gap") is None:
            issues.append("China liquidity scissors missing")
        categories["macro"]["sources"]["fred_macro"] = _build_source_entry(
            label="US + China Macro Composite",
            available=True,
            configured=True,
            key_configured=key_configured,
            has_cached_data=has_data,
            ready=has_data,
            last_updated=_source_latest_cache_mtime("data/macro", active_series or list(snapshot.keys())),
            max_age_sec=14 * 24 * 3600,
            support_level="core",
            issues=issues,
            recommendation="Keep US and China macro cache refreshed so CPI/PPI and scissors metrics stay continuous.",
            snapshot=snapshot,
            extra={
                "active_series": active_series,
                "regions": grouped,
                "focus_regions": ["us", "china"],
                "upstreams": {
                    "market": "yfinance/yahoo",
                    "us_macro": "fred",
                    "china_macro": "stats.gov.cn + pbc.gov.cn",
                },
            },
        )
    except Exception as exc:
        categories["macro"]["sources"]["fred_macro"] = _build_source_entry(
            label="US + China Macro Composite",
            available=False,
            configured=True,
            key_configured=False,
            has_cached_data=False,
            ready=False,
            support_level="core",
            error=str(exc),
            recommendation="Restore macro collection/cache before relying on US and China macro research.",
            extra={"active_series": [], "regions": {"market": {}, "us": {}, "china": {}}, "focus_regions": ["us", "china"]},
        )

    try:
        provider = FundingRateProvider(FundingProviderConfig(exchange="binance", source="local"))
        series = provider.load_local_cache("BTC/USDT", exchange="binance")
        funding_snapshot = _serialize_funding_cache(provider, exchange="binance", symbol="BTC/USDT", series=series)
        rows = int(funding_snapshot.get("rows") or 0)
        last_updated = ((funding_snapshot.get("coverage") or {}).get("end")) if rows > 0 else None
        issues = []
        if rows <= 0:
            issues.append("Funding cache empty")
        if not bool(funding_snapshot.get("cache_exists")):
            issues.append("Funding parquet missing")
        categories["funding"]["sources"]["coinglass_funding_cache"] = _build_source_entry(
            label="CoinGlass Funding Cache",
            available=True,
            configured=True,
            has_cached_data=rows > 0,
            ready=rows > 0,
            last_updated=last_updated,
            max_age_sec=36 * 3600,
            support_level="core",
            issues=issues,
            recommendation="Warm CoinGlass funding cache before research/backtest so perp funding signals remain available.",
            snapshot=funding_snapshot,
            extra={"upstream": "coinglass funding_rate_history", "exchange_http_fallback": "explicit_only"},
        )
    except Exception as exc:
        categories["funding"]["sources"]["coinglass_funding_cache"] = _build_source_entry(
            label="CoinGlass Funding Cache",
            available=False,
            configured=True,
            has_cached_data=False,
            ready=False,
            support_level="core",
            error=str(exc),
            recommendation="Fix CoinGlass funding cache read/write path so perp research does not degrade.",
            extra={"upstream": "coinglass funding_rate_history", "exchange_http_fallback": "explicit_only"},
        )

    news_catalog = _news_source_catalog()
    news_flags = _configured_news_source_flags()
    news_source_names = list(news_catalog.keys())
    try:
        from core.news.collectors.manager import MultiSourceNewsCollector  # noqa: PLC0415

        manager = MultiSourceNewsCollector()
        if manager.sources:
            news_source_names = list(manager.sources)
    except Exception:
        pass

    coverage_payload: Dict[str, Any] = {}
    coverage_error: Optional[str] = None
    source_states: List[Dict[str, Any]] = []
    state_error: Optional[str] = None
    llm_queue: Dict[str, Any] = {}
    queue_error: Optional[str] = None
    try:
        coverage_payload = await news_db.summarize_news_raw_coverage(max_sources=30)
    except Exception as exc:
        coverage_error = str(exc)
    try:
        source_states = await news_db.list_source_states()
    except Exception as exc:
        state_error = str(exc)
    try:
        llm_queue = await news_db.get_llm_queue_stats()
    except Exception as exc:
        queue_error = str(exc)

    coverage_by_source = dict(coverage_payload.get("source_summary") or {})
    states_by_source = {
        str(row.get("source") or "").strip().lower(): dict(row)
        for row in source_states
        if isinstance(row, dict) and str(row.get("source") or "").strip()
    }
    categories["news"]["runtime"] = {
        "tracked_sources": len(news_source_names),
        "coverage_error": coverage_error,
        "state_error": state_error,
        "llm_queue_error": queue_error,
        "llm_queue": llm_queue,
        "coverage": {
            "raw_news_total": int(coverage_payload.get("raw_news_total") or 0),
            "events_total": int(coverage_payload.get("events_total") or 0),
        },
    }
    for source_name in news_source_names:
        meta = dict(news_catalog.get(source_name) or {})
        coverage_row = dict(coverage_by_source.get(source_name) or {})
        state_row = dict(states_by_source.get(source_name) or {})
        configured = bool(news_flags.get(source_name, False))
        inserted_count = int(coverage_row.get("inserted_count") or 0)
        # A successful empty pull is healthy. Failure bookkeeping is not a
        # successful refresh, and article publication time is not fetch time.
        last_updated = state_row.get("last_success_at") or coverage_row.get("latest_at")
        issues = []
        if not configured:
            issues.append("Disabled or key missing" if meta.get("requires_key") else "Disabled")
        elif inserted_count <= 0 and not state_row.get("last_success_at") and not state_row.get("updated_at"):
            issues.append("No successful pull recorded")
        if state_row.get("last_error"):
            issues.append(f"Last error: {state_row.get('last_error')}")
        failure_rate = float(coverage_row.get("failure_rate") or 0.0)
        if failure_rate >= 0.5:
            issues.append(f"Failure rate high: {failure_rate:.0%}")
        if _is_future_timestamp(state_row.get("paused_until")):
            issues.append(f"Paused until {state_row.get('paused_until')}")
        if not state_row and not coverage_row and state_error:
            issues.append(f"State read failed: {state_error}")
        ready = bool(configured and (inserted_count > 0 or state_row.get("last_success_at")))
        if meta.get("requires_key") and not configured:
            recommendation = "Optional keyed source is unavailable; core free/official news sources can still support baseline research."
        elif issues:
            recommendation = "Check news worker, source configuration, and source_state freshness so this feed keeps updating."
        else:
            recommendation = "Keep this source enabled to maintain event coverage."
        categories["news"]["sources"][source_name] = _build_source_entry(
            label=str(meta.get("label") or source_name),
            available=bool(configured or inserted_count > 0 or state_row),
            configured=configured,
            has_cached_data=inserted_count > 0,
            ready=ready,
            last_updated=last_updated,
            max_age_sec=48 * 3600,
            support_level=str(meta.get("support_level") or "enhancement"),
            issues=issues,
            recommendation=recommendation,
            snapshot={
                "inserted_count": inserted_count,
                "latest_at": coverage_row.get("latest_at"),
                "failure_rate": failure_rate,
                "success_count": int(state_row.get("success_count") or 0),
                "error_count": int(state_row.get("error_count") or 0),
            },
            extra={
                "requires_key": bool(meta.get("requires_key")),
                "source_type": meta.get("source_type"),
            },
        )

    try:
        from core.data.google_trends_collector import load_latest as _gt  # noqa: PLC0415

        bitcoin_value = _gt("bitcoin")
        categories["search_interest"]["sources"]["google_trends"] = _build_source_entry(
            label="Google Trends (bitcoin)",
            available=True,
            configured=True,
            has_cached_data=bitcoin_value is not None,
            ready=bitcoin_value is not None,
            last_updated=_source_cache_mtime("data/google_trends", "bitcoin_trends"),
            max_age_sec=72 * 3600,
            support_level="enhancement",
            issues=["Google Trends cache empty"] if bitcoin_value is None else [],
            recommendation="pytrends is unofficial; use it as enhancement data and refresh cache proactively.",
            snapshot={"bitcoin": bitcoin_value},
            extra={"upstream": "pytrends (unofficial)"},
        )
    except Exception as exc:
        categories["search_interest"]["sources"]["google_trends"] = _build_source_entry(
            label="Google Trends (bitcoin)",
            available=False,
            configured=True,
            has_cached_data=False,
            ready=False,
            support_level="enhancement",
            error=str(exc),
            recommendation="Restore pytrends dependency or cache generation if search-interest signals are needed.",
            extra={"upstream": "pytrends (unofficial)"},
        )

    try:
        from core.data.options_collector import options_collector  # noqa: PLC0415

        cached_snapshot = options_collector.load_cached_snapshot("BTC")
        options_snapshot = cached_snapshot.to_dict() if cached_snapshot else {}
        categories["options"]["sources"]["deribit_options"] = _build_source_entry(
            label="Deribit Options Snapshot",
            available=True,
            configured=True,
            has_cached_data=bool(options_snapshot),
            ready=bool(options_snapshot),
            last_updated=options_snapshot.get("timestamp"),
            max_age_sec=6 * 3600,
            support_level="enhancement",
            issues=["Options snapshot not warmed"] if not options_snapshot else [],
            recommendation="Persist options cache if you want this source to survive restarts.",
            snapshot=options_snapshot,
            extra={"upstream": "deribit public REST", "storage": "memory_cache"},
        )
    except Exception as exc:
        categories["options"]["sources"]["deribit_options"] = _build_source_entry(
            label="Deribit Options Snapshot",
            available=False,
            configured=True,
            has_cached_data=False,
            ready=False,
            support_level="enhancement",
            error=str(exc),
            recommendation="Restore Deribit snapshot fetch if options-structure signals are required.",
            extra={"upstream": "deribit public REST", "storage": "memory_cache"},
        )

    try:
        from core.data.coinglass_client import coinglass_key_configured, load_dataset_rows_for_symbol  # noqa: PLC0415

        option_datasets = [
            "option_max_pain",
            "options_info",
            "options_exchange_oi_history",
            "options_exchange_volume_history",
            "option_vs_futures_oi_ratio",
        ]
        active_option_datasets: List[str] = []
        latest_option_ts: Optional[str] = None
        option_rows = 0
        for dataset in option_datasets:
            frame = load_dataset_rows_for_symbol(dataset, "BTC")
            if frame.empty:
                continue
            active_option_datasets.append(dataset)
            option_rows += int(len(frame.index))
            if "source_ts" in frame.columns:
                parsed = pd.to_datetime(frame["source_ts"], utc=True, errors="coerce").dropna()
                if not parsed.empty:
                    ts = parsed.max().isoformat()
                    latest_option_ts = max([latest_option_ts, ts]) if latest_option_ts else ts
        key_configured = bool(coinglass_key_configured())
        categories["options"]["sources"]["coinglass_options"] = _build_source_entry(
            label="CoinGlass Options Cache",
            available=bool(key_configured or active_option_datasets),
            configured=key_configured,
            key_configured=key_configured,
            has_cached_data=bool(active_option_datasets),
            ready=bool(active_option_datasets),
            last_updated=latest_option_ts,
            max_age_sec=6 * 3600,
            support_level="enhancement",
            issues=["CoinGlass options cache empty"] if key_configured and not active_option_datasets else [],
            recommendation="Refresh BTC/ETH CoinGlass options datasets manually or on a low-frequency schedule before options-driven research.",
            snapshot={
                "active_datasets": active_option_datasets,
                "rows": option_rows,
                "latest_source_ts": latest_option_ts,
            },
            extra={"upstream": "coinglass optional options datasets", "symbols": ["BTC", "ETH"]},
        )
    except Exception as exc:
        categories["options"]["sources"]["coinglass_options"] = _build_source_entry(
            label="CoinGlass Options Cache",
            available=False,
            configured=False,
            has_cached_data=False,
            ready=False,
            support_level="enhancement",
            error=str(exc),
            recommendation="Restore CoinGlass options cache probing if options source health is required.",
        )

    premium_specs = [
        {
            "name": "glassnode",
            "label": "Glassnode",
            "module": "core.data.glassnode_collector",
            "load_fn": "load_glassnode_snapshot",
            "key_fn": "_api_key",
            "cache_dir": "data/premium/glassnode",
            "cache_probe": "sopr",
            "recommendation": "Add Glassnode key if you want this on-chain confirmation source.",
        },
        {
            "name": "cryptoquant",
            "label": "CryptoQuant",
            "module": "core.data.cryptoquant_collector",
            "load_fn": "load_cryptoquant_snapshot",
            "key_fn": "_api_key",
            "cache_dir": "data/premium/cryptoquant",
            "cache_probe": "exchange_netflow",
            "recommendation": "Add CryptoQuant key if you want exchange flow confirmation.",
        },
        {
            "name": "nansen",
            "label": "Nansen",
            "module": "core.data.nansen_collector",
            "load_fn": "load_nansen_snapshot",
            "key_fn": "_api_key",
            "cache_dir": "data/premium/nansen",
            "cache_probe": "smart_money_netflow",
            "recommendation": "Add Nansen key if you want smart-money flow confirmation.",
        },
        {
            "name": "kaiko",
            "label": "Kaiko",
            "module": "core.data.kaiko_collector",
            "load_fn": "load_kaiko_snapshot",
            "key_fn": "_api_key",
            "cache_dir": "data/premium/kaiko",
            "cache_probe": "cross_exchange_spread_bps",
            "recommendation": "Add Kaiko key if you want institution-grade market structure enrichment.",
        },
    ]
    for spec in premium_specs:
        try:
            module = __import__(spec["module"], fromlist=[spec["load_fn"], spec["key_fn"]])
            premium_snapshot = getattr(module, spec["load_fn"])()
            key_configured = bool(getattr(module, spec["key_fn"])())
            has_cached_data = any(value is not None for value in premium_snapshot.values())
            issues = []
            if key_configured and not has_cached_data:
                issues.append("Key configured but local cache empty")
            if not key_configured and not has_cached_data:
                issues.append("Key missing and no cache")
            categories["premium_onchain"]["sources"][spec["name"]] = _build_source_entry(
                label=str(spec["label"]),
                available=bool(key_configured or has_cached_data),
                configured=key_configured,
                key_configured=key_configured,
                has_cached_data=has_cached_data,
                ready=has_cached_data,
                last_updated=_source_cache_mtime(str(spec["cache_dir"]), str(spec["cache_probe"])),
                max_age_sec=48 * 3600,
                support_level="optional",
                issues=issues,
                recommendation=str(spec["recommendation"]),
                snapshot=premium_snapshot,
            )
        except Exception as exc:
            categories["premium_onchain"]["sources"][spec["name"]] = _build_source_entry(
                label=str(spec["label"]),
                available=False,
                configured=False,
                key_configured=False,
                has_cached_data=False,
                ready=False,
                support_level="optional",
                error=str(exc),
                recommendation=str(spec["recommendation"]),
            )

    try:
        from core.data.coinglass_client import coinglass_key_configured, load_dataset_rows_for_symbol  # noqa: PLC0415

        onchain_datasets = ["spot_coin_netflow", "exchange_balance_list", "exchange_balance_chart"]
        active_onchain_datasets: List[str] = []
        latest_onchain_ts: Optional[str] = None
        onchain_rows = 0
        for dataset in onchain_datasets:
            frame = load_dataset_rows_for_symbol(dataset, "BTC")
            if frame.empty:
                continue
            active_onchain_datasets.append(dataset)
            onchain_rows += int(len(frame.index))
            if "source_ts" in frame.columns:
                parsed = pd.to_datetime(frame["source_ts"], utc=True, errors="coerce").dropna()
                if not parsed.empty:
                    ts = parsed.max().isoformat()
                    latest_onchain_ts = max([latest_onchain_ts, ts]) if latest_onchain_ts else ts
        key_configured = bool(coinglass_key_configured())
        categories["premium_onchain"]["sources"]["coinglass_onchain"] = _build_source_entry(
            label="CoinGlass On-chain / Flow Cache",
            available=bool(key_configured or active_onchain_datasets),
            configured=key_configured,
            key_configured=key_configured,
            has_cached_data=bool(active_onchain_datasets),
            ready=bool(active_onchain_datasets),
            last_updated=latest_onchain_ts,
            max_age_sec=4 * 3600,
            support_level="optional",
            issues=["CoinGlass on-chain cache empty"] if key_configured and not active_onchain_datasets else [],
            recommendation="Refresh CoinGlass spot netflow and exchange balance datasets at low frequency; use cached values for radar scans.",
            snapshot={
                "active_datasets": active_onchain_datasets,
                "rows": onchain_rows,
                "latest_source_ts": latest_onchain_ts,
            },
            extra={"upstream": "coinglass spot netflow + exchange balance"},
        )
    except Exception as exc:
        categories["premium_onchain"]["sources"]["coinglass_onchain"] = _build_source_entry(
            label="CoinGlass On-chain / Flow Cache",
            available=False,
            configured=False,
            has_cached_data=False,
            ready=False,
            support_level="optional",
            error=str(exc),
            recommendation="Restore CoinGlass on-chain cache probing if premium flow status is required.",
        )

    try:
        from core.data.coinglass_feature_builder import load_coinglass_status_snapshot  # noqa: PLC0415

        coinglass_status = await load_coinglass_status_snapshot()
        active_datasets = list(coinglass_status.get("active_datasets") or [])
        quota_headroom = dict(coinglass_status.get("quota_headroom") or {})
        freshness_sec = coinglass_status.get("freshness_sec")
        issues: List[str] = []
        if bool(coinglass_status.get("key_configured")) and not active_datasets:
            issues.append("CoinGlass key configured but cache is empty")
        if freshness_sec is not None and float(freshness_sec or 0.0) > 1800:
            issues.append("CoinGlass cache is stale")
        categories["premium_derivatives"]["sources"]["coinglass"] = _build_source_entry(
            label="CoinGlass Premium Derivatives",
            available=bool(coinglass_status.get("key_configured") or active_datasets),
            configured=bool(coinglass_status.get("key_configured")),
            key_configured=bool(coinglass_status.get("key_configured")),
            has_cached_data=bool(active_datasets),
            ready=bool(coinglass_status.get("available")),
            last_updated=None,
            max_age_sec=1800,
            support_level="optional",
            issues=issues,
            recommendation="Keep CoinGlass behind the background cache path and watch quota headroom before widening coverage.",
            snapshot={
                "freshness_sec": freshness_sec,
                "active_datasets": active_datasets,
                "quota_headroom": quota_headroom,
                "status_count": len(coinglass_status.get("status") or []),
            },
        )
    except Exception as exc:
        categories["premium_derivatives"]["sources"]["coinglass"] = _build_source_entry(
            label="CoinGlass Premium Derivatives",
            available=False,
            configured=False,
            has_cached_data=False,
            ready=False,
            support_level="optional",
            error=str(exc),
            recommendation="Restore CoinGlass worker/cache wiring if derivatives context is required.",
        )

    primary_openai_key = bool(str(getattr(settings, "OPENAI_API_KEY", "") or "").strip())
    backup_openai_key = bool(str(getattr(settings, "OPENAI_BACKUP_API_KEY", "") or "").strip())
    research_llm_ready = bool(primary_openai_key or backup_openai_key)
    categories["ai_sources"]["sources"]["research_context_llm"] = _build_source_entry(
        label="Research Context LLM",
        available=research_llm_ready,
        configured=research_llm_ready,
        has_cached_data=False,
        ready=research_llm_ready,
        support_level="core",
        issues=[] if research_llm_ready else ["OPENAI_API_KEY / OPENAI_BACKUP_API_KEY missing"],
        recommendation="At least one OpenAI-compatible key is required for AI research planning.",
        snapshot={
            "provider": "codex",
            "default_model": str(getattr(settings, "OPENAI_MODEL", "") or "gpt-5.5"),
            "primary_available": primary_openai_key,
            "backup_available": backup_openai_key,
            "failover_enabled": bool(
                backup_openai_key or str(getattr(settings, "OPENAI_BACKUP_BASE_URL", "") or "").strip()
            ),
        },
    )

    live_cfg = live_decision_router.get_runtime_config()
    live_providers = dict(live_cfg.get("providers") or {})
    live_provider = str(live_cfg.get("provider") or "codex").strip().lower() or "codex"
    live_any_available = any(bool((meta or {}).get("available")) for meta in live_providers.values())
    live_active_available = bool((live_providers.get(live_provider) or {}).get("available"))
    live_issues = []
    if not live_any_available:
        live_issues.append("No live decision provider available")
    if bool(live_cfg.get("provider_fallback")):
        live_issues.append(f"Requested provider unavailable; fell back to {live_provider}")
    categories["ai_sources"]["sources"]["live_decision_llm"] = _build_source_entry(
        label="Live Decision LLM",
        available=live_any_available,
        configured=live_any_available,
        has_cached_data=False,
        ready=live_active_available,
        support_level="enhancement",
        issues=live_issues,
        recommendation="Live-decision AI is optional, but it should not rely on silent provider fallback.",
        snapshot={
            "enabled": bool(live_cfg.get("enabled")),
            "mode": live_cfg.get("mode"),
            "provider": live_provider,
            "model": live_cfg.get("model"),
            "provider_requested": live_cfg.get("provider_requested"),
            "provider_fallback": bool(live_cfg.get("provider_fallback")),
            "providers": live_providers,
        },
    )

    agent_cfg = autonomous_trading_agent.get_runtime_config()
    agent_providers = dict(agent_cfg.get("providers") or {})
    agent_provider = str(agent_cfg.get("provider") or "codex").strip().lower() or "codex"
    agent_any_available = any(bool((meta or {}).get("available")) for meta in agent_providers.values())
    agent_active_available = bool((agent_providers.get(agent_provider) or {}).get("available"))
    agent_issues = []
    if not agent_any_available:
        agent_issues.append("No autonomous agent provider available")
    if bool(agent_cfg.get("provider_fallback")):
        agent_issues.append(f"Requested provider unavailable; fell back to {agent_provider}")
    runtime_issue = _agent_model_runtime_issue(autonomous_trading_agent.get_status())
    if runtime_issue:
        agent_issues.append(runtime_issue)
    safety = dict(agent_cfg.get("safety") or {})
    if bool(agent_cfg.get("enabled")) and str(safety.get("status") or "") not in {"ready", ""}:
        agent_issues.append(f"Safety status: {safety.get('status')}")
    categories["ai_sources"]["sources"]["autonomous_agent_llm"] = _build_source_entry(
        label="Autonomous Agent LLM",
        available=agent_any_available,
        configured=agent_any_available,
        has_cached_data=False,
        ready=agent_active_available,
        support_level="enhancement",
        issues=agent_issues,
        recommendation="Autonomous agent AI is optional, but should stay provider-ready and safety-ready.",
        snapshot={
            "enabled": bool(agent_cfg.get("enabled")),
            "mode": agent_cfg.get("mode"),
            "provider": agent_provider,
            "model": agent_cfg.get("model"),
            "provider_requested": agent_cfg.get("provider_requested"),
            "provider_fallback": bool(agent_cfg.get("provider_fallback")),
            "runtime_profile": agent_cfg.get("runtime_profile"),
            "allow_live": bool(agent_cfg.get("allow_live")),
            "safety": safety,
            "providers": agent_providers,
        },
    )

    try:
        from importlib import util as importlib_util  # noqa: PLC0415

        xgboost_installed = importlib_util.find_spec("xgboost") is not None
    except Exception:
        xgboost_installed = False
    canonical_model_path = Path("models") / "ml_signal_xgb.json"
    alternative_candidates = [
        Path("models") / "ml_signal_xgb",
        Path("models") / "ml_signal_xgb.ubj",
        Path("models") / "ml_signal_xgb.bin",
        Path("models") / "ml_signal_xgb.model",
    ]
    detected_alternatives = [str(path) for path in alternative_candidates if path.exists()]
    ml_ready = bool(xgboost_installed and canonical_model_path.exists())
    ml_issues = []
    if not xgboost_installed:
        ml_issues.append("xgboost not installed")
    if not canonical_model_path.exists():
        if detected_alternatives:
            ml_issues.append("Non-canonical model filename detected; runtime still expects models/ml_signal_xgb.json")
        else:
            ml_issues.append("Default ML model file missing")
    categories["ai_sources"]["sources"]["ml_signal_model"] = _build_source_entry(
        label="ML Signal Model",
        available=bool(xgboost_installed and (canonical_model_path.exists() or detected_alternatives)),
        configured=bool(canonical_model_path.exists()),
        has_cached_data=bool(canonical_model_path.exists()),
        ready=ml_ready,
        last_updated=(
            datetime.fromtimestamp(canonical_model_path.stat().st_mtime, tz=timezone.utc).isoformat()
            if canonical_model_path.exists()
            else None
        ),
        # Model artifacts are versioned files, not live feeds. File age alone
        # says nothing about whether inference is available.
        max_age_sec=None,
        support_level="enhancement",
        issues=ml_issues,
        recommendation="Use the canonical path models/ml_signal_xgb.json so ML signal loading stays deterministic.",
        snapshot={
            "canonical_path": str(canonical_model_path),
            "canonical_path_exists": canonical_model_path.exists(),
            "alternative_candidates": detected_alternatives,
            "xgboost_installed": xgboost_installed,
            "review_recommended": bool(canonical_model_path.exists() and (datetime.now(timezone.utc).timestamp() - canonical_model_path.stat().st_mtime) > 30 * 24 * 3600),
        },
    )

    summary = _build_source_health_summary(categories)
    return {
        "ts": generated_at,
        "focus_regions": ["us", "china"],
        "summary": summary,
        "categories": categories,
    }


def _clear_sources_health_cache_task(task: asyncio.Task) -> None:
    global _SOURCES_HEALTH_CACHE_TASK
    if _SOURCES_HEALTH_CACHE_TASK is task:
        _SOURCES_HEALTH_CACHE_TASK = None
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.debug(f"sources health background refresh failed: {exc}")


def _reset_sources_health_cache_for_tests() -> None:
    global _SOURCES_HEALTH_CACHE_TASK
    task = _SOURCES_HEALTH_CACHE_TASK
    if task is not None and not task.done():
        task.cancel()
    _SOURCES_HEALTH_CACHE_TASK = None
    _SOURCES_HEALTH_CACHE["payload"] = None
    _SOURCES_HEALTH_CACHE["expires_at"] = None


def _start_sources_health_cache_task(*, force: bool = False) -> asyncio.Task:
    global _SOURCES_HEALTH_CACHE_TASK
    task = _SOURCES_HEALTH_CACHE_TASK
    if not force and task is not None and not task.done():
        return task

    async def _runner() -> Dict[str, Any]:
        result = await _build_sources_health_payload()
        _SOURCES_HEALTH_CACHE["payload"] = dict(result)
        _SOURCES_HEALTH_CACHE["expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=_SOURCES_HEALTH_CACHE_TTL_SEC)
        return result

    task = asyncio.create_task(_runner())
    task.add_done_callback(_clear_sources_health_cache_task)
    _SOURCES_HEALTH_CACHE_TASK = task
    return task


async def _get_sources_health_payload(*, force: bool = False) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    expires_at = _SOURCES_HEALTH_CACHE.get("expires_at")
    payload = _SOURCES_HEALTH_CACHE.get("payload")
    if (
        not force
        and isinstance(payload, dict)
        and isinstance(expires_at, datetime)
        and expires_at > now
    ):
        return dict(payload)

    task = _start_sources_health_cache_task(force=force)
    if not force and isinstance(payload, dict):
        return dict(payload)

    try:
        result = await asyncio.wait_for(asyncio.shield(task), timeout=max(0.01, float(_SOURCES_HEALTH_BUILD_TIMEOUT_SEC)))
        return dict(result)
    except asyncio.TimeoutError:
        return _operating_mode_source_health_fallback("sources_health_build_timeout")
    except Exception as exc:
        if isinstance(payload, dict):
            return dict(payload)
        return _operating_mode_source_health_fallback("sources_health_build_failed", error=str(exc))


@router.get("/sources/health")
async def get_sources_health(force: bool = False):
    """Return a unified health inventory for data and AI sources."""
    return await _get_sources_health_payload(force=force)


@router.get("/operating-mode")
async def get_operating_mode(request: Request = None):
    """Return the authoritative AI/runtime operating-mode snapshot."""
    payload = await _get_operating_mode_payload()
    if request is not None:
        from core.ai.autonomous_research_loop import get_research_loop
        loop = get_research_loop(request.app).status()
        payload["research_planner"] = {**payload.get("research_planner", {}), "autonomous_loop": loop["config"]["enabled"], "loop": loop}
    return payload


@router.get("/research-loop")
async def get_ai_research_loop(request: Request):
    from core.ai.autonomous_research_loop import get_research_loop
    return get_research_loop(request.app).status()


@router.patch("/research-loop", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def configure_ai_research_loop(request: Request, payload: Dict[str, Any]):
    from core.ai.autonomous_research_loop import ResearchLoopConfig, get_research_loop
    from pydantic import ValidationError
    loop = get_research_loop(request.app)
    if set(payload) - set(ResearchLoopConfig.model_fields):
        raise HTTPException(status_code=422, detail="Unknown research loop configuration field")
    try:
        config = ResearchLoopConfig.model_validate({**loop.state["config"], **payload})
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return loop.configure(config)


@router.post("/research-loop/run", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def run_ai_research_loop_once(request: Request):
    from core.ai.autonomous_research_loop import get_research_loop
    loop = get_research_loop(request.app)
    snapshot = loop.status()
    if snapshot['attempts_today'] >= snapshot['config']['max_rounds_per_day']:
        raise HTTPException(status_code=409, detail='今日研究预算已用完，请等待预算重置')
    return loop.request_run()


@router.post("/research-loop/reset-budget", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def reset_ai_research_loop_budget(request: Request):
    from core.ai.autonomous_research_loop import get_research_loop
    from core.ai.research_loop_evaluation import ResearchStageError
    try:
        return get_research_loop(request.app).reset_daily_budget()
    except ResearchStageError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/research-loop-v2")
async def get_ai_research_loop_v2(request: Request):
    from core.ai.research_loop_v2 import get_cross_sectional_loop
    return get_cross_sectional_loop(request.app).status()


@router.patch("/research-loop-v2", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def configure_ai_research_loop_v2(request: Request, payload: Dict[str, Any]):
    from core.ai.research_loop_v2 import CrossSectionalLoopConfig, get_cross_sectional_loop
    from pydantic import ValidationError
    loop = get_cross_sectional_loop(request.app)
    if set(payload) - set(CrossSectionalLoopConfig.model_fields):
        raise HTTPException(status_code=422, detail="Unknown research loop v2 configuration field")
    try:
        config = CrossSectionalLoopConfig.model_validate({**loop.state["config"], **payload})
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return loop.configure(config)


@router.post("/research-loop-v2/run", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def run_ai_research_loop_v2_once(request: Request):
    from core.ai.research_loop_v2 import get_cross_sectional_loop
    loop = get_cross_sectional_loop(request.app)
    snapshot = loop.status()
    if snapshot["attempts_today"] >= snapshot["config"]["max_rounds_per_day"]:
        raise HTTPException(status_code=409, detail="今日研究预算已用完，请等待预算重置")
    return loop.request_run()


@router.get("/listing-short")
async def get_listing_short_tracker():
    """Paper tracker for the new-perp D2->D14 short (research only)."""
    from core.research import listing_short_tracker
    from core.research.exchange_research_runner import status as runner_status

    state = await asyncio.to_thread(listing_short_tracker.load_state)
    trades = sorted(state.get("trades", {}).values(), key=lambda t: -int(t.get("onboard_ms") or 0))
    return {"summary": listing_short_tracker.summary(state), "trades": trades[:60], "runner": runner_status()}


@router.get("/unlock-short")
async def get_unlock_short_tracker():
    """Forward paper tracker: short perps before large cliff unlocks (research only)."""
    from core.research import unlock_short_tracker

    state = await asyncio.to_thread(unlock_short_tracker.load_state)
    trades = sorted(state.get("trades", {}).values(), key=lambda t: t.get("unlock_date") or "", reverse=True)
    return {"summary": unlock_short_tracker.summary(state), "trades": trades[:80]}


@router.get("/supply-factor")
async def get_supply_factor_tracker():
    """Forward paper tracker: monthly long/short on scheduled supply growth (research only)."""
    from core.research import supply_factor_tracker

    state = await asyncio.to_thread(supply_factor_tracker.load_state)
    months = sorted(state.get("months", {}).values(), key=lambda m: m.get("rebalance_day") or "", reverse=True)
    return {"summary": supply_factor_tracker.summary(state), "months": months[:24]}


@router.get("/upbit-caution")
async def get_upbit_caution_tracker():
    """Forward paper tracker: 7-day perp short after an Upbit caution designation (research only)."""
    from core.research import upbit_caution_tracker

    state = await asyncio.to_thread(upbit_caution_tracker.load_state)
    trades = sorted(state.get("trades", {}).values(), key=lambda t: t.get("notice_at") or "", reverse=True)
    return {"summary": upbit_caution_tracker.summary(state), "trades": trades[:80]}


@router.get("/upbit-krw-listing")
async def get_upbit_krw_listing_tracker():
    """Forward paper tracker: 7-day perp short after an Upbit KRW-market listing (research only)."""
    from core.research import upbit_caution_tracker

    path = upbit_caution_tracker.KRW_LISTING_STATE_PATH
    state = await asyncio.to_thread(upbit_caution_tracker.load_state, path)
    trades = sorted(state.get("trades", {}).values(), key=lambda t: t.get("notice_at") or "", reverse=True)
    return {"summary": upbit_caution_tracker.summary(state, "krw_listing"), "trades": trades[:80]}


async def _build_operating_mode_payload() -> Dict[str, Any]:
    from core.runtime.operating_mode import validate_operating_mode
    from core.runtime.state import runtime_state

    sources = await _build_sources_health_payload()
    snapshot = validate_operating_mode(
        live_decision_config=live_decision_router.get_runtime_config(),
        agent_config=autonomous_trading_agent.get_runtime_config(),
        runtime_state_snapshot=runtime_state.snapshot(),
        source_health=sources,
        trading_mode=_current_trading_mode(),
    )
    return snapshot.to_dict()


def _clear_operating_mode_cache_task(task: asyncio.Task) -> None:
    global _OPERATING_MODE_CACHE_TASK
    if _OPERATING_MODE_CACHE_TASK is task:
        _OPERATING_MODE_CACHE_TASK = None
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.debug(f"operating mode background refresh failed: {exc}")


def _reset_operating_mode_cache_for_tests() -> None:
    global _OPERATING_MODE_CACHE_TASK
    task = _OPERATING_MODE_CACHE_TASK
    if task is not None and not task.done():
        task.cancel()
    _OPERATING_MODE_CACHE_TASK = None
    _OPERATING_MODE_CACHE["payload"] = None
    _OPERATING_MODE_CACHE["expires_at"] = None


def _operating_mode_source_health_fallback(reason: str, *, error: Optional[str] = None) -> Dict[str, Any]:
    issues = _dedupe_text_items([reason, error])
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "focus_regions": ["us", "china"],
        "summary": {
            "support_assessment": "unknown",
            "headline": "source health build did not complete before the operating-mode response budget",
            "total_sources": 1,
            "healthy_count": 0,
            "degraded_count": 1,
            "stale_count": 0,
            "missing_count": 0,
            "core_sources": 1,
            "core_healthy": 0,
            "core_ready": False,
            "problematic_sources": ["runtime:source_health_build"],
            "recommendations": ["Retry source-health diagnostics or inspect the background cache refresh."],
            "by_category": {"runtime": {"total": 1, "healthy": 0, "degraded": 1, "stale": 0, "missing": 0}},
        },
        "categories": {},
        "items": [
            {
                "key": "source_health_build",
                "label": "Source Health Build",
                "health": "degraded",
                "ready": False,
                "support_level": "core",
                "issues": issues,
            }
        ],
        "cache_status": "fallback",
        "degraded_reason": reason,
    }


def _build_operating_mode_fallback_payload(reason: str, *, error: Optional[str] = None) -> Dict[str, Any]:
    from core.runtime.operating_mode import validate_operating_mode
    from core.runtime.state import runtime_state

    try:
        runtime_snapshot = runtime_state.snapshot()
    except Exception as exc:
        runtime_snapshot = {"error": str(exc)}

    snapshot = validate_operating_mode(
        live_decision_config=live_decision_router.get_runtime_config(),
        agent_config=autonomous_trading_agent.get_runtime_config(),
        runtime_state_snapshot=runtime_snapshot,
        source_health=_operating_mode_source_health_fallback(reason, error=error),
        trading_mode=_current_trading_mode(),
    ).to_dict()
    snapshot["operating_mode_cache"] = {
        "status": "fallback",
        "reason": reason,
        "error": error,
    }
    return snapshot


def _start_operating_mode_cache_task(*, force: bool = False) -> asyncio.Task:
    global _OPERATING_MODE_CACHE_TASK
    task = _OPERATING_MODE_CACHE_TASK
    if not force and task is not None and not task.done():
        return task

    async def _runner() -> Dict[str, Any]:
        result = await _build_operating_mode_payload()
        _OPERATING_MODE_CACHE["payload"] = dict(result)
        _OPERATING_MODE_CACHE["expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=_OPERATING_MODE_CACHE_TTL_SEC)
        return result

    task = asyncio.create_task(_runner())
    task.add_done_callback(_clear_operating_mode_cache_task)
    _OPERATING_MODE_CACHE_TASK = task
    return task


async def _get_operating_mode_payload(*, force: bool = False) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    expires_at = _OPERATING_MODE_CACHE.get("expires_at")
    payload = _OPERATING_MODE_CACHE.get("payload")
    if (
        not force
        and isinstance(payload, dict)
        and isinstance(expires_at, datetime)
        and expires_at > now
    ):
        return dict(payload)

    task = _start_operating_mode_cache_task(force=force)
    if not force and isinstance(payload, dict):
        return dict(payload)

    try:
        result = await asyncio.wait_for(asyncio.shield(task), timeout=max(0.01, float(_OPERATING_MODE_BUILD_TIMEOUT_SEC)))
        return dict(result)
    except asyncio.TimeoutError:
        return _build_operating_mode_fallback_payload("source_health_build_timeout")
    except Exception as exc:
        if isinstance(payload, dict):
            return dict(payload)
        return _build_operating_mode_fallback_payload("source_health_build_failed", error=str(exc))


@router.get("/gate-audit/summary")
async def get_gate_audit_summary(limit: int = 500):
    from core.audit.gate_counterfactuals import summarize_gate_counterfactuals

    return summarize_gate_counterfactuals(limit=max(1, min(int(limit), 5000)))


@router.get("/premium-data/status")
async def get_premium_data_status():
    """Backward-compatible premium data status with richer metadata."""
    payload = await _get_sources_health_payload()
    # The cached source-health path is time-bounded and can return a degraded
    # fallback (empty `categories`) when the background build is slow under DB or
    # network contention. This endpoint must expose the detailed per-source
    # inventory, so rebuild directly when the cached payload came back degraded
    # rather than handing callers empty source dicts.
    if payload.get("cache_status") == "fallback" or not payload.get("categories"):
        payload = await _build_sources_health_payload()
    categories = payload.get("categories") or {}
    sources = {
        "glassnode": dict((((categories.get("premium_onchain") or {}).get("sources") or {}).get("glassnode") or {})),
        "cryptoquant": dict((((categories.get("premium_onchain") or {}).get("sources") or {}).get("cryptoquant") or {})),
        "nansen": dict((((categories.get("premium_onchain") or {}).get("sources") or {}).get("nansen") or {})),
        "kaiko": dict((((categories.get("premium_onchain") or {}).get("sources") or {}).get("kaiko") or {})),
        "coinglass": dict((((categories.get("premium_derivatives") or {}).get("sources") or {}).get("coinglass") or {})),
        "google_trends": dict((((categories.get("search_interest") or {}).get("sources") or {}).get("google_trends") or {})),
        "fred_macro": dict((((categories.get("macro") or {}).get("sources") or {}).get("fred_macro") or {})),
        "deribit_options": dict((((categories.get("options") or {}).get("sources") or {}).get("deribit_options") or {})),
    }
    return {
        "sources": sources,
        "focus_regions": payload.get("focus_regions") or ["us", "china"],
        "summary": payload.get("summary") or {},
        "ts": payload.get("ts") or datetime.now(timezone.utc).isoformat(),
    }
