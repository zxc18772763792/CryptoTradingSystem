"""Web API for AI research workbench."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from loguru import logger
import pandas as pd
from pydantic import BaseModel, Field

from config.database import StrategyPerformanceSnapshot, async_session_maker
from config.settings import settings
from core.backtest.funding_provider import FundingProviderConfig, FundingRateProvider
from core.governance.audit import GovernanceAuditEvent, write_audit
from core.deployment.promotion_engine import transition_candidate, transition_proposal
from core.news.storage import db as news_db
from core.research.orchestrator import (
    cancel_proposal_job,
    create_manual_proposal,
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


router = APIRouter()


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


class AICandidatePromotionRequest(BaseModel):
    target: Optional[str] = None


class AICandidateRegisterRequest(BaseModel):
    mode: str = "paper"       # paper | live_candidate
    name: Optional[str] = None  # optional custom strategy name (stored in metadata)


class AIHumanApprovalRequest(BaseModel):
    target: Optional[str] = None   # paper | live_candidate
    notes: str = ""


class AIResearchContextRequest(BaseModel):
    market_summary: Dict[str, Any] = Field(default_factory=dict)
    goals: str = ""
    timeout: int = Field(default=30, ge=5, le=90)


class AIRetireRequest(BaseModel):
    notes: str = ""


class AIFundingWarmRequest(BaseModel):
    exchange: str = "binance"
    symbol: str = "BTC/USDT"
    days: int = Field(default=60, ge=1, le=3650)
    source: str = "auto"


def _proposal_job_summary(app: Request | Any, proposal_id: str, preferred_job_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    jobs = dict(getattr(app.state, "research_jobs", {}) or {})
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


def _news_key(symbol: str) -> str:
    raw = _normalize_symbol(symbol).split(":")[0]
    if "/" in raw:
        return raw.split("/", 1)[0]
    return raw.replace("USDT", "") or raw


def _serialize_funding_cache(provider: FundingRateProvider, *, exchange: str, symbol: str, series) -> Dict[str, Any]:
    path = provider._cache_path(symbol, exchange=exchange)  # noqa: SLF001
    rows = int(len(series)) if series is not None else 0
    latest_rate = None
    coverage = {"start": None, "end": None}
    if rows > 0:
        try:
            coverage = {
                "start": pd.Timestamp(series.index.min()).isoformat(),
                "end": pd.Timestamp(series.index.max()).isoformat(),
            }
            latest_rate = float(series.iloc[-1])
        except Exception:
            coverage = {"start": None, "end": None}
            latest_rate = None
    return {
        "exchange": exchange,
        "symbol": symbol,
        "cache_path": str(path),
        "cache_exists": bool(path.exists()),
        "rows": rows,
        "latest_rate": latest_rate,
        "coverage": coverage,
        "storage": {
            "mode": "parquet",
            "description": "资金费率历史缓存写入本地 Parquet，研究回测直接读取",
        },
    }


@router.get("/runtime-config")
async def get_ai_runtime_config(request: Request):
    """Expose lightweight runtime switches for the AI research UI."""
    ensure_ai_research_runtime_state(request.app)
    return {
        "governance_enabled": bool(getattr(settings, "GOVERNANCE_ENABLED", True)),
        "decision_mode": str(getattr(settings, "DECISION_MODE", "shadow") or "shadow"),
        "trading_mode": str(getattr(settings, "TRADING_MODE", "paper") or "paper"),
    }


@router.post("/proposals/generate")
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


@router.post("/proposals")
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


@router.delete("/proposals/{proposal_id}")
async def delete_ai_proposal(request: Request, proposal_id: str):
    ensure_ai_research_runtime_state(request.app)
    result = delete_proposal(
        request.app,
        proposal_id=proposal_id,
        actor="web_ui",
    )
    return result


@router.post("/proposals/{proposal_id}/retire")
async def retire_ai_proposal(request: Request, proposal_id: str, payload: AIRetireRequest = AIRetireRequest()):
    ensure_ai_research_runtime_state(request.app)
    proposal = get_proposal(request.app, proposal_id)
    proposal_status = str(proposal.status or "")
    if proposal_status in {"research_queued", "research_running", "paper_running"}:
        raise HTTPException(status_code=409, detail=f"proposal in state {proposal_status}, retire is not allowed")

    retired_candidates = 0
    all_candidates = request.app.state.ai_candidate_registry.list(limit=None)
    for cand in all_candidates:
        if str(cand.proposal_id) != str(proposal_id):
            continue
        status = str(cand.status or "")
        if status == "paper_running":
            raise HTTPException(status_code=409, detail="paper_running candidate must be stopped before retire")
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


@router.post("/proposals/{proposal_id}/run")
async def run_ai_proposal_endpoint(request: Request, proposal_id: str, payload: AIProposalRunRequest):
    ensure_ai_research_runtime_state(request.app)
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
            "source": "local_cache",
            "error": cache_error,
        },
        "how_to_enable": [
            "点击“预热宏观缓存”，系统会从 Binance 公共资金费率接口抓取历史并写入本地。",
            "缓存文件保存在 data/funding/<exchange>/<symbol>_funding.parquet。",
            "研究回测会自动读取本地缓存；有缓存时宏观层中的 funding 会显示为已启用。",
        ],
    }


@router.post("/diagnostics/funding-cache/warm")
async def warm_ai_funding_cache(request: Request, payload: AIFundingWarmRequest):
    ensure_ai_research_runtime_state(request.app)
    exchange_norm = _normalize_exchange(payload.exchange)
    symbol_norm = _normalize_symbol(payload.symbol)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=max(1, min(int(payload.days or 60), 3650)))
    provider = FundingRateProvider(FundingProviderConfig(exchange=exchange_norm, source=str(payload.source or "auto")))
    try:
        series = provider.ensure_history(
            symbol_norm,
            start_time=start_time,
            end_time=end_time,
            source=str(payload.source or "auto"),
            save=True,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"funding warm failed: {exc}") from exc
    return {
        "warmed": True,
        "funding": {
            **_serialize_funding_cache(provider, exchange=exchange_norm, symbol=symbol_norm, series=series),
            "requested_days": int(payload.days),
            "source": str(payload.source or "auto"),
        },
    }


@router.post("/proposals/{proposal_id}/cancel")
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
    job: dict = {}
    if job_id:
        raw = request.app.state.research_jobs.get(job_id) or {}
        job = {k: v for k, v in raw.items() if k != "result"}
    return {
        "proposal_id": proposal_id,
        "proposal_status": item.status,
        "job_id": job_id,
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


@router.get("/candidates/{candidate_id}/lifecycle")
async def get_ai_candidate_lifecycle(request: Request, candidate_id: str, limit: int = 200):
    ensure_ai_research_runtime_state(request.app)
    _ = get_candidate(request.app, candidate_id)
    rows = list_lifecycle(request.app, "candidate", candidate_id, limit=limit)
    return {"candidate_id": candidate_id, "items": rows, "count": len(rows)}


@router.post("/candidates/{candidate_id}/promote")
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


@router.post("/candidates/{candidate_id}/register")
async def register_ai_candidate(request: Request, candidate_id: str, payload: AICandidateRegisterRequest):
    """One-click: promote + register a validated candidate as a live strategy instance."""
    ensure_ai_research_runtime_state(request.app)
    if bool(getattr(settings, "GOVERNANCE_ENABLED", True)):
        raise HTTPException(
            status_code=409,
            detail="governance enabled: one-click register disabled; use pending-approval + human-approve workflow",
        )
    # Persist custom display name into candidate metadata before promoting
    if payload.name:
        try:
            cand = get_candidate(request.app, candidate_id)
            cand.metadata["display_name"] = payload.name
            request.app.state.ai_candidate_registry.save(cand)
        except Exception:
            pass
    target = "paper" if payload.mode == "shadow" else payload.mode
    valid_modes = {"paper", "live_candidate"}
    target = target if target in valid_modes else "paper"
    result = await promote_existing_candidate(
        request.app, candidate_id=candidate_id, actor="web_ui_register", target=target
    )
    return {
        "candidate_id": candidate_id,
        "candidate": result["candidate"].model_dump(mode="json"),
        "proposal": result["proposal"].model_dump(mode="json"),
        "promotion": result["promotion"].model_dump(mode="json"),
        "runtime_status": result.get("runtime_status"),
        "registered_strategy_name": result.get("registered_strategy_name"),
    }


@router.post("/candidates/{candidate_id}/human-approve")
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
    cand.promotion.decision = target

    # Clear the gate; record approval metadata
    cand.metadata.pop("promotion_pending_human_gate", None)
    proposal.metadata.pop("promotion_pending_human_gate", None)
    cand.metadata["human_approved_at"] = datetime.now(timezone.utc).isoformat()
    cand.metadata["human_approved_target"] = target
    cand.metadata["human_approval_notes"] = payload.notes

    # Promote (bypasses the 409 guard in promote_existing_candidate)
    result = await promote_candidate(
        request.app, proposal=proposal, candidate=cand, promotion=cand.promotion, actor="human_approver"
    )
    request.app.state.ai_candidate_registry.save(cand)
    save_proposal(request.app, proposal)

    asyncio.create_task(write_audit(
        GovernanceAuditEvent(
            module="ai.research",
            action="human_approve",
            status="approved",
            actor="web_ui",
            role="HUMAN",
            input_payload={"candidate_id": candidate_id, "target": target, "notes": payload.notes},
            output_payload={"runtime_status": result.get("runtime_status")},
        )
    ))
    return {
        "candidate_id": candidate_id,
        "candidate": result["candidate"].model_dump(mode="json"),
        "promotion": result["promotion"].model_dump(mode="json"),
        "runtime_status": result.get("runtime_status"),
        "registered_strategy_name": result.get("registered_strategy_name"),
    }


@router.post("/candidates/{candidate_id}/human-reject")
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

    asyncio.create_task(write_audit(
        GovernanceAuditEvent(
            module="ai.research",
            action="human_reject",
            status="denied",
            actor="web_ui",
            role="HUMAN",
            input_payload={"candidate_id": candidate_id, "reason": payload.notes},
            output_payload={"status": "retired"},
        )
    ))
    return {
        "candidate_id": candidate_id,
        "status": "retired",
        "candidate": cand.model_dump(mode="json"),
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


@router.get("/candidates/{candidate_id}/decay-check")
async def get_candidate_decay_check(request: Request, candidate_id: str):
    """Run CUSUM decay detection on the candidate's live/paper trade history.

    Reads the risk_manager's in-memory trade history, filters by strategy name,
    converts to a return series, runs CUSUM, persists summary to candidate
    metadata, and returns the full CUSUM result dict.
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
    cand.metadata["cusum_status"] = status_summary
    request.app.state.ai_candidate_registry.save(cand)

    return {"candidate_id": candidate_id, "cusum": result, "strategy_name": strat_name, "n_trades": len(filtered)}


@router.get("/promotions")
async def get_ai_promotions(request: Request, limit: int = 50):
    ensure_ai_research_runtime_state(request.app)
    rows = list_promotions(request.app, limit=limit)
    return {"items": rows, "count": len(rows)}


@router.get("/deployments/status")
async def get_ai_deployments_status(request: Request):
    ensure_ai_research_runtime_state(request.app)
    return get_deployment_status(request.app)


@router.post("/research/generate-context")
async def generate_research_context_endpoint(request: Request, payload: AIResearchContextRequest):
    """Call GLM to generate a research hypothesis + experiment plan from market signals."""
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
    import pandas as pd

    from core.ai.signal_aggregator import signal_aggregator
    from core.data import data_storage

    market_data: Optional[pd.DataFrame] = None
    try:
        from datetime import datetime, timedelta, timezone

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=48)
        market_data = await data_storage.load_klines_from_parquet(
            exchange="binance",
            symbol=symbol,
            timeframe="1h",
            start_time=start_time,
            end_time=end_time,
        )
    except Exception:
        pass

    if market_data is None:
        market_data = pd.DataFrame()

    try:
        agg = await signal_aggregator.aggregate(symbol=symbol, market_data=market_data)
        return agg.to_dict()
    except Exception as exc:
        return {
            "symbol": symbol,
            "direction": "FLAT",
            "confidence": 0.0,
            "requires_approval": True,
            "blocked_by_risk": False,
            "risk_reason": "",
            "components": {},
            "error": str(exc),
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


@router.post("/performance/snapshots")
async def save_performance_snapshot(body: PerformanceSnapshotRequest):
    """Persist a strategy performance snapshot to the DB."""
    from sqlalchemy import insert as sa_insert

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
    return {"ok": True, "snapshot_at": row.snapshot_at.isoformat()}


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
