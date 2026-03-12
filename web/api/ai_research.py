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
from core.ai.autonomous_agent import autonomous_trading_agent
from core.ai.live_decision_router import live_decision_router
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
    enabled: Optional[bool] = None
    auto_start: Optional[bool] = None
    mode: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    interval_sec: Optional[int] = Field(default=None, ge=15, le=7200)
    lookback_bars: Optional[int] = Field(default=None, ge=30, le=4000)
    min_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    default_leverage: Optional[float] = Field(default=None, ge=1.0, le=125.0)
    max_leverage: Optional[float] = Field(default=None, ge=1.0, le=125.0)
    default_stop_loss_pct: Optional[float] = Field(default=None, ge=0.001, le=0.5)
    default_take_profit_pct: Optional[float] = Field(default=None, ge=0.001, le=2.0)
    timeout_ms: Optional[int] = Field(default=None, ge=1000, le=120000)
    max_tokens: Optional[int] = Field(default=None, ge=32, le=4096)
    temperature: Optional[float] = Field(default=None, ge=0.0, le=1.5)
    cooldown_sec: Optional[int] = Field(default=None, ge=0, le=86400)
    allow_live: Optional[bool] = None
    account_id: Optional[str] = None
    strategy_name: Optional[str] = None


class AIAutonomousAgentStartRequest(BaseModel):
    enable: bool = True


class AIAutonomousAgentRunOnceRequest(BaseModel):
    force: bool = False


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
        "ai_live_decision": live_decision_router.get_runtime_config(),
        "ai_autonomous_agent": autonomous_trading_agent.get_runtime_config(),
    }


@router.get("/runtime-config/live-decision")
async def get_ai_live_decision_runtime_config(request: Request):
    ensure_ai_research_runtime_state(request.app)
    return live_decision_router.get_runtime_config()


@router.post("/runtime-config/live-decision")
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


@router.get("/runtime-config/autonomous-agent")
async def get_ai_autonomous_agent_runtime_config(request: Request):
    ensure_ai_research_runtime_state(request.app)
    return autonomous_trading_agent.get_runtime_config()


@router.post("/runtime-config/autonomous-agent")
async def update_ai_autonomous_agent_runtime_config(
    request: Request,
    payload: AIAutonomousAgentConfigUpdateRequest,
):
    ensure_ai_research_runtime_state(request.app)
    try:
        updated = await autonomous_trading_agent.update_runtime_config(**payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": True, "config": updated}


@router.get("/autonomous-agent/status")
async def get_ai_autonomous_agent_status(request: Request):
    ensure_ai_research_runtime_state(request.app)
    return {
        "status": autonomous_trading_agent.get_status(),
        "config": autonomous_trading_agent.get_runtime_config(),
    }


@router.post("/autonomous-agent/start")
async def start_ai_autonomous_agent(
    request: Request,
    payload: AIAutonomousAgentStartRequest = AIAutonomousAgentStartRequest(),
):
    ensure_ai_research_runtime_state(request.app)
    if payload.enable:
        await autonomous_trading_agent.update_runtime_config(enabled=True)
    status = await autonomous_trading_agent.start()
    return {"started": True, "status": status, "config": autonomous_trading_agent.get_runtime_config()}


@router.post("/autonomous-agent/stop")
async def stop_ai_autonomous_agent(request: Request):
    ensure_ai_research_runtime_state(request.app)
    status = await autonomous_trading_agent.stop()
    return {"stopped": True, "status": status, "config": autonomous_trading_agent.get_runtime_config()}


@router.post("/autonomous-agent/run-once")
async def run_ai_autonomous_agent_once(
    request: Request,
    payload: AIAutonomousAgentRunOnceRequest = AIAutonomousAgentRunOnceRequest(),
):
    ensure_ai_research_runtime_state(request.app)
    result = await autonomous_trading_agent.run_once(trigger="api_manual", force=bool(payload.force))
    return {"result": result, "status": autonomous_trading_agent.get_status()}


@router.get("/autonomous-agent/journal")
async def get_ai_autonomous_agent_journal(request: Request, limit: int = 50):
    ensure_ai_research_runtime_state(request.app)
    rows = autonomous_trading_agent.read_journal(limit=limit)
    return {"items": rows, "count": len(rows)}


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


@router.get("/candidates/{candidate_id}/param-sensitivity")
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
        base_result = _run_backtest_core(
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
                result = _run_backtest_core(
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


# ── Phase A: Live signals for all active candidates ───────────────────────────

@router.get("/live-signals")
@router.get("/candidates/live-signals")
async def get_live_signals(request: Request, symbol: Optional[str] = None):
    """Return SignalAggregator output for all paper_running/live_running candidates.

    Uses the module-level signal_aggregator singleton (same one used by /signals/latest).
    Market data loaded from local parquet cache — no live network calls.
    """
    import pandas as pd
    from core.ai.signal_aggregator import signal_aggregator
    from core.data import data_storage

    ensure_ai_research_runtime_state(request.app)

    all_candidates = list_candidates(request.app, limit=200)
    active = [
        c for c in all_candidates
        if str(c.status) in {"paper_running", "shadow_running", "live_running", "live_candidate"}
    ]
    if symbol:
        sym_norm = _normalize_symbol(symbol)
        active = [c for c in active if _candidate_primary_symbol(c) == sym_norm]

    results = []
    for cand in active:
        cand_symbol = _candidate_primary_symbol(cand)
        cand_strategy = _candidate_strategy_name(cand)
        cand_status = str(getattr(cand, "status", "unknown"))
        try:
            df = pd.DataFrame()
            try:
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(hours=48)
                loaded = await data_storage.load_klines_from_parquet(
                    exchange="binance", symbol=cand_symbol,
                    timeframe="1h", start_time=start_time, end_time=end_time,
                )
                if loaded is not None:
                    df = loaded
            except Exception:
                pass
            sig = await signal_aggregator.aggregate(cand_symbol, df)
            results.append({
                "candidate_id": cand.candidate_id,
                "strategy": cand_strategy,
                "symbol": cand_symbol,
                "status": cand_status,
                "signal": sig.to_dict(),
            })
        except Exception as exc:
            logger.debug(f"live-signals: error for {cand.candidate_id}: {exc}")
            results.append({
                "candidate_id": cand.candidate_id,
                "strategy": cand_strategy,
                "symbol": cand_symbol,
                "status": cand_status,
                "signal": None,
                "error": str(exc),
            })

    return {"items": results, "count": len(results), "ts": datetime.now(timezone.utc).isoformat()}


# ── Phase B: Quick register (human-approve shortcut with allocation_pct) ─────

class AIQuickRegisterRequest(BaseModel):
    allocation_pct: float = Field(default=0.05, ge=0.001, le=1.0)


@router.post("/candidates/{candidate_id}/quick-register")
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

    if not cand.metadata.get("promotion_pending_human_gate"):
        raise HTTPException(
            status_code=400,
            detail="candidate is not pending human approval; use /register for non-gated candidates",
        )

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

    result = await promote_candidate(
        request.app,
        proposal=proposal,
        candidate=cand,
        promotion=cand.promotion,
        actor="quick_register",
    )
    request.app.state.ai_candidate_registry.save(result["candidate"])
    save_proposal(request.app, result["proposal"])

    asyncio.create_task(write_audit(
        GovernanceAuditEvent(
            module="ai.research",
            action="quick_register",
            status="approved",
            actor="web_ui",
            role="HUMAN",
            input_payload={"candidate_id": candidate_id, "allocation_pct": payload.allocation_pct},
            output_payload={"runtime_status": result.get("runtime_status")},
        )
    ))
    return {
        "candidate_id": candidate_id,
        "allocation_pct": payload.allocation_pct,
        "candidate": result["candidate"].model_dump(mode="json"),
        "promotion": result["promotion"].model_dump(mode="json"),
        "runtime_status": result.get("runtime_status"),
        "registered_strategy_name": result.get("registered_strategy_name"),
    }


# ── Phase D — Order Preview (read-only, no order placed) ──────────────────────

@router.post("/candidates/{candidate_id}/order-preview")
async def generate_order_preview(request: Request, candidate_id: str):
    """Generate a suggested order preview from SignalAggregator. Does NOT place any order.

    Returns direction, estimated size, stop/take levels, and component breakdown.
    The candidate must be in validated/paper_running/shadow_running/live_candidate/live_running.
    """
    import pandas as pd

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

    # Load market data from strategy_manager cache (non-blocking, 30s TTL)
    df = pd.DataFrame()
    try:
        from core.strategies import strategy_manager as sm  # noqa: PLC0415
        df = sm._load_market_data(cand_symbol, "1h")
    except Exception:
        pass

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
        "note": "此为预览，不会自动下单。确认后请在交易面板手动执行。",
        "ts": sig.timestamp.isoformat(),
    }


# ── Premium Data Status ────────────────────────────────────────────────────────

@router.get("/premium-data/status")
async def get_premium_data_status():
    """Return availability and latest snapshot for all premium/optional data sources.

    Sources: Glassnode, CryptoQuant, Nansen, Kaiko, Google Trends, FRED Macro.
    Each entry includes: available (bool), last_updated (str|None), snapshot (dict).
    No keys required — sources without keys report available=False gracefully.
    """
    from pathlib import Path as _Path  # noqa: PLC0415

    def _cache_mtime(cache_dir: str, name: str) -> Optional[str]:
        p = _Path(cache_dir) / f"{name}.parquet"
        try:
            mtime = p.stat().st_mtime
            return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
        except Exception:
            return None

    result = {}

    # Glassnode
    try:
        from core.data.glassnode_collector import load_glassnode_snapshot, _api_key as _gn_key  # noqa: PLC0415
        snap = load_glassnode_snapshot()
        has_data = any(v is not None for v in snap.values())
        key_configured = bool(_gn_key())
        result["glassnode"] = {
            "available": bool(has_data or key_configured),
            "key_configured": key_configured,
            "has_cached_data": has_data,
            "last_updated": _cache_mtime("data/premium/glassnode", "sopr"),
            "snapshot": snap,
        }
    except Exception as exc:
        result["glassnode"] = {"available": False, "error": str(exc)}

    # CryptoQuant
    try:
        from core.data.cryptoquant_collector import load_cryptoquant_snapshot, _api_key as _cq_key  # noqa: PLC0415
        snap = load_cryptoquant_snapshot()
        has_data = any(v is not None for v in snap.values())
        key_configured = bool(_cq_key())
        result["cryptoquant"] = {
            "available": bool(has_data or key_configured),
            "key_configured": key_configured,
            "has_cached_data": has_data,
            "last_updated": _cache_mtime("data/premium/cryptoquant", "exchange_netflow"),
            "snapshot": snap,
        }
    except Exception as exc:
        result["cryptoquant"] = {"available": False, "error": str(exc)}

    # Nansen
    try:
        from core.data.nansen_collector import load_nansen_snapshot, _api_key as _ns_key  # noqa: PLC0415
        snap = load_nansen_snapshot()
        has_data = any(v is not None for v in snap.values())
        key_configured = bool(_ns_key())
        result["nansen"] = {
            "available": bool(has_data or key_configured),
            "key_configured": key_configured,
            "has_cached_data": has_data,
            "last_updated": _cache_mtime("data/premium/nansen", "smart_money_netflow"),
            "snapshot": snap,
        }
    except Exception as exc:
        result["nansen"] = {"available": False, "error": str(exc)}

    # Kaiko
    try:
        from core.data.kaiko_collector import load_kaiko_snapshot, _api_key as _kk_key  # noqa: PLC0415
        snap = load_kaiko_snapshot()
        has_data = any(v is not None for v in snap.values())
        key_configured = bool(_kk_key())
        result["kaiko"] = {
            "available": bool(has_data or key_configured),
            "key_configured": key_configured,
            "has_cached_data": has_data,
            "last_updated": _cache_mtime("data/premium/kaiko", "cross_exchange_spread_bps"),
            "snapshot": snap,
        }
    except Exception as exc:
        result["kaiko"] = {"available": False, "error": str(exc)}

    # Google Trends (free)
    try:
        from core.data.google_trends_collector import load_latest as _gt  # noqa: PLC0415
        btc_val = _gt("bitcoin")
        result["google_trends"] = {
            "available": True,
            "has_cached_data": btc_val is not None,
            "last_updated": _cache_mtime("data/google_trends", "bitcoin_trends"),
            "snapshot": {"bitcoin": btc_val},
        }
    except Exception as exc:
        result["google_trends"] = {"available": False, "error": str(exc)}

    # FRED Macro (free with key)
    try:
        from core.data.macro_collector import load_macro_snapshot, _api_key as _fred_key  # noqa: PLC0415
        snap = load_macro_snapshot()
        has_data = any(v is not None for v in snap.values())
        result["fred_macro"] = {
            "available": bool(_fred_key()),
            "has_cached_data": has_data,
            "last_updated": _cache_mtime("data/macro", "vix"),
            "snapshot": snap,
        }
    except Exception as exc:
        result["fred_macro"] = {"available": False, "error": str(exc)}

    # Deribit Options (free)
    try:
        from core.data.options_collector import options_collector  # noqa: PLC0415
        cached = options_collector._cache.get("BTC")
        snap_dict = cached[1].to_dict() if cached and cached[1] else {}
        result["deribit_options"] = {
            "available": True,
            "has_cached_data": bool(snap_dict),
            "last_updated": snap_dict.get("timestamp"),
            "snapshot": snap_dict,
        }
    except Exception as exc:
        result["deribit_options"] = {"available": False, "error": str(exc)}

    return {"sources": result, "ts": datetime.now(timezone.utc).isoformat()}
