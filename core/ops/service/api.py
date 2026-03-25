from __future__ import annotations

import asyncio
import json
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Path as FPath, Request
from pydantic import BaseModel, Field

from config.strategy_registry import get_backtest_optimization_grid
from config.database import close_db, init_db
from config.settings import settings
from core.ai.proposal_schemas import ProposalValidationSummary, ResearchProposal
from core.audit.ops_audit import ops_audit_scope
from core.data import data_storage
from core.exchanges import exchange_manager
from core.news.service.api import IngestRequest, load_service_config, run_ingest_pull_now
from core.news.service.worker import process_llm_batch, run_pull_cycle
from core.news.storage import db as news_db
from core.governance.rbac import GovernanceIdentity
from core.governance.schemas import RiskConfigPayload
from core.governance.service import (
    approve_risk_change,
    ensure_risk_config_initialized,
    get_active_risk_config,
    list_api_users as list_governance_api_users,
    list_audit_records as list_governance_audit_records,
    list_risk_change_requests,
    list_strategy_specs as list_governance_strategy_specs,
    propose_strategy as governance_propose_strategy,
    request_risk_change as governance_request_risk_change,
    transition_strategy as governance_transition_strategy,
    upsert_api_user as governance_upsert_api_user,
)
from core.ops.service.auth import get_ops_token, get_request_auth, ops_token_configured, require_ops_auth
from core.research.orchestrator import (
    create_manual_proposal,
    delete_proposal as delete_ai_proposal_item,
    ensure_ai_research_runtime_state,
    get_candidate as get_ai_candidate_item,
    get_deployment_status as get_ai_deployment_status,
    get_experiment as get_ai_experiment_item,
    get_proposal as get_ai_proposal_item,
    list_candidates as list_ai_candidate_items,
    list_experiment_runs as list_ai_experiment_runs,
    list_experiments as list_ai_experiment_items,
    list_lifecycle as list_ai_lifecycle,
    list_promotions as list_ai_promotions,
    list_proposals as list_ai_proposal_items,
    promote_existing_candidate,
    run_proposal as run_ai_proposal_service,
)
from core.research.strategy_research import ResearchConfig, run_strategy_research
from core.risk.risk_manager import risk_manager
from core.strategies import Signal, SignalType
from core.trading.execution_engine import execution_engine
from core.trading.order_manager import OrderRequest, OrderSide, OrderType, order_manager
from core.trading.position_manager import PositionSide, position_manager
from prediction_markets.polymarket.clob_trader import PolymarketTrader
from prediction_markets.polymarket.config import load_polymarket_config
from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.worker import (
    get_runtime_status as get_pm_worker_runtime_status,
    refresh_markets_once as pm_refresh_markets_once,
    refresh_quotes_once as pm_refresh_quotes_once,
    run_worker_once as pm_run_worker_once,
)
from web.services import ensure_trading_mode_started


class OpsNewsPullRequest(BaseModel):
    since_minutes: int = Field(default=240, ge=15, le=1440)
    max_records: int = Field(default=120, ge=10, le=500)
    query: Optional[str] = None


class OpsWorkerRunRequest(BaseModel):
    sources: List[str] = Field(default_factory=list)
    llm_limit: int = Field(default=8, ge=1, le=50)
    pull_only: bool = False
    llm_only: bool = False


class ResearchRunRequest(BaseModel):
    exchange: str = "binance"
    symbol: str = "BTCUSDT"
    days: int = Field(default=30, ge=1, le=3650)
    timeframes: List[str] = Field(default_factory=lambda: ["1m", "5m", "15m"])
    strategies: List[str] = Field(default_factory=list)
    commission_rate: float = Field(default=0.0004, ge=0.0, le=1.0)
    slippage_bps: float = Field(default=2.0, ge=0.0, le=10000.0)
    initial_capital: float = Field(default=10000.0, gt=0.0)
    background: bool = True


class AIProposalCreateRequest(BaseModel):
    thesis: str = Field(..., min_length=8, max_length=600)
    symbols: List[str] = Field(default_factory=lambda: ["BTCUSDT"])
    timeframes: List[str] = Field(default_factory=lambda: ["5m", "15m", "1h"])
    market_regime: str = "mixed"
    strategy_templates: List[str] = Field(default_factory=list)
    source: str = Field(default="ai")
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


class ManualSignalRequest(BaseModel):
    symbol: str
    signal_type: str
    strength: float = Field(default=1.0, ge=0.0, le=1.0)
    reason: str = Field(default="OpenClaw manual intervention")


class PolymarketSubscribeRequest(BaseModel):
    category: str
    mode: str = Field(default="auto")
    keywords: List[str] = Field(default_factory=list)
    tags: List[int] = Field(default_factory=list)
    max_markets: Optional[int] = Field(default=None, ge=1, le=100)


class PolymarketUnsubscribeRequest(BaseModel):
    market_ids: List[str] = Field(default_factory=list)


class PolymarketWorkerRunRequest(BaseModel):
    refresh_markets: bool = True
    refresh_quotes: bool = True
    categories: List[str] = Field(default_factory=list)


class GovernanceStrategyProposeRequest(BaseModel):
    strategy_id: str
    name: str
    strategy_class: str
    params: Dict[str, Any] = Field(default_factory=dict)
    guardrails: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    regime: str = "mixed"


class GovernanceStrategyTransitionRequest(BaseModel):
    note: str = ""


class GovernanceRiskChangeRequest(BaseModel):
    proposed_config: RiskConfigPayload
    reason: str = ""


class GovernanceRiskToggleRequest(BaseModel):
    enabled: bool = True
    reason: str = ""


class GovernanceAuditQuery(BaseModel):
    module: Optional[str] = None
    action: Optional[str] = None
    actor: Optional[str] = None
    trace_id: Optional[str] = None
    limit: int = Field(default=200, ge=1, le=2000)


class GovernanceApiUserUpsertRequest(BaseModel):
    name: str
    role: str
    api_key: str
    is_active: bool = True


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ok(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"ok": True, "ts": _now_utc().isoformat(), "data": data or {}, "error": None}


def _err(message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"ok": False, "ts": _now_utc().isoformat(), "data": data or {}, "error": str(message)}


def _governance_identity_from_auth(auth: Any) -> GovernanceIdentity:
    return GovernanceIdentity(
        actor=str(getattr(auth, "actor", "") or "openclaw"),
        role=str(getattr(auth, "role", "") or "SYSTEM"),
        api_key_present=bool(getattr(auth, "api_key_present", False)),
        token_present=bool(getattr(auth, "token_present", False)),
        client_ip=str(getattr(auth, "client_ip", "") or ""),
    )


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if "/" in raw:
        return raw
    if "_" in raw:
        left, right = raw.split("_", 1)
        return f"{left}/{right}"
    if raw.endswith("USDT") and len(raw) > 4:
        return f"{raw[:-4]}/USDT"
    return raw


def _dedupe_keep_order(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values or []:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _normalize_timeframes(values: List[str]) -> List[str]:
    cleaned = _dedupe_keep_order([str(item or "").strip() for item in values or []])
    return cleaned or ["5m", "15m", "1h"]


def _default_strategy_templates(market_regime: str, symbols: List[str]) -> List[str]:
    regime = str(market_regime or "").strip().lower()
    if regime in {"trend", "trending"}:
        return ["MAStrategy", "EMAStrategy", "MACDStrategy", "TrendFollowingStrategy"]
    if regime in {"mean_reversion", "reversion"}:
        return ["RSIStrategy", "BollingerBandsStrategy", "MeanReversionStrategy", "VWAPReversionStrategy"]
    if regime in {"breakout"}:
        return ["DonchianBreakoutStrategy", "BollingerSqueezeStrategy", "MomentumStrategy"]
    if regime in {"stat_arb", "statarb", "cross_sectional"}:
        return ["PairsTradingStrategy", "FamaFactorArbitrageStrategy"]
    if regime in {"news", "news_event", "event"}:
        return ["MarketSentimentStrategy", "SocialSentimentStrategy", "FundFlowStrategy", "WhaleActivityStrategy"]
    if len(symbols) >= 2:
        return ["PairsTradingStrategy", "FamaFactorArbitrageStrategy", "MAStrategy"]
    return ["MAStrategy", "RSIStrategy", "MACDStrategy", "BollingerBandsStrategy", "MarketSentimentStrategy"]


def _derive_required_features(strategy_templates: List[str], provided: List[str]) -> List[str]:
    features = set(_dedupe_keep_order(provided))
    if not features:
        features.add("ohlcv")
    for name in strategy_templates:
        if name in {"PairsTradingStrategy"}:
            features.update({"pair_prices", "spread"})
        elif name in {"FamaFactorArbitrageStrategy"}:
            features.update({"cross_sectional_close", "cross_sectional_volume", "factor_scores"})
        elif name in {"MarketSentimentStrategy", "SocialSentimentStrategy", "FundFlowStrategy", "WhaleActivityStrategy"}:
            features.update({"news_events", "sentiment", "onchain_or_flow"})
    return sorted(features)


def _derive_parameter_space(
    strategy_templates: List[str],
    parameter_space: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    if parameter_space:
        return {str(k): dict(v or {}) for k, v in dict(parameter_space).items()}
    out: Dict[str, Dict[str, Any]] = {}
    for name in strategy_templates:
        grid = get_backtest_optimization_grid(name)
        if grid:
            out[name] = dict(grid)
    return out


def _signal_type(value: str) -> SignalType:
    text = str(value or "").strip().upper()
    mapping = {
        "BUY": SignalType.BUY,
        "SELL": SignalType.SELL,
        "CLOSE_LONG": SignalType.CLOSE_LONG,
        "CLOSE_SHORT": SignalType.CLOSE_SHORT,
        "HOLD": SignalType.HOLD,
    }
    if text not in mapping:
        raise ValueError(f"unsupported signal_type: {value}")
    return mapping[text]


def _ensure_ops_runtime_state(app: FastAPI) -> None:
    if not isinstance(getattr(app.state, "live_approvals", None), dict):
        app.state.live_approvals = {}
    if not isinstance(getattr(app.state, "research_jobs", None), dict):
        app.state.research_jobs = {}
    if not hasattr(app.state, "research_latest_path"):
        app.state.research_latest_path = (Path(settings.DATA_STORAGE_PATH) / ".." / "research" / "latest.json").resolve()
    if not hasattr(app.state, "ops_exchange_init_error"):
        app.state.ops_exchange_init_error = None
    if not hasattr(app.state, "polymarket_cfg"):
        app.state.polymarket_cfg = load_polymarket_config()
    if not hasattr(app.state, "polymarket_trading_approvals"):
        app.state.polymarket_trading_approvals = {}
    if not isinstance(getattr(app.state, "governance_runtime", None), dict):
        app.state.governance_runtime = {
            "reduce_only": False,
            "kill_switch": False,
            "risk_config_version": None,
        }
    ensure_ai_research_runtime_state(app)


def _cleanup_live_approvals(app: FastAPI) -> None:
    _ensure_ops_runtime_state(app)
    approvals = getattr(app.state, "live_approvals", {})
    now = _now_utc()
    expired = [
        code
        for code, item in approvals.items()
        if bool(item.get("used")) or (item.get("expires_at") and item["expires_at"] <= now)
    ]
    for code in expired:
        approvals.pop(code, None)


def _build_status_execution() -> Dict[str, Any]:
    return {
        "running": bool(execution_engine.is_running),
        "mode": execution_engine.get_trading_mode(),
        "queue_size": int(execution_engine.get_queue_size()),
        "queue_worker_alive": bool(execution_engine.is_queue_worker_alive()),
        "diagnostics": execution_engine.get_signal_diagnostics(),
        "conditional_orders_count": len(execution_engine.list_conditional_orders()),
    }


def _build_risk_status() -> Dict[str, Any]:
    report = risk_manager.get_risk_report()
    alerts = report.get("alerts") or []
    return {
        "trading_halted": bool(report.get("trading_halted", False)),
        "halt_reason": str(report.get("halt_reason") or ""),
        "alerts_count": len(alerts),
        "report": report,
    }


async def _build_exchange_status(app: FastAPI) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "connected_exchanges": [],
        "health": {},
        "init_error": getattr(app.state, "ops_exchange_init_error", None),
    }
    try:
        connected = list(exchange_manager.get_connected_exchanges())
        out["connected_exchanges"] = connected
        out["health"] = {name: True for name in connected}
        out["health_source"] = "connected_exchange_snapshot"
    except Exception as exc:
        out["error"] = f"connected_exchanges failed: {exc}"
        return out
    return out


async def _build_news_status() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        source_states, llm_queue = await asyncio.gather(
            news_db.list_source_states(),
            news_db.get_llm_queue_stats(),
        )
        out["source_states"] = source_states
        out["llm_queue"] = llm_queue
    except Exception as exc:
        out["error"] = str(exc)
    return out


async def _build_polymarket_status(app: FastAPI) -> Dict[str, Any]:
    out: Dict[str, Any] = {"worker_runtime": get_pm_worker_runtime_status()}
    try:
        out.update(await pm_db.get_pm_status())
    except Exception as exc:
        out["error"] = str(exc)
    out["trading_enabled"] = bool((getattr(app.state, "polymarket_cfg", {}) or {}).get("defaults", {}).get("trading", {}).get("enabled", False))
    return out


async def _ensure_live_mode_started() -> None:
    await ensure_trading_mode_started("live")


async def _ensure_paper_mode_started() -> None:
    await ensure_trading_mode_started("paper")


async def _get_ticker_price(symbol: str, exchange_name: str = "binance") -> float:
    connector = exchange_manager.get_exchange(exchange_name)
    if not connector:
        raise RuntimeError(f"exchange not connected: {exchange_name}")
    ticker = await connector.get_ticker(symbol)
    return float(getattr(ticker, "last", 0.0) or 0.0)


async def _close_exchange_orphan_positions() -> Dict[str, Any]:
    failures: List[Dict[str, Any]] = []
    closed: List[Dict[str, Any]] = []
    local_keys = {
        (
            str(pos.exchange or "").lower(),
            str(pos.symbol or "").upper(),
            str(pos.account_id or "main"),
        )
        for pos in position_manager.get_all_positions()
    }
    for exchange_name in exchange_manager.get_connected_exchanges():
        connector = exchange_manager.get_exchange(exchange_name)
        if not connector:
            continue
        try:
            exchange_positions = await connector.get_positions()
        except Exception as exc:
            failures.append({"exchange": exchange_name, "error": f"get_positions failed: {exc}"})
            continue
        for pos in exchange_positions or []:
            symbol = str(getattr(pos, "symbol", "") or "").upper()
            side = str(getattr(pos, "side", "") or "").lower()
            amount = abs(float(getattr(pos, "amount", 0.0) or 0.0))
            if not symbol or side not in {"long", "short"} or amount <= 0:
                continue
            key = (exchange_name.lower(), symbol, "main")
            if key in local_keys:
                continue
            request = OrderRequest(
                symbol=symbol,
                side=OrderSide.SELL if side == "long" else OrderSide.BUY,
                order_type=OrderType.MARKET,
                amount=amount,
                exchange=exchange_name,
                strategy="ops_kill_switch",
                account_id="main",
                reduce_only=True,
            )
            try:
                order = await order_manager.create_order(request)
                closed.append(
                    {
                        "exchange": exchange_name,
                        "symbol": symbol,
                        "side": side,
                        "amount": amount,
                        "order_id": getattr(order, "id", None),
                    }
                )
            except Exception as exc:
                failures.append({"exchange": exchange_name, "symbol": symbol, "error": f"close orphan failed: {exc}"})
    return {"closed": closed, "failures": failures}


async def _close_local_positions() -> Dict[str, Any]:
    closed: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for pos in list(position_manager.get_all_positions()):
        close_signal = Signal(
            symbol=str(pos.symbol or ""),
            signal_type=(SignalType.CLOSE_LONG if pos.side == PositionSide.LONG else SignalType.CLOSE_SHORT),
            price=float(pos.current_price or pos.entry_price or 0.0),
            timestamp=datetime.now(timezone.utc),
            strategy_name=str(pos.strategy or "ops_kill_switch"),
            strength=1.0,
            quantity=float(pos.quantity or 0.0),
            metadata={
                "exchange": str(pos.exchange or "binance"),
                "account_id": str(pos.account_id or "main"),
                "source": "ops_kill_switch",
            },
        )
        try:
            result = await execution_engine.execute_signal(close_signal)
            closed.append(
                {
                    "exchange": str(pos.exchange or "binance"),
                    "symbol": str(pos.symbol or ""),
                    "side": str(getattr(pos.side, "value", pos.side) or ""),
                    "quantity": float(pos.quantity or 0.0),
                    "result": result,
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "exchange": str(pos.exchange or "binance"),
                    "symbol": str(pos.symbol or ""),
                    "error": str(exc),
                }
            )
    return {"closed": closed, "failures": failures}


async def _run_research_job(app: FastAPI, job_id: str, config: ResearchConfig, request_payload: Dict[str, Any]) -> None:
    _ensure_ops_runtime_state(app)
    jobs = app.state.research_jobs
    job = jobs.get(job_id) or {}
    job["status"] = "running"
    job["started_at"] = _now_utc().isoformat()
    jobs[job_id] = job
    try:
        result = await run_strategy_research(config)
        output_dir = str(Path(result.get("csv_path") or "").resolve().parent) if result.get("csv_path") else str(config.output_dir.resolve())
        latest_payload = {
            "job_id": job_id,
            "request_summary": request_payload,
            "output_dir": output_dir,
            "csv_path": result.get("csv_path"),
            "markdown_path": result.get("markdown_path"),
            "top_result_summary": result.get("best"),
            "finished_at": _now_utc().isoformat(),
        }
        latest_path = Path(app.state.research_latest_path)
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        job.update(
            {
                "status": "completed",
                "finished_at": _now_utc().isoformat(),
                "result": result,
                "error": None,
            }
        )
    except Exception as exc:
        job.update(
            {
                "status": "failed",
                "finished_at": _now_utc().isoformat(),
                "result": None,
                "error": str(exc),
            }
        )
    finally:
        jobs[job_id] = job


async def _run_ai_proposal_research_job(
    app: FastAPI,
    job_id: str,
    proposal_id: str,
    config: ResearchConfig,
    request_payload: Dict[str, Any],
) -> None:
    _ensure_ops_runtime_state(app)
    jobs = app.state.research_jobs
    job = jobs.get(job_id) or {}
    job["status"] = "running"
    job["started_at"] = _now_utc().isoformat()
    jobs[job_id] = job

    proposal = _proposal_from_registry(app, proposal_id)
    proposal.status = "research_running"
    proposal.metadata["last_research_request"] = request_payload
    proposal.metadata["last_research_job_id"] = job_id
    _save_proposal(app, proposal)

    try:
        result = await run_strategy_research(config)
        output_dir = str(Path(result.get("csv_path") or "").resolve().parent) if result.get("csv_path") else str(config.output_dir.resolve())
        latest_payload = {
            "job_id": job_id,
            "proposal_id": proposal_id,
            "request_summary": request_payload,
            "output_dir": output_dir,
            "csv_path": result.get("csv_path"),
            "markdown_path": result.get("markdown_path"),
            "top_result_summary": result.get("best"),
            "finished_at": _now_utc().isoformat(),
        }
        latest_path = Path(app.state.research_latest_path)
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        job.update(
            {
                "status": "completed",
                "finished_at": _now_utc().isoformat(),
                "result": result,
                "error": None,
            }
        )
        proposal = _proposal_from_registry(app, proposal_id)
        proposal = _apply_research_result_to_proposal(proposal, result, job_id=job_id)
        _save_proposal(app, proposal)
    except Exception as exc:
        job.update(
            {
                "status": "failed",
                "finished_at": _now_utc().isoformat(),
                "result": None,
                "error": str(exc),
            }
        )
        proposal = _proposal_from_registry(app, proposal_id)
        proposal.status = "rejected"
        proposal.metadata["last_research_error"] = str(exc)
        proposal.metadata["last_research_job_id"] = job_id
        _save_proposal(app, proposal)
    finally:
        jobs[job_id] = job


def _build_ai_research_proposal(payload: AIProposalCreateRequest, actor: str) -> ResearchProposal:
    now = _now_utc()
    symbols = _dedupe_keep_order([_normalize_symbol(item) for item in payload.symbols])
    if not symbols:
        symbols = ["BTC/USDT"]
    timeframes = _normalize_timeframes(payload.timeframes)
    strategy_templates = _dedupe_keep_order(payload.strategy_templates) or _default_strategy_templates(
        market_regime=payload.market_regime,
        symbols=symbols,
    )
    required_features = _derive_required_features(
        strategy_templates=strategy_templates,
        provided=payload.required_features,
    )
    proposal = ResearchProposal(
        proposal_id=f"proposal-{int(now.timestamp())}-{secrets.token_hex(4)}",
        created_at=now,
        updated_at=now,
        status="draft",
        source=_normalize_proposal_source(payload.source),
        thesis=str(payload.thesis).strip(),
        market_regime=str(payload.market_regime or "mixed").strip() or "mixed",
        target_symbols=symbols,
        target_timeframes=timeframes,
        strategy_templates=strategy_templates,
        parameter_space=_derive_parameter_space(strategy_templates, payload.parameter_space),
        required_features=required_features,
        risk_hypothesis=str(payload.risk_hypothesis or "").strip(),
        invalidation_rules=_dedupe_keep_order(payload.invalidation_rules),
        expected_holding_period=str(payload.expected_holding_period or "1d").strip() or "1d",
        notes=_dedupe_keep_order(payload.notes),
        metadata={
            "created_by": actor,
            "input_symbol_count": len(payload.symbols or []),
            **dict(payload.metadata or {}),
        },
    )
    return proposal


def _save_proposal(app: FastAPI, proposal: ResearchProposal) -> ResearchProposal:
    _ensure_ops_runtime_state(app)
    proposal.updated_at = _now_utc()
    return app.state.ai_proposal_registry.save(proposal)


def _proposal_from_registry(app: FastAPI, proposal_id: str) -> ResearchProposal:
    _ensure_ops_runtime_state(app)
    proposal = app.state.ai_proposal_registry.get(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="proposal not found")
    return proposal


def _build_research_config_from_proposal(
    proposal: ResearchProposal,
    payload: AIProposalRunRequest,
) -> ResearchConfig:
    symbol = _normalize_symbol(payload.symbol or (proposal.target_symbols[0] if proposal.target_symbols else "BTC/USDT"))
    timeframes = _normalize_timeframes(payload.timeframes or proposal.target_timeframes)
    strategies = _dedupe_keep_order(payload.strategies or proposal.strategy_templates)
    if not strategies:
        raise ValueError("proposal has no strategy templates to research")
    return ResearchConfig(
        exchange=str(payload.exchange or "binance").strip().lower() or "binance",
        symbol=symbol,
        days=int(payload.days),
        initial_capital=float(payload.initial_capital),
        timeframes=timeframes,
        strategies=strategies,
        commission_rate=float(payload.commission_rate),
        slippage_bps=float(payload.slippage_bps),
    )


def _proposal_status_from_research_result(result: Dict[str, Any]) -> str:
    if int(result.get("valid_runs", 0) or 0) > 0 and result.get("best"):
        return "validated"
    return "rejected"


def _normalize_proposal_source(value: str) -> str:
    text = str(value or "").strip().lower()
    return text if text in {"ai", "human", "hybrid"} else "ai"


def _clip_score(value: float) -> float:
    return round(max(0.0, min(100.0, float(value))), 2)


def _score_ratio(value: float, good_at: float) -> float:
    good = max(float(good_at), 1e-9)
    return _clip_score(float(value) / good * 100.0)


def _inverse_score(value: float, bad_at: float) -> float:
    bad = max(float(bad_at), 1e-9)
    return _clip_score(100.0 - (float(value) / bad * 100.0))


def _build_validation_summary_from_research_result(result: Dict[str, Any]) -> ProposalValidationSummary:
    now = _now_utc()
    runs = max(0, int(result.get("runs", 0) or 0))
    valid_runs = max(0, int(result.get("valid_runs", 0) or 0))
    best = dict(result.get("best") or {})
    quality_counts = dict(result.get("quality_counts") or {})
    quality_ok = int(quality_counts.get("ok", 0) or 0)

    if not best or valid_runs <= 0:
        return ProposalValidationSummary(
            computed_at=now,
            decision="reject",
            edge_score=0.0,
            risk_score=0.0,
            stability_score=0.0,
            efficiency_score=0.0,
            deployment_score=0.0,
            reasons=["no valid research runs"],
            metrics={
                "runs": runs,
                "valid_runs": valid_runs,
                "quality_counts": quality_counts,
            },
        )

    total_return = float(best.get("total_return", 0.0) or 0.0)
    gross_total_return = float(best.get("gross_total_return", total_return) or total_return)
    sharpe_ratio = float(best.get("sharpe_ratio", 0.0) or 0.0)
    max_drawdown = float(best.get("max_drawdown", 0.0) or 0.0)
    win_rate = float(best.get("win_rate", 0.0) or 0.0)
    total_trades = float(best.get("total_trades", 0.0) or 0.0)
    anomaly_ratio = float(best.get("anomaly_bar_ratio", 0.0) or 0.0)
    cost_drag = abs(float(best.get("cost_drag_return_pct", 0.0) or 0.0))
    valid_ratio = (valid_runs / max(runs, 1)) * 100.0
    ok_ratio = (quality_ok / max(runs, 1)) * 100.0 if runs else 0.0

    return_score = _score_ratio(max(total_return, 0.0), 25.0)
    sharpe_score = _score_ratio(max(sharpe_ratio, 0.0), 2.0)
    win_score = _clip_score(win_rate)
    edge_score = _clip_score(return_score * 0.45 + sharpe_score * 0.4 + win_score * 0.15)

    drawdown_score = _inverse_score(max(max_drawdown, 0.0), 25.0)
    anomaly_score = _inverse_score(max(anomaly_ratio, 0.0), 0.03)
    risk_score = _clip_score(drawdown_score * 0.8 + anomaly_score * 0.2)

    stability_score = _clip_score(valid_ratio * 0.6 + ok_ratio * 0.4)

    gross_abs = max(abs(gross_total_return), 1.0)
    cost_burden_pct = cost_drag / gross_abs * 100.0
    cost_score = _inverse_score(cost_burden_pct, 35.0)
    trade_score = _clip_score(100.0 if total_trades >= 20 else total_trades / 20.0 * 100.0)
    efficiency_score = _clip_score(cost_score * 0.7 + trade_score * 0.3)

    deployment_score = _clip_score(
        edge_score * 0.35
        + risk_score * 0.25
        + stability_score * 0.20
        + efficiency_score * 0.20
    )

    reasons: List[str] = []
    if total_return <= 0:
        reasons.append("best strategy net return is non-positive")
    if max_drawdown > 15:
        reasons.append(f"max drawdown too high ({max_drawdown:.2f}%)")
    if sharpe_ratio < 1.0:
        reasons.append(f"sharpe too low ({sharpe_ratio:.2f})")
    if cost_burden_pct > 35:
        reasons.append(f"cost drag too high ({cost_burden_pct:.2f}% of gross return)")
    if valid_ratio < 50:
        reasons.append(f"valid run ratio too low ({valid_ratio:.2f}%)")
    if anomaly_ratio > 0.02:
        reasons.append(f"anomaly ratio elevated ({anomaly_ratio:.4f})")

    decision = "reject"
    if deployment_score >= 75 and sharpe_ratio >= 1.2 and max_drawdown <= 12 and valid_ratio >= 60:
        decision = "live_candidate"
    elif deployment_score >= 60 and sharpe_ratio >= 1.0 and max_drawdown <= 15:
        decision = "paper"
    elif deployment_score >= 45 and valid_runs > 0:
        decision = "shadow"

    return ProposalValidationSummary(
        computed_at=now,
        decision=decision,
        edge_score=edge_score,
        risk_score=risk_score,
        stability_score=stability_score,
        efficiency_score=efficiency_score,
        deployment_score=deployment_score,
        reasons=reasons,
        metrics={
            "runs": runs,
            "valid_runs": valid_runs,
            "valid_ratio_pct": round(valid_ratio, 2),
            "quality_ok_ratio_pct": round(ok_ratio, 2),
            "best_return_pct": round(total_return, 4),
            "best_gross_return_pct": round(gross_total_return, 4),
            "best_sharpe_ratio": round(sharpe_ratio, 4),
            "best_max_drawdown_pct": round(max_drawdown, 4),
            "best_win_rate_pct": round(win_rate, 4),
            "best_total_trades": int(total_trades),
            "anomaly_ratio": round(anomaly_ratio, 6),
            "cost_drag_return_pct": round(cost_drag, 4),
            "cost_burden_pct_of_gross": round(cost_burden_pct, 4),
            "quality_counts": quality_counts,
        },
    )


def _apply_research_result_to_proposal(proposal: ResearchProposal, result: Dict[str, Any], *, job_id: str | None = None) -> ResearchProposal:
    validation_summary = _build_validation_summary_from_research_result(result)
    proposal.validation_summary = validation_summary
    proposal.status = "validated" if validation_summary.decision != "reject" else "rejected"
    proposal.metadata["last_research_result"] = {
        "job_id": job_id,
        "best": result.get("best"),
        "valid_runs": int(result.get("valid_runs", 0) or 0),
        "runs": int(result.get("runs", 0) or 0),
        "exchange": result.get("exchange"),
        "symbol": result.get("symbol"),
        "timeframes": result.get("timeframes"),
        "strategies": result.get("strategies"),
        "csv_path": result.get("csv_path"),
        "markdown_path": result.get("markdown_path"),
        "validation_summary": validation_summary.model_dump(mode="json"),
    }
    proposal.metadata.pop("last_research_error", None)
    return proposal


async def _ops_startup(app: FastAPI, standalone: bool) -> None:
    _ensure_ops_runtime_state(app)
    app.state.ops_standalone = bool(standalone)
    app.state.ops_enabled = bool(ops_token_configured())
    if standalone:
        get_ops_token(required=True)
    if standalone:
        await init_db()
        await news_db.init_news_db()
        await data_storage.initialize()
    await pm_db.init_pm_db()
    try:
        ok = await exchange_manager.initialize(["binance"])
        app.state.ops_exchange_init_error = None if ok else "exchange_manager.initialize returned false"
    except Exception as exc:
        app.state.ops_exchange_init_error = str(exc)


async def _ops_shutdown(app: FastAPI, standalone: bool) -> None:
    try:
        if execution_engine.is_running:
            await execution_engine.stop()
    except Exception:
        pass
    try:
        await exchange_manager.close_all()
    except Exception:
        pass
    try:
        await pm_db.close_pm_db()
    except Exception:
        pass
    if standalone:
        try:
            await data_storage.close()
        except Exception:
            pass
        try:
            await news_db.close_news_db()
        except Exception:
            pass
        try:
            await close_db()
        except Exception:
            pass


async def initialize_ops_runtime(app: FastAPI, standalone: bool = False) -> None:
    _ensure_ops_runtime_state(app)
    app.state.ops_standalone = bool(standalone)
    app.state.ops_enabled = bool(ops_token_configured())
    if standalone:
        get_ops_token(required=True)
    await pm_db.init_pm_db()
    current = await ensure_risk_config_initialized(actor="system")
    cfg = dict(current.get("config") or {})
    app.state.governance_runtime["reduce_only"] = bool(cfg.get("reduce_only", False))
    app.state.governance_runtime["kill_switch"] = bool(cfg.get("kill_switch", False))
    app.state.governance_runtime["risk_config_version"] = current.get("version")


async def shutdown_ops_runtime(app: FastAPI, standalone: bool = False) -> None:
    _ensure_ops_runtime_state(app)
    app.state.ops_standalone = bool(standalone)
    app.state.ops_enabled = bool(ops_token_configured())
    await pm_db.close_pm_db()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    await _ops_startup(app, standalone=True)
    try:
        yield
    finally:
        await _ops_shutdown(app, standalone=True)


def create_router() -> APIRouter:
    from core.ops.service import ai_routes, governance_routes, news_routes, polymarket_routes, research_routes, trading_routes

    router = APIRouter(prefix="/ops", dependencies=[Depends(require_ops_auth)], tags=["ops"])

    @router.get("/health")
    async def health(request: Request):
        data: Dict[str, Any] = {"service": "ops", "version": "0.1.0"}
        try:
            data["engine_running"] = bool(execution_engine.is_running)
            data["trading_mode"] = execution_engine.get_trading_mode()
        except Exception as exc:
            data["engine_running"] = False
            data["trading_mode"] = "unknown"
            data["engine_error"] = str(exc)
        try:
            data["exchanges_connected"] = list(exchange_manager.get_connected_exchanges())
            data["exchange_health"] = {name: True for name in data["exchanges_connected"]}
            data["exchange_health_source"] = "connected_exchange_snapshot"
        except Exception as exc:
            data["exchanges_connected"] = []
            data["exchange_health"] = {}
            data["exchange_health_error"] = str(exc)
        try:
            report = risk_manager.get_risk_report()
            data["risk_halted"] = bool(report.get("trading_halted", False))
        except Exception as exc:
            data["risk_halted"] = False
            data["risk_error"] = str(exc)
        try:
            queue = await asyncio.wait_for(news_db.get_llm_queue_stats(), timeout=2.0)
            data["llm_enabled"] = bool(
                os.getenv("OPENAI_API_KEY")
                or os.getenv("ZHIPU_API_KEY")
                or getattr(settings, "OPENAI_API_KEY", "")
                or getattr(settings, "ZHIPU_API_KEY", "")
            )
            data["news_llm_queue_pending"] = int(queue.get("pending_total", 0))
        except Exception as exc:
            data["llm_enabled"] = bool(
                os.getenv("OPENAI_API_KEY")
                or os.getenv("ZHIPU_API_KEY")
                or getattr(settings, "OPENAI_API_KEY", "")
                or getattr(settings, "ZHIPU_API_KEY", "")
            )
            data["news_error"] = str(exc)
        return _ok(data)

    @router.get("/status")
    async def status(request: Request):
        data: Dict[str, Any] = {}
        try:
            data["execution_engine"] = _build_status_execution()
        except Exception as exc:
            data["execution_engine"] = {"error": str(exc)}
        try:
            data["risk_manager"] = _build_risk_status()
        except Exception as exc:
            data["risk_manager"] = {"error": str(exc)}
        try:
            data["exchange_manager"] = await _build_exchange_status(request.app)
        except Exception as exc:
            data["exchange_manager"] = {"error": str(exc)}
        try:
            data["news"] = await _build_news_status()
        except Exception as exc:
            data["news"] = {"error": str(exc)}
        try:
            data["polymarket"] = await _build_polymarket_status(request.app)
        except Exception as exc:
            data["polymarket"] = {"error": str(exc)}
        return _ok(data)

    router.include_router(governance_routes.router)

    router.include_router(news_routes.router)
    router.include_router(research_routes.router)
    router.include_router(ai_routes.router)
    router.include_router(polymarket_routes.router)

    router.include_router(trading_routes.router)

    if str(os.getenv("OPS_ALLOW_MANUAL_SIGNAL") or "").strip().lower() in {"1", "true", "yes", "on", "y"}:

        @router.post("/trading/submit_manual_signal")
        async def trading_submit_manual_signal(request: Request, payload: ManualSignalRequest):
            auth = get_request_auth(request)
            params = payload.model_dump()
            async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/submit_manual_signal", method="POST", params=params, ip=auth.client_ip) as audit_state:
                try:
                    symbol = _normalize_symbol(payload.symbol)
                    signal_type = _signal_type(payload.signal_type)
                    price = 0.0
                    price_error = None
                    try:
                        price = await _get_ticker_price(symbol, "binance")
                    except Exception as exc:
                        price_error = str(exc)
                    signal = Signal(
                        symbol=symbol,
                        signal_type=signal_type,
                        price=float(price or 0.0),
                        timestamp=datetime.now(timezone.utc),
                        strategy_name="ops_manual_signal",
                        strength=float(payload.strength or 0.0),
                        metadata={
                            "source": "openclaw_ops",
                            "reason": payload.reason,
                            "exchange": "binance",
                            "account_id": "main",
                            "price_lookup_error": price_error,
                        },
                    )
                    allowed = await risk_manager.check_signal(signal, account_equity=None, order_value=None)
                    if not allowed:
                        return _err("manual signal rejected by risk manager")
                    accepted = await execution_engine.submit_signal(signal)
                    return _ok({"accepted": bool(accepted), "signal": signal.to_dict()})
                except Exception as exc:
                    audit_state["status"] = "failed"
                    audit_state["error"] = str(exc)
                    return _err(str(exc))

    return router


def create_app() -> FastAPI:
    app = FastAPI(title="Crypto Trading Ops API", version="0.1.0", lifespan=_lifespan)
    app.include_router(create_router())
    return app


app = create_app()
