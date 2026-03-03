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

from config.database import close_db, init_db
from config.settings import settings
from core.audit.ops_audit import ops_audit_scope
from core.data import data_storage
from core.exchanges import exchange_manager
from core.news.service.api import IngestRequest, load_service_config, run_ingest_pull_now
from core.news.service.worker import process_llm_batch, run_pull_cycle
from core.news.storage import db as news_db
from core.ops.service.auth import get_ops_token, get_request_auth, ops_token_configured, require_ops_auth
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


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ok(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"ok": True, "ts": _now_utc().isoformat(), "data": data or {}, "error": None}


def _err(message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"ok": False, "ts": _now_utc().isoformat(), "data": data or {}, "error": str(message)}


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
    was_running = bool(execution_engine.is_running)
    execution_engine.set_paper_trading(False)
    if was_running:
        try:
            await execution_engine._prime_live_equity()  # type: ignore[attr-defined]
        except Exception:
            pass
        return
    await execution_engine.start()


async def _ensure_paper_mode_started() -> None:
    execution_engine.set_paper_trading(True)
    if not execution_engine.is_running:
        await execution_engine.start()


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
            timestamp=datetime.utcnow(),
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
            data["llm_enabled"] = bool(os.getenv("ZHIPU_API_KEY"))
            data["news_llm_queue_pending"] = int(queue.get("pending_total", 0))
        except Exception as exc:
            data["llm_enabled"] = bool(os.getenv("ZHIPU_API_KEY"))
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

    @router.post("/news/pull_now")
    async def news_pull_now(request: Request, payload: OpsNewsPullRequest):
        auth = get_request_auth(request)
        params = payload.model_dump()
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/news/pull_now", method="POST", params=params, ip=auth.client_ip) as audit_state:
            try:
                cfg = load_service_config()
                result = await run_ingest_pull_now(cfg=cfg, payload=IngestRequest(**params))
                audit_state["extra"] = {"queued_count": result.get("queued_count", 0), "events_count": result.get("events_count", 0)}
                return _ok(result)
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/news/worker_run_once")
    async def news_worker_run_once(request: Request, payload: OpsWorkerRunRequest):
        auth = get_request_auth(request)
        params = payload.model_dump()
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/news/worker_run_once", method="POST", params=params, ip=auth.client_ip) as audit_state:
            try:
                cfg = load_service_config()
                sources = [str(x).strip().lower() for x in payload.sources if str(x).strip()]
                out: Dict[str, Any] = {}
                if not payload.llm_only:
                    out["pull"] = await run_pull_cycle(cfg, sources or (cfg.get("defaults") or {}).get("news_sources") or [])
                if not payload.pull_only:
                    out["llm"] = await process_llm_batch(cfg, limit=payload.llm_limit)
                out["source_states"] = await news_db.list_source_states()
                out["llm_queue"] = await news_db.get_llm_queue_stats()
                return _ok(out)
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/research/run")
    async def research_run(request: Request, payload: ResearchRunRequest):
        auth = get_request_auth(request)
        params = payload.model_dump()
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/research/run", method="POST", params=params, ip=auth.client_ip) as audit_state:
            try:
                _ensure_ops_runtime_state(request.app)
                symbol = _normalize_symbol(payload.symbol)
                default_research = ResearchConfig()
                config = ResearchConfig(
                    exchange=str(payload.exchange or "binance").strip().lower(),
                    symbol=symbol,
                    days=int(payload.days),
                    initial_capital=float(payload.initial_capital),
                    timeframes=list(payload.timeframes or ["1m", "5m", "15m"]),
                    strategies=list(payload.strategies or default_research.strategies),
                    commission_rate=float(payload.commission_rate),
                    slippage_bps=float(payload.slippage_bps),
                    output_dir=default_research.output_dir,
                )
                if not payload.background:
                    result = await run_strategy_research(config)
                    latest_payload = {
                        "job_id": None,
                        "request_summary": params,
                        "output_dir": str(Path(result.get("csv_path") or "").resolve().parent) if result.get("csv_path") else str(config.output_dir.resolve()),
                        "csv_path": result.get("csv_path"),
                        "markdown_path": result.get("markdown_path"),
                        "top_result_summary": result.get("best"),
                        "finished_at": _now_utc().isoformat(),
                    }
                    latest_path = Path(request.app.state.research_latest_path)
                    latest_path.parent.mkdir(parents=True, exist_ok=True)
                    latest_path.write_text(json.dumps(latest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                    return _ok(result)
                job_id = f"research-{int(_now_utc().timestamp())}-{secrets.token_hex(4)}"
                job = {
                    "job_id": job_id,
                    "status": "pending",
                    "created_at": _now_utc().isoformat(),
                    "started_at": None,
                    "finished_at": None,
                    "request": params,
                    "result": None,
                    "error": None,
                }
                request.app.state.research_jobs[job_id] = job
                asyncio.create_task(_run_research_job(request.app, job_id, config, params), name=f"ops_research_{job_id}")
                audit_state["extra"] = {"job_id": job_id}
                return _ok(job)
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.get("/research/job/{job_id}")
    async def research_job(request: Request, job_id: str = FPath(...)):
        _ensure_ops_runtime_state(request.app)
        job = request.app.state.research_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="research job not found")
        return _ok(job)

    @router.get("/research/latest")
    async def research_latest(request: Request):
        _ensure_ops_runtime_state(request.app)
        latest_path = Path(request.app.state.research_latest_path)
        if not latest_path.exists():
            return _err("latest research not found")
        try:
            payload = json.loads(latest_path.read_text(encoding="utf-8"))
            return _ok(payload)
        except Exception as exc:
            return _err(f"failed to read latest research: {exc}")

    @router.get("/polymarket/status")
    async def polymarket_status(request: Request):
        try:
            return _ok(await _build_polymarket_status(request.app))
        except Exception as exc:
            return _err(str(exc))

    @router.post("/polymarket/subscribe")
    async def polymarket_subscribe(request: Request, payload: PolymarketSubscribeRequest):
        auth = get_request_auth(request)
        params = payload.model_dump()
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/subscribe", method="POST", params=params, ip=auth.client_ip) as audit_state:
            try:
                _ensure_ops_runtime_state(request.app)
                cfg = dict(request.app.state.polymarket_cfg or load_polymarket_config())
                categories = dict(cfg.get("categories") or {})
                cat = str(payload.category or "").strip().upper()
                if cat not in categories:
                    return _err(f"unknown category: {cat}")
                if payload.mode == "manual":
                    item = dict(categories.get(cat) or {})
                    if payload.keywords:
                        item["keywords"] = list(payload.keywords)
                    if payload.tags:
                        item["tags"] = [int(x) for x in payload.tags]
                    if payload.max_markets:
                        item["max_markets"] = int(payload.max_markets)
                    categories[cat] = item
                    cfg["categories"] = categories
                    request.app.state.polymarket_cfg = cfg
                result = await pm_refresh_markets_once(cfg, categories=[cat])
                audit_state["extra"] = {"category": cat}
                return _ok({"category": cat, "mode": payload.mode, "result": result})
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/polymarket/unsubscribe")
    async def polymarket_unsubscribe(request: Request, payload: PolymarketUnsubscribeRequest):
        auth = get_request_auth(request)
        params = payload.model_dump()
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/unsubscribe", method="POST", params=params, ip=auth.client_ip) as audit_state:
            try:
                result = await pm_db.disable_subscriptions(payload.market_ids)
                return _ok(result)
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/polymarket/worker_run_once")
    async def polymarket_worker_run_once(request: Request, payload: PolymarketWorkerRunRequest):
        auth = get_request_auth(request)
        params = payload.model_dump()
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/worker_run_once", method="POST", params=params, ip=auth.client_ip) as audit_state:
            try:
                _ensure_ops_runtime_state(request.app)
                cfg = request.app.state.polymarket_cfg or load_polymarket_config()
                result = await pm_run_worker_once(
                    cfg,
                    refresh_markets=bool(payload.refresh_markets),
                    refresh_quotes=bool(payload.refresh_quotes),
                    categories=[str(x).strip().upper() for x in payload.categories if str(x).strip()] or None,
                )
                return _ok(result)
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.get("/polymarket/alerts")
    async def polymarket_alerts(since: Optional[str] = None, category: Optional[str] = None, limit: int = 200):
        try:
            since_ts = parse_any_datetime(since) if since else None
            rows = await pm_db.list_alerts(since=since_ts, category=category, limit=limit)
            return _ok({"count": len(rows), "items": rows})
        except Exception as exc:
            return _err(str(exc))

    @router.get("/polymarket/features")
    async def polymarket_features(symbol: str, tf: str = "1m", since: Optional[str] = None):
        try:
            since_ts = parse_any_datetime(since) if since else (_now_utc() - timedelta(hours=24))
            rows = await pm_db.get_features_range(
                symbol=str(symbol or "").strip().upper(),
                since=since_ts,
                until=_now_utc(),
                timeframe=str(tf or "1m").strip().lower(),
            )
            return _ok({"count": len(rows), "items": rows})
        except Exception as exc:
            return _err(str(exc))

    @router.post("/polymarket/arm_trading")
    async def polymarket_arm_trading(request: Request):
        auth = get_request_auth(request)
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/arm_trading", method="POST", params={}, ip=auth.client_ip) as audit_state:
            try:
                _ensure_ops_runtime_state(request.app)
                code = secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:10]
                expires_at = _now_utc() + timedelta(seconds=120)
                request.app.state.polymarket_trading_approvals[code] = {
                    "approval_code": code,
                    "issued_at": _now_utc(),
                    "expires_at": expires_at,
                    "actor": auth.actor,
                    "used": False,
                }
                return _ok({"approval_code": code, "expires_at": expires_at.isoformat(), "note": "Call /ops/polymarket/enable_trading with X-OPS-APPROVAL within 120s"})
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/polymarket/enable_trading")
    async def polymarket_enable_trading(request: Request, x_ops_approval: Optional[str] = Header(default=None, alias="X-OPS-APPROVAL")):
        auth = get_request_auth(request)
        params = {"approval_code": x_ops_approval or ""}
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/enable_trading", method="POST", params=params, ip=auth.client_ip) as audit_state:
            try:
                _ensure_ops_runtime_state(request.app)
                code = str(x_ops_approval or "").strip()
                approval = request.app.state.polymarket_trading_approvals.get(code)
                if not approval:
                    audit_state["status"] = "denied"
                    audit_state["error"] = "invalid approval code"
                    raise HTTPException(status_code=403, detail="invalid approval code")
                if approval.get("expires_at") <= _now_utc():
                    request.app.state.polymarket_trading_approvals.pop(code, None)
                    audit_state["status"] = "denied"
                    audit_state["error"] = "approval code expired"
                    raise HTTPException(status_code=403, detail="approval code expired")
                request.app.state.polymarket_cfg.setdefault("defaults", {}).setdefault("trading", {})["enabled"] = True
                request.app.state.polymarket_trading_approvals.pop(code, None)
                return _ok({"trading_enabled": True})
            except HTTPException:
                raise
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/trading/start_paper")
    async def trading_start_paper(request: Request):
        auth = get_request_auth(request)
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/start_paper", method="POST", params={}, ip=auth.client_ip) as audit_state:
            try:
                await _ensure_paper_mode_started()
                return _ok(
                    {
                        "running": bool(execution_engine.is_running),
                        "mode": execution_engine.get_trading_mode(),
                        "queue_size": int(execution_engine.get_queue_size()),
                        "risk_scope": "paper",
                    }
                )
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/trading/arm_live")
    async def trading_arm_live(request: Request):
        auth = get_request_auth(request)
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/arm_live", method="POST", params={}, ip=auth.client_ip) as audit_state:
            try:
                _ensure_ops_runtime_state(request.app)
                _cleanup_live_approvals(request.app)
                code = secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:10]
                expires_at = _now_utc() + timedelta(seconds=120)
                request.app.state.live_approvals[code] = {
                    "approval_code": code,
                    "issued_at": _now_utc(),
                    "expires_at": expires_at,
                    "actor": auth.actor,
                    "used": False,
                }
                audit_state["extra"] = {"approval_expires_at": expires_at.isoformat()}
                return _ok(
                    {
                        "approval_code": code,
                        "expires_at": expires_at.isoformat(),
                        "note": "Call /ops/trading/start_live with X-OPS-APPROVAL within 120s",
                    }
                )
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/trading/start_live")
    async def trading_start_live(request: Request, x_ops_approval: Optional[str] = Header(default=None, alias="X-OPS-APPROVAL")):
        auth = get_request_auth(request)
        params = {"approval_code": x_ops_approval or ""}
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/start_live", method="POST", params=params, ip=auth.client_ip) as audit_state:
            _ensure_ops_runtime_state(request.app)
            _cleanup_live_approvals(request.app)
            code = str(x_ops_approval or "").strip()
            approval = request.app.state.live_approvals.get(code)
            if not code or not approval:
                audit_state["status"] = "denied"
                audit_state["error"] = "invalid approval code"
                raise HTTPException(status_code=403, detail="invalid approval code")
            if approval.get("used"):
                audit_state["status"] = "denied"
                audit_state["error"] = "approval code already used"
                raise HTTPException(status_code=403, detail="approval code already used")
            expires_at = approval.get("expires_at")
            if not isinstance(expires_at, datetime) or expires_at <= _now_utc():
                request.app.state.live_approvals.pop(code, None)
                audit_state["status"] = "denied"
                audit_state["error"] = "approval code expired"
                raise HTTPException(status_code=403, detail="approval code expired")
            try:
                await _ensure_live_mode_started()
                approval["used"] = True
                request.app.state.live_approvals.pop(code, None)
                return _ok(_build_status_execution())
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/trading/stop")
    async def trading_stop(request: Request):
        auth = get_request_auth(request)
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/stop", method="POST", params={}, ip=auth.client_ip) as audit_state:
            try:
                await execution_engine.stop()
                return _ok(
                    {
                        "running": False,
                        "mode": execution_engine.get_trading_mode(),
                        "conditional_orders_count": len(execution_engine.list_conditional_orders()),
                    }
                )
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/trading/kill_switch")
    async def trading_kill_switch(request: Request):
        auth = get_request_auth(request)
        params = {"mode": execution_engine.get_trading_mode()}
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/kill_switch", method="POST", params=params, ip=auth.client_ip) as audit_state:
            try:
                connected = list(exchange_manager.get_connected_exchanges())
                pre_open_orders: Dict[str, int] = {}
                for exchange_name in connected:
                    connector = exchange_manager.get_exchange(exchange_name)
                    if not connector:
                        continue
                    try:
                        pre_open_orders[exchange_name] = len(await connector.get_open_orders())
                    except Exception:
                        pre_open_orders[exchange_name] = 0
                conditional_before = len(execution_engine.list_conditional_orders())
                await execution_engine.stop()
                local = await _close_local_positions()
                orphan = {"closed": [], "failures": []}
                if not execution_engine.is_paper_mode():
                    orphan = await _close_exchange_orphan_positions()
                result = {
                    "engine_stopped": True,
                    "cancelled_orders": {
                        "exchange_open_orders_before": pre_open_orders,
                        "conditional_orders_before": conditional_before,
                        "total_exchange_open_orders_before": int(sum(pre_open_orders.values())),
                    },
                    "closed_local_positions": local.get("closed", []),
                    "closed_exchange_positions": orphan.get("closed", []),
                    "failures": list(local.get("failures", [])) + list(orphan.get("failures", [])),
                }
                audit_state["extra"] = {
                    "closed_local": len(result["closed_local_positions"]),
                    "closed_exchange": len(result["closed_exchange_positions"]),
                    "failures": len(result["failures"]),
                }
                return _ok(result)
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

    @router.post("/risk/reset_halt")
    async def risk_reset_halt(request: Request):
        auth = get_request_auth(request)
        async with ops_audit_scope(actor=auth.actor, endpoint="/ops/risk/reset_halt", method="POST", params={}, ip=auth.client_ip) as audit_state:
            try:
                risk_manager.reset_halt()
                report = risk_manager.get_risk_report()
                return _ok(
                    {
                        "trading_halted": bool(report.get("trading_halted", False)),
                        "halt_reason": str(report.get("halt_reason") or ""),
                        "report": report,
                    }
                )
            except Exception as exc:
                audit_state["status"] = "failed"
                audit_state["error"] = str(exc)
                return _err(str(exc))

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
                        timestamp=datetime.utcnow(),
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
