"""FastAPI service for news ingest/event query/signal generation."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from core.ai.risk_gate import RiskGate
from core.ai.signal_engine import generate_signal
from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.eventizer.llm_glm5 import extract_events_glm5_with_meta
from core.news.eventizer.rules import load_news_rule_config
from core.news.storage import db as news_db
from core.news.storage.models import PullStats, parse_any_datetime


class SignalRequest(BaseModel):
    symbol: str
    market_features: Dict[str, Any] = Field(default_factory=dict)
    since_minutes: int = 240


class IngestRequest(BaseModel):
    since_minutes: int = 240
    max_records: int = 120
    query: Optional[str] = None


def _config_paths() -> Dict[str, Path]:
    root = Path(__file__).resolve().parents[3]
    return {
        "rules": root / "config" / "news_rules.yaml",
        "symbols": root / "config" / "symbols.yaml",
    }


def load_service_config() -> Dict[str, Any]:
    paths = _config_paths()
    return load_news_rule_config(rules_path=paths["rules"], symbols_path=paths["symbols"])


async def run_ingest_pull_now(cfg: Dict[str, Any], payload: IngestRequest) -> Dict[str, Any]:
    collector = MultiSourceNewsCollector(cfg)
    errors: List[str] = []
    source_stats: Dict[str, Any] = {}

    try:
        pulled_bundle = collector.pull_latest(
            query=payload.query,
            max_records=payload.max_records,
            since_minutes=payload.since_minutes,
        )
        pulled = pulled_bundle.get("items") or []
        source_stats = pulled_bundle.get("source_stats") or {}
        errors.extend([str(x) for x in (pulled_bundle.get("errors") or []) if str(x).strip()])
    except Exception as exc:
        errors.append(f"news pull failed: {exc}")
        pulled = []

    raw_stats = await news_db.save_news_raw(pulled)
    new_news = raw_stats.get("inserted") or []

    llm_used = False
    llm_errors: List[str] = []

    if new_news:
        events, llm_used, llm_errors = extract_events_glm5_with_meta(new_news, cfg)
    else:
        events = []

    errors.extend(llm_errors)

    event_stats = await news_db.save_events(events, model_source="mixed")

    stats = PullStats(
        pulled_count=int(raw_stats.get("pulled_count") or 0),
        deduped_count=int(raw_stats.get("deduped_count") or 0),
        events_count=int(event_stats.get("events_count") or 0),
        llm_used=bool(llm_used),
        errors=errors,
    )
    payload_out = stats.model_dump(mode="json")
    payload_out["source_stats"] = source_stats
    return payload_out


async def run_pull_once() -> Dict[str, Any]:
    cfg = load_service_config()
    await news_db.init_news_db()
    try:
        return await run_ingest_pull_now(cfg=cfg, payload=IngestRequest())
    finally:
        await news_db.close_news_db()


def _parse_since(since: Optional[str]) -> datetime:
    if not since:
        return datetime.now(timezone.utc) - timedelta(hours=24)

    raw = str(since).strip()
    if raw.isdigit():
        return datetime.now(timezone.utc) - timedelta(minutes=int(raw))

    try:
        return parse_any_datetime(raw)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid since value: {exc}") from exc


def create_app() -> FastAPI:
    app = FastAPI(title="Crypto News Signal Service", version="1.0.0")

    @app.on_event("startup")
    async def _startup() -> None:
        cfg = load_service_config()
        app.state.cfg = cfg
        app.state.risk_gate = RiskGate(cfg)
        await news_db.init_news_db()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await news_db.close_news_db()

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        cfg = getattr(app.state, "cfg", load_service_config())
        return {
            "status": "ok",
            "service": "news_signal",
            "ts": datetime.now(timezone.utc).isoformat(),
            "llm_enabled": bool(os.environ.get("ZHIPU_API_KEY")),
            "thresholds": cfg.get("thresholds") or {},
        }

    @app.post("/ingest/pull_now")
    async def ingest_pull_now(payload: IngestRequest = IngestRequest()) -> Dict[str, Any]:
        cfg = getattr(app.state, "cfg", load_service_config())
        return await run_ingest_pull_now(cfg=cfg, payload=payload)

    @app.get("/events")
    async def events(
        symbol: Optional[str] = Query(default=None),
        since: Optional[str] = Query(default=None),
        limit: int = Query(default=200, ge=1, le=1000),
    ) -> Dict[str, Any]:
        cfg = getattr(app.state, "cfg", load_service_config())
        mapper = cfg.get("_symbol_mapper")
        symbol_norm = mapper.normalize_symbol(symbol) if (mapper and symbol) else (symbol.upper() if symbol else None)
        since_ts = _parse_since(since)
        rows = await news_db.list_events(symbol=symbol_norm, since=since_ts, limit=limit)
        return {
            "count": len(rows),
            "symbol": symbol_norm,
            "since": since_ts.isoformat(),
            "items": rows,
        }

    @app.post("/signal")
    async def signal(payload: SignalRequest) -> Dict[str, Any]:
        cfg = getattr(app.state, "cfg", load_service_config())
        gate = getattr(app.state, "risk_gate", RiskGate(cfg))
        try:
            result = await generate_signal(
                symbol=payload.symbol,
                market_features=payload.market_features,
                since_minutes=payload.since_minutes,
                cfg=cfg,
                risk_gate=gate,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"signal generation failed: {exc}") from exc
        return result

    @app.get("/report/daily")
    async def report_daily(date: str = Query(..., description="YYYY-MM-DD")) -> Dict[str, Any]:
        try:
            day = datetime.strptime(date, "%Y-%m-%d").date()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from exc

        report = await news_db.build_daily_report(day)
        return report

    return app


app = create_app()
