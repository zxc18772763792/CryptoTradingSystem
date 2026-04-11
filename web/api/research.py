"""Research workbench API for the advanced research page."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select

from config.database import (
    AnalyticsCommunitySnapshot,
    AnalyticsMicrostructureSnapshot,
    AnalyticsWhaleSnapshot,
    async_session_maker,
)
from core.news.storage import db as news_db
from web.api.data import (
    get_factor_library,
    get_fama_like_factors,
    get_multi_assets_overview,
    get_onchain_overview,
    get_research_symbols,
)
from web.api.trading import (
    _load_analytics_ingest_status_map,
    get_behavior_report,
    get_community_overview,
    get_market_microstructure,
    get_risk_dashboard,
    get_trading_calendar,
    get_stoploss_policy,
)

router = APIRouter()
_UI_TIMEZONE = str(os.environ.get("CTS_UI_TIMEZONE") or os.environ.get("UI_TIMEZONE") or "Asia/Shanghai").strip() or "Asia/Shanghai"
try:
    _UI_ZONEINFO = ZoneInfo(_UI_TIMEZONE)
except Exception:
    _UI_ZONEINFO = timezone.utc
_TIMEZONE_BASIS = f"UTC storage, {_UI_TIMEZONE} display"

_VALID_TIMEFRAMES = {"1m", "5m", "15m", "1h", "4h", "1d"}
_DEFAULT_UNIVERSE = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "DOGE/USDT",
    "TRX/USDT",
    "LINK/USDT",
    "AVAX/USDT",
    "DOT/USDT",
    "POL/USDT",
    "LTC/USDT",
    "BCH/USDT",
    "ETC/USDT",
    "ATOM/USDT",
    "NEAR/USDT",
    "APT/USDT",
    "ARB/USDT",
    "OP/USDT",
    "SUI/USDT",
    "INJ/USDT",
    "RUNE/USDT",
    "AAVE/USDT",
    "MKR/USDT",
    "UNI/USDT",
    "FIL/USDT",
    "HBAR/USDT",
    "ICP/USDT",
    "TON/USDT",
]
_MODULE_ORDER = ["market_state", "factors", "cross_asset", "onchain", "discipline"]
_MODULE_TIMEOUT_SEC = {
    "market_state": 40.0,
    "factors": 45.0,
    "cross_asset": 8.0,
    "onchain": 30.0,
    "discipline": 5.0,
}
_MARKET_STATE_HISTORY_PREFERRED_MAX_AGE_SEC = 20 * 60


class ResearchProfile(BaseModel):
    exchange: str = "binance"
    primary_symbol: str = "BTC/USDT"
    universe_symbols: List[str] = Field(default_factory=lambda: list(_DEFAULT_UNIVERSE))
    timeframe: str = "5m"
    lookback: int = 1200
    exclude_retired: bool = True
    horizon: str = "short_intraday"


class ResearchWorkbenchRequest(BaseModel):
    profile: ResearchProfile = Field(default_factory=ResearchProfile)

    @model_validator(mode="before")
    @classmethod
    def coerce_profile(cls, value: Any) -> Any:
        if isinstance(value, dict) and "profile" not in value:
            keys = {"exchange", "primary_symbol", "universe_symbols", "timeframe", "lookback", "exclude_retired", "horizon"}
            if any(key in value for key in keys):
                return {"profile": value}
        return value


class ResearchRecommendationRequest(BaseModel):
    profile: ResearchProfile = Field(default_factory=ResearchProfile)
    overview: Optional[Dict[str, Any]] = None
    modules: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def coerce_profile(cls, value: Any) -> Any:
        if isinstance(value, dict) and "profile" not in value:
            keys = {"exchange", "primary_symbol", "universe_symbols", "timeframe", "lookback", "exclude_retired", "horizon"}
            if any(key in value for key in keys):
                cloned = dict(value)
                profile = {key: cloned.pop(key) for key in list(cloned.keys()) if key in keys}
                cloned["profile"] = profile
                return cloned
        return value


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coerce_utc_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _utc_iso(value: Any) -> Optional[str]:
    dt = _coerce_utc_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _local_iso(value: Any) -> Optional[str]:
    dt = _coerce_utc_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(_UI_ZONEINFO).isoformat()


def _symbol_to_news_key(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return "BTC"
    main = raw.split(":")[0]
    if "/" in main:
        return main.split("/")[0]
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if main.endswith(suffix):
            return main[: -len(suffix)] or main
    return main


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return "BTC/USDT"
    if ":" in raw:
        raw = raw.split(":")[0]
    if "/" not in raw:
        raw = f"{raw}/USDT"
    return raw


def _normalize_profile(profile: ResearchProfile) -> ResearchProfile:
    exchange = str(profile.exchange or "binance").strip().lower() or "binance"
    timeframe = str(profile.timeframe or "5m").strip().lower()
    if timeframe not in _VALID_TIMEFRAMES:
        timeframe = "5m"
    lookback = max(120, min(int(profile.lookback or 1200), 5000))
    primary_symbol = _normalize_symbol(profile.primary_symbol or "BTC/USDT")

    universe: List[str] = []
    seen = set()
    for symbol in [primary_symbol, *(profile.universe_symbols or [])]:
        normalized = _normalize_symbol(symbol)
        if normalized in seen:
            continue
        seen.add(normalized)
        universe.append(normalized)
    if not universe:
        universe = list(_DEFAULT_UNIVERSE)
    if primary_symbol not in universe:
        universe.insert(0, primary_symbol)

    return ResearchProfile(
        exchange=exchange,
        primary_symbol=primary_symbol,
        universe_symbols=universe[:30],
        timeframe=timeframe,
        lookback=lookback,
        exclude_retired=bool(profile.exclude_retired),
        horizon=str(profile.horizon or "short_intraday"),
    )


def _build_profile_from_query(
    exchange: str = "binance",
    primary_symbol: str = "BTC/USDT",
    universe_symbols: str = "",
    timeframe: str = "5m",
    lookback: int = 1200,
    exclude_retired: bool = True,
    horizon: str = "short_intraday",
) -> ResearchProfile:
    symbols = [item.strip() for item in str(universe_symbols or "").split(",") if item.strip()]
    return _normalize_profile(
        ResearchProfile(
            exchange=exchange,
            primary_symbol=primary_symbol,
            universe_symbols=symbols or list(_DEFAULT_UNIVERSE),
            timeframe=timeframe,
            lookback=lookback,
            exclude_retired=exclude_retired,
            horizon=horizon,
        )
    )


def _profile_symbol_window(profile: ResearchProfile, limit: int) -> List[str]:
    universe = list(profile.universe_symbols or [])
    if not universe:
        universe = [profile.primary_symbol]
    return universe[: max(2, int(limit))]


def _status_from_flags(ok: bool = True, degraded: bool = False) -> Literal["ok", "degraded", "error"]:
    if not ok:
        return "error"
    return "degraded" if degraded else "ok"


def _module_result(
    module: str,
    *,
    status: Literal["ok", "degraded", "error"],
    source_labels: List[str],
    summary: Dict[str, Any],
    payload: Dict[str, Any],
    warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "module": module,
        "status": status,
        "freshness_sec": 0,
        "source_labels": source_labels,
        "warnings": list(warnings or []),
        "summary": summary,
        "payload": payload,
        "generated_at": _now_iso(),
    }


def _extract_module_payload(module_or_wrapper: Any) -> Dict[str, Any]:
    if not isinstance(module_or_wrapper, dict):
        return {}
    payload = module_or_wrapper.get("payload")
    return payload if isinstance(payload, dict) else module_or_wrapper


async def _wait_or_none(coro: Any, timeout_sec: float) -> Any:
    try:
        return await asyncio.wait_for(coro, timeout=timeout_sec)
    except Exception:
        return None


def _merge_nested_payload(primary: Any, fallback: Any) -> Any:
    if isinstance(primary, dict) and isinstance(fallback, dict):
        merged = dict(fallback)
        for key, value in primary.items():
            merged[key] = _merge_nested_payload(value, merged.get(key))
        return merged
    if primary is None:
        return fallback
    return primary


async def _build_news_summary(symbol: str, hours: int = 24) -> Dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(hours=max(1, min(int(hours or 24), 168)))
    symbol_key = _symbol_to_news_key(symbol)
    db_timeout = 8.0
    events_task = asyncio.wait_for(news_db.list_events(symbol=symbol_key, since=since, limit=300), timeout=db_timeout)
    raw_task = asyncio.wait_for(news_db.list_news_raw(since=since, limit=400), timeout=db_timeout)
    states_task = asyncio.wait_for(news_db.list_source_states(), timeout=db_timeout)
    queue_task = asyncio.wait_for(news_db.get_llm_queue_stats(), timeout=db_timeout)
    events_raw, raw_rows_raw, source_states_raw, llm_queue_raw = await asyncio.gather(
        events_task,
        raw_task,
        states_task,
        queue_task,
        return_exceptions=True,
    )

    events = [] if isinstance(events_raw, Exception) else list(events_raw or [])
    raw_rows = [] if isinstance(raw_rows_raw, Exception) else list(raw_rows_raw or [])
    source_states = [] if isinstance(source_states_raw, Exception) else list(source_states_raw or [])
    llm_queue = {} if isinstance(llm_queue_raw, Exception) else dict(llm_queue_raw or {})

    sentiment = {"positive": 0, "neutral": 0, "negative": 0}
    by_type: Dict[str, int] = {}

    def consume(rows: List[Dict[str, Any]]) -> None:
        for event in rows:
            score = int(event.get("sentiment") or 0)
            if score > 0:
                sentiment["positive"] += 1
            elif score < 0:
                sentiment["negative"] += 1
            else:
                sentiment["neutral"] += 1
            event_type = str(event.get("event_type") or "other")
            by_type[event_type] = by_type.get(event_type, 0) + 1

    scope = "symbol"
    consume(events)
    if not events:
        scope = "global_fallback"
        try:
            events = await asyncio.wait_for(news_db.list_events(symbol=None, since=since, limit=300), timeout=5.0)
        except Exception:
            events = []
        sentiment = {"positive": 0, "neutral": 0, "negative": 0}
        by_type = {}
        consume(events)

    recent_flow_cutoff = datetime.now(timezone.utc) - timedelta(hours=min(4, max(1, int(hours or 24) // 6 or 1)))
    feed_count = 0
    active_providers: set[str] = set()
    for row in raw_rows or []:
        provider = str(row.get("provider") or ((row.get("payload") or {}).get("provider")) or "").strip()
        if provider:
            active_providers.add(provider)
        published_raw = row.get("published_at") or row.get("timestamp") or row.get("created_at")
        published_at: Optional[datetime] = None
        if isinstance(published_raw, datetime):
            published_at = published_raw if published_raw.tzinfo else published_raw.replace(tzinfo=timezone.utc)
        else:
            text = str(published_raw or "").strip()
            if text:
                try:
                    published_at = datetime.fromisoformat(text.replace("Z", "+00:00"))
                    if published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=timezone.utc)
                except Exception:
                    published_at = None
        if published_at and published_at >= recent_flow_cutoff:
            feed_count += 1
    if feed_count <= 0 and raw_rows:
        feed_count = max(1, min(len(raw_rows), len(active_providers) or 0))

    generated_at = _now_iso()
    return {
        "symbol": symbol_key,
        "hours": int(hours),
        "scope": scope,
        "events_count": int(len(events or [])),
        "raw_count": int(len(raw_rows or [])),
        "feed_count": int(feed_count),
        "active_provider_count": int(len(active_providers)),
        "sentiment": sentiment,
        "by_type": dict(sorted(by_type.items(), key=lambda item: item[1], reverse=True)[:8]),
        "source_states": source_states,
        "llm_queue": llm_queue,
        "timestamp": generated_at,
        "generated_at_utc": generated_at,
        "generated_at_local": _local_iso(generated_at),
        "window_since_utc": _utc_iso(since),
        "window_since_local": _local_iso(since),
        "ui_timezone": _UI_TIMEZONE,
        "timezone_basis": _TIMEZONE_BASIS,
    }


def _build_market_regime(
    analytics: Dict[str, Any],
    microstructure: Dict[str, Any],
    news: Dict[str, Any],
) -> Dict[str, Any]:
    risk_module = dict((analytics.get("modules") or {}).get("risk_dashboard", {}).get("data") or {})
    micro_module = dict((analytics.get("modules") or {}).get("microstructure", {}).get("data") or {})

    risk_level = str(risk_module.get("risk_level") or "unknown")
    spread_bps = float(
        microstructure.get("orderbook", {}).get("spread_bps")
        or micro_module.get("orderbook", {}).get("spread_bps")
        or 0.0
    )
    imbalance = float(
        microstructure.get("aggressor_flow", {}).get("imbalance")
        or micro_module.get("aggressor_flow", {}).get("imbalance")
        or 0.0
    )
    sentiment = dict(news.get("sentiment") or {})
    total_news = sum(int(sentiment.get(key) or 0) for key in ("positive", "neutral", "negative"))
    news_bias = ((sentiment.get("positive", 0) - sentiment.get("negative", 0)) / total_news) if total_news else 0.0
    confidence = min(0.95, max(0.2, 0.35 + min(0.25, total_news / 200.0) + (0.15 if spread_bps > 0 else 0.0)))

    if risk_level == "high" or spread_bps >= 8:
        regime = "高风险震荡"
        bias = "defensive"
    elif imbalance >= 0.12 and news_bias >= 0.1:
        regime = "顺势偏多"
        bias = "bullish"
    elif imbalance <= -0.12 and news_bias <= -0.1:
        regime = "顺势偏空"
        bias = "bearish"
    elif abs(imbalance) <= 0.05 and total_news < 5:
        regime = "低信息震荡"
        bias = "neutral"
    else:
        regime = "事件驱动混合"
        bias = "neutral"

    return {
        "regime": regime,
        "bias": bias,
        "confidence": round(confidence, 4),
        "risk_level": risk_level,
        "spread_bps": round(spread_bps, 4),
        "imbalance": round(imbalance, 4),
        "news_bias": round(news_bias, 4),
    }


def _has_positive_number(value: Any) -> bool:
    try:
        return float(value) > 0
    except Exception:
        return False


def _snapshot_age_sec(payload: Dict[str, Any]) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    ts = _coerce_utc_datetime(payload.get("timestamp"))
    if ts is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def _snapshot_is_recent(payload: Dict[str, Any], max_age_sec: float) -> bool:
    age_sec = _snapshot_age_sec(payload)
    return age_sec is not None and age_sec <= float(max_age_sec)


def _analytics_module_entry(task_name: str, data: Dict[str, Any], *, ok: bool, error: Optional[str] = None) -> Dict[str, Any]:
    payload = dict(data or {})
    if error:
        payload.setdefault("error", error)
    return {
        "task": task_name,
        "ok": bool(ok),
        "latency_ms": 0.0,
        "data": payload,
    }


def _microstructure_has_signal(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    orderbook = payload.get("orderbook") or {}
    aggressor = payload.get("aggressor_flow") or {}
    funding = payload.get("funding_rate") or {}
    basis = payload.get("spot_futures_basis") or {}
    return any(
        [
            _has_positive_number(orderbook.get("mid_price")),
            _has_positive_number(orderbook.get("spread_bps")),
            bool(aggressor.get("count")),
            funding.get("available") is True,
            basis.get("available") is True,
        ]
    )


def _community_has_signal(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(
        [
            bool(payload.get("announcements")),
            bool((payload.get("whale_transfers") or {}).get("count")),
            bool((payload.get("flow_proxy") or {}).get("count")),
            bool((payload.get("security_alerts") or {}).get("events")),
        ]
    )


def _map_calendar_rows(rows: Any) -> List[Dict[str, Any]]:
    mapped: List[Dict[str, Any]] = []
    for row in list(rows or [])[:8]:
        if not isinstance(row, dict):
            continue
        mapped.append(
            {
                "title": row.get("title") or row.get("name") or row.get("event") or "事件",
                "timestamp": row.get("timestamp") or row.get("time_utc") or row.get("start_time") or row.get("time"),
                "importance": row.get("importance") or "medium",
                "category": row.get("category") or "event",
                "note": row.get("note"),
            }
        )
    return mapped


async def _load_latest_snapshot(model: Any, exchange: str, symbol: str) -> Optional[Any]:
    async with async_session_maker() as session:
        preferred_stmt = (
            select(model)
            .where(model.exchange == exchange, model.symbol == symbol, model.capture_status.in_(["ok", "degraded"]))
            .order_by(model.timestamp.desc())
            .limit(1)
        )
        row = (await session.execute(preferred_stmt)).scalars().first()
        if row is not None:
            return row
        fallback_stmt = (
            select(model)
            .where(model.exchange == exchange, model.symbol == symbol)
            .order_by(model.timestamp.desc())
            .limit(1)
        )
        return (await session.execute(fallback_stmt)).scalars().first()


async def _load_latest_microstructure_snapshot(exchange: str, symbol: str) -> Dict[str, Any]:
    row = await _load_latest_snapshot(AnalyticsMicrostructureSnapshot, exchange, symbol)
    if row is None:
        return {}
    payload = dict(row.payload or {})
    return dict(
        _merge_nested_payload(
            payload,
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "timestamp": _utc_iso(row.timestamp) or _now_iso(),
                "available": row.capture_status != "failed" and row.mid_price > 0,
                "source_error": None,
                "source_name": row.source_name,
                "capture_status": row.capture_status,
                "latency_ms": row.latency_ms,
                "orderbook": {
                    "mid_price": row.mid_price,
                    "spread_bps": row.spread_bps,
                },
                "aggressor_flow": {
                    "imbalance": row.order_flow_imbalance,
                },
                "funding_rate": {
                    "available": row.funding_rate is not None,
                    "funding_rate": row.funding_rate,
                },
                "spot_futures_basis": {
                    "available": row.basis_pct is not None,
                    "basis_pct": row.basis_pct,
                },
            },
        )
        or {}
    )


async def _load_latest_community_snapshot(exchange: str, symbol: str) -> Dict[str, Any]:
    row = await _load_latest_snapshot(AnalyticsCommunitySnapshot, exchange, symbol)
    if row is None:
        return {}
    payload = dict(row.payload or {})
    return dict(
        _merge_nested_payload(
            payload,
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "timestamp": _utc_iso(row.timestamp) or _now_iso(),
                "source_error": None,
                "source_name": row.source_name,
                "capture_status": row.capture_status,
                "latency_ms": row.latency_ms,
                "flow_proxy": {
                    "imbalance": row.flow_imbalance,
                    "buy_ratio": row.buy_ratio,
                    "sell_ratio": row.sell_ratio,
                },
                "announcements": list(payload.get("announcements") or [])[:10],
                "security_alerts": payload.get("security_alerts") or {},
                "twitter_watchlist": list(payload.get("twitter_watchlist") or [])[:10],
            },
        )
        or {}
    )


async def _load_latest_whale_snapshot(exchange: str, symbol: str) -> Dict[str, Any]:
    row = await _load_latest_snapshot(AnalyticsWhaleSnapshot, exchange, symbol)
    if row is None:
        return {}
    payload = dict(row.payload or {})
    return dict(
        _merge_nested_payload(
            payload,
            {
                "exchange": row.exchange,
                "symbol": row.symbol,
                "timestamp": _utc_iso(row.timestamp) or _now_iso(),
                "available": row.capture_status != "failed",
                "error": None,
                "source_name": row.source_name,
                "capture_status": row.capture_status,
                "latency_ms": row.latency_ms,
                "count": int(row.whale_count or 0),
                "threshold_btc": payload.get("threshold_btc"),
                "btc_price": payload.get("btc_price"),
                "transactions": list(payload.get("transactions") or [])[:10],
            },
        )
        or {}
    )


def _compact_factor_library(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    if not data.get("factors") and not data.get("latest") and not data.get("asset_scores") and not data.get("points"):
        return {}
    return {
        "exchange": data.get("exchange"),
        "timeframe": data.get("timeframe"),
        "lookback_effective": data.get("lookback_effective"),
        "symbols_used": list(data.get("symbols_used") or [])[:12],
        "retired_filter": data.get("retired_filter") or {},
        "points": int(data.get("points") or 0),
        "factors": list(data.get("factors") or []),
        "universe_size": int(data.get("universe_size") or 0),
        "universe_quality": data.get("universe_quality") or "unknown",
        "warnings": list(data.get("warnings") or []),
        "latest": dict(data.get("latest") or {}),
        "mean_24": dict(data.get("mean_24") or {}),
        "std_24": dict(data.get("std_24") or {}),
        "correlation": dict(data.get("correlation") or {}),
        "series": [row for row in list(data.get("series") or [])[:120] if isinstance(row, dict)],
        "asset_scores": [row for row in list(data.get("asset_scores") or [])[:12] if isinstance(row, dict)],
    }


def _compact_fama(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    if not data.get("latest") and not data.get("series") and not data.get("points"):
        return {}
    return {
        "exchange": data.get("exchange"),
        "timeframe": data.get("timeframe"),
        "symbols_used": list(data.get("symbols_used") or [])[:12],
        "points": int(data.get("points") or 0),
        "universe_size": int(data.get("universe_size") or 0),
        "universe_quality": data.get("universe_quality") or "unknown",
        "latest": dict(data.get("latest") or {}),
        "mean_24": dict(data.get("mean_24") or {}),
        "std_24": dict(data.get("std_24") or {}),
        "series": [row for row in list(data.get("series") or [])[:120] if isinstance(row, dict)],
    }


def _fallback_factor_library_from_fama(
    profile: ResearchProfile,
    fama: Dict[str, Any],
    cross_asset: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(fama, dict) or not fama:
        return {}

    series_rows = [row for row in list(fama.get("series") or []) if isinstance(row, dict)]
    factor_names = [str(name) for name in list((fama.get("latest") or {}).keys()) if str(name)]
    correlation: Dict[str, Dict[str, float]] = {}

    if series_rows and factor_names:
        frame = pd.DataFrame(series_rows)
        if "timestamp" in frame.columns:
            frame = frame.drop(columns=["timestamp"])
        usable_cols = [col for col in factor_names if col in frame.columns]
        if usable_cols:
            corr_df = frame[usable_cols].apply(pd.to_numeric, errors="coerce").corr().round(4).fillna(0.0)
            correlation = corr_df.to_dict()

    asset_scores: List[Dict[str, Any]] = []
    for row in list((cross_asset or {}).get("assets") or [])[:12]:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        ret_pct = float(row.get("return_pct") or 0.0)
        vol_pct = abs(float(row.get("volatility_pct") or 0.0))
        score = round(ret_pct / 100.0, 6)
        low_vol = round(max(0.0, 1.0 - min(vol_pct / 100.0, 1.0)), 6)
        asset_scores.append(
            {
                "symbol": symbol,
                "score": score,
                "momentum": score,
                "value": 0.0,
                "quality": low_vol,
                "low_vol": low_vol,
                "liquidity": 0.0,
                "low_beta": 0.0,
                "size": 0.0,
            }
        )

    return {
        "exchange": profile.exchange,
        "timeframe": profile.timeframe,
        "lookback_effective": min(360, profile.lookback),
        "symbols_used": list(fama.get("symbols_used") or [])[:12],
        "retired_filter": dict((cross_asset or {}).get("retired_filter") or {}),
        "points": int(fama.get("points") or 0),
        "factors": factor_names,
        "universe_size": int(fama.get("universe_size") or len(asset_scores)),
        "universe_quality": fama.get("universe_quality") or "low",
        "warnings": ["因子库已降级为 Fama 风格快照，横截面打分使用多币种收益近似生成。"],
        "latest": dict(fama.get("latest") or {}),
        "mean_24": dict(fama.get("mean_24") or {}),
        "std_24": dict(fama.get("std_24") or {}),
        "correlation": correlation,
        "series": series_rows[:120],
        "asset_scores": asset_scores,
    }


async def _build_market_state_module(profile: ResearchProfile) -> Dict[str, Any]:
    risk_task = _wait_or_none(
        get_risk_dashboard(lookback=min(720, profile.lookback)),
        2.0,
    )
    news_task = _wait_or_none(_build_news_summary(profile.primary_symbol, hours=24), 8.0)
    calendar_task = _wait_or_none(get_trading_calendar(days=7), 2.0)
    history_micro_task = _wait_or_none(_load_latest_microstructure_snapshot(profile.exchange, profile.primary_symbol), 4.0)
    history_community_task = _wait_or_none(_load_latest_community_snapshot(profile.exchange, profile.primary_symbol), 4.0)
    history_whale_task = _wait_or_none(_load_latest_whale_snapshot(profile.exchange, profile.primary_symbol), 4.0)
    risk_dashboard, news, calendar_data, history_micro, history_community, history_whale = await asyncio.gather(
        risk_task,
        news_task,
        calendar_task,
        history_micro_task,
        history_community_task,
        history_whale_task,
    )
    risk_dashboard = dict(risk_dashboard or {})
    news = dict(news or {})
    calendar_data = dict(calendar_data or {})
    history_micro = dict(history_micro or {})
    history_community = dict(history_community or {})
    history_whale = dict(history_whale or {})

    prefer_history_micro = _snapshot_is_recent(history_micro, _MARKET_STATE_HISTORY_PREFERRED_MAX_AGE_SEC) and _microstructure_has_signal(history_micro)
    prefer_history_community = _snapshot_is_recent(history_community, _MARKET_STATE_HISTORY_PREFERRED_MAX_AGE_SEC) and _community_has_signal(history_community)

    live_micro_task = asyncio.sleep(0, result=None)
    live_community_task = asyncio.sleep(0, result=None)
    if not prefer_history_micro:
        live_micro_task = _wait_or_none(
            get_market_microstructure(
                exchange=profile.exchange,
                symbol=profile.primary_symbol,
                depth_limit=20,
            ),
            2.5,
        )
    if not prefer_history_community:
        live_community_task = _wait_or_none(
            get_community_overview(
                symbol=profile.primary_symbol,
                exchange=profile.exchange,
            ),
            2.5,
        )

    live_micro, live_community = await asyncio.gather(live_micro_task, live_community_task)
    live_micro = dict(live_micro or {})
    live_community = dict(live_community or {})

    micro = dict(history_micro if prefer_history_micro else (_merge_nested_payload(live_micro, history_micro) or {}))
    community = dict(history_community if prefer_history_community else (_merge_nested_payload(live_community, history_community) or {}))
    used_history_micro = prefer_history_micro or (
        not prefer_history_micro and _microstructure_has_signal(history_micro) and not _microstructure_has_signal(live_micro)
    )
    used_history_community = prefer_history_community or (
        not prefer_history_community and _community_has_signal(history_community) and not _community_has_signal(live_community)
    )
    used_history_whale = False

    if not isinstance(community.get("whale_transfers"), dict) or "count" not in (community.get("whale_transfers") or {}):
        if history_whale:
            community["whale_transfers"] = {
                "available": bool(history_whale.get("available", True)),
                "count": int(history_whale.get("count") or 0),
                "threshold_btc": history_whale.get("threshold_btc"),
                "btc_price": history_whale.get("btc_price"),
                "transactions": list(history_whale.get("transactions") or [])[:10],
            }
            used_history_whale = True

    calendar_rows = _map_calendar_rows(calendar_data.get("events") or [])
    analytics_modules = {
        "risk_dashboard": _analytics_module_entry(
            "risk_dashboard",
            risk_dashboard,
            ok=bool(risk_dashboard),
            error=None if risk_dashboard else "risk dashboard unavailable",
        ),
        "calendar": _analytics_module_entry(
            "calendar",
            calendar_data,
            ok=bool(calendar_rows),
            error=None if calendar_rows else "calendar unavailable",
        ),
        "microstructure": _analytics_module_entry(
            "microstructure",
            micro,
            ok=_microstructure_has_signal(micro),
            error=None if _microstructure_has_signal(micro) else "microstructure unavailable",
        ),
        "community": _analytics_module_entry(
            "community",
            community,
            ok=_community_has_signal(community),
            error=None if _community_has_signal(community) else "community unavailable",
        ),
    }
    analytics = {
        "timestamp": _now_iso(),
        "all_ok": all(bool((item or {}).get("ok")) for item in analytics_modules.values()),
        "ok_count": len([item for item in analytics_modules.values() if bool((item or {}).get("ok"))]),
        "total": len(analytics_modules),
        "modules": analytics_modules,
    }

    regime = _build_market_regime(analytics, micro, news)
    degraded = False
    warnings: List[str] = []

    if not risk_dashboard:
        degraded = True
        warnings.append("分析总览未在时限内返回，当前仅基于局部结果给出判断。")
    if str(news.get("scope")) == "global_fallback":
        degraded = True
        warnings.append("当前标的新闻不足，已回退到全市场摘要。")
    if int(news.get("events_count") or 0) + int(news.get("feed_count") or 0) + int(news.get("raw_count") or 0) <= 0:
        degraded = True
        warnings.append("News summary returned no usable samples, so event coverage may be stale.")
    if used_history_micro and not prefer_history_micro:
        degraded = True
        warnings.append("实时微观结构未及时返回，已回退到历史快照。")
    if (used_history_community and not prefer_history_community) or used_history_whale:
        degraded = True
        warnings.append("社区/巨鲸实时数据未及时返回，已回退到历史快照。")
    if not calendar_rows:
        degraded = True
        warnings.append("交易日历已使用观察清单回退结果。")
    if float(micro.get("orderbook", {}).get("spread_bps") or 0.0) <= 0:
        degraded = True
        warnings.append("微观结构深度不足，盘口与主动流解释力受限。")
    if regime.get("risk_level") == "high":
        warnings.append("当前风险等级偏高，建议下调研究结论置信度。")

    return _module_result(
        "market_state",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=[
            "trading.analytics.risk_dashboard",
            "trading.analytics.calendar",
            "trading.analytics.microstructure",
            "trading.analytics.community",
            "news.storage.summary",
            "analytics.history.snapshots",
        ],
        warnings=warnings,
        summary={
            "headline": f"{regime['regime']} | {profile.primary_symbol} | {profile.timeframe}",
            "market_regime": regime["regime"],
            "direction_bias": regime["bias"],
            "confidence": regime["confidence"],
            "risk_level": regime["risk_level"],
        },
        payload={
            "analytics_overview": analytics,
            "sentiment_dashboard": {
                "exchange": profile.exchange,
                "symbol": profile.primary_symbol,
                "timestamp": _now_iso(),
                "microstructure": micro,
                "community": community,
                "news": news,
            },
            "calendar_watchlist": calendar_rows,
            "regime": regime,
        },
    )


async def _build_factors_module(profile: ResearchProfile) -> Dict[str, Any]:
    symbols = ",".join(_profile_symbol_window(profile, 30))
    factor_task = _wait_or_none(
        get_factor_library(
            exchange=profile.exchange,
            symbols=symbols,
            timeframe=profile.timeframe,
            lookback=min(900, profile.lookback),
            quantile=0.3,
            series_limit=240,
            exclude_retired=profile.exclude_retired,
        ),
        14.0,
    )
    fama_task = _wait_or_none(
        get_fama_like_factors(
            exchange=profile.exchange,
            symbols=symbols,
            timeframe=profile.timeframe,
            lookback=min(360, profile.lookback),
            exclude_retired=profile.exclude_retired,
        ),
        12.0,
    )
    cross_task = _wait_or_none(
        get_multi_assets_overview(
            exchange=profile.exchange,
            symbols=symbols,
            timeframe=profile.timeframe,
            lookback=min(360, profile.lookback),
            exclude_retired=profile.exclude_retired,
        ),
        8.0,
    )
    factor_raw, fama_raw, cross_asset_raw = await asyncio.gather(factor_task, fama_task, cross_task)
    fama = _compact_fama(fama_raw or {})
    factor_library = _compact_factor_library(factor_raw or {})
    cross_asset = dict(cross_asset_raw or {})

    fallback_library = _fallback_factor_library_from_fama(profile, fama, cross_asset) if fama else {}
    if not factor_library and fallback_library:
        factor_library = fallback_library
    elif factor_library:
        if not factor_library.get("asset_scores") and fallback_library.get("asset_scores"):
            factor_library["asset_scores"] = fallback_library["asset_scores"]
        if not factor_library.get("correlation") and fallback_library.get("correlation"):
            factor_library["correlation"] = fallback_library["correlation"]

    warnings: List[str] = list(factor_library.get("warnings") or [])
    if not factor_library:
        warnings.append("因子库未在时限内完成，当前仅保留最小摘要。")
    if not fama:
        warnings.append("Fama 风格因子已降级，当前不展示完整时序。")
    if not cross_asset:
        warnings.append("多币种横截面快照未在时限内完成，资产排序可能不完整。")
    warnings.append("当前为高级研究快路径，因子模块展示的是轻量摘要，不等同于全量因子实验。")

    latest_fama = dict(fama.get("latest") or {})
    top_symbols = [str(item.get("symbol") or "") for item in list(factor_library.get("asset_scores") or [])[:3] if item.get("symbol")]
    degraded = (
        not factor_library
        or not fama
        or str(factor_library.get("universe_quality") or "") == "low"
        or int(factor_library.get("universe_size") or 0) < 4
    )

    return _module_result(
        "factors",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=["data.factors.library", "data.factors.fama"],
        warnings=warnings[:8],
        summary={
            "headline": "因子与风格偏置",
            "top_symbols": top_symbols,
            "universe_size": int(factor_library.get("universe_size") or 0),
            "factor_count": len(factor_library.get("factors") or []),
            "mkt": float(latest_fama.get("MKT") or 0.0),
            "mom": float(latest_fama.get("MOM") or 0.0),
        },
        payload={"factor_library": factor_library, "fama": fama},
    )


async def _build_cross_asset_module(profile: ResearchProfile) -> Dict[str, Any]:
    data = await _wait_or_none(
        get_multi_assets_overview(
            exchange=profile.exchange,
            symbols=",".join(_profile_symbol_window(profile, 10)),
            timeframe=profile.timeframe,
            lookback=min(720, profile.lookback),
            exclude_retired=profile.exclude_retired,
        ),
        8.0,
    ) or {}
    assets = list(data.get("assets") or [])
    leader = assets[0] if assets else {}
    degraded = int(data.get("count") or 0) < 3
    warnings = ["可用币种不足 3 个，轮动判断可能失真。"] if degraded else []

    return _module_result(
        "cross_asset",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=["data.multi_assets.overview"],
        warnings=warnings,
        summary={
            "headline": "多币种强弱与相关性",
            "asset_count": int(data.get("count") or 0),
            "leader_symbol": str(leader.get("symbol") or "-"),
            "leader_return_pct": float(leader.get("return_pct") or 0.0),
        },
        payload={"cross_asset": data},
    )


async def _build_onchain_module(profile: ResearchProfile) -> Dict[str, Any]:
    onchain_task = _wait_or_none(
        get_onchain_overview(
            exchange=profile.exchange,
            symbol=profile.primary_symbol,
            whale_threshold_btc=10.0,
            chain="Ethereum",
            refresh=False,
        ),
        8.0,
    )
    community_snapshot_task = _wait_or_none(_load_latest_community_snapshot(profile.exchange, profile.primary_symbol), 4.0)
    whale_snapshot_task = _wait_or_none(_load_latest_whale_snapshot(profile.exchange, profile.primary_symbol), 4.0)
    news_task = _wait_or_none(_build_news_summary(profile.primary_symbol, hours=72), 8.0)
    history_task = _wait_or_none(_load_analytics_ingest_status_map(), 6.0)
    onchain, community_snapshot, whale_snapshot, news, history_status = await asyncio.gather(
        onchain_task,
        community_snapshot_task,
        whale_snapshot_task,
        news_task,
        history_task,
    )

    onchain = dict(onchain or {})
    community_snapshot = dict(community_snapshot or {})
    community = community_snapshot
    whale_snapshot = dict(whale_snapshot or {})
    news = dict(news or {})
    history_status = dict(history_status or {})

    funding_multi = dict(onchain.get("funding_rate_multi_source") or {})
    fear_greed = dict(onchain.get("fear_greed_index") or {})
    funding_count = int(funding_multi.get("count") or 0)
    fear_greed_available = bool(fear_greed.get("available"))

    degraded = (
        bool(onchain.get("degraded"))
        or not onchain
        or str(news.get("scope")) == "global_fallback"
        or (funding_count <= 0 and not fear_greed_available)
    )
    warnings: List[str] = []
    if not onchain:
        warnings.append("链上概览未在时限内返回，当前使用外生信息摘要。")
    elif onchain.get("degraded"):
        warnings.append("链上数据包含代理源或缓存结果，可靠性低于实时链路。")
    if funding_count <= 0:
        warnings.append("多交易所 Funding 费率暂不可用。")
    if not fear_greed_available:
        warnings.append("恐慌贪婪指数暂不可用。")
    if str(news.get("scope")) == "global_fallback":
        warnings.append("当前标的外生新闻覆盖不足。")

    return _module_result(
        "onchain",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=["data.onchain.overview", "trading.analytics.community", "news.storage.summary", "analytics.history"],
        warnings=warnings[:8],
        summary={
            "headline": "链上与外生信息",
            "whale_count": int(
                (onchain.get("whale_activity") or {}).get("count")
                or (community.get("whale_transfers") or {}).get("count")
                or whale_snapshot.get("count")
                or 0
            ),
            "news_events": int(news.get("events_count") or 0),
            "tvl_chain": str((onchain.get("defi_tvl") or {}).get("chain") or "Ethereum"),
            "served_mode": str(onchain.get("served_mode") or "live"),
            "funding_sources": funding_count,
            "funding_mean_rate_pct": float(funding_multi.get("mean_rate_pct") or 0.0) if funding_count > 0 else None,
            "fear_greed_value": int(fear_greed.get("value") or 0) if fear_greed_available else None,
            "fear_greed_classification": str(fear_greed.get("classification") or "") if fear_greed_available else None,
        },
        payload={
            "onchain": onchain,
            "community": community,
            "whale_snapshot": whale_snapshot,
            "news_summary": news,
            "analytics_history_status": history_status,
        },
    )


async def _build_discipline_module(_: ResearchProfile) -> Dict[str, Any]:
    behavior_task = _wait_or_none(get_behavior_report(days=7), 4.0)
    stoploss_task = _wait_or_none(get_stoploss_policy(), 4.0)
    behavior, stoploss = await asyncio.gather(behavior_task, stoploss_task)
    behavior = dict(behavior or {})
    stoploss = dict(stoploss or {})
    suggestions = list(stoploss.get("position_suggestions") or [])
    impulsive_ratio = float(behavior.get("impulsive_ratio") or 0.0)
    overtrade = bool(behavior.get("overtrading_warning"))
    degraded = int(behavior.get("entries") or 0) == 0

    warnings: List[str] = []
    if degraded:
        warnings.append("近期没有行为记录，纪律模块只能提供通用建议。")
    if overtrade:
        warnings.append("检测到过度交易风险。")
    if impulsive_ratio >= 0.3:
        warnings.append("近期冲动交易占比偏高。")

    return _module_result(
        "discipline",
        status=_status_from_flags(ok=True, degraded=degraded),
        source_labels=["trading.analytics.behavior.report", "trading.analytics.stoploss.policy"],
        warnings=warnings,
        summary={
            "headline": "纪律与风控",
            "entries": int(behavior.get("entries") or 0),
            "impulsive_ratio": round(impulsive_ratio, 4),
            "overtrading_warning": overtrade,
            "position_suggestions": len(suggestions),
        },
        payload={"behavior_report": behavior, "stoploss_policy": stoploss},
    )


async def _build_module(module_name: str, profile: ResearchProfile) -> Dict[str, Any]:
    if module_name == "market_state":
        return await _build_market_state_module(profile)
    if module_name == "factors":
        return await _build_factors_module(profile)
    if module_name == "cross_asset":
        return await _build_cross_asset_module(profile)
    if module_name == "onchain":
        return await _build_onchain_module(profile)
    if module_name == "discipline":
        return await _build_discipline_module(profile)
    raise HTTPException(status_code=404, detail=f"Unknown research module: {module_name}")


async def _capture_module_build(module_name: str, profile: ResearchProfile) -> Dict[str, Any]:
    try:
        return await asyncio.wait_for(_build_module(module_name, profile), timeout=_MODULE_TIMEOUT_SEC.get(module_name, 12.0))
    except asyncio.TimeoutError:
        return _module_result(
            module_name,
            status="error",
            source_labels=[f"research.workbench.{module_name}"],
            warnings=[f"{module_name} 超时，已跳过本轮结果。"],
            summary={"headline": f"{module_name} 加载超时", "error": "timeout"},
            payload={"error": "timeout"},
        )
    except HTTPException as exc:
        if exc.status_code == 404 and "Unknown research module" in str(exc.detail):
            raise
        error_text = str(exc.detail or "module failed")
        return _module_result(
            module_name,
            status="error",
            source_labels=[f"research.workbench.{module_name}"],
            warnings=[error_text],
            summary={"headline": f"{module_name} 加载失败", "error": error_text},
            payload={"error": error_text},
        )
    except Exception as exc:
        error_text = str(exc)
        return _module_result(
            module_name,
            status="error",
            source_labels=[f"research.workbench.{module_name}"],
            warnings=[error_text],
            summary={"headline": f"{module_name} 加载失败", "error": error_text},
            payload={"error": error_text},
        )


def _build_recommendations(
    profile: ResearchProfile,
    modules: Dict[str, Any],
    overview: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    market_payload = _extract_module_payload(modules.get("market_state"))
    factors_payload = _extract_module_payload(modules.get("factors"))
    cross_payload = _extract_module_payload(modules.get("cross_asset"))
    onchain_payload = _extract_module_payload(modules.get("onchain"))
    discipline_payload = _extract_module_payload(modules.get("discipline"))

    regime = dict(market_payload.get("regime") or {})
    factor_library = dict(factors_payload.get("factor_library") or {})
    cross_asset = dict(cross_payload.get("cross_asset") or {})
    onchain = dict(onchain_payload.get("onchain") or {})
    sentiment_dashboard = dict(market_payload.get("sentiment_dashboard") or {})
    news_summary = dict(onchain_payload.get("news_summary") or sentiment_dashboard.get("news") or {})
    behavior = dict(discipline_payload.get("behavior_report") or {})

    direction_bias = str(regime.get("bias") or "neutral")
    if direction_bias == "bullish":
        preferred = ["趋势跟随", "动量突破", "低杠杆顺势"]
    elif direction_bias == "bearish":
        preferred = ["防守型均值回归", "反弹做空", "事件驱动快进快出"]
    else:
        preferred = ["均值回归", "低频震荡", "轻仓事件跟踪"]

    avoid: List[str] = []
    next_actions: List[str] = []
    jump_targets: List[Dict[str, Any]] = []

    asset_scores = list(factor_library.get("asset_scores") or [])
    top_symbols = [str(item.get("symbol") or "") for item in asset_scores[:3] if item.get("symbol")]
    if top_symbols:
        next_actions.append(f"优先观察 {' / '.join(top_symbols)} 的短周期结构与回测表现。")
        jump_targets.append(
            {
                "label": "去回测这些币",
                "target": "backtest",
                "params": {"symbols": top_symbols, "timeframe": profile.timeframe},
            }
        )

    if bool(onchain.get("degraded")):
        avoid.append("链上与外生信息当前包含降级样本，不适合单独作为开仓依据。")
    if int(news_summary.get("events_count") or 0) == 0:
        avoid.append("当前标的新闻事件覆盖不足，不要过度依赖事件驱动判断。")
    if bool(behavior.get("overtrading_warning")):
        avoid.append("当前存在过度交易风险，建议减少试错频率。")
    if float(behavior.get("impulsive_ratio") or 0.0) >= 0.3:
        avoid.append("近期执行纪律偏弱，避免同时追多个币。")

    if direction_bias == "bullish":
        next_actions.append("先验证顺势策略在 5m / 15m 上的连续性，再决定是否放大仓位。")
        jump_targets.append(
            {
                "label": "去 AI 研究筛候选",
                "target": "ai-research",
                "params": {"goal": "短周期顺势与动量候选", "timeframe": profile.timeframe},
            }
        )
    elif direction_bias == "bearish":
        next_actions.append("优先评估防守型与回撤控制策略，不建议直接追单边。")
    else:
        next_actions.append("先在回测页验证均值回归或震荡策略，不要直接扩到过多币种。")

    if int(cross_asset.get("count") or 0) < 3:
        next_actions.append("先补足多币种覆盖，再做横截面轮动判断。")

    headline = str((overview or {}).get("market_regime") or regime.get("regime") or "短周期研究建议")
    return {
        "direction_bias": direction_bias,
        "preferred_strategy_families": preferred,
        "avoid_conditions": avoid,
        "next_actions": next_actions,
        "backtest_jump_targets": jump_targets,
        "headline": headline,
        "generated_at": _now_iso(),
    }


def _build_structured_recommendations(
    profile: ResearchProfile,
    modules: Dict[str, Any],
    overview: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    def _derive_research_timeframes(base_timeframe: str) -> List[str]:
        presets = {
            "1m": ["1m", "5m", "15m"],
            "5m": ["5m", "15m", "1h"],
            "15m": ["5m", "15m", "1h", "4h"],
            "1h": ["15m", "1h", "4h"],
            "4h": ["1h", "4h", "1d"],
            "1d": ["4h", "1d"],
        }
        selected = presets.get(str(base_timeframe or "5m").lower(), ["5m", "15m", "1h"])
        normalized: List[str] = []
        for timeframe in selected:
            tf = str(timeframe or "").lower()
            if tf in _VALID_TIMEFRAMES and tf not in normalized:
                normalized.append(tf)
        return normalized or ["5m", "15m", "1h"]

    def _pick_backtest_strategy(bias: str, title: str) -> Dict[str, str]:
        headline_text = str(title or "")
        headline_lower = headline_text.lower()
        if "breakout" in headline_lower or "突破" in headline_text:
            return {"strategy_type": "DonchianBreakoutStrategy", "label": "突破"}
        if bias == "bullish":
            return {"strategy_type": "TrendFollowingStrategy", "label": "趋势"}
        if bias == "bearish":
            return {"strategy_type": "MeanReversionStrategy", "label": "防守"}
        return {"strategy_type": "MeanReversionStrategy", "label": "均值回归"}

    def _map_planner_regime(bias: str, title: str) -> str:
        headline_text = str(title or "")
        headline_lower = headline_text.lower()
        if "news" in headline_lower or "事件" in headline_text or "新闻" in headline_text:
            return "news_event"
        if "breakout" in headline_lower or "突破" in headline_text:
            return "breakout"
        if bias == "bullish":
            return "trend_up"
        if bias == "bearish":
            return "trend_down"
        if "mean" in headline_lower or "回归" in headline_text or "震荡" in headline_text:
            return "mean_reversion"
        return "mixed"

    market_payload = _extract_module_payload(modules.get("market_state"))
    factors_payload = _extract_module_payload(modules.get("factors"))
    cross_payload = _extract_module_payload(modules.get("cross_asset"))
    onchain_payload = _extract_module_payload(modules.get("onchain"))
    discipline_payload = _extract_module_payload(modules.get("discipline"))

    regime = dict(market_payload.get("regime") or {})
    factor_library = dict(factors_payload.get("factor_library") or {})
    cross_asset = dict(cross_payload.get("cross_asset") or {})
    onchain = dict(onchain_payload.get("onchain") or {})
    sentiment_dashboard = dict(market_payload.get("sentiment_dashboard") or {})
    news_summary = dict(onchain_payload.get("news_summary") or sentiment_dashboard.get("news") or {})
    behavior = dict(discipline_payload.get("behavior_report") or {})

    direction_bias = str(regime.get("bias") or "neutral")
    if direction_bias == "bullish":
        preferred = ["趋势跟随", "动量突破", "低杠杆顺势"]
    elif direction_bias == "bearish":
        preferred = ["防守型均值回归", "反弹做空", "事件驱动快进快出"]
    else:
        preferred = ["均值回归", "低频震荡", "轻仓事件跟踪"]

    avoid: List[str] = []
    next_actions: List[str] = []
    jump_targets: List[Dict[str, Any]] = []

    asset_scores = list(factor_library.get("asset_scores") or [])
    factor_focus = [
        {
            "symbol": str(item.get("symbol") or "").strip(),
            "score": round(float(item.get("score") or 0.0), 4),
            "momentum": round(float(item.get("momentum") or 0.0), 4),
            "quality": round(float(item.get("quality") or 0.0), 4),
        }
        for item in asset_scores[:3]
        if str(item.get("symbol") or "").strip()
    ]
    top_symbols = [item["symbol"] for item in factor_focus]
    focus_symbols = top_symbols or [profile.primary_symbol]

    if bool(onchain.get("degraded")):
        avoid.append("链上与外生信息当前包含降级样本，不适合作为单独开仓依据。")
    if int(news_summary.get("events_count") or 0) == 0:
        avoid.append("当前标的新闻事件覆盖不足，不要过度依赖事件驱动判断。")
    if bool(behavior.get("overtrading_warning")):
        avoid.append("当前存在过度交易风险，建议减少试错频率。")
    if float(behavior.get("impulsive_ratio") or 0.0) >= 0.3:
        avoid.append("近期执行纪律偏弱，避免同时追多个币。")

    if direction_bias == "bullish":
        next_actions.append("先验证顺势策略在 5m / 15m 上的连续性，再决定是否放大仓位。")
    elif direction_bias == "bearish":
        next_actions.append("优先评估防守型与回撤控制策略，不建议直接追单边。")
    else:
        next_actions.append("先在回测页验证均值回归或震荡策略，不要直接扩到过多币种。")

    if int(cross_asset.get("count") or 0) < 3:
        next_actions.append("先补足多币种覆盖，再做横截面轮动判断。")

    headline = str((overview or {}).get("market_regime") or regime.get("regime") or "短周期研究建议")
    planner_regime = _map_planner_regime(direction_bias, headline)
    research_timeframes = _derive_research_timeframes(profile.timeframe)
    backtest_strategy = _pick_backtest_strategy(direction_bias, headline)
    factor_source_meta = {
        "served_mode": str(factor_library.get("served_mode") or "unknown"),
        "cached": bool(factor_library.get("cached")),
        "cache_age_sec": round(float(factor_library.get("cache_age_sec") or 0.0), 3),
        "universe_size": int(factor_library.get("universe_size") or 0),
        "symbols_used": int(len(factor_library.get("symbols_used") or [])),
        "generated_at": str(modules.get("factors", {}).get("generated_at") or ""),
    }
    cross_leader = str(
        cross_asset.get("leader_symbol")
        or ((cross_asset.get("assets") or [{}])[0].get("symbol") if isinstance(cross_asset.get("assets"), list) else "")
        or ""
    )
    whale_count = int((onchain.get("whale_activity") or {}).get("count") or 0)

    thesis_points: List[str] = []
    if factor_focus:
        thesis_points.append(
            "因子面当前靠前："
            + " / ".join(f"{item['symbol']}({item['score']:.2f})" for item in factor_focus)
            + "。"
        )
    if cross_leader:
        thesis_points.append(f"横截面轮动当前由 {cross_leader} 领涨领跌，可作为强弱锚点。")
    if whale_count > 0:
        thesis_points.append(f"链上巨鲸活跃 {whale_count} 笔，短期波动可能被放大。")
    if int(news_summary.get("events_count") or 0) > 0:
        thesis_points.append(f"近 24h 新闻事件 {int(news_summary.get('events_count') or 0)} 条，可作为事件风向参考。")
    if not thesis_points:
        thesis_points.append("当前结论主要来自研究总览摘要，建议补齐模块后再下结论。")

    ai_goal = (
        f"围绕 {'、'.join(focus_symbols)} 在 {headline} 环境下，"
        f"优先验证 {' / '.join(preferred[:2])} 策略，明确触发条件、失效条件与仓位约束。"
    )
    ai_brief = {
        "headline": headline,
        "goal": ai_goal,
        "planner_regime": planner_regime,
        "market_regime": headline,
        "direction_bias": direction_bias,
        "symbols": focus_symbols,
        "timeframes": research_timeframes,
        "preferred_strategy_families": preferred,
        "thesis": thesis_points[:4],
        "risk_notes": (avoid or ["暂无明显额外风险，但仍需先做回测与成交质量验证。"])[:4],
        "next_steps": next_actions[:4],
        "factor_focus": factor_focus,
    }
    ai_brief["prompt_context"] = "\n".join(
        [
            f"研究任务：{ai_goal}",
            f"市场状态：{headline} / {direction_bias}",
            f"关注标的：{' / '.join(ai_brief['symbols'])}",
            f"观察周期：{' / '.join(ai_brief['timeframes'])}",
            f"优先策略：{' / '.join(preferred)}",
            f"研究观察：{'；'.join(ai_brief['thesis'])}",
            f"风险提示：{'；'.join(ai_brief['risk_notes'])}",
            f"下一步：{'；'.join(ai_brief['next_steps'])}",
        ]
    )

    action_items: List[Dict[str, Any]] = [
        {
            "id": "prefill_ai_research",
            "kind": "ai_prefill",
            "label": "填入 AI 研究器",
            "description": "把市场状态、币种、周期和风险约束写入 AI 研究页面。",
            "tone": "primary",
            "params": {
                "goal": ai_brief["prompt_context"],
                "regime": planner_regime,
                "symbols": focus_symbols,
                "timeframes": research_timeframes,
                "brief": ai_brief,
            },
        }
    ]
    if focus_symbols:
        backtest_params = {
            "exchange": profile.exchange,
            "symbol": focus_symbols[0],
            "symbols": focus_symbols,
            "timeframe": profile.timeframe,
            "strategy_type": backtest_strategy["strategy_type"],
        }
        action_items.append(
            {
                "id": "open_backtest_focus_symbol",
                "kind": "backtest",
                "label": f"回测 {focus_symbols[0]} {backtest_strategy['label']}策略",
                "description": f"跳转到回测页并预填 {focus_symbols[0]} / {profile.timeframe}。",
                "tone": "positive",
                "params": backtest_params,
            }
        )
        jump_targets.append(
            {
                "label": f"回测 {focus_symbols[0]} {backtest_strategy['label']}策略",
                "target": "backtest",
                "params": backtest_params,
            }
        )
    if not top_symbols:
        action_items.append(
            {
                "id": "refresh_factor_module",
                "kind": "module",
                "label": "刷新因子面",
                "description": "当前还没有清晰的优先币种，先补齐因子排序。",
                "tone": "neutral",
                "module": "factors",
            }
        )
    if int(cross_asset.get("count") or 0) < 3:
        action_items.append(
            {
                "id": "refresh_cross_asset_module",
                "kind": "module",
                "label": "补齐横截面覆盖",
                "description": "可用币种不足，先刷新多币种轮动面板。",
                "tone": "neutral",
                "module": "cross_asset",
            }
        )
    if bool(onchain.get("degraded")) or int(news_summary.get("events_count") or 0) == 0:
        action_items.append(
            {
                "id": "refresh_onchain_module",
                "kind": "module",
                "label": "刷新链上与外生",
                "description": "链上或新闻覆盖偏弱，先补齐外部上下文。",
                "tone": "warn",
                "module": "onchain",
            }
        )

    insight_cards: List[Dict[str, Any]] = []
    if factor_focus:
        insight_cards.append(
            {
                "title": "因子观察",
                "tone": "neutral",
                "body": " / ".join(
                    f"{item['symbol']} 评分 {item['score']:.2f}"
                    + (f" | 动量 {item['momentum']:.2f}" if item["momentum"] else "")
                    for item in factor_focus
                ),
            }
        )
    insight_cards.extend({"title": "下一步", "tone": "positive", "body": text} for text in next_actions[:4])
    insight_cards.extend({"title": "风险提示", "tone": "warn", "body": text} for text in avoid[:4])
    insight_cards.extend({"title": "研究观察", "tone": "neutral", "body": text} for text in thesis_points[:4])
    return {
        "direction_bias": direction_bias,
        "preferred_strategy_families": preferred,
        "avoid_conditions": avoid,
        "next_actions": next_actions,
        "backtest_jump_targets": jump_targets,
        "action_items": action_items[:4],
        "insight_cards": insight_cards[:8],
        "ai_brief": ai_brief,
        "factor_focus": factor_focus,
        "source_meta": factor_source_meta,
        "focus_symbols": focus_symbols,
        "headline": headline,
        "generated_at": _now_iso(),
    }


async def _get_research_workbench_context(exchange: str = "binance") -> Dict[str, Any]:
    symbols = await get_research_symbols(exchange=exchange)
    analytics_history_status = await _load_analytics_ingest_status_map()
    available_symbols = list(symbols.get("symbols") or [])
    default_symbols = available_symbols[:30] or list(_DEFAULT_UNIVERSE)
    profile = _normalize_profile(
        ResearchProfile(
            exchange=exchange,
            primary_symbol=default_symbols[0] if default_symbols else "BTC/USDT",
            universe_symbols=default_symbols,
            timeframe="5m",
            lookback=1200,
            exclude_retired=True,
            horizon="short_intraday",
        )
    )
    return {
        "profile": profile.model_dump(),
        "available_symbols": available_symbols,
        "defaults": {"overview_days": 3, "calendar_days": 7, "news_hours": 24},
        "available_modules": list(_MODULE_ORDER),
        "data_status": {
            "news_events_available": True,
            "analytics_history_collectors": analytics_history_status,
        },
        "generated_at": _now_iso(),
    }


async def _run_research_workbench_overview(payload: ResearchWorkbenchRequest) -> Dict[str, Any]:
    profile = _normalize_profile(payload.profile)
    module_names = list(_MODULE_ORDER)
    module_tasks = [_capture_module_build(name, profile) for name in module_names]
    module_results = await asyncio.gather(*module_tasks, return_exceptions=True)

    modules: Dict[str, Any] = {}
    for name, result in zip(module_names, module_results):
        if isinstance(result, Exception):
            modules[name] = _module_result(
                name,
                status="error",
                source_labels=[f"research.workbench.{name}"],
                warnings=[str(result)],
                summary={"headline": f"{name} 加载失败", "error": str(result)},
                payload={"error": str(result)},
            )
            continue
        modules[name] = dict(result or {})
    ok_count = len([module for module in modules.values() if module.get("status") == "ok"])
    degraded_count = len([module for module in modules.values() if module.get("status") == "degraded"])
    warnings: List[str] = []
    for module in modules.values():
        warnings.extend(module.get("warnings") or [])
    regime = dict(_extract_module_payload(modules.get("market_state")).get("regime") or {})
    return {
        "profile": profile.model_dump(),
        "market_regime": regime.get("regime") or "待确认",
        "direction_bias": regime.get("bias") or "neutral",
        "confidence": float(regime.get("confidence") or 0.0),
        "coverage": {
            "ok_count": ok_count,
            "degraded_count": degraded_count,
            "total": len(modules),
        },
        "warnings": warnings[:12],
        "modules": modules,
        "generated_at": _now_iso(),
    }


async def _run_research_workbench_module(module_name: str, payload: ResearchWorkbenchRequest) -> Dict[str, Any]:
    profile = _normalize_profile(payload.profile)
    return await _capture_module_build(module_name, profile)


async def _get_research_workbench_recommendations(payload: ResearchRecommendationRequest) -> Dict[str, Any]:
    profile = _normalize_profile(payload.profile)
    modules = dict(payload.modules or {})
    overview = dict(payload.overview or {})
    return _build_structured_recommendations(profile, modules, overview)


@router.get("/workbench/context")
async def get_research_workbench_context(exchange: str = "binance") -> Dict[str, Any]:
    return await _get_research_workbench_context(exchange)


@router.post("/workbench/overview")
async def run_research_workbench_overview(payload: ResearchWorkbenchRequest) -> Dict[str, Any]:
    return await _run_research_workbench_overview(payload)


@router.get("/workbench/overview")
async def run_research_workbench_overview_query(
    exchange: str = "binance",
    primary_symbol: str = "BTC/USDT",
    universe_symbols: str = "",
    timeframe: str = "5m",
    lookback: int = 1200,
    exclude_retired: bool = True,
    horizon: str = "short_intraday",
) -> Dict[str, Any]:
    profile = _build_profile_from_query(
        exchange=exchange,
        primary_symbol=primary_symbol,
        universe_symbols=universe_symbols,
        timeframe=timeframe,
        lookback=lookback,
        exclude_retired=exclude_retired,
        horizon=horizon,
    )
    return await _run_research_workbench_overview(ResearchWorkbenchRequest(profile=profile))


@router.post("/workbench/modules/{module_name}")
async def run_research_workbench_module(module_name: str, payload: ResearchWorkbenchRequest) -> Dict[str, Any]:
    return await _run_research_workbench_module(module_name, payload)


@router.get("/workbench/modules/{module_name}")
async def run_research_workbench_module_query(
    module_name: str,
    exchange: str = "binance",
    primary_symbol: str = "BTC/USDT",
    universe_symbols: str = "",
    timeframe: str = "5m",
    lookback: int = 1200,
    exclude_retired: bool = True,
    horizon: str = "short_intraday",
) -> Dict[str, Any]:
    profile = _build_profile_from_query(
        exchange=exchange,
        primary_symbol=primary_symbol,
        universe_symbols=universe_symbols,
        timeframe=timeframe,
        lookback=lookback,
        exclude_retired=exclude_retired,
        horizon=horizon,
    )
    return await _run_research_workbench_module(module_name, ResearchWorkbenchRequest(profile=profile))


@router.post("/workbench/recommendations")
async def get_research_workbench_recommendations(payload: ResearchRecommendationRequest) -> Dict[str, Any]:
    return await _get_research_workbench_recommendations(payload)


@router.get("/workbench/regime-calendar")
async def get_regime_calendar(
    exchange: str = "binance",
    symbol: str = "BTC/USDT",
    days: int = 7,
) -> Dict[str, Any]:
    """Return a daily market-regime timeline for the past N days.

    Uses stored microstructure + community snapshots to reconstruct the
    intraday regime label for each calendar day.
    """
    from config.database import AnalyticsMicrostructureSnapshot, AnalyticsCommunitySnapshot, async_session_maker as _asm
    from sqlalchemy import select as _sel

    days = max(1, min(int(days), 30))
    since = datetime.now(timezone.utc) - timedelta(days=days)
    sym_key = _normalize_symbol(symbol)

    try:
        async with _asm() as session:
            micro_stmt = (
                _sel(AnalyticsMicrostructureSnapshot)
                .where(
                    AnalyticsMicrostructureSnapshot.exchange == exchange,
                    AnalyticsMicrostructureSnapshot.symbol == sym_key,
                    AnalyticsMicrostructureSnapshot.timestamp >= since,
                    AnalyticsMicrostructureSnapshot.capture_status.in_(["ok", "degraded"]),
                )
                .order_by(AnalyticsMicrostructureSnapshot.timestamp.asc())
            )
            micro_rows = (await session.execute(micro_stmt)).scalars().all()
    except Exception as exc:
        return {"calendar": [], "error": str(exc), "generated_at": _now_iso()}

    # Group by calendar date (UTC)
    from collections import defaultdict
    daily: Dict[str, list] = defaultdict(list)
    for row in micro_rows:
        ts = row.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        date_str = ts.strftime("%Y-%m-%d")
        daily[date_str].append(row)

    calendar = []
    for date_str in sorted(daily.keys()):
        rows = daily[date_str]
        # Average key metrics over the day
        imbalances = [r.order_flow_imbalance for r in rows if r.order_flow_imbalance is not None]
        funding_rates = [r.funding_rate for r in rows if r.funding_rate is not None]
        basis_pcts = [r.basis_pct for r in rows if r.basis_pct is not None]
        spread_bps_list = [r.spread_bps for r in rows if r.spread_bps is not None]

        avg_imbalance = sum(imbalances) / len(imbalances) if imbalances else 0.0
        avg_funding = sum(funding_rates) / len(funding_rates) if funding_rates else None
        avg_basis = sum(basis_pcts) / len(basis_pcts) if basis_pcts else None
        avg_spread = sum(spread_bps_list) / len(spread_bps_list) if spread_bps_list else 0.0

        # Classify daily regime
        if avg_spread >= 8:
            regime = "高风险震荡"
            bias = "defensive"
        elif avg_imbalance >= 0.12:
            regime = "顺势偏多"
            bias = "bullish"
        elif avg_imbalance <= -0.12:
            regime = "顺势偏空"
            bias = "bearish"
        elif abs(avg_imbalance) <= 0.05:
            regime = "低信息震荡"
            bias = "neutral"
        else:
            regime = "事件驱动混合"
            bias = "neutral"

        calendar.append({
            "date": date_str,
            "regime": regime,
            "bias": bias,
            "avg_imbalance": round(avg_imbalance, 4),
            "avg_funding": round(avg_funding, 6) if avg_funding is not None else None,
            "avg_basis": round(avg_basis, 4) if avg_basis is not None else None,
            "avg_spread_bps": round(avg_spread, 2),
            "snapshot_count": len(rows),
        })

    return {
        "symbol": sym_key,
        "exchange": exchange,
        "days": days,
        "calendar": calendar,
        "generated_at": _now_iso(),
    }
