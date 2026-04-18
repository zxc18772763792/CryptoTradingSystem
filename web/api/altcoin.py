"""Altcoin radar API routes."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select

from config.database import (
    AnalyticsCommunitySnapshot,
    AnalyticsMicrostructureSnapshot,
    AnalyticsWhaleSnapshot,
    async_session_maker,
)
from core.notifications import notification_manager
from core.research.altcoin_radar import (
    VALID_TIMEFRAMES,
    build_altcoin_rows,
    build_detail_payload,
    sort_rows,
    summarize_rows,
)
from web.api.auth import require_sensitive_ops_permissions
from web.api.data import (
    _load_symbol_df,
    _research_retired_filter,
    get_factor_library,
    get_multi_assets_overview,
    get_onchain_overview,
    get_research_symbols,
)


router = APIRouter()

DEFAULT_EXCHANGE = "binance"
DEFAULT_TIMEFRAME = "4h"
DEFAULT_LIMIT = 30
DEFAULT_SORT = "layout"
MAX_UNIVERSE_SIZE = 30
TTL_BY_TIMEFRAME = {"1h": 120.0, "4h": 300.0, "1d": 900.0}
ALLOWED_SORTS = {"layout", "alert", "anomaly", "accumulation", "control", "chain", "heat"}
_ALTCOIN_SCAN_CACHE: Dict[str, Dict[str, Any]] = {}
_ALTCOIN_SCAN_LOCKS: Dict[str, asyncio.Lock] = {}


class AltcoinAlertPresetRequest(BaseModel):
    preset: str
    exchange: str = DEFAULT_EXCHANGE
    timeframe: str = DEFAULT_TIMEFRAME
    symbol: str
    universe_symbols: List[str] = Field(default_factory=list)
    channels: List[str] = Field(default_factory=lambda: ["feishu"])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _clone_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(payload or {}))


def _normalize_symbols(symbols: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for symbol in symbols:
        text = str(symbol or "").strip().upper()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _parse_symbols_param(symbols: Optional[str]) -> List[str]:
    if not symbols:
        return []
    parts = [part.strip() for part in str(symbols).replace(";", ",").split(",")]
    return _normalize_symbols(parts)


def _normalize_exchange(exchange: str) -> str:
    text = str(exchange or DEFAULT_EXCHANGE).strip().lower()
    return text or DEFAULT_EXCHANGE


def _normalize_timeframe(timeframe: str) -> str:
    tf = str(timeframe or DEFAULT_TIMEFRAME).strip().lower()
    if tf not in VALID_TIMEFRAMES:
        return DEFAULT_TIMEFRAME
    return tf


def _normalize_sort(sort_by: str) -> str:
    text = str(sort_by or DEFAULT_SORT).strip().lower()
    if text not in ALLOWED_SORTS:
        return DEFAULT_SORT
    return text


def _cache_ttl(timeframe: str) -> float:
    return float(TTL_BY_TIMEFRAME.get(_normalize_timeframe(timeframe), TTL_BY_TIMEFRAME[DEFAULT_TIMEFRAME]))


def _hash_universe(symbols: Sequence[str]) -> str:
    normalized = _normalize_symbols(symbols)
    raw = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def build_altcoin_notification_config_key(
    *,
    exchange: str,
    timeframe: str,
    universe_symbols: Sequence[str],
    exclude_retired: bool = True,
) -> str:
    payload = {
        "exchange": _normalize_exchange(exchange),
        "timeframe": _normalize_timeframe(timeframe),
        "universe_symbols": _normalize_symbols(universe_symbols),
        "exclude_retired": bool(exclude_retired),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_key(*, exchange: str, timeframe: str, symbols: Sequence[str], exclude_retired: bool) -> str:
    universe_hash = _hash_universe(symbols)
    return f"{_normalize_exchange(exchange)}|{_normalize_timeframe(timeframe)}|{universe_hash}|{bool(exclude_retired)}"


def _cache_lock(cache_key: str) -> asyncio.Lock:
    lock = _ALTCOIN_SCAN_LOCKS.get(cache_key)
    if lock is None:
        lock = asyncio.Lock()
        _ALTCOIN_SCAN_LOCKS[cache_key] = lock
    return lock


def _serialize_micro_snapshot(row: AnalyticsMicrostructureSnapshot) -> Dict[str, Any]:
    payload = dict(row.payload or {})
    return {
        "exchange": row.exchange,
        "symbol": row.symbol,
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "available": row.capture_status != "failed" and float(row.mid_price or 0.0) > 0.0,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "capture_status": row.capture_status,
        "latency_ms": row.latency_ms,
        "payload": payload,
        "orderbook": {
            "mid_price": float(row.mid_price or 0.0),
            "spread_bps": float(row.spread_bps or 0.0),
        },
        "aggressor_flow": {
            "imbalance": float(row.order_flow_imbalance or 0.0),
            "buy_ratio": float(row.buy_ratio or 0.0),
            "sell_ratio": float(row.sell_ratio or 0.0),
        },
        "funding_rate": {
            "available": row.funding_rate is not None,
            "funding_rate": row.funding_rate,
        },
        "spot_futures_basis": {
            "available": row.basis_pct is not None,
            "basis_pct": row.basis_pct,
        },
    }


def _serialize_community_snapshot(row: AnalyticsCommunitySnapshot) -> Dict[str, Any]:
    payload = dict(row.payload or {})
    return {
        "exchange": row.exchange,
        "symbol": row.symbol,
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "capture_status": row.capture_status,
        "latency_ms": row.latency_ms,
        "payload": payload,
        "flow_proxy": {
            "imbalance": float(row.flow_imbalance or 0.0),
            "buy_ratio": float(row.buy_ratio or 0.0),
            "sell_ratio": float(row.sell_ratio or 0.0),
        },
        "announcements": list(payload.get("announcements") or []),
        "security_alerts": payload.get("security_alerts") or {},
        "twitter_watchlist": list(payload.get("twitter_watchlist") or []),
    }


def _serialize_whale_snapshot(row: AnalyticsWhaleSnapshot) -> Dict[str, Any]:
    payload = dict(row.payload or {})
    return {
        "exchange": row.exchange,
        "symbol": row.symbol,
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "available": row.capture_status != "failed",
        "source_error": row.source_error,
        "source_name": row.source_name,
        "capture_status": row.capture_status,
        "latency_ms": row.latency_ms,
        "payload": payload,
        "count": int(row.whale_count or 0),
        "threshold_btc": payload.get("threshold_btc"),
        "btc_price": payload.get("btc_price"),
        "transactions": list(payload.get("transactions") or []),
    }


async def _load_latest_snapshot_map(
    model: Any,
    *,
    exchange: str,
    symbols: Sequence[str],
) -> List[Any]:
    normalized = _normalize_symbols(symbols)
    if not normalized:
        return []
    async with async_session_maker() as session:
        result = await session.execute(
            select(model)
            .where(model.exchange == exchange, model.symbol.in_(normalized))
            .order_by(model.symbol.asc(), model.timestamp.desc())
        )
        rows = result.scalars().all()
    latest_by_symbol: Dict[str, Any] = {}
    for row in rows:
        key = str(getattr(row, "symbol", "") or "").strip().upper()
        if not key or key in latest_by_symbol:
            continue
        latest_by_symbol[key] = row
    return [latest_by_symbol[symbol] for symbol in normalized if symbol in latest_by_symbol]


async def _load_snapshot_maps(
    *,
    exchange: str,
    symbols: Sequence[str],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    micro_rows, community_rows, whale_rows = await asyncio.gather(
        _load_latest_snapshot_map(AnalyticsMicrostructureSnapshot, exchange=exchange, symbols=symbols),
        _load_latest_snapshot_map(AnalyticsCommunitySnapshot, exchange=exchange, symbols=symbols),
        _load_latest_snapshot_map(AnalyticsWhaleSnapshot, exchange=exchange, symbols=symbols),
    )
    micro = {
        str(row.symbol).strip().upper(): _serialize_micro_snapshot(row)
        for row in micro_rows
        if getattr(row, "symbol", None)
    }
    community = {
        str(row.symbol).strip().upper(): _serialize_community_snapshot(row)
        for row in community_rows
        if getattr(row, "symbol", None)
    }
    whale = {
        str(row.symbol).strip().upper(): _serialize_whale_snapshot(row)
        for row in whale_rows
        if getattr(row, "symbol", None)
    }
    return micro, community, whale


async def _resolve_universe(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
) -> Tuple[List[str], List[str], List[str], List[str]]:
    requested = _normalize_symbols(symbols)
    fallback_used = False
    if not requested:
        research_symbols = await get_research_symbols(exchange=exchange)
        requested = _normalize_symbols((research_symbols.get("symbols") or [])[:MAX_UNIVERSE_SIZE])
        fallback_used = True
    filtered, excluded_retired = _research_retired_filter(
        exchange=exchange,
        timeframe=timeframe,
        requested=requested,
        exclude_retired=exclude_retired,
    )
    filtered = _normalize_symbols(filtered)[:MAX_UNIVERSE_SIZE]
    if not filtered:
        research_symbols = await get_research_symbols(exchange=exchange)
        requested = _normalize_symbols((research_symbols.get("symbols") or [])[:MAX_UNIVERSE_SIZE])
        filtered, excluded_retired = _research_retired_filter(
            exchange=exchange,
            timeframe=timeframe,
            requested=requested,
            exclude_retired=exclude_retired,
        )
        filtered = _normalize_symbols(filtered)[:MAX_UNIVERSE_SIZE]
        fallback_used = True
    warnings: List[str] = []
    if fallback_used:
        warnings.append("symbols 为空或不可用，已回退到 research universe 默认币池。")
    return requested[:MAX_UNIVERSE_SIZE], filtered, excluded_retired, warnings


async def _load_market_frames(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    warnings: List[str] = []
    tasks = [_load_symbol_df(exchange=exchange, symbol=symbol, timeframe=timeframe) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    frames: Dict[str, pd.DataFrame] = {}
    for symbol, result in zip(symbols, results):
        if isinstance(result, Exception):
            warnings.append(f"{symbol} K 线加载失败: {result}")
            continue
        frame = result.copy()
        if frame.empty:
            warnings.append(f"{symbol} 本地 K 线为空，已跳过。")
            continue
        frames[str(symbol).strip().upper()] = frame.sort_index()
    return frames, warnings


async def _load_active_altcoin_rules() -> List[Dict[str, Any]]:
    rules = await notification_manager.list_rules()
    return [
        dict(rule or {})
        for rule in rules
        if bool(rule.get("enabled"))
        and str(rule.get("rule_type") or "") in {"altcoin_score_above", "altcoin_rank_top_n"}
    ]


def _universe_matches(rule_symbols: Sequence[str], current_symbols: Sequence[str]) -> bool:
    left = _normalize_symbols(rule_symbols)
    right = _normalize_symbols(current_symbols)
    if not left or not right:
        return False
    return left == right


def _alerted_symbols_for_scan(
    rules: Sequence[Mapping[str, Any]],
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
) -> List[str]:
    out: List[str] = []
    for rule in rules:
        params = dict(rule.get("params") or {})
        if str(params.get("exchange") or "").strip().lower() != exchange:
            continue
        if str(params.get("timeframe") or "").strip().lower() != timeframe:
            continue
        universe = params.get("universe_symbols") or []
        universe_list = universe if isinstance(universe, list) else _parse_symbols_param(str(universe))
        if universe_list and not _universe_matches(universe_list, symbols):
            continue
        symbol = str(params.get("symbol") or "").strip().upper()
        if symbol:
            out.append(symbol)
    return _normalize_symbols(out)


async def _compute_scan_payload(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
) -> Dict[str, Any]:
    requested_symbols, symbols_used, excluded_retired, warnings = await _resolve_universe(
        exchange=exchange,
        timeframe=timeframe,
        symbols=symbols,
        exclude_retired=exclude_retired,
    )
    frames, frame_warnings = await _load_market_frames(exchange=exchange, timeframe=timeframe, symbols=symbols_used)
    warnings.extend(frame_warnings)
    symbols_used = _normalize_symbols(frames.keys())
    if not symbols_used:
        return {
            "exchange": exchange,
            "timeframe": timeframe,
            "symbols_requested": requested_symbols,
            "symbols_used": [],
            "excluded_retired": excluded_retired,
            "warnings": warnings + ["没有可用于山寨雷达扫描的本地 K 线数据。"],
            "rows": [],
            "generated_at": _utcnow().isoformat(),
        }

    symbol_csv = ",".join(symbols_used)
    factor_task = get_factor_library(
        exchange=exchange,
        symbols=symbol_csv,
        timeframe=timeframe,
        lookback=600 if timeframe == "1h" else 900 if timeframe == "4h" else 1200,
        quantile=0.3,
        series_limit=500,
        exclude_retired=exclude_retired,
    )
    multi_task = get_multi_assets_overview(
        exchange=exchange,
        symbols=symbol_csv,
        timeframe=timeframe,
        lookback=400 if timeframe == "1h" else 300 if timeframe == "4h" else 240,
        exclude_retired=exclude_retired,
    )
    snapshots_task = _load_snapshot_maps(exchange=exchange, symbols=symbols_used)
    rules_task = _load_active_altcoin_rules()
    factor_payload, multi_payload, snapshots, rules = await asyncio.gather(
        factor_task,
        multi_task,
        snapshots_task,
        rules_task,
    )
    micro_map, community_map, whale_map = snapshots
    alerted_symbols = _alerted_symbols_for_scan(
        rules,
        exchange=exchange,
        timeframe=timeframe,
        symbols=symbols_used,
    )

    if factor_payload.get("warnings"):
        warnings.extend([str(item) for item in (factor_payload.get("warnings") or [])[:4]])
    if multi_payload.get("retired_filter", {}).get("excluded_symbols"):
        warnings.append("部分币种因 retired_like 被排除。")
    rows = build_altcoin_rows(
        market_frames=frames,
        timeframe=timeframe,
        factor_library=factor_payload,
        multi_assets=multi_payload,
        micro_snapshots=micro_map,
        community_snapshots=community_map,
        whale_snapshots=whale_map,
        alerted_symbols=alerted_symbols,
    )
    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "symbols_requested": requested_symbols,
        "symbols_used": symbols_used,
        "excluded_retired": excluded_retired,
        "warnings": _normalize_symbols([]) and [] or list(dict.fromkeys(warnings)),
        "rows": rows,
        "generated_at": _utcnow().isoformat(),
    }


async def get_altcoin_scan_snapshot(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool = True,
    refresh: bool = False,
) -> Dict[str, Any]:
    normalized_exchange = _normalize_exchange(exchange)
    normalized_timeframe = _normalize_timeframe(timeframe)
    normalized_symbols = _normalize_symbols(symbols)
    requested_symbols, filtered_symbols, _, pre_warnings = await _resolve_universe(
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        symbols=normalized_symbols,
        exclude_retired=exclude_retired,
    )
    cache_key = _cache_key(
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        symbols=filtered_symbols or requested_symbols,
        exclude_retired=exclude_retired,
    )
    cached_entry = _ALTCOIN_SCAN_CACHE.get(cache_key)
    ttl = _cache_ttl(normalized_timeframe)
    now_ts = time.time()
    if cached_entry and not refresh:
        age_sec = max(0.0, now_ts - float(cached_entry.get("stored_at", 0.0)))
        if age_sec <= ttl:
            payload = _clone_payload(cached_entry.get("payload") or {})
            payload["cache"] = {
                "cache_key": cache_key,
                "hit": True,
                "age_sec": round(age_sec, 3),
                "ttl_sec": ttl,
            }
            return payload
    async with _cache_lock(cache_key):
        cached_entry = _ALTCOIN_SCAN_CACHE.get(cache_key)
        if cached_entry and not refresh:
            age_sec = max(0.0, now_ts - float(cached_entry.get("stored_at", 0.0)))
            if age_sec <= ttl:
                payload = _clone_payload(cached_entry.get("payload") or {})
                payload["cache"] = {
                    "cache_key": cache_key,
                    "hit": True,
                    "age_sec": round(age_sec, 3),
                    "ttl_sec": ttl,
                }
                return payload
        payload = await _compute_scan_payload(
            exchange=normalized_exchange,
            timeframe=normalized_timeframe,
            symbols=filtered_symbols or requested_symbols,
            exclude_retired=exclude_retired,
        )
        payload["warnings"] = list(dict.fromkeys(pre_warnings + list(payload.get("warnings") or [])))
        stored_at = time.time()
        _ALTCOIN_SCAN_CACHE[cache_key] = {"stored_at": stored_at, "payload": _clone_payload(payload)}
        payload["cache"] = {
            "cache_key": cache_key,
            "hit": False,
            "age_sec": 0.0,
            "ttl_sec": ttl,
        }
        return payload


def _build_scan_response(
    *,
    scan_payload: Mapping[str, Any],
    sort_by: str,
    limit: int,
) -> Dict[str, Any]:
    normalized_sort = _normalize_sort(sort_by)
    rows = sort_rows(scan_payload.get("rows") or [], sort_by=normalized_sort)
    limited_rows = rows[: max(1, min(int(limit or DEFAULT_LIMIT), MAX_UNIVERSE_SIZE))]
    summarized = summarize_rows(
        rows=rows,
        exchange=str(scan_payload.get("exchange") or DEFAULT_EXCHANGE),
        timeframe=str(scan_payload.get("timeframe") or DEFAULT_TIMEFRAME),
        sort_by=normalized_sort,
        symbols_requested=scan_payload.get("symbols_requested") or [],
        symbols_used=scan_payload.get("symbols_used") or [],
        excluded_retired=scan_payload.get("excluded_retired") or [],
        cache_key=str((scan_payload.get("cache") or {}).get("cache_key") or ""),
        warnings=scan_payload.get("warnings") or [],
    )
    response = dict(summarized)
    response["rows"] = limited_rows
    response["scan_meta"] = {
        **response.get("scan_meta", {}),
        "generated_at": scan_payload.get("generated_at"),
        "cache": scan_payload.get("cache") or {},
        "row_count_before_limit": len(rows),
        "limit": max(1, min(int(limit or DEFAULT_LIMIT), MAX_UNIVERSE_SIZE)),
    }
    return response


def _score_key_to_field(score_key: str) -> str:
    normalized = str(score_key or "").strip().lower()
    mapping = {
        "layout": "layout_score",
        "alert": "alert_score",
        "anomaly": "anomaly_score",
        "accumulation": "accumulation_score",
        "control": "control_score",
    }
    return mapping.get(normalized, "layout_score")


async def build_altcoin_notification_context(
    rules: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    active_rules = [
        dict(rule or {})
        for rule in rules
        if bool(rule.get("enabled"))
        and str(rule.get("rule_type") or "") in {"altcoin_score_above", "altcoin_rank_top_n"}
    ]
    unique_configs: Dict[str, Dict[str, Any]] = {}
    for rule in active_rules:
        params = dict(rule.get("params") or {})
        exchange = _normalize_exchange(str(params.get("exchange") or DEFAULT_EXCHANGE))
        timeframe = _normalize_timeframe(str(params.get("timeframe") or DEFAULT_TIMEFRAME))
        universe = params.get("universe_symbols") or []
        universe_symbols = universe if isinstance(universe, list) else _parse_symbols_param(str(universe))
        universe_symbols = _normalize_symbols(universe_symbols)
        exclude_retired = bool(params.get("exclude_retired", True))
        config_key = str(params.get("config_key") or "").strip() or build_altcoin_notification_config_key(
            exchange=exchange,
            timeframe=timeframe,
            universe_symbols=universe_symbols,
            exclude_retired=exclude_retired,
        )
        unique_configs[config_key] = {
            "exchange": exchange,
            "timeframe": timeframe,
            "universe_symbols": universe_symbols,
            "exclude_retired": exclude_retired,
        }
    if not unique_configs:
        return {}

    tasks = {
        config_key: get_altcoin_scan_snapshot(
            exchange=config["exchange"],
            timeframe=config["timeframe"],
            symbols=config["universe_symbols"],
            exclude_retired=bool(config["exclude_retired"]),
            refresh=False,
        )
        for config_key, config in unique_configs.items()
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    scans: Dict[str, Any] = {}
    for config_key, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            scans[config_key] = {"error": str(result), "rows": [], "sort_indexes": {}}
            continue
        layout_rows = sort_rows(result.get("rows") or [], sort_by="layout")
        alert_rows = sort_rows(result.get("rows") or [], sort_by="alert")
        control_rows = sort_rows(result.get("rows") or [], sort_by="control")
        scans[config_key] = {
            "config": unique_configs[config_key],
            "generated_at": result.get("generated_at"),
            "warnings": result.get("warnings") or [],
            "rows": result.get("rows") or [],
            "sort_indexes": {
                "layout": {str(row.get("symbol") or ""): int(row.get("rank") or 0) for row in layout_rows},
                "alert": {str(row.get("symbol") or ""): int(row.get("rank") or 0) for row in alert_rows},
                "control": {str(row.get("symbol") or ""): int(row.get("rank") or 0) for row in control_rows},
            },
        }
    return {"scans": scans}


@router.get("/radar/scan")
async def scan_altcoin_radar(
    exchange: str = DEFAULT_EXCHANGE,
    timeframe: str = DEFAULT_TIMEFRAME,
    symbols: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    sort_by: str = DEFAULT_SORT,
    exclude_retired: bool = True,
    refresh: bool = False,
):
    normalized_symbols = _parse_symbols_param(symbols)
    scan_payload = await get_altcoin_scan_snapshot(
        exchange=exchange,
        timeframe=timeframe,
        symbols=normalized_symbols,
        exclude_retired=exclude_retired,
        refresh=refresh,
    )
    return _build_scan_response(scan_payload=scan_payload, sort_by=sort_by, limit=limit)


@router.get("/radar/detail")
async def get_altcoin_radar_detail(
    exchange: str = DEFAULT_EXCHANGE,
    timeframe: str = DEFAULT_TIMEFRAME,
    symbol: str = "",
    symbols: Optional[str] = None,
    refresh: bool = False,
    exclude_retired: bool = True,
):
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    normalized_symbols = _parse_symbols_param(symbols)
    scan_payload = await get_altcoin_scan_snapshot(
        exchange=exchange,
        timeframe=timeframe,
        symbols=normalized_symbols,
        exclude_retired=exclude_retired,
        refresh=refresh,
    )
    onchain_context = await get_onchain_overview(
        symbol=normalized_symbol,
        exchange=_normalize_exchange(exchange),
        whale_threshold_btc=10.0,
        chain="Ethereum",
        refresh=refresh,
        hours=4,
    )
    detail = build_detail_payload(
        rows=scan_payload.get("rows") or [],
        symbol=normalized_symbol,
        sort_by=_normalize_sort("layout"),
        onchain_context=onchain_context,
    )
    detail["scan_meta"] = {
        "exchange": scan_payload.get("exchange"),
        "timeframe": scan_payload.get("timeframe"),
        "symbols_used": scan_payload.get("symbols_used") or [],
        "generated_at": scan_payload.get("generated_at"),
        "cache": scan_payload.get("cache") or {},
    }
    return detail


@router.post("/alerts/preset", dependencies=[Depends(require_sensitive_ops_permissions("manage_notifications"))])
async def create_altcoin_alert_preset(request: AltcoinAlertPresetRequest):
    preset = str(request.preset or "").strip()
    normalized_exchange = _normalize_exchange(request.exchange)
    normalized_timeframe = _normalize_timeframe(request.timeframe)
    symbol = str(request.symbol or "").strip().upper()
    if preset not in {"异动预警", "吸筹预警", "高控盘预警"}:
        raise HTTPException(status_code=400, detail="unsupported preset")
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    universe_symbols = _normalize_symbols(request.universe_symbols) or [symbol]
    config_key = build_altcoin_notification_config_key(
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        universe_symbols=universe_symbols,
        exclude_retired=True,
    )
    rule_type = "altcoin_score_above"
    if preset == "异动预警":
        score_key = "anomaly"
        threshold = 0.72
    elif preset == "吸筹预警":
        score_key = "accumulation"
        threshold = 0.68
    else:
        score_key = "control"
        threshold = 0.70

    rule_name = f"山寨雷达 | {preset} | {symbol} | {normalized_exchange} {normalized_timeframe}"
    params = {
        "exchange": normalized_exchange,
        "timeframe": normalized_timeframe,
        "universe_symbols": universe_symbols,
        "symbol": symbol,
        "score_key": score_key,
        "threshold": threshold,
        "channels": list(request.channels or ["feishu"]),
        "source_page": "altcoin_radar",
        "exclude_retired": True,
        "config_key": config_key,
    }
    existing_rules = await notification_manager.list_rules()
    existing = next(
        (
            rule
            for rule in existing_rules
            if bool(rule.get("enabled"))
            and str(rule.get("rule_type") or "") == rule_type
            and dict(rule.get("params") or {}).get("symbol") == symbol
            and dict(rule.get("params") or {}).get("score_key") == score_key
            and str(dict(rule.get("params") or {}).get("exchange") or "") == normalized_exchange
            and str(dict(rule.get("params") or {}).get("timeframe") or "") == normalized_timeframe
            and float(dict(rule.get("params") or {}).get("threshold") or 0.0) == threshold
        ),
        None,
    )
    if existing:
        return {"success": True, "existing": True, "rule": existing}

    rule = await notification_manager.add_rule(
        name=rule_name,
        rule_type=rule_type,
        params=params,
        enabled=True,
        cooldown_seconds=300,
    )
    return {"success": True, "existing": False, "rule": rule}
