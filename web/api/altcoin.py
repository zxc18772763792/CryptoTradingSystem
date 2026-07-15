"""Altcoin radar API routes."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import httpx
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from config.database import (
    AnalyticsCommunitySnapshot,
    AnalyticsDerivativesSnapshot,
    AnalyticsMicrostructureSnapshot,
    AnalyticsWhaleSnapshot,
    async_session_maker,
)
from core.notifications import notification_manager
from core.data.coinglass_altcoin import (
    build_derivatives_snapshot_from_market_snapshot,
    is_alt_candidate_symbol,
    load_coinglass_market_snapshots,
)
from core.data.coinglass_registry import normalize_coinglass_symbol
from core.research.altcoin_radar import (
    TIMEFRAME_SECONDS,
    VALID_TIMEFRAMES,
    build_altcoin_rows,
    build_detail_payload,
    sort_rows,
    summarize_rows,
)
from core.research.altcoin_radar_events import get_all_recent_events, get_recent_symbol_events
from core.research.altcoin_radar_universe import (
    add_watchlist_symbol,
    get_watchlist_symbols,
    normalize_altcoin_pair,
    remove_watchlist_symbol,
    resolve_universe_scope,
    universe_meta,
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
# Budgets for the radar-detail external sources (onchain overview / live chain
# context). Production incident: an unbounded chain call hung the whole detail
# endpoint past the frontend's 60s budget, so the drawer never opened.
DETAIL_ONCHAIN_TIMEOUT_SEC = 15.0
DETAIL_LIVE_CHAIN_TIMEOUT_SEC = 10.0
DEFAULT_LIMIT = 30
DEFAULT_SORT = "priority"
MAX_UNIVERSE_SIZE = 30
MAX_EXPANDED_SIZE = 100
TTL_BY_TIMEFRAME = {"1h": 120.0, "4h": 300.0, "1d": 900.0}
# Phase 1: shorter cache for faster radar views
TTL_BY_VIEW = {"15m": 30.0, "1h": 60.0, "4h": 300.0}
ALLOWED_SORTS = {
    "priority", "layout", "alert", "anomaly", "accumulation", "control", "chain", "heat",
    # Phase 1 new sorts
    "ignition", "continuation", "rank_jump", "crowding",
    # Phase 2 new sorts
    "narrative", "meme_rotation",
}
ALLOWED_MODES = {"perp", "narrative", "combined"}
ALLOWED_VIEWS = {"15m", "1h", "4h"}
ALLOWED_UNIVERSE_SCOPES = {"research", "expanded", "watchlist"}
ALTCOIN_RULE_TYPES = {
    "altcoin_score_above",
    "altcoin_rank_top_n",
    "altcoin_ignition_cross_up",
    "altcoin_rank_jump_top_n",
    "altcoin_crowding_risk_spike",
    "altcoin_narrative_heat_spike",
}
_ALTCOIN_SCAN_CACHE: Dict[str, Dict[str, Any]] = {}
_ALTCOIN_SCAN_LOCKS: Dict[str, asyncio.Lock] = {}
_ALTCOIN_SCAN_LOCKS_GUARD = threading.Lock()
_ALTCOIN_SCAN_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_PUBLIC_MARKET_SNAPSHOT_CACHE: Dict[str, Dict[str, Any]] = {}
_PUBLIC_MARKET_SNAPSHOT_TTL_SEC = 45.0


class AltcoinAlertPresetRequest(BaseModel):
    preset: str
    exchange: str = DEFAULT_EXCHANGE
    timeframe: str = DEFAULT_TIMEFRAME
    symbol: str
    universe_symbols: List[str] = Field(default_factory=list)
    channels: List[str] = Field(default_factory=lambda: ["feishu"])
    mode: str = "combined"
    view: str = ""
    universe_scope: str = "research"


class AltcoinWatchlistMutationRequest(BaseModel):
    symbol: str


def _preset_definition(preset: str) -> Tuple[str, str, float, str]:
    text = str(preset or "").strip()
    if text == "点火预警":
        return "altcoin_ignition_cross_up", "ignition", 0.60, "anomaly"
    if text == "跃升预警":
        return "altcoin_rank_jump_top_n", "rank_jump", 0.30, "accumulation"
    if text == "拥挤预警":
        return "altcoin_crowding_risk_spike", "crowding", 0.65, "control"
    if text == "叙事预警":
        return "altcoin_narrative_heat_spike", "narrative", 0.55, "narrative"
    if text == "异动预警":
        return "altcoin_score_above", "anomaly", 0.72, "legacy_anomaly"
    if text == "吸筹预警":
        return "altcoin_score_above", "accumulation", 0.68, "legacy_accumulation"
    if text == "高控盘预警":
        return "altcoin_score_above", "control", 0.70, "legacy_control"
    raise HTTPException(status_code=400, detail="unsupported preset")


def _preset_label_from_rule(rule_type: str, score_key: str) -> str:
    normalized_type = str(rule_type or "").strip()
    normalized_key = str(score_key or "").strip().lower()
    if normalized_type == "altcoin_ignition_cross_up" or normalized_key == "ignition":
        return "点火预警"
    if normalized_type == "altcoin_rank_jump_top_n" or normalized_key == "rank_jump":
        return "跃升预警"
    if normalized_type == "altcoin_crowding_risk_spike" or normalized_key == "crowding":
        return "拥挤预警"
    if normalized_type == "altcoin_narrative_heat_spike" or normalized_key == "narrative":
        return "叙事预警"
    if normalized_key == "anomaly":
        return "异动预警"
    if normalized_key == "accumulation":
        return "吸筹预警"
    if normalized_key == "control":
        return "高控盘预警"
    return "山寨预警"


def _alert_kind_from_rule(rule_type: str, score_key: str) -> str:
    _, _, _, kind = _preset_definition(_preset_label_from_rule(rule_type, score_key))
    return kind


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _clone_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(dict(payload or {}))


def _normalize_symbols(symbols: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for symbol in symbols:
        text = normalize_altcoin_pair(symbol)
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


def _normalize_mode(mode: str) -> str:
    text = str(mode or "combined").strip().lower()
    return text if text in ALLOWED_MODES else "combined"


def _normalize_view(view: str) -> str:
    text = str(view or "4h").strip().lower()
    return text if text in ALLOWED_VIEWS else "4h"


def _normalize_universe_scope(scope: str) -> str:
    text = str(scope or "research").strip().lower()
    return text if text in ALLOWED_UNIVERSE_SCOPES else "research"


def _cache_ttl(timeframe: str, view: str = "") -> float:
    if view and view in TTL_BY_VIEW:
        return TTL_BY_VIEW[view]
    return float(TTL_BY_TIMEFRAME.get(_normalize_timeframe(timeframe), TTL_BY_TIMEFRAME[DEFAULT_TIMEFRAME]))


def _resolve_requested_timeframe(*, timeframe: str, view: str) -> str:
    normalized_view = _normalize_view(view) if view else ""
    if normalized_view:
        return _normalize_timeframe(normalized_view)
    return _normalize_timeframe(timeframe)


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
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
) -> str:
    payload = {
        "exchange": _normalize_exchange(exchange),
        "timeframe": _normalize_timeframe(timeframe),
        "universe_symbols": _normalize_symbols(universe_symbols),
        "exclude_retired": bool(exclude_retired),
        "mode": _normalize_mode(mode),
        "view": _normalize_view(view) if view else "",
        "universe_scope": _normalize_universe_scope(universe_scope),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_key(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
) -> str:
    universe_hash = _hash_universe(symbols)
    return (
        f"{_normalize_exchange(exchange)}"
        f"|{_normalize_timeframe(timeframe)}"
        f"|{universe_hash}"
        f"|{bool(exclude_retired)}"
        f"|{_normalize_mode(mode)}"
        f"|{_normalize_view(view) if view else 'none'}"
        f"|{_normalize_universe_scope(universe_scope)}"
    )


def _cache_lock(cache_key: str) -> asyncio.Lock:
    with _ALTCOIN_SCAN_LOCKS_GUARD:
        lock = _ALTCOIN_SCAN_LOCKS.get(cache_key)
        if lock is None:
            lock = asyncio.Lock()
            _ALTCOIN_SCAN_LOCKS[cache_key] = lock
        return lock


def _cache_age_sec(cached_entry: Optional[Mapping[str, Any]], now_ts: Optional[float] = None) -> float:
    if not cached_entry:
        return 0.0
    current_ts = float(now_ts if now_ts is not None else time.time())
    return max(0.0, current_ts - float(cached_entry.get("stored_at", 0.0)))


def _build_cached_scan_payload(
    *,
    cached_entry: Mapping[str, Any],
    cache_key: str,
    ttl: float,
    now_ts: Optional[float] = None,
    stale: bool = False,
    refreshing: bool = False,
    served_mode: str = "cache_hit",
    warnings: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    payload = _clone_payload(cached_entry.get("payload") or {})
    merged_warnings = [
        str(item)
        for item in list(payload.get("warnings") or []) + list(warnings or [])
        if str(item or "").strip()
    ]
    payload["warnings"] = list(dict.fromkeys(merged_warnings))
    payload["cache"] = {
        "cache_key": cache_key,
        "hit": True,
        "age_sec": round(_cache_age_sec(cached_entry, now_ts), 3),
        "ttl_sec": ttl,
        "stale": bool(stale),
        "refreshing": bool(refreshing),
        "served_mode": str(served_mode or "cache_hit"),
    }
    return payload


def _finalize_scan_payload(
    *,
    payload: Mapping[str, Any],
    cache_key: str,
    ttl: float,
    mode: str,
    view: str,
    timeframe: str,
    pre_warnings: Sequence[str],
    cache_hit: bool,
    age_sec: float = 0.0,
    stale: bool = False,
    refreshing: bool = False,
    served_mode: str = "live_compute",
) -> Dict[str, Any]:
    final_payload = _clone_payload(payload)
    final_payload["mode"] = _normalize_mode(mode)
    final_payload["view"] = (_normalize_view(view) if view else "") or _normalize_timeframe(timeframe)
    merged_warnings = [
        str(item)
        for item in list(pre_warnings or []) + list(final_payload.get("warnings") or [])
        if str(item or "").strip()
    ]
    final_payload["warnings"] = list(dict.fromkeys(merged_warnings))
    final_payload["cache"] = {
        "cache_key": cache_key,
        "hit": bool(cache_hit),
        "age_sec": round(float(age_sec or 0.0), 3),
        "ttl_sec": ttl,
        "stale": bool(stale),
        "refreshing": bool(refreshing),
        "served_mode": str(served_mode or ("cache_hit" if cache_hit else "live_compute")),
    }
    return final_payload


def _should_cache_scan_payload(payload: Mapping[str, Any]) -> bool:
    rows = list(payload.get("rows") or [])
    if not rows:
        return False
    for row in rows:
        freshness = dict((row or {}).get("freshness") or {})
        data_quality = dict((row or {}).get("data_quality") or {})
        if bool(freshness.get("using_market_snapshot") or data_quality.get("using_market_snapshot")):
            return True
        market_freshness = _safe_float(data_quality.get("market_data_freshness"), 0.0)
        if market_freshness >= 0.45:
            return True
    return False


async def _refresh_altcoin_scan_cache(
    *,
    cache_key: str,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
    refresh: bool,
    mode: str,
    view: str,
    universe_scope: str,
    resolved_universe: Tuple[List[str], List[str], List[str], List[str]],
    pre_warnings: Sequence[str],
    ttl: float,
) -> Dict[str, Any]:
    async with _cache_lock(cache_key):
        payload = await _compute_scan_payload(
            exchange=exchange,
            timeframe=timeframe,
            symbols=symbols,
            exclude_retired=exclude_retired,
            refresh=refresh,
            universe_scope=universe_scope,
            mode=mode,
            view=view,
            resolved_universe=resolved_universe,
        )
        stored_payload = _finalize_scan_payload(
            payload=payload,
            cache_key=cache_key,
            ttl=ttl,
            mode=mode,
            view=view,
            timeframe=timeframe,
            pre_warnings=pre_warnings,
            cache_hit=False,
            age_sec=0.0,
            stale=False,
            refreshing=False,
            served_mode="live_compute",
        )
        stored_at = time.time()
        payload_to_store = _clone_payload(stored_payload)
        payload_to_store.pop("cache", None)
        if _should_cache_scan_payload(stored_payload):
            _ALTCOIN_SCAN_CACHE[cache_key] = {"stored_at": stored_at, "payload": payload_to_store}
        else:
            _ALTCOIN_SCAN_CACHE.pop(cache_key, None)
            stored_payload["warnings"] = list(
                dict.fromkeys(
                    list(stored_payload.get("warnings") or [])
                    + ["Altcoin radar result was not cached because market data is stale or unavailable."]
                )
            )
        return stored_payload


def _consume_altcoin_scan_refresh_task(cache_key: str, task: asyncio.Task) -> None:
    try:
        task.exception()
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    finally:
        if _ALTCOIN_SCAN_REFRESH_TASKS.get(cache_key) is task:
            _ALTCOIN_SCAN_REFRESH_TASKS.pop(cache_key, None)


def _ensure_altcoin_scan_refresh_task(
    *,
    cache_key: str,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
    refresh: bool,
    mode: str,
    view: str,
    universe_scope: str,
    resolved_universe: Tuple[List[str], List[str], List[str], List[str]],
    pre_warnings: Sequence[str],
    ttl: float,
) -> asyncio.Task:
    task = _ALTCOIN_SCAN_REFRESH_TASKS.get(cache_key)
    if task is None or task.done():
        task = asyncio.create_task(
            _refresh_altcoin_scan_cache(
                cache_key=cache_key,
                exchange=exchange,
                timeframe=timeframe,
                symbols=symbols,
                exclude_retired=exclude_retired,
                refresh=refresh,
                mode=mode,
                view=view,
                universe_scope=universe_scope,
                resolved_universe=resolved_universe,
                pre_warnings=pre_warnings,
                ttl=ttl,
            )
        )
        task.add_done_callback(lambda finished, key=cache_key: _consume_altcoin_scan_refresh_task(key, finished))
        _ALTCOIN_SCAN_REFRESH_TASKS[cache_key] = task
    return task


def _clear_altcoin_scan_cache() -> None:
    for task in list(_ALTCOIN_SCAN_REFRESH_TASKS.values()):
        if task and not task.done():
            task.cancel()
    _ALTCOIN_SCAN_REFRESH_TASKS.clear()
    _ALTCOIN_SCAN_CACHE.clear()


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


def _serialize_derivatives_snapshot(row: AnalyticsDerivativesSnapshot) -> Dict[str, Any]:
    payload = dict(row.payload or {})
    return {
        "exchange": row.exchange,
        "symbol": row.symbol,
        "timestamp": row.timestamp.replace(tzinfo=timezone.utc).isoformat() if row.timestamp else None,
        "capture_status": row.capture_status,
        "source_error": row.source_error,
        "source_name": row.source_name,
        "latency_ms": row.latency_ms,
        "payload": payload,
        "oi_usd": row.oi_usd,
        "oi_change_1h": row.oi_change_1h,
        "oi_change_4h": row.oi_change_4h,
        "oi_change_24h": row.oi_change_24h,
        "funding_rate": row.funding_rate,
        "long_short_ratio": row.long_short_ratio,
        "basis_pct": row.basis_pct,
        "taker_buy_sell_imbalance": row.taker_buy_sell_imbalance,
        "crowding_score": row.crowding_score,
        "squeeze_score": row.squeeze_score,
        "distribution_score": row.distribution_score,
        "orderbook_imbalance_score": row.orderbook_imbalance_score,
        "depth_thinness_score": row.depth_thinness_score,
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


def _map_derivatives_rows_to_requested_symbols(
    rows: Sequence[Any],
    *,
    requested_symbols: Sequence[str],
) -> Dict[str, Any]:
    requested = _normalize_symbols(requested_symbols)
    alias_map: Dict[str, List[str]] = {}
    for symbol in requested:
        alias_map.setdefault(symbol, []).append(symbol)
        base_symbol = str(normalize_coinglass_symbol(symbol) or "").strip().upper()
        if base_symbol:
            alias_map.setdefault(base_symbol, []).append(symbol)

    matched: Dict[str, Any] = {}
    for row in rows:
        row_symbol = str(getattr(row, "symbol", "") or "").strip().upper()
        if not row_symbol:
            continue
        for requested_symbol in alias_map.get(row_symbol, []):
            if requested_symbol not in matched:
                matched[requested_symbol] = row
    return matched


async def _load_latest_derivatives_snapshot_map(symbols: Sequence[str]) -> Dict[str, Any]:
    requested = _normalize_symbols(symbols)
    if not requested:
        return {}

    lookup_keys = set(requested)
    for symbol in requested:
        base_symbol = str(normalize_coinglass_symbol(symbol) or "").strip().upper()
        if base_symbol:
            lookup_keys.add(base_symbol)

    async with async_session_maker() as session:
        result = await session.execute(
            select(AnalyticsDerivativesSnapshot)
            .where(
                AnalyticsDerivativesSnapshot.exchange == "aggregate",
                AnalyticsDerivativesSnapshot.symbol.in_(sorted(lookup_keys)),
            )
            .order_by(AnalyticsDerivativesSnapshot.timestamp.desc())
        )
        rows = result.scalars().all()

    return _map_derivatives_rows_to_requested_symbols(rows, requested_symbols=requested)


async def _load_snapshot_maps(
    *,
    exchange: str,
    symbols: Sequence[str],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    micro_rows, community_rows, whale_rows, derivatives_rows = await asyncio.gather(
        _load_latest_snapshot_map(AnalyticsMicrostructureSnapshot, exchange=exchange, symbols=symbols),
        _load_latest_snapshot_map(AnalyticsCommunitySnapshot, exchange=exchange, symbols=symbols),
        _load_latest_snapshot_map(AnalyticsWhaleSnapshot, exchange=exchange, symbols=symbols),
        _load_latest_derivatives_snapshot_map(symbols),
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
    derivatives = {
        str(symbol).strip().upper(): _serialize_derivatives_snapshot(row)
        for symbol, row in dict(derivatives_rows or {}).items()
        if symbol and row is not None
    }
    return micro, community, whale, derivatives


def _snapshot_age_seconds(value: Any) -> Optional[float]:
    return _age_seconds_at(value, now=_utcnow())


def _age_seconds_at(value: Any, *, now: datetime) -> Optional[float]:
    if not value:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return max(0.0, (now - ts.to_pydatetime()).total_seconds())


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    if pd.isna(parsed):
        return float(default)
    return float(parsed)


def _market_data_stale_cutoff_seconds(timeframe: str) -> float:
    normalized = _normalize_timeframe(timeframe)
    return float(TIMEFRAME_SECONDS.get(normalized, TIMEFRAME_SECONDS[DEFAULT_TIMEFRAME]) * 4.0)


def _market_frame_age_seconds(frame: Any, *, now: datetime) -> Optional[float]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return None
    try:
        index = frame.sort_index().index
        if len(index) == 0:
            return None
        return _age_seconds_at(index[-1], now=now)
    except Exception:
        return None


def _is_fresh_market_snapshot(snapshot: Optional[Mapping[str, Any]], *, timeframe: str, now: datetime) -> bool:
    if not snapshot:
        return False
    status = str(snapshot.get("capture_status") or "ok").strip().lower()
    if status in {"failed", "error", "unavailable"}:
        return False
    if _safe_float(snapshot.get("current_price") or snapshot.get("last_price"), 0.0) <= 0:
        return False
    age_sec = _age_seconds_at(snapshot.get("timestamp"), now=now)
    if age_sec is None:
        return False
    return age_sec <= _market_data_stale_cutoff_seconds(timeframe)


def _filter_fresh_market_frames(
    *,
    frames: Mapping[str, Any],
    market_snapshots: Mapping[str, Mapping[str, Any]],
    timeframe: str,
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    if not frames:
        return {}, [], []

    now = _utcnow()
    stale_cutoff_sec = _market_data_stale_cutoff_seconds(timeframe)
    snapshots_by_symbol = {
        str(symbol or "").strip().upper(): snapshot
        for symbol, snapshot in dict(market_snapshots or {}).items()
        if str(symbol or "").strip()
    }
    kept: Dict[str, Any] = {}
    dropped_with_snapshot: List[str] = []
    dropped_without_snapshot: List[str] = []
    for symbol, frame in dict(frames or {}).items():
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            continue
        frame_age_sec = _market_frame_age_seconds(frame, now=now)
        if frame_age_sec is not None and frame_age_sec > stale_cutoff_sec:
            if _is_fresh_market_snapshot(snapshots_by_symbol.get(normalized_symbol), timeframe=timeframe, now=now):
                dropped_with_snapshot.append(normalized_symbol)
            else:
                dropped_without_snapshot.append(normalized_symbol)
            continue
        kept[normalized_symbol] = frame
    return kept, _normalize_symbols(dropped_with_snapshot), _normalize_symbols(dropped_without_snapshot)


def _pair_symbol_from_base(base: str) -> str:
    text = str(base or "").strip().upper()
    return f"{text}/USDT" if text else ""


def _binance_public_symbol_key(symbol: str) -> str:
    base = normalize_coinglass_symbol(symbol)
    return f"{base}USDT" if base else ""


def _match_binance_public_ticker(
    ticker_map: Mapping[str, Mapping[str, Any]],
    symbol: str,
) -> Tuple[Optional[Mapping[str, Any]], float, str]:
    direct_key = _binance_public_symbol_key(symbol)
    if not direct_key:
        return None, 1.0, ""
    direct = ticker_map.get(direct_key)
    if direct:
        return direct, 1.0, direct_key

    candidates: List[Tuple[float, str, Mapping[str, Any]]] = []
    for key, ticker in ticker_map.items():
        key_text = str(key or "").strip().upper()
        if not key_text.endswith(direct_key):
            continue
        prefix = key_text[: -len(direct_key)]
        if not prefix.isdigit():
            continue
        multiplier = _safe_float(prefix, 1.0)
        if multiplier <= 1:
            continue
        candidates.append((multiplier, key_text, ticker))
    if not candidates:
        return None, 1.0, ""
    multiplier, matched_key, ticker = sorted(candidates, key=lambda item: (item[0], item[1]))[0]
    return ticker, multiplier, matched_key


def _binance_timestamp_from_ticker(ticker: Mapping[str, Any]) -> str:
    for key in ("closeTime", "time", "openTime"):
        raw = ticker.get(key)
        if raw in (None, ""):
            continue
        try:
            numeric = float(raw)
        except Exception:
            continue
        if numeric > 1_000_000_000_000:
            numeric = numeric / 1000.0
        return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()
    return _utcnow().isoformat()


def _build_binance_public_market_snapshot(
    *,
    requested_symbol: str,
    ticker: Mapping[str, Any],
    divisor: float,
    matched_symbol: str,
    source_name: str,
) -> Dict[str, Any]:
    base = normalize_coinglass_symbol(requested_symbol)
    symbol = _pair_symbol_from_base(base)
    price_divisor = max(1.0, float(divisor or 1.0))

    def _price(*keys: str) -> float:
        for key in keys:
            value = ticker.get(key)
            if value not in (None, ""):
                return _safe_float(value, 0.0) / price_divisor
        return 0.0

    last_price = _price("lastPrice", "last")
    high_price = _price("highPrice", "high")
    low_price = _price("lowPrice", "low")
    bid_price = _price("bidPrice", "bid")
    ask_price = _price("askPrice", "ask")
    quote_volume = _safe_float(
        ticker.get("quoteVolume")
        or ticker.get("quote_volume")
        or ticker.get("turnover")
        or ticker.get("turnover_usd"),
        0.0,
    )
    spread_bps = 0.0
    mid = (bid_price + ask_price) / 2.0 if bid_price > 0 and ask_price > 0 else 0.0
    if mid > 0 and ask_price >= bid_price:
        spread_bps = ((ask_price - bid_price) / mid) * 10_000.0

    return {
        "symbol": symbol,
        "raw_symbol": matched_symbol or str(ticker.get("symbol") or ""),
        "base_symbol": base,
        "exchange": "binance",
        "timestamp": _binance_timestamp_from_ticker(ticker),
        "source_name": source_name,
        "capture_status": "ok",
        "source_error": None,
        "latency_ms": 0,
        "current_price": last_price,
        "high_price_24h": high_price,
        "low_price_24h": low_price,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "spread_bps": spread_bps,
        "quote_volume_24h": quote_volume,
        "base_volume_24h": _safe_float(ticker.get("volume") or ticker.get("baseVolume"), 0.0),
        "price_change_percent_24h": _safe_float(ticker.get("priceChangePercent"), 0.0),
        "price_change_abs_24h": _safe_float(ticker.get("priceChange"), 0.0) / price_divisor,
    }


async def _fetch_binance_public_tickers(url: str) -> List[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=8.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        payload = response.json()
    if isinstance(payload, list):
        return [dict(item or {}) for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        return [dict(payload)]
    return []


async def _load_exchange_public_market_snapshots(
    *,
    exchange: str,
    symbols: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    normalized_exchange = _normalize_exchange(exchange)
    requested = _normalize_symbols(symbols)
    if normalized_exchange != "binance" or not requested:
        return {}

    cache_key = f"{normalized_exchange}|public_ticker_24h"
    now_ts = time.time()
    cached = _PUBLIC_MARKET_SNAPSHOT_CACHE.get(cache_key)
    if cached and (now_ts - float(cached.get("stored_at", 0.0))) <= _PUBLIC_MARKET_SNAPSHOT_TTL_SEC:
        rows = dict(cached.get("rows") or {})
    else:
        spot_rows, futures_rows = await asyncio.gather(
            _fetch_binance_public_tickers("https://api.binance.com/api/v3/ticker/24hr"),
            _fetch_binance_public_tickers("https://fapi.binance.com/fapi/v1/ticker/24hr"),
            return_exceptions=True,
        )
        rows = {}
        if not isinstance(spot_rows, Exception):
            rows["spot"] = {
                str(item.get("symbol") or "").strip().upper(): item
                for item in spot_rows
                if str(item.get("symbol") or "").strip()
            }
        else:
            rows["spot"] = {}
        if not isinstance(futures_rows, Exception):
            rows["futures"] = {
                str(item.get("symbol") or "").strip().upper(): item
                for item in futures_rows
                if str(item.get("symbol") or "").strip()
            }
        else:
            rows["futures"] = {}
        _PUBLIC_MARKET_SNAPSHOT_CACHE[cache_key] = {"stored_at": now_ts, "rows": rows}

    spot_map = dict(rows.get("spot") or {})
    futures_map = dict(rows.get("futures") or {})
    out: Dict[str, Dict[str, Any]] = {}
    for symbol in requested:
        ticker, divisor, matched_key = _match_binance_public_ticker(futures_map, symbol)
        source_name = "binance_futures_ticker_24h"
        if ticker is None:
            ticker, divisor, matched_key = _match_binance_public_ticker(spot_map, symbol)
            source_name = "binance_spot_ticker_24h"
        if ticker is None:
            continue
        snapshot = _build_binance_public_market_snapshot(
            requested_symbol=symbol,
            ticker=ticker,
            divisor=divisor,
            matched_symbol=matched_key,
            source_name=source_name,
        )
        if snapshot.get("symbol"):
            out[str(snapshot["symbol"]).strip().upper()] = snapshot
    return out


def _should_overlay_coinglass_market_snapshot(snapshot: Optional[Mapping[str, Any]]) -> bool:
    if not snapshot:
        return True
    capture_status = str(snapshot.get("capture_status") or "").strip().lower()
    if capture_status and capture_status not in {"ok", "success"}:
        return True
    age_sec = _snapshot_age_seconds(snapshot.get("timestamp"))
    if age_sec is None:
        return True
    return age_sec > 1800.0


def _needs_detail_chain_fallback(row: Optional[Mapping[str, Any]]) -> bool:
    percentiles = (((row or {}).get("metrics") or {}).get("percentiles") or {})
    return any(
        percentiles.get(key) is None
        for key in ("community_flow", "announcements", "funding_basis", "whale_context")
    )


async def _load_detail_live_chain_context(
    *,
    exchange: str,
    symbol: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    try:
        from web.api.trading import get_community_overview  # noqa: PLC0415
    except Exception:
        return {}, {}

    try:
        payload = dict(await get_community_overview(symbol=symbol, exchange=exchange) or {})
    except Exception:
        return {}, {}

    timestamp = payload.get("timestamp") or _utcnow().isoformat()
    community_snapshot = {
        "exchange": exchange,
        "symbol": symbol,
        "timestamp": timestamp,
        "source_error": payload.get("error"),
        "source_name": "live_community_overview",
        "capture_status": "ok" if not payload.get("error") else "degraded",
        "latency_ms": payload.get("latency_ms"),
        "payload": payload,
        "flow_proxy": dict(payload.get("flow_proxy") or {}),
        "announcements": list(payload.get("announcements") or []),
        "security_alerts": payload.get("security_alerts") or {},
        "twitter_watchlist": list(payload.get("twitter_watchlist") or []),
    }
    whale_payload = dict(payload.get("whale_transfers") or {})
    whale_snapshot = {
        "exchange": exchange,
        "symbol": symbol,
        "timestamp": timestamp,
        "available": bool(whale_payload.get("available", True)),
        "source_error": whale_payload.get("error"),
        "source_name": "live_community_overview",
        "capture_status": "ok" if not whale_payload.get("error") else "degraded",
        "latency_ms": payload.get("latency_ms"),
        "payload": whale_payload,
        "count": int(whale_payload.get("count") or 0),
        "threshold_btc": whale_payload.get("threshold_btc"),
        "btc_price": whale_payload.get("btc_price"),
        "transactions": list(whale_payload.get("transactions") or []),
    }
    return community_snapshot, whale_snapshot


async def _resolve_universe(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
    universe_scope: str = "research",
) -> Tuple[List[str], List[str], List[str], List[str]]:
    scope = _normalize_universe_scope(universe_scope)
    cap = MAX_EXPANDED_SIZE if scope == "expanded" else MAX_UNIVERSE_SIZE

    explicit_requested = _normalize_symbols(symbols)
    requested = list(explicit_requested)
    fallback_warning = ""

    # For watchlist scope: always merge watchlist symbols
    if scope == "watchlist" and not requested:
        requested = get_watchlist_symbols()[:cap]
    elif scope == "expanded" and not requested:
        # Load research + watchlist
        research_symbols = await get_research_symbols(exchange=exchange, include_major=False)
        base = _normalize_symbols((research_symbols.get("symbols") or []))
        requested = resolve_universe_scope(
            scope,
            research_symbols=base,
        )[:cap]
    elif not requested:
        research_symbols = await get_research_symbols(exchange=exchange, include_major=False)
        requested = _normalize_symbols((research_symbols.get("symbols") or [])[:MAX_UNIVERSE_SIZE])

    filtered, excluded_retired = _research_retired_filter(
        exchange=exchange,
        timeframe=timeframe,
        requested=requested,
        exclude_retired=exclude_retired,
    )
    filtered = _normalize_symbols(filtered)[:cap]
    if not filtered:
        research_symbols = await get_research_symbols(exchange=exchange, include_major=False)
        requested = _normalize_symbols((research_symbols.get("symbols") or [])[:MAX_UNIVERSE_SIZE])
        filtered, excluded_retired = _research_retired_filter(
            exchange=exchange,
            timeframe=timeframe,
            requested=requested,
            exclude_retired=exclude_retired,
        )
        filtered = _normalize_symbols(filtered)[:MAX_UNIVERSE_SIZE]
        if explicit_requested:
            fallback_warning = "传入的 symbols 过滤后为空或不可用，已回退到 research universe 默认币池。"
        elif scope == "watchlist":
            fallback_warning = "当前 Watchlist 候选不可用，已回退到 research universe 默认币池。"
        elif scope == "expanded":
            fallback_warning = "当前扩展扫描候选不可用，已回退到 research universe 默认币池。"
    warnings: List[str] = []
    if fallback_warning:
        warnings.append(fallback_warning)
    return requested[:cap], filtered, excluded_retired, warnings


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
        and str(rule.get("rule_type") or "") in ALTCOIN_RULE_TYPES
    ]


def _universe_matches(rule_symbols: Sequence[str], current_symbols: Sequence[str]) -> bool:
    left = _normalize_symbols(rule_symbols)
    right = _normalize_symbols(current_symbols)
    if not left or not right:
        return False
    return left == right


def _rule_matches_scan_context(
    params: Mapping[str, Any],
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
    config_key: str = "",
) -> bool:
    if str(params.get("exchange") or "").strip().lower() != exchange:
        return False
    if str(params.get("timeframe") or "").strip().lower() != timeframe:
        return False
    params_config_key = str(params.get("config_key") or "").strip()
    if config_key and params_config_key:
        return params_config_key == config_key
    if _normalize_mode(str(params.get("mode") or "combined")) != _normalize_mode(mode):
        return False
    params_view_raw = str(params.get("view") or "").strip()
    params_view = _normalize_view(params_view_raw) if params_view_raw else ""
    normalized_view = _normalize_view(view) if str(view or "").strip() else ""
    if params_view != normalized_view:
        return False
    if _normalize_universe_scope(str(params.get("universe_scope") or "research")) != _normalize_universe_scope(universe_scope):
        return False
    universe = params.get("universe_symbols") or []
    universe_list = universe if isinstance(universe, list) else _parse_symbols_param(str(universe))
    if universe_list and not _universe_matches(universe_list, symbols):
        return False
    return True


def _normalize_alert_rule(rule: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(rule or {})
    params = dict(payload.get("params") or {})
    rule_type = str(payload.get("rule_type") or "")
    score_key = str(params.get("score_key") or "")
    return {
        "id": str(payload.get("id") or ""),
        "name": str(payload.get("name") or ""),
        "rule_type": rule_type,
        "score_key": score_key,
        "symbol": str(params.get("symbol") or "").strip().upper(),
        "preset": _preset_label_from_rule(rule_type, score_key),
        "kind": _alert_kind_from_rule(rule_type, score_key),
        "exchange": str(params.get("exchange") or "").strip().lower(),
        "timeframe": str(params.get("timeframe") or "").strip().lower(),
        "mode": _normalize_mode(str(params.get("mode") or "combined")),
        "view": _normalize_view(str(params.get("view") or "")) if str(params.get("view") or "").strip() else "",
        "universe_scope": _normalize_universe_scope(str(params.get("universe_scope") or "research")),
        "config_key": str(params.get("config_key") or "").strip(),
        "threshold": float(params.get("threshold") or 0.0),
        "enabled": bool(payload.get("enabled")),
    }


def _alert_rules_for_scan(
    rules: Sequence[Mapping[str, Any]],
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
    config_key: str = "",
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for rule in rules:
        params = dict(rule.get("params") or {})
        if not _rule_matches_scan_context(
            params,
            exchange=exchange,
            timeframe=timeframe,
            symbols=symbols,
            mode=mode,
            view=view,
            universe_scope=universe_scope,
            config_key=config_key,
        ):
            continue
        normalized = _normalize_alert_rule(rule)
        symbol = str(normalized.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        out.setdefault(symbol, []).append(normalized)
    return out


def _apply_alert_rules_to_rows(
    rows: Sequence[Mapping[str, Any]],
    alert_rules_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row or {})
        symbol = str(item.get("symbol") or "").strip().upper()
        alert_rules = [dict(rule or {}) for rule in (alert_rules_by_symbol.get(symbol) or [])]
        item["alert_rules"] = alert_rules
        item["has_alert_rule"] = bool(alert_rules)
        tags = [str(tag or "") for tag in (item.get("tags") or []) if str(tag or "").strip()]
        if alert_rules:
            if "已建预警" not in tags:
                tags.append("已建预警")
        else:
            tags = [tag for tag in tags if tag != "已建预警"]
        item["tags"] = tags
        out.append(item)
    return out


async def _compute_scan_payload(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
    refresh: bool = False,
    universe_scope: str = "research",
    mode: str = "combined",
    view: str = "",
    resolved_universe: Optional[Tuple[List[str], List[str], List[str], List[str]]] = None,
) -> Dict[str, Any]:
    if resolved_universe is None:
        requested_symbols, symbols_used, excluded_retired_symbols, warnings = await _resolve_universe(
            exchange=exchange,
            timeframe=timeframe,
            symbols=symbols,
            exclude_retired=exclude_retired,
            universe_scope=universe_scope,
        )
    else:
        requested_symbols, symbols_used, excluded_retired_symbols, warnings = resolved_universe
        requested_symbols = _normalize_symbols(requested_symbols)
        symbols_used = _normalize_symbols(symbols_used)
        excluded_retired_symbols = _normalize_symbols(excluded_retired_symbols)
        warnings = [str(item) for item in (warnings or []) if str(item or "").strip()]

    frame_result, market_snapshot_result = await asyncio.gather(
        _load_market_frames(exchange=exchange, timeframe=timeframe, symbols=symbols_used),
        load_coinglass_market_snapshots(
            exchange=exchange,
            symbols=symbols_used,
            refresh=refresh,
        ),
        return_exceptions=True,
    )
    if isinstance(frame_result, Exception):
        frames = {}
        frame_warnings = [f"本地 K 线加载失败: {frame_result}"]
    else:
        frames, frame_warnings = frame_result
    warnings.extend(frame_warnings)
    if isinstance(market_snapshot_result, Exception):
        market_snapshots = {}
        warnings.append(f"CoinGlass market snapshot unavailable: {market_snapshot_result}")
    else:
        market_snapshots = market_snapshot_result

    fallback_targets = [symbol for symbol in symbols_used if symbol not in (market_snapshots or {})]
    if fallback_targets:
        try:
            public_snapshots = await _load_exchange_public_market_snapshots(
                exchange=exchange,
                symbols=fallback_targets,
            )
        except Exception as exc:
            public_snapshots = {}
            if isinstance(market_snapshot_result, Exception):
                warnings.append(f"Exchange public ticker fallback unavailable: {exc}")
        if public_snapshots:
            for symbol, snapshot in public_snapshots.items():
                market_snapshots.setdefault(symbol, snapshot)
            warnings.append(
                f"CoinGlass market snapshot fallback: using exchange public ticker for {len(public_snapshots)} symbols."
            )
        elif _normalize_exchange(exchange) == "binance":
            warnings.append(
                f"Exchange public ticker fallback returned no data for {len(fallback_targets)} symbols."
            )

    recovered_symbols = {str(symbol or "").strip().upper() for symbol in (market_snapshots or {}).keys()}
    if recovered_symbols:
        recovered_kline_warnings = [
            str(warning or "")
            for warning in warnings
            if any(
                str(warning or "").strip().upper().startswith(f"{symbol} ") and "K" in str(warning or "")
                for symbol in recovered_symbols
            )
        ]
        if recovered_kline_warnings:
            warnings = [
                warning
                for warning in warnings
                if not any(
                    str(warning or "").strip().upper().startswith(f"{symbol} ") and "K" in str(warning or "")
                    for symbol in recovered_symbols
                )
            ]
            warnings.append(
                f"Local K-line missing for {len(recovered_kline_warnings)} symbols; using live market snapshots instead."
            )

    frames, stale_frame_symbols, stale_without_snapshot_symbols = _filter_fresh_market_frames(
        frames=frames,
        market_snapshots=market_snapshots,
        timeframe=timeframe,
    )
    if stale_frame_symbols:
        warnings.append(
            "Ignored stale local K-line frames for "
            f"{len(stale_frame_symbols)} symbols because fresher live market snapshots are available."
        )
    if stale_without_snapshot_symbols:
        warnings.append(
            "Ignored stale local K-line frames for "
            f"{len(stale_without_snapshot_symbols)} symbols because no fresh market snapshot was available."
        )

    symbols_used = _normalize_symbols(list(frames.keys()) + list(market_snapshots.keys()))
    if not symbols_used:
        return {
            "exchange": exchange,
            "timeframe": timeframe,
            "symbols_requested": requested_symbols,
            "symbols_used": [],
            "excluded_retired": excluded_retired_symbols,
            "excluded_reasons": {"retired_like": excluded_retired_symbols},
            "warnings": warnings + ["没有可用于山寨雷达扫描的本地 K 线数据。"],
            "rows": [],
            "generated_at": _utcnow().isoformat(),
            "universe_meta": universe_meta([], universe_scope),
        }

    factor_symbols = _normalize_symbols(frames.keys())
    if factor_symbols:
        symbol_csv = ",".join(factor_symbols)
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
    else:
        warnings.append(
            "Factor and correlation inputs skipped because no fresh local K-line frames were available."
        )
        factor_task = asyncio.sleep(0, result={"warnings": [], "asset_scores": []})
        multi_task = asyncio.sleep(
            0,
            result={"retired_filter": {"excluded_symbols": []}, "assets": [], "correlation": {}},
        )
    snapshots_task = _load_snapshot_maps(exchange=exchange, symbols=symbols_used)
    rules_task = _load_active_altcoin_rules()
    factor_payload, multi_payload, snapshots, rules = await asyncio.gather(
        factor_task,
        multi_task,
        snapshots_task,
        rules_task,
    )
    micro_map, community_map, whale_map, derivatives_map = snapshots
    for symbol, snapshot in market_snapshots.items():
        if symbol not in symbols_used:
            continue
        if (
            str((snapshot or {}).get("source_name") or "").strip() == "coinglass_coins_markets"
            and _should_overlay_coinglass_market_snapshot(derivatives_map.get(symbol))
        ):
            derivatives_map[symbol] = build_derivatives_snapshot_from_market_snapshot(snapshot)
    config_key = build_altcoin_notification_config_key(
        exchange=exchange,
        timeframe=timeframe,
        universe_symbols=symbols_used,
        exclude_retired=exclude_retired,
        mode=mode,
        view=view,
        universe_scope=universe_scope,
    )
    alert_rules_by_symbol = _alert_rules_for_scan(
        rules,
        exchange=exchange,
        timeframe=timeframe,
        symbols=symbols_used,
        mode=mode,
        view=view,
        universe_scope=universe_scope,
        config_key=config_key,
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
        market_snapshots=market_snapshots,
        micro_snapshots=micro_map,
        community_snapshots=community_map,
        whale_snapshots=whale_map,
        derivatives_snapshots=derivatives_map,
        alerted_symbols=list(alert_rules_by_symbol.keys()),
    )
    rows = _apply_alert_rules_to_rows(rows, alert_rules_by_symbol)
    return {
        "exchange": exchange,
        "timeframe": timeframe,
        "symbols_requested": requested_symbols,
        "symbols_used": symbols_used,
        "excluded_retired": excluded_retired_symbols,
        "excluded_reasons": {"retired_like": excluded_retired_symbols},
        "warnings": list(dict.fromkeys(warnings)),
        "rows": rows,
        "generated_at": _utcnow().isoformat(),
        "universe_meta": universe_meta(symbols_used, universe_scope),
    }


async def get_altcoin_scan_snapshot(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool = True,
    refresh: bool = False,
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
) -> Dict[str, Any]:
    normalized_exchange = _normalize_exchange(exchange)
    normalized_timeframe = _resolve_requested_timeframe(timeframe=timeframe, view=view)
    normalized_mode = _normalize_mode(mode)
    normalized_view = _normalize_view(view) if view else ""
    normalized_scope = _normalize_universe_scope(universe_scope)
    normalized_symbols = _normalize_symbols(symbols)
    requested_symbols, filtered_symbols, excluded_retired_symbols, pre_warnings = await _resolve_universe(
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        symbols=normalized_symbols,
        exclude_retired=exclude_retired,
        universe_scope=normalized_scope,
    )
    cache_key = _cache_key(
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        symbols=filtered_symbols or requested_symbols,
        exclude_retired=exclude_retired,
        mode=normalized_mode,
        view=normalized_view,
        universe_scope=normalized_scope,
    )
    cached_entry = _ALTCOIN_SCAN_CACHE.get(cache_key)
    ttl = _cache_ttl(normalized_timeframe, normalized_view)
    now_ts = time.time()
    if cached_entry and not refresh:
        age_sec = _cache_age_sec(cached_entry, now_ts)
        if age_sec <= ttl:
            return _build_cached_scan_payload(
                cached_entry=cached_entry,
                cache_key=cache_key,
                ttl=ttl,
                now_ts=now_ts,
                stale=False,
                refreshing=False,
                served_mode="cache_hit",
                warnings=pre_warnings,
            )

    resolved_universe = (
        requested_symbols,
        filtered_symbols or requested_symbols,
        excluded_retired_symbols,
        pre_warnings,
    )
    symbols_to_scan = filtered_symbols or requested_symbols

    if cached_entry:
        age_sec = _cache_age_sec(cached_entry, now_ts)
        refresh_task = _ensure_altcoin_scan_refresh_task(
            cache_key=cache_key,
            exchange=normalized_exchange,
            timeframe=normalized_timeframe,
            symbols=symbols_to_scan,
            exclude_retired=exclude_retired,
            refresh=refresh,
            mode=normalized_mode,
            view=normalized_view,
            universe_scope=normalized_scope,
            resolved_universe=resolved_universe,
            pre_warnings=pre_warnings,
            ttl=ttl,
        )
        if refresh:
            return await asyncio.shield(refresh_task)
        background_warning = (
            "Altcoin radar refresh started in background; serving previous snapshot."
            if refresh
            else "Altcoin radar cache expired; background refresh in progress, serving previous snapshot."
        )
        return _build_cached_scan_payload(
            cached_entry=cached_entry,
            cache_key=cache_key,
            ttl=ttl,
            now_ts=now_ts,
            stale=age_sec > ttl,
            refreshing=not refresh_task.done(),
            served_mode="cache_refresh" if refresh else "stale_cache_refresh",
            warnings=[*pre_warnings, background_warning],
        )

    refresh_task = _ensure_altcoin_scan_refresh_task(
        cache_key=cache_key,
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        symbols=symbols_to_scan,
        exclude_retired=exclude_retired,
        refresh=refresh,
        mode=normalized_mode,
        view=normalized_view,
        universe_scope=normalized_scope,
        resolved_universe=resolved_universe,
        pre_warnings=pre_warnings,
        ttl=ttl,
    )
    return await asyncio.shield(refresh_task)


_PERP_ONLY_SOURCES = frozenset({"perp_ignition", "perp_continuation"})
_NARRATIVE_SOURCES = frozenset({"narrative_ignition", "narrative_confirmation"})


def _filter_rows_by_mode(rows: List[Dict[str, Any]], mode: str) -> List[Dict[str, Any]]:
    """Filter rows based on radar mode.

    perp      : rows with perp signal_source, crowded_late_stage, or derivatives available
    narrative : rows with narrative signal_source, watchlist members, or no strong perp signal
    combined  : no filter
    """
    m = _normalize_mode(mode)
    if m == "combined":
        return rows
    if m == "perp":
        return [
            r for r in rows
            if (r.get("signal_source") or "") in _PERP_ONLY_SOURCES | {"crowded_late_stage"}
            or r.get("derivatives_context", {}).get("available")
        ]
    # narrative: include narrative sources, watchlist members, or rows without strong perp signal
    return [
        r for r in rows
        if (r.get("signal_source") or "") in _NARRATIVE_SOURCES | {"crowded_late_stage", ""}
        or bool(r.get("in_watchlist"))
    ]


def _build_scan_response(
    *,
    scan_payload: Mapping[str, Any],
    sort_by: str,
    limit: int,
    mode: str = "combined",
) -> Dict[str, Any]:
    normalized_sort = _normalize_sort(sort_by)
    all_rows = list(scan_payload.get("rows") or [])
    filtered_by_mode = _filter_rows_by_mode(all_rows, mode)
    sorted_rows = sort_rows(filtered_by_mode, sort_by=normalized_sort)
    rows: List[Dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for raw_row in sorted_rows:
        row = dict(raw_row or {})
        symbol = normalize_altcoin_pair(row.get("symbol"))
        if not symbol or symbol in seen_symbols:
            continue
        row["symbol"] = symbol
        row["tags"] = list(
            dict.fromkeys(
                str(tag).strip()
                for tag in (row.get("tags") or [])
                if str(tag).strip()
            )
        )
        seen_symbols.add(symbol)
        rows.append(row)
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    cap = MAX_EXPANDED_SIZE if len(all_rows) > MAX_UNIVERSE_SIZE else MAX_UNIVERSE_SIZE
    limited_rows = rows[: max(1, min(int(limit or DEFAULT_LIMIT), cap))]
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
    response["mode"] = _normalize_mode(mode)
    response["view"] = scan_payload.get("view") or str(scan_payload.get("timeframe") or DEFAULT_TIMEFRAME)
    response["universe_meta"] = scan_payload.get("universe_meta") or {}
    response["scan_meta"] = {
        **response.get("scan_meta", {}),
        "generated_at": scan_payload.get("generated_at"),
        "cache": scan_payload.get("cache") or {},
        "row_count_before_limit": len(rows),
        "limit": max(1, min(int(limit or DEFAULT_LIMIT), cap)),
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
        "ignition": "ignition_score",
        "continuation": "continuation_score",
        "rank_jump": "rank_jump_score",
        "crowding": "crowding_late_score",
        "narrative": "narrative_heat_score",
        "meme_rotation": "meme_rotation_score",
    }
    return mapping.get(normalized, "layout_score")


def _notification_eligible_altcoin_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    eligible: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row or {})
        symbol = str(item.get("symbol") or "").strip().upper()
        alt_eligible = item.get("alt_eligible")
        if alt_eligible is None:
            alt_eligible = is_alt_candidate_symbol(symbol)
        if not bool(alt_eligible):
            continue
        eligible.append(item)
    return eligible


async def build_altcoin_notification_context(
    rules: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    active_rules = [
        dict(rule or {})
        for rule in rules
        if bool(rule.get("enabled"))
        and str(rule.get("rule_type") or "") in ALTCOIN_RULE_TYPES
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
        mode = _normalize_mode(str(params.get("mode") or "combined"))
        view = _normalize_view(str(params.get("view") or "")) if str(params.get("view") or "").strip() else ""
        universe_scope = _normalize_universe_scope(str(params.get("universe_scope") or "research"))
        config_key = str(params.get("config_key") or "").strip() or build_altcoin_notification_config_key(
            exchange=exchange,
            timeframe=timeframe,
            universe_symbols=universe_symbols,
            exclude_retired=exclude_retired,
            mode=mode,
            view=view,
            universe_scope=universe_scope,
        )
        unique_configs[config_key] = {
            "exchange": exchange,
            "timeframe": timeframe,
            "universe_symbols": universe_symbols,
            "exclude_retired": exclude_retired,
            "mode": mode,
            "view": view,
            "universe_scope": universe_scope,
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
            mode=str(config.get("mode") or "combined"),
            view=str(config.get("view") or ""),
            universe_scope=str(config.get("universe_scope") or "research"),
        )
        for config_key, config in unique_configs.items()
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    scans: Dict[str, Any] = {}
    for config_key, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            scans[config_key] = {"error": str(result), "rows": [], "sort_indexes": {}}
            continue
        eligible_rows = _notification_eligible_altcoin_rows(result.get("rows") or [])
        layout_rows = sort_rows(eligible_rows, sort_by="layout")
        alert_rows = sort_rows(eligible_rows, sort_by="alert")
        control_rows = sort_rows(eligible_rows, sort_by="control")
        scans[config_key] = {
            "config": unique_configs[config_key],
            "generated_at": result.get("generated_at"),
            "warnings": result.get("warnings") or [],
            "rows": eligible_rows,
            "sort_indexes": {
                "layout": {str(row.get("symbol") or ""): int(row.get("rank") or 0) for row in layout_rows},
                "alert": {str(row.get("symbol") or ""): int(row.get("rank") or 0) for row in alert_rows},
                "control": {str(row.get("symbol") or ""): int(row.get("rank") or 0) for row in control_rows},
                "ignition": {
                    str(row.get("symbol") or ""): int(row.get("rank") or 0)
                    for row in sort_rows(eligible_rows, sort_by="ignition")
                },
                "rank_jump": {
                    str(row.get("symbol") or ""): int(row.get("rank") or 0)
                    for row in sort_rows(eligible_rows, sort_by="rank_jump")
                },
                "crowding": {
                    str(row.get("symbol") or ""): int(row.get("rank") or 0)
                    for row in sort_rows(eligible_rows, sort_by="crowding")
                },
                "continuation": {
                    str(row.get("symbol") or ""): int(row.get("rank") or 0)
                    for row in sort_rows(eligible_rows, sort_by="continuation")
                },
                "narrative": {
                    str(row.get("symbol") or ""): int(row.get("rank") or 0)
                    for row in sort_rows(eligible_rows, sort_by="narrative")
                },
                "meme_rotation": {
                    str(row.get("symbol") or ""): int(row.get("rank") or 0)
                    for row in sort_rows(eligible_rows, sort_by="meme_rotation")
                },
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
    # Phase 1 new params
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
):
    normalized_symbols = _parse_symbols_param(symbols)
    requested_view = _normalize_view(view) if str(view or "").strip() else ""
    scan_payload = await get_altcoin_scan_snapshot(
        exchange=exchange,
        timeframe=_resolve_requested_timeframe(timeframe=timeframe, view=requested_view),
        symbols=normalized_symbols,
        exclude_retired=exclude_retired,
        refresh=refresh,
        mode=mode,
        view=requested_view,
        universe_scope=universe_scope,
    )
    return _build_scan_response(scan_payload=scan_payload, sort_by=sort_by, limit=limit, mode=mode)


async def warm_default_scan_cache() -> Dict[str, Any]:
    """Pre-compute the default-parameter radar scan once (startup warmer).

    The first scan after a restart pays the full factor-library cold compute
    (observed ~63s), which exceeds the frontend's 60s budget — so the radar
    page's first load after every service restart failed with a timeout.
    Warming the default combo in the background right after startup means the
    page always lands on a cache hit (~40ms).
    """
    return await get_altcoin_scan_snapshot(
        exchange=DEFAULT_EXCHANGE,
        timeframe=DEFAULT_TIMEFRAME,
        symbols=[],
        exclude_retired=True,
        refresh=False,
        mode="combined",
        view="",
        universe_scope="research",
    )


@router.get("/radar/events")
async def get_altcoin_radar_events(
    symbol: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = 50,
    max_age_sec: float = 3600.0,
):
    """Return recent altcoin radar events from in-memory cache.

    Events include: ignition_cross_up, rank_jump_top_n, crowding_risk_spike.
    """
    if symbol:
        events = get_recent_symbol_events(
            symbol=str(symbol).strip().upper(),
            limit=max(1, min(int(limit), 200)),
            max_age_sec=max(60.0, min(float(max_age_sec), 86400.0)),
        )
    else:
        events = get_all_recent_events(
            limit=max(1, min(int(limit), 200)),
            max_age_sec=max(60.0, min(float(max_age_sec), 86400.0)),
            event_type=str(event_type).strip() if event_type else None,
        )
    return {
        "events": events,
        "count": len(events),
        "ts": _utcnow().isoformat(),
    }


@router.get("/radar/detail")
async def get_altcoin_radar_detail(
    exchange: str = DEFAULT_EXCHANGE,
    timeframe: str = DEFAULT_TIMEFRAME,
    symbol: str = "",
    symbols: Optional[str] = None,
    watchlist_focus: bool = False,
    refresh: bool = False,
    exclude_retired: bool = True,
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
):
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    normalized_exchange = _normalize_exchange(exchange)
    normalized_symbols = _parse_symbols_param(symbols)
    requested_view = _normalize_view(view) if str(view or "").strip() else ""
    scan_payload = await get_altcoin_scan_snapshot(
        exchange=exchange,
        timeframe=_resolve_requested_timeframe(timeframe=timeframe, view=requested_view),
        symbols=normalized_symbols,
        exclude_retired=exclude_retired,
        refresh=refresh,
        mode=mode,
        view=requested_view,
        universe_scope=universe_scope,
    )
    selected_row = next(
        (
            dict(row or {})
            for row in (scan_payload.get("rows") or [])
            if str((row or {}).get("symbol") or "").strip().upper() == normalized_symbol
        ),
        None,
    )
    # A symbol that is neither in the scan result nor explicitly requested via
    # ``symbols`` is simply unknown — fail fast instead of spending ~25s running
    # the full onchain/community pipeline only to return an empty detail.
    if selected_row is None and normalized_symbol not in set(
        _normalize_symbols(list(scan_payload.get("symbols_used") or []) + normalized_symbols)
    ):
        raise HTTPException(
            status_code=404,
            detail=f"symbol {normalized_symbol} is not in the current radar universe",
        )
    fast_watchlist_focus = bool(watchlist_focus) and normalized_symbols == [normalized_symbol]
    detail_warnings: List[str] = []

    async def _bounded_detail_source(coro: Any, *, timeout: float, label: str, fallback: Any) -> Any:
        # External chain/community sources have hung well past 60s in
        # production, timing out the whole detail endpoint (the drawer never
        # opened). Bound each source and degrade with a warning instead.
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            detail_warnings.append(f"{label} timed out after {timeout:.0f}s; showing partial detail")
            return fallback
        except Exception as exc:
            detail_warnings.append(f"{label} unavailable: {type(exc).__name__}")
            return fallback

    onchain_context: Dict[str, Any] = {}
    live_community_snapshot: Dict[str, Any] = {}
    live_whale_snapshot: Dict[str, Any] = {}
    if not fast_watchlist_focus:
        need_chain_fallback = _needs_detail_chain_fallback(selected_row)
        onchain_task = _bounded_detail_source(
            get_onchain_overview(
                symbol=normalized_symbol,
                exchange=normalized_exchange,
                whale_threshold_btc=10.0,
                chain="auto",
                refresh=refresh,
                hours=4,
            ),
            timeout=DETAIL_ONCHAIN_TIMEOUT_SEC,
            label="onchain overview",
            fallback={},
        )
        live_chain_task = _bounded_detail_source(
            _load_detail_live_chain_context(
                exchange=normalized_exchange,
                symbol=normalized_symbol,
            ),
            timeout=DETAIL_LIVE_CHAIN_TIMEOUT_SEC,
            label="live chain context",
            fallback=({}, {}),
        ) if need_chain_fallback else asyncio.sleep(0, result=({}, {}))
        onchain_context, live_chain_context = await asyncio.gather(onchain_task, live_chain_task)
        live_community_snapshot, live_whale_snapshot = live_chain_context
        if not live_whale_snapshot:
            onchain_whales = dict((onchain_context or {}).get("whale_activity") or {})
            if onchain_whales:
                live_whale_snapshot = {
                    "exchange": normalized_exchange,
                    "symbol": normalized_symbol,
                    "timestamp": (onchain_context or {}).get("generated_at"),
                    "available": bool(onchain_whales.get("available", True)),
                    "source_error": onchain_whales.get("error"),
                    "source_name": "onchain_overview",
                    "capture_status": "ok" if not onchain_whales.get("error") else "degraded",
                    "latency_ms": (onchain_context or {}).get("latency_ms"),
                    "payload": onchain_whales,
                    "count": int(onchain_whales.get("count") or 0),
                    "threshold_btc": onchain_whales.get("threshold_btc"),
                    "btc_price": onchain_whales.get("btc_price"),
                    "transactions": list(onchain_whales.get("transactions") or []),
                }
    detail = build_detail_payload(
        rows=scan_payload.get("rows") or [],
        symbol=normalized_symbol,
        sort_by=_normalize_sort("layout"),
        onchain_context=onchain_context,
        detail_community_snapshot=live_community_snapshot,
        detail_whale_snapshot=live_whale_snapshot,
    )
    detail["scan_meta"] = {
        "exchange": scan_payload.get("exchange"),
        "timeframe": scan_payload.get("timeframe"),
        "view": scan_payload.get("view") or requested_view or scan_payload.get("timeframe"),
        "mode": scan_payload.get("mode") or _normalize_mode(mode),
        "universe_meta": scan_payload.get("universe_meta") or {},
        "symbols_used": scan_payload.get("symbols_used") or [],
        "generated_at": scan_payload.get("generated_at"),
        "cache": scan_payload.get("cache") or {},
        "watchlist_focus": fast_watchlist_focus,
    }
    if detail_warnings:
        detail["warnings"] = list(detail.get("warnings") or []) + detail_warnings
    return detail


@router.post("/radar/{symbol}/research-proposal", dependencies=[Depends(require_sensitive_ops_permissions("manage_ai_research"))])
async def create_research_proposal_from_radar(
    request: Request,
    symbol: str,
    exchange: str = DEFAULT_EXCHANGE,
    timeframe: str = DEFAULT_TIMEFRAME,
    refresh: bool = False,
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
):
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    detail = await get_altcoin_radar_detail(
        exchange=exchange,
        timeframe=timeframe,
        symbol=normalized_symbol,
        refresh=refresh,
        mode=mode,
        view=view,
        universe_scope=universe_scope,
    )
    selected = dict(detail.get("selected_row") or {})
    action_plan = dict(detail.get("action_plan") or {})
    scan_meta = dict(detail.get("scan_meta") or {})
    thesis = (
        f"Altcoin radar opportunity for {normalized_symbol}: "
        f"priority={selected.get('priority_score', 0)} state={selected.get('signal_state') or 'watch'}"
    )
    from core.research.orchestrator import create_manual_proposal

    proposal = create_manual_proposal(
        request.app,
        actor="altcoin_radar",
        thesis=thesis,
        symbols=[normalized_symbol],
        timeframes=[str(scan_meta.get("timeframe") or timeframe or DEFAULT_TIMEFRAME)],
        market_regime=str((selected.get("market_regime") or selected.get("signal_state") or "mixed")),
        strategy_templates=[],
        source="hybrid",
        expected_holding_period="1d",
        risk_hypothesis=str(action_plan.get("risk_hypothesis") or "Radar-derived opportunity requires validation before capital deployment."),
        invalidation_rules=[
            str(item)
            for item in list(action_plan.get("invalidate_conditions") or action_plan.get("invalidation_rules") or [])
            if str(item or "").strip()
        ],
        required_features=["ohlcv", "microstructure", "derivatives_shadow"],
        parameter_space={},
        notes=[
            "created from altcoin radar",
            f"next_best_action={selected.get('next_best_action') or 'watch'}",
        ],
        metadata={
            "origin_source": "altcoin_radar",
            "radar_score": selected.get("priority_score"),
            "radar_lens": "priority",
            "action_plan": action_plan,
            "selected_row": selected,
            "market_state_snapshot_id": selected.get("market_state_snapshot_id"),
            "scan_meta": scan_meta,
        },
    )
    return {
        "proposal_id": proposal.proposal_id,
        "proposal": proposal.model_dump(mode="json"),
        "next_action": "open_ai_research_proposal",
        "source_detail": detail,
    }


@router.post("/alerts/preset", dependencies=[Depends(require_sensitive_ops_permissions("manage_notifications"))])
async def create_altcoin_alert_preset(request: AltcoinAlertPresetRequest):
    preset = str(request.preset or "").strip()
    normalized_exchange = _normalize_exchange(request.exchange)
    normalized_timeframe = _normalize_timeframe(request.timeframe)
    normalized_mode = _normalize_mode(request.mode)
    normalized_view = _normalize_view(request.view) if str(request.view or "").strip() else ""
    normalized_scope = _normalize_universe_scope(request.universe_scope)
    symbol = str(request.symbol or "").strip().upper()
    rule_type, score_key, threshold, kind = _preset_definition(preset)
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    universe_symbols = _normalize_symbols(request.universe_symbols) or [symbol]
    target_scan = await get_altcoin_scan_snapshot(
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        symbols=universe_symbols,
        exclude_retired=True,
        refresh=False,
        mode=normalized_mode,
        view=normalized_view,
        universe_scope=normalized_scope,
    )
    target_row = next(
        (
            dict(row or {})
            for row in (target_scan.get("rows") or [])
            if str((row or {}).get("symbol") or "").strip().upper() == symbol
        ),
        None,
    )
    if target_row is not None and not bool(target_row.get("alt_eligible", True)):
        raise HTTPException(status_code=400, detail="benchmark symbols are not supported for altcoin radar alerts")
    if target_row is None and not is_alt_candidate_symbol(symbol):
        raise HTTPException(status_code=400, detail="benchmark symbols are not supported for altcoin radar alerts")
    config_key = build_altcoin_notification_config_key(
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        universe_symbols=universe_symbols,
        exclude_retired=True,
        mode=normalized_mode,
        view=normalized_view,
        universe_scope=normalized_scope,
    )
    rule_name = f"山寨雷达 | {preset} | {symbol} | {normalized_exchange} {normalized_timeframe}"
    params = {
        "exchange": normalized_exchange,
        "timeframe": normalized_timeframe,
        "universe_symbols": universe_symbols,
        "symbol": symbol,
        "score_key": score_key,
        "threshold": threshold,
        "rank_n": 15,
        "channels": list(request.channels or ["feishu"]),
        "source_page": "altcoin_radar",
        "exclude_retired": True,
        "config_key": config_key,
        "mode": normalized_mode,
        "view": normalized_view,
        "universe_scope": normalized_scope,
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
            and str(dict(rule.get("params") or {}).get("config_key") or "") == config_key
            and float(dict(rule.get("params") or {}).get("threshold") or 0.0) == threshold
        ),
        None,
    )
    if existing:
        return {
            "success": True,
            "existing": True,
            "rule": existing,
            "rule_meta": {"preset": preset, "kind": kind, "config_key": config_key},
        }

    rule = await notification_manager.add_rule(
        name=rule_name,
        rule_type=rule_type,
        params=params,
        enabled=True,
        cooldown_seconds=300,
    )
    return {
        "success": True,
        "existing": False,
        "rule": rule,
        "rule_meta": {"preset": preset, "kind": kind, "config_key": config_key},
    }


@router.delete("/alerts/preset", dependencies=[Depends(require_sensitive_ops_permissions("manage_notifications"))])
async def delete_altcoin_alert_preset(
    exchange: str = DEFAULT_EXCHANGE,
    timeframe: str = DEFAULT_TIMEFRAME,
    symbol: str = "",
    symbols: Optional[str] = None,
    preset: Optional[str] = None,
    mode: str = "combined",
    view: str = "",
    universe_scope: str = "research",
):
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    normalized_exchange = _normalize_exchange(exchange)
    normalized_timeframe = _normalize_timeframe(timeframe)
    normalized_mode = _normalize_mode(mode)
    normalized_view = _normalize_view(view) if str(view or "").strip() else ""
    normalized_scope = _normalize_universe_scope(universe_scope)
    universe_symbols = _normalize_symbols(_parse_symbols_param(symbols)) or [normalized_symbol]
    config_key = build_altcoin_notification_config_key(
        exchange=normalized_exchange,
        timeframe=normalized_timeframe,
        universe_symbols=universe_symbols,
        exclude_retired=True,
        mode=normalized_mode,
        view=normalized_view,
        universe_scope=normalized_scope,
    )
    target_rule_type = None
    target_score_key = None
    if str(preset or "").strip():
        target_rule_type, target_score_key, _, _ = _preset_definition(str(preset or "").strip())
    deleted_rules: List[Dict[str, Any]] = []
    for rule in await notification_manager.list_rules():
        params = dict(rule.get("params") or {})
        if not _rule_matches_scan_context(
            params,
            exchange=normalized_exchange,
            timeframe=normalized_timeframe,
            symbols=universe_symbols,
            mode=normalized_mode,
            view=normalized_view,
            universe_scope=normalized_scope,
            config_key=config_key,
        ):
            continue
        if str(params.get("symbol") or "").strip().upper() != normalized_symbol:
            continue
        if target_rule_type and str(rule.get("rule_type") or "") != target_rule_type:
            continue
        if target_score_key and str(params.get("score_key") or "").strip().lower() != str(target_score_key).strip().lower():
            continue
        rule_id = str(rule.get("id") or "").strip()
        if rule_id and await notification_manager.delete_rule(rule_id):
            deleted_rules.append(_normalize_alert_rule(rule))
    return {
        "success": True,
        "deleted_count": len(deleted_rules),
        "deleted_rules": deleted_rules,
        "symbol": normalized_symbol,
        "preset": str(preset or "").strip() or None,
        "config_key": config_key,
    }


@router.get("/radar/watchlist")
async def get_altcoin_radar_watchlist():
    symbols = get_watchlist_symbols()
    # Flag entries the coverage data marks retired-like (delisted/inactive) —
    # the watchlist accumulated 2024-era meme coins that permanently show as
    # degraded rows in watchlist-scope scans. Surfacing the flag lets the UI
    # (and the user) prune them; we never silently delete user entries.
    retired_symbols: List[str] = []
    try:
        _, retired_symbols = _research_retired_filter(
            DEFAULT_EXCHANGE,
            DEFAULT_TIMEFRAME,
            list(symbols),
            True,
        )
    except Exception:
        retired_symbols = []
    # Coverage data only marks research-universe symbols, so also check the
    # exchange's live tickers (TTL-cached; multiplier-aware, e.g. 1000PEPE):
    # a watchlist entry with no spot AND no futures ticker is delisted.
    unlisted_symbols: List[str] = []
    try:
        live_map = await asyncio.wait_for(
            _load_exchange_public_market_snapshots(
                exchange=DEFAULT_EXCHANGE,
                symbols=list(symbols),
            ),
            timeout=8.0,
        )
        normalized_watch = _normalize_symbols(symbols)
        unlisted_symbols = [
            symbol
            for symbol in normalized_watch
            if symbol not in live_map
            or _safe_float(
                (live_map.get(symbol) or {}).get("current_price")
                or (live_map.get(symbol) or {}).get("last_price"),
                0.0,
            )
            <= 0
        ]
    except Exception:
        unlisted_symbols = []
    return {
        "symbols": symbols,
        "count": len(symbols),
        "retired_symbols": retired_symbols,
        "retired_count": len(retired_symbols),
        "unlisted_symbols": unlisted_symbols,
        "unlisted_count": len(unlisted_symbols),
        "meta": universe_meta(symbols, "watchlist"),
        "ts": _utcnow().isoformat(),
    }


@router.post("/radar/watchlist", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def add_altcoin_radar_watchlist_symbol(request: AltcoinWatchlistMutationRequest):
    symbol = str(request.symbol or "").strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    symbols = add_watchlist_symbol(symbol)
    _clear_altcoin_scan_cache()
    return {
        "success": True,
        "symbol": symbol,
        "symbols": symbols,
        "count": len(symbols),
        "meta": universe_meta(symbols, "watchlist"),
    }


@router.delete("/radar/watchlist", dependencies=[Depends(require_sensitive_ops_permissions("manage_data_sources"))])
async def remove_altcoin_radar_watchlist_symbol(symbol: str):
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    symbols = remove_watchlist_symbol(normalized_symbol)
    _clear_altcoin_scan_cache()
    return {
        "success": True,
        "symbol": normalized_symbol,
        "symbols": symbols,
        "count": len(symbols),
        "meta": universe_meta(symbols, "watchlist"),
    }
