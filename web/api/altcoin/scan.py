"""Altcoin radar scan pipeline (the cache-backed discovery engine).

Universe resolution, market-frame + snapshot loading, alert-rule matching, the
cross-sectional scoring compute, the single-flight cached snapshot, the
notification context, and the /radar/scan + /radar/events routes.

This is the hot path the test suite drives most heavily. Its helper dependencies
are module-level names here (imported or defined), so tests monkeypatch them at
web.api.altcoin.scan.<name>; the package __init__ re-exports the public entry
points (get_altcoin_scan_snapshot, warm_default_scan_cache,
build_altcoin_notification_context) so external callers keep working.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import httpx
import pandas as pd
from fastapi import APIRouter
from sqlalchemy import select

from config.database import (
    AnalyticsCommunitySnapshot,
    AnalyticsDerivativesSnapshot,
    AnalyticsMicrostructureSnapshot,
    AnalyticsWhaleSnapshot,
    async_session_maker,
)
from core.data.binance_alpha import (
    alpha_catalog_meta,
    alpha_symbols,
    build_alpha_market_snapshots,
    load_alpha_token_catalog,
)
from core.data.coinglass_altcoin import (
    build_derivatives_snapshot_from_market_snapshot,
    is_alt_candidate_symbol,
    load_coinglass_market_snapshots,
)
from core.data.coinglass_registry import normalize_coinglass_symbol
from core.notifications import notification_manager
from core.research.altcoin_radar import (
    build_altcoin_rows,
    sort_rows,
    summarize_rows,
)
from core.research.altcoin_radar_events import get_all_recent_events, get_recent_symbol_events
from core.research.altcoin_radar_universe import (
    get_watchlist_symbols,
    normalize_altcoin_pair,
    resolve_universe_scope,
    universe_meta,
)
from web.api.data import (
    _load_symbol_df,
    _research_retired_filter,
    get_factor_library,
    get_multi_assets_overview,
    get_research_symbols,
)

from .cache import (
    _build_cached_scan_payload,
    _cache_age_sec,
    _cache_key,
    _cache_lock,
    _evict_altcoin_scan_cache,
    _finalize_scan_payload,
    _should_cache_scan_payload,
    build_altcoin_notification_config_key,
)
from .constants import (
    ALTCOIN_RULE_TYPES,
    DEFAULT_EXCHANGE,
    DEFAULT_LIMIT,
    DEFAULT_SORT,
    DEFAULT_TIMEFRAME,
    MAX_EXPANDED_SIZE,
    MAX_UNIVERSE_SIZE,
    _PUBLIC_MARKET_SNAPSHOT_TTL_SEC,
)
from .helpers import (
    _alert_kind_from_rule,
    _build_binance_public_market_snapshot,
    _cache_ttl,
    _clone_payload,
    _filter_fresh_market_frames,
    _is_alpha_symbol,
    _match_binance_public_ticker,
    _normalize_exchange,
    _normalize_mode,
    _normalize_sort,
    _normalize_symbols,
    _normalize_timeframe,
    _normalize_universe_scope,
    _normalize_view,
    _parse_symbols_param,
    _preset_label_from_rule,
    _resolve_requested_timeframe,
    _serialize_community_snapshot,
    _serialize_derivatives_snapshot,
    _serialize_micro_snapshot,
    _serialize_whale_snapshot,
    _should_overlay_coinglass_market_snapshot,
    _utcnow,
)
from .state import (
    _ALTCOIN_SCAN_CACHE,
    _ALTCOIN_SCAN_REFRESH_TASKS,
    _PUBLIC_MARKET_SNAPSHOT_CACHE,
)

router = APIRouter()


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
            _evict_altcoin_scan_cache()
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






async def _load_latest_snapshot_map(
    model: Any,
    *,
    exchange: str,
    symbols: Sequence[str],
) -> List[Any]:
    normalized = _normalize_symbols(symbols)
    if not normalized:
        return []
    # Bound the scan: without a cutoff this pulls EVERY historical snapshot row
    # for the universe on each radar scan (tables grow forever on a live box).
    # Older-than-7d snapshots are far past every freshness horizon anyway.
    cutoff = _utcnow().replace(tzinfo=None) - pd.Timedelta(days=7)
    async with async_session_maker() as session:
        result = await session.execute(
            select(model)
            .where(
                model.exchange == exchange,
                model.symbol.in_(normalized),
                model.timestamp >= cutoff,
            )
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

    cutoff = _utcnow().replace(tzinfo=None) - pd.Timedelta(days=7)
    async with async_session_maker() as session:
        result = await session.execute(
            select(AnalyticsDerivativesSnapshot)
            .where(
                AnalyticsDerivativesSnapshot.exchange == "aggregate",
                AnalyticsDerivativesSnapshot.symbol.in_(sorted(lookup_keys)),
                AnalyticsDerivativesSnapshot.timestamp >= cutoff,
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
    # Alpha pairs are synthetic collector identifiers and are never valid
    # inputs for the ordinary Binance spot/futures ticker endpoints. Keep this
    # guard here as well as at individual call sites so a future fallback path
    # cannot accidentally cross the source boundary.
    requested = [symbol for symbol in _normalize_symbols(symbols) if not _is_alpha_symbol(symbol)]
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


async def _resolve_universe(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str],
    exclude_retired: bool,
    universe_scope: str = "research",
    refresh: bool = False,
) -> Tuple[List[str], List[str], List[str], List[str]]:
    scope = _normalize_universe_scope(universe_scope)
    # Discovery scopes are intentionally larger than the hand-curated research
    # scope.  Alpha can contain hundreds of low-cap tokens, so keep a bounded
    # top slice rather than turning every dashboard refresh into an unbounded
    # network/data scan.
    cap = MAX_EXPANDED_SIZE if scope in {"expanded", "alpha", "watchlist"} else MAX_UNIVERSE_SIZE

    explicit_requested = _normalize_symbols(symbols)
    requested = list(explicit_requested)
    fallback_warning = ""
    truncation_warning = ""
    alpha_pool: List[str] = []
    alpha_warning = ""
    if scope in {"expanded", "alpha"} and not explicit_requested:
        try:
            alpha_payload = await load_alpha_token_catalog(refresh=refresh)
            alpha_pool = _normalize_symbols(alpha_symbols(alpha_payload.get("tokens") or []))
            alpha_warning = str(alpha_payload.get("warning") or "").strip()
            if len(alpha_pool) > cap:
                truncation_warning = (
                    f"Binance Alpha 当前 {len(alpha_pool)} 个活跃候选，单次雷达扫描上限为 {cap}，只扫描前 {cap} 个。"
                )
        except Exception as exc:
            alpha_warning = f"Binance Alpha universe unavailable: {type(exc).__name__}"

    # Watchlist scope without explicit symbols scans the stored watchlist.
    if scope == "watchlist" and not requested:
        watchlist_symbols = get_watchlist_symbols()
        requested = watchlist_symbols[:cap]
        if len(watchlist_symbols) > cap:
            truncation_warning = (
                f"Watchlist 共 {len(watchlist_symbols)} 个，超出扫描上限 {cap}，只扫描前 {cap} 个。"
            )
    elif scope == "alpha" and not requested:
        requested = alpha_pool[:cap]
        if not requested:
            fallback_warning = "Binance Alpha 当前没有可用候选，已回退到 research universe 默认币池。"
    elif scope == "expanded" and not requested:
        # Load research + Alpha + watchlist.
        research_symbols = await get_research_symbols(exchange=exchange, include_major=False)
        base = _normalize_symbols((research_symbols.get("symbols") or []))
        requested = resolve_universe_scope(
            scope,
            research_symbols=base,
            alpha_symbols=alpha_pool,
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
        elif scope == "alpha":
            fallback_warning = "当前 Alpha 候选不可用，已回退到 research universe 默认币池。"
    warnings: List[str] = []
    if truncation_warning:
        warnings.append(truncation_warning)
    if fallback_warning:
        warnings.append(fallback_warning)
    if alpha_warning:
        warnings.append(alpha_warning)
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

    alpha_needed = _normalize_universe_scope(universe_scope) in {"expanded", "alpha"} or any(
        _is_alpha_symbol(symbol) for symbol in symbols_used
    )
    alpha_task = load_alpha_token_catalog(refresh=refresh) if alpha_needed else asyncio.sleep(0, result={})
    coinglass_symbols = [symbol for symbol in symbols_used if not _is_alpha_symbol(symbol)]
    market_snapshot_task = (
        load_coinglass_market_snapshots(
            exchange=exchange,
            symbols=coinglass_symbols,
            refresh=refresh,
        )
        if coinglass_symbols
        else asyncio.sleep(0, result={})
    )
    frame_result, market_snapshot_result, alpha_result = await asyncio.gather(
        _load_market_frames(exchange=exchange, timeframe=timeframe, symbols=symbols_used),
        market_snapshot_task,
        alpha_task,
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
        # str(exc) is empty for argument-less exceptions -- asyncio.TimeoutError
        # being the common one here -- which rendered this warning as
        # "CoinGlass market snapshot unavailable: " with nothing after the colon
        # and no way to tell a timeout from a connection failure. Lead with the
        # type, like the Binance Alpha branch below already does.
        _detail = str(market_snapshot_result).strip()
        warnings.append(
            "CoinGlass market snapshot unavailable: "
            f"{type(market_snapshot_result).__name__}"
            + (f": {_detail}" if _detail else "")
        )
    else:
        market_snapshots = market_snapshot_result

    alpha_payload: Dict[str, Any] = {}
    if isinstance(alpha_result, Exception):
        if alpha_needed:
            warnings.append(f"Binance Alpha catalog unavailable: {type(alpha_result).__name__}")
    elif isinstance(alpha_result, Mapping):
        alpha_payload = dict(alpha_result)
        alpha_warning = str(alpha_payload.get("warning") or "").strip()
        if alpha_warning:
            warnings.append(alpha_warning)
        try:
            alpha_snapshots = build_alpha_market_snapshots(
                alpha_payload.get("tokens") or [],
                timestamp=str(alpha_payload.get("updated_at") or "").strip() or None,
            )
        except Exception as exc:
            alpha_snapshots = {}
            warnings.append(f"Binance Alpha snapshot mapping failed: {type(exc).__name__}")
        allowed_alpha_symbols = set(symbols_used)
        for symbol, snapshot in alpha_snapshots.items():
            if symbol not in allowed_alpha_symbols:
                continue
            # Alpha IDs are unique and must not be replaced by a same-named
            # spot/futures symbol from another provider.
            market_snapshots[symbol] = snapshot

    # Alpha pairs are synthetic identifiers backed exclusively by the
    # collector-owned SQLite store.  If a catalog snapshot is temporarily
    # missing, leave the row degraded rather than sending the identifier to a
    # normal Binance spot/futures ticker fallback.
    fallback_targets = [
        symbol
        for symbol in symbols_used
        if symbol not in (market_snapshots or {}) and not _is_alpha_symbol(symbol)
    ]
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
            "alpha_meta": alpha_catalog_meta(alpha_payload) if alpha_needed else {},
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
        "alpha_meta": alpha_catalog_meta(alpha_payload) if alpha_needed else {},
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
        refresh=refresh,
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
            "Altcoin radar cache expired; background refresh in progress, serving previous snapshot."
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
    # summarize_rows counted the full scan; the table only shows `limited_rows`.
    # Publish the displayed-scope figure next to it (inside `summary`, where the
    # other counts live) so the UI can render both rather than showing a number
    # the reader cannot reconcile with the table. Copied rather than mutated in
    # place because `summarized` may be shared with a cached scan payload.
    response["summary"] = {
        **(response.get("summary") or {}),
        "degraded_count_displayed": sum(
            1 for row in limited_rows if (row.get("data_quality") or {}).get("degraded_reason")
        ),
        "displayed_row_count": len(limited_rows),
    }
    response["mode"] = _normalize_mode(mode)
    response["view"] = scan_payload.get("view") or str(scan_payload.get("timeframe") or DEFAULT_TIMEFRAME)
    response["universe_meta"] = scan_payload.get("universe_meta") or {}
    response["alpha_meta"] = scan_payload.get("alpha_meta") or {}
    response["scan_meta"] = {
        **response.get("scan_meta", {}),
        "generated_at": scan_payload.get("generated_at"),
        "cache": scan_payload.get("cache") or {},
        "row_count_before_limit": len(rows),
        "limit": max(1, min(int(limit or DEFAULT_LIMIT), cap)),
        "alpha_meta": scan_payload.get("alpha_meta") or {},
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
