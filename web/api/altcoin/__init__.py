"""Altcoin radar API routes."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Mapping, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request

from core.notifications import notification_manager
from core.data.coinglass_altcoin import is_alt_candidate_symbol
from core.data.binance_alpha import (
    alpha_catalog_meta,
    alpha_pair_from_id,
    alpha_symbols,
    load_alpha_token_catalog,
)
from core.research.altcoin_radar import build_detail_payload
from core.research.altcoin_radar_universe import (
    add_watchlist_symbol,
    get_watchlist_symbols,
    remove_watchlist_symbol,
    universe_meta,
)
from web.api.auth import require_sensitive_ops_permissions
from web.api.data import (
    _research_retired_filter,
    get_onchain_overview,
    get_research_symbols,
)

# --- extracted submodules (re-exported to preserve the public + patch surface) ---
from .constants import (  # noqa: E402
    ALLOWED_MODES as ALLOWED_MODES,
    ALLOWED_SORTS as ALLOWED_SORTS,
    ALLOWED_UNIVERSE_SCOPES as ALLOWED_UNIVERSE_SCOPES,
    ALLOWED_VIEWS as ALLOWED_VIEWS,
    ALTCOIN_RULE_TYPES as ALTCOIN_RULE_TYPES,
    DEFAULT_EXCHANGE as DEFAULT_EXCHANGE,
    DEFAULT_LIMIT as DEFAULT_LIMIT,
    DEFAULT_SORT as DEFAULT_SORT,
    DEFAULT_TIMEFRAME as DEFAULT_TIMEFRAME,
    DETAIL_LIVE_CHAIN_TIMEOUT_SEC as DETAIL_LIVE_CHAIN_TIMEOUT_SEC,
    DETAIL_ONCHAIN_TIMEOUT_SEC as DETAIL_ONCHAIN_TIMEOUT_SEC,
    MAX_EXPANDED_SIZE as MAX_EXPANDED_SIZE,
    MAX_UNIVERSE_SIZE as MAX_UNIVERSE_SIZE,
    TTL_BY_TIMEFRAME as TTL_BY_TIMEFRAME,
    TTL_BY_VIEW as TTL_BY_VIEW,
    _PUBLIC_MARKET_SNAPSHOT_TTL_SEC as _PUBLIC_MARKET_SNAPSHOT_TTL_SEC,
    AltcoinAlertPresetRequest as AltcoinAlertPresetRequest,
    AltcoinWatchlistMutationRequest as AltcoinWatchlistMutationRequest,
)
from .state import (  # noqa: E402
    _ALTCOIN_SCAN_CACHE as _ALTCOIN_SCAN_CACHE,
    _ALTCOIN_SCAN_LOCKS as _ALTCOIN_SCAN_LOCKS,
    _ALTCOIN_SCAN_LOCKS_GUARD as _ALTCOIN_SCAN_LOCKS_GUARD,
    _ALTCOIN_SCAN_REFRESH_TASKS as _ALTCOIN_SCAN_REFRESH_TASKS,
    _PUBLIC_MARKET_SNAPSHOT_CACHE as _PUBLIC_MARKET_SNAPSHOT_CACHE,
)
from .helpers import (  # noqa: E402
    _age_seconds_at as _age_seconds_at,
    _binance_public_symbol_key as _binance_public_symbol_key,
    _binance_timestamp_from_ticker as _binance_timestamp_from_ticker,
    _build_binance_public_market_snapshot as _build_binance_public_market_snapshot,
    _cache_ttl as _cache_ttl,
    _clone_payload as _clone_payload,
    _filter_fresh_market_frames as _filter_fresh_market_frames,
    _hash_universe as _hash_universe,
    _is_alpha_symbol as _is_alpha_symbol,
    _is_fresh_market_snapshot as _is_fresh_market_snapshot,
    _market_data_stale_cutoff_seconds as _market_data_stale_cutoff_seconds,
    _market_frame_age_seconds as _market_frame_age_seconds,
    _match_binance_public_ticker as _match_binance_public_ticker,
    _needs_detail_chain_fallback as _needs_detail_chain_fallback,
    _normalize_exchange as _normalize_exchange,
    _normalize_mode as _normalize_mode,
    _normalize_sort as _normalize_sort,
    _normalize_symbols as _normalize_symbols,
    _normalize_timeframe as _normalize_timeframe,
    _normalize_universe_scope as _normalize_universe_scope,
    _normalize_view as _normalize_view,
    _pair_symbol_from_base as _pair_symbol_from_base,
    _parse_symbols_param as _parse_symbols_param,
    _preset_definition as _preset_definition,
    _preset_label_from_rule as _preset_label_from_rule,
    _alert_kind_from_rule as _alert_kind_from_rule,
    _resolve_requested_timeframe as _resolve_requested_timeframe,
    _safe_float as _safe_float,
    _serialize_community_snapshot as _serialize_community_snapshot,
    _serialize_derivatives_snapshot as _serialize_derivatives_snapshot,
    _serialize_micro_snapshot as _serialize_micro_snapshot,
    _serialize_whale_snapshot as _serialize_whale_snapshot,
    _should_overlay_coinglass_market_snapshot as _should_overlay_coinglass_market_snapshot,
    _snapshot_age_seconds as _snapshot_age_seconds,
    _utcnow as _utcnow,
)
from .cache import (  # noqa: E402
    _ALTCOIN_SCAN_CACHE_MAX_ENTRIES as _ALTCOIN_SCAN_CACHE_MAX_ENTRIES,
    _build_cached_scan_payload as _build_cached_scan_payload,
    _cache_age_sec as _cache_age_sec,
    _cache_key as _cache_key,
    _cache_lock as _cache_lock,
    _clear_altcoin_scan_cache as _clear_altcoin_scan_cache,
    _evict_altcoin_scan_cache as _evict_altcoin_scan_cache,
    _finalize_scan_payload as _finalize_scan_payload,
    _should_cache_scan_payload as _should_cache_scan_payload,
    build_altcoin_notification_config_key as build_altcoin_notification_config_key,
)
# --- end extracted submodules ---



router = APIRouter()











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
    scan_payload = await scan.get_altcoin_scan_snapshot(
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
    target_scan = await scan.get_altcoin_scan_snapshot(
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
    normalized_watch = _normalize_symbols(symbols)
    exchange_watch = [symbol for symbol in normalized_watch if not _is_alpha_symbol(symbol)]
    try:
        live_map = await asyncio.wait_for(
            scan._load_exchange_public_market_snapshots(
                exchange=DEFAULT_EXCHANGE,
                symbols=exchange_watch,
            ),
            timeout=8.0,
        )
        unlisted_symbols = [
            symbol
            for symbol in exchange_watch
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


@router.get("/radar/universe")
async def get_altcoin_radar_universe(
    exchange: str = DEFAULT_EXCHANGE,
    refresh: bool = False,
):
    """Return the radar's merged research + Alpha directory for the UI.

    This endpoint is intentionally read-only.  Alpha metadata is cached to
    disk by ``core.data.binance_alpha`` and the response only returns compact
    labels, while the raw snapshots remain available locally for research.
    """
    research_task = get_research_symbols(exchange=exchange, include_major=False)
    alpha_task = load_alpha_token_catalog(refresh=bool(refresh))
    research_result, alpha_result = await asyncio.gather(
        research_task,
        alpha_task,
        return_exceptions=True,
    )

    warnings: List[str] = []
    research_symbols: List[str] = []
    if isinstance(research_result, Mapping):
        research_symbols = _normalize_symbols(research_result.get("symbols") or [])
    else:
        warnings.append(f"research universe unavailable: {type(research_result).__name__}")

    alpha_payload: Dict[str, Any] = {}
    if isinstance(alpha_result, Mapping):
        alpha_payload = dict(alpha_result)
    else:
        warnings.append(f"Binance Alpha catalog unavailable: {type(alpha_result).__name__}")
    alpha_tokens = list(alpha_payload.get("tokens") or [])
    alpha_pool = _normalize_symbols(alpha_symbols(alpha_tokens))
    alpha_meta = alpha_catalog_meta(alpha_payload)
    if alpha_meta.get("warning"):
        warnings.append(str(alpha_meta["warning"]))

    watchlist = _normalize_symbols(get_watchlist_symbols())
    if len(alpha_pool) > MAX_EXPANDED_SIZE:
        warnings.append(
            f"Binance Alpha 当前 {len(alpha_pool)} 个活跃候选，单次雷达扫描上限为 {MAX_EXPANDED_SIZE}。"
        )
    merged = _normalize_symbols(research_symbols + alpha_pool + watchlist)
    merged = merged[:MAX_EXPANDED_SIZE]
    compact_labels: Dict[str, Dict[str, Any]] = {}
    for token in alpha_tokens:
        if not isinstance(token, Mapping):
            continue
        pair = alpha_pair_from_id(token.get("alphaId"))
        if not pair:
            continue
        compact_labels[pair] = {
            "display_symbol": str(token.get("symbol") or "").strip(),
            "name": str(token.get("name") or "").strip(),
            "alpha_id": str(token.get("alphaId") or "").strip(),
            "chain_name": str(token.get("chainName") or "").strip(),
            "hot_tag": str(token.get("hotTag") or "").strip().lower() in {"1", "true", "yes", "y", "on"},
        }

    return {
        "exchange": _normalize_exchange(exchange),
        "symbols": merged,
        "count": len(merged),
        "default_count": min(MAX_UNIVERSE_SIZE, len(merged)),
        "max_scan_count": MAX_EXPANDED_SIZE,
        "research_count": len(research_symbols),
        "alpha_symbols": alpha_pool[:MAX_EXPANDED_SIZE],
        "alpha_count": len(alpha_pool),
        "alpha_meta": alpha_meta,
        "symbol_meta": compact_labels,
        "watchlist_count": len(watchlist),
        "source": "research_plus_binance_alpha",
        "warnings": list(dict.fromkeys(warnings)),
        "updated_at": _utcnow().isoformat(),
    }


@router.get("/radar/collector")
async def get_binance_alpha_collector_status():
    """Return persisted Binance Alpha collector health and coverage stats."""
    from core.data.binance_alpha_collector import load_collector_status  # noqa: PLC0415

    return load_collector_status()


# --- Pump-precursor weekly watchlist (model-ranked, research-only) -----------
# NOTE: this file lives at web/api/altcoin/__init__.py, so the repo root is parents[3].

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

# --- scan pipeline (extracted engine; routes merged, entry points re-exported) ---
from . import scan as scan  # noqa: E402
from .scan import (  # noqa: E402
    _alert_rules_for_scan as _alert_rules_for_scan,
    _apply_alert_rules_to_rows as _apply_alert_rules_to_rows,
    _compute_scan_payload as _compute_scan_payload,
    _normalize_alert_rule as _normalize_alert_rule,
    _rule_matches_scan_context as _rule_matches_scan_context,
    _universe_matches as _universe_matches,
    _fetch_binance_public_tickers as _fetch_binance_public_tickers,
    _load_active_altcoin_rules as _load_active_altcoin_rules,
    _load_exchange_public_market_snapshots as _load_exchange_public_market_snapshots,
    _load_market_frames as _load_market_frames,
    _load_snapshot_maps as _load_snapshot_maps,
    _map_derivatives_rows_to_requested_symbols as _map_derivatives_rows_to_requested_symbols,
    _resolve_universe as _resolve_universe,
    build_altcoin_notification_context as build_altcoin_notification_context,
    get_altcoin_scan_snapshot as get_altcoin_scan_snapshot,
    warm_default_scan_cache as warm_default_scan_cache,
)
router.include_router(scan.router)

# --- feature route clusters (own sub-routers, merged into the package router) ---
from . import pump as pump  # noqa: E402
from . import signals as signals  # noqa: E402
router.include_router(pump.router)
router.include_router(signals.router)
