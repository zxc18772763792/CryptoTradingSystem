"""Radar universe / watchlist / Alpha-collector routes.

Read/mutate the discovery universe: the stored watchlist, the resolved research/
expanded/alpha scopes, and the Alpha collector status. Watchlist mutations clear
the scan cache. Source functions (get_watchlist_symbols, add/remove) are
module-level names here; tests patch them at web.api.altcoin.universe.<name>.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Mapping

from fastapi import APIRouter, Depends, HTTPException

from core.data.binance_alpha import (
    alpha_catalog_meta,
    alpha_pair_from_id,
    alpha_symbols,
    load_alpha_token_catalog,
)
from core.research.altcoin_radar_universe import (
    add_watchlist_symbol,
    get_watchlist_symbols,
    remove_watchlist_symbol,
    universe_meta,
)
from web.api.auth import require_sensitive_ops_permissions
from web.api.data import _research_retired_filter, get_research_symbols

from . import scan
from .cache import _clear_altcoin_scan_cache
from .constants import (
    AltcoinWatchlistMutationRequest,
    DEFAULT_EXCHANGE,
    DEFAULT_TIMEFRAME,
    MAX_EXPANDED_SIZE,
    MAX_UNIVERSE_SIZE,
)
from .helpers import (
    _is_alpha_symbol,
    _normalize_exchange,
    _safe_float,
    _utcnow,
    _normalize_symbols,
)

router = APIRouter()


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
