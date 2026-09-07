"""Radar single-symbol detail drawer + research-proposal routes.

The detail route fans out to the scan snapshot, on-chain overview and a live
chain-context fallback under strict per-source timeouts. Route-level dependencies
(get_onchain_overview, _load_detail_live_chain_context, get_altcoin_radar_detail,
DETAIL_ONCHAIN_TIMEOUT_SEC) are module-level names here; tests patch them at
web.api.altcoin.detail.<name>.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request

from core.research.altcoin_radar import build_detail_payload
from web.api.auth import require_sensitive_ops_permissions
from web.api.data import get_onchain_overview

from . import scan
from .constants import (
    DEFAULT_EXCHANGE,
    DEFAULT_TIMEFRAME,
    DETAIL_LIVE_CHAIN_TIMEOUT_SEC,
    DETAIL_ONCHAIN_TIMEOUT_SEC,
)
from .helpers import (
    _needs_detail_chain_fallback,
    _normalize_exchange,
    _normalize_mode,
    _normalize_sort,
    _normalize_symbols,
    _normalize_view,
    _parse_symbols_param,
    _resolve_requested_timeframe,
    _utcnow,
)

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
