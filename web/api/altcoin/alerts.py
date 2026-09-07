"""Radar alert-preset create/delete routes.

Turn a preset label into a notification rule (and back), snapshotting the
current scan so the rule captures a config key. Uses the scan engine via
scan.<name> and the shared preset/rule helpers.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from core.data.coinglass_altcoin import is_alt_candidate_symbol
from core.notifications import notification_manager
from web.api.auth import require_sensitive_ops_permissions

from . import scan
from .cache import build_altcoin_notification_config_key
from .constants import AltcoinAlertPresetRequest, DEFAULT_EXCHANGE, DEFAULT_TIMEFRAME
from .helpers import (
    _normalize_exchange,
    _normalize_mode,
    _normalize_symbols,
    _normalize_timeframe,
    _normalize_universe_scope,
    _normalize_view,
    _parse_symbols_param,
    _preset_definition,
)
from .scan import _normalize_alert_rule, _rule_matches_scan_context

router = APIRouter()


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
