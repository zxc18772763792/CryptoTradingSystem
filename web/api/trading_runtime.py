from __future__ import annotations

import asyncio
import copy
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request

from core.audit import audit_logger
from core.runtime import runtime_state
from core.risk.risk_manager import risk_manager
from core.trading import execution_engine, order_manager, position_manager
from web.api.auth import require_request_permissions, require_sensitive_ops_auth, require_sensitive_ops_permissions
from web.api.trading import (
    RiskUpdateRequest,
    TradingModeConfirmRequest,
    TradingModeRequest,
    _build_effective_risk_report,
)
from web.services import (
    build_runtime_diagnostics,
    cancel_mode_switch as cancel_trading_mode_switch_token,
    clear_local_trading_runtime as clear_local_runtime_service,
    get_mode_confirm_text,
    list_pending_mode_switches,
    request_mode_switch as request_trading_mode_switch_service,
    switch_trading_mode as switch_trading_mode_service,
)


router = APIRouter()
_TRADING_STATS_CACHE_TTL_SEC = 2.0
_trading_stats_cache_payload = None
_trading_stats_cache_at = 0.0
_trading_stats_cache_lock = asyncio.Lock()


def invalidate_trading_stats_cache() -> None:
    global _trading_stats_cache_payload, _trading_stats_cache_at
    _trading_stats_cache_payload = None
    _trading_stats_cache_at = 0.0


async def _build_trading_stats_payload() -> dict:
    degraded = False
    try:
        risk_report = await asyncio.wait_for(
            _build_effective_risk_report(force_live_refresh=False),
            timeout=6.0,
        )
    except Exception:
        risk_report = risk_manager.get_risk_report()
        degraded = True
    return {
        "orders": order_manager.get_stats(),
        "positions": position_manager.get_stats(),
        "risk": risk_report,
        "risk_degraded": degraded,
        "trading_mode": execution_engine.get_trading_mode(),
    }


def _pending_mode_target(token: str) -> str:
    safe_token = str(token or "").strip()
    if not safe_token:
        return ""
    for item in list_pending_mode_switches(include_token=True):
        if str((item or {}).get("token") or "").strip() == safe_token:
            return str((item or {}).get("target_mode") or "").strip().lower()
    return ""


@router.get("/risk/report")
async def get_risk_report():
    return await _build_effective_risk_report(force_live_refresh=False)


@router.post("/risk/params", dependencies=[Depends(require_sensitive_ops_permissions("approve_risk_change"))])
async def update_risk_params(request: RiskUpdateRequest):
    payload = request.model_dump(exclude_none=True)
    risk_manager.update_parameters(payload)
    invalidate_trading_stats_cache()
    await audit_logger.log(
        module="risk",
        action="update_params",
        status="success",
        message="Risk params updated",
        details=payload,
    )
    return {
        "success": True,
        "report": await _build_effective_risk_report(force_live_refresh=True),
    }


@router.post("/risk/reset", dependencies=[Depends(require_sensitive_ops_permissions("ack_alerts", "approve_risk_change"))])
async def reset_risk_halt():
    risk_manager.reset_halt()
    invalidate_trading_stats_cache()
    await audit_logger.log(
        module="risk",
        action="reset_halt",
        status="success",
        message="Risk halt reset",
    )
    return {
        "success": True,
        "report": await _build_effective_risk_report(force_live_refresh=True),
    }


@router.post("/paper/reset", dependencies=[Depends(require_sensitive_ops_permissions("reset_paper_runtime", "rotate_runtime"))])
async def reset_paper_trading_state(clear_snapshots: bool = True):
    if not execution_engine.is_paper_mode():
        raise HTTPException(status_code=400, detail="Paper mode is required")

    payload = await clear_local_runtime_service(clear_paper_snapshots=clear_snapshots)
    payload["cache_reset"] = runtime_state.clear_registered_caches(scope="paper")
    invalidate_trading_stats_cache()
    await audit_logger.log(
        module="trading",
        action="paper_reset",
        status="success",
        message="Paper trading state reset",
        details=payload,
    )
    return {"success": True, "result": payload}


@router.get("/stats")
async def get_trading_stats(force_refresh: bool = False):
    global _trading_stats_cache_payload, _trading_stats_cache_at
    now_mono = time.monotonic()
    if not force_refresh and _trading_stats_cache_payload is not None:
        if (now_mono - _trading_stats_cache_at) <= _TRADING_STATS_CACHE_TTL_SEC:
            return copy.deepcopy(_trading_stats_cache_payload)

    async with _trading_stats_cache_lock:
        now_mono = time.monotonic()
        if not force_refresh and _trading_stats_cache_payload is not None:
            if (now_mono - _trading_stats_cache_at) <= _TRADING_STATS_CACHE_TTL_SEC:
                return copy.deepcopy(_trading_stats_cache_payload)
        payload = await _build_trading_stats_payload()
        _trading_stats_cache_payload = copy.deepcopy(payload)
        _trading_stats_cache_at = time.monotonic()
        return payload


@router.get("/mode")
async def get_trading_mode():
    now = datetime.now(timezone.utc).isoformat()
    return {
        "mode": execution_engine.get_trading_mode(),
        "paper_trading": execution_engine.is_paper_mode(),
        "server_time": now,
        "pending_switches": list_pending_mode_switches(),
        "confirm_hint": get_mode_confirm_text(),
    }


@router.post("/mode/request", dependencies=[Depends(require_sensitive_ops_auth)])
async def request_trading_mode_switch(req: TradingModeRequest, request: Request):
    if str(req.target_mode or "").strip().lower() == "live":
        require_request_permissions(request, "request_live", "approve_live")
    else:
        require_request_permissions(request, "rotate_runtime", "reset_paper_runtime")
    return request_trading_mode_switch_service(
        target_mode=req.target_mode,
        current_mode=execution_engine.get_trading_mode(),
        reason=req.reason or "",
    )


@router.post("/mode/confirm", dependencies=[Depends(require_sensitive_ops_auth)])
async def confirm_trading_mode_switch(req: TradingModeConfirmRequest, request: Request):
    pending_target = _pending_mode_target(req.token)
    if pending_target == "live":
        require_request_permissions(request, "approve_live")
    elif pending_target == "paper":
        require_request_permissions(request, "rotate_runtime", "reset_paper_runtime")
    else:
        require_request_permissions(request, "approve_live", "rotate_runtime", "reset_paper_runtime")
    result = await switch_trading_mode_service(
        token=req.token,
        confirm_text=req.confirm_text,
        app=request.app,
        reason="web.api.trading_runtime.confirm_mode",
        clear_paper_snapshots=True,
    )
    invalidate_trading_stats_cache()
    await audit_logger.log(
        module="trading",
        action="switch_mode",
        status="success",
        message=f"mode={result.get('mode')}",
        details=result,
    )
    return result


@router.post("/mode/cancel", dependencies=[Depends(require_sensitive_ops_auth)])
async def cancel_trading_mode_switch(token: str, request: Request):
    pending_target = _pending_mode_target(token)
    if pending_target == "live":
        require_request_permissions(request, "request_live", "approve_live")
    elif pending_target == "paper":
        require_request_permissions(request, "rotate_runtime", "reset_paper_runtime")
    else:
        require_request_permissions(request, "request_live", "approve_live", "rotate_runtime", "reset_paper_runtime")
    if cancel_trading_mode_switch_token(token):
        return {"success": True, "token": token}
    raise HTTPException(status_code=404, detail="Mode switch token not found")


@router.get("/runtime/diagnostics")
async def get_runtime_diagnostics_endpoint():
    return build_runtime_diagnostics()
