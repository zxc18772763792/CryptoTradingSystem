from __future__ import annotations

import asyncio
import copy
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger

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
_TRADING_STATS_CACHE_TTL_SEC = 8.0
_TRADING_STATS_STALE_TTL_SEC = 60.0
_TRADING_STATS_BUILD_TIMEOUT_SEC = 7.5
_trading_stats_cache_payload = None
_trading_stats_cache_at = 0.0
_trading_stats_cache_lock: asyncio.Lock | None = None  # lazily created


def _consume_audit_task_result(task) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.warning(f"Background audit log task failed: {exc}")


def _schedule_audit_log(**kwargs) -> None:
    async def _run() -> None:
        try:
            await audit_logger.log(**kwargs)
        except Exception as exc:
            logger.warning(f"Background audit log failed: {exc}")

    coro = _run()
    try:
        task = asyncio.create_task(coro)
    except RuntimeError as exc:
        coro.close()
        logger.warning(f"Failed to schedule audit log: {exc}")
        return
    if hasattr(task, "add_done_callback"):
        task.add_done_callback(_consume_audit_task_result)


def _get_stats_lock() -> asyncio.Lock:
    global _trading_stats_cache_lock
    if _trading_stats_cache_lock is None:
        _trading_stats_cache_lock = asyncio.Lock()
    return _trading_stats_cache_lock


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


def _clone_trading_stats_cache(*, max_age_sec: float, stale: bool = False) -> dict | None:
    if _trading_stats_cache_payload is None:
        return None
    age_sec = max(0.0, time.monotonic() - float(_trading_stats_cache_at or 0.0))
    if age_sec > max(0.0, float(max_age_sec or 0.0)):
        return None
    payload = copy.deepcopy(_trading_stats_cache_payload)
    payload["from_cache"] = True
    payload["cache_age_sec"] = round(age_sec, 2)
    if stale:
        payload["stale"] = True
        payload.setdefault("warning", "统计快照刷新中，已先返回最近缓存。")
    return payload


def _build_trading_stats_fallback_payload(reason: str) -> dict:
    return {
        "orders": order_manager.get_stats(),
        "positions": position_manager.get_stats(),
        "risk": risk_manager.get_risk_report(),
        "risk_degraded": True,
        "trading_mode": execution_engine.get_trading_mode(),
        "stale": True,
        "warning": reason,
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
    _schedule_audit_log(
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
    _schedule_audit_log(
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
    _schedule_audit_log(
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
    if not force_refresh:
        cached = _clone_trading_stats_cache(max_age_sec=_TRADING_STATS_CACHE_TTL_SEC)
        if cached is not None:
            return cached
        if _get_stats_lock().locked():
            stale = _clone_trading_stats_cache(
                max_age_sec=_TRADING_STATS_STALE_TTL_SEC,
                stale=True,
            )
            if stale is not None:
                return stale
            return _build_trading_stats_fallback_payload("统计快照刷新中，已返回轻量级快照。")

    async with _get_stats_lock():
        if not force_refresh:
            cached = _clone_trading_stats_cache(
                max_age_sec=_TRADING_STATS_CACHE_TTL_SEC
            )
            if cached is not None:
                return cached
        try:
            payload = await asyncio.wait_for(
                _build_trading_stats_payload(),
                timeout=_TRADING_STATS_BUILD_TIMEOUT_SEC,
            )
        except Exception as exc:
            logger.warning(f"trading stats refresh fell back to cached risk report: {exc}")
            payload = _build_trading_stats_fallback_payload("统计快照刷新超时，已返回轻量级快照。")
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
    _schedule_audit_log(
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
