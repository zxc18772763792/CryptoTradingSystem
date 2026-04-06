from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request

from core.audit import audit_logger
from core.runtime import runtime_state
from core.risk.risk_manager import risk_manager
from core.trading import execution_engine, order_manager, position_manager
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


@router.get("/risk/report")
async def get_risk_report():
    return await _build_effective_risk_report(force_live_refresh=False)


@router.post("/risk/params")
async def update_risk_params(request: RiskUpdateRequest):
    payload = request.model_dump(exclude_none=True)
    risk_manager.update_parameters(payload)
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


@router.post("/risk/reset")
async def reset_risk_halt():
    risk_manager.reset_halt()
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


@router.post("/paper/reset")
async def reset_paper_trading_state(clear_snapshots: bool = True):
    if not execution_engine.is_paper_mode():
        raise HTTPException(status_code=400, detail="当前不是模拟盘模式")

    payload = await clear_local_runtime_service(clear_paper_snapshots=clear_snapshots)
    payload["cache_reset"] = runtime_state.clear_registered_caches(scope="paper")
    await audit_logger.log(
        module="trading",
        action="paper_reset",
        status="success",
        message="Paper trading state reset",
        details=payload,
    )
    return {"success": True, "result": payload}


@router.get("/stats")
async def get_trading_stats():
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


@router.post("/mode/request")
async def request_trading_mode_switch(req: TradingModeRequest):
    return request_trading_mode_switch_service(
        target_mode=req.target_mode,
        current_mode=execution_engine.get_trading_mode(),
        reason=req.reason or "",
    )


@router.post("/mode/confirm")
async def confirm_trading_mode_switch(req: TradingModeConfirmRequest, request: Request):
    result = await switch_trading_mode_service(
        token=req.token,
        confirm_text=req.confirm_text,
        app=request.app,
        reason="web.api.trading_runtime.confirm_mode",
        clear_paper_snapshots=True,
    )
    await audit_logger.log(
        module="trading",
        action="switch_mode",
        status="success",
        message=f"mode={result.get('mode')}",
        details=result,
    )
    return result


@router.post("/mode/cancel")
async def cancel_trading_mode_switch(token: str):
    if cancel_trading_mode_switch_token(token):
        return {"success": True, "token": token}
    raise HTTPException(status_code=404, detail="切换令牌不存在")


@router.get("/runtime/diagnostics")
async def get_runtime_diagnostics_endpoint():
    return build_runtime_diagnostics()
