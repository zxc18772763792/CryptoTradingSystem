from __future__ import annotations

import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request

from core.audit.ops_audit import ops_audit_scope
from core.exchanges import exchange_manager
from core.ops.service import api as ops_api
from core.ops.service.auth import get_request_auth
from core.risk.risk_manager import risk_manager
from core.trading.execution_engine import execution_engine


router = APIRouter()


@router.post("/trading/start_paper")
async def trading_start_paper(request: Request):
    auth = get_request_auth(request)
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/start_paper", method="POST", params={}, ip=auth.client_ip) as audit_state:
        try:
            await ops_api._ensure_paper_mode_started()
            result = {
                "running": bool(execution_engine.is_running),
                "mode": execution_engine.get_trading_mode(),
                "queue_size": int(execution_engine.get_queue_size()),
                "risk_scope": "paper",
            }
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/trading/arm_live")
async def trading_arm_live(request: Request):
    auth = get_request_auth(request)
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/arm_live", method="POST", params={}, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            ops_api._cleanup_live_approvals(request.app)
            code = secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:10]
            expires_at = ops_api._now_utc() + timedelta(seconds=120)
            request.app.state.live_approvals[code] = {
                "approval_code": code,
                "issued_at": ops_api._now_utc(),
                "expires_at": expires_at,
                "actor": auth.actor,
                "used": False,
            }
            audit_state["extra"] = {"approval_expires_at": expires_at.isoformat()}
            return ops_api._ok(
                {
                    "approval_code": code,
                    "expires_at": expires_at.isoformat(),
                    "note": "Call /ops/trading/start_live with X-OPS-APPROVAL within 120s",
                }
            )
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/trading/start_live")
async def trading_start_live(request: Request, x_ops_approval: Optional[str] = Header(default=None, alias="X-OPS-APPROVAL")):
    auth = get_request_auth(request)
    params = {"approval_code": x_ops_approval or ""}
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/start_live", method="POST", params=params, ip=auth.client_ip) as audit_state:
        ops_api._ensure_ops_runtime_state(request.app)
        ops_api._cleanup_live_approvals(request.app)
        code = str(x_ops_approval or "").strip()
        approval = request.app.state.live_approvals.get(code)
        if not code or not approval:
            audit_state["status"] = "denied"
            audit_state["error"] = "invalid approval code"
            raise HTTPException(status_code=403, detail="invalid approval code")
        if approval.get("used"):
            audit_state["status"] = "denied"
            audit_state["error"] = "approval code already used"
            raise HTTPException(status_code=403, detail="approval code already used")
        expires_at = approval.get("expires_at")
        if not isinstance(expires_at, datetime) or expires_at <= ops_api._now_utc():
            request.app.state.live_approvals.pop(code, None)
            audit_state["status"] = "denied"
            audit_state["error"] = "approval code expired"
            raise HTTPException(status_code=403, detail="approval code expired")
        try:
            await ops_api._ensure_live_mode_started()
            approval["used"] = True
            request.app.state.live_approvals.pop(code, None)
            return ops_api._ok(ops_api._build_status_execution())
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/trading/stop")
async def trading_stop(request: Request):
    auth = get_request_auth(request)
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/stop", method="POST", params={}, ip=auth.client_ip) as audit_state:
        try:
            await execution_engine.stop()
            return ops_api._ok(
                {
                    "running": False,
                    "mode": execution_engine.get_trading_mode(),
                    "conditional_orders_count": len(execution_engine.list_conditional_orders()),
                }
            )
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/trading/kill_switch")
async def trading_kill_switch(request: Request):
    auth = get_request_auth(request)
    params = {"mode": execution_engine.get_trading_mode()}
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/trading/kill_switch", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            connected = list(exchange_manager.get_connected_exchanges())
            pre_open_orders: Dict[str, int] = {}
            for exchange_name in connected:
                connector = exchange_manager.get_exchange(exchange_name)
                if not connector:
                    continue
                try:
                    pre_open_orders[exchange_name] = len(await connector.get_open_orders())
                except Exception:
                    pre_open_orders[exchange_name] = 0
            conditional_before = len(execution_engine.list_conditional_orders())
            await execution_engine.stop()
            local = await ops_api._close_local_positions()
            orphan = {"closed": [], "failures": []}
            if not execution_engine.is_paper_mode():
                orphan = await ops_api._close_exchange_orphan_positions()
            result = {
                "engine_stopped": True,
                "cancelled_orders": {
                    "exchange_open_orders_before": pre_open_orders,
                    "conditional_orders_before": conditional_before,
                    "total_exchange_open_orders_before": int(sum(pre_open_orders.values())),
                },
                "closed_local_positions": local.get("closed", []),
                "closed_exchange_positions": orphan.get("closed", []),
                "failures": list(local.get("failures", [])) + list(orphan.get("failures", [])),
            }
            audit_state["extra"] = {
                "closed_local": len(result["closed_local_positions"]),
                "closed_exchange": len(result["closed_exchange_positions"]),
                "failures": len(result["failures"]),
            }
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/risk/reset_halt")
async def risk_reset_halt(request: Request):
    auth = get_request_auth(request)
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/risk/reset_halt", method="POST", params={}, ip=auth.client_ip) as audit_state:
        try:
            risk_manager.reset_halt()
            report = risk_manager.get_risk_report()
            return ops_api._ok(
                {
                    "trading_halted": bool(report.get("trading_halted", False)),
                    "halt_reason": str(report.get("halt_reason") or ""),
                    "report": report,
                }
            )
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))
