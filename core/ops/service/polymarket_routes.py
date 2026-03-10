from __future__ import annotations

import secrets
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request

from core.audit.ops_audit import ops_audit_scope
from core.ops.service import api as ops_api
from core.ops.service.auth import get_request_auth


router = APIRouter()


@router.get("/polymarket/status")
async def polymarket_status(request: Request):
    try:
        return ops_api._ok(await ops_api._build_polymarket_status(request.app))
    except Exception as exc:
        return ops_api._err(str(exc))


@router.post("/polymarket/subscribe")
async def polymarket_subscribe(request: Request, payload: ops_api.PolymarketSubscribeRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/subscribe", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            cfg = dict(request.app.state.polymarket_cfg or ops_api.load_polymarket_config())
            categories = dict(cfg.get("categories") or {})
            cat = str(payload.category or "").strip().upper()
            if cat not in categories:
                return ops_api._err(f"unknown category: {cat}")
            if payload.mode == "manual":
                item = dict(categories.get(cat) or {})
                if payload.keywords:
                    item["keywords"] = list(payload.keywords)
                if payload.tags:
                    item["tags"] = [int(x) for x in payload.tags]
                if payload.max_markets:
                    item["max_markets"] = int(payload.max_markets)
                categories[cat] = item
                cfg["categories"] = categories
                request.app.state.polymarket_cfg = cfg
            result = await ops_api.pm_refresh_markets_once(cfg, categories=[cat])
            audit_state["extra"] = {"category": cat}
            return ops_api._ok({"category": cat, "mode": payload.mode, "result": result})
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/polymarket/unsubscribe")
async def polymarket_unsubscribe(request: Request, payload: ops_api.PolymarketUnsubscribeRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/unsubscribe", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            result = await ops_api.pm_db.disable_subscriptions(payload.market_ids)
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/polymarket/worker_run_once")
async def polymarket_worker_run_once(request: Request, payload: ops_api.PolymarketWorkerRunRequest):
    auth = get_request_auth(request)
    params = payload.model_dump()
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/worker_run_once", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            cfg = request.app.state.polymarket_cfg or ops_api.load_polymarket_config()
            result = await ops_api.pm_run_worker_once(
                cfg,
                refresh_markets=bool(payload.refresh_markets),
                refresh_quotes=bool(payload.refresh_quotes),
                categories=[str(x).strip().upper() for x in payload.categories if str(x).strip()] or None,
            )
            return ops_api._ok(result)
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.get("/polymarket/alerts")
async def polymarket_alerts(since: Optional[str] = None, category: Optional[str] = None, limit: int = 200):
    try:
        since_ts = ops_api.parse_any_datetime(since) if since else None
        rows = await ops_api.pm_db.list_alerts(since=since_ts, category=category, limit=limit)
        return ops_api._ok({"count": len(rows), "items": rows})
    except Exception as exc:
        return ops_api._err(str(exc))


@router.get("/polymarket/features")
async def polymarket_features(symbol: str, tf: str = "1m", since: Optional[str] = None):
    try:
        since_ts = ops_api.parse_any_datetime(since) if since else (ops_api._now_utc() - timedelta(hours=24))
        rows = await ops_api.pm_db.get_features_range(
            symbol=str(symbol or "").strip().upper(),
            since=since_ts,
            until=ops_api._now_utc(),
            timeframe=str(tf or "1m").strip().lower(),
        )
        return ops_api._ok({"count": len(rows), "items": rows})
    except Exception as exc:
        return ops_api._err(str(exc))


@router.post("/polymarket/arm_trading")
async def polymarket_arm_trading(request: Request):
    auth = get_request_auth(request)
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/arm_trading", method="POST", params={}, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            code = secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:10]
            expires_at = ops_api._now_utc() + timedelta(seconds=120)
            request.app.state.polymarket_trading_approvals[code] = {
                "approval_code": code,
                "issued_at": ops_api._now_utc(),
                "expires_at": expires_at,
                "actor": auth.actor,
                "used": False,
            }
            return ops_api._ok({"approval_code": code, "expires_at": expires_at.isoformat(), "note": "Call /ops/polymarket/enable_trading with X-OPS-APPROVAL within 120s"})
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))


@router.post("/polymarket/enable_trading")
async def polymarket_enable_trading(request: Request, x_ops_approval: Optional[str] = Header(default=None, alias="X-OPS-APPROVAL")):
    auth = get_request_auth(request)
    params = {"approval_code": x_ops_approval or ""}
    async with ops_audit_scope(actor=auth.actor, endpoint="/ops/polymarket/enable_trading", method="POST", params=params, ip=auth.client_ip) as audit_state:
        try:
            ops_api._ensure_ops_runtime_state(request.app)
            code = str(x_ops_approval or "").strip()
            approval = request.app.state.polymarket_trading_approvals.get(code)
            if not approval:
                audit_state["status"] = "denied"
                audit_state["error"] = "invalid approval code"
                raise HTTPException(status_code=403, detail="invalid approval code")
            if approval.get("expires_at") <= ops_api._now_utc():
                request.app.state.polymarket_trading_approvals.pop(code, None)
                audit_state["status"] = "denied"
                audit_state["error"] = "approval code expired"
                raise HTTPException(status_code=403, detail="approval code expired")
            request.app.state.polymarket_cfg.setdefault("defaults", {}).setdefault("trading", {})["enabled"] = True
            request.app.state.polymarket_trading_approvals.pop(code, None)
            return ops_api._ok({"trading_enabled": True})
        except HTTPException:
            raise
        except Exception as exc:
            audit_state["status"] = "failed"
            audit_state["error"] = str(exc)
            return ops_api._err(str(exc))
