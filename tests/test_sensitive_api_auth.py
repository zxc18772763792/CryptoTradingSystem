from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from core.governance.rbac import GovernanceIdentity
from core.ops.service import auth as ops_auth_module
from web.api import ai_agent, auth as web_auth, notifications, trading as trading_api, trading_orders, trading_positions, trading_runtime


def _ops_headers() -> dict[str, str]:
    return {
        "X-OPS-TOKEN": "test-token",
        "X-OPS-CALLER": "pytest",
    }


def _build_app(*routers: tuple[str, object]) -> FastAPI:
    app = FastAPI()
    for prefix, router in routers:
        app.include_router(router, prefix=prefix)
    return app


def _api_identity(role: str) -> GovernanceIdentity:
    return GovernanceIdentity(
        actor=f"{role.lower()}_api",
        role=role,
        api_key_present=True,
        token_present=False,
        client_ip="127.0.0.1",
    )


def test_trading_runtime_write_route_requires_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    trading_runtime.invalidate_trading_stats_cache()

    app = _build_app(("/api/trading", trading_runtime.router))
    client = TestClient(app)

    monkeypatch.setattr(
        trading_runtime,
        "request_trading_mode_switch_service",
        lambda **kwargs: {"success": True, "token": "tok-1", "target_mode": kwargs.get("target_mode")},
    )

    response = client.post("/api/trading/mode/request", json={"target_mode": "live", "reason": "verify"})
    assert response.status_code == 401

    response = client.post(
        "/api/trading/mode/request",
        json={"target_mode": "live", "reason": "verify"},
        headers=_ops_headers(),
    )
    assert response.status_code == 200
    assert response.json()["target_mode"] == "live"


def test_loopback_ui_cookie_allows_sensitive_post(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    monkeypatch.setattr(web_auth, "_request_client_ip", lambda request: "127.0.0.1")

    app = _build_app(("/api/trading", trading_runtime.router))

    @app.get("/")
    async def index(request: Request):
        response = JSONResponse({"ok": True, "ts": datetime.now(timezone.utc).isoformat()})
        web_auth.set_local_ui_session_cookie(request, response)
        return response

    monkeypatch.setattr(
        trading_runtime,
        "request_trading_mode_switch_service",
        lambda **kwargs: {"success": True, "token": "tok-2", "target_mode": kwargs.get("target_mode")},
    )

    client = TestClient(app, base_url="http://127.0.0.1:8000")
    home = client.get("/")
    assert home.status_code == 200
    assert web_auth._LOCAL_UI_COOKIE_NAME in client.cookies

    response = client.post("/api/trading/mode/request", json={"target_mode": "paper", "reason": "loopback-ui"})
    assert response.status_code == 200
    assert response.json()["target_mode"] == "paper"


def test_loopback_ui_cookie_rejects_cross_port_origin(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    monkeypatch.setattr(web_auth, "_request_client_ip", lambda request: "127.0.0.1")

    app = _build_app(("/api/trading", trading_runtime.router))

    @app.get("/")
    async def index(request: Request):
        response = JSONResponse({"ok": True})
        web_auth.set_local_ui_session_cookie(request, response)
        return response

    monkeypatch.setattr(
        trading_runtime,
        "request_trading_mode_switch_service",
        lambda **kwargs: {"success": True, "token": "tok-cross-port", "target_mode": kwargs.get("target_mode")},
    )

    client = TestClient(app, base_url="http://127.0.0.1:8000")
    assert client.get("/").status_code == 200
    response = client.post(
        "/api/trading/mode/request",
        json={"target_mode": "paper", "reason": "cross-port"},
        headers={"Origin": "http://127.0.0.1:3000"},
    )
    assert response.status_code == 401


def test_trading_stats_route_uses_short_ttl_cache(monkeypatch):
    trading_runtime.invalidate_trading_stats_cache()
    counter = {"count": 0}

    async def fake_risk_report(force_live_refresh: bool = False):
        counter["count"] += 1
        return {"risk_level": "low", "force_live_refresh": force_live_refresh}

    monkeypatch.setattr(trading_runtime, "_build_effective_risk_report", fake_risk_report)
    monkeypatch.setattr(trading_runtime.order_manager, "get_stats", lambda: {"count": 1})
    monkeypatch.setattr(trading_runtime.position_manager, "get_stats", lambda: {"count": 2})
    monkeypatch.setattr(trading_runtime.execution_engine, "get_trading_mode", lambda: "paper")

    app = _build_app(("/api/trading", trading_runtime.router))
    client = TestClient(app)

    first = client.get("/api/trading/stats")
    second = client.get("/api/trading/stats")
    forced = client.get("/api/trading/stats?force_refresh=true")

    assert first.status_code == 200
    assert second.status_code == 200
    assert forced.status_code == 200
    assert counter["count"] == 2
    assert first.json()["risk"]["risk_level"] == "low"
    assert second.json()["positions"]["count"] == 2
    assert forced.json()["risk"]["force_live_refresh"] is False

    trading_runtime.invalidate_trading_stats_cache()


def test_ai_agent_write_route_requires_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = _build_app(("/api/ai", ai_agent.router))
    client = TestClient(app)

    update_mock = AsyncMock(return_value={"updated": True, "config": {"enabled": True}})
    monkeypatch.setattr(ai_agent.ai_research_module, "update_ai_autonomous_agent_runtime_config", update_mock)

    payload = {"enabled": True, "mode": "shadow", "provider": "glm"}
    response = client.post("/api/ai/runtime-config/autonomous-agent", json=payload)
    assert response.status_code == 401

    response = client.post("/api/ai/runtime-config/autonomous-agent", json=payload, headers=_ops_headers())
    assert response.status_code == 200
    assert response.json()["updated"] is True
    assert update_mock.await_count == 1


def test_notifications_write_route_requires_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = _build_app(("/api/notifications", notifications.router))
    client = TestClient(app)

    add_rule_mock = AsyncMock(return_value={"id": "rule-1", "name": "high-risk"})
    monkeypatch.setattr(notifications.notification_manager, "add_rule", add_rule_mock)

    payload = {"name": "high-risk", "rule_type": "equity", "params": {"threshold": 1}}
    response = client.post("/api/notifications/rules", json=payload)
    assert response.status_code == 401

    response = client.post("/api/notifications/rules", json=payload, headers=_ops_headers())
    assert response.status_code == 200
    assert response.json()["rule"]["id"] == "rule-1"
    assert add_rule_mock.await_count == 1


def test_order_and_position_mutations_require_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = _build_app(
        ("/api/trading", trading_orders.router),
        ("/api/trading", trading_positions.router),
    )
    client = TestClient(app)

    cancel_all_mock = AsyncMock(return_value={"success": True, "cancelled": 3})
    close_position_mock = AsyncMock(return_value={"ok": True, "symbol": "BTCUSDT", "exchange": "binance", "side": "long"})
    monkeypatch.setattr(trading_api, "cancel_all_orders", cancel_all_mock)
    monkeypatch.setattr(trading_api, "close_position", close_position_mock)

    response = client.delete("/api/trading/orders?exchange=binance")
    assert response.status_code == 401
    response = client.delete("/api/trading/orders?exchange=binance", headers=_ops_headers())
    assert response.status_code == 200
    assert response.json()["cancelled"] == 3

    response = client.post("/api/trading/positions/close", json={"exchange": "binance", "symbol": "BTCUSDT", "side": "long"})
    assert response.status_code == 401
    response = client.post(
        "/api/trading/positions/close",
        json={"exchange": "binance", "symbol": "BTCUSDT", "side": "long"},
        headers=_ops_headers(),
    )
    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert cancel_all_mock.await_count == 1
    assert close_position_mock.await_count == 1


def test_api_key_role_must_have_manage_orders_permission(monkeypatch):
    app = _build_app(("/api/trading", trading_orders.router))
    client = TestClient(app)

    cancel_all_mock = AsyncMock(return_value={"success": True, "cancelled": 2})
    monkeypatch.setattr(trading_api, "cancel_all_orders", cancel_all_mock)

    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(return_value=_api_identity("AUDITOR")),
    )
    response = client.delete("/api/trading/orders?exchange=binance", headers={"X-API-KEY": "auditor-key"})
    assert response.status_code == 403
    assert cancel_all_mock.await_count == 0

    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(return_value=_api_identity("OPERATOR")),
    )
    response = client.delete("/api/trading/orders?exchange=binance", headers={"X-API-KEY": "operator-key"})
    assert response.status_code == 200
    assert response.json()["cancelled"] == 2
    assert cancel_all_mock.await_count == 1


def test_live_mode_confirm_requires_approve_live_permission_for_api_key(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    trading_runtime.invalidate_trading_stats_cache()
    app = _build_app(("/api/trading", trading_runtime.router))
    client = TestClient(app)

    monkeypatch.setattr(
        trading_runtime,
        "list_pending_mode_switches",
        lambda include_token=False: (
            [
                {
                    "token": "live-token",
                    "target_mode": "live",
                    "reason": "verify",
                    "created_at": "2026-04-11T00:00:00+00:00",
                    "expires_at": "2026-04-11T00:05:00+00:00",
                }
            ]
            if include_token
            else []
        ),
    )
    switch_mock = AsyncMock(return_value={"success": True, "mode": "live"})
    monkeypatch.setattr(trading_runtime, "switch_trading_mode_service", switch_mock)

    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(return_value=_api_identity("OPERATOR")),
    )
    response = client.post(
        "/api/trading/mode/confirm",
        json={"token": "live-token", "confirm_text": "CONFIRM LIVE TRADING"},
        headers={"X-API-KEY": "operator-key"},
    )
    assert response.status_code == 403
    assert switch_mock.await_count == 0

    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(return_value=_api_identity("RISK_OWNER")),
    )
    response = client.post(
        "/api/trading/mode/confirm",
        json={"token": "live-token", "confirm_text": "CONFIRM LIVE TRADING"},
        headers={"X-API-KEY": "risk-owner-key"},
    )
    assert response.status_code == 200
    assert response.json()["mode"] == "live"
    assert switch_mock.await_count == 1
