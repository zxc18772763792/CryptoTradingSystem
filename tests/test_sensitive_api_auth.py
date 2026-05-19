from __future__ import annotations

import asyncio
import ast
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from core.governance.rbac import GovernanceIdentity, has_permission
from core.ops.service import auth as ops_auth_module
from web.api import (
    ai_agent,
    ai_research,
    altcoin,
    auth as web_auth,
    data,
    ml,
    news,
    notifications,
    strategies,
    trading as trading_api,
    trading_analytics,
    trading_accounts,
    trading_orders,
    trading_positions,
    trading_runtime,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


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


def test_loopback_ui_cookie_uses_settings_ops_token_when_env_missing(monkeypatch):
    monkeypatch.delenv("OPS_TOKEN", raising=False)
    monkeypatch.setattr(ops_auth_module.settings, "OPS_TOKEN", "test-token")
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
        lambda **kwargs: {"success": True, "token": "tok-settings", "target_mode": kwargs.get("target_mode")},
    )

    client = TestClient(app, base_url="http://127.0.0.1:8000")
    assert client.get("/").status_code == 200
    assert web_auth._LOCAL_UI_COOKIE_NAME in client.cookies

    response = client.post("/api/trading/mode/request", json={"target_mode": "paper", "reason": "settings-token"})
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


@pytest.mark.parametrize(
    ("role", "permission"),
    [
        ("RESEARCH_LEAD", "manage_ai_research"),
        ("RESEARCH_LEAD", "manage_ml"),
        ("RESEARCH_LEAD", "manage_strategies"),
        ("RISK_OWNER", "manage_accounts"),
        ("RISK_OWNER", "manage_strategies"),
        ("OPERATOR", "manage_accounts"),
        ("OPERATOR", "manage_ai_research"),
        ("OPERATOR", "manage_data_sources"),
        ("OPERATOR", "manage_ml"),
        ("OPERATOR", "manage_news"),
        ("OPERATOR", "manage_strategies"),
        ("ENGINEER", "manage_data_sources"),
        ("ENGINEER", "manage_ml"),
        ("ENGINEER", "manage_news"),
    ],
)
def test_role_matrix_covers_new_sensitive_mutation_surfaces(role: str, permission: str):
    assert has_permission(role, permission)


def test_auditor_cannot_mutate_new_sensitive_surfaces():
    for permission in (
        "manage_accounts",
        "manage_ai_research",
        "manage_data_sources",
        "manage_ml",
        "manage_news",
        "manage_strategies",
    ):
        assert not has_permission("AUDITOR", permission)


def test_new_sensitive_mutation_routes_require_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = _build_app(
        ("/api/trading", trading_accounts.router),
        ("/api/strategies", strategies.router),
        ("/api/ai", ai_research.router),
        ("/api/altcoin", altcoin.router),
        ("/api/ml", ml.router),
        ("/api/data", data.router),
        ("/api/news", news.router),
        ("/api/trading", trading_analytics.router),
    )
    client = TestClient(app)

    checks = [
        ("delete", "/api/trading/accounts/demo", {}),
        ("post", "/api/strategies/start-all", {}),
        ("post", "/api/ai/runtime-config/live-decision", {"json": {"enabled": True}}),
        ("post", "/api/altcoin/radar/watchlist", {"json": {"symbol": "WIF/USDT"}}),
        ("delete", "/api/ml/models/demo-model", {}),
        ("post", "/api/data/reconnect?exchange=binance", {}),
        ("post", "/api/news/engine/start", {}),
        ("post", "/api/trading/analytics/history/collect", {}),
    ]
    for method, path, kwargs in checks:
        response = getattr(client, method)(path, **kwargs)
        assert response.status_code == 401, path


@pytest.mark.parametrize(
    ("router_name", "router"),
    [
        ("trading_accounts", trading_accounts.router),
        ("trading_analytics", trading_analytics.router),
        ("strategies", strategies.router),
        ("ai_research", ai_research.router),
        ("altcoin", altcoin.router),
        ("ml", ml.router),
        ("data", data.router),
        ("news", news.router),
    ],
)
def test_sensitive_router_mutations_are_route_dependency_gated(router_name: str, router):
    missing = []
    for route in router.routes:
        if not isinstance(route, APIRoute):
            continue
        if not (set(route.methods or set()) & {"POST", "PUT", "DELETE"}):
            continue
        if not route.dependencies:
            missing.append(f"{sorted(route.methods)} {route.path}")

    assert missing == [], f"{router_name} mutation routes missing sensitive dependencies: {missing}"


def test_remaining_unguarded_post_routes_are_explicit_query_operations():
    allowed = {
        ("web/api/backtest.py", "post", "'/run'", "run_backtest"),
        ("web/api/backtest.py", "post", "'/compare'", "compare_backtests"),
        ("web/api/backtest.py", "post", "'/run_custom'", "run_backtest_custom"),
        ("web/api/backtest.py", "post", "'/optimize'", "optimize_backtest"),
        ("web/api/research.py", "post", "'/workbench/overview'", "run_research_workbench_overview"),
        ("web/api/research.py", "post", "'/workbench/modules/{module_name}'", "run_research_workbench_module"),
        ("web/api/research.py", "post", "'/workbench/recommendations'", "get_research_workbench_recommendations"),
    }
    unguarded = set()
    for path in (REPO_ROOT / "web" / "api").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for dec in node.decorator_list:
                call = dec if isinstance(dec, ast.Call) else None
                if not call or not isinstance(call.func, ast.Attribute):
                    continue
                method = call.func.attr
                if method not in {"post", "put", "delete"}:
                    continue
                if any(kw.arg == "dependencies" for kw in call.keywords):
                    continue
                route = ast.unparse(call.args[0]) if call.args else "''"
                rel_path = path.relative_to(REPO_ROOT).as_posix()
                unguarded.add((rel_path, method, route, node.name))

    assert unguarded == allowed


def test_account_mutation_api_key_must_have_manage_accounts_permission(monkeypatch):
    app = _build_app(("/api/trading", trading_accounts.router))
    client = TestClient(app)
    monkeypatch.setattr(trading_api.account_manager, "delete_account", lambda account_id: True)

    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(return_value=_api_identity("AUDITOR")),
    )
    response = client.delete("/api/trading/accounts/demo", headers={"X-API-KEY": "auditor-key"})
    assert response.status_code == 403

    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(return_value=_api_identity("OPERATOR")),
    )
    response = client.delete("/api/trading/accounts/demo", headers={"X-API-KEY": "operator-key"})
    assert response.status_code == 200
    assert response.json()["success"] is True


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
