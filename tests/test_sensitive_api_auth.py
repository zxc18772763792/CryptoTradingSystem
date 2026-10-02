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
    backtest,
    data,
    ml,
    news,
    notifications,
    strategies,
    trading as trading_api,
    trading_analytics,
    trading_accounts,
    trading_balances,
    trading_orders,
    trading_positions,
    trading_runtime,
)
from web import main as web_main


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
    monkeypatch.setenv("OPS_TOKEN", "test-token")
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

    first = client.get("/api/trading/stats", headers=_ops_headers())
    second = client.get("/api/trading/stats", headers=_ops_headers())
    forced = client.get("/api/trading/stats?force_refresh=true", headers=_ops_headers())

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


def test_ai_agent_status_read_requires_ops_auth_and_is_passive_by_default(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = _build_app(("/api/ai", ai_agent.router))
    client = TestClient(app)

    status_mock = AsyncMock(return_value={"status": {"running": False}, "config": {}})
    monkeypatch.setattr(ai_agent.ai_research_module, "get_ai_autonomous_agent_status", status_mock)

    response = client.get("/api/ai/autonomous-agent/status")
    assert response.status_code == 401
    assert status_mock.await_count == 0

    response = client.get("/api/ai/autonomous-agent/status", headers=_ops_headers())
    assert response.status_code == 200
    assert status_mock.await_count == 1
    assert status_mock.await_args.kwargs["warm_preview"] is False


def test_ml_and_notification_reads_require_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = _build_app(
        ("/api/ml", ml.router),
        ("/api/notifications", notifications.router),
    )
    client = TestClient(app)

    response = client.get("/api/ml/features")
    assert response.status_code == 401
    response = client.get("/api/ml/features", headers=_ops_headers())
    assert response.status_code == 200

    response = client.get("/api/notifications/channels")
    assert response.status_code == 401
    response = client.get("/api/notifications/channels", headers=_ops_headers())
    assert response.status_code == 200


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
        ("/api/backtest", backtest.router),
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
        ("post", "/api/backtest/run", {}),
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
        ("backtest", backtest.router),
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


def test_backtest_sensitive_routes_are_dependency_gated():
    expected = {
        ("POST", "/run"),
        ("POST", "/compare"),
        ("POST", "/run_custom"),
        ("POST", "/optimize"),
        ("GET", "/export"),
    }
    gated = set()
    for route in backtest.router.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.dependencies:
            for method in route.methods or set():
                gated.add((method, route.path))

    assert expected <= gated


def test_ai_research_decay_post_and_param_sensitivity_are_dependency_gated():
    expected = {
        ("GET", "/candidates/{candidate_id}/param-sensitivity"),
        ("POST", "/candidates/{candidate_id}/decay-check"),
    }
    gated = set()
    for route in ai_research.router.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.dependencies:
            for method in route.methods or set():
                gated.add((method, route.path))

    assert expected <= gated


def test_ai_research_decay_get_is_read_only_and_post_persists(monkeypatch):
    calls = []

    def fake_build_payload(request, candidate_id, *, persist):
        calls.append((candidate_id, persist))
        return {"candidate_id": candidate_id, "persisted": persist}

    monkeypatch.setattr(ai_research, "_build_candidate_decay_payload", fake_build_payload)
    request = object()

    post_payload = asyncio.run(ai_research.post_candidate_decay_check(request, "cand-1"))
    get_payload = asyncio.run(ai_research.get_candidate_decay_check(request, "cand-1"))

    assert post_payload == {"candidate_id": "cand-1", "persisted": True}
    assert get_payload == {"candidate_id": "cand-1", "persisted": False}
    assert calls == [("cand-1", True), ("cand-1", False)]


def test_strategy_export_routes_are_dependency_gated_and_sanitized():
    expected = {
        ("GET", "/export/{name}"),
        ("GET", "/export"),
    }
    gated = set()
    for route in strategies.router.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.dependencies:
            for method in route.methods or set():
                gated.add((method, route.path))

    assert expected <= gated

    payload = strategies._strategy_export_payload(
        {
            "name": "demo",
            "strategy_type": "MAStrategy",
            "params": {"period": 10, "api_key": "secret-value"},
            "metadata": {"nested": {"token": "tok", "safe": "ok"}},
        }
    )
    assert payload["params"]["api_key"] == "[REDACTED]"
    assert payload["metadata"]["nested"]["token"] == "[REDACTED]"
    assert payload["metadata"]["nested"]["safe"] == "ok"


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


def test_account_summary_requires_read_trading_state_permission(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-only-dummy-token")
    app = _build_app(("/api/trading", trading_accounts.router))
    client = TestClient(app)
    monkeypatch.setattr(trading_api.account_manager, "list_accounts", lambda: [])
    monkeypatch.setattr(trading_api.position_manager, "get_all_positions", lambda: [])
    monkeypatch.setattr(trading_api.order_manager, "get_recent_orders", lambda limit=1000: [])

    response = client.get("/api/trading/accounts/summary")
    assert response.status_code == 401

    monkeypatch.setattr(
        ops_auth_module,
        "resolve_api_key_identity",
        AsyncMock(return_value=_api_identity("AUDITOR")),
    )
    response = client.get(
        "/api/trading/accounts/summary",
        headers={"X-API-KEY": "auditor-key"},
    )
    assert response.status_code == 200
    assert response.json()["accounts"] == []


def test_sensitive_read_routes_are_dependency_gated():
    expected = {
        ("main", "GET", "/api/status"),
        ("main", "GET", "/api/market-data/status"),
        ("trading_balances", "GET", "/balances"),
        ("trading_balances", "GET", "/balances/history"),
        ("trading_analytics", "GET", "/analytics/overview"),
        ("trading_analytics", "GET", "/analytics/performance"),
        ("trading_analytics", "GET", "/analytics/risk-dashboard"),
        ("trading_analytics", "GET", "/analytics/calendar"),
        ("trading_analytics", "GET", "/analytics/microstructure"),
        ("trading_analytics", "GET", "/market_microstructure"),
        ("trading_analytics", "GET", "/analytics/behavior/report"),
        ("trading_analytics", "GET", "/analytics/stoploss/policy"),
        ("trading_analytics", "GET", "/analytics/equity/rebalance"),
        ("trading_analytics", "GET", "/analytics/community/overview"),
        ("trading_analytics", "GET", "/analytics/history/health"),
        ("trading_analytics", "GET", "/analytics/history/status"),
        ("trading_analytics", "GET", "/audit"),
        ("trading_analytics", "GET", "/analytics/live-trade-review"),
        ("trading_analytics", "GET", "/pnl/heatmap"),
        ("trading_runtime", "GET", "/risk/report"),
        ("trading_runtime", "GET", "/stats"),
        ("trading_runtime", "GET", "/mode"),
        ("trading_runtime", "GET", "/runtime/diagnostics"),
        ("strategies", "GET", "/list"),
        ("strategies", "GET", "/library"),
        ("strategies", "GET", "/audit"),
        ("strategies", "GET", "/summary"),
        ("strategies", "GET", "/runtime"),
        ("strategies", "GET", "/signals/aggregated"),
        ("strategies", "GET", "/health/monitor"),
        ("strategies", "GET", "/health"),
        ("strategies", "GET", "/health-monitor"),
        ("strategies", "GET", "/{name}"),
        ("strategies", "GET", "/{name}/params/schema"),
        ("strategies", "GET", "/{name}/sizing-preview"),
        ("strategies", "GET", "/{name}/live-vs-backtest"),
        ("strategies", "GET", "/{name}/signals"),
        ("strategies", "GET", "/{name}/monitor-data"),
        ("ai_agent", "GET", "/runtime-config/autonomous-agent"),
        ("ai_agent", "GET", "/autonomous-agent/risk-config"),
        ("ai_agent", "GET", "/autonomous-agent/status"),
        ("ai_agent", "GET", "/autonomous-agent/journal"),
        ("ai_agent", "GET", "/autonomous-agent/review"),
        ("ai_agent", "GET", "/autonomous-agent/scorecard"),
        ("ai_agent", "GET", "/autonomous-agent/risk-status"),
        ("ai_agent", "GET", "/autonomous-agent/symbol-ranking"),
        ("ai_agent", "GET", "/autonomous-agent/live-signals"),
        ("ml", "GET", "/diagnostics"),
        ("ml", "GET", "/features"),
        ("ml", "GET", "/models"),
        ("ml", "GET", "/jobs/{job_id}"),
        ("ml", "GET", "/jobs"),
        ("notifications", "GET", "/channels"),
        ("notifications", "GET", "/rules"),
        ("notifications", "GET", "/events"),
        ("data", "GET", "/research/refresh/status"),
    }
    routers = {
        "main": web_main.app.routes,
        "ai_agent": ai_agent.router.routes,
        "ml": ml.router.routes,
        "notifications": notifications.router.routes,
        "trading_analytics": trading_analytics.router.routes,
        "trading_balances": trading_balances.router.routes,
        "trading_runtime": trading_runtime.router.routes,
        "strategies": strategies.router.routes,
        "data": data.router.routes,
    }
    gated = set()
    for router_name, routes in routers.items():
        for route in routes:
            if not isinstance(route, APIRoute) or not route.dependencies:
                continue
            for method in route.methods or set():
                gated.add((router_name, method, route.path))

    assert expected <= gated


def test_ai_research_get_routes_require_read_permission():
    missing = []
    for route in ai_research.router.routes:
        if not isinstance(route, APIRoute) or "GET" not in set(route.methods or set()):
            continue
        if not route.dependencies:
            missing.append(route.path)

    assert missing == []


def test_ai_research_runtime_config_read_requires_ops_auth(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = _build_app(("/api/ai", ai_research.router))
    client = TestClient(app)

    response = client.get("/api/ai/runtime-config")

    assert response.status_code == 401


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
    monkeypatch.setattr(trading_runtime.audit_logger, "log", AsyncMock(return_value=None))

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


def test_loopback_ui_cookie_sliding_renewal(monkeypatch):
    """Regression: the 8h cookie hard-expired under an open dashboard, so every
    poll 401'ed and the UI showed "状态延迟". With sliding renewal, any request
    carrying a VALID cookie gets it re-issued (idle timeout semantics)."""
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    monkeypatch.setattr(web_auth, "_request_client_ip", lambda request: "127.0.0.1")

    app = _build_app()

    @app.middleware("http")
    async def sliding(request: Request, call_next):
        response = await call_next(request)
        web_auth.renew_local_ui_session_cookie(request, response)
        return response

    @app.get("/")
    async def index(request: Request):
        response = JSONResponse({"ok": True})
        web_auth.set_local_ui_session_cookie(request, response)
        return response

    @app.get("/api/anything")
    async def anything(request: Request):
        return JSONResponse({"ok": True})

    client = TestClient(app, base_url="http://127.0.0.1:8000")

    # No cookie yet -> API response must NOT issue one (renewal is not issuance).
    bare = client.get("/api/anything")
    assert web_auth._LOCAL_UI_COOKIE_NAME not in (bare.headers.get("set-cookie") or "")

    # Acquire the session cookie from the index page.
    client.get("/")
    assert web_auth._LOCAL_UI_COOKIE_NAME in client.cookies

    # Any subsequent request carrying the valid cookie gets it RE-ISSUED,
    # extending max-age (sliding renewal).
    renewed = client.get("/api/anything")
    set_cookie = renewed.headers.get("set-cookie") or ""
    assert web_auth._LOCAL_UI_COOKIE_NAME in set_cookie
    assert "Max-Age" in set_cookie or "max-age" in set_cookie


def test_loopback_ui_cookie_renewal_rejects_invalid_cookie(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    monkeypatch.setattr(web_auth, "_request_client_ip", lambda request: "127.0.0.1")

    app = _build_app()

    @app.middleware("http")
    async def sliding(request: Request, call_next):
        response = await call_next(request)
        web_auth.renew_local_ui_session_cookie(request, response)
        return response

    @app.get("/api/anything")
    async def anything(request: Request):
        return JSONResponse({"ok": True})

    client = TestClient(app, base_url="http://127.0.0.1:8000")
    client.cookies.set(web_auth._LOCAL_UI_COOKIE_NAME, "forged-value")
    response = client.get("/api/anything")
    assert web_auth._LOCAL_UI_COOKIE_NAME not in (response.headers.get("set-cookie") or "")
