"""Isolated API regressions; no network, credentials, or persisted state."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.ops.service.auth import OpsAuthContext
from core.notifications.notification_manager import AlertRule, NotificationManager
from web.api import auth, notifications, trading_analytics, trading_runtime


@pytest.fixture
def api(monkeypatch):
    role = ["OPERATOR"]

    async def identify(request):
        ctx = OpsAuthContext(actor="synthetic-api-audit", role=role[0],
                             token_present=False, api_key_present=True, client_ip="testclient")
        request.state.ops_auth = ctx
        return ctx

    monkeypatch.setattr(auth, "require_ops_auth", identify)
    app = FastAPI()
    app.include_router(trading_analytics.router, prefix="/api/trading")
    app.include_router(trading_runtime.router, prefix="/api/trading")
    app.include_router(notifications.router, prefix="/api/notifications")
    return TestClient(app), role


@pytest.mark.parametrize("field", ["max_position_size", "max_daily_loss_ratio", "max_daily_loss_usd",
                                    "max_leverage", "balance_volatility_alert_pct"])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_risk_params_returns_client_error_for_nonfinite_limits(api, monkeypatch, field, value):
    from core.risk.risk_manager import RiskManager
    client, identity = api
    identity[0] = "RISK_OWNER"
    # Invalid updates reject before any state/lock/persistence is touched.
    # Exercise the real validation without constructing a live manager.
    manager = RiskManager.__new__(RiskManager)
    manager.max_leverage = 2.0
    monkeypatch.setattr(trading_runtime, "risk_manager", manager)
    report = AsyncMock(return_value={})
    monkeypatch.setattr(trading_runtime, "_build_effective_risk_report", report)
    response = client.post("/api/trading/risk/params", json={"max_leverage": 3, field: value})
    assert response.status_code == 400
    assert "finite number" in response.json()["detail"]
    assert manager.max_leverage == 2.0
    report.assert_not_awaited()


@pytest.mark.parametrize("role,status", [
    ("AUDITOR", 403), ("ENGINEER", 403), ("OPERATOR", 403),
    ("RESEARCH_LEAD", 403), ("RISK_OWNER", 200), ("SYSTEM", 200),
])
def test_stoploss_policy_requires_risk_approval(api, monkeypatch, role, status):
    client, identity = api
    identity[0] = role
    update = AsyncMock(return_value={"success": True, "policy": {}})
    monkeypatch.setattr(trading_analytics.trading_api, "update_stoploss_policy", update)
    response = client.post("/api/trading/analytics/stoploss/policy", json={"policy": {"enabled": False}})
    assert response.status_code == status
    assert update.await_count == int(status == 200)


@pytest.fixture
def notification_context(monkeypatch):
    manager = SimpleNamespace(list_rules=AsyncMock(return_value=[]),
                              evaluate_rules=AsyncMock(return_value={"triggered_count": 0}))
    monkeypatch.setattr(notifications, "notification_manager", manager)
    monkeypatch.setattr(notifications, "_load_prices", AsyncMock(return_value={}))
    monkeypatch.setattr(notifications, "risk_manager", SimpleNamespace(
        get_risk_report=lambda: {"equity": {"current": 5000.0}}))
    monkeypatch.setattr(notifications, "strategy_manager", SimpleNamespace(get_dashboard_summary=lambda **_: {}))
    monkeypatch.setattr(notifications, "position_manager", SimpleNamespace(get_position_count=lambda: 0))
    monkeypatch.setattr(notifications, "exchange_manager", SimpleNamespace(get_connected_exchanges=lambda: []))
    monkeypatch.setattr(notifications, "build_altcoin_notification_context", AsyncMock(return_value={}))
    return manager


@pytest.mark.parametrize("payload,expected", [({}, 5000.0), ({"total_usd": None}, 5000.0),
                                               ({"total_usd": 0}, 0.0), ({"total_usd": -10}, -10.0)])
def test_notification_equity_only_defaults_when_omitted(api, notification_context, payload, expected):
    client, _ = api
    response = client.post("/api/notifications/evaluate", json=payload)
    assert response.status_code == 200
    assert response.json()["context"]["total_usd"] == expected
    context = notification_context.evaluate_rules.await_args.args[0]
    assert context["total_usd"] == expected
    equity_rule = AlertRule("synthetic-equity", "Low equity", "equity_below", {"threshold_usd": 100})
    assert bool(NotificationManager()._eval_rule(equity_rule, context)) == (expected <= 100)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_notification_equity_rejects_nonfinite_before_evaluation(api, notification_context, value):
    client, _ = api
    response = client.post("/api/notifications/evaluate", json={"total_usd": value})
    assert response.status_code == 422
    notification_context.evaluate_rules.assert_not_awaited()


@pytest.mark.parametrize("previous_count", [0, 10, 999, 1000])
def test_notification_test_returns_new_events_after_buffer_rollover(api, monkeypatch, previous_count):
    client, _ = api
    manager = NotificationManager()
    for index in range(previous_count):
        manager._record_event("telegram", "success", f"old-{index}", "synthetic")

    async def send_message(**kwargs):
        for channel in ("telegram", "feishu"):
            manager._record_event(channel, "success", kwargs["title"], kwargs["message"])
        return {"telegram": True, "feishu": True}

    monkeypatch.setattr(manager, "send_message", send_message)
    monkeypatch.setattr(manager, "channel_status", lambda: {})
    monkeypatch.setattr(notifications, "notification_manager", manager)
    response = client.post("/api/notifications/test", json={"title": "new-test", "message": "synthetic"})
    assert response.status_code == 200
    events = response.json()["events"]
    assert len(events) == 2
    assert [event["channel"] for event in events] == ["telegram", "feishu"]
    assert all(event["title"] == "new-test" for event in events)
