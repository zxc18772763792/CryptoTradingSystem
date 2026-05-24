from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import trading as trading_api
from web.api import trading_balances


def test_balance_history_route_uses_resolved_mode(monkeypatch):
    app = FastAPI()
    app.include_router(trading_balances.router, prefix="/api/trading")
    client = TestClient(app)

    captured = {}

    async def fake_get_history(*, hours, exchange, limit, mode):
        captured["hours"] = hours
        captured["exchange"] = exchange
        captured["limit"] = limit
        captured["mode"] = mode
        return [{"timestamp": "2026-03-08T00:00:00Z", "total_usd": 1234.5}]

    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: True)
    monkeypatch.setattr(trading_api.account_snapshot_manager, "get_history", fake_get_history)

    response = client.get("/api/trading/balances/history?hours=48&exchange=binance&limit=10&mode=invalid")
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "paper"
    assert payload["points"] == 1
    assert captured == {
        "hours": 48,
        "exchange": "binance",
        "limit": 10,
        "mode": "paper",
    }


def test_all_balances_payload_does_not_switch_risk_scope(monkeypatch):
    calls = []

    monkeypatch.setattr(trading_api.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: False)
    monkeypatch.setattr(trading_api.risk_manager, "get_account_scope", lambda: "paper")
    monkeypatch.setattr(
        trading_api.risk_manager,
        "set_account_scope",
        lambda *args, **kwargs: calls.append(("set_scope", args, kwargs)),
    )
    monkeypatch.setattr(
        trading_api.risk_manager,
        "get_risk_report",
        lambda *args, **kwargs: {
            "scope": kwargs.get("scope") or "paper",
            "equity": {"current": 1000.0},
            "risk_level": "low",
            "trading_halted": False,
        },
    )
    monkeypatch.setattr(
        trading_api.risk_manager,
        "update_equity",
        lambda *args, **kwargs: calls.append(("update_equity", args, kwargs)),
    )
    monkeypatch.setattr(trading_api.exchange_manager, "get_connected_exchanges", lambda: [])
    monkeypatch.setattr(
        trading_api.account_snapshot_manager,
        "record_snapshot",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        trading_balances,
        "_evaluate_notifications_for_balance_payload",
        AsyncMock(return_value={"triggered_count": 0}),
    )
    monkeypatch.setattr(
        trading_api,
        "_collect_live_position_snapshot",
        AsyncMock(return_value={"unrealized_pnl_usd": 0.0, "position_count": 0}),
    )
    monkeypatch.setattr(
        trading_api,
        "_resolve_live_equity_baseline",
        AsyncMock(return_value={"portfolio_total_usd": 0.0}),
    )
    monkeypatch.setattr(
        trading_api,
        "_resolve_live_daily_realized_pnl",
        AsyncMock(return_value={"pnl": 0.0, "source": "unit"}),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_balances_via_fallback",
        AsyncMock(return_value=[]),
    )
    trading_api._BALANCE_SNAPSHOT_CACHE.clear()

    class _Connector:
        is_connected = False

        async def get_balance(self):
            raise RuntimeError("offline")

    monkeypatch.setattr(
        trading_api.exchange_manager,
        "get_exchange",
        lambda exchange: None if exchange != "binance" else _Connector(),
    )

    payload = asyncio.run(trading_balances._build_all_balances_payload())

    assert payload["mode"] == "live"
    assert payload["risk_scope"] == "paper"
    assert [call[0] for call in calls if call[0] == "set_scope"] == []
    equity_calls = [call for call in calls if call[0] == "update_equity"]
    assert equity_calls
    assert equity_calls[-1][2]["scope"] == "live"
