from __future__ import annotations

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
