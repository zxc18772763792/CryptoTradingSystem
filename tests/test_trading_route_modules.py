from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import trading as trading_api
from web.api import trading_analytics, trading_orders, trading_positions


def test_orders_route_bridges_to_service(monkeypatch):
    app = FastAPI()
    app.include_router(trading_orders.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_get_orders(*, symbol=None, exchange=None, include_history=True, limit=100):
        return {
            "orders": [{"id": "o-1"}],
            "symbol": symbol,
            "exchange": exchange,
            "include_history": include_history,
            "limit": limit,
        }

    monkeypatch.setattr(trading_api, "get_orders", fake_get_orders)

    response = client.get("/api/trading/orders?symbol=BTCUSDT&exchange=binance&include_history=false&limit=5")
    assert response.status_code == 200
    payload = response.json()
    assert payload["orders"] == [{"id": "o-1"}]
    assert payload["symbol"] == "BTCUSDT"
    assert payload["exchange"] == "binance"
    assert payload["include_history"] is False
    assert payload["limit"] == 5


def test_positions_close_route_bridges_to_service(monkeypatch):
    monkeypatch.setenv("OPS_TOKEN", "test-token")
    app = FastAPI()
    app.include_router(trading_positions.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_close_position(req):
        return {"ok": True, "symbol": req.symbol, "exchange": req.exchange, "side": req.side}

    monkeypatch.setattr(trading_api, "close_position", fake_close_position)

    response = client.post(
        "/api/trading/positions/close",
        json={"exchange": "binance", "symbol": "BTCUSDT", "side": "long"},
        headers={"X-OPS-TOKEN": "test-token", "X-OPS-CALLER": "pytest"},
    )
    assert response.status_code == 200
    assert response.json() == {"ok": True, "symbol": "BTCUSDT", "exchange": "binance", "side": "long"}


def test_analytics_overview_route_bridges_to_service(monkeypatch):
    app = FastAPI()
    app.include_router(trading_analytics.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_overview(*, days, lookback, calendar_days, exchange, symbol):
        return {
            "days": days,
            "lookback": lookback,
            "calendar_days": calendar_days,
            "exchange": exchange,
            "symbol": symbol,
            "all_ok": True,
        }

    monkeypatch.setattr(trading_api, "get_analytics_overview", fake_overview)

    response = client.get(
        "/api/trading/analytics/overview?days=30&lookback=120&calendar_days=10&exchange=okx&symbol=ETH/USDT"
    )
    assert response.status_code == 200
    assert response.json() == {
        "days": 30,
        "lookback": 120,
        "calendar_days": 10,
        "exchange": "okx",
        "symbol": "ETH/USDT",
        "all_ok": True,
    }
