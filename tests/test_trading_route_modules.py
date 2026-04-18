from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

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


def test_get_community_overview_reports_security_alert_source_truthfully(monkeypatch):
    monkeypatch.setattr(
        trading_api,
        "_fetch_trade_imbalance",
        AsyncMock(return_value={"imbalance": 0.12, "buy_volume": 12.0, "sell_volume": 8.0}),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_whale_transfers",
        AsyncMock(return_value={"available": True, "count": 1, "transactions": [{"btc": 120.0}]}),
    )
    monkeypatch.setattr(
        trading_api,
        "_fetch_binance_announcements",
        AsyncMock(return_value=[{"title": "Listing update"}]),
    )

    payload = asyncio.run(trading_api.get_community_overview(symbol="BTC/USDT", exchange="binance"))

    assert payload["security_alerts"]["available"] is False
    assert payload["security_alerts"]["source"] == "unavailable"
    assert payload["security_alerts"]["events"] == []
    assert "占位" in payload["security_alerts"]["note"]


def test_analytics_fallback_community_does_not_fabricate_security_events():
    payload = trading_api._analytics_fallback_community("binance", "BTC/USDT", "collector offline")

    assert payload["security_alerts"]["available"] is False
    assert payload["security_alerts"]["source"] == "unavailable"
    assert payload["security_alerts"]["events"] == []
    assert "collector offline" in payload["source_error"]
