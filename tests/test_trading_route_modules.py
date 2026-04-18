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


def test_analytics_history_status_route_bridges_to_service(monkeypatch):
    app = FastAPI()
    app.include_router(trading_analytics.router, prefix="/api/trading")
    client = TestClient(app)

    async def fake_status(*, exchange, symbol):
        return {
            "exchange": exchange,
            "symbol": symbol,
            "collectors": [{"collector": "derivatives", "status": "ok"}],
        }

    monkeypatch.setattr(trading_api, "get_analytics_history_status", fake_status)

    response = client.get("/api/trading/analytics/history/status?exchange=okx&symbol=ETH/USDT")
    assert response.status_code == 200
    assert response.json() == {
        "exchange": "okx",
        "symbol": "ETH/USDT",
        "collectors": [{"collector": "derivatives", "status": "ok"}],
    }


def test_analytics_history_status_includes_derivatives_coinglass_surface(monkeypatch):
    trading_api._ANALYTICS_HISTORY_STATUS_CACHE.clear()
    trading_api._ANALYTICS_HISTORY_STATUS_LAST.clear()

    monkeypatch.setattr(
        trading_api,
        "_load_analytics_ingest_status_map",
        AsyncMock(
            return_value={
                "microstructure": {
                    "collector": "microstructure",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "ok",
                    "rows_written": 12,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "exchange_public"},
                },
                "community": {
                    "collector": "community",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "ok",
                    "rows_written": 8,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "proxy_layer"},
                },
                "whales": {
                    "collector": "whales",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "degraded",
                    "rows_written": 2,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "public_chain_proxy"},
                },
            }
        ),
    )
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.build_coinglass_overview_payload",
        AsyncMock(
            return_value={
                "symbol": "BTC/USDT",
                "available": True,
                "freshness_sec": 120.0,
                "degraded_reason": None,
                "quota_headroom": {"minute_remaining": 8, "daily_remaining": 49900, "monthly_remaining": 499000},
                "active_datasets": ["funding_rate_exchange_list", "open_interest_exchange_list"],
                "status": [
                    {
                        "dataset": "open_interest_exchange_list",
                        "status": "ok",
                        "last_success_at": "2026-04-18T10:02:00Z",
                        "updated_at": "2026-04-18T10:02:05Z",
                    }
                ],
                "snapshot": {"timestamp": "2026-04-18T10:02:00Z"},
                "cached": True,
                "refreshing": False,
                "key_configured": True,
            }
        ),
    )

    payload = asyncio.run(trading_api.get_analytics_history_status(exchange="binance", symbol="BTC/USDT"))
    collectors = {item["collector"]: item for item in payload["collectors"]}

    assert {"microstructure", "community", "whales", "derivatives"} <= set(collectors)
    assert payload["derivatives"]["collector"] == "derivatives"
    assert collectors["derivatives"]["status"] == "ok"
    assert collectors["derivatives"]["available"] is True
    assert collectors["derivatives"]["details"]["provider"] == "coinglass"
    assert collectors["derivatives"]["details"]["freshness_sec"] == 120.0
    assert collectors["derivatives"]["details"]["quota_headroom"]["daily_remaining"] == 49900
    assert collectors["derivatives"]["details"]["snapshot"]["timestamp"] == "2026-04-18T10:02:00Z"


def test_analytics_history_health_includes_derivatives_coinglass_dataset(monkeypatch):
    trading_api._ANALYTICS_HISTORY_HEALTH_CACHE.clear()
    trading_api._ANALYTICS_HISTORY_STATUS_CACHE.clear()
    trading_api._ANALYTICS_HISTORY_STATUS_LAST.clear()

    monkeypatch.setattr(
        trading_api,
        "_load_analytics_ingest_status_map",
        AsyncMock(
            return_value={
                "microstructure": {
                    "collector": "microstructure",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "ok",
                    "rows_written": 12,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "exchange_public", "summary": {"spread_bps": 2.4}},
                },
                "community": {
                    "collector": "community",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "ok",
                    "rows_written": 8,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "proxy_layer", "summary": {"buy_ratio": 0.57}},
                },
                "whales": {
                    "collector": "whales",
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "status": "degraded",
                    "rows_written": 2,
                    "finished_at": "2026-04-18T10:00:00Z",
                    "updated_at": "2026-04-18T10:00:01Z",
                    "details": {"source_name": "public_chain_proxy", "summary": {"whale_count": 2}},
                },
            }
        ),
    )
    monkeypatch.setattr(
        "core.data.coinglass_feature_builder.build_coinglass_overview_payload",
        AsyncMock(
            return_value={
                "symbol": "BTC/USDT",
                "available": True,
                "freshness_sec": 120.0,
                "degraded_reason": None,
                "quota_headroom": {"minute_remaining": 8, "daily_remaining": 49900, "monthly_remaining": 499000},
                "active_datasets": ["funding_rate_exchange_list", "open_interest_exchange_list"],
                "status": [
                    {
                        "dataset": "open_interest_exchange_list",
                        "status": "ok",
                        "last_success_at": "2026-04-18T10:02:00Z",
                        "updated_at": "2026-04-18T10:02:05Z",
                    }
                ],
                "snapshot": {
                    "timestamp": "2026-04-18T10:02:00Z",
                    "funding_rate": 0.0008,
                    "basis_pct": 0.013,
                    "long_short_ratio": 1.24,
                    "oi_change_1h": 3.8,
                    "oi_change_24h": 12.1,
                    "crowding_score": 0.71,
                    "squeeze_score": 0.43,
                    "distribution_score": 0.28,
                    "taker_buy_sell_imbalance": 0.19,
                    "orderbook_imbalance_score": 0.16,
                    "depth_thinness_score": 0.22,
                },
                "cached": True,
                "refreshing": False,
                "key_configured": True,
            }
        ),
    )

    payload = asyncio.run(
        trading_api.get_analytics_history_health(exchange="binance", symbol="BTC/USDT", hours=48, refresh=False)
    )
    datasets = {item["key"]: item for item in payload["datasets"]}
    sources = {item["stored_as"]: item for item in payload["sources"]}

    assert payload["status"]["derivatives"]["status"] == "ok"
    assert "analytics_derivatives_snapshots" in payload["storage"]["tables"]
    assert "analytics_market_structure_snapshots" in payload["storage"]["tables"]
    assert "analytics_derivatives_snapshots" in sources
    assert datasets["derivatives"]["latest_summary"]["funding_rate"] == 0.0008
    assert datasets["derivatives"]["latest_summary"]["crowding_score"] == 0.71
    assert payload["recent"]["derivatives"][0]["crowding_score"] == 0.71
    assert payload["summary"]["dataset_count"] == 4
    assert payload["summary"]["ready_datasets"] == 4


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
