from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient


def test_coinglass_overview_route_uses_shared_payload_builder(monkeypatch):
    from web.api import data as data_api

    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    client = TestClient(app)

    async def fake_builder(symbol: str, refresh: bool = False, manual: bool = False):
        return {
            "symbol": symbol,
            "available": True,
            "freshness_sec": 42.0,
            "degraded_reason": None,
            "quota_headroom": {"minute_remaining": 8, "daily_remaining": 49900, "monthly_remaining": 499000},
            "active_datasets": ["derivatives"],
            "status": [{"dataset": "derivatives", "status": "ok"}],
            "snapshot": {"symbol": symbol},
            "generated_at": "2026-04-18T12:00:00+00:00",
            "cached": True,
            "refreshing": False,
            "key_configured": True,
            "echo": {"refresh": refresh, "manual": manual},
        }

    monkeypatch.setattr(data_api, "build_coinglass_overview_payload", fake_builder)

    response = client.get("/api/data/coinglass/overview?symbol=BTC/USDT&refresh=true&manual=true")
    assert response.status_code == 200
    payload = response.json()

    assert payload["symbol"] == "BTC/USDT"
    assert payload["available"] is True
    assert payload["quota_headroom"]["daily_remaining"] == 49900
    assert payload["echo"] == {"refresh": True, "manual": True}


def test_derivatives_overview_route_is_alias_of_coinglass_overview(monkeypatch):
    from web.api import data as data_api

    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    client = TestClient(app)

    async def fake_builder(symbol: str, refresh: bool = False, manual: bool = False):
        return {
            "symbol": symbol,
            "available": False,
            "freshness_sec": None,
            "degraded_reason": "coinglass_cache_empty",
            "quota_headroom": {"minute_remaining": 10, "daily_remaining": 50000, "monthly_remaining": 500000},
            "active_datasets": [],
            "status": [],
            "snapshot": None,
            "generated_at": "2026-04-18T12:00:00+00:00",
            "cached": False,
            "refreshing": False,
            "key_configured": False,
        }

    monkeypatch.setattr(data_api, "build_coinglass_overview_payload", fake_builder)

    response = client.get("/api/data/derivatives/overview?symbol=ETH/USDT")
    assert response.status_code == 200
    payload = response.json()

    assert payload["symbol"] == "ETH/USDT"
    assert payload["degraded_reason"] == "coinglass_cache_empty"
    assert payload["active_datasets"] == []
