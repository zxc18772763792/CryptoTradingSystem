from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import data as data_api


def test_research_symbols_prefers_coinglass_altcoin_universe(monkeypatch):
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    client = TestClient(app)

    async def fake_build_exchange_altcoin_universe(exchange: str, **kwargs):
        assert exchange == "binance"
        return {
            "exchange": "binance",
            "symbols": ["LINK/USDT", "AAVE/USDT", "TAO/USDT"],
            "count": 3,
            "source": "coinglass_altcoin_universe",
            "board_count": 2,
            "boards": [],
        }

    monkeypatch.setattr(data_api, "build_exchange_altcoin_universe", fake_build_exchange_altcoin_universe)

    response = client.get("/api/data/research/symbols?exchange=binance")
    assert response.status_code == 200
    payload = response.json()

    assert payload["source"] == "coinglass_altcoin_universe"
    assert payload["symbols"] == ["LINK/USDT", "AAVE/USDT", "TAO/USDT"]
    assert payload["default_count"] == 3


def test_research_symbols_falls_back_when_coinglass_universe_fails(monkeypatch):
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    client = TestClient(app)

    async def fake_build_exchange_altcoin_universe(exchange: str, **kwargs):
        raise RuntimeError("coinglass unavailable")

    async def fake_get_data_symbols(exchange: str = "binance"):
        return {
            "exchange": exchange,
            "symbols": ["BTC/USDT", "ETH/USDT"],
            "count": 2,
        }

    monkeypatch.setattr(data_api, "build_exchange_altcoin_universe", fake_build_exchange_altcoin_universe)
    monkeypatch.setattr(data_api, "get_data_symbols", fake_get_data_symbols)

    response = client.get("/api/data/research/symbols?exchange=binance")
    assert response.status_code == 200
    payload = response.json()

    assert payload["source"] == "research_universe_fallback"
    assert payload["symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert payload["default_count"] == 2
