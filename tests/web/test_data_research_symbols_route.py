from __future__ import annotations

import asyncio

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
            "major_market_cap_symbols": ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"],
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
    assert payload["primary_symbol"] == "BTC/USDT"
    assert payload["major_market_cap_symbols"] == ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"]
    assert payload["symbols"][:8] == [
        "BTC/USDT",
        "ETH/USDT",
        "BNB/USDT",
        "SOL/USDT",
        "XRP/USDT",
        "LINK/USDT",
        "AAVE/USDT",
        "TAO/USDT",
    ]
    assert payload["default_count"] == 8


def test_research_symbols_can_return_altcoin_only_universe(monkeypatch):
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    client = TestClient(app)

    async def fake_build_exchange_altcoin_universe(exchange: str, **kwargs):
        assert exchange == "binance"
        return {
            "exchange": "binance",
            "major_market_cap_symbols": ["BTC/USDT", "ETH/USDT", "BNB/USDT"],
            "symbols": ["LINK/USDT", "AAVE/USDT", "TAO/USDT"],
            "count": 3,
            "source": "coinglass_altcoin_universe",
            "board_count": 2,
            "boards": [],
            "excluded_major_symbols": ["BTC/USDT", "ETH/USDT", "BNB/USDT"],
        }

    monkeypatch.setattr(data_api, "build_exchange_altcoin_universe", fake_build_exchange_altcoin_universe)

    response = client.get("/api/data/research/symbols?exchange=binance&include_major=false")
    assert response.status_code == 200
    payload = response.json()

    assert payload["include_major"] is False
    assert payload["symbol_scope"] == "altcoin_only"
    assert payload["primary_symbol"] == "LINK/USDT"
    assert payload["symbols"] == ["LINK/USDT", "AAVE/USDT", "TAO/USDT"]
    assert "BTC/USDT" not in payload["symbols"]
    assert payload["default_count"] == 3


def test_research_symbols_altcoin_only_filters_data_symbol_fallback(monkeypatch):
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    client = TestClient(app)

    async def fake_build_exchange_altcoin_universe(exchange: str, **kwargs):
        raise RuntimeError("coinglass unavailable")

    async def fake_get_data_symbols(exchange: str = "binance"):
        return {
            "exchange": exchange,
            "symbols": ["BTC/USDT", "ETH/USDT", "LINK/USDT", "1000PEPE/USDT", "AAVE/USDT"],
            "count": 5,
        }

    monkeypatch.setattr(data_api, "build_exchange_altcoin_universe", fake_build_exchange_altcoin_universe)
    monkeypatch.setattr(data_api, "load_cached_exchange_altcoin_universe", lambda exchange, allow_stale=True: None)
    monkeypatch.setattr(data_api, "get_data_symbols", fake_get_data_symbols)

    response = client.get("/api/data/research/symbols?exchange=binance&include_major=false")
    assert response.status_code == 200
    payload = response.json()

    assert payload["include_major"] is False
    assert payload["symbol_scope"] == "altcoin_only"
    assert payload["symbols"] == ["LINK/USDT", "AAVE/USDT"]
    assert payload["primary_symbol"] == "LINK/USDT"


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
    monkeypatch.setattr(data_api, "load_cached_exchange_altcoin_universe", lambda exchange, allow_stale=True: None)
    monkeypatch.setattr(data_api, "get_data_symbols", fake_get_data_symbols)

    response = client.get("/api/data/research/symbols?exchange=binance")
    assert response.status_code == 200
    payload = response.json()

    assert payload["source"] == "research_universe_fallback"
    assert payload["primary_symbol"] == "BTC/USDT"
    assert payload["symbols"][:10] == [
        "BTC/USDT",
        "ETH/USDT",
        "BNB/USDT",
        "SOL/USDT",
        "XRP/USDT",
        "DOGE/USDT",
        "ADA/USDT",
        "TRX/USDT",
        "TON/USDT",
        "LINK/USDT",
    ]
    assert payload["default_count"] == 10


def test_research_symbols_falls_back_when_coinglass_universe_times_out(monkeypatch):
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    client = TestClient(app)

    async def fake_build_exchange_altcoin_universe(exchange: str, **kwargs):
        raise asyncio.TimeoutError()

    async def fake_get_data_symbols(exchange: str = "binance"):
        return {
            "exchange": exchange,
            "symbols": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
            "count": 3,
        }

    monkeypatch.setattr(data_api, "build_exchange_altcoin_universe", fake_build_exchange_altcoin_universe)
    monkeypatch.setattr(data_api, "load_cached_exchange_altcoin_universe", lambda exchange, allow_stale=True: None)
    monkeypatch.setattr(data_api, "get_data_symbols", fake_get_data_symbols)

    response = client.get("/api/data/research/symbols?exchange=binance")
    assert response.status_code == 200
    payload = response.json()

    assert payload["source"] == "research_universe_fallback"
    assert payload["primary_symbol"] == "BTC/USDT"
    assert payload["symbols"][:10] == [
        "BTC/USDT",
        "ETH/USDT",
        "BNB/USDT",
        "SOL/USDT",
        "XRP/USDT",
        "DOGE/USDT",
        "ADA/USDT",
        "TRX/USDT",
        "TON/USDT",
        "LINK/USDT",
    ]
    assert payload["default_count"] == 10
    assert "timed out" in payload["warning"]


def test_research_symbols_prefers_stale_cached_universe_on_timeout(monkeypatch):
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    client = TestClient(app)

    async def fake_build_exchange_altcoin_universe(exchange: str, **kwargs):
        raise asyncio.TimeoutError()

    def fake_load_cached_exchange_altcoin_universe(exchange: str, allow_stale: bool = False):
        assert exchange == "binance"
        assert allow_stale is True
        return {
            "exchange": "binance",
            "major_market_cap_symbols": ["BTC/USDT", "ETH/USDT"],
            "symbols": ["LINK/USDT", "AAVE/USDT"],
            "count": 2,
            "source": "coinglass_altcoin_universe",
            "updated_at": "2026-04-20T00:00:00+00:00",
        }

    async def fail_get_data_symbols(exchange: str = "binance"):
        raise AssertionError("get_data_symbols should not be used when cached universe exists")

    monkeypatch.setattr(data_api, "build_exchange_altcoin_universe", fake_build_exchange_altcoin_universe)
    monkeypatch.setattr(data_api, "load_cached_exchange_altcoin_universe", fake_load_cached_exchange_altcoin_universe)
    monkeypatch.setattr(data_api, "get_data_symbols", fail_get_data_symbols)

    response = client.get("/api/data/research/symbols?exchange=binance")
    assert response.status_code == 200
    payload = response.json()

    assert payload["source"] == "research_universe_fallback"
    assert payload["fallback_source"] == "coinglass_altcoin_universe_cache"
    assert payload["stale_fallback"] is True
    assert payload["primary_symbol"] == "BTC/USDT"
    assert payload["symbols"][:4] == ["BTC/USDT", "ETH/USDT", "LINK/USDT", "AAVE/USDT"]
    assert payload["default_count"] == 4
    assert "timed out" in payload["warning"]
