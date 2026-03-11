import asyncio

import pytest

from web.api import trading as trading_api


def test_market_microstructure_includes_orderbook_and_flow_availability(monkeypatch):
    async def fake_orderbook(*args, **kwargs):
        return {
            "available": True,
            "bids": [[100.0, 2.0]],
            "asks": [[100.2, 1.5]],
            "timestamp": 1,
        }

    async def fake_flow(*args, **kwargs):
        return {
            "available": True,
            "count": 12,
            "buy_volume": 8.0,
            "sell_volume": 4.0,
            "imbalance": 0.333333,
        }

    async def fake_funding_basis(*args, **kwargs):
        return {"funding": {"available": False}, "basis": {"available": False}}

    monkeypatch.setattr(trading_api, "_fetch_orderbook", fake_orderbook)
    monkeypatch.setattr(trading_api, "_fetch_trade_imbalance", fake_flow)
    monkeypatch.setattr(trading_api, "_fetch_binance_public_funding_and_basis", fake_funding_basis)

    payload = asyncio.run(trading_api.get_market_microstructure(exchange="binance", symbol="BTC/USDT", depth_limit=20))

    assert payload["orderbook"]["available"] is True
    assert payload["orderbook"]["mid_price"] > 0
    assert payload["aggressor_flow"]["available"] is True
    assert payload["aggressor_flow"]["imbalance"] == pytest.approx(0.333333, rel=1e-6)


def test_market_microstructure_preserves_flow_error_flag(monkeypatch):
    async def fake_orderbook(*args, **kwargs):
        return {
            "available": True,
            "bids": [[100.0, 2.0]],
            "asks": [[100.2, 1.5]],
            "timestamp": 1,
        }

    async def fake_flow(*args, **kwargs):
        return {
            "available": False,
            "error": "timeout_or_cancelled",
            "count": 0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "imbalance": 0.0,
        }

    async def fake_funding_basis(*args, **kwargs):
        return {"funding": {"available": False}, "basis": {"available": False}}

    monkeypatch.setattr(trading_api, "_fetch_orderbook", fake_orderbook)
    monkeypatch.setattr(trading_api, "_fetch_trade_imbalance", fake_flow)
    monkeypatch.setattr(trading_api, "_fetch_binance_public_funding_and_basis", fake_funding_basis)

    payload = asyncio.run(trading_api.get_market_microstructure(exchange="binance", symbol="BTC/USDT", depth_limit=20))

    assert payload["aggressor_flow"]["available"] is False
    assert payload["aggressor_flow"]["error"] == "timeout_or_cancelled"
