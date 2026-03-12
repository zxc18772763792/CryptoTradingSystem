from __future__ import annotations

import asyncio


def test_market_microstructure_compat_alias(monkeypatch):
    from web.api import trading_analytics as analytics_api

    async def fake_micro(*, exchange: str, symbol: str, depth_limit: int):
        return {
            "exchange": exchange,
            "symbol": symbol,
            "depth_limit": depth_limit,
            "ok": True,
        }

    monkeypatch.setattr(analytics_api.trading_api, "get_market_microstructure", fake_micro)
    payload = asyncio.run(
        analytics_api.get_market_microstructure_compat(
            exchange="binance",
            symbol="BTC/USDT",
            depth_limit=33,
        )
    )
    assert payload["ok"] is True
    assert payload["exchange"] == "binance"
    assert payload["symbol"] == "BTC/USDT"
    assert payload["depth_limit"] == 33

