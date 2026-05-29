from __future__ import annotations

import asyncio


def test_trading_preferred_coinglass_overview_is_cache_only(monkeypatch):
    from core.data import coinglass_feature_builder as builder
    from web.api import trading as trading_api

    calls = []

    async def fake_overview(symbol: str, refresh: bool = False, manual: bool = False):
        calls.append({"symbol": symbol, "refresh": refresh, "manual": manual})
        return {
            "key_configured": True,
            "available": False,
            "freshness_sec": 999999,
            "degraded_reason": "stale_cache",
        }

    monkeypatch.setattr(builder, "build_coinglass_overview_payload", fake_overview)

    result = asyncio.run(trading_api._load_preferred_coinglass_overview("BTC"))

    assert result["degraded_reason"] == "stale_cache"
    assert calls == [{"symbol": "BTC", "refresh": False, "manual": False}]


def test_research_preferred_coinglass_overview_is_cache_only(monkeypatch):
    from core.data import coinglass_feature_builder as builder
    from web.api import research as research_api

    calls = []

    async def fake_overview(symbol: str, refresh: bool = False, manual: bool = False):
        calls.append({"symbol": symbol, "refresh": refresh, "manual": manual})
        return {
            "key_configured": True,
            "available": False,
            "freshness_sec": 999999,
            "degraded_reason": "stale_cache",
        }

    monkeypatch.setattr(builder, "build_coinglass_overview_payload", fake_overview)

    result = asyncio.run(research_api._load_preferred_coinglass_overview("BTC"))

    assert result["degraded_reason"] == "stale_cache"
    assert calls == [{"symbol": "BTC", "refresh": False, "manual": False}]
