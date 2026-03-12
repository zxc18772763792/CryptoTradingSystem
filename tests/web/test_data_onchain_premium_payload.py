from __future__ import annotations

import asyncio


def test_onchain_component_status_marks_premium_degraded_when_keys_but_no_cache():
    from web.api import data as data_api

    payload = {
        "exchange_flow_proxy": {"available": True},
        "defi_tvl": {"available": True},
        "whale_activity": {"available": True},
        "funding_rate_multi_source": {"available": True},
        "fear_greed_index": {"available": True},
        "premium_external": {
            "summary": {
                "total_sources": 4,
                "configured_keys": 2,
                "cached_sources": 0,
            },
            "sources": {},
        },
    }
    status = data_api._build_onchain_component_status(payload)
    assert status["premium_external"]["status"] == "degraded"
    assert status["premium_external"]["error"] == "premium_sources_configured_but_cache_empty"


def test_onchain_component_status_marks_premium_ok_when_optional_disabled():
    from web.api import data as data_api

    payload = {
        "exchange_flow_proxy": {"available": False},
        "defi_tvl": {"available": False},
        "whale_activity": {"available": False},
        "funding_rate_multi_source": {"available": False},
        "fear_greed_index": {"available": False},
        "premium_external": {
            "summary": {
                "total_sources": 4,
                "configured_keys": 0,
                "cached_sources": 0,
            },
            "sources": {},
        },
    }
    status = data_api._build_onchain_component_status(payload)
    assert status["premium_external"]["status"] == "ok"
    assert status["premium_external"]["error"] is None


def test_compute_onchain_overview_includes_premium_snapshot(monkeypatch):
    from web.api import data as data_api

    async def fake_tvl(*args, **kwargs):
        return {
            "chain": "Ethereum",
            "available": True,
            "latest_tvl": 123.0,
            "change_1d_pct": 1.2,
            "change_7d_pct": 2.3,
            "series": [],
        }

    async def fake_whales(*args, **kwargs):
        return {"available": True, "count": 1, "transactions": []}

    async def fake_funding(*args, **kwargs):
        return {"available": True, "count": 2, "rates": {"binance": 0.0001, "okx": 0.0002}}

    async def fake_fear(*args, **kwargs):
        return {"available": True, "value": 55, "classification": "Neutral", "signal": "neutral"}

    def fake_premium():
        return {
            "sources": {
                "kaiko": {
                    "available": True,
                    "has_cached_data": True,
                    "key_configured": True,
                    "snapshot": {"cross_exchange_spread_bps": 1.8},
                }
            },
            "summary": {
                "total_sources": 4,
                "configured_keys": 1,
                "cached_sources": 1,
                "available_sources": 1,
                "active_sources": ["kaiko"],
            },
        }

    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda *_: None)
    monkeypatch.setattr(data_api, "_fetch_defillama_chain_tvl", fake_tvl)
    monkeypatch.setattr(data_api, "_fetch_btc_whale_unconfirmed", fake_whales)
    monkeypatch.setattr(data_api, "_fetch_multi_exchange_funding", fake_funding)
    monkeypatch.setattr(data_api, "_fetch_fear_greed_snapshot", fake_fear)
    monkeypatch.setattr(data_api, "_load_premium_external_snapshot", fake_premium)

    payload = asyncio.run(
        data_api._compute_onchain_overview(
            exchange="binance",
            symbol="BTC/USDT",
            whale_threshold_btc=10.0,
            chain="Ethereum",
        )
    )
    assert payload["premium_external"]["summary"]["cached_sources"] == 1
    assert payload["component_status"]["premium_external"]["status"] == "ok"
    assert payload["component_status"]["premium_external"]["detail"].startswith("cached=1/")

