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
                },
                "coinglass": {
                    "available": True,
                    "has_cached_data": True,
                    "key_configured": True,
                    "snapshot": {"active_datasets": ["derivatives"], "freshness_sec": 120.0},
                },
            },
            "summary": {
                "total_sources": 5,
                "configured_keys": 2,
                "cached_sources": 2,
                "available_sources": 2,
                "active_sources": ["kaiko", "coinglass"],
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
    assert payload["premium_external"]["summary"]["cached_sources"] == 2
    assert payload["component_status"]["premium_external"]["status"] == "ok"
    assert payload["component_status"]["premium_external"]["detail"].startswith("cached=2/")


def test_compute_onchain_overview_uses_coinglass_whales(monkeypatch):
    from web.api import data as data_api

    async def fake_tvl(*args, **kwargs):
        return {"chain": "Bitcoin", "available": True, "latest_tvl": 1.0, "series": []}

    async def fake_whales(*args, **kwargs):
        return {
            "available": True,
            "source": "coinglass_whale_transfer",
            "count": 1,
            "transactions": [{"hash": "cg", "btc": 12.0}],
        }

    async def fail_public_whales(*args, **kwargs):
        raise AssertionError("public blockchain fallback should not run when Coinglass whales are available")

    async def fake_funding(*args, **kwargs):
        return {"available": True, "source": "coinglass_cache", "count": 1, "rates": {}}

    async def fake_fear(*args, **kwargs):
        return {"available": True, "value": 52}

    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda *_: None)
    monkeypatch.setattr(data_api, "_fetch_defillama_chain_tvl", fake_tvl)
    monkeypatch.setattr(data_api, "_fetch_whale_activity", fake_whales)
    monkeypatch.setattr(data_api, "_fetch_btc_whale_unconfirmed", fail_public_whales)
    monkeypatch.setattr(data_api, "_fetch_multi_exchange_funding", fake_funding)
    monkeypatch.setattr(data_api, "_fetch_fear_greed_snapshot", fake_fear)
    monkeypatch.setattr(
        data_api,
        "_load_premium_external_snapshot",
        lambda: {"sources": {}, "summary": {"total_sources": 0, "configured_keys": 0, "cached_sources": 0}},
    )

    payload = asyncio.run(
        data_api._compute_onchain_overview(
            exchange="binance",
            symbol="BTC/USDT",
            whale_threshold_btc=10.0,
            chain="Bitcoin",
        )
    )

    assert payload["whale_activity"]["source"] == "coinglass_whale_transfer"
    assert payload["component_status"]["whale_activity"]["source"] == "coinglass_whale_transfer"


def test_compute_onchain_overview_surfaces_coinglass_spot_flow_and_balance(monkeypatch):
    from web.api import data as data_api

    async def fake_tvl(*args, **kwargs):
        return {"chain": "Bitcoin", "available": True, "latest_tvl": 1.0, "series": []}

    async def fake_whales(*args, **kwargs):
        return {"available": True, "source": "coinglass_whale_transfer", "count": 0, "transactions": []}

    async def fake_spot_flow(*args, **kwargs):
        return {
            "available": True,
            "source": "coinglass_spot_netflow",
            "spot_exchange_inflow_usd": 150_000_000.0,
            "spot_exchange_outflow_usd": 90_000_000.0,
            "spot_exchange_netflow_usd": 60_000_000.0,
            "spot_netflow_score": 0.25,
            "exchange_flow_pressure": "inflow_sell_pressure",
        }

    async def fake_balance(*args, **kwargs):
        return {
            "available": True,
            "source": "coinglass_exchange_balance",
            "exchange_balance_btc": 10_000.0,
            "exchange_balance_usd": 1_000_000_000.0,
            "exchange_balance_change_24h": 1.2,
        }

    async def fake_funding(*args, **kwargs):
        return {"available": True, "source": "coinglass_cache", "count": 1, "rates": {}}

    async def fake_fear(*args, **kwargs):
        return {"available": True, "value": 52}

    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda *_: None)
    monkeypatch.setattr(data_api, "_fetch_defillama_chain_tvl", fake_tvl)
    monkeypatch.setattr(data_api, "_fetch_whale_activity", fake_whales)
    monkeypatch.setattr(data_api, "fetch_spot_netflow_summary", fake_spot_flow)
    monkeypatch.setattr(data_api, "fetch_exchange_balance_snapshot", fake_balance)
    monkeypatch.setattr(data_api, "_fetch_multi_exchange_funding", fake_funding)
    monkeypatch.setattr(data_api, "_fetch_fear_greed_snapshot", fake_fear)
    monkeypatch.setattr(
        data_api,
        "_load_premium_external_snapshot",
        lambda: {"sources": {}, "summary": {"total_sources": 0, "configured_keys": 0, "cached_sources": 0}},
    )

    payload = asyncio.run(
        data_api._compute_onchain_overview(
            exchange="binance",
            symbol="BTC/USDT",
            whale_threshold_btc=10.0,
            chain="Bitcoin",
        )
    )

    assert payload["exchange_flow_proxy"]["source"] == "coinglass_spot_netflow"
    assert payload["exchange_flow_proxy"]["spot_exchange_netflow_usd"] == 60_000_000.0
    assert payload["exchange_balance"]["source"] == "coinglass_exchange_balance"
    assert payload["exchange_balance"]["exchange_balance_usd"] == 1_000_000_000.0
    assert payload["component_status"]["exchange_balance"]["status"] == "ok"


def test_resolve_onchain_chain_context_maps_bsc_and_brc20():
    from web.api import data as data_api

    bsc = data_api.resolve_onchain_chain_context("BNB/USDT", "auto")
    brc20 = data_api.resolve_onchain_chain_context("ORDI/USDT", "auto")

    assert bsc["display_name"] == "BSC"
    assert bsc["lookup_chain"] == "BSC"
    assert bsc["tvl_supported"] is True
    assert brc20["display_name"] == "BRC-20"
    assert brc20["lookup_chain"] == "Bitcoin"
    assert brc20["tvl_supported"] is True


def test_compute_onchain_overview_uses_lookup_chain_but_preserves_display_chain(monkeypatch):
    from web.api import data as data_api

    captured: dict[str, object] = {}

    async def fake_tvl(*, chain, display_chain=None, chain_context=None):
        captured["chain"] = chain
        captured["display_chain"] = display_chain
        captured["chain_context"] = dict(chain_context or {})
        return {
            "chain": display_chain,
            "lookup_chain": chain,
            "available": True,
            "latest_tvl": 456.0,
            "change_1d_pct": 1.5,
            "change_7d_pct": 4.2,
            "series": [],
        }

    async def fake_whales(*args, **kwargs):
        return {"available": True, "count": 0, "transactions": []}

    async def fake_funding(*args, **kwargs):
        return {"available": True, "count": 1, "rates": {"binance": 0.0001}}

    async def fake_fear(*args, **kwargs):
        return {"available": True, "value": 52, "classification": "Neutral", "signal": "neutral"}

    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda *_: None)
    monkeypatch.setattr(data_api, "_fetch_defillama_chain_tvl", fake_tvl)
    monkeypatch.setattr(data_api, "_fetch_btc_whale_unconfirmed", fake_whales)
    monkeypatch.setattr(data_api, "_fetch_multi_exchange_funding", fake_funding)
    monkeypatch.setattr(data_api, "_fetch_fear_greed_snapshot", fake_fear)
    monkeypatch.setattr(
        data_api,
        "_load_premium_external_snapshot",
        lambda: {"sources": {}, "summary": {"total_sources": 0, "configured_keys": 0, "cached_sources": 0}},
    )

    payload = asyncio.run(
        data_api._compute_onchain_overview(
            exchange="binance",
            symbol="ORDI/USDT",
            whale_threshold_btc=10.0,
            chain="auto",
        )
    )

    assert captured["chain"] == "Bitcoin"
    assert captured["display_chain"] == "BRC-20"
    assert payload["chain_context"]["display_name"] == "BRC-20"
    assert payload["defi_tvl"]["chain"] == "BRC-20"
    assert payload["defi_tvl"]["lookup_chain"] == "Bitcoin"


def test_load_premium_external_snapshot_includes_coinglass(monkeypatch):
    from web.api import data as data_api

    monkeypatch.setattr("core.data.glassnode_collector.load_glassnode_snapshot", lambda: {})
    monkeypatch.setattr("core.data.glassnode_collector._api_key", lambda: "")
    monkeypatch.setattr("core.data.cryptoquant_collector.load_cryptoquant_snapshot", lambda: {})
    monkeypatch.setattr("core.data.cryptoquant_collector._api_key", lambda: "")
    monkeypatch.setattr("core.data.nansen_collector.load_nansen_snapshot", lambda: {})
    monkeypatch.setattr("core.data.nansen_collector._api_key", lambda: "")
    monkeypatch.setattr("core.data.kaiko_collector.load_kaiko_snapshot", lambda: {})
    monkeypatch.setattr("core.data.kaiko_collector._api_key", lambda: "")
    monkeypatch.setattr(
        data_api,
        "load_coinglass_cached_source_snapshot",
        lambda: {
            "key_configured": True,
            "has_cached_data": True,
            "active_datasets": ["derivatives"],
            "freshness_sec": 120.0,
            "dataset_count": 1,
        },
    )

    payload = data_api._load_premium_external_snapshot()

    assert payload["summary"]["total_sources"] == 5
    assert payload["sources"]["coinglass"]["available"] is True
    assert payload["sources"]["coinglass"]["has_cached_data"] is True
    assert payload["sources"]["coinglass"]["snapshot"]["active_datasets"] == ["derivatives"]
    assert "coinglass" in payload["summary"]["active_sources"]
