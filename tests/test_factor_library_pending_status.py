from __future__ import annotations

import asyncio

from web.api import data as data_api


def _clear_factor_state() -> None:
    data_api._FACTOR_LIBRARY_CACHE.clear()
    data_api._FACTOR_LIBRARY_REFRESH_TASKS.clear()
    data_api._FACTOR_LIBRARY_REFRESH_META.clear()


async def _call_factor_library_and_cleanup(**kwargs):
    try:
        return await data_api.get_factor_library(**kwargs)
    finally:
        tasks = list(data_api._FACTOR_LIBRARY_REFRESH_TASKS.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        _clear_factor_state()


def test_get_factor_library_returns_disk_cache_while_refreshing(monkeypatch):
    _clear_factor_state()

    async def fake_refresh_factor_library_cache(*args, **kwargs):
        await asyncio.sleep(0.2)

    monkeypatch.setattr(data_api, "_refresh_factor_library_cache", fake_refresh_factor_library_cache)
    monkeypatch.setattr(
        data_api,
        "_load_research_disk_cached_payload",
        lambda prefix, cache_key: {
            "exchange": "binance",
            "timeframe": "1h",
            "lookback_effective": 1200,
            "symbols_requested": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
            "retired_filter": {
                "enabled": True,
                "excluded_symbols": [],
                "requested_after_filter": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
            },
            "symbols_used": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
            "points": 800,
            "factors": ["MKT", "MOM"],
            "catalog": {},
            "universe_size": 3,
            "universe_quality": "normal",
            "warnings": [],
            "latest": {"MKT": 0.12, "MOM": 0.08},
            "mean_24": {"MKT": 0.1, "MOM": 0.05},
            "std_24": {"MKT": 0.02, "MOM": 0.01},
            "correlation": {"MKT": {"MKT": 1.0}},
            "series": [{"timestamp": "2026-04-19T00:00:00Z", "MKT": 0.12, "MOM": 0.08}],
            "asset_scores": [{"symbol": "BTC/USDT", "score": 0.6}],
            "error": "",
            "degraded": False,
            "cache_age_sec": 1200.0,
            "cache_source": "disk",
        },
    )

    payload = asyncio.run(
        _call_factor_library_and_cleanup(
            exchange="binance",
            symbols="BTC/USDT,ETH/USDT,SOL/USDT",
            timeframe="1h",
            lookback=1200,
        )
    )

    assert payload["served_mode"] == "cache_refresh"
    assert payload["refreshing"] is True
    assert payload["pending"] is True
    assert payload["pending_stage"] == "refreshing"
    assert payload["cache_source"] == "disk"
    assert payload["points"] == 800
    assert payload["universe_size"] == 3
    assert payload["warnings"]
    assert payload["error"] == ""
    assert payload["retry_after_sec"] == data_api._FACTOR_LIBRARY_PENDING_RETRY_SEC


def test_get_factor_library_returns_bootstrap_progress_when_no_cache(monkeypatch):
    _clear_factor_state()

    async def fake_refresh_factor_library_cache(*args, **kwargs):
        await asyncio.sleep(0.2)

    monkeypatch.setattr(data_api, "_refresh_factor_library_cache", fake_refresh_factor_library_cache)
    monkeypatch.setattr(data_api, "_load_research_disk_cached_payload", lambda prefix, cache_key: None)
    monkeypatch.setattr(data_api, "_FACTOR_LIBRARY_BOOTSTRAP_WAIT_SEC", 0.01)

    payload = asyncio.run(
        _call_factor_library_and_cleanup(
            exchange="binance",
            symbols="BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT,XRP/USDT,DOGE/USDT,ADA/USDT",
            timeframe="1h",
            lookback=1200,
        )
    )

    assert payload["served_mode"] == "bootstrap"
    assert payload["refreshing"] is True
    assert payload["pending"] is True
    assert payload["pending_stage"] == "bootstrap"
    assert payload["pending_since"]
    assert payload["pending_elapsed_sec"] >= 0
    assert payload["pending_expected_sec"] > 0
    assert payload["retry_after_sec"] == data_api._FACTOR_LIBRARY_PENDING_RETRY_SEC
    assert "lookback 1200" in payload["error"]
    assert payload["points"] == 0
    assert payload["universe_size"] == 0
