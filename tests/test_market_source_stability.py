from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock


def _recent_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _history_micro_snapshot() -> dict:
    return {
        "timestamp": _recent_ts(),
        "orderbook": {"mid_price": 100000.0, "spread_bps": 2.5},
        "aggressor_flow": {"count": 8, "imbalance": 0.18},
        "large_orders": [{"side": "bid", "notional": 5000000.0}],
        "iceberg_detection": {"candidate_count": 1},
        "long_short_ratio": {"available": True, "long_short_ratio": 1.12},
        "funding_rate": {"available": True, "funding_rate": 0.0001},
        "spot_futures_basis": {"available": True, "basis_pct": 0.11},
    }


def _history_community_snapshot() -> dict:
    return {
        "timestamp": _recent_ts(),
        "announcements": [{"title": "Listing update"}],
        "whale_transfers": {"count": 1},
        "flow_proxy": {"count": 4, "imbalance": 0.1},
        "security_alerts": {"events": []},
    }


def _news_summary(events_count: int = 1) -> dict:
    return {
        "events_count": events_count,
        "feed_count": 0,
        "raw_count": 0,
        "scope": "symbol",
        "sentiment": {"positive": events_count, "neutral": 0, "negative": 0},
    }


def test_safe_dt_returns_timezone_aware_utc():
    from web.api import trading as module

    parsed = module._safe_dt("2026-04-19T00:00:00+00:00")

    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.utcoffset().total_seconds() == 0


def test_coinglass_request_json_drops_none_query_params(monkeypatch):
    from core.data import coinglass_client as module

    captured: dict = {}

    async def fake_reserve_budget(*, manual: bool):
        return None

    async def fake_finalize_budget(*, status_code=None, error_text=""):
        captured["status_code"] = status_code
        captured["error_text"] = error_text

    class _FakeResponse:
        status = 200

        async def json(self, content_type=None):
            return {"data": [{"ok": True}]}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

    class _FakeSession:
        closed = False

        def get(self, url, *, params=None, headers=None):
            captured["url"] = url
            captured["params"] = dict(params or {})
            captured["headers"] = dict(headers or {})
            return _FakeResponse()

    monkeypatch.setattr(module, "coinglass_enabled", lambda: True)
    monkeypatch.setattr(module, "coinglass_api_key", lambda: "test-key")
    monkeypatch.setattr(module, "_coinglass_base_url", lambda: "https://example.com")
    monkeypatch.setattr(module, "_coinglass_root_url", lambda: "https://example.com")
    monkeypatch.setattr(module, "_reserve_budget", fake_reserve_budget)
    monkeypatch.setattr(module, "_finalize_budget", fake_finalize_budget)

    client = module.CoinglassClient()

    async def fake_get_session():
        return _FakeSession()

    monkeypatch.setattr(client, "_get_session", fake_get_session)

    result = asyncio.run(
        client.request_json(
            "/v4/api/calendar/economic-data",
            params={
                "language": "zh",
                "start_time": 1234567890,
                "end_time": None,
                "page": None,
                "empty_text": "   ",
                "enabled": True,
            },
            manual=False,
        )
    )

    assert result["status_code"] == 200
    assert captured["params"] == {
        "language": "zh",
        "start_time": 1234567890,
        "enabled": "true",
    }


def test_deribit_options_client_session_respects_env_proxy_settings(monkeypatch):
    from core.data import options_collector as module

    captured: dict = {}

    class _FakeSession:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        async def __aenter__(self):
            raise RuntimeError("stop after session creation")

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

    monkeypatch.setattr(module.aiohttp, "ClientSession", _FakeSession)

    collector = module.DeribitOptionsCollector()
    result = asyncio.run(collector._fetch_from_api("BTC"))

    assert result is None
    assert captured["trust_env"] is True
    assert captured["timeout"] == collector._timeout


def test_deribit_options_uses_persisted_snapshot_when_fetch_unavailable(
    tmp_path, monkeypatch
):
    from core.data import options_collector as module

    monkeypatch.chdir(tmp_path)
    collector = module.DeribitOptionsCollector()
    expected = module.OptionsSnapshot(
        currency="BTC",
        atm_iv=0.5321,
        skew_25d=-0.01,
        put_call_ratio=0.71,
        n_calls=463,
        n_puts=463,
        timestamp=datetime.now(timezone.utc),
    )
    collector._persist_snapshot(expected)
    monkeypatch.setattr(
        collector,
        "_fetch_from_api",
        AsyncMock(return_value=None),
    )

    result = asyncio.run(collector.fetch_snapshot("BTC"))

    assert result is not None
    assert result.currency == "BTC"
    assert result.atm_iv == expected.atm_iv
    assert result.put_call_ratio == expected.put_call_ratio


def test_public_fear_greed_uses_recent_stale_cache_on_refresh_failure(monkeypatch):
    from core.data.sentiment import fear_greed_collector as fg_module
    from web.api import research as module

    module._PUBLIC_MARKET_DATA_CACHE.clear()
    module._PUBLIC_MARKET_DATA_CACHE["fear_greed"] = {
        "ts": time.time() - (module._PUBLIC_MARKET_DATA_CACHE_TTL_SEC + 5),
        "payload": {
            "available": True,
            "value": 33,
            "classification": "fear",
            "signal": "neutral",
            "signal_strength": 0.33,
            "timestamp": _recent_ts(),
            "source": "alternative.me",
        },
    }

    class _BrokenCollector:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def fetch_current(self):
            raise RuntimeError("fear-greed upstream unavailable")

    monkeypatch.setattr(fg_module, "FearGreedCollector", _BrokenCollector)

    result = asyncio.run(module._load_public_fear_greed_snapshot())

    assert result["available"] is True
    assert result["value"] == 33
    assert result["stale"] is True
    assert result["cache_hit"] is True
    assert result["source_status"] == "cache_stale"
    assert "unavailable" in str(result.get("stale_reason") or "")


def test_public_market_breadth_uses_recent_stale_cache_on_refresh_failure(monkeypatch):
    from web.api import research as module

    module._PUBLIC_MARKET_DATA_CACHE.clear()
    module._PUBLIC_MARKET_DATA_CACHE["global_market_breadth"] = {
        "ts": time.time() - (module._PUBLIC_MARKET_DATA_CACHE_TTL_SEC + 5),
        "payload": {
            "available": True,
            "source": "coingecko_global",
            "active_cryptocurrencies": 12345,
            "markets": 900,
            "total_market_cap_usd": 2500000000000.0,
            "total_volume_usd": 100000000000.0,
            "market_cap_change_pct_24h": -1.2,
            "volume_change_pct_24h": 2.4,
            "btc_dominance_pct": 58.5,
            "eth_dominance_pct": 9.1,
            "updated_at": 1713456000,
        },
    }

    class _BrokenAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def get(self, url):
            raise RuntimeError("coingecko unavailable")

    monkeypatch.setattr(module.httpx, "AsyncClient", _BrokenAsyncClient)

    result = asyncio.run(module._load_public_market_breadth_snapshot())

    assert result["available"] is True
    assert result["market_cap_change_pct_24h"] == -1.2
    assert result["stale"] is True
    assert result["cache_hit"] is True
    assert result["source_status"] == "cache_stale"
    assert "unavailable" in str(result.get("stale_reason") or "")


def test_slowmist_security_alerts_use_recent_stale_cache_on_refresh_failure(
    monkeypatch,
):
    from web.api import trading as module

    module._SECURITY_ALERTS_CACHE.clear()
    module._cache_put(
        module._SECURITY_ALERTS_CACHE,
        "BTC|6",
        {
            "available": True,
            "source": "slowmist_hacked",
            "scope": "global_fallback",
            "events": [
                {
                    "title": "Cached bridge exploit",
                    "severity": "high",
                    "amount_usd": 3200000.0,
                }
            ],
            "note": "cached incidents",
        },
    )
    module._SECURITY_ALERTS_CACHE["BTC|6"]["ts"] = time.time() - (
        module._SECURITY_ALERTS_CACHE_TTL_SEC + 5
    )

    class _BrokenAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def get(self, url):
            raise RuntimeError("slowmist unavailable")

    monkeypatch.setattr(module.httpx, "AsyncClient", _BrokenAsyncClient)

    result = asyncio.run(module._fetch_slowmist_security_alerts("BTC/USDT", limit=6))

    assert result["available"] is True
    assert result["events"][0]["title"] == "Cached bridge exploit"
    assert result["stale"] is True
    assert result["cache_hit"] is True
    assert result["source_status"] == "cache_stale"
    assert "回退" in str(result.get("note") or "")


def test_get_trading_calendar_uses_recent_official_stale_cache_when_live_refresh_fails(
    monkeypatch,
):
    from web.api import trading as module

    module._TRADING_CALENDAR_CACHE.clear()
    cached_payload = {
        "source": "coinglass_economic_data+coinglass_unlock_list",
        "note": "cached official snapshot",
        "days": 7,
        "events": [
            {
                "category": "economic",
                "name": "US CPI",
                "time_utc": "2026-04-20T12:30:00+00:00",
                "importance": "high",
                "source": "coinglass_economic_data",
            }
        ],
        "count": 1,
        "source_details": {
            "economic": {
                "available": True,
                "count": 1,
                "coverage_end": "2026-04-27T00:00:00+00:00",
                "error": None,
            },
            "central_bank": {
                "available": False,
                "count": 0,
                "coverage_end": None,
                "error": "timeout",
            },
            "unlocks": {"available": False, "count": 0, "error": "timeout"},
            "internal_estimate": {"used": False, "count": 0},
        },
    }
    module._cache_put(module._TRADING_CALENDAR_CACHE, "days:7", cached_payload)
    module._TRADING_CALENDAR_CACHE["days:7"]["ts"] = time.time() - (
        module._TRADING_CALENDAR_CACHE_TTL_SEC + 5
    )

    monkeypatch.setattr(
        module,
        "_fetch_coinglass_economic_calendar_events",
        AsyncMock(
            return_value={
                "available": False,
                "events": [],
                "coverage_end": None,
                "error": "budget exhausted",
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_fetch_coinglass_central_bank_calendar_events",
        AsyncMock(
            return_value={
                "available": False,
                "events": [],
                "coverage_end": None,
                "error": "budget exhausted",
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_fetch_coinglass_unlock_calendar_events",
        AsyncMock(
            return_value={"available": False, "events": [], "error": "budget exhausted"}
        ),
    )

    def fake_internal(
        *,
        now,
        end,
        start=None,
        include_economic=True,
        include_unlocks=True,
        include_expiry=True,
    ):
        return [
            {
                "category": "expiry",
                "name": "Friday expiry reminder",
                "time_utc": "2026-04-24T08:00:00+00:00",
                "importance": "medium",
                "source": "internal_estimate",
            }
        ]

    monkeypatch.setattr(
        module, "_build_internal_estimate_calendar_events", fake_internal
    )

    result = asyncio.run(module.get_trading_calendar(days=7))

    assert result["source"] == "coinglass_economic_data+coinglass_unlock_list"
    assert result["stale"] is True
    assert result["cache_hit"] is True
    assert result["source_status"] == "cache_stale"
    assert result["stale_reason"] == "official_calendar_refresh_failed"
    assert "recent" not in result["note"].lower()
    assert "快照" in result["note"]
    assert any(
        str(item.get("source") or "").startswith("coinglass_")
        for item in result["events"]
    )


def test_market_state_surfaces_stale_calendar_warning(monkeypatch):
    from web.api import research as module

    module._PUBLIC_MARKET_DATA_CACHE.clear()
    monkeypatch.setattr(
        module,
        "_load_public_fear_greed_snapshot",
        AsyncMock(
            return_value={
                "available": True,
                "value": 45,
                "source": "alternative.me",
                "stale": False,
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_public_market_breadth_snapshot",
        AsyncMock(
            return_value={
                "available": True,
                "market_cap_change_pct_24h": 1.2,
                "source": "coingecko_global",
                "stale": False,
            }
        ),
    )
    monkeypatch.setattr(
        module, "_load_preferred_coinglass_overview", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_analytics_history_status", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"})
    )
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(
            return_value={
                "source": "coinglass_economic_data",
                "note": "cached official snapshot",
                "stale": True,
                "cache_age_sec": 601.0,
                "source_status": "cache_stale",
                "events": [
                    {
                        "name": "US CPI",
                        "time_utc": _recent_ts(),
                        "importance": "high",
                        "category": "economic",
                        "source": "coinglass_economic_data",
                    }
                ],
                "source_details": {
                    "economic": {"available": True, "count": 1},
                    "central_bank": {"available": False, "count": 0},
                    "unlocks": {"available": False, "count": 0},
                    "internal_estimate": {"used": False, "count": 0},
                },
            }
        ),
    )
    monkeypatch.setattr(
        module, "_build_news_summary", AsyncMock(return_value=_news_summary(1))
    )
    monkeypatch.setattr(
        module,
        "_load_latest_microstructure_snapshot",
        AsyncMock(return_value=_history_micro_snapshot()),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_community_snapshot",
        AsyncMock(return_value=_history_community_snapshot()),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_whale_snapshot",
        AsyncMock(return_value={"count": 1, "transactions": []}),
    )
    monkeypatch.setattr(
        module,
        "get_market_microstructure",
        AsyncMock(side_effect=AssertionError("live microstructure should be skipped")),
    )
    monkeypatch.setattr(
        module,
        "get_community_overview",
        AsyncMock(side_effect=AssertionError("live community should be skipped")),
    )

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["payload"]["calendar_source_summary"]["stale"] is True
    assert (
        result["payload"]["calendar_source_summary"]["source_status"] == "cache_stale"
    )
    assert any("cached CoinGlass snapshot" in warning for warning in result["warnings"])


def test_market_state_surfaces_stale_news_warning(monkeypatch):
    from web.api import research as module

    module._PUBLIC_MARKET_DATA_CACHE.clear()
    monkeypatch.setattr(
        module,
        "_load_public_fear_greed_snapshot",
        AsyncMock(
            return_value={
                "available": True,
                "value": 45,
                "source": "alternative.me",
                "stale": False,
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_public_market_breadth_snapshot",
        AsyncMock(
            return_value={
                "available": True,
                "market_cap_change_pct_24h": 1.2,
                "source": "coingecko_global",
                "stale": False,
            }
        ),
    )
    monkeypatch.setattr(
        module, "_load_preferred_coinglass_overview", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_analytics_history_status", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_risk_dashboard", AsyncMock(return_value={"risk_level": "low"})
    )
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(
            return_value={
                "events": [
                    {"name": "US CPI", "time_utc": _recent_ts(), "importance": "high"}
                ]
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_build_news_summary",
        AsyncMock(
            return_value={
                **_news_summary(2),
                "stale": True,
                "cache_hit": True,
                "cache_age_sec": 601.0,
                "source_status": "cache_stale",
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_microstructure_snapshot",
        AsyncMock(return_value=_history_micro_snapshot()),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_community_snapshot",
        AsyncMock(return_value=_history_community_snapshot()),
    )
    monkeypatch.setattr(
        module,
        "_load_latest_whale_snapshot",
        AsyncMock(return_value={"count": 1, "transactions": []}),
    )
    monkeypatch.setattr(
        module,
        "get_market_microstructure",
        AsyncMock(side_effect=AssertionError("live microstructure should be skipped")),
    )
    monkeypatch.setattr(
        module,
        "get_community_overview",
        AsyncMock(side_effect=AssertionError("live community should be skipped")),
    )

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert any(
        "News summary live refresh failed" in warning for warning in result["warnings"]
    )


def test_risk_dashboard_uses_recent_stale_cache_without_blocking(monkeypatch):
    from web.api import trading as module

    module._RISK_DASHBOARD_CACHE.clear()
    module._RISK_DASHBOARD_REFRESH_TASKS.clear()
    module._cache_put(
        module._RISK_DASHBOARD_CACHE,
        "lookback:240",
        {
            "timestamp": _recent_ts(),
            "risk_level": "low",
            "total_exposure": 12.5,
            "exposure_pct_of_equity": 8.2,
            "var": {"var95_pct": 1.4, "sample_points": 120},
        },
    )
    module._RISK_DASHBOARD_CACHE["lookback:240"]["ts"] = time.time() - (
        module._RISK_DASHBOARD_CACHE_TTL_SEC + 5
    )

    monkeypatch.setattr(module, "_schedule_cache_refresh", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "_build_risk_dashboard_payload",
        AsyncMock(side_effect=AssertionError("live rebuild should not block")),
    )

    result = asyncio.run(module.get_risk_dashboard(lookback=240))

    assert result["risk_level"] == "low"
    assert result["stale"] is True
    assert result["cache_hit"] is True
    assert result["source_status"] == "cache_stale"


def test_microstructure_uses_recent_stale_cache_without_blocking(monkeypatch):
    from web.api import trading as module

    module._MICROSTRUCTURE_SNAPSHOT_CACHE.clear()
    module._MICROSTRUCTURE_REFRESH_TASKS.clear()
    cache_key = "binance|BTC/USDT|80"
    module._cache_put(
        module._MICROSTRUCTURE_SNAPSHOT_CACHE,
        cache_key,
        {
            "timestamp": _recent_ts(),
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "available": True,
            "source_error": "",
            "orderbook": {"mid_price": 100000.0, "spread_bps": 2.4},
            "aggressor_flow": {"available": True, "count": 6, "imbalance": 0.13},
            "large_orders": [{"side": "bid", "notional": 2500000.0}],
            "iceberg_detection": {"candidate_count": 1},
            "long_short_ratio": {"available": True, "long_short_ratio": 1.18},
            "funding_rate": {"available": True, "funding_rate": 0.00012},
            "spot_futures_basis": {"available": True, "basis_pct": 0.09},
        },
    )
    module._MICROSTRUCTURE_SNAPSHOT_CACHE[cache_key]["ts"] = time.time() - (
        module._MICROSTRUCTURE_SNAPSHOT_CACHE_TTL_SEC + 5
    )

    monkeypatch.setattr(module, "_schedule_cache_refresh", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "_build_market_microstructure_payload",
        AsyncMock(side_effect=AssertionError("live rebuild should not block")),
    )

    result = asyncio.run(
        module.get_market_microstructure(
            exchange="binance", symbol="BTC/USDT", depth_limit=80
        )
    )

    assert result["orderbook"]["spread_bps"] == 2.4
    assert result["stale"] is True
    assert result["cache_hit"] is True
    assert result["source_status"] == "cache_stale"


def test_community_overview_uses_recent_stale_cache_without_blocking(monkeypatch):
    from web.api import trading as module

    module._COMMUNITY_OVERVIEW_CACHE.clear()
    module._COMMUNITY_REFRESH_TASKS.clear()
    cache_key = "binance|CACHETEST/USDT"
    module._cache_put(
        module._COMMUNITY_OVERVIEW_CACHE,
        cache_key,
        {
            "timestamp": _recent_ts(),
            "symbol": "CACHETEST/USDT",
            "exchange": "binance",
            "twitter_watchlist": ["elonmusk"],
            "flow_proxy": {"imbalance": 0.11, "buy_volume": 12.0, "sell_volume": 8.0},
            "whale_transfers": {
                "available": True,
                "count": 1,
                "transactions": [{"btc": 15.0}],
            },
            "security_alerts": {
                "available": True,
                "source": "slowmist_hacked",
                "events": [],
            },
            "announcements": [{"title": "Listing update"}],
            "news_provider": "binance_announcements",
            "news_sources": ["binance_announcements"],
        },
    )
    module._COMMUNITY_OVERVIEW_CACHE[cache_key]["ts"] = time.time() - (
        module._COMMUNITY_OVERVIEW_CACHE_TTL_SEC + 5
    )

    monkeypatch.setattr(module, "_schedule_cache_refresh", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        module,
        "_build_community_overview_payload",
        AsyncMock(side_effect=AssertionError("live rebuild should not block")),
    )

    result = asyncio.run(
        module.get_community_overview(symbol="CACHETEST/USDT", exchange="binance")
    )

    assert result["announcements"][0]["title"] == "Listing update"
    assert result["stale"] is True
    assert result["cache_hit"] is True
    assert result["source_status"] == "cache_stale"


def test_market_state_surfaces_stale_risk_micro_and_community_warnings(monkeypatch):
    from web.api import research as module

    module._PUBLIC_MARKET_DATA_CACHE.clear()
    monkeypatch.setattr(
        module,
        "_load_public_fear_greed_snapshot",
        AsyncMock(
            return_value={
                "available": True,
                "value": 45,
                "source": "alternative.me",
                "stale": False,
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "_load_public_market_breadth_snapshot",
        AsyncMock(
            return_value={
                "available": True,
                "market_cap_change_pct_24h": 1.2,
                "source": "coingecko_global",
                "stale": False,
            }
        ),
    )
    monkeypatch.setattr(
        module, "_load_preferred_coinglass_overview", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "get_analytics_history_status", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module,
        "get_risk_dashboard",
        AsyncMock(
            return_value={
                "risk_level": "low",
                "stale": True,
                "cache_hit": True,
                "cache_age_sec": 35.0,
                "source_status": "cache_stale",
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "get_trading_calendar",
        AsyncMock(
            return_value={
                "events": [
                    {"name": "US CPI", "time_utc": _recent_ts(), "importance": "high"}
                ]
            }
        ),
    )
    monkeypatch.setattr(
        module, "_build_news_summary", AsyncMock(return_value=_news_summary(2))
    )
    monkeypatch.setattr(
        module, "_load_latest_microstructure_snapshot", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "_load_latest_community_snapshot", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module, "_load_latest_whale_snapshot", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        module,
        "get_market_microstructure",
        AsyncMock(
            return_value={
                **_history_micro_snapshot(),
                "stale": True,
                "cache_hit": True,
                "source_status": "cache_stale",
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "get_community_overview",
        AsyncMock(
            return_value={
                **_history_community_snapshot(),
                "stale": True,
                "cache_hit": True,
                "source_status": "cache_stale",
            }
        ),
    )

    result = asyncio.run(module._build_market_state_module(module.ResearchProfile()))

    assert result["status"] == "ok"
    assert any(
        "Risk dashboard live refresh is pending" in warning
        for warning in result["warnings"]
    )
    assert any(
        "Microstructure live refresh is pending" in warning
        for warning in result["warnings"]
    )
    assert any(
        "Community overview live refresh is pending" in warning
        for warning in result["warnings"]
    )
