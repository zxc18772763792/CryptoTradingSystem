from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock


def test_build_news_summary_exposes_timezone_basis(monkeypatch):
    from web.api import research as module

    requested_symbols = []

    async def fake_list_events(*, symbol=None, since=None, limit=300):
        requested_symbols.append(symbol)
        if symbol == "BTCUSDT":
            return [
                {
                    "event_id": "evt-1",
                    "ts": "2026-04-06T08:00:00+00:00",
                    "symbol": "BTCUSDT",
                    "event_type": "etf",
                    "sentiment": 1,
                }
            ]
        return []

    monkeypatch.setattr(
        module.news_db,
        "list_events",
        fake_list_events,
    )
    monkeypatch.setattr(
        module.news_db,
        "list_news_raw",
        AsyncMock(
            return_value=[
                {
                    "id": 1,
                    "source": "rss",
                    "title": "ETF inflow",
                    "published_at": "2026-04-06T08:30:00+00:00",
                    "payload": {"provider": "rss"},
                }
            ]
        ),
    )
    monkeypatch.setattr(
        module.news_db, "list_source_states", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        module.news_db,
        "get_llm_queue_stats",
        AsyncMock(return_value={"counts": {"failed": 0}}),
    )

    payload = asyncio.run(module._build_news_summary("BTC/USDT", hours=24))

    assert payload["symbol"] == "BTC"
    assert payload["query_symbols"] == ["BTCUSDT", "BTC"]
    assert requested_symbols == ["BTCUSDT", "BTC"]
    assert payload["events_count"] == 1
    assert payload["ui_timezone"] == "Asia/Shanghai"
    assert "UTC storage" in payload["timezone_basis"]
    assert payload["generated_at_utc"].endswith("+00:00")
    assert payload["generated_at_local"].endswith("+08:00")
    assert payload["window_since_utc"].endswith("Z")
    assert payload["window_since_local"].endswith("+08:00")


def test_build_news_summary_uses_recent_stale_cache_on_query_failure(monkeypatch):
    from web.api import research as module

    module._NEWS_SUMMARY_CACHE.clear()
    module._NEWS_SUMMARY_CACHE["BTC|24"] = {
        "ts": time.time() - (module._NEWS_SUMMARY_CACHE_TTL_SEC + 5),
        "payload": {
            "symbol": "BTC",
            "query_symbols": ["BTCUSDT", "BTC"],
            "hours": 24,
            "scope": "symbol",
            "events_count": 2,
            "raw_count": 4,
            "feed_count": 1,
            "active_provider_count": 2,
            "sentiment": {"positive": 1, "neutral": 1, "negative": 0},
            "by_type": {"listing": 2},
            "source_states": [],
            "llm_queue": {"counts": {"failed": 0}},
            "timestamp": "2026-04-19T10:00:00+00:00",
            "generated_at_utc": "2026-04-19T10:00:00+00:00",
            "generated_at_local": "2026-04-19T18:00:00+08:00",
            "window_since_utc": "2026-04-18T10:00:00Z",
            "window_since_local": "2026-04-18T18:00:00+08:00",
            "ui_timezone": "Asia/Shanghai",
            "timezone_basis": "UTC storage, Asia/Shanghai display",
            "source_errors": [],
        },
    }

    failing = AsyncMock(side_effect=RuntimeError("news db unavailable"))
    monkeypatch.setattr(module.news_db, "list_events", failing)
    monkeypatch.setattr(
        module.news_db,
        "list_news_raw",
        AsyncMock(side_effect=RuntimeError("raw store unavailable")),
    )
    monkeypatch.setattr(
        module.news_db,
        "list_source_states",
        AsyncMock(side_effect=RuntimeError("source states unavailable")),
    )
    monkeypatch.setattr(
        module.news_db,
        "get_llm_queue_stats",
        AsyncMock(side_effect=RuntimeError("llm queue unavailable")),
    )

    payload = asyncio.run(module._build_news_summary("BTC/USDT", hours=24))

    assert payload["events_count"] == 2
    assert payload["stale"] is True
    assert payload["cache_hit"] is True
    assert payload["source_status"] == "cache_stale"
    assert "unavailable" in str(payload.get("stale_reason") or "")
