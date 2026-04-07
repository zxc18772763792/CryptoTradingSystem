from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock


def test_build_news_summary_exposes_timezone_basis(monkeypatch):
    from web.api import research as module

    monkeypatch.setattr(
        module.news_db,
        "list_events",
        AsyncMock(
            return_value=[
                {
                    "event_id": "evt-1",
                    "ts": "2026-04-06T08:00:00+00:00",
                    "symbol": "BTCUSDT",
                    "event_type": "etf",
                    "sentiment": 1,
                }
            ]
        ),
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
    monkeypatch.setattr(module.news_db, "list_source_states", AsyncMock(return_value=[]))
    monkeypatch.setattr(module.news_db, "get_llm_queue_stats", AsyncMock(return_value={"counts": {"failed": 0}}))

    payload = asyncio.run(module._build_news_summary("BTC/USDT", hours=24))

    assert payload["symbol"] == "BTC"
    assert payload["ui_timezone"] == "Asia/Shanghai"
    assert "UTC storage" in payload["timezone_basis"]
    assert payload["generated_at_utc"].endswith("+00:00")
    assert payload["generated_at_local"].endswith("+08:00")
    assert payload["window_since_utc"].endswith("Z")
    assert payload["window_since_local"].endswith("+08:00")
