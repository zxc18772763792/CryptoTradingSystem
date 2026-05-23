from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from types import SimpleNamespace

from core.data import news_collector as news_collector_module


def test_legacy_fetch_rss_does_not_block_event_loop(monkeypatch, tmp_path):
    def slow_parse(_url):
        time.sleep(0.1)
        return SimpleNamespace(entries=[])

    monkeypatch.setattr(news_collector_module, "feedparser", SimpleNamespace(parse=slow_parse))

    async def _run():
        collector = news_collector_module.NewsCollector(storage_path=str(tmp_path / "news"))
        fetch_task = asyncio.create_task(collector.fetch_rss("rss", "https://example.test/feed.xml"))
        tick_task = asyncio.create_task(asyncio.sleep(0.01))

        done, _pending = await asyncio.wait(
            {fetch_task, tick_task},
            timeout=0.05,
            return_when=asyncio.FIRST_COMPLETED,
        )

        assert tick_task in done
        assert fetch_task not in done
        assert await fetch_task == []

    asyncio.run(_run())


def test_legacy_fetch_rss_returns_utc_aware_timestamps(monkeypatch, tmp_path):
    entries = [
        SimpleNamespace(
            published_parsed=(2026, 5, 23, 1, 2, 3, 0, 0, 0),
            get=lambda key, default=None: {"title": "published", "link": "https://example.test/a"}.get(key, default),
            summary="",
        ),
        SimpleNamespace(
            updated_parsed=(2026, 5, 23, 2, 3, 4, 0, 0, 0),
            get=lambda key, default=None: {"title": "updated", "link": "https://example.test/b"}.get(key, default),
            summary="",
        ),
        SimpleNamespace(
            get=lambda key, default=None: {"title": "fallback", "link": "https://example.test/c"}.get(key, default),
            summary="",
        ),
    ]

    monkeypatch.setattr(
        news_collector_module,
        "feedparser",
        SimpleNamespace(parse=lambda _url: SimpleNamespace(entries=entries)),
    )

    async def _run():
        collector = news_collector_module.NewsCollector(storage_path=str(tmp_path / "news"))
        rows = await collector.fetch_rss("rss", "https://example.test/feed.xml")

        assert [row.title for row in rows] == ["published", "updated", "fallback"]
        assert all(row.published_at.tzinfo is not None for row in rows)
        assert all(row.published_at.utcoffset() == timedelta(0) for row in rows)
        assert rows[0].published_at.isoformat() == "2026-05-23T01:02:03+00:00"
        assert rows[1].published_at.isoformat() == "2026-05-23T02:03:04+00:00"

    asyncio.run(_run())


def test_legacy_fetch_cryptopanic_fallback_timestamp_is_utc_aware(monkeypatch, tmp_path):
    captured_session = {}

    class FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def json(self):
            return {
                "results": [
                    {
                        "title": "fallback timestamp",
                        "source": {"domain": "cryptopanic.test"},
                        "url": "https://example.test/news",
                    }
                ]
            }

    class FakeSession:
        def __init__(self):
            captured_session["created"] = True

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def get(self, *_args, **_kwargs):
            return FakeResponse()

    monkeypatch.setattr(news_collector_module.aiohttp, "ClientSession", FakeSession)

    async def _run():
        collector = news_collector_module.NewsCollector(storage_path=str(tmp_path / "news"))
        rows = await collector.fetch_cryptopanic(api_key="test-key")

        assert captured_session["created"] is True
        assert len(rows) == 1
        assert rows[0].published_at.tzinfo is not None
        assert rows[0].published_at.utcoffset() == timedelta(0)

    asyncio.run(_run())
