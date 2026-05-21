from __future__ import annotations

import asyncio
import time
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
