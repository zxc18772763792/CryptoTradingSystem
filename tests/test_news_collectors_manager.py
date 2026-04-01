import asyncio
import pytest
from typing import Optional

from core.news.collectors.manager import MultiSourceNewsCollector, _CollectorSpec
from core.news.storage import db as news_db


class _DummyCollector:
    def __init__(self, *, items=None, cursor="next-cursor", error: Optional[Exception] = None):
        self._items = list(items or [])
        self._cursor = cursor
        self._error = error
        self.closed = 0

    def pull_latest(self, **kwargs):
        if self._error is not None:
            raise self._error
        return list(self._items)

    def pull_incremental(self, **kwargs):
        if self._error is not None:
            raise self._error
        return list(self._items), self._cursor

    def close(self):
        self.closed += 1


class _DummyManager(MultiSourceNewsCollector):
    def __init__(self, collector):
        super().__init__({"defaults": {"news_sources": ["dummy"]}})
        self._collector = collector

    def _build_collectors(self, source_names=None):
        return ([_CollectorSpec(name="dummy", collector=self._collector)], [])


def test_pull_latest_closes_collectors_after_errors():
    collector = _DummyCollector(error=RuntimeError("boom"))
    manager = _DummyManager(collector)

    payload = manager.pull_latest(max_records=20, since_minutes=60)

    assert payload["items"] == []
    assert collector.closed == 1


def test_pull_latest_incremental_closes_collectors(monkeypatch):
    collector = _DummyCollector(
        items=[
            {
                "title": "BTC jumps",
                "url": "https://example.com/btc-jumps",
                "published_at": "2026-03-31T12:00:00+00:00",
            }
        ]
    )
    manager = _DummyManager(collector)

    async def _fake_get_source_state(name):
        return None

    async def _fake_set_source_state(name, **kwargs):
        return {"cursor_value": kwargs.get("cursor_value")}

    monkeypatch.setattr(news_db, "get_source_state", _fake_get_source_state)
    monkeypatch.setattr(news_db, "set_source_state", _fake_set_source_state)

    payload = asyncio.run(manager.pull_latest_incremental(max_records=20, since_minutes=60))

    assert len(payload["items"]) == 1
    assert payload["source_stats"]["dummy"]["cursor_after"] == "next-cursor"
    assert collector.closed == 1
