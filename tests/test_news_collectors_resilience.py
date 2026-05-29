from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest
import requests

from core.news.collectors.jin10 import Jin10Collector
from core.news.collectors.newsapi import NewsAPICollector
from core.news.collectors.common import parse_rss_items
from core.news.collectors.rss import RSSNewsCollector


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: Optional[Dict[str, Any]] = None,
        *,
        text: str = "",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.headers = headers or {}

    def json(self) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}", response=self)


def test_jin10_retries_transient_status(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[str] = []
    fresh_ts = datetime.now(timezone.utc).isoformat()
    responses = [
        _FakeResponse(429, headers={"Retry-After": "0"}),
        _FakeResponse(
            200,
            {
                "data": [
                    {
                        "id": "flash-1",
                        "time": fresh_ts,
                        "data": {"title": "BTC ETF inflow", "content": "content"},
                    }
                ]
            },
        ),
    ]

    def fake_get(*args: Any, **kwargs: Any) -> _FakeResponse:
        calls.append(str(args[0]))
        return responses.pop(0)

    monkeypatch.setattr("core.news.collectors.jin10.requests.get", fake_get)
    collector = Jin10Collector({"defaults": {"jin10_retry_base_sleep_sec": 0, "jin10_retry_attempts": 2}})

    items = collector.pull_latest(max_records=10, since_minutes=60)

    assert len(items) == 1
    assert items[0]["payload"]["id"] == "flash-1"
    assert len(calls) == 2


def test_newsapi_retries_retry_after_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEWSAPI_KEY", "unit-key")
    calls: List[Dict[str, Any]] = []
    fresh_ts = datetime.now(timezone.utc).isoformat()
    responses = [
        _FakeResponse(503, {"status": "error", "message": "busy"}, headers={"Retry-After": "0"}),
        _FakeResponse(
            200,
            {
                "status": "ok",
                "articles": [
                    {
                        "title": "ETH staking update",
                        "url": "https://example.test/eth",
                        "description": "desc",
                        "publishedAt": fresh_ts,
                        "source": {"name": "Unit"},
                    }
                ],
            },
        ),
    ]

    def fake_get(*args: Any, **kwargs: Any) -> _FakeResponse:
        calls.append(dict(kwargs))
        return responses.pop(0)

    monkeypatch.setattr("core.news.collectors.newsapi.requests.get", fake_get)
    collector = NewsAPICollector({"defaults": {"newsapi_retry_base_sleep_sec": 0, "newsapi_retry_attempts": 2}})

    items = collector.pull_latest(query="eth", max_records=10, since_minutes=60)

    assert len(items) == 1
    assert items[0]["source"] == "Unit"
    assert len(calls) == 2
    assert calls[0]["headers"]["X-Api-Key"] == "unit-key"


def test_rss_parser_rejects_external_entities() -> None:
    collector = RSSNewsCollector({"defaults": {"rss_feeds": []}})
    malicious_xml = """<?xml version="1.0"?>
<!DOCTYPE foo [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
<rss><channel><item><title>&xxe;</title><link>https://example.test/x</link></item></channel></rss>
"""

    with pytest.raises(Exception):
        collector._parse_feed("unit", "https://example.test/feed", malicious_xml)


def test_common_rss_parser_rejects_external_entities() -> None:
    malicious_xml = """<?xml version="1.0"?>
<!DOCTYPE foo [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
<rss><channel><item><title>&xxe;</title><link>https://example.test/x</link></item></channel></rss>
"""

    with pytest.raises(Exception):
        parse_rss_items(malicious_xml, "unit", "unit-feed", "https://example.test/feed")
