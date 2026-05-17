from __future__ import annotations

from typing import Any, Dict, Optional

from core.news.collectors.manager import MultiSourceNewsCollector, _parse_ts_to_unix
from core.news.collectors.opennews import OpenNewsCollector, _extract_rows
from core.news.storage import db as news_db


class _FakeResponse:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload

    def json(self) -> Dict[str, Any]:
        return self._payload


def test_opennews_normalizes_ai_rated_item() -> None:
    raw = {
        "id": "n1",
        "text": "SEC approves spot ETH ETF staking rule change",
        "newsType": "Reuters",
        "engineType": "news",
        "link": "https://example.com/eth-etf",
        "coins": [{"symbol": "ETH", "market_type": "spot"}],
        "aiRating": {
            "score": 85,
            "grade": "A",
            "signal": "long",
            "status": "done",
            "summary": "ETH 利好",
            "enSummary": "Positive ETF catalyst for ETH.",
        },
        "ts": "2026-05-05T01:02:03+00:00",
    }

    item = OpenNewsCollector._normalize_item(raw)

    assert item is not None
    assert item["source"] == "opennews"
    assert item["title"] == raw["text"]
    assert item["url"] == raw["link"]
    assert item["symbols"] == ["ETHUSDT"]
    assert item["payload"]["provider"] == "opennews"
    assert item["payload"]["opennews_news_type"] == "Reuters"
    assert item["payload"]["opennews_engine_type"] == "news"
    assert item["payload"]["opennews_ai_score"] == 85
    assert item["payload"]["opennews_ai_signal"] == "long"


def test_opennews_pull_latest_posts_expected_body(monkeypatch) -> None:
    monkeypatch.setenv("OPENNEWS_TOKEN", "token-123")
    captured: Dict[str, Any] = {}
    collector = OpenNewsCollector(
        {
            "defaults": {
                "opennews_endpoint": "https://unit.test/open/news_search",
                "opennews_coins": "BTC,ETH",
                "opennews_engine_types": "news:Bloomberg,Reuters;market:",
                "opennews_min_score": 70,
                "opennews_has_coin": True,
            }
        }
    )

    def fake_request(url: str, **kwargs: Any) -> _FakeResponse:
        captured["url"] = url
        captured.update(kwargs)
        return _FakeResponse(
            {
                "data": [
                    {
                        "id": "n1",
                        "text": "BTC funding rate jumps",
                        "link": "https://example.com/btc",
                        "coins": [{"symbol": "BTC"}],
                        "aiRating": {"score": 74, "signal": "long", "status": "done"},
                        "ts": "2026-05-05T02:00:00+00:00",
                    }
                ]
            }
        )

    monkeypatch.setattr(collector, "_request", fake_request)

    items = collector.pull_latest(query="funding", max_records=20, since_minutes=24 * 60)

    assert len(items) == 1
    assert captured["url"] == "https://unit.test/open/news_search"
    assert captured["method"] == "POST"
    assert captured["headers"]["Authorization"] == "Bearer token-123"
    assert captured["json_body"] == {
        "limit": 20,
        "page": 1,
        "q": "funding",
        "coins": ["BTC", "ETH"],
        "engineTypes": {"news": ["Bloomberg", "Reuters"], "market": []},
        "hasCoin": True,
        "score": 70,
    }


def test_opennews_manager_registers_only_when_enabled(monkeypatch) -> None:
    monkeypatch.delenv("OPENNEWS_TOKEN", raising=False)
    monkeypatch.delenv("NEWS_ENABLE_OPENNEWS", raising=False)
    manager = MultiSourceNewsCollector({"defaults": {"news_sources": ["opennews"]}})

    specs, errors = manager._build_collectors(source_names=["opennews"])

    assert specs == []
    assert "opennews disabled: NEWS_ENABLE_OPENNEWS is not true" in errors

    monkeypatch.setenv("NEWS_ENABLE_OPENNEWS", "true")
    manager = MultiSourceNewsCollector({"defaults": {"news_sources": ["opennews"]}})
    specs, errors = manager._build_collectors(source_names=["opennews"])

    assert specs == []
    assert "opennews disabled: OPENNEWS_TOKEN missing" in errors

    monkeypatch.setenv("OPENNEWS_TOKEN", "token-123")
    manager = MultiSourceNewsCollector({"defaults": {"news_sources": ["opennews"]}})
    specs, errors = manager._build_collectors(source_names=["opennews"])

    assert [spec.name for spec in specs] == ["opennews"]
    assert errors == []
    assert isinstance(specs[0].collector, OpenNewsCollector)


def test_opennews_source_is_added_dynamically_when_enabled(monkeypatch) -> None:
    monkeypatch.delenv("NEWS_ENABLE_OPENNEWS", raising=False)
    monkeypatch.delenv("OPENNEWS_TOKEN", raising=False)

    manager = MultiSourceNewsCollector({"defaults": {"news_sources": ["jin10"]}})

    assert manager.sources == ["jin10"]

    monkeypatch.setenv("NEWS_ENABLE_OPENNEWS", "true")
    manager = MultiSourceNewsCollector({"defaults": {"news_sources": ["jin10"]}})

    assert manager.sources == ["jin10"]

    monkeypatch.setenv("OPENNEWS_TOKEN", "token-123")
    manager = MultiSourceNewsCollector({"defaults": {"news_sources": ["jin10"]}})

    assert manager.sources == ["jin10", "opennews"]


def test_opennews_ai_score_drives_importance() -> None:
    row = news_db._normalize_news_item(
        {
            "source": "opennews",
            "title": "Quiet update",
            "url": "https://example.com/quiet",
            "published_at": "2026-05-05T02:00:00+00:00",
            "payload": {
                "provider": "opennews",
                "opennews_ai_score": 91,
            },
        }
    )

    assert row["payload"]["importance_score"] == 91


def test_opennews_accepts_float_score_and_millisecond_timestamp() -> None:
    item = OpenNewsCollector._normalize_item(
        {
            "text": "SOL market anomaly",
            "link": "https://example.com/sol",
            "coins": [{"symbol": "SOL"}],
            "aiRating": {"score": "85.9", "signal": "short", "status": "done"},
            "ts": "1778000000000",
        }
    )

    assert item is not None
    assert item["published_at"].startswith("2026-")
    assert item["payload"]["opennews_ai_score"] == 85
    assert item["symbols"] == ["SOLUSDT"]


def test_explicit_opennews_does_not_fallback_to_other_sources_when_disabled(monkeypatch) -> None:
    monkeypatch.delenv("NEWS_ENABLE_OPENNEWS", raising=False)
    monkeypatch.delenv("OPENNEWS_TOKEN", raising=False)
    manager = MultiSourceNewsCollector({"defaults": {"news_sources": ["opennews", "jin10"]}})

    bundle = manager.pull_latest(source_names=["opennews"], max_records=20, since_minutes=60)

    assert bundle["items"] == []
    assert bundle["source_stats"] == {}
    assert bundle["pulled_total"] == 0
    assert "opennews disabled: NEWS_ENABLE_OPENNEWS is not true" in bundle["errors"]


def test_explicit_opennews_source_filter_is_normalized_before_fallback_check(monkeypatch) -> None:
    monkeypatch.delenv("NEWS_ENABLE_OPENNEWS", raising=False)
    monkeypatch.delenv("OPENNEWS_TOKEN", raising=False)
    manager = MultiSourceNewsCollector({"defaults": {"news_sources": ["opennews", "jin10"]}})

    bundle = manager.pull_latest(source_names=[" OpenNews ", "opennews"], max_records=20, since_minutes=60)

    assert bundle["items"] == []
    assert bundle["source_stats"] == {}
    assert "all configured sources unavailable; fallback to jin10/rss/gdelt" not in bundle["errors"]


def test_manager_timestamp_sort_accepts_numeric_and_millisecond_values() -> None:
    assert _parse_ts_to_unix(1_778_000_000_000) == 1_778_000_000
    assert _parse_ts_to_unix(1_778_000_000) == 1_778_000_000


def test_opennews_extract_rows_accepts_nested_api_shapes() -> None:
    assert _extract_rows({"data": [{"text": "flat"}]}) == [{"text": "flat"}]
    assert _extract_rows({"data": {"items": [{"text": "nested"}]}}) == [{"text": "nested"}]
    assert _extract_rows({"results": [{"text": "top-level"}]}) == [{"text": "top-level"}]
    assert _extract_rows({"data": {"items": ["bad", {"text": "kept"}]}}) == [{"text": "kept"}]
