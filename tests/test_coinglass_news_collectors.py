from __future__ import annotations

from datetime import datetime, timezone

from core.news.collectors.coinglass_news import (
    CoinGlassArticlesCollector,
    CoinGlassEconomicDataCollector,
    CoinGlassNewsflashCollector,
)
from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.eventizer.rules import load_news_rule_config


def _cfg():
    return load_news_rule_config()


def _now_ms(offset_sec: int = 0) -> int:
    return int((datetime.now(timezone.utc).timestamp() + offset_sec) * 1000)


def test_coinglass_newsflash_collector_normalizes_and_infers_symbols(monkeypatch):
    collector = CoinGlassNewsflashCollector(_cfg())

    async def fake_request_rows(self, client, *, page, per_page):
        return [
            {
                    "newsflash_title": "Bitcoin rebounds above $71,000 after ETF chatter.",
                    "newsflash_content": "Traders say bitcoin and ethereum caught a bid after fresh ETF headlines.",
                    "source_name": "BLOCKBEATS",
                    "source_website_logo": "https://cdn.example/logo.png",
                    "newsflash_release_time": _now_ms(-300),
                }
            ]

    monkeypatch.setattr(collector, "_request_rows", fake_request_rows.__get__(collector, CoinGlassNewsflashCollector))

    items = collector.pull_latest(max_records=10, since_minutes=24 * 60)

    assert len(items) == 1
    item = items[0]
    assert item["source"] == "coinglass_newsflash"
    assert item["title"].startswith("Bitcoin rebounds")
    assert item["payload"]["source_name"] == "BLOCKBEATS"
    assert "BTCUSDT" in item["symbols"]
    assert "ETHUSDT" in item["symbols"]
    assert item["url"].startswith("https://www.coinglass.com/news/newsflash/")


def test_coinglass_economic_data_collector_uses_macro_symbol_fallback(monkeypatch):
    collector = CoinGlassEconomicDataCollector(_cfg())

    async def fake_request_rows(self, client, *, page, per_page):
        return [
            {
                "calendar_name": "Federal Funds Benchmark Rate",
                "country_code": "USA",
                "country_name": "US",
                    "data_effect": "Minor Impact",
                    "forecast_value": "3.50% - 3.75%",
                    "previous_value": "3.50% - 3.75%",
                    "published_value": "3.50% - 3.75%",
                    "publish_timestamp": _now_ms(-60),
                    "importance_level": 3,
                }
            ]

    monkeypatch.setattr(collector, "_request_rows", fake_request_rows.__get__(collector, CoinGlassEconomicDataCollector))

    items = collector.pull_latest(max_records=10, since_minutes=24 * 60)

    assert len(items) == 1
    item = items[0]
    assert item["source"] == "coinglass_economic_data"
    assert item["title"] == "US economic data: Federal Funds Benchmark Rate"
    assert item["payload"]["importance_level"] == 3
    assert item["symbols"] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert "Forecast:" in item["content"]
    assert item["url"].startswith("https://www.coinglass.com/news/economic-data/")


def test_coinglass_articles_collector_builds_stable_articles(monkeypatch):
    collector = CoinGlassArticlesCollector(_cfg())

    async def fake_request_rows(self, client, *, page, per_page):
        return [
            {
                    "article_title": "Solana developers unveil new staking roadmap",
                    "article_content": "<p>Solana staking and validator updates are rolling out this quarter.</p>",
                    "article_release_time": _now_ms(-120),
                    "source_name": "CoinDesk",
                    "article_picture": "https://cdn.example/article.jpg",
                }
        ]

    monkeypatch.setattr(collector, "_request_rows", fake_request_rows.__get__(collector, CoinGlassArticlesCollector))

    items = collector.pull_latest(max_records=10, since_minutes=24 * 60)

    assert len(items) == 1
    item = items[0]
    assert item["source"] == "coinglass_articles"
    assert item["payload"]["article_picture"] == "https://cdn.example/article.jpg"
    assert "SOLUSDT" in item["symbols"]


def test_manager_builds_coinglass_collectors_when_enabled(monkeypatch):
    from core.news.collectors import manager as manager_module

    monkeypatch.setattr(manager_module, "coinglass_enabled", lambda: True)
    manager = MultiSourceNewsCollector(
        {
            "defaults": {
                "news_sources": [
                    "coinglass_newsflash",
                    "coinglass_articles",
                    "coinglass_economic_data",
                ]
            }
        }
    )

    specs, errors = manager._build_collectors()
    names = [spec.name for spec in specs]
    manager._close_collectors(specs)

    assert errors == []
    assert names == [
        "coinglass_newsflash",
        "coinglass_articles",
        "coinglass_economic_data",
    ]
