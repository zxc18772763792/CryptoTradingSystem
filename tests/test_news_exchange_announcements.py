from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from core.news.collectors.binance_announcements import (
    BinanceAnnouncementsCollector,
    parse_binance_cms_articles,
)
from core.news.collectors.exchange_events import classify_exchange_announcement
from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.collectors.okx_announcements import OKXAnnouncementsCollector, parse_okx_announcements
from core.news.collectors.quality import junk_reason, split_junk


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


NOW = datetime.now(timezone.utc).replace(microsecond=0)


def _binance_payload(release: datetime) -> Dict[str, Any]:
    # Shape returned by GET cms/article/list/query?catalogId=48 (captured 2026-09-26).
    return {
        "code": "000000",
        "data": {
            "catalogs": [
                {
                    "catalogId": 48,
                    "catalogName": "New Cryptocurrency Listing",
                    "articles": [
                        {
                            "id": 285492,
                            "code": "d49de208bb6d4a26b02677d7c3d89b5d",
                            "title": "Binance Will List Hyperliquid (HYPE) with Seed Tag Applied",
                            "type": 1,
                            "releaseDate": _ms(release),
                        },
                        {
                            "id": 285396,
                            "code": "8bd2b3a7684749bba472e71fc3ad8043",
                            "title": "Binance Futures Will Launch OURAUSDT USDⓈ-Margined Perpetual Contract (2026-09-23)",
                            "type": 1,
                            "releaseDate": _ms(release - timedelta(days=3)),
                        },
                        {"id": 1, "code": "", "title": "missing code", "releaseDate": _ms(release)},
                    ],
                }
            ]
        },
    }


def _okx_payload(p_time: datetime) -> Dict[str, Any]:
    # Shape returned by GET /api/v5/support/announcements (captured 2026-09-26).
    return {
        "code": "0",
        "msg": "",
        "data": [
            {
                "details": [
                    {
                        "annType": "announcements-new-listings",
                        "title": "OKX to list perpetual futures for KII crypto",
                        "url": "https://www.okx.com/help/okx-to-list-perpetual-futures-for-kii-crypto",
                        "pTime": str(_ms(p_time)),
                        "businessPTime": str(_ms(p_time)),
                    },
                    {
                        "annType": "announcements-delistings",
                        "title": "OKX to delist ABC spot trading pairs",
                        "url": "https://www.okx.com/help/okx-to-delist-abc",
                        "pTime": str(_ms(p_time - timedelta(days=5))),
                    },
                ],
                "totalPage": "1",
            }
        ],
    }


class _Resp:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    def json(self) -> Dict[str, Any]:
        return self._payload


# --- Binance -----------------------------------------------------------------


def test_parse_binance_cms_uses_release_date_and_catalog():
    items = parse_binance_cms_articles(_binance_payload(NOW))

    assert [i["title"][:21] for i in items] == ["Binance Will List Hyp", "Binance Futures Will "]
    first = items[0]
    assert first["source"] == "binance"
    assert first["url"] == "https://www.binance.com/en/support/announcement/detail/d49de208bb6d4a26b02677d7c3d89b5d"
    assert first["published_at"] == NOW.isoformat()
    assert first["payload"]["catalog_id"] == 48
    assert first["payload"]["event_type"] == "listing"
    assert items[1]["payload"]["event_type"] == "futures_launch"


def test_parse_binance_cms_accepts_flat_articles_and_since_filter():
    payload = {"data": {"articles": _binance_payload(NOW)["data"]["catalogs"][0]["articles"]}}
    items = parse_binance_cms_articles(payload, since_ts=NOW - timedelta(days=1))
    assert len(items) == 1
    assert parse_binance_cms_articles({"data": None}) == []


def test_binance_collector_queries_catalogs_with_get(monkeypatch: pytest.MonkeyPatch):
    collector = BinanceAnnouncementsCollector({"defaults": {"binance_announcements_categories": ["listing", "delisting"]}})
    calls: List[Dict[str, Any]] = []

    def fake_request(url: str, **kwargs: Any) -> _Resp:
        calls.append({"url": url, **kwargs})
        return _Resp(_binance_payload(NOW) if kwargs["params"]["catalogId"] == 48 else {"data": {"catalogs": []}})

    monkeypatch.setattr(collector, "_request", fake_request)
    items, cursor = collector.pull_incremental(since_minutes=24 * 60, cursor=str(NOW.timestamp() + 3600))

    assert [c["params"]["catalogId"] for c in calls] == [48, 161]
    assert all(c.get("method", "GET") == "GET" for c in calls)
    # A cursor ahead of the item must not hide it: dedupe happens by URL on insert.
    assert len(items) == 1 and items[0]["payload"]["event_type"] == "listing"
    assert float(cursor) == pytest.approx(NOW.timestamp())


def test_binance_collector_raises_when_every_catalog_fails(monkeypatch: pytest.MonkeyPatch):
    collector = BinanceAnnouncementsCollector({})

    def boom(url: str, **kwargs: Any) -> _Resp:
        raise RuntimeError("403 Forbidden")

    monkeypatch.setattr(collector, "_request", boom)
    with pytest.raises(RuntimeError, match="403"):
        collector.pull_latest()


# --- OKX ---------------------------------------------------------------------


def test_parse_okx_announcements_uses_ptime():
    items = parse_okx_announcements(_okx_payload(NOW))

    assert [i["payload"]["event_type"] for i in items] == ["futures_launch", "delisting"]
    assert items[0]["source"] == "okx"
    assert items[0]["published_at"] == NOW.isoformat()
    assert items[0]["url"].startswith("https://www.okx.com/help/")
    assert items[0]["payload"]["ann_type"] == "announcements-new-listings"


def test_parse_okx_announcements_rejects_api_error():
    with pytest.raises(RuntimeError, match="50011"):
        parse_okx_announcements({"code": "50011", "msg": "Too Many Requests", "data": []})


def test_okx_collector_merges_types_and_applies_window(monkeypatch: pytest.MonkeyPatch):
    collector = OKXAnnouncementsCollector({})
    seen_types: List[str] = []

    def fake_request(url: str, **kwargs: Any) -> _Resp:
        seen_types.append(kwargs["params"]["annType"])
        return _Resp(_okx_payload(NOW))

    monkeypatch.setattr(collector, "_request", fake_request)
    items = collector.pull_latest(since_minutes=60)

    assert seen_types == ["announcements-new-listings", "announcements-delistings"]
    # Same URLs from both responses are de-duplicated; the 5-day-old one is outside the window.
    assert [i["title"] for i in items] == ["OKX to list perpetual futures for KII crypto"]


# --- classifier ----------------------------------------------------------------


@pytest.mark.parametrize(
    "title, expected",
    [
        ("Binance Will List Fabric Protocol (ROBO) with Seed Tag Applied", "listing"),
        ("Binance Will Add Opinion (OPN) on Earn, Buy Crypto, Convert, VIP Loan, Margin & Futures", "listing"),
        ("OKX to list METUSD, ARUSD, COREUSD X-Perps", "futures_launch"),
        ("Binance Futures Will Launch USDⓈ-Margined COPPERUSDT Perpetual Contract (2026-03-06)", "futures_launch"),
        ("Notice of Removal of Spot Trading Pairs - 2026-09-25", "delisting"),
        ("OKX to support new USDC spot trading pairs", "listing"),
        ("Binance Will Delist ABC, DEF on 2026-10-01", "delisting"),
        ("Binance Earn New Listing Special Offer: Subscribe to OPN Locked Products", "other"),
        ("OKX to Adjust KIOXIA Equity Perpetual Futures Due to Corporate Action", "other"),
    ],
)
def test_classify_exchange_announcement(title: str, expected: str):
    assert classify_exchange_announcement(title) == expected


# --- junk guard ----------------------------------------------------------------


@pytest.mark.parametrize(
    "title, reason",
    [
        ("English", "language_label"),
        ("Português (Brasil)", "language_label"),
        ("Bahasa Indonesia", "language_label"),
        ("العربية", "language_label"),
        ("简体中文", "language_label"),
        ("Español (Latinoamérica)", "language_label"),
        ("Privacy Notice", "site_chrome"),
        ("UNI/USDT - Binance", "pair_page"),
        ("ETH/USDT", "pair_page"),
        ("0.003856 | PUMPU | Binance Spot - Binance", "pair_page"),
        ("0.00000 Trade 牛来/USDT Spot | Crypto, bStocks & tCommodities - Binance", "pair_page"),
        ("KT Corp. (KT) Stock Price Today - Binance", "exchange_seo_page"),
        ("ZIZY Price Today | Live ZIZY Price, Chart & Market Data - Binance", "exchange_seo_page"),
        ("Oklo Inc. Price Today | OKLO Live Price, Chart & Market Cap - OKX", "exchange_seo_page"),
        ("LEO Price to Vietnamese Dong | Convert LEO to VND - Binance", "exchange_seo_page"),
        ("How to buy Western Digital Corporation (WDC) in United States - OKX", "exchange_seo_page"),
        ("Investor (INVESTOR) Preț Azi - Bybit", "exchange_seo_page"),
        ("#usdcfreezedebate Community Insights & Market Sentiment | Binance Square - Binance", "exchange_seo_page"),
        ("Crypto Sat(@CryptoSatRed)'s insights - Binance", "exchange_seo_page"),
    ],
)
def test_junk_reason_flags_non_news(title: str, reason: str):
    assert junk_reason({"title": title}) == reason


@pytest.mark.parametrize(
    "title",
    [
        "Binance Will List Hyperliquid (HYPE) with Seed Tag Applied",
        "OKX to list SEIUSD, AGLDUSD, CFXUSD X-Perps - OKX",
        "Bitcoin price today: BTC slips below $60K as ETF outflows mount",
        "English Premier League club signs crypto sponsorship deal",
        "1926 | « Dejó esta vida en la edad de las ilusiones »",
        "ETH/USDT funding flips negative as traders pile into shorts",
        "Market News: Bitcoin Slips to $63,000 as Global Chip Rout Deepens - Binance",
    ],
)
def test_junk_reason_keeps_real_headlines(title: str):
    assert junk_reason({"title": title}) is None


def test_empty_title_kept_only_when_content_present():
    assert junk_reason({"title": "", "content": "美联储宣布维持利率不变"}) is None
    assert junk_reason({"title": "", "content": ""}) == "empty"


def test_split_junk_counts_reasons():
    kept, dropped = split_junk([{"title": "English"}, {"title": "Italiano"}, {"title": "BTC ETF inflows hit record"}])
    assert [i["title"] for i in kept] == ["BTC ETF inflows hit record"]
    assert dropped == {"language_label": 2}


def test_manager_merge_drops_junk_and_reports_it():
    items = [
        {"title": "Русский", "url": "https://www.okx.com/ru/help/x", "published_at": NOW.isoformat(), "provider": "okx_announcements"},
        {"title": "OKX to list KIIUSD X-Perp", "url": "https://www.okx.com/help/y", "published_at": NOW.isoformat(), "provider": "okx_announcements"},
    ]
    stats: Dict[str, Dict[str, Any]] = {"okx_announcements": {"enabled": True, "pulled_count": 2, "kept_count": 0, "errors": []}}

    result = MultiSourceNewsCollector._merge_results(items, stats, [], max_records=50)

    assert [i["title"] for i in result["items"]] == ["OKX to list KIIUSD X-Perp"]
    assert result["junk_dropped_total"] == 1
    assert stats["okx_announcements"]["junk_dropped"] == {"language_label": 1}
    assert stats["okx_announcements"]["kept_count"] == 1


# --- storage guards ------------------------------------------------------------


def test_save_news_raw_and_enqueue_skip_junk(tmp_path, monkeypatch):
    import asyncio

    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    from core.news.storage import db as news_db
    from core.news.storage.models import NewsBase, NewsLLMTask, NewsRaw

    async def _run():
        engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'news.db').as_posix()}", poolclass=NullPool)
        monkeypatch.setattr(news_db, "NewsSessionLocal", async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False))
        async with engine.begin() as conn:
            await conn.run_sync(NewsBase.metadata.create_all)
        try:
            ts = NOW.isoformat()
            result = await news_db.save_news_raw(
                [
                    {"source": "okx", "title": "Italiano", "url": "https://www.okx.com/it/help/a", "published_at": ts},
                    {"source": "Binance", "title": "UNI/USDT - Binance", "url": "https://news.google.com/x", "published_at": ts},
                    {
                        "source": "binance",
                        "title": "Binance Will List Hyperliquid (HYPE) with Seed Tag Applied",
                        "url": "https://www.binance.com/en/support/announcement/detail/abc",
                        "published_at": ts,
                        "payload": {"provider": "binance_announcements"},
                    },
                ]
            )
            assert result["junk_dropped_count"] == 2
            assert [row["title"] for row in result["inserted"]] == ["Binance Will List Hyperliquid (HYPE) with Seed Tag Applied"]
            async with news_db.news_session_scope() as session:
                assert (await session.execute(select(func.count()).select_from(NewsRaw))).scalar_one() == 1

            real = dict(result["inserted"][0])
            junk = {"id": 999, "source": "okx", "title": "Deutsch", "payload": {"importance_score": 90}}
            queue = await news_db.enqueue_llm_tasks([real, junk], min_importance=0)
            assert queue["queued_count"] == 1 and queue["skipped_count"] == 1
            async with news_db.news_session_scope() as session:
                ids = (await session.execute(select(NewsLLMTask.raw_news_id))).scalars().all()
            assert ids == [real["id"]]
        finally:
            await engine.dispose()

    asyncio.run(_run())
