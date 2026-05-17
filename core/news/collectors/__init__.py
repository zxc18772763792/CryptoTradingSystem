"""News collectors package with lazy collector exports."""
from __future__ import annotations

from typing import Any


__all__ = [
    "GDELTCollector",
    "NewsAPICollector",
    "OpenNewsCollector",
    "CryptoPanicCollector",
    "Jin10Collector",
    "RSSNewsCollector",
    "CoinGlassNewsflashCollector",
    "CoinGlassArticlesCollector",
    "CoinGlassEconomicDataCollector",
    "CoinGlassFinancialEventsCollector",
    "CoinGlassCentralBankCollector",
    "MultiSourceNewsCollector",
]


def __getattr__(name: str) -> Any:
    if name == "GDELTCollector":
        from core.news.collectors.gdelt import GDELTCollector

        return GDELTCollector
    if name == "NewsAPICollector":
        from core.news.collectors.newsapi import NewsAPICollector

        return NewsAPICollector
    if name == "OpenNewsCollector":
        from core.news.collectors.opennews import OpenNewsCollector

        return OpenNewsCollector
    if name == "CryptoPanicCollector":
        from core.news.collectors.cryptopanic import CryptoPanicCollector

        return CryptoPanicCollector
    if name == "Jin10Collector":
        from core.news.collectors.jin10 import Jin10Collector

        return Jin10Collector
    if name == "RSSNewsCollector":
        from core.news.collectors.rss import RSSNewsCollector

        return RSSNewsCollector
    if name == "CoinGlassNewsflashCollector":
        from core.news.collectors.coinglass_news import CoinGlassNewsflashCollector

        return CoinGlassNewsflashCollector
    if name == "CoinGlassArticlesCollector":
        from core.news.collectors.coinglass_news import CoinGlassArticlesCollector

        return CoinGlassArticlesCollector
    if name == "CoinGlassEconomicDataCollector":
        from core.news.collectors.coinglass_news import CoinGlassEconomicDataCollector

        return CoinGlassEconomicDataCollector
    if name == "CoinGlassFinancialEventsCollector":
        from core.news.collectors.coinglass_news import CoinGlassFinancialEventsCollector

        return CoinGlassFinancialEventsCollector
    if name == "CoinGlassCentralBankCollector":
        from core.news.collectors.coinglass_news import CoinGlassCentralBankCollector

        return CoinGlassCentralBankCollector
    if name == "MultiSourceNewsCollector":
        from core.news.collectors.manager import MultiSourceNewsCollector

        return MultiSourceNewsCollector
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
