"""News collectors package."""

from core.news.collectors.cryptopanic import CryptoPanicCollector
from core.news.collectors.gdelt import GDELTCollector
from core.news.collectors.jin10 import Jin10Collector
from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.collectors.newsapi import NewsAPICollector
from core.news.collectors.rss import RSSNewsCollector

__all__ = [
    "GDELTCollector",
    "NewsAPICollector",
    "CryptoPanicCollector",
    "Jin10Collector",
    "RSSNewsCollector",
    "MultiSourceNewsCollector",
]
