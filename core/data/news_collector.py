"""
财经新闻数据采集和分析模块
支持多个数据源的加密货币新闻采集和情感分析
"""
import asyncio
import aiohttp
import feedparser
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path
import json
import re

from loguru import logger
import pandas as pd


@dataclass
class NewsItem:
    """新闻条目"""
    title: str
    summary: str
    source: str
    url: str
    published_at: datetime
    sentiment: float = 0.0  # -1 到 1, 负面到正面
    keywords: List[str] = field(default_factory=list)
    category: str = "general"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "summary": self.summary,
            "source": self.source,
            "url": self.url,
            "published_at": self.published_at.isoformat(),
            "sentiment": self.sentiment,
            "keywords": self.keywords,
            "category": self.category,
        }


class NewsCollector:
    """新闻采集器"""

    def __init__(self, storage_path: str = "./data/news"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

        # RSS源配置
        self.rss_sources = {
            # 英文源
            "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "cointelegraph": "https://cointelegraph.com/rss",
            "bitcoinist": "https://bitcoinist.com/feed/",
            "newsbtc": "https://www.newsbtc.com/feed/",
            "cryptoslate": "https://cryptoslate.com/feed/",
            # 中文源
            "jinse": "https://www.jinse.com/live/rss",  # 金色财经
            "8btc": "https://www.8btc.com/rss",  # 巴比特
            "chainnews": "https://www.chainnews.com/rss.xml",
        }

        # API源配置
        self.api_sources = {
            "cryptopanic": "https://cryptopanic.com/api/v1/posts/",
            "newsapi": "https://newsapi.org/v2/everything",
        }

        # 关键词权重 (用于情感分析)
        self.bullish_keywords = [
            "bullish", "surge", "rally", "gain", "rise", "soar", "breakthrough",
            "adoption", "approval", "etf", "institutional", "upgrade", "launch",
            "partnership", "integration", "milestone", "record", "high",
            "看涨", "暴涨", "突破", "利好", " adoption", "批准", "升级",
        ]

        self.bearish_keywords = [
            "bearish", "crash", "dump", "fall", "drop", "decline", "sell-off",
            "hack", "exploit", "ban", "regulation", "lawsuit", "fraud", "scam",
            "collapse", "bankruptcy", "liquidation", "fear", "concern",
            "看跌", "暴跌", "崩盘", "利空", "监管", "黑客", "诈骗",
        ]

        # 加密货币关键词
        self.crypto_keywords = [
            "bitcoin", "btc", "ethereum", "eth", "crypto", "blockchain",
            "defi", "nft", "web3", "altcoin", "stablecoin",
            "比特币", "以太坊", "加密货币", "区块链",
        ]

    async def fetch_rss(self, source_name: str, url: str) -> List[NewsItem]:
        """获取RSS新闻"""
        news_items = []

        try:
            # 使用feedparser解析RSS
            feed = feedparser.parse(url)

            for entry in feed.entries[:50]:  # 限制50条
                try:
                    # 解析发布时间
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        published_at = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                        published_at = datetime(*entry.updated_parsed[:6])
                    else:
                        published_at = datetime.now()

                    # 获取摘要
                    summary = ""
                    if hasattr(entry, "summary"):
                        summary = entry.summary
                    elif hasattr(entry, "description"):
                        summary = entry.description

                    # 清理HTML标签
                    summary = re.sub(r"<[^>]+>", "", summary)
                    summary = summary[:500]  # 限制长度

                    news_item = NewsItem(
                        title=entry.get("title", ""),
                        summary=summary,
                        source=source_name,
                        url=entry.get("link", ""),
                        published_at=published_at,
                    )

                    news_items.append(news_item)

                except Exception as e:
                    logger.warning(f"解析RSS条目失败: {e}")
                    continue

            logger.info(f"从 {source_name} 获取 {len(news_items)} 条新闻")

        except Exception as e:
            logger.error(f"获取RSS失败 {source_name}: {e}")

        return news_items

    async def fetch_cryptopanic(self, api_key: Optional[str] = None) -> List[NewsItem]:
        """从CryptoPanic获取新闻 (需要API key)"""
        news_items = []

        if not api_key:
            logger.warning("CryptoPanic需要API key")
            return news_items

        try:
            async with aiohttp.ClientSession() as session:
                url = self.api_sources["cryptopanic"]
                params = {
                    "auth_token": api_key,
                    "currencies": "BTC,ETH",
                    "filter": "rising",
                }

                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()

                        for item in data.get("results", [])[:50]:
                            news_item = NewsItem(
                                title=item.get("title", ""),
                                summary=item.get("title", ""),
                                source=item.get("source", {}).get("domain", "cryptopanic"),
                                url=item.get("url", ""),
                                published_at=datetime.fromisoformat(
                                    item.get("published_at", "").replace("Z", "+00:00")
                                ) if item.get("published_at") else datetime.now(),
                            )
                            news_items.append(news_item)

                        logger.info(f"从CryptoPanic获取 {len(news_items)} 条新闻")

        except Exception as e:
            logger.error(f"CryptoPanic API失败: {e}")

        return news_items

    async def fetch_fear_greed_index(self) -> Dict[str, Any]:
        """获取恐惧贪婪指数"""
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://api.alternative.me/fng/"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("data"):
                            latest = data["data"][0]
                            return {
                                "value": int(latest["value"]),
                                "classification": latest["value_classification"],
                                "timestamp": datetime.fromtimestamp(int(latest["timestamp"])),
                            }
        except Exception as e:
            logger.error(f"获取恐惧贪婪指数失败: {e}")

        return {"value": 50, "classification": "Neutral", "timestamp": datetime.now()}

    def analyze_sentiment(self, text: str) -> float:
        """简单情感分析（后半段关键词权重1.5x）"""
        text_lower = text.lower()
        mid = len(text_lower) // 2
        first_half = text_lower[:mid]
        second_half = text_lower[mid:]

        bullish_score = (
            sum(1.0 for kw in self.bullish_keywords if kw in first_half)
            + sum(1.5 for kw in self.bullish_keywords if kw in second_half)
        )
        bearish_score = (
            sum(1.0 for kw in self.bearish_keywords if kw in first_half)
            + sum(1.5 for kw in self.bearish_keywords if kw in second_half)
        )

        total = bullish_score + bearish_score
        if total == 0:
            return 0.0

        sentiment = (bullish_score - bearish_score) / total
        return round(sentiment, 3)

    def extract_keywords(self, text: str) -> List[str]:
        """提取关键词"""
        text_lower = text.lower()
        found = []

        for kw in self.crypto_keywords:
            if kw in text_lower:
                found.append(kw)

        return list(set(found))[:5]  # 最多5个

    def categorize_news(self, title: str, summary: str) -> str:
        """分类新闻"""
        text = (title + " " + summary).lower()

        if any(kw in text for kw in ["etf", "sec", "regulation", "监管"]):
            return "regulation"
        elif any(kw in text for kw in ["defi", "uniswap", "aave", "lending"]):
            return "defi"
        elif any(kw in text for kw in ["nft", "opensea"]):
            return "nft"
        elif any(kw in text for kw in ["exchange", "trading", "交易", "交易所"]):
            return "exchange"
        elif any(kw in text for kw in ["bitcoin", "btc", "比特币"]):
            return "bitcoin"
        elif any(kw in text for kw in ["ethereum", "eth", "以太坊"]):
            return "ethereum"
        else:
            return "general"

    async def collect_all_news(self) -> List[NewsItem]:
        """从所有源采集新闻"""
        all_news = []

        # 并行获取所有RSS源
        tasks = []
        for source_name, url in self.rss_sources.items():
            tasks.append(self.fetch_rss(source_name, url))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, list):
                all_news.extend(result)

        # 分析情感和关键词
        for news in all_news:
            full_text = news.title + " " + news.summary
            news.sentiment = self.analyze_sentiment(full_text)
            news.keywords = self.extract_keywords(full_text)
            news.category = self.categorize_news(news.title, news.summary)

        # 按时间排序
        all_news.sort(key=lambda x: x.published_at, reverse=True)

        # 去重 (基于标题相似度)
        unique_news = []
        seen_titles = set()

        for news in all_news:
            title_key = news.title.lower()[:50]
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_news.append(news)

        logger.info(f"总共采集 {len(unique_news)} 条唯一新闻")
        return unique_news

    def save_news(self, news_items: List[NewsItem], filename: str = None):
        """保存新闻到文件"""
        if not filename:
            filename = f"news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        filepath = self.storage_path / filename

        data = [news.to_dict() for news in news_items]

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"保存 {len(news_items)} 条新闻到 {filepath}")
        return filepath

    def get_sentiment_summary(self, news_items: List[NewsItem]) -> Dict[str, Any]:
        """获取情感分析摘要"""
        if not news_items:
            return {"average": 0, "bullish": 0, "bearish": 0, "neutral": 0}

        sentiments = [n.sentiment for n in news_items]
        avg_sentiment = sum(sentiments) / len(sentiments)

        bullish = sum(1 for s in sentiments if s > 0.1)
        bearish = sum(1 for s in sentiments if s < -0.1)
        neutral = len(sentiments) - bullish - bearish

        return {
            "average": round(avg_sentiment, 3),
            "bullish_count": bullish,
            "bearish_count": bearish,
            "neutral_count": neutral,
            "total": len(news_items),
            "interpretation": self._interpret_sentiment(avg_sentiment),
        }

    def _interpret_sentiment(self, sentiment: float) -> str:
        """解释情感分数"""
        if sentiment > 0.3:
            return "非常乐观"
        elif sentiment > 0.1:
            return "乐观"
        elif sentiment > -0.1:
            return "中性"
        elif sentiment > -0.3:
            return "悲观"
        else:
            return "非常悲观"

    def get_category_distribution(self, news_items: List[NewsItem]) -> Dict[str, int]:
        """获取类别分布"""
        distribution = {}
        for news in news_items:
            distribution[news.category] = distribution.get(news.category, 0) + 1
        return distribution


async def main():
    """测试新闻采集"""
    collector = NewsCollector()

    print("=" * 60)
    print("加密货币新闻采集测试")
    print("=" * 60)

    # 采集新闻
    news = await collector.collect_all_news()

    # 获取恐惧贪婪指数
    fng = await collector.fetch_fear_greed_index()
    print(f"\n恐惧贪婪指数: {fng['value']} ({fng['classification']})")

    # 情感分析摘要
    summary = collector.get_sentiment_summary(news)
    print(f"\n情感分析摘要:")
    print(f"  平均情感: {summary['average']} ({summary['interpretation']})")
    print(f"  看涨: {summary['bullish_count']}, 看跌: {summary['bearish_count']}, 中性: {summary['neutral_count']}")

    # 类别分布
    categories = collector.get_category_distribution(news)
    print(f"\n类别分布:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count}")

    # 显示最新10条新闻
    print(f"\n最新10条新闻:")
    for i, item in enumerate(news[:10], 1):
        sentiment_emoji = "🟢" if item.sentiment > 0.1 else "🔴" if item.sentiment < -0.1 else "⚪"
        print(f"{i:2}. {sentiment_emoji} [{item.source:12}] {item.title[:60]}...")

    # 保存新闻
    collector.save_news(news)


if __name__ == "__main__":
    asyncio.run(main())
