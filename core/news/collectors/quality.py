"""Cheap pre-insert guard that drops items which are obviously not news.

Scrapers and Google News occasionally surface navigation chrome (language
selectors), exchange market pages ("UNI/USDT - Binance", "ZIZY Price Today")
or social-feed index pages. None of these carry an event, and every one that
reaches ``news_raw`` also costs an LLM labelling call, so they are rejected
before storage and again before ``news_llm_tasks`` enqueueing.

Rules are deliberately narrow: a false positive silently loses real news,
so only patterns that cannot plausibly be a headline are listed here.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Language-menu labels as rendered by exchange help centres (OKX, Binance,
# Bybit). Compared after casefolding and dropping a trailing "(region)".
_LANGUAGE_LABELS = {
    "english", "简体中文", "繁體中文", "中文", "русский", "français", "francais", "العربية",
    "українська", "tiếng việt", "polski", "español", "espanol", "bahasa indonesia",
    "čeština", "svenska", "suomi", "română", "português", "norsk", "norsk bokmål",
    "nederlands", "italiano", "deutsch", "日本語", "한국어", "türkçe", "ไทย", "magyar",
    "ελληνικά", "български", "dansk", "slovenčina", "slovenščina", "hrvatski", "srpski",
    "latviešu", "lietuvių", "eesti", "қазақ тілі", "o'zbek", "azərbaycan", "filipino",
    "tagalog", "bahasa melayu", "हिन्दी", "বাংলা", "اردو", "فارسی", "עברית", "kiswahili",
}

# Exchanges whose own site Google News relays with a " - <Exchange>" suffix.
_EXCHANGE_SUFFIX_RE = re.compile(
    r"\s+[-|]\s+(Binance|OKX|Bybit|Bitget|KuCoin|Gate(?:\.io)?|MEXC|HTX|Coinbase|Kraken)\s*$",
    re.I,
)

# Bare trading-pair titles: "UNI/USDT", "牛来/USDT Spot", "BNCBUSDT | Binance Spot".
_PAIR_TOKEN = r"[A-Za-z0-9一-鿿]{1,20}/[A-Z0-9]{2,10}"
_BARE_PAIR_RE = re.compile(rf"^\s*{_PAIR_TOKEN}\s*$")
# Ticker/price pages such as "0.003856 | PUMPU | Binance Spot" or
# "0.00000 Trade 牛来/USDT Spot | Crypto, bStocks & tCommodities".
_PRICE_TICKER_PAGE_RE = re.compile(
    rf"^\s*[\d.,]+\s*(\|\s*[A-Z0-9]{{2,20}}\s*\||Trade\s+{_PAIR_TOKEN})"
    rf"|^\s*(Trade\s+)?{_PAIR_TOKEN}\s+(Spot|Futures|Margin|Perpetual)\b"
    r"|\|\s*[A-Z0-9]{2,20}\s*\|\s*\w+\s+(Spot|Futures)\s*$",
)

# Exchange SEO pages: quotes, converters, "how to buy", social-feed indices.
_SEO_PAGE_RES = (
    re.compile(r"\bPrice Today\b.*\b(Live|Chart|Market Cap|Market Data)\b", re.I),
    re.compile(r"^\S.{0,80}\([A-Za-z0-9]{1,15}\)\s+(Stock\s+)?Price Today$", re.I),
    re.compile(r"\bPre[țt] Azi$", re.I),
    re.compile(r"\bPrice to [A-Za-z ]+\|\s*Convert\b", re.I),
    re.compile(r"^How to buy .+ in [A-Z][A-Za-z ]+$", re.I),
    re.compile(r"Community Insights & Market Sentiment \| Binance Square$", re.I),
    re.compile(r"^Latest #.+ News, Opinions and Feed Today \| Binance Square$", re.I),
    re.compile(r"\(@[^)]+\)'s insights$", re.I),
    re.compile(r"^Word of the Day: ", re.I),
)

# Site chrome that the old OKX scraper picked up from footers.
_CHROME_TITLES = {
    "announcements", "terms of service", "privacy notice", "candidate privacy notice",
    "disclosures", "law enforcement", "whistleblower notice", "cookie policy",
}


def _normalize_title(title: str) -> str:
    text = re.sub(r"\s+", " ", str(title or "")).strip()
    return _EXCHANGE_SUFFIX_RE.sub("", text).strip()


def _is_language_label(title: str) -> bool:
    key = title.casefold().strip()
    if key in _LANGUAGE_LABELS:
        return True
    base = re.sub(r"\s*\([^)]*\)\s*$", "", key).strip()
    return bool(base) and base != key and base in _LANGUAGE_LABELS


def junk_reason(item: Dict[str, Any]) -> Optional[str]:
    """Return why ``item`` is not news, or ``None`` if it should be kept."""
    if not isinstance(item, dict):
        return "not_a_dict"
    raw_title = str(item.get("title") or "")
    title = _normalize_title(raw_title)
    if not title:
        # Some flash feeds carry the whole story in ``content``; only a row with
        # neither is empty.
        return None if str(item.get("content") or "").strip() else "empty"
    if _is_language_label(title):
        return "language_label"
    if title.casefold() in _CHROME_TITLES:
        return "site_chrome"
    if _BARE_PAIR_RE.match(title) or _PRICE_TICKER_PAGE_RE.search(title):
        return "pair_page"
    for pattern in _SEO_PAGE_RES:
        if pattern.search(title) or pattern.search(raw_title.strip()):
            return "exchange_seo_page"
    return None


def is_junk_news_item(item: Dict[str, Any]) -> bool:
    return junk_reason(item) is not None


def split_junk(items: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Return ``(kept, {reason: dropped_count})``."""
    kept: List[Dict[str, Any]] = []
    dropped: Dict[str, int] = {}
    for item in items or []:
        reason = junk_reason(item)
        if reason is None:
            kept.append(item)
        else:
            dropped[reason] = dropped.get(reason, 0) + 1
    return kept, dropped
