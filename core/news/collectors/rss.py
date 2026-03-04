"""RSS collector for free multi-source crypto/news ingestion."""
from __future__ import annotations

import html
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus
from xml.etree import ElementTree as ET

import requests
from dateutil import parser as dt_parser
from loguru import logger


def _strip_html(text: str) -> str:
    value = str(text or "")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return html.unescape(value)


class RSSNewsCollector:
    """Pull crypto-related headlines from public RSS/Atom feeds."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        defaults = cfg.get("defaults") or {}
        self.timeout_sec = int(defaults.get("rss_timeout_sec") or 20)
        self.max_records = int(defaults.get("rss_max_records") or 120)
        self.query_template = str(
            defaults.get("rss_query_template")
            or "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        )
        raw_feeds = defaults.get("rss_feeds") or [
            {"name": "coindesk", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
            {"name": "cointelegraph", "url": "https://cointelegraph.com/rss"},
            {"name": "decrypt", "url": "https://decrypt.co/feed"},
            {"name": "google_crypto", "url": "https://news.google.com/rss/search?q=bitcoin%20OR%20ethereum%20OR%20crypto%20OR%20etf&hl=en-US&gl=US&ceid=US:en"},
            {"name": "google_macro", "url": "https://news.google.com/rss/search?q=fed%20OR%20interest%20rate%20OR%20inflation%20crypto&hl=en-US&gl=US&ceid=US:en"},
        ]

        feeds: List[Dict[str, str]] = []
        for item in raw_feeds:
            if isinstance(item, str):
                url = item.strip()
                if url:
                    feeds.append({"name": "rss", "url": url})
                continue
            if isinstance(item, dict):
                name = str(item.get("name") or "rss").strip() or "rss"
                url = str(item.get("url") or "").strip()
                if url:
                    feeds.append({"name": name, "url": url})
        self.feeds = feeds

    @staticmethod
    def _parse_ts(value: Any) -> str:
        if not value:
            return datetime.now(timezone.utc).isoformat()
        try:
            dt = dt_parser.parse(str(value).strip())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.isoformat()
        except Exception:
            return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _first_text(node: ET.Element, tags: List[str]) -> str:
        for tag in tags:
            el = node.find(tag)
            if el is not None:
                if el.text and str(el.text).strip():
                    return str(el.text).strip()
                href = str(el.attrib.get("href") or "").strip()
                if href:
                    return href
        return ""

    @staticmethod
    def _normalize_url(url: str) -> str:
        text = str(url or "").strip()
        if not text:
            return ""
        if text.startswith("//"):
            return f"https:{text}"
        return text

    def _parse_feed(self, feed_name: str, feed_url: str, body: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        root = ET.fromstring(body)

        # RSS item
        items = root.findall(".//item")
        for node in items:
            title = _strip_html(self._first_text(node, ["title"]))
            link = self._normalize_url(self._first_text(node, ["link", "{*}link"]))
            pub = self._first_text(node, ["pubDate", "{*}pubDate", "published", "{*}published", "updated", "{*}updated"])
            desc = _strip_html(self._first_text(node, ["description", "{*}description", "summary", "{*}summary"]))
            source = _strip_html(self._first_text(node, ["source", "{*}source"])) or feed_name
            if not link:
                guid = self._first_text(node, ["guid", "{*}guid"])
                link = self._normalize_url(guid)
            if not title or not link:
                continue
            out.append(
                {
                    "source": source,
                    "title": title[:600],
                    "url": link,
                    "content": desc[:2000],
                    "published_at": self._parse_ts(pub),
                    "lang": "en",
                    "payload": {"provider": "rss", "feed": feed_name, "feed_url": feed_url},
                }
            )

        # Atom entry
        entries = root.findall(".//{*}entry")
        for node in entries:
            title = _strip_html(self._first_text(node, ["{*}title", "title"]))
            link = self._normalize_url(self._first_text(node, ["{*}link", "link"]))
            pub = self._first_text(node, ["{*}published", "{*}updated", "published", "updated"])
            desc = _strip_html(self._first_text(node, ["{*}summary", "{*}content", "summary", "content"]))
            source = feed_name
            if not title or not link:
                continue
            out.append(
                {
                    "source": source,
                    "title": title[:600],
                    "url": link,
                    "content": desc[:2000],
                    "published_at": self._parse_ts(pub),
                    "lang": "en",
                    "payload": {"provider": "rss", "feed": feed_name, "feed_url": feed_url},
                }
            )
        return out

    def _build_sources(self, query: Optional[str]) -> List[Dict[str, str]]:
        sources = list(self.feeds)
        text = str(query or "").strip()
        if text:
            url = self.query_template.format(query=quote_plus(text))
            sources.insert(0, {"name": "google_query", "url": url})
        return sources

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        max_records = max(10, min(int(max_records or self.max_records), 300))
        since_minutes = max(15, min(int(since_minutes or 240), 24 * 60))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)

        all_items: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        headers = {"User-Agent": "crypto-trading-system/1.0 (+rss)"}

        for source in self._build_sources(query):
            name = str(source.get("name") or "rss").strip() or "rss"
            url = str(source.get("url") or "").strip()
            if not url:
                continue
            try:
                resp = requests.get(url, timeout=self.timeout_sec, headers=headers)
                resp.raise_for_status()
                body = resp.text or ""
                parsed = self._parse_feed(name, url, body)
            except Exception as e:
                logger.warning(f"RSS feed error for {url}: {type(e).__name__}: {e}")
                continue

            for item in parsed:
                item_url = str(item.get("url") or "").strip()
                if not item_url or item_url in seen_urls:
                    continue
                try:
                    ts = dt_parser.parse(str(item.get("published_at") or ""))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    else:
                        ts = ts.astimezone(timezone.utc)
                    if ts < since_ts:
                        continue
                except Exception:
                    pass
                seen_urls.add(item_url)
                all_items.append(item)

        all_items.sort(
            key=lambda x: dt_parser.parse(str(x.get("published_at") or datetime.now(timezone.utc).isoformat())).timestamp(),
            reverse=True,
        )
        return all_items[:max_records]
