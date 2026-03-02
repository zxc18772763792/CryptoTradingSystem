from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from core.news.collectors.common import BeautifulSoup, BaseNewsCollector, discover_rss_links, parse_datetime_utc, parse_rss_items


class BybitAnnouncementsCollector(BaseNewsCollector):
    provider_name = "bybit_announcements"
    endpoint = "https://www.bybit.com/en/help-center/article/Announcements"
    candidate_rss = [
        "https://announcements.bybit.com/en/rss",
        "https://announcements.bybit.com/rss",
        "https://www.bybit.com/en/help-center/rss",
    ]

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        self.endpoint = str(defaults.get("bybit_announcements_endpoint") or self.endpoint)
        self.timeout_sec = min(self.timeout_sec, 8)
        self.retry_count = 0
        if self.min_interval_sec <= 0:
            self.min_interval_sec = float(defaults.get("bybit_announcements_min_interval_sec") or 25.0)
        if self.jitter_sec <= 0:
            self.jitter_sec = float(defaults.get("bybit_announcements_jitter_sec") or 1.0)

    def _try_rss(self, html: str) -> List[Dict[str, Any]]:
        discovered = discover_rss_links(html, self.endpoint)
        links = discovered if discovered else list(self.candidate_rss[:1])
        seen: set[str] = set()
        original_timeout = self.timeout_sec
        for link in links:
            if link in seen:
                continue
            seen.add(link)
            try:
                self.timeout_sec = min(original_timeout, 2)
                resp = self._request(link)
                body = resp.text or ""
                if "<rss" not in body.lower() and "<feed" not in body.lower():
                    continue
                items = parse_rss_items(body, self.provider_name, "bybit", link)
                if items:
                    self.timeout_sec = original_timeout
                    return items
            except Exception:
                continue
            finally:
                self.timeout_sec = original_timeout
        return []

    def _parse_html(self, html: str) -> List[Dict[str, Any]]:
        if not BeautifulSoup:
            return []
        soup = BeautifulSoup(html, "html.parser")
        items: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for anchor in soup.select('a[href*="/help-center/article/"]'):
            href = str(anchor.get("href") or "").strip()
            if not href or href.endswith("/Announcements"):
                continue
            title = anchor.get_text(" ", strip=True)
            if not title:
                continue
            abs_url = urljoin(self.endpoint, href)
            if abs_url in seen:
                continue
            seen.add(abs_url)
            container = anchor.find_parent(["article", "div", "li"]) or anchor.parent
            text = container.get_text(" ", strip=True) if container else title
            pub_match = re.search(r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})", text)
            published = parse_datetime_utc(pub_match.group(1)).isoformat() if pub_match else datetime.now(timezone.utc).isoformat()
            items.append(
                {
                    "source": "bybit",
                    "title": title[:600],
                    "url": abs_url,
                    "content": re.sub(r"\s+", " ", text)[:1600],
                    "published_at": published,
                    "lang": "en",
                    "payload": {"provider": self.provider_name, "origin": "html"},
                }
            )
        return items

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query
        limit = max(5, min(int(max_records or self.max_records), 120))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(since_minutes or 240)))
        response = self._request(self.endpoint)
        html = response.text or ""
        items = self._try_rss(html)
        if not items:
            items = self._parse_html(html)
        out: List[Dict[str, Any]] = []
        for item in items:
            try:
                if parse_datetime_utc(item.get("published_at")) < since_ts:
                    continue
            except Exception:
                pass
            out.append(item)
            if len(out) >= limit:
                break
        return out

    def pull_incremental(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        items = self.pull_latest(query=query, max_records=max_records, since_minutes=since_minutes)
        filtered = self.filter_incremental(items, cursor)
        return filtered, self.build_ts_cursor(items, fallback=cursor)
