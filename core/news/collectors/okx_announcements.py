from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from core.news.collectors.common import BeautifulSoup, BaseNewsCollector, discover_rss_links, parse_datetime_utc, parse_rss_items


class OKXAnnouncementsCollector(BaseNewsCollector):
    provider_name = "okx_announcements"
    endpoint = "https://www.okx.com/help/section/announcements-latest-announcements"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        self.endpoint = str(defaults.get("okx_announcements_endpoint") or self.endpoint)
        if self.min_interval_sec <= 0:
            self.min_interval_sec = float(defaults.get("okx_announcements_min_interval_sec") or 20.0)
        if self.jitter_sec <= 0:
            self.jitter_sec = float(defaults.get("okx_announcements_jitter_sec") or 1.0)

    def _parse_html(self, html: str) -> List[Dict[str, Any]]:
        if not BeautifulSoup:
            raise RuntimeError("bs4 is required for okx announcement parsing")
        soup = BeautifulSoup(html, "html.parser")
        items: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for anchor in soup.select('a[href*="/help/"]'):
            href = str(anchor.get("href") or "").strip()
            text = anchor.get_text(" ", strip=True)
            if not href or "/section/" in href or not text:
                continue
            abs_url = urljoin(self.endpoint, href)
            if abs_url in seen:
                continue
            seen.add(abs_url)
            title = re.sub(r"\s+Published on\s+[A-Za-z]{3}\s+\d{1,2},\s+\d{4}.*$", "", text).strip()
            pub_match = re.search(r"Published on\s+([A-Za-z]{3}\s+\d{1,2},\s+\d{4})", text)
            published = parse_datetime_utc(pub_match.group(1)).isoformat() if pub_match else datetime.now(timezone.utc).isoformat()
            if not title:
                continue
            items.append(
                {
                    "source": "okx",
                    "title": title[:600],
                    "url": abs_url,
                    "content": title[:1200],
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
        rss_links = discover_rss_links(html, self.endpoint)
        items: List[Dict[str, Any]] = []
        if rss_links:
            try:
                rss_resp = self._request(rss_links[0])
                items = parse_rss_items(rss_resp.text or "", self.provider_name, "okx", rss_links[0])
            except Exception:
                items = []
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
