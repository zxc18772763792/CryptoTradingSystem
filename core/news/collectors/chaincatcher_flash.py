from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from core.news.collectors.common import BeautifulSoup, BaseNewsCollector, extract_json_script, parse_datetime_utc, to_utc_iso


class ChainCatcherFlashCollector(BaseNewsCollector):
    provider_name = "chaincatcher_flash"
    endpoint = "https://www.chaincatcher.com/en/news"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        self.endpoint = str(defaults.get("chaincatcher_flash_endpoint") or self.endpoint)
        if self.min_interval_sec <= 0:
            self.min_interval_sec = float(defaults.get("chaincatcher_flash_min_interval_sec") or 15.0)
        if self.jitter_sec <= 0:
            self.jitter_sec = float(defaults.get("chaincatcher_flash_jitter_sec") or 1.5)

    @staticmethod
    def _parse_mmdd_hhmm(value: str) -> str:
        text = str(value or "").strip()
        match = re.search(r"(?P<month>\d{2})-(?P<day>\d{2})\s+(?P<hour>\d{2}):(?P<minute>\d{2})", text)
        if not match:
            return datetime.now(timezone.utc).isoformat()
        now_cn = datetime.now(timezone(timedelta(hours=8)))
        year = now_cn.year
        month = int(match.group("month"))
        day = int(match.group("day"))
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        dt = datetime(year, month, day, hour, minute, tzinfo=timezone(timedelta(hours=8)))
        if dt - now_cn > timedelta(days=7):
            dt = dt.replace(year=year - 1)
        return dt.astimezone(timezone.utc).isoformat()

    def _from_next_data(self, html: str) -> List[Dict[str, Any]]:
        data = extract_json_script(html, script_id="__NEXT_DATA__")
        if not isinstance(data, dict):
            return []
        items: List[Dict[str, Any]] = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                href = str(node.get("href") or node.get("url") or "").strip()
                title = str(node.get("title") or node.get("name") or "").strip()
                content = str(node.get("summary") or node.get("content") or node.get("description") or "").strip()
                published = node.get("published_at") or node.get("publishTime") or node.get("createdAt")
                if href.startswith("/en/article/") and title:
                    items.append(
                        {
                            "source": "chaincatcher",
                            "title": title[:600],
                            "url": urljoin(self.endpoint, href),
                            "content": content[:2000],
                            "published_at": to_utc_iso(published) if published else datetime.now(timezone.utc).isoformat(),
                            "lang": "en",
                            "payload": {"provider": self.provider_name, "origin": "next_data"},
                        }
                    )
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(data)
        deduped: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in items:
            key = item["url"]
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _from_html(self, html: str) -> List[Dict[str, Any]]:
        if not BeautifulSoup:
            raise RuntimeError("bs4 is required for chaincatcher html parsing")
        soup = BeautifulSoup(html, "html.parser")
        items: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for anchor in soup.select('a[href^="/en/article/"]'):
            href = str(anchor.get("href") or "").strip()
            if not href or href in seen:
                continue
            seen.add(href)
            container = anchor.find_parent("div", class_=re.compile(r"timeline|v-timeline-item", re.I)) or anchor.parent
            text = container.get_text(" ", strip=True) if container else anchor.get_text(" ", strip=True)
            text = re.sub(r"\s+", " ", text)
            ts_match = re.search(r"(\d{2}-\d{2}\s+\d{2}:\d{2})", text)
            published = self._parse_mmdd_hhmm(ts_match.group(1)) if ts_match else datetime.now(timezone.utc).isoformat()
            parts = re.split(r"Scan with WeChat|\d{2}-\d{2}\s+\d{2}:\d{2}", text, maxsplit=2)
            title = (parts[0] if parts else anchor.get_text(" ", strip=True)).strip()
            content = (parts[-1] if len(parts) > 1 else text).strip()
            if not title:
                continue
            items.append(
                {
                    "source": "chaincatcher",
                    "title": title[:600],
                    "url": urljoin(self.endpoint, href),
                    "content": content[:2000],
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
        items = self._from_next_data(html)
        if not items:
            items = self._from_html(html)
        filtered: List[Dict[str, Any]] = []
        for item in items:
            try:
                if parse_datetime_utc(item.get("published_at")) < since_ts:
                    continue
            except Exception:
                pass
            filtered.append(item)
            if len(filtered) >= limit:
                break
        return filtered

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
