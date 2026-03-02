from __future__ import annotations

import json
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests
from dateutil import parser as dt_parser

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - optional dependency
    BeautifulSoup = None


DEFAULT_HEADERS = {
    "User-Agent": "crypto-trading-system/1.0 (+news-collector)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
}


def clamp_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        num = int(value)
    except Exception:
        num = int(default)
    return max(low, min(num, high))


def parse_datetime_utc(value: Any, default_tz: timezone = timezone.utc) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    else:
        text = str(value or "").strip()
        if not text:
            return datetime.now(timezone.utc)
        dt = dt_parser.parse(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)
    return dt.astimezone(timezone.utc)


def to_utc_iso(value: Any, default_tz: timezone = timezone.utc) -> str:
    return parse_datetime_utc(value, default_tz=default_tz).isoformat()


def extract_json_script(html: str, script_id: Optional[str] = None) -> Any:
    text = str(html or "")
    if not text:
        return None
    if script_id:
        pattern = rf'<script[^>]+id=["\']{re.escape(script_id)}["\'][^>]*>(.*?)</script>'
        match = re.search(pattern, text, flags=re.I | re.S)
        if match:
            try:
                return json.loads(match.group(1).strip())
            except Exception:
                return None
    match = re.search(r"__NEXT_DATA__[^>]*>(.*?)</script>", text, flags=re.I | re.S)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except Exception:
            return None
    return None


def discover_rss_links(html: str, base_url: str) -> List[str]:
    links: List[str] = []
    if BeautifulSoup:
        try:
            soup = BeautifulSoup(html, "html.parser")
            for node in soup.select('link[rel="alternate"][type*="rss"], link[type*="atom"], a[href*="rss"]'):
                href = str(node.get("href") or "").strip()
                if href:
                    links.append(urljoin(base_url, href))
        except Exception:
            pass
    for match in re.findall(r'href=["\']([^"\']+(?:rss|feed)[^"\']*)["\']', html, flags=re.I):
        links.append(urljoin(base_url, match))
    deduped: List[str] = []
    seen: set[str] = set()
    for item in links:
        key = item.strip()
        if key and key not in seen:
            seen.add(key)
            deduped.append(key)
    return deduped


def parse_rss_items(body: str, provider: str, feed_name: str, feed_url: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    root = ET.fromstring(body)

    def first_text(node: ET.Element, tags: Iterable[str]) -> str:
        for tag in tags:
            el = node.find(tag)
            if el is not None:
                if el.text and str(el.text).strip():
                    return str(el.text).strip()
                href = str(el.attrib.get("href") or "").strip()
                if href:
                    return href
        return ""

    def strip_html(text: str) -> str:
        out = re.sub(r"<[^>]+>", " ", str(text or ""))
        return re.sub(r"\s+", " ", out).strip()

    for node in root.findall(".//item") + root.findall(".//{*}entry"):
        title = strip_html(first_text(node, ["title", "{*}title"]))
        link = first_text(node, ["link", "{*}link", "guid", "{*}guid"])
        published = first_text(node, ["pubDate", "{*}pubDate", "published", "{*}published", "updated", "{*}updated"])
        content = strip_html(first_text(node, ["description", "{*}description", "summary", "{*}summary", "content", "{*}content"]))
        if not title or not link:
            continue
        items.append(
            {
                "source": feed_name,
                "title": title[:600],
                "url": urljoin(feed_url, link),
                "content": content[:2000],
                "published_at": to_utc_iso(published),
                "lang": "en",
                "payload": {"provider": provider, "feed": feed_name, "feed_url": feed_url},
            }
        )
    return items


class BaseNewsCollector:
    provider_name = "news"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        defaults = cfg.get("defaults") or {}
        prefix = self.provider_name.lower()
        self.cfg = cfg
        self.timeout_sec = clamp_int(defaults.get(f"{prefix}_timeout_sec"), 20, 5, 60)
        self.max_records = clamp_int(defaults.get(f"{prefix}_max_records"), 80, 5, 500)
        self.retry_count = clamp_int(defaults.get(f"{prefix}_retry_count"), 2, 0, 5)
        self.min_interval_sec = float(defaults.get(f"{prefix}_min_interval_sec") or 0.0)
        self.jitter_sec = float(defaults.get(f"{prefix}_jitter_sec") or 0.0)
        self._last_request_ts = 0.0
        self._session = requests.Session()
        self._session.headers.update(DEFAULT_HEADERS)

    def _respect_rate_limit(self) -> None:
        wait = 0.0
        now = time.monotonic()
        if self.min_interval_sec > 0 and self._last_request_ts > 0:
            wait = max(0.0, self.min_interval_sec - (now - self._last_request_ts))
        if self.jitter_sec > 0:
            wait += random.uniform(0.0, self.jitter_sec)
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.monotonic()

    def _request(self, url: str, *, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        last_error: Optional[Exception] = None
        merged_headers = dict(self._session.headers)
        if headers:
            merged_headers.update(headers)
        for attempt in range(self.retry_count + 1):
            try:
                self._respect_rate_limit()
                resp = self._session.get(url, params=params, headers=merged_headers, timeout=self.timeout_sec)
                resp.raise_for_status()
                return resp
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_count:
                    break
                time.sleep(min(3.0, 0.5 * (2 ** attempt) + random.uniform(0.0, 0.5)))
        raise RuntimeError(f"{self.provider_name} request failed: {last_error}")

    @staticmethod
    def filter_incremental(items: List[Dict[str, Any]], cursor: Optional[str]) -> List[Dict[str, Any]]:
        if not cursor:
            return items
        try:
            cursor_ts = float(str(cursor).strip())
        except Exception:
            return items
        out: List[Dict[str, Any]] = []
        for item in items:
            try:
                ts = parse_datetime_utc(item.get("published_at")).timestamp()
                if ts > cursor_ts:
                    out.append(item)
            except Exception:
                out.append(item)
        return out

    @staticmethod
    def build_ts_cursor(items: List[Dict[str, Any]], fallback: Optional[str] = None) -> Optional[str]:
        values: List[float] = []
        for item in items:
            try:
                values.append(parse_datetime_utc(item.get("published_at")).timestamp())
            except Exception:
                continue
        if not values:
            return fallback
        return str(max(values))

    def pull_incremental(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        items = self.pull_latest(query=query, max_records=max_records, since_minutes=since_minutes)
        filtered = self.filter_incremental(items, cursor)
        new_cursor = self.build_ts_cursor(items, fallback=cursor)
        return filtered, new_cursor

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError


def optional_env(name: str) -> str:
    return str(os.getenv(name) or "").strip()
