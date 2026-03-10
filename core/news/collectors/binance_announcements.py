from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from core.news.collectors.common import BeautifulSoup, BaseNewsCollector, extract_json_script, parse_datetime_utc


class BinanceAnnouncementsCollector(BaseNewsCollector):
    provider_name = "binance_announcements"
    endpoint = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    html_endpoint = "https://www.binance.com/en/support/announcement"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        self.endpoint = str(defaults.get("binance_announcements_endpoint") or self.endpoint)
        raw_categories = defaults.get("binance_announcements_categories") or ["listing", "delisting", "api", "maintenance"]
        self.allowed_categories = {str(x).strip().lower() for x in raw_categories if str(x).strip()}
        if self.min_interval_sec <= 0:
            self.min_interval_sec = float(defaults.get("binance_announcements_min_interval_sec") or 20.0)
        if self.jitter_sec <= 0:
            self.jitter_sec = float(defaults.get("binance_announcements_jitter_sec") or 1.0)

    def _matches_category(self, catalog_name: str, title: str) -> bool:
        text = f"{catalog_name} {title}".lower()
        checks = {
            "listing": ["listing", "launch", "new cryptocurrency", "futures will launch", "new pairs"],
            "delisting": ["delist", "removal", "will remove"],
            "api": ["api", "tick size", "websocket", "rest"],
            "maintenance": ["maintenance", "suspend", "system upgrade", "wallet maintenance"],
        }
        for group, keywords in checks.items():
            if group in self.allowed_categories and any(keyword in text for keyword in keywords):
                return True
        return False

    def _api_pull(self, limit: int, since_ts: datetime) -> List[Dict[str, Any]]:
        response = self._request(
            self.endpoint,
            method="POST",
            json_body={"type": 1, "pageNo": 1, "pageSize": max(20, min(limit, 50))},
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Content-Type": "application/json",
                "Origin": "https://www.binance.com",
                "Referer": "https://www.binance.com/en/support/announcement",
            },
        )
        payload = response.json()
        catalogs = (((payload or {}).get("data") or {}).get("catalogs") or []) if isinstance(payload, dict) else []
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for catalog in catalogs:
            catalog_name = str((catalog or {}).get("catalogName") or "binance announcement")
            for article in (catalog or {}).get("articles") or []:
                title = str((article or {}).get("title") or "").strip()
                if not title or not self._matches_category(catalog_name, title):
                    continue
                code = str((article or {}).get("code") or "").strip()
                url = f"https://www.binance.com/en/support/announcement/detail/{code}" if code else self.html_endpoint
                if url in seen:
                    continue
                seen.add(url)
                published = parse_datetime_utc(float((article or {}).get("releaseDate") or 0) / 1000.0).isoformat()
                if parse_datetime_utc(published) < since_ts:
                    continue
                out.append(
                    {
                        "source": "binance",
                        "title": title[:600],
                        "url": url,
                        "content": catalog_name[:1200],
                        "published_at": published,
                        "lang": "en",
                        "payload": {
                            "provider": self.provider_name,
                            "catalog": catalog_name,
                            "article_id": (article or {}).get("id"),
                            "code": code,
                            "origin": "api",
                        },
                    }
                )
                if len(out) >= limit:
                    return out
        return out

    def _parse_html(self, html: str, limit: int, since_ts: datetime) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        seen: set[str] = set()

        next_data = extract_json_script(html)
        if isinstance(next_data, dict):
            text = str(next_data)
            for code, title in re.findall(r'"code":"([^"]+)".{0,200}?"title":"([^"]+)"', text, flags=re.S):
                clean_title = re.sub(r"\s+", " ", title).strip()
                if not clean_title:
                    continue
                catalog_name = "binance announcement"
                if not self._matches_category(catalog_name, clean_title):
                    continue
                url = f"https://www.binance.com/en/support/announcement/detail/{code}"
                if url in seen:
                    continue
                seen.add(url)
                items.append(
                    {
                        "source": "binance",
                        "title": clean_title[:600],
                        "url": url,
                        "content": catalog_name[:1200],
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "lang": "en",
                        "payload": {"provider": self.provider_name, "code": code, "origin": "html_json"},
                    }
                )
                if len(items) >= limit:
                    return items

        if BeautifulSoup:
            soup = BeautifulSoup(html, "html.parser")
            for anchor in soup.select('a[href*="/support/announcement/detail/"]'):
                href = str(anchor.get("href") or "").strip()
                title = anchor.get_text(" ", strip=True)
                if not href or not title:
                    continue
                catalog_name = "binance announcement"
                if not self._matches_category(catalog_name, title):
                    continue
                url = urljoin(self.html_endpoint, href)
                if url in seen:
                    continue
                seen.add(url)
                container = anchor.find_parent(["article", "div", "li"]) or anchor.parent
                context_text = re.sub(r"\s+", " ", container.get_text(" ", strip=True) if container else title).strip()
                pub_match = re.search(r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})", context_text)
                published = parse_datetime_utc(pub_match.group(1)).isoformat() if pub_match else datetime.now(timezone.utc).isoformat()
                if parse_datetime_utc(published) < since_ts:
                    continue
                items.append(
                    {
                        "source": "binance",
                        "title": title[:600],
                        "url": url,
                        "content": context_text[:1200],
                        "published_at": published,
                        "lang": "en",
                        "payload": {"provider": self.provider_name, "origin": "html"},
                    }
                )
                if len(items) >= limit:
                    break

        return items

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query
        limit = max(5, min(int(max_records or self.max_records), 160))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(since_minutes or 240)))
        try:
            items = self._api_pull(limit=limit, since_ts=since_ts)
            if items:
                return items
        except Exception:
            pass

        response = self._request(
            self.html_endpoint,
            headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
        )
        return self._parse_html(response.text or "", limit=limit, since_ts=since_ts)

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
