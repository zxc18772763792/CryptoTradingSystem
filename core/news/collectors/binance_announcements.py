from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.news.collectors.common import BaseNewsCollector, parse_datetime_utc


class BinanceAnnouncementsCollector(BaseNewsCollector):
    provider_name = "binance_announcements"
    endpoint = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"

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

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query
        limit = max(5, min(int(max_records or self.max_records), 160))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(since_minutes or 240)))
        response = self._request(
            self.endpoint,
            params={"type": 1, "pageNo": 1, "pageSize": max(20, min(limit, 50))},
            headers={"Accept": "application/json,text/plain,*/*", "Referer": "https://www.binance.com/en/support/announcement"},
        )
        payload = response.json()
        catalogs = (((payload or {}).get("data") or {}).get("catalogs") or []) if isinstance(payload, dict) else []
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for catalog in catalogs:
            catalog_name = str((catalog or {}).get("catalogName") or "binance announcement")
            for article in (catalog or {}).get("articles") or []:
                title = str((article or {}).get("title") or "").strip()
                if not title:
                    continue
                if not self._matches_category(catalog_name, title):
                    continue
                code = str((article or {}).get("code") or "").strip()
                url = f"https://www.binance.com/en/support/announcement/detail/{code}" if code else "https://www.binance.com/en/support/announcement"
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
                        },
                    }
                )
                if len(out) >= limit:
                    return out
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
