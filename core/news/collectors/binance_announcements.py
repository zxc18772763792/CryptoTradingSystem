from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.news.collectors.common import BaseNewsCollector, parse_datetime_utc
from core.news.collectors.exchange_events import classify_exchange_announcement

# Binance CMS catalog ids. 48 carries spot listings, Earn/Margin additions and
# futures launches; 161 carries delistings and pair removals.
CATALOG_IDS = {
    "listing": 48,
    "delisting": 161,
    "api": 51,
    "maintenance": 157,
}
DEFAULT_CATEGORIES = ["listing", "delisting"]
ANNOUNCEMENT_URL = "https://www.binance.com/en/support/announcement/detail/{code}"


def parse_binance_cms_articles(payload: Any, since_ts: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Turn a ``cms/article/list/query`` response into news items.

    With ``catalogId`` the articles sit under ``data.catalogs[].articles``; some
    responses put them directly under ``data.articles``. Both are accepted.
    """
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return []
    groups: List[Tuple[str, Any, List[Any]]] = []
    for catalog in data.get("catalogs") or []:
        groups.append(
            (
                str((catalog or {}).get("catalogName") or "Binance Announcement"),
                (catalog or {}).get("catalogId"),
                list((catalog or {}).get("articles") or []),
            )
        )
    if data.get("articles"):
        groups.append(("Binance Announcement", None, list(data.get("articles") or [])))

    items: List[Dict[str, Any]] = []
    for catalog_name, catalog_id, articles in groups:
        for article in articles:
            title = str((article or {}).get("title") or "").strip()
            code = str((article or {}).get("code") or "").strip()
            release = (article or {}).get("releaseDate")
            if not title or not code or not release:
                continue
            published = parse_datetime_utc(float(release) / 1000.0)
            if since_ts is not None and published < since_ts:
                continue
            items.append(
                {
                    "source": "binance",
                    "title": title[:600],
                    "url": ANNOUNCEMENT_URL.format(code=code),
                    "content": f"{catalog_name}: {title}"[:1200],
                    "published_at": published.isoformat(),
                    "lang": "en",
                    "payload": {
                        "provider": "binance_announcements",
                        "origin": "api",
                        "catalog": catalog_name,
                        "catalog_id": catalog_id,
                        "article_id": (article or {}).get("id"),
                        "code": code,
                        "event_type": classify_exchange_announcement(title),
                    },
                }
            )
    return items


class BinanceAnnouncementsCollector(BaseNewsCollector):
    provider_name = "binance_announcements"
    endpoint = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        self.endpoint = str(defaults.get("binance_announcements_endpoint") or self.endpoint)
        raw_ids = defaults.get("binance_announcements_catalog_ids")
        if raw_ids:
            self.catalog_ids = [int(x) for x in raw_ids]
        else:
            raw_categories = defaults.get("binance_announcements_categories") or DEFAULT_CATEGORIES
            self.catalog_ids = [
                CATALOG_IDS[key]
                for key in (str(x).strip().lower() for x in raw_categories)
                if key in CATALOG_IDS
            ]
        if self.min_interval_sec <= 0:
            self.min_interval_sec = float(defaults.get("binance_announcements_min_interval_sec") or 1.0)
        if self.jitter_sec <= 0:
            self.jitter_sec = float(defaults.get("binance_announcements_jitter_sec") or 0.5)

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query
        limit = max(5, min(int(max_records or self.max_records), 160))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(since_minutes or 240)))
        items: List[Dict[str, Any]] = []
        seen: set[str] = set()
        errors: List[str] = []
        for catalog_id in self.catalog_ids:
            try:
                # GET with query params; the old POST body form now returns 403.
                response = self._request(
                    self.endpoint,
                    params={"type": 1, "catalogId": catalog_id, "pageNo": 1, "pageSize": 20},
                    headers={"Accept": "application/json,text/plain,*/*"},
                )
                parsed = parse_binance_cms_articles(response.json(), since_ts=since_ts)
            except Exception as exc:
                errors.append(f"catalog {catalog_id}: {exc}")
                continue
            for item in parsed:
                if item["url"] in seen:
                    continue
                seen.add(item["url"])
                items.append(item)
        if errors and len(errors) == len(self.catalog_ids):
            # Surface total failure so source state records it instead of a
            # silent zero-row "success".
            raise RuntimeError("; ".join(errors))
        items.sort(key=lambda x: x["published_at"], reverse=True)
        return items[:limit]

    def pull_incremental(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        # No cursor filtering: releaseDate can be older than an already-seen
        # article, and a stale cursor would hide it for good. The volume is
        # tiny and save_news_raw de-duplicates by URL.
        items = self.pull_latest(query=query, max_records=max_records, since_minutes=since_minutes)
        return items, self.build_ts_cursor(items, fallback=cursor)
