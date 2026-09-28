from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.news.collectors.common import BaseNewsCollector, parse_datetime_utc
from core.news.collectors.exchange_events import classify_exchange_announcement

# OKX public help-centre API (no auth). ``annType`` values come from
# /api/v5/support/announcement-types. Futures/perp launches are published under
# new-listings. The HTML help page used previously is JS-rendered, so scraping it
# only yielded the language-selector menu.
DEFAULT_ANN_TYPES = ["announcements-new-listings", "announcements-delistings"]


def parse_okx_announcements(payload: Any, since_ts: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Turn an ``/api/v5/support/announcements`` response into news items."""
    if not isinstance(payload, dict):
        return []
    if str(payload.get("code", "0")) != "0":
        raise RuntimeError(f"okx announcements api error: code={payload.get('code')} msg={payload.get('msg')}")
    items: List[Dict[str, Any]] = []
    for page in payload.get("data") or []:
        for row in (page or {}).get("details") or []:
            title = str((row or {}).get("title") or "").strip()
            url = str((row or {}).get("url") or "").strip()
            p_time = (row or {}).get("pTime")
            if not title or not url or not p_time:
                continue
            published = parse_datetime_utc(float(p_time) / 1000.0)
            if since_ts is not None and published < since_ts:
                continue
            ann_type = str(row.get("annType") or "")
            business_time = row.get("businessPTime")
            items.append(
                {
                    "source": "okx",
                    "title": title[:600],
                    "url": url,
                    "content": title[:1200],
                    "published_at": published.isoformat(),
                    "lang": "en",
                    "payload": {
                        "provider": "okx_announcements",
                        "origin": "api",
                        "ann_type": ann_type,
                        "event_type": classify_exchange_announcement(title),
                        "business_time": (
                            parse_datetime_utc(float(business_time) / 1000.0).isoformat() if business_time else None
                        ),
                    },
                }
            )
    return items


class OKXAnnouncementsCollector(BaseNewsCollector):
    provider_name = "okx_announcements"
    endpoint = "https://www.okx.com/api/v5/support/announcements"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        self.endpoint = str(defaults.get("okx_announcements_endpoint") or self.endpoint)
        raw_types = defaults.get("okx_announcements_types") or DEFAULT_ANN_TYPES
        self.ann_types = [str(x).strip() for x in raw_types if str(x).strip()]
        if self.min_interval_sec <= 0:
            self.min_interval_sec = float(defaults.get("okx_announcements_min_interval_sec") or 1.0)
        if self.jitter_sec <= 0:
            self.jitter_sec = float(defaults.get("okx_announcements_jitter_sec") or 0.5)

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query
        limit = max(5, min(int(max_records or self.max_records), 120))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(since_minutes or 240)))
        items: List[Dict[str, Any]] = []
        seen: set[str] = set()
        errors: List[str] = []
        for ann_type in self.ann_types:
            try:
                response = self._request(self.endpoint, params={"annType": ann_type})
                parsed = parse_okx_announcements(response.json(), since_ts=since_ts)
            except Exception as exc:
                errors.append(f"{ann_type}: {exc}")
                continue
            for item in parsed:
                if item["url"] in seen:
                    continue
                seen.add(item["url"])
                items.append(item)
        if errors and len(errors) == len(self.ann_types):
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
        # No cursor filtering: announcements can surface with a pTime older than
        # an already-seen one, and a stale cursor would hide them for good. The
        # volume is tiny and save_news_raw de-duplicates by URL.
        items = self.pull_latest(query=query, max_records=max_records, since_minutes=since_minutes)
        return items, self.build_ts_cursor(items, fallback=cursor)
