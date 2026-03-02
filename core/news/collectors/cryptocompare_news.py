from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.news.collectors.common import BaseNewsCollector, optional_env, parse_datetime_utc


class CryptoCompareNewsCollector(BaseNewsCollector):
    provider_name = "cryptocompare_news"
    endpoint = "https://min-api.cryptocompare.com/data/v2/news/"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        super().__init__(cfg)
        defaults = (cfg or {}).get("defaults") or {}
        self.endpoint = str(defaults.get("cryptocompare_news_endpoint") or self.endpoint)
        self.api_key = optional_env("CRYPTOCOMPARE_API_KEY")
        if self.min_interval_sec <= 0:
            self.min_interval_sec = 90.0 if self.api_key else 180.0
        if self.jitter_sec <= 0:
            self.jitter_sec = float(defaults.get("cryptocompare_news_jitter_sec") or 1.0)

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query
        limit = max(5, min(int(max_records or self.max_records), 200))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=max(1, int(since_minutes or 240)))
        params: Dict[str, Any] = {"lang": "EN"}
        if self.api_key:
            params["api_key"] = self.api_key
        response = self._request(self.endpoint, params=params, headers={"Accept": "application/json"})
        payload = response.json()
        rows = (payload or {}).get("Data") or []
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            url = str(raw.get("url") or raw.get("guid") or "").strip()
            title = str(raw.get("title") or "").strip()
            if not url or not title or url in seen:
                continue
            seen.add(url)
            published_dt = parse_datetime_utc(raw.get("published_on") or raw.get("publishedAt") or 0)
            if published_dt < since_ts:
                continue
            out.append(
                {
                    "source": str(raw.get("source_info", {}).get("name") or raw.get("source") or "cryptocompare").strip() or "cryptocompare",
                    "title": title[:600],
                    "url": url,
                    "content": str(raw.get("body") or "").strip()[:3000],
                    "published_at": published_dt.isoformat(),
                    "lang": "en",
                    "payload": {
                        "provider": self.provider_name,
                        "categories": str(raw.get("categories") or ""),
                        "source_info": raw.get("source_info") or {},
                        "id": raw.get("id"),
                    },
                }
            )
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
