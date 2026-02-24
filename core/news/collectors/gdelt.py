"""GDELT news collector."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from dateutil import parser as dt_parser
from loguru import logger


class GDELTCollector:
    """Pull crypto-related headlines from GDELT DOC API."""

    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        defaults = cfg.get("defaults") or {}
        self.timeout_sec = int(defaults.get("gdelt_timeout_sec") or 30)
        self.max_records = int(defaults.get("gdelt_max_records") or 120)
        self.default_query = str(
            defaults.get("gdelt_query")
            or "(bitcoin OR ethereum OR binance OR crypto OR ETF OR SEC OR hack OR liquidation)"
        )

    @staticmethod
    def _parse_ts(value: Any) -> str:
        if not value:
            return datetime.now(timezone.utc).isoformat()

        text = str(value).strip()
        if not text:
            return datetime.now(timezone.utc).isoformat()

        try:
            if text.endswith("Z") and "T" in text and len(text) == 16:
                dt = datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            elif text.endswith("Z") and "T" not in text and len(text) == 15:
                dt = datetime.strptime(text, "%Y%m%d%H%M%SZ").replace(tzinfo=timezone.utc)
            else:
                dt = dt_parser.parse(text)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            return dt.isoformat()
        except Exception:
            return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
        title = str(raw.get("title") or "").strip()
        url = str(raw.get("url") or raw.get("socialimage") or "").strip()
        source = str(raw.get("domain") or raw.get("sourcecountry") or "gdelt").strip() or "gdelt"
        published = raw.get("seendate") or raw.get("date") or raw.get("published")
        language = str(raw.get("language") or "en").strip() or "en"
        summary = str(raw.get("snippet") or raw.get("tone") or "").strip()

        return {
            "source": source,
            "title": title,
            "url": url,
            "content": summary,
            "published_at": GDELTCollector._parse_ts(published),
            "lang": language,
            "payload": raw,
        }

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        """Pull latest crypto news from GDELT."""
        max_records = max(10, min(int(max_records or self.max_records), 250))
        since_minutes = max(15, min(int(since_minutes or 240), 24 * 60))

        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=since_minutes)

        params = {
            "query": str(query or self.default_query),
            "mode": "ArtList",
            "format": "json",
            "sort": "datedesc",
            "maxrecords": str(max_records),
            "startdatetime": start.strftime("%Y%m%d%H%M%S"),
            "enddatetime": end.strftime("%Y%m%d%H%M%S"),
        }

        response = requests.get(self.endpoint, params=params, timeout=self.timeout_sec)
        response.raise_for_status()

        payload = response.json()
        raw_items = payload.get("articles") if isinstance(payload, dict) else None
        if not isinstance(raw_items, list):
            logger.warning("GDELT response has no articles list")
            return []

        normalized: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            item = self._normalize_item(raw)
            if not item["title"] or not item["url"]:
                continue
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            normalized.append(item)

        return normalized
