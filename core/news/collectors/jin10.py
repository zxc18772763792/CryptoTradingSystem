"""Jin10 flash collector."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from dateutil import parser as dt_parser


def _to_utc_iso(value: Any) -> str:
    if not value:
        return datetime.now(timezone.utc).isoformat()
    text = str(value).strip()
    if not text:
        return datetime.now(timezone.utc).isoformat()
    try:
        dt = dt_parser.parse(text)
        if dt.tzinfo is None:
            # Jin10 times are Beijing time.
            dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


class Jin10Collector:
    """Pull fast news flashes from Jin10 public endpoint."""

    endpoint = "https://flash-api.jin10.com/get_flash_list"

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        cfg = cfg or {}
        defaults = cfg.get("defaults") or {}
        self.timeout_sec = int(defaults.get("jin10_timeout_sec") or 20)
        self.max_records = int(defaults.get("jin10_max_records") or 120)
        self.endpoint = str(defaults.get("jin10_endpoint") or self.endpoint)
        self.app_id = str(defaults.get("jin10_app_id") or "rU6QIu7JHe2gOUeR")
        self.version = str(defaults.get("jin10_version") or "1.0.0")
        self.referer = str(defaults.get("jin10_referer") or "https://www.jin10.com/")

    @staticmethod
    def _normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
        body = raw.get("data") if isinstance(raw.get("data"), dict) else {}
        flash_id = str(raw.get("id") or "")
        published = raw.get("time")
        title = str(body.get("title") or "").strip()
        content = str(body.get("content") or "").strip()
        source = str(body.get("source") or "jin10").strip() or "jin10"
        text = title or content
        if len(text) > 600:
            text = text[:600]
        url = f"https://www.jin10.com/flash_newest.jsp?id={flash_id}" if flash_id else "https://www.jin10.com/"
        return {
            "source": source,
            "title": text,
            "url": url,
            "content": content[:4000],
            "published_at": _to_utc_iso(published),
            "lang": "zh",
            "payload": {"provider": "jin10", "id": flash_id, "raw": raw},
        }

    def pull_latest(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
    ) -> List[Dict[str, Any]]:
        del query  # Jin10 endpoint does not support free-text query.
        max_records = max(10, min(int(max_records or self.max_records), 250))
        since_minutes = max(15, min(int(since_minutes or 240), 24 * 60))
        since_ts = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)

        headers = {
            "x-app-id": self.app_id,
            "x-version": self.version,
            "referer": self.referer,
            "user-agent": "crypto-trading-system/1.0 (+jin10)",
        }

        response = requests.get(self.endpoint, headers=headers, timeout=self.timeout_sec)
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return []

        out: List[Dict[str, Any]] = []
        for raw in rows[: max_records * 3]:
            if not isinstance(raw, dict):
                continue
            item = self._normalize_item(raw)
            if not item.get("title"):
                continue
            try:
                ts = dt_parser.parse(str(item.get("published_at") or ""))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
                if ts < since_ts:
                    continue
            except Exception:
                pass
            out.append(item)
            if len(out) >= max_records:
                break
        return out
