"""Jin10 flash collector."""
from __future__ import annotations

import time
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
        self.retry_attempts = max(1, int(defaults.get("jin10_retry_attempts") or 3))
        self.retry_base_sleep_sec = max(0.0, float(defaults.get("jin10_retry_base_sleep_sec") or 0.5))

    @staticmethod
    def _retry_after_seconds(response: requests.Response) -> Optional[float]:
        value = str(getattr(response, "headers", {}).get("Retry-After") or "").strip()
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except Exception:
            return None

    def _get_with_retries(self, headers: Dict[str, str]) -> requests.Response:
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = requests.get(self.endpoint, headers=headers, timeout=self.timeout_sec)
                if response.status_code not in {429, 500, 502, 503, 504}:
                    return response
                last_exc = requests.HTTPError(f"transient status {response.status_code}", response=response)
                if attempt >= self.retry_attempts:
                    return response
                retry_after = self._retry_after_seconds(response)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt >= self.retry_attempts:
                    raise
                retry_after = None
            sleep_sec = retry_after if retry_after is not None else self.retry_base_sleep_sec * attempt
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Jin10 request failed without response")

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

        response = self._get_with_retries(headers)
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
