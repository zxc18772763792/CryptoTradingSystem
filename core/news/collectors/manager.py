"""Multi-source news collector manager with de-duplication and source stats."""
from __future__ import annotations

import hashlib
import math
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dateutil import parser as dt_parser
from loguru import logger

from core.news.collectors.binance_announcements import BinanceAnnouncementsCollector
from core.news.collectors.bybit_announcements import BybitAnnouncementsCollector
from core.news.collectors.chaincatcher_flash import ChainCatcherFlashCollector
from core.news.collectors.cryptocompare_news import CryptoCompareNewsCollector
from core.news.collectors.cryptopanic import CryptoPanicCollector
from core.news.collectors.gdelt import GDELTCollector
from core.news.collectors.jin10 import Jin10Collector
from core.news.collectors.newsapi import NewsAPICollector
from core.news.collectors.okx_announcements import OKXAnnouncementsCollector
from core.news.collectors.rss import RSSNewsCollector
from core.news.storage import db as news_db


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on", "y"}


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _parse_ts_to_unix(value: Any) -> float:
    if not value:
        return 0.0
    try:
        dt = dt_parser.parse(str(value).strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _normalize_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    try:
        parts = urlsplit(text)
        query_items = []
        for k, v in parse_qsl(parts.query, keep_blank_values=False):
            key = str(k or "").lower()
            if key.startswith("utm_") or key in {"fbclid", "gclid", "spm"}:
                continue
            query_items.append((k, v))
        query = urlencode(query_items, doseq=True)
        normalized = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, query, ""))
        return normalized
    except Exception:
        return text


def _canonical_title(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\u4e00-\u9fff ]+", "", text)
    return text.strip()


def _dedupe_key(item: Dict[str, Any]) -> str:
    title_key = _canonical_title(item.get("title"))
    ts_unix = _parse_ts_to_unix(item.get("published_at"))
    if title_key and ts_unix > 0:
        bucket = int(ts_unix // 1800)
        return f"title_bucket:{hashlib.sha1(f'{title_key}|{bucket}'.encode('utf-8')).hexdigest()}"
    url_norm = _normalize_url(item.get("url") or "")
    if url_norm:
        return f"url:{url_norm}"
    title = title_key or str(item.get("title") or "").strip().lower()
    ts = str(item.get("published_at") or "").strip()
    seed = f"{title}|{ts}"
    return f"title:{hashlib.sha1(seed.encode('utf-8')).hexdigest()}"


@dataclass
class _CollectorSpec:
    name: str
    collector: Any


class MultiSourceNewsCollector:
    """Aggregate from multiple collectors, de-duplicate and return source stats."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None):
        self.cfg = cfg or {}
        self.defaults = self.cfg.get("defaults") or {}
        raw_sources = self.defaults.get("news_sources") or [
            "jin10",
            "rss",
            "cryptopanic",
            "gdelt",
            "newsapi",
            "chaincatcher_flash",
            "okx_announcements",
            "bybit_announcements",
            "binance_announcements",
            "cryptocompare_news",
        ]
        self.sources: List[str] = [str(x).strip().lower() for x in raw_sources if str(x).strip()]
        if not self.sources:
            self.sources = ["jin10", "rss", "gdelt"]

    def _build_collectors(self, source_names: Optional[List[str]] = None) -> Tuple[List[_CollectorSpec], List[str]]:
        specs: List[_CollectorSpec] = []
        errors: List[str] = []
        selected = {str(x).strip().lower() for x in (source_names or self.sources) if str(x).strip()}
        if not selected:
            selected = set(self.sources)

        for name in self.sources:
            if name not in selected:
                continue
            if name == "gdelt":
                if _env_bool("NEWS_ENABLE_GDELT", True):
                    specs.append(_CollectorSpec(name=name, collector=GDELTCollector(self.cfg)))
                continue
            if name == "jin10":
                if _env_bool("NEWS_ENABLE_JIN10", True):
                    specs.append(_CollectorSpec(name=name, collector=Jin10Collector(self.cfg)))
                continue
            if name == "rss":
                if _env_bool("NEWS_ENABLE_RSS", True):
                    specs.append(_CollectorSpec(name=name, collector=RSSNewsCollector(self.cfg)))
                continue
            if name == "newsapi":
                if not _env_bool("NEWS_ENABLE_NEWSAPI", True):
                    continue
                if not str(os.getenv("NEWSAPI_KEY") or "").strip():
                    errors.append("newsapi disabled: NEWSAPI_KEY missing")
                    continue
                specs.append(_CollectorSpec(name=name, collector=NewsAPICollector(self.cfg)))
                continue
            if name == "cryptopanic":
                if not _env_bool("NEWS_ENABLE_CRYPTOPANIC", True):
                    continue
                token = str(os.getenv("CRYPTOPANIC_TOKEN") or os.getenv("CRYPTOPANIC_API_KEY") or "").strip()
                if not token:
                    errors.append("cryptopanic disabled: CRYPTOPANIC_TOKEN missing")
                    continue
                specs.append(_CollectorSpec(name=name, collector=CryptoPanicCollector(self.cfg)))
                continue
            if name == "chaincatcher_flash":
                if _env_bool("NEWS_ENABLE_CHAINCATCHER_FLASH", True):
                    specs.append(_CollectorSpec(name=name, collector=ChainCatcherFlashCollector(self.cfg)))
                continue
            if name == "okx_announcements":
                if _env_bool("NEWS_ENABLE_OKX_ANNOUNCEMENTS", True):
                    specs.append(_CollectorSpec(name=name, collector=OKXAnnouncementsCollector(self.cfg)))
                continue
            if name == "bybit_announcements":
                if _env_bool("NEWS_ENABLE_BYBIT_ANNOUNCEMENTS", True):
                    specs.append(_CollectorSpec(name=name, collector=BybitAnnouncementsCollector(self.cfg)))
                continue
            if name == "binance_announcements":
                if _env_bool("NEWS_ENABLE_BINANCE_ANNOUNCEMENTS", True):
                    specs.append(_CollectorSpec(name=name, collector=BinanceAnnouncementsCollector(self.cfg)))
                continue
            if name == "cryptocompare_news":
                if _env_bool("NEWS_ENABLE_CRYPTOCOMPARE_NEWS", True):
                    if not str(os.getenv("CRYPTOCOMPARE_API_KEY") or "").strip():
                        errors.append("cryptocompare running without CRYPTOCOMPARE_API_KEY; stricter rate limit applied")
                    specs.append(_CollectorSpec(name=name, collector=CryptoCompareNewsCollector(self.cfg)))
                continue
            errors.append(f"unsupported collector source: {name}")

        if not specs:
            specs.append(_CollectorSpec(name="jin10", collector=Jin10Collector(self.cfg)))
            specs.append(_CollectorSpec(name="rss", collector=RSSNewsCollector(self.cfg)))
            specs.append(_CollectorSpec(name="gdelt", collector=GDELTCollector(self.cfg)))
            errors.append("all configured sources unavailable; fallback to jin10/rss/gdelt")
        return specs, errors

    def pull_latest(self, query: Optional[str] = None, max_records: Optional[int] = None, since_minutes: int = 240) -> Dict[str, Any]:
        max_records = max(10, min(_safe_int(max_records, 120), 500))
        since_minutes = max(15, min(_safe_int(since_minutes, 240), 24 * 60))
        specs, setup_errors = self._build_collectors()
        source_stats: Dict[str, Dict[str, Any]] = {}
        errors: List[str] = list(setup_errors)
        all_items: List[Dict[str, Any]] = []
        source_count = max(1, len(specs))
        per_source = max(10, min(250, int(math.ceil(max_records / source_count * 1.6))))
        for spec in specs:
            source_stats[spec.name] = {"enabled": True, "pulled_count": 0, "kept_count": 0, "errors": []}

        worker_count = max(1, min(len(specs), _safe_int(os.getenv("NEWS_PULL_WORKERS"), 6)))
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = {
                pool.submit(spec.collector.pull_latest, query=query, max_records=per_source, since_minutes=since_minutes): spec
                for spec in specs
            }
            for future in as_completed(futures):
                spec = futures[future]
                try:
                    items = future.result()
                    source_stats[spec.name]["pulled_count"] = len(items)
                except Exception as exc:
                    err_msg = f"{spec.name} pull failed: {exc}"
                    logger.warning(err_msg)
                    errors.append(err_msg)
                    source_stats[spec.name]["errors"].append(str(exc))
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
                    payload["provider"] = spec.name
                    item["payload"] = payload
                    item["provider"] = spec.name
                    if not str(item.get("source") or "").strip():
                        item["source"] = spec.name
                    all_items.append(item)

        return self._merge_results(all_items, source_stats, errors, max_records)

    async def pull_latest_incremental(
        self,
        query: Optional[str] = None,
        max_records: Optional[int] = None,
        since_minutes: int = 240,
        source_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        max_records = max(10, min(_safe_int(max_records, 120), 500))
        since_minutes = max(15, min(_safe_int(since_minutes, 240), 24 * 60))
        specs, setup_errors = self._build_collectors(source_names=source_names)
        source_stats: Dict[str, Dict[str, Any]] = {}
        errors: List[str] = list(setup_errors)
        all_items: List[Dict[str, Any]] = []
        source_count = max(1, len(specs))
        per_source = max(10, min(250, int(math.ceil(max_records / source_count * 1.6))))

        for spec in specs:
            source_stats[spec.name] = {
                "enabled": True,
                "mode": "incremental",
                "pulled_count": 0,
                "kept_count": 0,
                "cursor_before": None,
                "cursor_after": None,
                "errors": [],
            }
            state = await news_db.get_source_state(spec.name)
            paused_until = None
            if state and state.get("paused_until"):
                try:
                    paused_until = dt_parser.parse(str(state.get("paused_until")))
                    if paused_until.tzinfo is None:
                        paused_until = paused_until.replace(tzinfo=timezone.utc)
                    else:
                        paused_until = paused_until.astimezone(timezone.utc)
                except Exception:
                    paused_until = None
            if paused_until and paused_until > datetime.now(timezone.utc):
                source_stats[spec.name]["errors"].append(f"paused until {paused_until.isoformat()}")
                source_stats[spec.name]["cursor_before"] = state.get("cursor_value") if state else None
                source_stats[spec.name]["cursor_after"] = state.get("cursor_value") if state else None
                continue
            cursor = state.get("cursor_value") if state else None
            source_stats[spec.name]["cursor_before"] = cursor
            try:
                if hasattr(spec.collector, "pull_incremental"):
                    items, new_cursor = spec.collector.pull_incremental(
                        query=query,
                        max_records=per_source,
                        since_minutes=since_minutes,
                        cursor=cursor,
                    )
                else:
                    items = spec.collector.pull_latest(query=query, max_records=per_source, since_minutes=since_minutes)
                    ts_values = [_parse_ts_to_unix(item.get("published_at")) for item in items]
                    new_cursor = str(max(ts_values)) if ts_values else cursor
                source_stats[spec.name]["pulled_count"] = len(items)
                source_stats[spec.name]["cursor_after"] = new_cursor
                await news_db.set_source_state(
                    spec.name,
                    cursor_type="ts",
                    cursor_value=new_cursor,
                    clear_error=True,
                    mark_success=True,
                )
            except Exception as exc:
                err_msg = f"{spec.name} incremental pull failed: {exc}"
                logger.warning(err_msg)
                errors.append(err_msg)
                source_stats[spec.name]["errors"].append(str(exc))
                pause_until = None
                if "429" in str(exc) or "Too Many Requests" in str(exc):
                    cooldown = max(180, _safe_int(self.defaults.get("news_source_429_cooldown_sec"), 900))
                    pause_until = datetime.now(timezone.utc) + timedelta(seconds=cooldown)
                state_after = await news_db.set_source_state(
                    spec.name,
                    last_error=str(exc),
                    mark_failure=True,
                    paused_until=pause_until,
                )
                source_stats[spec.name]["cursor_after"] = state_after.get("cursor_value")
                if pause_until is not None:
                    source_stats[spec.name]["errors"].append(f"paused until {pause_until.isoformat()}")
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue
                payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
                payload["provider"] = spec.name
                item["payload"] = payload
                item["provider"] = spec.name
                if not str(item.get("source") or "").strip():
                    item["source"] = spec.name
                all_items.append(item)

        return self._merge_results(all_items, source_stats, errors, max_records)

    @staticmethod
    def _merge_results(
        all_items: List[Dict[str, Any]],
        source_stats: Dict[str, Dict[str, Any]],
        errors: List[str],
        max_records: int,
    ) -> Dict[str, Any]:
        all_items.sort(key=lambda x: _parse_ts_to_unix(x.get("published_at")), reverse=True)
        deduped: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in all_items:
            key = _dedupe_key(item)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
            if len(deduped) >= max_records:
                break
        for item in deduped:
            provider = str(item.get("provider") or (item.get("payload") or {}).get("provider") or "unknown")
            if provider not in source_stats:
                source_stats[provider] = {"enabled": True, "pulled_count": 0, "kept_count": 0, "errors": []}
            source_stats[provider]["kept_count"] = int(source_stats[provider].get("kept_count") or 0) + 1
        total_pulled = sum(int(v.get("pulled_count") or 0) for v in source_stats.values())
        return {
            "items": deduped,
            "source_stats": source_stats,
            "pulled_total": total_pulled,
            "kept_total": len(deduped),
            "errors": errors,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
