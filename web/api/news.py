"""News API for dashboard widget and standalone news page."""
from __future__ import annotations

import asyncio
import contextlib
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request
from loguru import logger
import pandas as pd
from pydantic import BaseModel, Field

from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.eventizer.llm_glm5 import (
    _summarize_fallback,
    batch_summarize_titles,
    extract_events_glm5_with_meta,
)
from core.news.eventizer.rules import SymbolMapper, load_news_rule_config
from core.news.storage import db as news_db
from core.news.storage.models import parse_any_datetime


router = APIRouter()
_DEFAULT_TOPIC_KEYWORDS = {
    "crypto",
    "bitcoin",
    "ethereum",
    "binance",
    "blockchain",
    "stablecoin",
    "defi",
    "etf",
    "fed",
    "sec",
    "比特币",
    "以太坊",
    "加密",
    "区块链",
    "币安",
    "美联储",
    "监管",
    "利率",
    "降息",
    "加息",
}
_AUTO_PULL_LOCK = asyncio.Lock()
_AUTO_PULL_RUNNING = False
_AUTO_PULL_LAST_AT: Optional[datetime] = None
_NEWS_PIPELINE_LOCK = asyncio.Lock()
_MANUAL_PULL_SEQ = 0
_NEWS_RESPONSE_CACHE: Dict[str, Dict[str, Dict[str, Any]]] = {"latest": {}, "summary": {}, "health": {}}


class PullNowRequest(BaseModel):
    since_minutes: int = Field(default=240, ge=15, le=1440)
    max_records: int = Field(default=120, ge=10, le=250)
    query: Optional[str] = None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on", "y"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name) or default)
    except Exception:
        return int(default)


def _cache_key(*parts: Any) -> str:
    return "|".join(str(part or "") for part in parts)


def _cache_get(namespace: str, key: str, ttl_sec: int) -> Optional[Dict[str, Any]]:
    bucket = _NEWS_RESPONSE_CACHE.setdefault(namespace, {})
    item = bucket.get(key)
    if not item:
        return None
    ts = item.get("_cached_at")
    if not isinstance(ts, datetime):
        return None
    if (_now_utc() - ts).total_seconds() > max(1, int(ttl_sec)):
        return None
    payload = dict(item.get("payload") or {})
    payload["_cache"] = {"hit": True, "stale": False, "age_sec": round((_now_utc() - ts).total_seconds(), 2)}
    return payload


def _cache_get_stale(namespace: str, key: str) -> Optional[Dict[str, Any]]:
    bucket = _NEWS_RESPONSE_CACHE.setdefault(namespace, {})
    item = bucket.get(key)
    if not item:
        return None
    ts = item.get("_cached_at")
    payload = dict(item.get("payload") or {})
    age = round((_now_utc() - ts).total_seconds(), 2) if isinstance(ts, datetime) else None
    payload["_cache"] = {"hit": True, "stale": True, "age_sec": age}
    return payload


def _cache_set(namespace: str, key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    bucket = _NEWS_RESPONSE_CACHE.setdefault(namespace, {})
    bucket[key] = {"_cached_at": _now_utc(), "payload": dict(payload or {})}
    result = dict(payload or {})
    result["_cache"] = {"hit": False, "stale": False, "age_sec": 0.0}
    return result


def _config_paths() -> Dict[str, Path]:
    root = Path(__file__).resolve().parents[2]
    return {
        "rules": root / "config" / "news_rules.yaml",
        "symbols": root / "config" / "symbols.yaml",
    }


def load_news_cfg() -> Dict[str, Any]:
    paths = _config_paths()
    return load_news_rule_config(rules_path=paths["rules"], symbols_path=paths["symbols"])


def _get_cfg(request: Request) -> Dict[str, Any]:
    cfg = getattr(request.app.state, "news_cfg", None)
    if isinstance(cfg, dict):
        return cfg
    return load_news_cfg()


def _get_mapper(cfg: Dict[str, Any]) -> SymbolMapper:
    mapper = cfg.get("_symbol_mapper")
    if isinstance(mapper, SymbolMapper):
        return mapper
    return SymbolMapper({"symbols": cfg.get("symbols") or {}})


def _normalize_symbol(symbol: Optional[str], cfg: Dict[str, Any]) -> Optional[str]:
    if not symbol:
        return None
    mapper = _get_mapper(cfg)
    normalized = mapper.normalize_symbol(symbol)
    return normalized or str(symbol).strip().upper() or None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    clean = sorted(float(v) for v in values)
    if len(clean) == 1:
        return round(clean[0], 3)
    idx = max(0.0, min(1.0, p / 100.0)) * (len(clean) - 1)
    lo = int(idx)
    hi = min(len(clean) - 1, lo + 1)
    frac = idx - lo
    return round(clean[lo] * (1 - frac) + clean[hi] * frac, 3)


def _canonical_url(url: Any) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    # Drop query/hash to improve raw-event matching across aggregators.
    text = text.split("#", 1)[0].split("?", 1)[0].strip()
    return text.rstrip("/")


def _canonical_title(title: Any) -> str:
    text = str(title or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\u4e00-\u9fff ]+", "", text)
    return text.strip()


def _contains_anchor(text: str, anchors: List[str]) -> bool:
    for anchor in anchors:
        token = str(anchor or "").strip().lower()
        if not token:
            continue
        # Short ascii tokens like btc/eth/sol need word boundaries to avoid false hits.
        if token.isascii() and token.isalnum() and len(token) <= 4:
            if re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", text):
                return True
            continue
        if token in text:
            return True
    return False


def _topic_keywords(cfg: Dict[str, Any]) -> List[str]:
    mapper = _get_mapper(cfg)
    keywords = set(_DEFAULT_TOPIC_KEYWORDS)
    for item in (cfg.get("symbols") or {}).values():
        if not isinstance(item, dict):
            continue
        aliases = [item.get("canonical"), *(item.get("aliases") or [])]
        for alias in aliases:
            text = str(alias or "").strip().lower()
            if len(text) >= 3:
                keywords.add(text)
            normalized = mapper.normalize_symbol(alias).lower()
            if len(normalized) >= 3:
                keywords.add(normalized)
    return sorted(keywords)


def _topic_anchor_keywords(cfg: Dict[str, Any]) -> List[str]:
    anchors = {
        "crypto",
        "bitcoin",
        "ethereum",
        "binance",
        "blockchain",
        "btc",
        "eth",
        "bnb",
        "sol",
        "xrp",
        "ada",
        "doge",
        "比特币",
        "以太坊",
        "加密",
        "区块链",
        "币安",
        "山寨币",
    }
    for item in (cfg.get("symbols") or {}).values():
        if not isinstance(item, dict):
            continue
        canonical = str(item.get("canonical") or "").strip().upper()
        if canonical:
            anchors.add(canonical.lower())
            if canonical.endswith("USDT"):
                base = canonical[:-4].lower()
                if base in {"btc", "eth", "bnb", "sol", "xrp", "ada", "doge", "trx", "ltc", "bch"}:
                    anchors.add(base)
    return sorted(anchors)


def _is_relevant_news(item: Dict[str, Any], keywords: List[str], anchor_keywords: Optional[List[str]] = None) -> bool:
    title = str(item.get("title") or "").strip().lower()
    content = str(item.get("content") or item.get("summary") or "").strip().lower()
    text = f"{title}\n{content}"
    if not text.strip():
        return False
    if not any(keyword in text for keyword in keywords):
        return False

    provider = str(item.get("provider") or (item.get("payload") or {}).get("provider") or "").strip().lower()
    source_name = str(item.get("source") or "").strip().lower()
    # GDELT/RSS are noisy on generic words like ETF/Fed; require a crypto anchor.
    if (provider in {"gdelt", "rss", "newsapi"} or (not provider and source_name != "jin10")) and anchor_keywords:
        if not _contains_anchor(text, anchor_keywords):
            return False
    return True


def _event_as_feed_item(event: Dict[str, Any]) -> Dict[str, Any]:
    evidence = event.get("evidence") or {}
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    provider = str(payload.get("provider") or "event")
    return {
        "id": f"event-{event.get('id')}",
        "published_at": event.get("ts"),
        "title": str(evidence.get("title") or ""),
        "url": str(evidence.get("url") or ""),
        "source": str(evidence.get("source") or "unknown"),
        "provider": provider,
        "symbol": str(event.get("symbol") or ""),
        "event_type": str(event.get("event_type") or ""),
        "sentiment": int(event.get("sentiment") or 0),
        "impact_score": _safe_float(event.get("impact_score")),
        "model_source": str(event.get("model_source") or "event"),
        "event_id": str(event.get("event_id") or ""),
        "has_event": True,
    }


def _raw_as_feed_item(raw: Dict[str, Any], event: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
    provider = str(payload.get("provider") or "raw")
    out = {
        "id": f"raw-{raw.get('id')}",
        "published_at": raw.get("published_at"),
        "title": str(raw.get("title") or ""),
        "url": str(raw.get("url") or ""),
        "source": str(raw.get("source") or "unknown"),
        "provider": provider,
        "symbol": "",
        "event_type": "",
        "sentiment": 0,
        "impact_score": 0.0,
        "model_source": "raw",
        "event_id": "",
        "has_event": False,
    }
    if not event:
        return out

    event_payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    out.update(
        {
            "symbol": str(event.get("symbol") or ""),
            "event_type": str(event.get("event_type") or ""),
            "sentiment": int(event.get("sentiment") or 0),
            "impact_score": _safe_float(event.get("impact_score")),
            "model_source": str(event.get("model_source") or "event"),
            "event_id": str(event.get("event_id") or ""),
            "provider": str(event_payload.get("provider") or provider),
            "has_event": True,
        }
    )
    return out


def _sort_by_published_desc(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(item: Dict[str, Any]) -> float:
        try:
            ts = parse_any_datetime(item.get("published_at"))
            return ts.timestamp()
        except Exception:
            return 0.0

    return sorted(items, key=_key, reverse=True)


def _feed_sentiment_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {"positive": 0, "neutral": 0, "negative": 0}
    structured = 0
    unstructured = 0
    for item in items or []:
        if bool(item.get("has_event")):
            structured += 1
            s = int(item.get("sentiment") or 0)
            if s > 0:
                counts["positive"] += 1
            elif s < 0:
                counts["negative"] += 1
            else:
                counts["neutral"] += 1
            continue
        unstructured += 1
        ss = str(item.get("summary_sentiment") or "neutral").strip().lower()
        if ss == "positive":
            counts["positive"] += 1
        elif ss == "negative":
            counts["negative"] += 1
        else:
            counts["neutral"] += 1
    return {
        "total": len(items or []),
        "structured": structured,
        "unstructured": unstructured,
        "sentiment": counts,
    }


def _bucketize_events(events: List[Dict[str, Any]], granularities: Optional[List[str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    rules = granularities or ["5m", "15m", "1h", "4h", "1d"]
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not events:
        return {g: [] for g in rules}

    rows: List[Dict[str, Any]] = []
    for event in events:
        try:
            ts = parse_any_datetime(event.get("ts"))
        except Exception:
            continue
        rows.append({"ts": ts, "sentiment": int(event.get("sentiment") or 0)})
    if not rows:
        return {g: [] for g in rules}

    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts")
    if df.empty:
        return {g: [] for g in rules}
    df = df.set_index("ts")

    rule_map = {"5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1d"}
    for g in rules:
        freq = rule_map.get(g)
        if not freq:
            continue
        buckets = defaultdict(lambda: {"count": 0, "positive": 0, "neutral": 0, "negative": 0})
        for ts, row in df.iterrows():
            key = ts.floor(freq)
            slot = buckets[key]
            slot["count"] += 1
            s = int(row.get("sentiment") or 0)
            if s > 0:
                slot["positive"] += 1
            elif s < 0:
                slot["negative"] += 1
            else:
                slot["neutral"] += 1
        series: List[Dict[str, Any]] = []
        for ts_key in sorted(buckets.keys()):
            slot = buckets[ts_key]
            series.append(
                {
                    "bucket_start": ts_key.isoformat(),
                    "count": int(slot["count"]),
                    "positive": int(slot["positive"]),
                    "neutral": int(slot["neutral"]),
                    "negative": int(slot["negative"]),
                }
            )
        out[g] = series
    return out


def _latest_item_age_min(items: List[Dict[str, Any]]) -> Optional[float]:
    if not items:
        return None
    try:
        ts = parse_any_datetime(items[0].get("published_at"))
    except Exception:
        return None
    return max(0.0, (_now_utc() - ts).total_seconds() / 60.0)


def _cfg_int(cfg: Dict[str, Any], key: str, default: int) -> int:
    try:
        return int((cfg.get("defaults") or {}).get(key) or default)
    except Exception:
        return int(default)


async def _auto_pull_if_stale(cfg: Dict[str, Any], latest_items: List[Dict[str, Any]], hours: int) -> bool:
    global _AUTO_PULL_RUNNING, _AUTO_PULL_LAST_AT
    stale_min = max(2, min(_cfg_int(cfg, "news_auto_pull_stale_min", 8), 180))
    cooldown_sec = max(5, min(_cfg_int(cfg, "news_auto_pull_cooldown_sec", 45), 600))
    latest_age = _latest_item_age_min(latest_items)
    should_pull = latest_age is None or latest_age >= float(stale_min)
    if not should_pull:
        return False

    now = _now_utc()
    if _AUTO_PULL_LAST_AT and (now - _AUTO_PULL_LAST_AT).total_seconds() < cooldown_sec:
        return False
    if _AUTO_PULL_RUNNING:
        return False

    async with _AUTO_PULL_LOCK:
        if _AUTO_PULL_RUNNING:
            return False
        if _AUTO_PULL_LAST_AT and (_now_utc() - _AUTO_PULL_LAST_AT).total_seconds() < cooldown_sec:
            return False
        _AUTO_PULL_RUNNING = True
        _AUTO_PULL_LAST_AT = _now_utc()

    async def _runner() -> None:
        global _AUTO_PULL_RUNNING
        try:
            await pull_and_store_news(
                cfg=cfg,
                payload=PullNowRequest(
                    since_minutes=max(60, min(1440, int(hours) * 60)),
                    max_records=max(60, _cfg_int(cfg, "news_auto_pull_max_records", 140)),
                ),
            )
        except Exception:
            pass
        finally:
            _AUTO_PULL_RUNNING = False

    asyncio.create_task(_runner())
    return True


def _count_by_provider(items: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        provider = str(item.get("provider") or payload.get("provider") or "legacy").strip().lower() or "legacy"
        counts[provider] = counts.get(provider, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True))


def _build_source_summary(raw_rows: List[Dict[str, Any]], source_states: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    summary: Dict[str, Dict[str, Any]] = {}
    for row in raw_rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        source = str(row.get("source") or payload.get("provider") or "unknown").strip().lower() or "unknown"
        slot = summary.setdefault(source, {"inserted_count": 0, "latencies": [], "max_importance": 0, "latest_at": None})
        slot["inserted_count"] += 1
        latency = _safe_float(payload.get("latency_sec"), 0.0)
        if latency > 0:
            slot["latencies"].append(latency)
        slot["max_importance"] = max(int(slot["max_importance"]), int(payload.get("importance_score") or 0))
        published = row.get("published_at")
        if published:
            slot["latest_at"] = str(published)
    state_map = {str(item.get("source") or "").strip().lower(): item for item in source_states}
    for source, slot in summary.items():
        latencies = slot.pop("latencies", [])
        state = state_map.get(source) or {}
        success_count = int(state.get("success_count") or 0)
        failure_count = int(state.get("failure_count") or 0)
        total_runs = success_count + failure_count
        slot["failure_rate"] = round((failure_count / total_runs), 4) if total_runs else 0.0
        slot["latency_p50"] = _percentile(latencies, 50)
        slot["latency_p95"] = _percentile(latencies, 95)
        slot["last_error"] = state.get("last_error")
        slot["paused_until"] = state.get("paused_until")
        slot["pending_errors"] = int(state.get("error_count") or 0)
    return dict(sorted(summary.items(), key=lambda kv: kv[1]["inserted_count"], reverse=True))


async def _emit_news_update_snapshot(limit: int = 12, hours: int = 24) -> None:
    try:
        from core.realtime import event_bus
    except Exception:
        return
    feed = await build_latest_feed(cfg=load_news_cfg(), symbol=None, hours=hours, limit=limit, summarize=False)
    await event_bus.publish_nowait_safe(
        event="news_update",
        payload={
            "timestamp": _now_utc().isoformat(),
            "count": int(feed.get("count") or 0),
            "items": feed.get("items") or [],
        },
    )


def _news_job_store(request: Request) -> Dict[str, Any]:
    store = getattr(request.app.state, "news_manual_jobs", None)
    if not isinstance(store, dict):
        store = {"active": None, "latest": None, "jobs": {}}
        request.app.state.news_manual_jobs = store
    return store


async def _run_manual_pull_job(request: Request, job_id: str, cfg: Dict[str, Any], payload: PullNowRequest) -> None:
    store = _news_job_store(request)
    job = store["jobs"].get(job_id) or {}
    job["status"] = "running"
    job["started_at"] = _now_utc().isoformat()
    store["active"] = job_id
    store["jobs"][job_id] = job
    try:
        result = await pull_and_store_news(cfg=cfg, payload=payload)
        job["status"] = "completed"
        job["result"] = result
        store["latest"] = result
        with contextlib.suppress(Exception):
            await _emit_news_update_snapshot(limit=12, hours=24)
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
    finally:
        job["finished_at"] = _now_utc().isoformat()
        if store.get("active") == job_id:
            store["active"] = None


def _event_lookup_maps(events: List[Dict[str, Any]]) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    by_url: Dict[str, List[Dict[str, Any]]] = {}
    by_title: Dict[str, List[Dict[str, Any]]] = {}
    for event in events:
        evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
        url = _canonical_url(evidence.get("url"))
        title_key = _canonical_title(evidence.get("title"))
        if url:
            by_url.setdefault(url, []).append(event)
        if title_key:
            by_title.setdefault(title_key, []).append(event)
    return by_url, by_title


async def _backfill_recent_events(
    cfg: Dict[str, Any],
    hours: int = 24,
    max_candidates: int = 120,
) -> Dict[str, Any]:
    hours = max(1, min(int(hours or 24), 168))
    max_candidates = max(10, min(int(max_candidates or 120), 300))
    since = _now_utc() - timedelta(hours=hours)
    raw_rows = await news_db.list_news_raw(since=since, limit=5000)
    events = await news_db.list_events(since=since, limit=5000)
    if not raw_rows:
        return {"candidate_count": 0, "events_count": 0, "deduped_count": 0, "llm_used": False, "errors": []}

    keywords = _topic_keywords(cfg)
    anchor_keywords = _topic_anchor_keywords(cfg)
    events_by_url, events_by_title = _event_lookup_maps(events)

    candidates: List[Dict[str, Any]] = []
    seen_raw_keys: set[str] = set()
    for raw in raw_rows:
        if not _is_relevant_news(raw, keywords, anchor_keywords):
            continue
        raw_url = _canonical_url(raw.get("url"))
        raw_title = _canonical_title(raw.get("title"))
        if raw_url and raw_url in events_by_url:
            continue
        if raw_title and raw_title in events_by_title:
            continue
        try:
            bucket = int(parse_any_datetime(raw.get("published_at")).timestamp() // 1800)
        except Exception:
            bucket = 0
        dedupe_key = f"{raw_title}|{bucket}"
        if not raw_title or dedupe_key in seen_raw_keys:
            continue
        seen_raw_keys.add(dedupe_key)
        candidates.append(raw)
        if len(candidates) >= max_candidates:
            break

    if not candidates:
        return {"candidate_count": 0, "events_count": 0, "deduped_count": 0, "llm_used": False, "errors": []}

    extracted, llm_used, errors = await asyncio.to_thread(extract_events_glm5_with_meta, candidates, cfg)
    url_to_provider: Dict[str, str] = {}
    title_to_provider: Dict[str, str] = {}
    for raw in candidates:
        provider = str(raw.get("provider") or (raw.get("payload") or {}).get("provider") or "").strip()
        if not provider:
            continue
        raw_url = str(raw.get("url") or "").strip()
        raw_title = _canonical_title(raw.get("title"))
        if raw_url:
            url_to_provider[raw_url] = provider
        if raw_title:
            title_to_provider[raw_title] = provider
    for event in extracted:
        evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
        provider = url_to_provider.get(str(evidence.get("url") or "").strip()) or title_to_provider.get(
            _canonical_title(evidence.get("title"))
        )
        if not provider:
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        payload["provider"] = provider
        event["payload"] = payload

    saved = await news_db.save_events(extracted, model_source="mixed_backfill")
    return {
        "candidate_count": len(candidates),
        "events_count": int(saved.get("events_count") or 0),
        "deduped_count": int(saved.get("deduped_count") or 0),
        "llm_used": bool(llm_used),
        "errors": errors,
    }


async def build_latest_feed(
    cfg: Dict[str, Any],
    symbol: Optional[str] = None,
    hours: int = 24,
    limit: int = 30,
    summarize: bool = False,
) -> Dict[str, Any]:
    hours = max(1, min(int(hours or 24), 168))
    limit = max(1, min(int(limit or 30), 300))
    since = _now_utc() - timedelta(hours=hours)
    symbol_norm = _normalize_symbol(symbol, cfg)

    raw_limit = max(200, limit * 8)
    event_limit = max(300, limit * 10)

    raw_items = await news_db.list_news_raw(since=since, limit=raw_limit)
    events = await news_db.list_events(symbol=symbol_norm, since=since, limit=event_limit)
    keywords = _topic_keywords(cfg)
    anchor_keywords = _topic_anchor_keywords(cfg)

    events_by_url, events_by_title = _event_lookup_maps(events)

    items: List[Dict[str, Any]] = []
    used_event_ids: set[str] = set()
    seen_story_keys: set[str] = set()
    reserve_structured = 0 if symbol_norm else min(max(6, limit // 3), max(0, limit - 8))
    raw_soft_limit = max(1, limit - reserve_structured) if reserve_structured else limit
    for raw in raw_items:
        if not _is_relevant_news(raw, keywords, anchor_keywords):
            continue
        url = _canonical_url(raw.get("url"))
        title_key = _canonical_title(raw.get("title"))
        story_key = title_key or url
        if story_key and story_key in seen_story_keys:
            continue
        matched_events = events_by_url.get(url) or []
        if not matched_events:
            matched_events = events_by_title.get(title_key) or []

        picked_event: Optional[Dict[str, Any]] = None
        if symbol_norm:
            for event in matched_events:
                if str(event.get("symbol") or "").upper() == symbol_norm:
                    picked_event = event
                    break
            if matched_events and not picked_event:
                continue
            if not matched_events:
                continue
        elif matched_events:
            picked_event = matched_events[0]

        feed_item = _raw_as_feed_item(raw, picked_event)
        items.append(feed_item)
        if story_key:
            seen_story_keys.add(story_key)
        if picked_event:
            used_event_ids.add(str(picked_event.get("event_id") or ""))
        if len(items) >= raw_soft_limit:
            break

    if len(items) < limit:
        for event in events:
            event_id = str(event.get("event_id") or "")
            if event_id and event_id in used_event_ids:
                continue
            feed_item = _event_as_feed_item(event)
            items.append(feed_item)
            if event_id:
                used_event_ids.add(event_id)
            if len(items) >= limit:
                break

    sorted_items = _sort_by_published_desc(items)

    # Summarize titles if requested
    if summarize and sorted_items:
        llm_cfg = cfg.get("llm") or {}
        summarize_limit = int(llm_cfg.get("summarize_limit") or min(40, limit))
        summarize_limit = max(5, min(120, summarize_limit))
        summarize_timeout_sec = int(llm_cfg.get("summarize_timeout_sec") or llm_cfg.get("timeout_sec") or 20)
        summarize_timeout_sec = max(2, min(90, summarize_timeout_sec))

        # Prioritize unstructured headlines for GLM sentiment tagging;
        # structured-event items already have event sentiment.
        prioritized = sorted(
            enumerate(sorted_items),
            key=lambda x: (1 if bool(x[1].get("has_event")) else 0, x[0]),
        )
        target_pairs = prioritized[:summarize_limit]
        target_indices = [idx for idx, _ in target_pairs]
        titles = [item.get("title") or "" for _, item in target_pairs]
        try:
            summarized_results = await asyncio.wait_for(
                asyncio.to_thread(batch_summarize_titles, titles, cfg, 60),
                timeout=summarize_timeout_sec,
            )
        except Exception as e:
            logger.warning(f"title summarize timeout/failure, fallback to rule sentiment: {e}")
            summarized_results = []
            for t in titles:
                item = _summarize_fallback(t or "", 60)
                item.setdefault("source", "api_timeout_fallback")
                summarized_results.append(item)
        for item_idx, result in zip(target_indices, summarized_results):
            item = sorted_items[item_idx]
            item["summary_title"] = result.get("summary", item.get("title", ""))
            item["summary_sentiment"] = result.get("sentiment", "neutral")
            item["summary_source"] = result.get("source", "unknown")
        summarized_set = set(target_indices)
        for idx, item in enumerate(sorted_items):
            if idx in summarized_set:
                continue
            item["summary_title"] = item.get("title", "")
            item["summary_sentiment"] = "neutral"
            item["summary_source"] = "not_summarized"

    by_provider = _count_by_provider(sorted_items)
    by_source: Dict[str, int] = {}
    for item in sorted_items:
        source_name = str(item.get("source") or "unknown").strip().lower() or "unknown"
        by_source[source_name] = by_source.get(source_name, 0) + 1

    return {
        "count": len(items),
        "symbol": symbol_norm,
        "hours": hours,
        "since": since.isoformat(),
        "items": sorted_items,
        "feed_stats": _feed_sentiment_summary(sorted_items),
        "source_stats": {
            "by_provider": by_provider,
            "by_source": dict(sorted(by_source.items(), key=lambda kv: kv[1], reverse=True)[:12]),
        },
    }


async def pull_and_store_news(cfg: Dict[str, Any], payload: PullNowRequest) -> Dict[str, Any]:
    async with _NEWS_PIPELINE_LOCK:
        collector = MultiSourceNewsCollector(cfg)
        errors: List[str] = []
        filtered_out_count = 0
        backfill_stats = {"candidate_count": 0, "events_count": 0, "deduped_count": 0, "llm_used": False, "errors": []}

        try:
            pulled_bundle = await collector.pull_latest_incremental(
                query=payload.query,
                max_records=payload.max_records,
                since_minutes=payload.since_minutes,
            )
            pulled_all = pulled_bundle.get("items") or []
            source_stats = pulled_bundle.get("source_stats") or {}
            errors.extend([str(x) for x in (pulled_bundle.get("errors") or []) if str(x).strip()])
            keywords = _topic_keywords(cfg)
            anchor_keywords = _topic_anchor_keywords(cfg)
            pulled = [item for item in pulled_all if _is_relevant_news(item, keywords, anchor_keywords)]
            filtered_out_count = max(0, len(pulled_all) - len(pulled))
            min_keep = min(max(12, int(payload.max_records * 0.3)), len(pulled_all))
            if len(pulled) < min_keep:
                preferred = []
                for item in pulled_all:
                    provider = str(item.get("provider") or (item.get("payload") or {}).get("provider") or "").strip().lower()
                    if provider in {"jin10", "rss", "newsapi", "cryptopanic"}:
                        preferred.append(item)
                candidates = preferred + [x for x in pulled_all if x not in preferred]
                seen_urls = {str(x.get("url") or "").strip() for x in pulled}
                for item in candidates:
                    url = str(item.get("url") or "").strip()
                    if url and url in seen_urls:
                        continue
                    pulled.append(item)
                    if url:
                        seen_urls.add(url)
                    if len(pulled) >= min_keep:
                        break
                filtered_out_count = max(0, len(pulled_all) - len(pulled))
            if not pulled and pulled_all:
                pulled = pulled_all[: min(12, len(pulled_all))]
                filtered_out_count = max(0, len(pulled_all) - len(pulled))
        except Exception as exc:
            errors.append(f"news pull failed: {exc}")
            pulled = []
            source_stats = {}

        raw_stats = await news_db.save_news_raw(pulled)
        new_news = raw_stats.get("inserted") or []
        queue_stats = await news_db.enqueue_llm_tasks(new_news, min_importance=_env_int("NEWS_LLM_MIN_IMPORTANCE", 35))

        topic_matched_by_provider = _count_by_provider(pulled)
        inserted_by_provider = _count_by_provider(new_news)
        for provider, stat in source_stats.items():
            if not isinstance(stat, dict):
                continue
            stat["topic_matched_count"] = int(topic_matched_by_provider.get(provider, 0))
            stat["raw_inserted_count"] = int(inserted_by_provider.get(provider, 0))

        llm_used = False
        sync_llm = _env_bool("NEWS_PULL_SYNC_LLM", False)
        if new_news:
            if sync_llm:
                events, llm_used, llm_errors = await asyncio.to_thread(extract_events_glm5_with_meta, new_news, cfg)
                errors.extend(llm_errors)
                url_to_provider: Dict[str, str] = {}
                for raw in new_news:
                    provider = str(raw.get("provider") or (raw.get("payload") or {}).get("provider") or "").strip()
                    url = str(raw.get("url") or "").strip()
                    if provider and url:
                        url_to_provider[url] = provider
                for event in events:
                    evidence = event.get("evidence") if isinstance(event.get("evidence"), dict) else {}
                    event_url = str(evidence.get("url") or "").strip()
                    provider = url_to_provider.get(event_url)
                    if not provider:
                        continue
                    event_payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                    event_payload["provider"] = provider
                    event["payload"] = event_payload
            else:
                events = []
        else:
            events = []

        event_stats = await news_db.save_events(events, model_source="mixed")
        events_by_provider = _count_by_provider(events)
        for provider, stat in source_stats.items():
            if not isinstance(stat, dict):
                continue
            stat["events_extracted_count"] = int(events_by_provider.get(provider, 0))

        if sync_llm:
            try:
                backfill_stats = await _backfill_recent_events(
                    cfg,
                    hours=max(24, max(1, int(payload.since_minutes / 60))),
                    max_candidates=min(180, max(60, int(payload.max_records * 1.5))),
                )
                errors.extend([str(x) for x in (backfill_stats.get("errors") or []) if str(x).strip()])
            except Exception as exc:
                errors.append(f"recent backfill failed: {exc}")

        return {
            "pulled_count": int(raw_stats.get("pulled_count") or 0),
            "pulled_all_count": len(pulled_all) if "pulled_all" in locals() else 0,
            "deduped_count": int(raw_stats.get("deduped_count") or 0),
            "raw_inserted_count": len(new_news),
            "filtered_out_count": int(filtered_out_count),
            "events_count": int(event_stats.get("events_count") or 0),
            "events_deduped_count": int(event_stats.get("deduped_count") or 0),
            "backfill_candidate_count": int(backfill_stats.get("candidate_count") or 0),
            "backfill_events_count": int(backfill_stats.get("events_count") or 0),
            "backfill_events_deduped_count": int(backfill_stats.get("deduped_count") or 0),
            "queued_count": int(queue_stats.get("queued_count") or 0),
            "source_stats": source_stats,
            "llm_used": bool(llm_used or backfill_stats.get("llm_used")),
            "sync_llm": bool(sync_llm),
            "errors": errors,
            "timestamp": _now_utc().isoformat(),
        }


@router.get("/health")
async def health() -> Dict[str, Any]:
    cache_key = "default"
    ttl_sec = _env_int("NEWS_API_HEALTH_CACHE_TTL_SEC", 15)
    cached = _cache_get("health", cache_key, ttl_sec)
    if cached:
        return cached

    def _enabled(name: str, default: bool = True) -> bool:
        return str(os.environ.get(name, "1" if default else "0")).strip().lower() not in {"0", "false", "no", "off"}

    gdelt_enabled = str(os.environ.get("NEWS_ENABLE_GDELT", "1")).strip().lower() not in {"0", "false", "no", "off"}
    jin10_enabled = str(os.environ.get("NEWS_ENABLE_JIN10", "1")).strip().lower() not in {"0", "false", "no", "off"}
    rss_enabled = str(os.environ.get("NEWS_ENABLE_RSS", "1")).strip().lower() not in {"0", "false", "no", "off"}
    newsapi_enabled = bool(os.environ.get("NEWSAPI_KEY"))
    if str(os.environ.get("NEWS_ENABLE_NEWSAPI", "1")).strip().lower() in {"0", "false", "no", "off"}:
        newsapi_enabled = False
    cryptopanic_enabled = bool(os.environ.get("CRYPTOPANIC_TOKEN") or os.environ.get("CRYPTOPANIC_API_KEY"))
    if str(os.environ.get("NEWS_ENABLE_CRYPTOPANIC", "1")).strip().lower() in {"0", "false", "no", "off"}:
        cryptopanic_enabled = False
    try:
        db_timeout = max(1, _env_int("NEWS_API_HEALTH_DB_TIMEOUT_SEC", 2))
        source_states, llm_queue = await asyncio.gather(
            asyncio.wait_for(news_db.list_source_states(), timeout=db_timeout),
            asyncio.wait_for(news_db.get_llm_queue_stats(), timeout=db_timeout),
        )
        payload = {
            "status": "ok",
            "service": "web_news",
            "timestamp": _now_utc().isoformat(),
            "llm_enabled": bool(os.environ.get("ZHIPU_API_KEY")),
            "sources": {
                "jin10": jin10_enabled,
                "rss": rss_enabled,
                "gdelt": gdelt_enabled,
                "newsapi": newsapi_enabled,
                "cryptopanic": cryptopanic_enabled,
                "chaincatcher_flash": _enabled("NEWS_ENABLE_CHAINCATCHER_FLASH", True),
                "binance_announcements": _enabled("NEWS_ENABLE_BINANCE_ANNOUNCEMENTS", True),
                "okx_announcements": _enabled("NEWS_ENABLE_OKX_ANNOUNCEMENTS", True),
                "bybit_announcements": _enabled("NEWS_ENABLE_BYBIT_ANNOUNCEMENTS", True),
                "cryptocompare_news": _enabled("NEWS_ENABLE_CRYPTOCOMPARE_NEWS", True),
            },
            "source_states": source_states,
            "llm_queue": llm_queue,
        }
        return _cache_set("health", cache_key, payload)
    except Exception as exc:
        logger.warning(f"news health failed: {exc}")
        stale = _cache_get_stale("health", cache_key)
        if stale:
            stale["status"] = "degraded"
            stale["fallback_reason"] = str(exc)
            return stale
        return {
            "status": "degraded",
            "service": "web_news",
            "timestamp": _now_utc().isoformat(),
            "llm_enabled": bool(os.environ.get("ZHIPU_API_KEY")),
            "sources": {
                "jin10": jin10_enabled,
                "rss": rss_enabled,
                "gdelt": gdelt_enabled,
                "newsapi": newsapi_enabled,
                "cryptopanic": cryptopanic_enabled,
                "chaincatcher_flash": _enabled("NEWS_ENABLE_CHAINCATCHER_FLASH", True),
                "binance_announcements": _enabled("NEWS_ENABLE_BINANCE_ANNOUNCEMENTS", True),
                "okx_announcements": _enabled("NEWS_ENABLE_OKX_ANNOUNCEMENTS", True),
                "bybit_announcements": _enabled("NEWS_ENABLE_BYBIT_ANNOUNCEMENTS", True),
                "cryptocompare_news": _enabled("NEWS_ENABLE_CRYPTOCOMPARE_NEWS", True),
            },
            "source_states": [],
            "llm_queue": {},
            "fallback_reason": str(exc),
        }


@router.post("/pull_now")
async def pull_now(
    request: Request,
    payload: PullNowRequest = Body(default_factory=PullNowRequest),
    background: bool = Query(default=True),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    if not background:
        return await pull_and_store_news(cfg=cfg, payload=payload)
    global _MANUAL_PULL_SEQ
    store = _news_job_store(request)
    active_job_id = store.get("active")
    if active_job_id:
        active = store.get("jobs", {}).get(active_job_id) or {}
        return {
            "queued": False,
            "status": "running",
            "job_id": active_job_id,
            "message": "已有新闻结构化任务在后台运行，当前请求未重复启动",
            "job": active,
        }
    _MANUAL_PULL_SEQ += 1
    job_id = f"news-pull-{_MANUAL_PULL_SEQ:06d}"
    job = {
        "job_id": job_id,
        "status": "pending",
        "created_at": _now_utc().isoformat(),
        "payload": payload.model_dump(),
        "result": None,
        "error": None,
    }
    store["jobs"][job_id] = job
    store["active"] = job_id
    asyncio.create_task(_run_manual_pull_job(request, job_id, cfg, payload))
    return {
        "queued": True,
        "status": "pending",
        "job_id": job_id,
        "message": "新闻抓取与结构化已转入后台串行执行",
        "job": job,
        "latest_result": store.get("latest"),
    }


@router.post("/ingest/pull_now")
async def pull_now_alias(
    request: Request,
    payload: PullNowRequest = Body(default_factory=PullNowRequest),
    background: bool = Query(default=True),
) -> Dict[str, Any]:
    return await pull_now(request=request, payload=payload, background=background)


@router.get("/pull_status")
async def pull_status(request: Request) -> Dict[str, Any]:
    store = _news_job_store(request)
    active_job_id = store.get("active")
    active_job = (store.get("jobs") or {}).get(active_job_id) if active_job_id else None
    return {
        "active_job_id": active_job_id,
        "active_job": active_job,
        "latest_result": store.get("latest"),
        "jobs": list((store.get("jobs") or {}).values())[-10:],
        "source_states": await news_db.list_source_states(),
        "llm_queue": await news_db.get_llm_queue_stats(),
    }


@router.get("/worker_status")
async def worker_status(request: Request) -> Dict[str, Any]:
    store = _news_job_store(request)
    return {
        "timestamp": _now_utc().isoformat(),
        "latest_result": store.get("latest"),
        "source_states": await news_db.list_source_states(),
        "llm_queue": await news_db.get_llm_queue_stats(),
    }


@router.get("/latest")
async def latest(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    hours: int = Query(default=24, ge=1, le=168),
    limit: int = Query(default=30, ge=1, le=300),
    summarize: bool = Query(default=False),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    symbol_norm = _normalize_symbol(symbol, cfg)
    cache_key = _cache_key(symbol_norm, hours, limit, "sum" if summarize else "fast")
    ttl_sec = _env_int("NEWS_API_SUMMARY_CACHE_TTL_SEC" if summarize else "NEWS_API_LATEST_CACHE_TTL_SEC", 20 if summarize else 8)
    cached = _cache_get("latest", cache_key, ttl_sec)
    if cached:
        cached["auto_pull_triggered"] = False
        return cached

    try:
        if summarize:
            timeout_sec = _env_int("NEWS_API_SUMMARIZE_TIMEOUT_SEC", 8)
            feed = await asyncio.wait_for(
                build_latest_feed(cfg=cfg, symbol=symbol, hours=hours, limit=limit, summarize=True),
                timeout=max(2, timeout_sec),
            )
        else:
            feed = await build_latest_feed(cfg=cfg, symbol=symbol, hours=hours, limit=limit, summarize=False)
        auto_pull = await _auto_pull_if_stale(cfg=cfg, latest_items=feed.get("items") or [], hours=hours)
        feed["auto_pull_triggered"] = bool(auto_pull)
        return _cache_set("latest", cache_key, feed)
    except Exception as exc:
        logger.warning(f"news latest failed summarize={summarize} symbol={symbol_norm or '-'}: {exc}")
        stale = _cache_get_stale("latest", cache_key)
        if stale:
            stale["auto_pull_triggered"] = False
            stale["fallback_reason"] = str(exc)
            return stale
        if summarize:
            fast_key = _cache_key(symbol_norm, hours, limit, "fast")
            fast_stale = _cache_get("latest", fast_key, _env_int("NEWS_API_LATEST_CACHE_TTL_SEC", 8)) or _cache_get_stale("latest", fast_key)
            if fast_stale:
                fast_stale["auto_pull_triggered"] = False
                fast_stale["fallback_reason"] = f"summarize fallback: {exc}"
                return fast_stale
            feed = await build_latest_feed(cfg=cfg, symbol=symbol, hours=hours, limit=limit, summarize=False)
            feed["auto_pull_triggered"] = False
            feed["fallback_reason"] = f"summarize fallback: {exc}"
            return _cache_set("latest", fast_key, feed)
        raise


@router.get("/events")
async def events(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    symbol_norm = _normalize_symbol(symbol, cfg)

    if since:
        try:
            since_ts = parse_any_datetime(since)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid since value: {exc}") from exc
    else:
        since_ts = _now_utc() - timedelta(hours=24)

    rows = await news_db.list_events(symbol=symbol_norm, since=since_ts, limit=limit)
    return {
        "count": len(rows),
        "symbol": symbol_norm,
        "since": since_ts.isoformat(),
        "items": rows,
    }


@router.get("/summary")
async def summary(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    hours: int = Query(default=24, ge=1, le=168),
    feed_limit: int = Query(default=120, ge=20, le=300),
) -> Dict[str, Any]:
    cfg = _get_cfg(request)
    symbol_norm = _normalize_symbol(symbol, cfg)
    cache_key = _cache_key(symbol_norm, hours, feed_limit)
    ttl_sec = _env_int("NEWS_API_SUMMARY_CACHE_TTL_SEC", 12)
    cached = _cache_get("summary", cache_key, ttl_sec)
    if cached:
        return cached
    since = _now_utc() - timedelta(hours=hours)
    try:
        events = await news_db.list_events(symbol=symbol_norm, since=since, limit=5000)
        raw_rows = await news_db.list_news_raw(since=since, limit=5000)
        source_states = await news_db.list_source_states()
        llm_queue = await news_db.get_llm_queue_stats()
        feed_preview = await build_latest_feed(cfg=cfg, symbol=symbol_norm, hours=hours, limit=feed_limit, summarize=False)

        sentiment = {"positive": 0, "neutral": 0, "negative": 0}
        by_type: Dict[str, int] = {}
        by_symbol: Dict[str, int] = {}
        by_provider: Dict[str, int] = _count_by_provider(raw_rows)

        for event in events:
            s = int(event.get("sentiment") or 0)
            if s > 0:
                sentiment["positive"] += 1
            elif s < 0:
                sentiment["negative"] += 1
            else:
                sentiment["neutral"] += 1

            event_type = str(event.get("event_type") or "other")
            by_type[event_type] = by_type.get(event_type, 0) + 1

            sym = str(event.get("symbol") or "")
            if sym:
                by_symbol[sym] = by_symbol.get(sym, 0) + 1

        sorted_by_type = dict(sorted(by_type.items(), key=lambda kv: kv[1], reverse=True))
        sorted_by_symbol = dict(sorted(by_symbol.items(), key=lambda kv: kv[1], reverse=True)[:12])

        payload = {
            "symbol": symbol_norm,
            "hours": hours,
            "since": since.isoformat(),
            "raw_count": len(raw_rows),
            "events_count": len(events),
            "sentiment": sentiment,
            "feed_count": int(feed_preview.get("count") or 0),
            "feed_stats": feed_preview.get("feed_stats") or _feed_sentiment_summary([]),
            "by_type": sorted_by_type,
            "by_symbol": sorted_by_symbol,
            "by_provider": by_provider,
            "source_summary": _build_source_summary(raw_rows, source_states),
            "source_states": source_states,
            "llm_queue": llm_queue,
            "bucket_stats": _bucketize_events(events),
            "timestamp": _now_utc().isoformat(),
        }
        return _cache_set("summary", cache_key, payload)
    except Exception as exc:
        logger.warning(f"news summary failed symbol={symbol_norm or '-'}: {exc}")
        stale = _cache_get_stale("summary", cache_key)
        if stale:
            stale["fallback_reason"] = str(exc)
            return stale
        raise
