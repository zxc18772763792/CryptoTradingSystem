"""Async DB helpers for news/event storage."""
from __future__ import annotations

import hashlib
import math
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config.settings import settings
from core.news.storage.models import (
    EventSchema,
    NewsBase,
    NewsEvent,
    NewsLLMTask,
    NewsRaw,
    NewsSourceState,
    parse_any_datetime,
)


news_engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True)
NewsSessionLocal = async_sessionmaker(news_engine, class_=AsyncSession, expire_on_commit=False)


SOURCE_IMPORTANCE = {
    "binance_announcements": 48,
    "okx_announcements": 46,
    "bybit_announcements": 46,
    "chaincatcher_flash": 42,
    "jin10": 40,
    "cryptopanic": 35,
    "cryptocompare_news": 34,
    "rss": 24,
    "newsapi": 18,
    "gdelt": 12,
}

KEYWORD_SCORES = {
    "listing": 18,
    "delist": 20,
    "delisting": 20,
    "perpetual": 16,
    "futures": 14,
    "maintenance": 18,
    "api update": 15,
    "api": 10,
    "hack": 26,
    "exploit": 24,
    "drained": 24,
    "stolen": 24,
    "etf": 18,
    "sec": 16,
    "lawsuit": 14,
    "liquidation": 16,
    "liquidations": 16,
    "whale": 10,
    "upgrade": 10,
    "mainnet": 10,
    "maintenance complete": 8,
    "maintenance completed": 8,
    "rule": 8,
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _utc_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    dt = value if isinstance(value, datetime) else parse_any_datetime(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _hash_news(url: str, title: str, published_at: datetime) -> str:
    title_bucket = _title_bucket_key(title, published_at)
    if title_bucket:
        seed = f"title_bucket|{title_bucket}"
    else:
        seed = f"{url}|{title}|{published_at.isoformat()}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _canonical_title(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\u4e00-\u9fff ]+", "", text)
    return text.strip()


def _title_bucket_key(title: Any, published_at: datetime) -> str:
    canon = _canonical_title(title)
    if not canon:
        return ""
    bucket = int(parse_any_datetime(published_at).timestamp() // 1800)
    return hashlib.sha1(f"{canon}|{bucket}".encode("utf-8")).hexdigest()


def _event_semantic_key(symbol: Any, event_type: Any, sentiment: Any, ts: Any, evidence: Optional[Dict[str, Any]]) -> str:
    dt = parse_any_datetime(ts)
    bucket = int(dt.timestamp() // 1800)
    title_key = _canonical_title((evidence or {}).get("title"))
    url_key = str((evidence or {}).get("url") or "").strip().lower()
    anchor = title_key or url_key or "no_anchor"
    seed = f"{str(symbol).upper()}|{str(event_type).lower()}|{int(sentiment)}|{bucket}|{anchor}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _importance_score(source: str, title: str, content: str, payload: Dict[str, Any]) -> int:
    text = f"{title} {content}".lower()
    score = SOURCE_IMPORTANCE.get(str(payload.get("provider") or source).lower(), SOURCE_IMPORTANCE.get(str(source).lower(), 10))
    for keyword, bonus in KEYWORD_SCORES.items():
        if keyword in text:
            score += bonus
    if re.search(r"\b[A-Z]{2,10}USDT\b", f"{title} {content}"):
        score += 8
    symbols = payload.get("currencies") or payload.get("symbols") or []
    if isinstance(symbols, list) and symbols:
        score += min(12, 3 * len(symbols))
    if "announcement" in text:
        score += 6
    return max(0, min(int(score), 100))


def _latency_seconds(published_at: datetime, fetched_at: datetime) -> float:
    return max(0.0, round((fetched_at - published_at).total_seconds(), 3))


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    clean = sorted(float(v) for v in values)
    if len(clean) == 1:
        return round(clean[0], 3)
    idx = max(0.0, min(1.0, p / 100.0)) * (len(clean) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return round(clean[lo], 3)
    frac = idx - lo
    return round(clean[lo] * (1 - frac) + clean[hi] * frac, 3)


def _normalize_news_item(item: Dict[str, Any]) -> Dict[str, Any]:
    url = str(item.get("url") or "").strip()
    title = str(item.get("title") or "").strip()
    source = str(item.get("source") or "gdelt").strip() or "gdelt"
    content = str(item.get("content") or item.get("summary") or "").strip()
    lang = str(item.get("lang") or item.get("language") or "en").strip() or "en"

    published_raw = item.get("published_at") or item.get("published") or item.get("seendate")
    published = parse_any_datetime(published_raw) if published_raw else datetime.now(timezone.utc)
    fetched_at = datetime.now(timezone.utc)
    payload = dict(item.get("payload") or {})
    symbols = item.get("symbols") or payload.get("symbols") or {}
    content_hash = str(item.get("content_hash") or "").strip() or _hash_news(url, title, published)
    dedupe_key = _title_bucket_key(title, published) or content_hash
    importance = _importance_score(source, title, content, payload)
    payload.setdefault("provider", str(item.get("provider") or payload.get("provider") or source))
    payload["dedupe_key"] = dedupe_key
    payload["importance_score"] = importance
    payload["latency_sec"] = _latency_seconds(published, fetched_at)
    payload["published_at"] = published.isoformat()
    payload["fetched_at"] = fetched_at.isoformat()

    return {
        "source": source,
        "title": title,
        "url": url,
        "content": content,
        "published_at": published,
        "fetched_at": fetched_at,
        "lang": lang,
        "content_hash": content_hash,
        "symbols": symbols,
        "payload": payload,
    }


def _row_to_news_dict(row: NewsRaw) -> Dict[str, Any]:
    return {
        "id": row.id,
        "source": row.source,
        "title": row.title,
        "url": row.url,
        "content": row.content,
        "published_at": _utc_iso(row.published_at),
        "fetched_at": _utc_iso(row.fetched_at),
        "lang": row.lang,
        "content_hash": row.content_hash,
        "symbols": row.symbols or {},
        "payload": row.payload or {},
    }


def _row_to_event_dict(row: NewsEvent) -> Dict[str, Any]:
    return {
        "id": row.id,
        "event_id": row.event_id,
        "ts": _utc_iso(row.ts),
        "symbol": row.symbol,
        "event_type": row.event_type,
        "sentiment": int(row.sentiment),
        "impact_score": _safe_float(row.impact_score),
        "half_life_min": int(row.half_life_min),
        "evidence": row.evidence or {},
        "model_source": row.model_source,
        "raw_news_id": row.raw_news_id,
        "payload": row.payload or {},
        "created_at": _utc_iso(row.created_at),
    }


def _row_to_state_dict(row: NewsSourceState) -> Dict[str, Any]:
    return {
        "source": row.source,
        "cursor_type": row.cursor_type,
        "cursor_value": row.cursor_value,
        "updated_at": _utc_iso(row.updated_at),
        "last_success_at": _utc_iso(row.last_success_at),
        "paused_until": _utc_iso(row.paused_until),
        "last_error": row.last_error,
        "error_count": int(row.error_count or 0),
        "success_count": int(row.success_count or 0),
        "failure_count": int(row.failure_count or 0),
    }


async def init_news_db() -> None:
    async with news_engine.begin() as conn:
        await conn.run_sync(NewsBase.metadata.create_all)


async def close_news_db() -> None:
    await news_engine.dispose()


@asynccontextmanager
async def news_session_scope() -> Iterable[AsyncSession]:
    session = NewsSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_source_state(source: str) -> Optional[Dict[str, Any]]:
    source_name = str(source or "").strip().lower()
    if not source_name:
        return None
    async with news_session_scope() as session:
        row = (await session.execute(select(NewsSourceState).where(NewsSourceState.source == source_name))).scalar_one_or_none()
    return _row_to_state_dict(row) if row else None


async def list_source_states() -> List[Dict[str, Any]]:
    async with news_session_scope() as session:
        rows = (await session.execute(select(NewsSourceState).order_by(NewsSourceState.source.asc()))).scalars().all()
    return [_row_to_state_dict(row) for row in rows]


async def get_llm_queue_stats() -> Dict[str, Any]:
    async with news_session_scope() as session:
        rows = (await session.execute(select(NewsLLMTask.status, NewsLLMTask.priority))).all()
    counts: Dict[str, int] = {"pending": 0, "running": 0, "retry": 0, "failed": 0, "done": 0}
    priorities: List[int] = []
    for status, priority in rows:
        key = str(status or "pending").strip().lower() or "pending"
        counts[key] = counts.get(key, 0) + 1
        try:
            priorities.append(int(priority or 0))
        except Exception:
            continue
    return {
        "counts": counts,
        "total": int(sum(counts.values())),
        "pending_total": int(counts.get("pending", 0) + counts.get("retry", 0)),
        "max_priority": max(priorities) if priorities else 0,
    }


async def set_source_state(
    source: str,
    *,
    cursor_type: str = "ts",
    cursor_value: Optional[str] = None,
    last_error: Optional[str] = None,
    clear_error: bool = False,
    mark_success: bool = False,
    mark_failure: bool = False,
    paused_until: Optional[datetime] = None,
) -> Dict[str, Any]:
    source_name = str(source or "").strip().lower()
    if not source_name:
        raise ValueError("source is required")
    now = datetime.now(timezone.utc)
    pause_ts = parse_any_datetime(paused_until) if paused_until else None
    async with news_session_scope() as session:
        row = (await session.execute(select(NewsSourceState).where(NewsSourceState.source == source_name))).scalar_one_or_none()
        if row is None:
            row = NewsSourceState(source=source_name, cursor_type=cursor_type or "ts")
            session.add(row)
        row.cursor_type = cursor_type or row.cursor_type or "ts"
        if cursor_value is not None:
            row.cursor_value = str(cursor_value)
        row.updated_at = now
        if clear_error:
            row.last_error = None
            row.error_count = 0
        if last_error:
            row.last_error = str(last_error)
        if mark_success:
            row.last_success_at = now
            row.success_count = int(row.success_count or 0) + 1
            if not last_error:
                row.last_error = None
                row.error_count = 0
        if mark_failure:
            row.failure_count = int(row.failure_count or 0) + 1
            row.error_count = int(row.error_count or 0) + 1
        if pause_ts is not None:
            row.paused_until = pause_ts
        await session.flush()
        return _row_to_state_dict(row)


async def list_news_raw_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    keys = [int(x) for x in ids if x]
    if not keys:
        return []
    async with news_session_scope() as session:
        rows = (await session.execute(select(NewsRaw).where(NewsRaw.id.in_(keys)).order_by(NewsRaw.published_at.desc()))).scalars().all()
    return [_row_to_news_dict(row) for row in rows]


async def save_news_raw(news_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    pulled_count = len(news_items)
    if pulled_count == 0:
        return {"inserted": [], "pulled_count": 0, "deduped_count": 0}

    normalized: List[Dict[str, Any]] = []
    seen_local: set[str] = set()
    seen_local_title_buckets: set[str] = set()
    local_dup = 0
    for item in news_items:
        row = _normalize_news_item(item)
        if not row["url"]:
            local_dup += 1
            continue
        dedupe_key = f"{row['url']}|{row['content_hash']}"
        title_bucket = _title_bucket_key(row["title"], row["published_at"])
        if dedupe_key in seen_local or (title_bucket and title_bucket in seen_local_title_buckets):
            local_dup += 1
            continue
        seen_local.add(dedupe_key)
        if title_bucket:
            seen_local_title_buckets.add(title_bucket)
        normalized.append(row)

    if not normalized:
        return {"inserted": [], "pulled_count": pulled_count, "deduped_count": pulled_count}

    urls = [item["url"] for item in normalized]
    hashes = [item["content_hash"] for item in normalized]
    min_ts = min(item["published_at"] for item in normalized) - timedelta(hours=12)
    max_ts = max(item["published_at"] for item in normalized) + timedelta(hours=12)

    async with news_session_scope() as session:
        existing_url_rows = await session.execute(select(NewsRaw.url).where(NewsRaw.url.in_(urls)))
        existing_hash_rows = await session.execute(select(NewsRaw.content_hash).where(NewsRaw.content_hash.in_(hashes)))
        existing_recent_rows = await session.execute(
            select(NewsRaw.title, NewsRaw.published_at).where(and_(NewsRaw.published_at >= min_ts, NewsRaw.published_at <= max_ts))
        )
        existing_urls = {row[0] for row in existing_url_rows.all()}
        existing_hashes = {row[0] for row in existing_hash_rows.all()}
        existing_title_buckets = {
            _title_bucket_key(row[0], row[1])
            for row in existing_recent_rows.all()
            if row and row[0] and row[1]
        }

        objects: List[NewsRaw] = []
        deduped_count = local_dup
        for item in normalized:
            title_bucket = _title_bucket_key(item["title"], item["published_at"])
            if item["url"] in existing_urls or item["content_hash"] in existing_hashes or (title_bucket and title_bucket in existing_title_buckets):
                deduped_count += 1
                continue
            obj = NewsRaw(**item)
            session.add(obj)
            objects.append(obj)
            if title_bucket:
                existing_title_buckets.add(title_bucket)

        await session.flush()
        inserted = [_row_to_news_dict(obj) for obj in objects]

    return {
        "inserted": inserted,
        "pulled_count": pulled_count,
        "deduped_count": deduped_count,
    }


async def enqueue_llm_tasks(news_items: List[Dict[str, Any]], min_importance: int = 35) -> Dict[str, Any]:
    rows = [item for item in news_items if isinstance(item, dict) and item.get("id")]
    if not rows:
        return {"queued_count": 0, "skipped_count": 0}
    raw_ids = [int(item["id"]) for item in rows]
    async with news_session_scope() as session:
        existing = {
            row[0]
            for row in (await session.execute(select(NewsLLMTask.raw_news_id).where(NewsLLMTask.raw_news_id.in_(raw_ids)))).all()
        }
        queued = 0
        skipped = 0
        for item in rows:
            raw_id = int(item["id"])
            if raw_id in existing:
                skipped += 1
                continue
            payload = item.get("payload") or {}
            importance = int(payload.get("importance_score") or 0)
            if importance < int(min_importance or 0):
                skipped += 1
                continue
            session.add(
                NewsLLMTask(
                    raw_news_id=raw_id,
                    source=str(item.get("source") or payload.get("provider") or "news"),
                    status="pending",
                    priority=importance,
                )
            )
            queued += 1
        await session.flush()
    return {"queued_count": queued, "skipped_count": skipped}


async def claim_llm_tasks(limit: int = 10) -> List[Dict[str, Any]]:
    max_rows = max(1, min(int(limit or 10), 100))
    now = datetime.now(timezone.utc)
    async with news_session_scope() as session:
        rows = (
            await session.execute(
                select(NewsLLMTask)
                .where(NewsLLMTask.status.in_(["pending", "retry"]))
                .order_by(NewsLLMTask.priority.desc(), NewsLLMTask.created_at.asc())
                .limit(max_rows)
            )
        ).scalars().all()
        raw_ids: List[int] = []
        for row in rows:
            row.status = "running"
            row.attempt_count = int(row.attempt_count or 0) + 1
            row.started_at = now
            row.updated_at = now
            raw_ids.append(int(row.raw_news_id))
        await session.flush()
        news_rows = (
            await session.execute(select(NewsRaw).where(NewsRaw.id.in_(raw_ids)).order_by(NewsRaw.published_at.desc()))
        ).scalars().all()
        news_by_id = {int(row.id): _row_to_news_dict(row) for row in news_rows}
        tasks: List[Dict[str, Any]] = []
        for row in rows:
            payload = news_by_id.get(int(row.raw_news_id))
            if not payload:
                continue
            payload["llm_task"] = {
                "task_id": row.id,
                "status": row.status,
                "attempt_count": row.attempt_count,
                "priority": row.priority,
            }
            tasks.append(payload)
        return tasks


async def finish_llm_tasks(raw_news_ids: List[int], *, success: bool, error: Optional[str] = None) -> None:
    keys = [int(x) for x in raw_news_ids if x]
    if not keys:
        return
    now = datetime.now(timezone.utc)
    async with news_session_scope() as session:
        rows = (
            await session.execute(select(NewsLLMTask).where(NewsLLMTask.raw_news_id.in_(keys)))
        ).scalars().all()
        for row in rows:
            row.status = "done" if success else ("failed" if int(row.attempt_count or 0) >= 2 else "retry")
            row.last_error = None if success else str(error or "llm extraction failed")
            row.updated_at = now
            row.finished_at = now if success or row.status == "failed" else None
        await session.flush()


async def save_events(events: List[Dict[str, Any]], model_source: str = "rules") -> Dict[str, Any]:
    if not events:
        return {"inserted": [], "events_count": 0, "deduped_count": 0}

    validated: List[Dict[str, Any]] = []
    seen_local_semantic: set[str] = set()
    deduped_count = 0
    for raw in events:
        item = EventSchema.model_validate(raw)
        payload = item.model_dump(mode="json")
        payload["model_source"] = str(raw.get("model_source") or model_source)
        payload["raw_news_id"] = raw.get("raw_news_id")
        payload["payload"] = raw.get("payload") or {}
        semantic_key = _event_semantic_key(payload["symbol"], payload["event_type"], payload["sentiment"], payload["ts"], payload.get("evidence"))
        if semantic_key in seen_local_semantic:
            deduped_count += 1
            continue
        seen_local_semantic.add(semantic_key)
        payload["_semantic_key"] = semantic_key
        validated.append(payload)

    event_ids = [item["event_id"] for item in validated]
    min_ts = min(parse_any_datetime(item["ts"]) for item in validated) - timedelta(hours=12)
    max_ts = max(parse_any_datetime(item["ts"]) for item in validated) + timedelta(hours=12)

    async with news_session_scope() as session:
        existing_rows = await session.execute(select(NewsEvent.event_id).where(NewsEvent.event_id.in_(event_ids)))
        existing = {row[0] for row in existing_rows.all()}
        existing_recent_rows = await session.execute(
            select(NewsEvent.symbol, NewsEvent.event_type, NewsEvent.sentiment, NewsEvent.ts, NewsEvent.evidence).where(
                and_(NewsEvent.ts >= min_ts, NewsEvent.ts <= max_ts)
            )
        )
        existing_semantic = {
            _event_semantic_key(row[0], row[1], row[2], row[3], row[4] if isinstance(row[4], dict) else {})
            for row in existing_recent_rows.all()
        }

        objects: List[NewsEvent] = []
        for item in validated:
            semantic_key = item.pop("_semantic_key", "")
            if item["event_id"] in existing or (semantic_key and semantic_key in existing_semantic):
                deduped_count += 1
                continue
            obj = NewsEvent(
                event_id=item["event_id"],
                ts=parse_any_datetime(item["ts"]),
                symbol=str(item["symbol"]).upper(),
                event_type=item["event_type"],
                sentiment=int(item["sentiment"]),
                impact_score=float(item["impact_score"]),
                half_life_min=int(item["half_life_min"]),
                evidence=item.get("evidence") or {},
                model_source=str(item.get("model_source") or model_source),
                raw_news_id=item.get("raw_news_id"),
                payload=item.get("payload") or {},
            )
            session.add(obj)
            objects.append(obj)
            if semantic_key:
                existing_semantic.add(semantic_key)

        await session.flush()
        inserted = [_row_to_event_dict(obj) for obj in objects]

    return {
        "inserted": inserted,
        "events_count": len(inserted),
        "deduped_count": deduped_count,
    }


async def list_news_raw(since: Optional[datetime] = None, limit: int = 200) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit or 200), 10000))
    since_ts = parse_any_datetime(since) if since else None
    async with news_session_scope() as session:
        stmt = select(NewsRaw)
        if since_ts:
            stmt = stmt.where(NewsRaw.published_at >= since_ts)
        stmt = stmt.order_by(NewsRaw.published_at.desc()).limit(limit)
        rows = (await session.execute(stmt)).scalars().all()
    return [_row_to_news_dict(row) for row in rows]


async def list_events(symbol: Optional[str] = None, since: Optional[datetime] = None, limit: int = 200) -> List[Dict[str, Any]]:
    symbol_norm = str(symbol or "").strip().upper()
    limit = max(1, min(int(limit or 200), 10000))
    since_ts = parse_any_datetime(since) if since else None
    async with news_session_scope() as session:
        stmt = select(NewsEvent)
        if symbol_norm:
            stmt = stmt.where(NewsEvent.symbol == symbol_norm)
        if since_ts:
            stmt = stmt.where(NewsEvent.ts >= since_ts)
        stmt = stmt.order_by(NewsEvent.ts.desc()).limit(limit)
        rows = (await session.execute(stmt)).scalars().all()
    return [_row_to_event_dict(row) for row in rows]


async def get_recent_events(symbol: Optional[str], since_minutes: int) -> List[Dict[str, Any]]:
    minutes = max(1, int(since_minutes or 240))
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return await list_events(symbol=symbol, since=since, limit=2000)


async def build_daily_report(day: date) -> Dict[str, Any]:
    day_start = datetime.combine(day, time.min).replace(tzinfo=timezone.utc)
    day_end = datetime.combine(day, time.max).replace(tzinfo=timezone.utc)

    async with news_session_scope() as session:
        event_rows = (
            await session.execute(select(NewsEvent).where(and_(NewsEvent.ts >= day_start, NewsEvent.ts <= day_end)).order_by(NewsEvent.ts.asc()))
        ).scalars().all()
        raw_rows = (
            await session.execute(select(NewsRaw).where(and_(NewsRaw.published_at >= day_start, NewsRaw.published_at <= day_end)).order_by(NewsRaw.published_at.asc()))
        ).scalars().all()
        state_rows = (await session.execute(select(NewsSourceState).order_by(NewsSourceState.source.asc()))).scalars().all()

    by_symbol: Dict[str, int] = {}
    by_type: Dict[str, int] = {}
    by_sentiment: Dict[str, int] = {"-1": 0, "0": 0, "1": 0}
    top_impacts: List[Dict[str, Any]] = []
    for row in event_rows:
        by_symbol[row.symbol] = by_symbol.get(row.symbol, 0) + 1
        by_type[row.event_type] = by_type.get(row.event_type, 0) + 1
        key = str(int(row.sentiment))
        by_sentiment[key] = by_sentiment.get(key, 0) + 1
        top_impacts.append(
            {
                "event_id": row.event_id,
                "symbol": row.symbol,
                "event_type": row.event_type,
                "impact_score": float(row.impact_score),
                "sentiment": int(row.sentiment),
                "title": (row.evidence or {}).get("title", ""),
                "url": (row.evidence or {}).get("url", ""),
            }
        )
    top_impacts.sort(key=lambda x: abs(x["impact_score"] * x["sentiment"]), reverse=True)

    source_summary: Dict[str, Dict[str, Any]] = {}
    top_importance_news: List[Dict[str, Any]] = []
    for row in raw_rows:
        payload = row.payload or {}
        source = str(row.source or payload.get("provider") or "unknown")
        summary = source_summary.setdefault(
            source,
            {"inserted_count": 0, "latencies": [], "max_importance": 0, "latest_at": None},
        )
        summary["inserted_count"] += 1
        latency = _safe_float(payload.get("latency_sec"), 0.0)
        if latency > 0:
            summary["latencies"].append(latency)
        importance = int(payload.get("importance_score") or 0)
        summary["max_importance"] = max(summary["max_importance"], importance)
        summary["latest_at"] = _utc_iso(row.published_at)
        top_importance_news.append(
            {
                "id": row.id,
                "source": source,
                "title": row.title,
                "url": row.url,
                "importance_score": importance,
                "latency_sec": latency,
                "published_at": _utc_iso(row.published_at),
            }
        )

    states = {row.source: row for row in state_rows}
    for source, summary in source_summary.items():
        latencies = summary.pop("latencies", [])
        state = states.get(source)
        success_count = int(getattr(state, "success_count", 0) or 0)
        failure_count = int(getattr(state, "failure_count", 0) or 0)
        total_runs = success_count + failure_count
        summary["failure_rate"] = round((failure_count / total_runs), 4) if total_runs else 0.0
        summary["latency_p50"] = _percentile(latencies, 50)
        summary["latency_p95"] = _percentile(latencies, 95)
        summary["last_error"] = getattr(state, "last_error", None) if state else None
        summary["paused_until"] = _utc_iso(getattr(state, "paused_until", None)) if state else None

    top_importance_news.sort(key=lambda x: (x["importance_score"], -x["latency_sec"]), reverse=True)

    return {
        "date": day.isoformat(),
        "events_total": len(event_rows),
        "raw_news_total": len(raw_rows),
        "by_symbol": dict(sorted(by_symbol.items(), key=lambda kv: kv[1], reverse=True)[:20]),
        "by_type": dict(sorted(by_type.items(), key=lambda kv: kv[1], reverse=True)),
        "by_sentiment": by_sentiment,
        "top_impacts": top_impacts[:20],
        "source_summary": dict(sorted(source_summary.items(), key=lambda kv: kv[1]["inserted_count"], reverse=True)),
        "top_importance_news": top_importance_news[:20],
        "top_importance_events": top_impacts[:10],
    }
