"""Async DB helpers for news/event storage."""
from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config.settings import settings
from core.news.storage.models import EventSchema, NewsBase, NewsEvent, NewsRaw, parse_any_datetime


news_engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True)
NewsSessionLocal = async_sessionmaker(news_engine, class_=AsyncSession, expire_on_commit=False)


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
    seed = f"{url}|{title}|{published_at.isoformat()}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _normalize_news_item(item: Dict[str, Any]) -> Dict[str, Any]:
    url = str(item.get("url") or "").strip()
    title = str(item.get("title") or "").strip()
    source = str(item.get("source") or "gdelt").strip() or "gdelt"
    content = str(item.get("content") or item.get("summary") or "").strip()
    lang = str(item.get("lang") or item.get("language") or "en").strip() or "en"

    published_raw = item.get("published_at") or item.get("published") or item.get("seendate")
    if not published_raw:
        published = datetime.now(timezone.utc)
    else:
        published = parse_any_datetime(published_raw)

    symbols = item.get("symbols") or {}
    payload = item.get("payload") or {}
    content_hash = str(item.get("content_hash") or "").strip() or _hash_news(url, title, published)

    return {
        "source": source,
        "title": title,
        "url": url,
        "content": content,
        "published_at": published,
        "fetched_at": datetime.now(timezone.utc),
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


async def init_news_db() -> None:
    """Create tables used by news service."""
    async with news_engine.begin() as conn:
        await conn.run_sync(NewsBase.metadata.create_all)


async def close_news_db() -> None:
    """Dispose news DB engine."""
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


async def save_news_raw(news_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Insert news rows with dedupe by URL/hash."""
    pulled_count = len(news_items)
    if pulled_count == 0:
        return {"inserted": [], "pulled_count": 0, "deduped_count": 0}

    normalized: List[Dict[str, Any]] = []
    seen_local: set[str] = set()
    local_dup = 0
    for item in news_items:
        row = _normalize_news_item(item)
        if not row["url"]:
            local_dup += 1
            continue
        dedupe_key = f"{row['url']}|{row['content_hash']}"
        if dedupe_key in seen_local:
            local_dup += 1
            continue
        seen_local.add(dedupe_key)
        normalized.append(row)

    if not normalized:
        return {"inserted": [], "pulled_count": pulled_count, "deduped_count": pulled_count}

    urls = [item["url"] for item in normalized]
    hashes = [item["content_hash"] for item in normalized]

    async with news_session_scope() as session:
        existing_url_rows = await session.execute(select(NewsRaw.url).where(NewsRaw.url.in_(urls)))
        existing_hash_rows = await session.execute(select(NewsRaw.content_hash).where(NewsRaw.content_hash.in_(hashes)))
        existing_urls = {row[0] for row in existing_url_rows.all()}
        existing_hashes = {row[0] for row in existing_hash_rows.all()}

        objects: List[NewsRaw] = []
        deduped_count = local_dup
        for item in normalized:
            if item["url"] in existing_urls or item["content_hash"] in existing_hashes:
                deduped_count += 1
                continue
            obj = NewsRaw(**item)
            session.add(obj)
            objects.append(obj)

        await session.flush()
        inserted = [_row_to_news_dict(obj) for obj in objects]

    return {
        "inserted": inserted,
        "pulled_count": pulled_count,
        "deduped_count": deduped_count,
    }


async def save_events(events: List[Dict[str, Any]], model_source: str = "rules") -> Dict[str, Any]:
    """Insert validated event rows with dedupe by event_id."""
    if not events:
        return {"inserted": [], "events_count": 0, "deduped_count": 0}

    validated: List[Dict[str, Any]] = []
    for raw in events:
        item = EventSchema.model_validate(raw)
        payload = item.model_dump(mode="json")
        payload["model_source"] = str(raw.get("model_source") or model_source)
        payload["raw_news_id"] = raw.get("raw_news_id")
        payload["payload"] = raw.get("payload") or {}
        validated.append(payload)

    event_ids = [item["event_id"] for item in validated]

    async with news_session_scope() as session:
        existing_rows = await session.execute(select(NewsEvent.event_id).where(NewsEvent.event_id.in_(event_ids)))
        existing = {row[0] for row in existing_rows.all()}

        objects: List[NewsEvent] = []
        deduped_count = 0
        for item in validated:
            if item["event_id"] in existing:
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

        await session.flush()
        inserted = [_row_to_event_dict(obj) for obj in objects]

    return {
        "inserted": inserted,
        "events_count": len(inserted),
        "deduped_count": deduped_count,
    }


async def list_news_raw(since: Optional[datetime] = None, limit: int = 200) -> List[Dict[str, Any]]:
    """Query raw news rows by optional time window."""
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
    """Query event list by optional symbol/time window."""
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
    """Get events in rolling window."""
    minutes = max(1, int(since_minutes or 240))
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return await list_events(symbol=symbol, since=since, limit=2000)


async def build_daily_report(day: date) -> Dict[str, Any]:
    """Aggregate daily event report."""
    day_start = datetime.combine(day, time.min).replace(tzinfo=timezone.utc)
    day_end = datetime.combine(day, time.max).replace(tzinfo=timezone.utc)

    async with news_session_scope() as session:
        stmt = select(NewsEvent).where(and_(NewsEvent.ts >= day_start, NewsEvent.ts <= day_end)).order_by(NewsEvent.ts.asc())
        rows = (await session.execute(stmt)).scalars().all()

    by_symbol: Dict[str, int] = {}
    by_type: Dict[str, int] = {}
    by_sentiment: Dict[str, int] = {"-1": 0, "0": 0, "1": 0}
    top_impacts: List[Dict[str, Any]] = []

    for row in rows:
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

    return {
        "date": day.isoformat(),
        "events_total": len(rows),
        "by_symbol": dict(sorted(by_symbol.items(), key=lambda kv: kv[1], reverse=True)[:20]),
        "by_type": dict(sorted(by_type.items(), key=lambda kv: kv[1], reverse=True)),
        "by_sentiment": by_sentiment,
        "top_impacts": top_impacts[:20],
    }
