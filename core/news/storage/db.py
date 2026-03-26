"""Async DB helpers for news/event storage."""
from __future__ import annotations

import asyncio
import hashlib
import math
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import and_, event, func, insert, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config.settings import settings
from core.news.text_normalizer import clean_news_text
from core.news.storage.models import (
    EventSchema,
    NewsBase,
    NewsEvent,
    NewsLLMTask,
    NewsRaw,
    NewsSourceState,
    parse_any_datetime,
)


_news_engine_kwargs: Dict[str, Any] = {"echo": False, "future": True}
try:
    _NEWS_SQLITE_BUSY_TIMEOUT_SEC = max(
        3.0,
        float(os.environ.get("NEWS_SQLITE_BUSY_TIMEOUT_SEC") or os.environ.get("SQLITE_BUSY_TIMEOUT_SEC") or "8"),
    )
except Exception:
    _NEWS_SQLITE_BUSY_TIMEOUT_SEC = 8.0
if str(settings.DATABASE_URL).startswith("sqlite"):
    _news_engine_kwargs["connect_args"] = {"timeout": _NEWS_SQLITE_BUSY_TIMEOUT_SEC}

news_engine = create_async_engine(settings.DATABASE_URL, **_news_engine_kwargs)
NewsSessionLocal = async_sessionmaker(news_engine, class_=AsyncSession, expire_on_commit=False)

if str(settings.DATABASE_URL).startswith("sqlite"):
    @event.listens_for(news_engine.sync_engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute(f"PRAGMA busy_timeout={int(_NEWS_SQLITE_BUSY_TIMEOUT_SEC * 1000)}")
        finally:
            cursor.close()

# Global rate limit backoff state
_global_rate_limit_backoff: Optional[datetime] = None
_global_rate_limit_lock = asyncio.Lock()

# Per-provider rate limit backoff state
_provider_rate_limit_backoff: Dict[str, datetime] = {}
_provider_rate_limit_lock = asyncio.Lock()


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
    "hack": 28,
    "hacked": 28,
    "exploit": 26,
    "drained": 26,
    "stolen": 26,
    "etf": 24,
    "etf approval": 28,
    "sec": 23,
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
    text = _strip_html_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\u4e00-\u9fff ]+", "", text)
    return text.strip()


def _strip_html_text(value: Any) -> str:
    return clean_news_text(value)


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


def _is_llm_summary_source(source: Any) -> bool:
    text = str(source or "").strip().lower()
    if not text:
        return False
    if text in {"glm", "glm5", "llm", "llm_cache", "glm_cache", "glm5_cache", "openai", "openai_responses", "codex", "responses"}:
        return True
    return ("glm" in text) or text.startswith("llm") or text.startswith("openai") or text.startswith("codex") or text.startswith("responses")


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


def _normalize_summary_sentiment(value: Any) -> str:
    sentiment = str(value or "neutral").strip().lower()
    if sentiment not in {"positive", "negative", "neutral"}:
        return "neutral"
    return sentiment


def _normalize_summary_source(value: Any) -> str:
    return str(value or "").strip().lower() or "unknown"


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
    title = _strip_html_text(item.get("title"))
    source = str(item.get("source") or "gdelt").strip() or "gdelt"
    content = _strip_html_text(item.get("content") or item.get("summary"))
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
        if news_engine.dialect.name == "sqlite":
            await _ensure_sqlite_news_schema(conn)


async def _ensure_sqlite_news_schema(conn) -> None:
    async def _table_columns(table_name: str) -> set[str]:
        rows = (await conn.execute(text(f"PRAGMA table_info({table_name})"))).fetchall()
        return {str(row[1]) for row in rows}

    llm_columns = await _table_columns("news_llm_tasks")
    llm_alters = {
        "next_retry_at": "ALTER TABLE news_llm_tasks ADD COLUMN next_retry_at DATETIME",
        "last_rate_limit_at": "ALTER TABLE news_llm_tasks ADD COLUMN last_rate_limit_at DATETIME",
        "started_at": "ALTER TABLE news_llm_tasks ADD COLUMN started_at DATETIME",
        "finished_at": "ALTER TABLE news_llm_tasks ADD COLUMN finished_at DATETIME",
    }
    for column_name, ddl in llm_alters.items():
        if column_name not in llm_columns:
            await conn.execute(text(ddl))

    state_columns = await _table_columns("news_source_state")
    state_alters = {
        "last_success_at": "ALTER TABLE news_source_state ADD COLUMN last_success_at DATETIME",
        "paused_until": "ALTER TABLE news_source_state ADD COLUMN paused_until DATETIME",
        "success_count": "ALTER TABLE news_source_state ADD COLUMN success_count INTEGER DEFAULT 0",
        "failure_count": "ALTER TABLE news_source_state ADD COLUMN failure_count INTEGER DEFAULT 0",
    }
    for column_name, ddl in state_alters.items():
        if column_name not in state_columns:
            await conn.execute(text(ddl))


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
        # Use SQL aggregation instead of full-table-scan + Python aggregation
        count_rows = (
            await session.execute(
                text("SELECT status, COUNT(*) as cnt FROM news_llm_tasks GROUP BY status")
            )
        ).all()
        agg_rows = (
            await session.execute(
                text(
                    "SELECT MAX(priority), MIN(next_retry_at) "
                    "FROM news_llm_tasks WHERE status IN ('pending','retry','running')"
                )
            )
        ).one_or_none()
    counts: Dict[str, int] = {"pending": 0, "running": 0, "retry": 0, "failed": 0, "done": 0}
    for status, cnt in count_rows:
        key = str(status or "pending").strip().lower() or "pending"
        counts[key] = int(cnt)
    max_priority = int(agg_rows[0] or 0) if agg_rows and agg_rows[0] is not None else 0
    next_retry_raw = agg_rows[1] if agg_rows else None
    next_retry_at = None
    if next_retry_raw:
        try:
            next_retry_at = parse_any_datetime(next_retry_raw)
        except Exception:
            pass
    backoff_until = await get_global_backoff()
    return {
        "counts": counts,
        "total": int(sum(counts.values())),
        "pending_total": int(counts.get("pending", 0) + counts.get("retry", 0)),
        "max_priority": max_priority,
        "backoff_until": _utc_iso(backoff_until),
        "next_retry_at": _utc_iso(next_retry_at),
    }


async def count_news_raw(since: Optional[datetime] = None) -> int:
    async with news_session_scope() as session:
        stmt = select(func.count(NewsRaw.id))
        if since is not None:
            stmt = stmt.where(NewsRaw.published_at >= parse_any_datetime(since))
        value = (await session.execute(stmt)).scalar_one()
    return int(value or 0)


async def count_events(symbol: Optional[str] = None, since: Optional[datetime] = None) -> int:
    async with news_session_scope() as session:
        stmt = select(func.count(NewsEvent.id))
        if symbol:
            stmt = stmt.where(NewsEvent.symbol == str(symbol).strip().upper())
        if since is not None:
            stmt = stmt.where(NewsEvent.ts >= parse_any_datetime(since))
        value = (await session.execute(stmt)).scalar_one()
    return int(value or 0)


async def latest_news_raw_timestamp(since: Optional[datetime] = None) -> Optional[str]:
    async with news_session_scope() as session:
        stmt = select(func.max(NewsRaw.published_at))
        if since is not None:
            stmt = stmt.where(NewsRaw.published_at >= parse_any_datetime(since))
        value = (await session.execute(stmt)).scalar_one()
    return _utc_iso(value) if value is not None else None


async def latest_event_timestamp(symbol: Optional[str] = None, since: Optional[datetime] = None) -> Optional[str]:
    async with news_session_scope() as session:
        stmt = select(func.max(NewsEvent.ts))
        if symbol:
            stmt = stmt.where(NewsEvent.symbol == str(symbol).strip().upper())
        if since is not None:
            stmt = stmt.where(NewsEvent.ts >= parse_any_datetime(since))
        value = (await session.execute(stmt)).scalar_one()
    return _utc_iso(value) if value is not None else None


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


async def save_news_raw_summaries(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Persist headline summaries into news_raw.payload with LLM-priority semantics."""
    if not rows:
        return {"updated_count": 0, "skipped_count": 0}

    normalized: Dict[int, Dict[str, Any]] = {}
    skipped_count = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped_count += 1
            continue
        raw_id = row.get("raw_news_id") or row.get("id")
        try:
            raw_key = int(raw_id)
        except Exception:
            skipped_count += 1
            continue

        summary_title = _strip_html_text(row.get("summary_title") or row.get("summary"))
        if not summary_title:
            skipped_count += 1
            continue
        summary_title = summary_title[:220]

        sentiment = _normalize_summary_sentiment(row.get("summary_sentiment") or row.get("sentiment") or "neutral")
        summary_source = _normalize_summary_source(row.get("summary_source") or row.get("source") or "")

        existing = normalized.get(raw_key)
        if existing:
            if _is_llm_summary_source(existing.get("summary_source")) and not _is_llm_summary_source(summary_source):
                continue
            if _is_llm_summary_source(summary_source) and not _is_llm_summary_source(existing.get("summary_source")):
                normalized[raw_key] = {
                    "summary_title": summary_title,
                    "summary_sentiment": sentiment,
                    "summary_source": summary_source,
                }
            continue

        normalized[raw_key] = {
            "summary_title": summary_title,
            "summary_sentiment": sentiment,
            "summary_source": summary_source,
        }

    if not normalized:
        return {"updated_count": 0, "skipped_count": skipped_count}

    now_iso = _utc_iso(datetime.now(timezone.utc))
    updated_count = 0
    async with news_session_scope() as session:
        db_rows = (
            await session.execute(select(NewsRaw).where(NewsRaw.id.in_(list(normalized.keys()))))
        ).scalars().all()
        for db_row in db_rows:
            incoming = normalized.get(int(db_row.id))
            if not incoming:
                skipped_count += 1
                continue

            payload = dict(db_row.payload or {})
            existing_source = _normalize_summary_source(payload.get("summary_source") or "")
            incoming_source = _normalize_summary_source(incoming.get("summary_source") or "")
            if _is_llm_summary_source(existing_source) and not _is_llm_summary_source(incoming_source):
                skipped_count += 1
                continue

            payload["summary_title"] = incoming.get("summary_title") or ""
            payload["summary_sentiment"] = _normalize_summary_sentiment(incoming.get("summary_sentiment") or "neutral")
            payload["summary_source"] = incoming_source or "unknown"
            payload["summary_updated_at"] = now_iso
            db_row.payload = payload
            updated_count += 1

        await session.flush()

    return {"updated_count": updated_count, "skipped_count": skipped_count}


async def save_news_event_summaries(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Persist translated summaries into news_events.payload with LLM-priority semantics."""
    if not rows:
        return {"updated_count": 0, "skipped_count": 0}

    normalized: Dict[str, Dict[str, Any]] = {}
    skipped_count = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped_count += 1
            continue
        event_id = str(row.get("event_id") or row.get("id") or "").strip()
        if not event_id:
            skipped_count += 1
            continue

        summary_title = _strip_html_text(row.get("summary_title") or row.get("summary"))
        if not summary_title:
            skipped_count += 1
            continue
        summary_title = summary_title[:220]

        sentiment = _normalize_summary_sentiment(row.get("summary_sentiment") or row.get("sentiment") or "neutral")
        summary_source = _normalize_summary_source(row.get("summary_source") or row.get("source") or "")

        existing = normalized.get(event_id)
        if existing:
            if _is_llm_summary_source(existing.get("summary_source")) and not _is_llm_summary_source(summary_source):
                continue
            if _is_llm_summary_source(summary_source) and not _is_llm_summary_source(existing.get("summary_source")):
                normalized[event_id] = {
                    "summary_title": summary_title,
                    "summary_sentiment": sentiment,
                    "summary_source": summary_source,
                }
            continue

        normalized[event_id] = {
            "summary_title": summary_title,
            "summary_sentiment": sentiment,
            "summary_source": summary_source,
        }

    if not normalized:
        return {"updated_count": 0, "skipped_count": skipped_count}

    now_iso = _utc_iso(datetime.now(timezone.utc))
    updated_count = 0
    async with news_session_scope() as session:
        db_rows = (
            await session.execute(select(NewsEvent).where(NewsEvent.event_id.in_(list(normalized.keys()))))
        ).scalars().all()
        for db_row in db_rows:
            incoming = normalized.get(str(db_row.event_id or "").strip())
            if not incoming:
                skipped_count += 1
                continue

            payload = dict(db_row.payload or {})
            existing_source = _normalize_summary_source(payload.get("summary_source") or "")
            incoming_source = _normalize_summary_source(incoming.get("summary_source") or "")
            if _is_llm_summary_source(existing_source) and not _is_llm_summary_source(incoming_source):
                skipped_count += 1
                continue

            payload["summary_title"] = incoming.get("summary_title") or ""
            payload["summary_sentiment"] = _normalize_summary_sentiment(incoming.get("summary_sentiment") or "neutral")
            payload["summary_source"] = incoming_source or "unknown"
            payload["summary_updated_at"] = now_iso
            db_row.payload = payload
            updated_count += 1

        await session.flush()

    return {"updated_count": updated_count, "skipped_count": skipped_count}


async def repair_news_raw_texts(since: Optional[datetime] = None, limit: int = 5000) -> Dict[str, int]:
    """Repair mojibake in stored raw news titles and persisted summaries."""
    max_rows = max(1, min(int(limit or 5000), 20000))
    since_ts = parse_any_datetime(since) if since else None
    scanned_count = 0
    updated_count = 0

    async with news_session_scope() as session:
        stmt = select(NewsRaw).order_by(NewsRaw.published_at.desc()).limit(max_rows)
        if since_ts:
            stmt = stmt.where(NewsRaw.published_at >= since_ts).order_by(NewsRaw.published_at.desc()).limit(max_rows)
        rows = (await session.execute(stmt)).scalars().all()
        for row in rows:
            scanned_count += 1
            changed = False

            cleaned_title = _strip_html_text(row.title)
            if cleaned_title and cleaned_title != str(row.title or ""):
                row.title = cleaned_title
                changed = True

            payload = dict(row.payload or {})
            existing_summary = payload.get("summary_title")
            cleaned_summary = _strip_html_text(existing_summary)
            if cleaned_summary and cleaned_summary != str(existing_summary or ""):
                payload["summary_title"] = cleaned_summary[:220]
                changed = True

            if changed:
                row.payload = payload
                updated_count += 1

        await session.flush()

    return {"scanned_count": scanned_count, "updated_count": updated_count}


async def list_llm_task_status(raw_ids: List[int]) -> Dict[int, str]:
    keys = [int(x) for x in raw_ids if x]
    if not keys:
        return {}
    async with news_session_scope() as session:
        rows = (
            await session.execute(
                select(NewsLLMTask.raw_news_id, NewsLLMTask.status).where(NewsLLMTask.raw_news_id.in_(keys))
            )
        ).all()
    out: Dict[int, str] = {}
    for raw_id, status in rows:
        try:
            out[int(raw_id)] = str(status or "").strip().lower()
        except Exception:
            continue
    return out


async def requeue_llm_tasks(statuses: Optional[List[str]] = None, limit: int = 200) -> Dict[str, Any]:
    target_statuses = [str(x or "").strip().lower() for x in (statuses or ["failed"]) if str(x or "").strip()]
    if not target_statuses:
        target_statuses = ["failed"]
    max_rows = max(1, min(int(limit or 200), 2000))
    now = datetime.now(timezone.utc)

    async with news_session_scope() as session:
        rows = (
            await session.execute(
                select(NewsLLMTask)
                .where(NewsLLMTask.status.in_(target_statuses))
                .order_by(NewsLLMTask.updated_at.desc())
                .limit(max_rows)
            )
        ).scalars().all()

        affected_ids: List[int] = []
        for row in rows:
            row.status = "retry"
            row.next_retry_at = now
            row.started_at = None
            row.finished_at = None
            row.updated_at = now
            row.attempt_count = 0
            affected_ids.append(int(row.raw_news_id))
        await session.flush()

    return {
        "matched_count": len(rows),
        "requeued_count": len(rows),
        "target_statuses": target_statuses,
        "raw_news_ids_sample": affected_ids[:20],
    }


async def auto_requeue_failed_llm_tasks(limit: int = 4, since: Optional[datetime] = None) -> Dict[str, Any]:
    """Gently requeue failed tasks that still need extraction.

    Selection rules:
    - status must be ``failed``
    - raw row must still exist
    - skip rows that already have an LLM summary persisted
    - skip rows that already produced at least one structured event
    """
    max_rows = max(1, min(int(limit or 4), 50))
    since_ts = parse_any_datetime(since) if since else None
    now = datetime.now(timezone.utc)
    scan_limit = min(480, max(80, max_rows * 24))

    async with news_session_scope() as session:
        stmt = (
            select(NewsLLMTask, NewsRaw)
            .join(NewsRaw, NewsRaw.id == NewsLLMTask.raw_news_id)
            .where(NewsLLMTask.status == "failed")
            .order_by(NewsLLMTask.priority.desc(), NewsLLMTask.updated_at.desc(), NewsRaw.published_at.desc())
            .limit(scan_limit)
        )
        rows = (await session.execute(stmt)).all()

        scanned_count = 0
        skipped_summary_count = 0
        skipped_event_count = 0
        candidate_rows: List[tuple[NewsLLMTask, NewsRaw]] = []
        candidate_ids: List[int] = []
        for task_row, raw_row in rows:
            scanned_count += 1
            if raw_row is None:
                continue
            if since_ts and parse_any_datetime(raw_row.published_at) < since_ts:
                continue
            payload = dict(raw_row.payload or {})
            summary_source = _normalize_summary_source(payload.get("summary_source") or "")
            if _is_llm_summary_source(summary_source):
                skipped_summary_count += 1
                continue
            candidate_rows.append((task_row, raw_row))
            candidate_ids.append(int(raw_row.id))

        event_raw_ids: set[int] = set()
        if candidate_ids:
            event_rows = (
                await session.execute(
                    select(NewsEvent.raw_news_id).where(
                        and_(
                            NewsEvent.raw_news_id.is_not(None),
                            NewsEvent.raw_news_id.in_(candidate_ids),
                        )
                    )
                )
            ).all()
            event_raw_ids = {int(row[0]) for row in event_rows if row and row[0] is not None}

        affected_ids: List[int] = []
        for task_row, raw_row in candidate_rows:
            raw_id = int(raw_row.id)
            if raw_id in event_raw_ids:
                skipped_event_count += 1
                continue
            task_row.status = "retry"
            task_row.next_retry_at = now
            task_row.started_at = None
            task_row.finished_at = None
            task_row.updated_at = now
            task_row.attempt_count = 0
            affected_ids.append(raw_id)
            if len(affected_ids) >= max_rows:
                break

        await session.flush()

    return {
        "scanned_count": scanned_count,
        "candidate_count": len(candidate_rows),
        "requeued_count": len(affected_ids),
        "raw_news_ids_sample": affected_ids[:20],
        "skipped_summary_repaired_count": skipped_summary_count,
        "skipped_existing_event_count": skipped_event_count,
        "since": _utc_iso(since_ts),
    }


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
        rows_to_insert: List[Dict[str, Any]] = []
        deduped_count = local_dup
        for item in normalized:
            title_bucket = _title_bucket_key(item["title"], item["published_at"])
            if item["url"] in existing_urls or item["content_hash"] in existing_hashes or (title_bucket and title_bucket in existing_title_buckets):
                deduped_count += 1
                continue
            rows_to_insert.append(item)
            if title_bucket:
                existing_title_buckets.add(title_bucket)
        if news_engine.dialect.name == "sqlite":
            if rows_to_insert:
                await session.execute(insert(NewsRaw).prefix_with("OR IGNORE"), rows_to_insert)
                await session.flush()
                inserted_hashes = [str(item["content_hash"]) for item in rows_to_insert]
                rows = (
                    await session.execute(
                        select(NewsRaw).where(NewsRaw.content_hash.in_(inserted_hashes)).order_by(NewsRaw.published_at.desc())
                    )
                ).scalars().all()
                inserted = [_row_to_news_dict(row) for row in rows]
                deduped_count = max(deduped_count, pulled_count - len(inserted))
            else:
                inserted = []
        else:
            for item in rows_to_insert:
                obj = NewsRaw(**item)
                session.add(obj)
                try:
                    await session.flush()
                    objects.append(obj)
                except IntegrityError:
                    await session.rollback()
                    deduped_count += 1
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
    """Claim LLM tasks for processing, respecting retry backoff."""
    max_rows = max(1, min(int(limit or 10), 100))
    now = datetime.now(timezone.utc)
    running_timeout_sec = max(120, int(os.getenv("NEWS_LLM_RUNNING_TIMEOUT_SEC") or 900))
    stale_before = now - timedelta(seconds=running_timeout_sec)
    async with news_session_scope() as session:
        # Reclaim tasks stuck in running state after worker crash/timeout.
        stale_running = (
            await session.execute(
                select(NewsLLMTask).where(
                    and_(
                        NewsLLMTask.status == "running",
                        NewsLLMTask.started_at.is_not(None),
                        NewsLLMTask.started_at <= stale_before,
                    )
                )
            )
        ).scalars().all()
        for row in stale_running:
            attempt = int(row.attempt_count or 0)
            row.updated_at = now
            row.started_at = None
            if attempt >= 6:
                row.status = "failed"
                row.finished_at = now
                row.last_error = f"llm task stale timeout after {running_timeout_sec}s"
            else:
                row.status = "retry"
                row.next_retry_at = now
                row.last_error = f"reclaimed stale running task after {running_timeout_sec}s"

        # Only claim tasks that are not in backoff period
        rows = (
            await session.execute(
                select(NewsLLMTask)
                .where(
                    and_(
                        NewsLLMTask.status.in_(["pending", "retry"]),
                        or_(
                            NewsLLMTask.next_retry_at.is_(None),
                            NewsLLMTask.next_retry_at <= now,
                        ),
                    )
                )
                .order_by(NewsLLMTask.priority.desc(), NewsLLMTask.created_at.asc())
                .limit(max_rows)
            )
        ).scalars().all()
        raw_ids: List[int] = []
        for row in rows:
            row.status = "running"
            row.attempt_count = int(row.attempt_count or 0) + 1
            row.started_at = now
            row.finished_at = None
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


async def finish_llm_tasks(
    raw_news_ids: List[int],
    *,
    success: bool,
    error: Optional[str] = None,
    error_type: str = "general",
    is_rate_limited: bool = False,
) -> None:
    """Mark LLM tasks as complete, with intelligent retry backoff.

    Args:
        raw_news_ids: List of raw news IDs to mark complete
        success: Whether processing succeeded
        error: Error message if failed
        error_type: Type of error ("general", "rate_limit", "timeout", etc.)
        is_rate_limited: Whether the error was due to rate limiting (429)
    """
    keys = [int(x) for x in raw_news_ids if x]
    if not keys:
        return
    now = datetime.now(timezone.utc)

    async with news_session_scope() as session:
        rows = (
            await session.execute(select(NewsLLMTask).where(NewsLLMTask.raw_news_id.in_(keys)))
        ).scalars().all()
        for row in rows:
            if success:
                row.status = "done"
                row.last_error = None
                row.finished_at = now
            else:
                attempt = int(row.attempt_count or 0)

                # Rate limited - set next_retry_at with exponential backoff
                if is_rate_limited or error_type == "rate_limit":
                    backoff_seconds = min(300, 30 * (2 ** (attempt - 1)))  # Max 5 min backoff
                    row.next_retry_at = now + timedelta(seconds=backoff_seconds)
                    row.last_rate_limit_at = now
                    row.status = "retry"
                    row.last_error = f"Rate limited, retry after {backoff_seconds}s: {error or '429'}"
                # Timeout errors get more attempts (network issues are transient)
                elif error_type == "timeout" and attempt >= 5:
                    row.status = "failed"
                    row.last_error = str(error or "llm extraction failed")
                    row.finished_at = now
                # Max attempts reached - mark as failed
                elif error_type != "timeout" and attempt >= 3:
                    row.status = "failed"
                    row.last_error = str(error or "llm extraction failed")
                    row.finished_at = now
                # Other errors - retry with backoff (timeout uses longer backoff)
                else:
                    if error_type == "timeout":
                        backoff_seconds = min(120, 30 * (2 ** (attempt - 1)))  # timeout: 30s/60s/120s
                    else:
                        backoff_seconds = min(60, 10 * (2 ** (attempt - 1)))   # other: 10s/20s/40s
                    row.next_retry_at = now + timedelta(seconds=backoff_seconds)
                    row.status = "retry"
                    row.last_error = str(error or "llm extraction failed")

            row.updated_at = now
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
            select(NewsEvent.symbol, NewsEvent.event_type, NewsEvent.sentiment, NewsEvent.ts, NewsEvent.evidence, NewsEvent.impact_score).where(
                and_(NewsEvent.ts >= min_ts, NewsEvent.ts <= max_ts)
            )
        )
        existing_semantic: Dict[str, Dict[str, Any]] = {}
        for row in existing_recent_rows.all():
            key = _event_semantic_key(row[0], row[1], row[2], row[3], row[4] if isinstance(row[4], dict) else {})
            existing_semantic[key] = {"sentiment": int(row[2]), "impact_score": float(row[5] or 0)}

        objects: List[NewsEvent] = []
        for item in validated:
            semantic_key = item.pop("_semantic_key", "")
            if item["event_id"] in existing:
                deduped_count += 1
                continue
            if semantic_key and semantic_key in existing_semantic:
                prev = existing_semantic[semantic_key]
                new_impact = float(item.get("impact_score") or 0)
                prev_impact = prev["impact_score"]
                new_sentiment = int(item.get("sentiment") or 0)
                prev_sentiment = prev["sentiment"]
                impact_diff = abs(new_impact - prev_impact) / max(abs(prev_impact), 0.01)
                if impact_diff <= 0.05 and new_sentiment == prev_sentiment:
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
                existing_semantic[semantic_key] = {"sentiment": int(item.get("sentiment") or 0), "impact_score": float(item.get("impact_score") or 0)}

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


async def set_global_backoff(backoff_until: datetime) -> None:
    """Set a global rate limit backoff that affects all LLM processing.

    Args:
        backoff_until: UTC datetime until which all LLM tasks should be paused
    """
    global _global_rate_limit_backoff
    async with _global_rate_limit_lock:
        _global_rate_limit_backoff = parse_any_datetime(backoff_until)


async def get_global_backoff() -> Optional[datetime]:
    """Get the current global rate limit backoff time.

    Returns:
        UTC datetime until which LLM processing should be paused, or None
    """
    global _global_rate_limit_backoff
    async with _global_rate_limit_lock:
        if _global_rate_limit_backoff is None:
            return None
        # Clear expired backoff
        if _global_rate_limit_backoff <= datetime.now(timezone.utc):
            _global_rate_limit_backoff = None
            return None
        return _global_rate_limit_backoff


async def clear_global_backoff() -> None:
    """Clear the global rate limit backoff."""
    global _global_rate_limit_backoff
    async with _global_rate_limit_lock:
        _global_rate_limit_backoff = None


async def is_in_global_backoff() -> bool:
    """Check if we are currently in a global rate limit backoff period."""
    backoff = await get_global_backoff()
    return backoff is not None


async def set_provider_backoff(provider: str, until: datetime) -> None:
    """Set a per-provider rate limit backoff."""
    async with _provider_rate_limit_lock:
        _provider_rate_limit_backoff[provider] = parse_any_datetime(until)


async def get_provider_backoff(provider: str) -> Optional[datetime]:
    """Get the current per-provider rate limit backoff time."""
    async with _provider_rate_limit_lock:
        backoff = _provider_rate_limit_backoff.get(provider)
        if backoff is None:
            return None
        if backoff <= datetime.now(timezone.utc):
            _provider_rate_limit_backoff.pop(provider, None)
            return None
        return backoff


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
