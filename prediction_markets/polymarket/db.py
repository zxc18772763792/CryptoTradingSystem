from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import and_, event, insert, select, text, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config.settings import settings
from prediction_markets.polymarket.models import PMAlert, PMBase, PMMarket, PMQuote, PMSourceState, PMSubscription
from prediction_markets.polymarket.utils import parse_ts_any, utc_now

_engine_kwargs: Dict[str, Any] = {"echo": False, "future": True}
if str(settings.DATABASE_URL).startswith("sqlite"):
    _engine_kwargs["connect_args"] = {"timeout": 30}

pm_engine = create_async_engine(settings.DATABASE_URL, **_engine_kwargs)
PMSessionLocal = async_sessionmaker(pm_engine, class_=AsyncSession, expire_on_commit=False)

if str(settings.DATABASE_URL).startswith("sqlite"):
    @event.listens_for(pm_engine.sync_engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
        finally:
            cursor.close()


def _utc_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    dt = parse_ts_any(value)
    return dt.isoformat()


def _market_to_dict(row: PMMarket) -> Dict[str, Any]:
    return {
        "market_id": row.market_id,
        "event_id": row.event_id,
        "slug": row.slug,
        "question": row.question,
        "description": row.description,
        "category": row.category,
        "tags": row.tags_json or {},
        "outcomes": row.outcomes_json or {},
        "token_ids": row.token_ids_json or {},
        "end_time": _utc_iso(row.end_time),
        "active": bool(row.active),
        "closed": bool(row.closed),
        "resolved": bool(row.resolved),
        "resolution": row.resolution_json or {},
        "liquidity": float(row.liquidity or 0.0),
        "volume_24h": float(row.volume_24h or 0.0),
        "relevance_score": float(row.relevance_score or 0.0),
        "updated_at": _utc_iso(row.updated_at),
        "payload": row.payload_json or {},
    }


def _quote_to_dict(row: PMQuote) -> Dict[str, Any]:
    return {
        "id": row.id,
        "ts": _utc_iso(row.ts),
        "market_id": row.market_id,
        "token_id": row.token_id,
        "outcome": row.outcome,
        "price": float(row.price or 0.0),
        "bid": None if row.bid is None else float(row.bid),
        "ask": None if row.ask is None else float(row.ask),
        "midpoint": None if row.midpoint is None else float(row.midpoint),
        "spread": None if row.spread is None else float(row.spread),
        "depth1": None if row.depth1 is None else float(row.depth1),
        "depth5": None if row.depth5 is None else float(row.depth5),
        "fetched_at": _utc_iso(row.fetched_at),
        "payload": row.payload_json or {},
    }


def _sub_to_dict(row: PMSubscription) -> Dict[str, Any]:
    return {
        "id": row.id,
        "category": row.category,
        "market_id": row.market_id,
        "token_id": row.token_id,
        "outcome": row.outcome,
        "relevance_score": float(row.relevance_score or 0.0),
        "symbol_weights": row.symbol_weights_json or {},
        "min_liquidity_snapshot": float(row.min_liquidity_snapshot or 0.0),
        "max_spread_snapshot": float(row.max_spread_snapshot or 0.0),
        "enabled": bool(row.enabled),
        "updated_at": _utc_iso(row.updated_at),
    }


def _alert_to_dict(row: PMAlert) -> Dict[str, Any]:
    return {
        "id": row.id,
        "ts": _utc_iso(row.ts),
        "category": row.category,
        "market_id": row.market_id,
        "token_id": row.token_id,
        "metric": row.metric,
        "value": float(row.value or 0.0),
        "severity": row.severity,
        "payload": row.payload_json or {},
    }


def _state_to_dict(row: PMSourceState) -> Dict[str, Any]:
    return {
        "source": row.source,
        "cursor_type": row.cursor_type,
        "cursor_value": row.cursor_value,
        "last_ts": _utc_iso(row.last_ts),
        "last_error": row.last_error,
        "error_count": int(row.error_count or 0),
        "success_count": int(row.success_count or 0),
        "paused_until": _utc_iso(row.paused_until),
        "updated_at": _utc_iso(row.updated_at),
    }


@asynccontextmanager
async def pm_session_scope() -> Iterable[AsyncSession]:
    session = PMSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def init_pm_db() -> None:
    try:
        async with pm_engine.begin() as conn:
            await conn.run_sync(PMBase.metadata.create_all)
    except OperationalError as exc:
        message = str(exc).lower()
        if "already exists" not in message:
            raise


async def close_pm_db() -> None:
    await pm_engine.dispose()


async def upsert_markets(markets: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not markets:
        return {"upserted": 0, "items": []}
    ids = [str(item.get("market_id") or "") for item in markets if str(item.get("market_id") or "").strip()]
    now = utc_now()
    async with pm_session_scope() as session:
        existing_rows = (
            await session.execute(select(PMMarket).where(PMMarket.market_id.in_(ids)))
        ).scalars().all()
        existing = {row.market_id: row for row in existing_rows}
        touched: List[PMMarket] = []
        for item in markets:
            market_id = str(item.get("market_id") or "").strip()
            if not market_id:
                continue
            row = existing.get(market_id)
            payload = {
                "event_id": item.get("event_id"),
                "slug": item.get("slug"),
                "question": str(item.get("question") or ""),
                "description": item.get("description"),
                "category": str(item.get("category") or "other"),
                "tags_json": item.get("tags") or item.get("tags_json") or {},
                "outcomes_json": item.get("outcomes") or item.get("outcomes_json") or {},
                "token_ids_json": item.get("token_ids") or item.get("token_ids_json") or {},
                "end_time": parse_ts_any(item.get("end_time")) if item.get("end_time") else None,
                "active": bool(item.get("active", True)),
                "closed": bool(item.get("closed", False)),
                "resolved": bool(item.get("resolved", False)),
                "resolution_json": item.get("resolution") or item.get("resolution_json") or {},
                "liquidity": float(item.get("liquidity") or 0.0),
                "volume_24h": float(item.get("volume_24h") or item.get("volume24hr") or 0.0),
                "relevance_score": float(item.get("relevance_score") or 0.0),
                "updated_at": parse_ts_any(item.get("updated_at")) if item.get("updated_at") else now,
                "payload_json": item.get("payload") or item.get("payload_json") or {},
            }
            if row is None:
                row = PMMarket(market_id=market_id, **payload)
                session.add(row)
                existing[market_id] = row
            else:
                for key, value in payload.items():
                    setattr(row, key, value)
            touched.append(row)
        await session.flush()
        return {"upserted": len(touched), "items": [_market_to_dict(row) for row in touched]}


async def set_subscriptions(category: str, subscriptions: List[Dict[str, Any]]) -> Dict[str, Any]:
    cat = str(category or "").strip().upper()
    now = utc_now()
    async with pm_session_scope() as session:
        existing_rows = (
            await session.execute(select(PMSubscription).where(PMSubscription.category == cat))
        ).scalars().all()
        existing = {(row.market_id, row.token_id, row.outcome): row for row in existing_rows}
        keep_keys = set()
        touched: List[PMSubscription] = []
        for item in subscriptions:
            key = (str(item.get("market_id") or ""), str(item.get("token_id") or ""), str(item.get("outcome") or "YES").upper())
            if not all(key):
                continue
            keep_keys.add(key)
            row = existing.get(key)
            payload = {
                "category": cat,
                "market_id": key[0],
                "token_id": key[1],
                "outcome": key[2],
                "relevance_score": float(item.get("relevance_score") or 0.0),
                "symbol_weights_json": item.get("symbol_weights") or {},
                "min_liquidity_snapshot": float(item.get("min_liquidity_snapshot") or item.get("min_liquidity") or 0.0),
                "max_spread_snapshot": float(item.get("max_spread_snapshot") or item.get("max_spread") or 0.0),
                "enabled": bool(item.get("enabled", True)),
                "updated_at": now,
            }
            if row is None:
                row = PMSubscription(**payload)
                session.add(row)
                existing[key] = row
            else:
                for field, value in payload.items():
                    setattr(row, field, value)
            touched.append(row)
        for key, row in existing.items():
            if key not in keep_keys:
                row.enabled = False
                row.updated_at = now
        await session.flush()
        return {"updated": len(touched), "items": [_sub_to_dict(row) for row in touched]}


async def list_active_subscriptions(category: Optional[str] = None) -> List[Dict[str, Any]]:
    async with pm_session_scope() as session:
        stmt = select(PMSubscription).where(PMSubscription.enabled.is_(True)).order_by(PMSubscription.relevance_score.desc())
        if category:
            stmt = stmt.where(PMSubscription.category == str(category).upper())
        rows = (await session.execute(stmt)).scalars().all()
        return [_sub_to_dict(row) for row in rows]


async def get_markets_map(market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    ids = [str(x) for x in (market_ids or []) if str(x).strip()]
    if not ids:
        return {}
    async with pm_session_scope() as session:
        rows = (await session.execute(select(PMMarket).where(PMMarket.market_id.in_(ids)))).scalars().all()
        return {row.market_id: _market_to_dict(row) for row in rows}


async def disable_subscriptions(market_ids: List[str]) -> Dict[str, Any]:
    ids = [str(x).strip() for x in (market_ids or []) if str(x).strip()]
    if not ids:
        return {"updated": 0}
    now = utc_now()
    async with pm_session_scope() as session:
        rows = (
            await session.execute(select(PMSubscription).where(PMSubscription.market_id.in_(ids)))
        ).scalars().all()
        for row in rows:
            row.enabled = False
            row.updated_at = now
        await session.flush()
    return {"updated": len(rows)}


async def insert_quotes(quotes: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not quotes:
        return {"inserted": 0}
    rows = []
    for item in quotes:
        market_id = str(item.get("market_id") or "").strip()
        token_id = str(item.get("token_id") or "").strip()
        if not market_id or not token_id:
            continue
        rows.append(
            {
                "ts": parse_ts_any(item.get("ts") or utc_now()),
                "market_id": market_id,
                "token_id": token_id,
                "outcome": str(item.get("outcome") or "YES").upper(),
                "price": float(item.get("price") or 0.0),
                "bid": None if item.get("bid") is None else float(item.get("bid")),
                "ask": None if item.get("ask") is None else float(item.get("ask")),
                "midpoint": None if item.get("midpoint") is None else float(item.get("midpoint")),
                "spread": None if item.get("spread") is None else float(item.get("spread")),
                "depth1": None if item.get("depth1") is None else float(item.get("depth1")),
                "depth5": None if item.get("depth5") is None else float(item.get("depth5")),
                "fetched_at": parse_ts_any(item.get("fetched_at") or utc_now()),
                "payload_json": item.get("payload") or item.get("payload_json") or {},
            }
        )
    if not rows:
        return {"inserted": 0}
    async with pm_session_scope() as session:
        await session.execute(insert(PMQuote), rows)
    return {"inserted": len(rows)}


async def insert_alerts(alerts: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not alerts:
        return {"inserted": 0}
    rows = []
    for item in alerts:
        rows.append(
            {
                "ts": parse_ts_any(item.get("ts") or utc_now()),
                "category": str(item.get("category") or "OTHER").upper(),
                "market_id": str(item.get("market_id") or ""),
                "token_id": item.get("token_id"),
                "metric": str(item.get("metric") or "shock"),
                "value": float(item.get("value") or 0.0),
                "severity": str(item.get("severity") or "info"),
                "payload_json": item.get("payload") or item.get("payload_json") or {},
            }
        )
    async with pm_session_scope() as session:
        await session.execute(insert(PMAlert), rows)
    return {"inserted": len(rows)}


async def get_market_quotes(market_id: str, since: datetime, until: datetime) -> List[Dict[str, Any]]:
    async with pm_session_scope() as session:
        rows = (
            await session.execute(
                select(PMQuote)
                .where(and_(PMQuote.market_id == str(market_id), PMQuote.ts >= parse_ts_any(since), PMQuote.ts <= parse_ts_any(until)))
                .order_by(PMQuote.ts.asc())
            )
        ).scalars().all()
    return [_quote_to_dict(row) for row in rows]


async def get_quotes_for_subscriptions(since: datetime, until: datetime, category: Optional[str] = None) -> List[Dict[str, Any]]:
    async with pm_session_scope() as session:
        stmt = (
            select(PMQuote, PMSubscription.category, PMSubscription.relevance_score, PMSubscription.symbol_weights_json)
            .join(PMSubscription, and_(PMQuote.market_id == PMSubscription.market_id, PMQuote.token_id == PMSubscription.token_id))
            .where(and_(PMSubscription.enabled.is_(True), PMQuote.ts >= parse_ts_any(since), PMQuote.ts <= parse_ts_any(until)))
            .order_by(PMQuote.ts.asc())
        )
        if category:
            stmt = stmt.where(PMSubscription.category == str(category).upper())
        rows = (await session.execute(stmt)).all()
    out = []
    for quote, cat, relevance, symbol_weights in rows:
        item = _quote_to_dict(quote)
        item["category"] = cat
        item["relevance_score"] = float(relevance or 0.0)
        item["symbol_weights"] = symbol_weights or {}
        out.append(item)
    return out


async def list_alerts(since: Optional[datetime] = None, category: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    async with pm_session_scope() as session:
        stmt = select(PMAlert).order_by(PMAlert.ts.desc()).limit(max(1, min(int(limit or 200), 2000)))
        if since:
            stmt = stmt.where(PMAlert.ts >= parse_ts_any(since))
        if category:
            stmt = stmt.where(PMAlert.category == str(category).upper())
        rows = (await session.execute(stmt)).scalars().all()
    return [_alert_to_dict(row) for row in rows]


async def set_source_state(
    source: str,
    *,
    cursor_type: str = "ts",
    cursor_value: Optional[str] = None,
    last_ts: Optional[datetime] = None,
    last_error: Optional[str] = None,
    mark_success: bool = False,
    mark_failure: bool = False,
    paused_until: Optional[datetime] = None,
) -> Dict[str, Any]:
    source_name = str(source or "").strip().lower()
    now = utc_now()
    async with pm_session_scope() as session:
        row = (await session.execute(select(PMSourceState).where(PMSourceState.source == source_name))).scalar_one_or_none()
        if row is None:
            row = PMSourceState(source=source_name, cursor_type=cursor_type or "ts", updated_at=now)
            session.add(row)
        row.cursor_type = cursor_type or row.cursor_type
        if cursor_value is not None:
            row.cursor_value = str(cursor_value)
        if last_ts is not None:
            row.last_ts = parse_ts_any(last_ts)
        if last_error is not None:
            row.last_error = str(last_error)
        if mark_success:
            row.success_count = int(row.success_count or 0) + 1
            row.error_count = 0
            row.last_error = None
        if mark_failure:
            row.error_count = int(row.error_count or 0) + 1
        row.paused_until = parse_ts_any(paused_until) if paused_until else row.paused_until
        row.updated_at = now
        await session.flush()
        return _state_to_dict(row)


async def get_source_state(source: str) -> Optional[Dict[str, Any]]:
    async with pm_session_scope() as session:
        row = (await session.execute(select(PMSourceState).where(PMSourceState.source == str(source).lower()))).scalar_one_or_none()
    return _state_to_dict(row) if row else None


async def list_source_states() -> List[Dict[str, Any]]:
    async with pm_session_scope() as session:
        rows = (await session.execute(select(PMSourceState).order_by(PMSourceState.source.asc()))).scalars().all()
    return [_state_to_dict(row) for row in rows]


async def compute_and_store_alerts(ts_from: datetime, ts_to: datetime) -> Dict[str, Any]:
    from prediction_markets.polymarket.features import build_feature_frame

    quotes = await get_quotes_for_subscriptions(ts_from, ts_to)
    if not quotes:
        return {"inserted": 0, "items": []}
    frame = build_feature_frame(quotes, timeframe="1m")
    if frame.empty:
        return {"inserted": 0, "items": []}
    alerts: List[Dict[str, Any]] = []
    for _, row in frame.iterrows():
        sev = float(row.get("cat_shock_sev") or 0.0)
        if sev < 1.0:
            continue
        alerts.append(
            {
                "ts": row["ts"],
                "category": row["category"],
                "market_id": row.get("top_market_id") or "aggregate",
                "token_id": row.get("top_token_id"),
                "metric": "cat_shock_sev",
                "value": sev,
                "severity": "high" if sev >= 2.0 else "medium",
                "payload": {
                    "cat_prob": float(row.get("cat_prob") or 0.0),
                    "cat_dprob_5m": float(row.get("cat_dprob_5m") or 0.0),
                    "cat_liquidity": float(row.get("cat_liquidity") or 0.0),
                },
            }
        )
    stats = await insert_alerts(alerts)
    stats["items"] = alerts
    return stats


async def get_features_asof(symbol: str, ts: datetime, timeframe: str = "1m") -> Dict[str, Any]:
    from prediction_markets.polymarket.features import get_features_asof_from_quotes

    since = parse_ts_any(ts) - timedelta(hours=48)
    quotes = await get_quotes_for_subscriptions(since, parse_ts_any(ts))
    return get_features_asof_from_quotes(quotes, symbol=symbol, ts=parse_ts_any(ts), timeframe=timeframe)


async def get_features_range(symbol: str, since: datetime, until: datetime, timeframe: str = "1m") -> List[Dict[str, Any]]:
    from prediction_markets.polymarket.features import get_features_range_from_quotes

    quotes = await get_quotes_for_subscriptions(parse_ts_any(since) - timedelta(hours=1), parse_ts_any(until))
    return get_features_range_from_quotes(quotes, symbol=symbol, since=parse_ts_any(since), until=parse_ts_any(until), timeframe=timeframe)


async def get_pm_status() -> Dict[str, Any]:
    one_minute_ago = utc_now() - timedelta(minutes=1)
    one_hour_ago = utc_now() - timedelta(hours=1)
    async with pm_session_scope() as session:
        markets_count = len((await session.execute(select(PMMarket.market_id))).all())
        subscriptions_count = len((await session.execute(select(PMSubscription.id).where(PMSubscription.enabled.is_(True)))).all())
        alerts_last_hour = len((await session.execute(select(PMAlert.id).where(PMAlert.ts >= one_hour_ago))).all())
        quotes_last_minute = len((await session.execute(select(PMQuote.id).where(PMQuote.ts >= one_minute_ago))).all())
    return {
        "markets_count": markets_count,
        "subscriptions_count": subscriptions_count,
        "alerts_last_hour": alerts_last_hour,
        "quotes_last_minute": quotes_last_minute,
        "source_states": await list_source_states(),
    }


async def cleanup_old_quotes(retention_days: int = 14) -> Dict[str, Any]:
    cutoff = utc_now() - timedelta(days=max(1, int(retention_days or 14)))
    async with pm_session_scope() as session:
        result = await session.execute(text("DELETE FROM pm_quotes WHERE ts < :cutoff"), {"cutoff": cutoff})
        deleted = int(getattr(result, "rowcount", 0) or 0)
    return {"deleted": deleted, "cutoff": cutoff.isoformat()}
