"""Read model for Binance Alpha data collected by the background worker.

The collector owns upstream requests and writes.  Dashboard, replay, and
research consumers use this module so they do not need to know the collector's
SQLite schema or accidentally route synthetic Alpha symbols to spot exchanges.
"""
from __future__ import annotations

import copy
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from core.data.binance_alpha import alpha_pair_from_id
from core.data.binance_alpha_collector import collector_database_path, load_collector_status


ALPHA_DATA_SOURCE = "binance_alpha"
ALPHA_DATA_SOURCE_ALIASES = {ALPHA_DATA_SOURCE, "alpha"}
_TIMEFRAME_ORDER = {
    "1m": 0,
    "3m": 1,
    "5m": 2,
    "15m": 3,
    "30m": 4,
    "1h": 5,
    "2h": 6,
    "4h": 7,
    "6h": 8,
    "8h": 9,
    "12h": 10,
    "1d": 11,
    "3d": 12,
    "1w": 13,
    "1M": 14,
}
_READ_CACHE: Dict[str, Dict[str, Any]] = {}
_READ_CACHE_LOCK = threading.RLock()


def normalize_data_source(value: Any) -> str:
    text = str(value or "").strip().lower()
    return ALPHA_DATA_SOURCE if text in ALPHA_DATA_SOURCE_ALIASES else text


def is_alpha_data_source(value: Any) -> bool:
    return normalize_data_source(value) == ALPHA_DATA_SOURCE


def is_alpha_symbol(value: Any) -> bool:
    """Return whether a value is one of our synthetic Alpha-ID pairs."""
    return bool(re.fullmatch(r"ALPHA\d+", _lookup_key(value)))


def _database_path(path: Optional[Path] = None) -> Path:
    return Path(path) if path is not None else collector_database_path()


def _database_generation(path: Path) -> tuple[int, int, int, int]:
    try:
        database_stat = path.stat() if path.exists() else None
    except OSError:
        database_stat = None
    wal_path = Path(f"{path}-wal")
    try:
        wal_stat = wal_path.stat() if wal_path.exists() else None
    except OSError:
        wal_stat = None
    return (
        int(database_stat.st_mtime_ns) if database_stat else 0,
        int(database_stat.st_size) if database_stat else 0,
        int(wal_stat.st_mtime_ns) if wal_stat else 0,
        int(wal_stat.st_size) if wal_stat else 0,
    )


def _cached_read(key: str, path: Path) -> Any:
    generation = _database_generation(path)
    with _READ_CACHE_LOCK:
        entry = _READ_CACHE.get(key)
        if entry and entry.get("generation") == generation:
            return copy.deepcopy(entry.get("payload"))
    return None


def _store_cached_read(key: str, path: Path, payload: Any) -> Any:
    with _READ_CACHE_LOCK:
        _READ_CACHE[key] = {
            "generation": _database_generation(path),
            "payload": copy.deepcopy(payload),
        }
    return payload


def _connect_readonly(path: Path) -> sqlite3.Connection:
    resolved = path.resolve().as_posix()
    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True, timeout=10.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _lookup_key(value: Any) -> str:
    text = "".join(ch for ch in str(value or "").upper() if ch.isalnum())
    for _ in range(2):
        for quote in ("USDT", "USDC", "USD"):
            if text.endswith(quote):
                text = text[: -len(quote)]
                break
        else:
            break
    return text


def _to_epoch_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        timestamp = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return int(timestamp.timestamp() * 1000)


def _resolve_alpha_id(connection: sqlite3.Connection, symbol: str) -> Optional[str]:
    wanted = _lookup_key(symbol)
    if not wanted:
        return None
    synthetic_match = re.fullmatch(r"ALPHA(\d+)", wanted)
    if synthetic_match:
        candidate = f"ALPHA_{synthetic_match.group(1)}"
        for table in ("alpha_klines", "market_snapshots"):
            row = connection.execute(
                f"SELECT alpha_id FROM {table} WHERE alpha_id = ? LIMIT 1",
                (candidate,),
            ).fetchone()
            if row:
                return str(row["alpha_id"])
    rows = connection.execute(
        "SELECT alpha_id, MAX(official_symbol) AS official_symbol "
        "FROM alpha_klines GROUP BY alpha_id"
    ).fetchall()
    for row in rows:
        if wanted in {_lookup_key(row["alpha_id"]), _lookup_key(row["official_symbol"])}:
            return str(row["alpha_id"])
    return None


def load_alpha_klines(
    *,
    symbol: str,
    timeframe: str,
    start_time: Any = None,
    end_time: Any = None,
    limit: Optional[int] = None,
    align: str = "tail",
    path: Optional[Path] = None,
) -> pd.DataFrame:
    """Load collected Alpha candles as a UTC-naive OHLCV frame."""
    database = _database_path(path)
    if not database.exists():
        return pd.DataFrame()

    try:
        connection = _connect_readonly(database)
    except sqlite3.Error:
        return pd.DataFrame()
    try:
        alpha_id = _resolve_alpha_id(connection, symbol)
        if not alpha_id:
            return pd.DataFrame()
        clauses = ["alpha_id = ?", "timeframe = ?"]
        params: List[Any] = [alpha_id, str(timeframe or "1m").strip()]
        start_ms = _to_epoch_ms(start_time)
        end_ms = _to_epoch_ms(end_time)
        if start_ms is not None:
            clauses.append("open_time_ms >= ?")
            params.append(start_ms)
        if end_ms is not None:
            clauses.append("open_time_ms <= ?")
            params.append(end_ms)

        align_mode = str(align or "tail").strip().lower()
        order = "ASC" if align_mode == "head" else "DESC"
        sql = (
            "SELECT open_time_ms, open, high, low, close, volume, quote_volume, "
            "trade_count, is_closed, captured_at FROM alpha_klines WHERE "
            + " AND ".join(clauses)
            + f" ORDER BY open_time_ms {order}"
        )
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(1, min(int(limit), 100_000)))
        rows = connection.execute(sql, params).fetchall()
    except sqlite3.Error:
        return pd.DataFrame()
    finally:
        connection.close()

    if not rows:
        return pd.DataFrame()
    records = [dict(row) for row in rows]
    if align_mode != "head":
        records.reverse()
    frame = pd.DataFrame.from_records(records)
    frame["timestamp"] = pd.to_datetime(frame.pop("open_time_ms"), unit="ms", utc=True).dt.tz_localize(None)
    frame = frame.set_index("timestamp").sort_index()
    return frame[~frame.index.duplicated(keep="last")]


def list_alpha_datasets(*, path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Return one coverage row per collected Alpha symbol and timeframe."""
    database = _database_path(path)
    if not database.exists():
        return []
    cache_key = f"datasets:{database.resolve()}"
    cached = _cached_read(cache_key, database)
    if cached is not None:
        return cached
    try:
        connection = _connect_readonly(database)
    except sqlite3.Error:
        return []
    try:
        coverage = connection.execute(
            """SELECT alpha_id, MAX(official_symbol) AS official_symbol, timeframe,
                      COUNT(*) AS rows, MIN(open_time_ms) AS start_ms,
                      MAX(open_time_ms) AS end_ms, MAX(captured_at) AS captured_at
               FROM alpha_klines
               GROUP BY alpha_id, timeframe"""
        ).fetchall()
        latest_meta = connection.execute(
            """SELECT snapshot.alpha_id, snapshot.display_symbol, snapshot.name,
                      snapshot.chain_name, snapshot.volume_24h, snapshot.liquidity,
                      snapshot.price, snapshot.price_change_24h, snapshot.hot_tag
               FROM market_snapshots AS snapshot
               INNER JOIN (
                   SELECT alpha_id, MAX(captured_at) AS captured_at
                   FROM market_snapshots GROUP BY alpha_id
               ) AS latest
               ON latest.alpha_id = snapshot.alpha_id
              AND latest.captured_at = snapshot.captured_at"""
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        connection.close()

    metadata = {str(row["alpha_id"]): dict(row) for row in latest_meta}
    datasets: List[Dict[str, Any]] = []
    for row in coverage:
        alpha_id = str(row["alpha_id"])
        meta = metadata.get(alpha_id, {})
        official_symbol = str(row["official_symbol"] or "")
        quote_asset = next(
            (quote for quote in ("USDT", "USDC", "USD") if official_symbol.endswith(quote)),
            "",
        )
        datasets.append(
            {
                "exchange": ALPHA_DATA_SOURCE,
                "source": ALPHA_DATA_SOURCE,
                "source_type": "collector_sqlite",
                "symbol": alpha_pair_from_id(alpha_id),
                "alpha_id": alpha_id,
                "official_symbol": official_symbol,
                "quote_asset": quote_asset,
                "timeframe": str(row["timeframe"] or ""),
                "rows": int(row["rows"] or 0),
                "start": pd.Timestamp(int(row["start_ms"]), unit="ms", tz="UTC").isoformat(),
                "end": pd.Timestamp(int(row["end_ms"]), unit="ms", tz="UTC").isoformat(),
                "modified_at": str(row["captured_at"] or ""),
                "display_symbol": str(meta.get("display_symbol") or ""),
                "name": str(meta.get("name") or ""),
                "chain_name": str(meta.get("chain_name") or ""),
                "volume_24h": float(meta.get("volume_24h") or 0.0),
                "liquidity": float(meta.get("liquidity") or 0.0),
                "price": float(meta.get("price") or 0.0),
                "price_change_24h": float(meta.get("price_change_24h") or 0.0),
                "hot_tag": bool(meta.get("hot_tag")),
            }
        )
    return _store_cached_read(cache_key, database, datasets)


def _timeframe_coverage_summary(*, path: Optional[Path] = None) -> List[Dict[str, Any]]:
    database = _database_path(path)
    if not database.exists():
        return []
    cache_key = f"timeframes:{database.resolve()}"
    cached = _cached_read(cache_key, database)
    if cached is not None:
        return cached
    try:
        connection = _connect_readonly(database)
    except sqlite3.Error:
        return []
    try:
        rows = connection.execute(
            """SELECT timeframe, COUNT(*) AS rows,
                      COUNT(DISTINCT alpha_id) AS symbols,
                      MIN(open_time_ms) AS start_ms,
                      MAX(open_time_ms) AS end_ms,
                      MAX(captured_at) AS captured_at
               FROM alpha_klines GROUP BY timeframe"""
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        connection.close()
    payload = [
        {
            "timeframe": str(row["timeframe"] or ""),
            "rows": int(row["rows"] or 0),
            "symbol_count": int(row["symbols"] or 0),
            "start": pd.Timestamp(int(row["start_ms"]), unit="ms", tz="UTC").isoformat(),
            "end": pd.Timestamp(int(row["end_ms"]), unit="ms", tz="UTC").isoformat(),
            "modified_at": str(row["captured_at"] or ""),
        }
        for row in rows
    ]
    return _store_cached_read(cache_key, database, payload)


def list_alpha_symbols(*, path: Optional[Path] = None) -> Dict[str, Any]:
    datasets = list_alpha_datasets(path=path)
    symbol_meta: Dict[str, Dict[str, Any]] = {}
    for row in datasets:
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        item = symbol_meta.setdefault(
            symbol,
            {
                "display_symbol": row.get("display_symbol") or "",
                "name": row.get("name") or "",
                "alpha_id": row.get("alpha_id") or "",
                "official_symbol": row.get("official_symbol") or "",
                "quote_asset": row.get("quote_asset") or "",
                "chain_name": row.get("chain_name") or "",
                "volume_24h": float(row.get("volume_24h") or 0.0),
                "liquidity": float(row.get("liquidity") or 0.0),
                "price": float(row.get("price") or 0.0),
                "price_change_24h": float(row.get("price_change_24h") or 0.0),
                "hot_tag": bool(row.get("hot_tag")),
                "timeframes": [],
                "rows": 0,
                "start": row.get("start"),
                "end": row.get("end"),
            },
        )
        item["timeframes"].append(str(row.get("timeframe") or ""))
        item["rows"] += int(row.get("rows") or 0)
        if str(row.get("start") or "") < str(item.get("start") or row.get("start") or ""):
            item["start"] = row.get("start")
        if str(row.get("end") or "") > str(item.get("end") or ""):
            item["end"] = row.get("end")

    for item in symbol_meta.values():
        item["timeframes"] = sorted(
            set(item["timeframes"]),
            key=lambda value: (_TIMEFRAME_ORDER.get(value, 999), value),
        )
    symbols = sorted(
        symbol_meta,
        key=lambda value: (
            -float(symbol_meta[value].get("volume_24h") or 0.0),
            str(symbol_meta[value].get("display_symbol") or value),
        ),
    )
    timeframes = sorted(
        {str(row.get("timeframe") or "") for row in datasets if row.get("timeframe")},
        key=lambda value: (_TIMEFRAME_ORDER.get(value, 999), value),
    )
    return {
        "exchange": ALPHA_DATA_SOURCE,
        "source": ALPHA_DATA_SOURCE,
        "source_type": "managed_collector",
        "symbols": symbols,
        "symbol_meta": symbol_meta,
        "timeframes": timeframes,
        "count": len(symbols),
        "local_count": len(symbols),
        "dataset_count": len(datasets),
    }


def load_alpha_ticker(*, symbol: str, path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    database = _database_path(path)
    if not database.exists():
        return None
    try:
        connection = _connect_readonly(database)
    except sqlite3.Error:
        return None
    try:
        alpha_id = _resolve_alpha_id(connection, symbol)
        if not alpha_id:
            return None
        row = connection.execute(
            """SELECT captured_at, alpha_id, official_symbol, display_symbol,
                      name, chain_name, price, price_change_24h, volume_24h,
                      liquidity
               FROM market_snapshots
               WHERE alpha_id = ?
               ORDER BY captured_at DESC LIMIT 1""",
            (alpha_id,),
        ).fetchone()
    except sqlite3.Error:
        return None
    finally:
        connection.close()
    if not row:
        return None
    official_symbol = str(row["official_symbol"] or "")
    quote_asset = next(
        (quote for quote in ("USDT", "USDC", "USD") if official_symbol.endswith(quote)),
        "",
    )
    return {
        "exchange": ALPHA_DATA_SOURCE,
        "symbol": alpha_pair_from_id(row["alpha_id"]),
        "alpha_id": str(row["alpha_id"] or ""),
        "official_symbol": official_symbol,
        "quote_asset": quote_asset,
        "display_symbol": str(row["display_symbol"] or ""),
        "name": str(row["name"] or ""),
        "chain_name": str(row["chain_name"] or ""),
        "last": float(row["price"] or 0.0),
        "bid": 0.0,
        "ask": 0.0,
        "high_24h": 0.0,
        "low_24h": 0.0,
        "volume_24h": float(row["volume_24h"] or 0.0),
        "change_24h": float(row["price_change_24h"] or 0.0) / 100.0,
        "change_24h_pct": float(row["price_change_24h"] or 0.0),
        "liquidity": float(row["liquidity"] or 0.0),
        "timestamp": str(row["captured_at"] or ""),
        "source": "binance_alpha_collector",
        "source_type": "collector_sqlite",
        "managed": True,
    }


def get_alpha_coverage(
    *,
    symbol: str,
    timeframe: str,
    path: Optional[Path] = None,
) -> Dict[str, Any]:
    wanted = _lookup_key(symbol)
    match = next(
        (
            row
            for row in list_alpha_datasets(path=path)
            if _lookup_key(row.get("symbol")) == wanted and row.get("timeframe") == timeframe
        ),
        None,
    )
    if not match:
        return {
            "exchange": ALPHA_DATA_SOURCE,
            "symbol": symbol,
            "timeframe": timeframe,
            "available": False,
            "rows": 0,
            "start": None,
            "end": None,
        }
    return {**match, "available": True}


def get_alpha_source_status(*, path: Optional[Path] = None) -> Dict[str, Any]:
    database = _database_path(path)
    collector = load_collector_status()
    coverage = _timeframe_coverage_summary(path=database)
    dataset_count = sum(int(row.get("symbol_count") or 0) for row in coverage)
    timeframes = [str(row.get("timeframe") or "") for row in coverage]
    row_count = sum(int(row.get("rows") or 0) for row in coverage)
    symbol_count = max([int(row.get("symbol_count") or 0) for row in coverage], default=0)
    start_at = min([str(row.get("start")) for row in coverage if row.get("start")], default=None)
    end_at = max([str(row.get("end")) for row in coverage if row.get("end")], default=None)
    updated_at = max(
        [str(row.get("modified_at")) for row in coverage if row.get("modified_at")],
        default=None,
    )

    last_success_at = collector.get("last_success_at")
    age_sec: Optional[float] = None
    if last_success_at:
        try:
            parsed = datetime.fromisoformat(str(last_success_at).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            age_sec = max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds())
        except Exception:
            age_sec = None
    interval_sec = int((collector.get("config") or {}).get("interval_sec") or 60)
    collector_config = dict(collector.get("config") or {})
    stale = bool(age_sec is not None and age_sec > max(180, interval_sec * 3))
    state = str(collector.get("state") or "not_started")
    message = "Alpha 采集数据可直接用于行情查看、完整性检查和回放；稳定 Alpha ID 的实际报价资产见标的下拉"
    if row_count <= 0:
        message = "Alpha 采集库暂无 K 线，等待后台采集器首次写入"
    elif stale:
        message = "Alpha 采集数据可读取，但后台采集已超过预期更新时间"

    return {
        "exchange": ALPHA_DATA_SOURCE,
        "source": ALPHA_DATA_SOURCE,
        "source_type": "managed_collector",
        "state": state,
        "enabled": bool(collector.get("enabled", True)),
        "available": row_count > 0,
        "stale": stale,
        "age_sec": None if age_sec is None else round(age_sec, 3),
        "last_started_at": collector.get("last_started_at"),
        "last_finished_at": collector.get("last_finished_at"),
        "last_success_at": last_success_at,
        "last_error": collector.get("last_error"),
        "catalog_count": int(collector.get("catalog_count") or 0),
        "active_count": int(collector.get("active_count") or 0),
        "market_symbol_coverage": int(collector.get("market_symbol_coverage") or 0),
        "symbol_count": symbol_count,
        "dataset_count": dataset_count,
        "kline_rows": row_count,
        "timeframes": sorted(set(timeframes), key=lambda value: (_TIMEFRAME_ORDER.get(value, 999), value)),
        "timeframe_coverage": sorted(
            coverage,
            key=lambda row: (_TIMEFRAME_ORDER.get(str(row.get("timeframe") or ""), 999), str(row.get("timeframe") or "")),
        ),
        "start": start_at,
        "end": end_at,
        "updated_at": updated_at,
        "database_path": str(database),
        "database_size_mb": round(database.stat().st_size / (1024 * 1024), 2) if database.exists() else 0.0,
        "database_rows": dict(collector.get("database_rows") or {}),
        "retention": {
            "kline_days": int(collector_config.get("kline_retention_days") or 14),
            "kline_min_bars": int(collector_config.get("kline_min_bars") or 1200),
            "trade_hours": int(collector_config.get("trade_retention_hours") or 6),
            "orderbook_hours": int(collector_config.get("orderbook_retention_hours") or 24),
            "market_hours": int(collector_config.get("market_retention_hours") or 24),
            "run_days": int(collector_config.get("run_retention_days") or 30),
            "prune_interval_sec": int(collector_config.get("prune_interval_sec") or 3600),
        },
        "capabilities": {
            "klines": True,
            "integrity_check": True,
            "replay": True,
            "managed_refresh": True,
            "manual_download": False,
            "repair": False,
            "exchange_reconnect": False,
        },
        "message": message,
    }


__all__ = [
    "ALPHA_DATA_SOURCE",
    "get_alpha_coverage",
    "get_alpha_source_status",
    "is_alpha_data_source",
    "is_alpha_symbol",
    "list_alpha_datasets",
    "list_alpha_symbols",
    "load_alpha_klines",
    "load_alpha_ticker",
    "normalize_data_source",
]
