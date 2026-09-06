"""Persistent background collector for Binance Alpha market data.

The Alpha identifiers are not normal Binance spot symbols.  This collector
therefore talks directly to Binance's public Alpha REST API and stores the
result in a dedicated SQLite database under ``data/research/binance_alpha``.
The existing JSONL catalog remains the raw replay source; SQLite adds indexed
market snapshots, klines, aggregated trades, and order-book snapshots for
research queries.

The collector is intentionally conservative about request pressure:

* all active Alpha tokens receive scheduled kline updates;
* the 1m kline path is incremental after its first backfill;
* aggregated trades and order books are sampled for the highest-volume
  ``BINANCE_ALPHA_COLLECTOR_AUX_TOP_N`` tokens;
* per-symbol failures enter a temporary backoff instead of hammering a
  delisted or unavailable market.

This module never places orders and never promotes a token to a trading
universe by itself.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import math
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import httpx
from loguru import logger

from config.settings import settings
from core.data.binance_alpha import (
    ALPHA_BASE_URL,
    ALPHA_SOURCE_NAME,
    alpha_catalog_path,
    alpha_snapshot_history_path,
    alpha_trade_symbol_from_id,
    load_alpha_token_catalog,
)
from core.utils.shared_ssl import get_shared_ssl_context


ALPHA_KLINES_PATH = "/bapi/defi/v1/public/alpha-trade/klines"
ALPHA_AGG_TRADES_PATH = "/bapi/defi/v1/public/alpha-trade/agg-trades"
ALPHA_FULL_DEPTH_PATH = "/bapi/defi/v1/public/alpha-trade/fullDepth"
ALPHA_EXCHANGE_INFO_PATH = "/bapi/defi/v1/public/alpha-trade/get-exchange-info"
ALPHA_KLINES_URL = f"{ALPHA_BASE_URL}{ALPHA_KLINES_PATH}"
ALPHA_AGG_TRADES_URL = f"{ALPHA_BASE_URL}{ALPHA_AGG_TRADES_PATH}"
ALPHA_FULL_DEPTH_URL = f"{ALPHA_BASE_URL}{ALPHA_FULL_DEPTH_PATH}"
ALPHA_EXCHANGE_INFO_URL = f"{ALPHA_BASE_URL}{ALPHA_EXCHANGE_INFO_PATH}"

DEFAULT_KLINE_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")
KLINE_INTERVAL_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
    "3d": 259200,
    "1w": 604800,
    "1M": 2592000,
}
_SUPPORTED_KLINE_INTERVALS = set(KLINE_INTERVAL_SECONDS)
_STATUS_LOCK = threading.RLock()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: Optional[datetime] = None) -> str:
    current = value or _utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).isoformat()


def _alpha_root() -> Path:
    return Path(settings.BASE_DIR) / "data" / "research" / "binance_alpha"


def collector_database_path() -> Path:
    return _alpha_root() / "alpha_market.db"


def collector_status_path() -> Path:
    return _alpha_root() / "collector_status.json"


def exchange_info_path() -> Path:
    return _alpha_root() / "exchange_info.json"


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return number if math.isfinite(number) else float(default)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _parse_csv(value: Any, default: Sequence[str]) -> Tuple[str, ...]:
    raw = str(value or "").strip()
    if not raw:
        return tuple(default)
    result: List[str] = []
    seen = set()
    for item in raw.replace(";", ",").split(","):
        normalized = item.strip()
        if normalized not in _SUPPORTED_KLINE_INTERVALS or normalized in seen:
            continue
        result.append(normalized)
        seen.add(normalized)
    return tuple(result or default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class AlphaCollectorConfig:
    enabled: bool = True
    interval_sec: int = 60
    catalog_interval_sec: int = 300
    max_tokens: int = 400
    request_concurrency: int = 8
    timeout_sec: float = 10.0
    kline_limit: int = 300
    incremental_kline_limit: int = 50
    kline_intervals: Tuple[str, ...] = DEFAULT_KLINE_INTERVALS
    aux_enabled: bool = True
    aux_interval_sec: int = 60
    aux_top_n: int = 60
    trades_limit: int = 1000
    orderbook_limit: int = 20
    error_backoff_sec: int = 900
    startup_delay_sec: int = 15
    prune_interval_sec: int = 3600
    kline_retention_days: int = 14
    kline_min_bars: int = 1200
    trade_retention_hours: int = 6
    orderbook_retention_hours: int = 24
    market_retention_hours: int = 24
    run_retention_days: int = 30

    @classmethod
    def from_settings(cls) -> "AlphaCollectorConfig":
        values = {
            "enabled": _env_bool(
                "BINANCE_ALPHA_COLLECTOR_ENABLED",
                bool(getattr(settings, "BINANCE_ALPHA_COLLECTOR_ENABLED", True)),
            ),
            "interval_sec": max(
                15,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_INTERVAL_SEC",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_INTERVAL_SEC", 60)),
                ),
            ),
            "catalog_interval_sec": max(
                60,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_CATALOG_INTERVAL_SEC",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_CATALOG_INTERVAL_SEC", 300)),
                ),
            ),
            "max_tokens": max(
                1,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_MAX_TOKENS",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_MAX_TOKENS", 400)),
                ),
            ),
            "request_concurrency": max(
                1,
                min(
                    32,
                    _env_int(
                        "BINANCE_ALPHA_COLLECTOR_CONCURRENCY",
                        int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_CONCURRENCY", 8)),
                    ),
                ),
            ),
            "timeout_sec": max(
                2.0,
                min(
                    30.0,
                    _env_float(
                        "BINANCE_ALPHA_COLLECTOR_TIMEOUT_SEC",
                        float(getattr(settings, "BINANCE_ALPHA_COLLECTOR_TIMEOUT_SEC", 10.0)),
                    ),
                ),
            ),
            "kline_limit": max(
                1,
                min(
                    1000,
                    _env_int(
                        "BINANCE_ALPHA_COLLECTOR_KLINE_LIMIT",
                        int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_KLINE_LIMIT", 300)),
                    ),
                ),
            ),
            "incremental_kline_limit": max(
                2,
                min(
                    200,
                    _env_int(
                        "BINANCE_ALPHA_COLLECTOR_INCREMENTAL_KLINE_LIMIT",
                        int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_INCREMENTAL_KLINE_LIMIT", 50)),
                    ),
                ),
            ),
            "kline_intervals": _parse_csv(
                os.getenv(
                    "BINANCE_ALPHA_COLLECTOR_KLINE_INTERVALS",
                    str(getattr(settings, "BINANCE_ALPHA_COLLECTOR_KLINE_INTERVALS", ",".join(DEFAULT_KLINE_INTERVALS))),
                ),
                DEFAULT_KLINE_INTERVALS,
            ),
            "aux_enabled": _env_bool(
                "BINANCE_ALPHA_COLLECTOR_AUX_ENABLED",
                bool(getattr(settings, "BINANCE_ALPHA_COLLECTOR_AUX_ENABLED", True)),
            ),
            "aux_interval_sec": max(
                30,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_AUX_INTERVAL_SEC",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_AUX_INTERVAL_SEC", 60)),
                ),
            ),
            "aux_top_n": max(
                0,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_AUX_TOP_N",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_AUX_TOP_N", 60)),
                ),
            ),
            "trades_limit": max(
                1,
                min(
                    1000,
                    _env_int(
                        "BINANCE_ALPHA_COLLECTOR_TRADES_LIMIT",
                        int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_TRADES_LIMIT", 1000)),
                    ),
                ),
            ),
            "orderbook_limit": max(
                5,
                min(
                    1000,
                    _env_int(
                        "BINANCE_ALPHA_COLLECTOR_ORDERBOOK_LIMIT",
                        int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_ORDERBOOK_LIMIT", 20)),
                    ),
                ),
            ),
            "error_backoff_sec": max(
                60,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_ERROR_BACKOFF_SEC",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_ERROR_BACKOFF_SEC", 900)),
                ),
            ),
            "startup_delay_sec": max(
                0,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_STARTUP_DELAY_SEC",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_STARTUP_DELAY_SEC", 15)),
                ),
            ),
            "prune_interval_sec": max(
                300,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_PRUNE_INTERVAL_SEC",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_PRUNE_INTERVAL_SEC", 3600)),
                ),
            ),
            "kline_retention_days": max(
                1,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_KLINE_RETENTION_DAYS",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_KLINE_RETENTION_DAYS", 14)),
                ),
            ),
            "kline_min_bars": max(
                300,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_KLINE_MIN_BARS",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_KLINE_MIN_BARS", 1200)),
                ),
            ),
            "trade_retention_hours": max(
                1,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_TRADE_RETENTION_HOURS",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_TRADE_RETENTION_HOURS", 6)),
                ),
            ),
            "orderbook_retention_hours": max(
                1,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_ORDERBOOK_RETENTION_HOURS",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_ORDERBOOK_RETENTION_HOURS", 24)),
                ),
            ),
            "market_retention_hours": max(
                1,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_MARKET_RETENTION_HOURS",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_MARKET_RETENTION_HOURS", 24)),
                ),
            ),
            "run_retention_days": max(
                1,
                _env_int(
                    "BINANCE_ALPHA_COLLECTOR_RUN_RETENTION_DAYS",
                    int(getattr(settings, "BINANCE_ALPHA_COLLECTOR_RUN_RETENTION_DAYS", 30)),
                ),
            ),
        }
        return cls(**values)

    def as_dict(self) -> Dict[str, Any]:
        data = dict(self.__dict__)
        data["kline_intervals"] = list(self.kline_intervals)
        return data


class AlphaAPIError(RuntimeError):
    """Raised when Binance returns an application-level Alpha API error."""


def _unwrap_api_payload(payload: Any) -> Any:
    if not isinstance(payload, Mapping):
        return payload
    code = _clean_text(payload.get("code"))
    success = payload.get("success")
    if code and code not in {"000000", "0", "200"}:
        raise AlphaAPIError(f"Binance Alpha API {code}: {_clean_text(payload.get('message'))}")
    if success is False:
        raise AlphaAPIError(_clean_text(payload.get("message")) or "Binance Alpha API returned success=false")
    return payload.get("data")


def _active_tokens(tokens: Iterable[Mapping[str, Any]], max_tokens: int) -> List[Dict[str, Any]]:
    unique: Dict[str, Dict[str, Any]] = {}
    for item in tokens:
        if not isinstance(item, Mapping):
            continue
        alpha_id = _clean_text(item.get("alphaId")).upper()
        if not alpha_id or _as_bool(item.get("offline")):
            continue
        unique.setdefault(alpha_id, dict(item))
    ranked = sorted(
        unique.values(),
        key=lambda item: (_as_float(item.get("volume24h")), _as_float(item.get("liquidity"))),
        reverse=True,
    )
    return ranked[: max(1, int(max_tokens))]


def _preferred_market_symbols(data: Any) -> Dict[str, str]:
    """Map Alpha base assets to a currently tradable official pair.

    The token directory can retain historical entries whose default USDT pair
    is no longer listed.  Exchange info is the authoritative filter, and USDT
    is preferred when both USDT and USDC markets exist.
    """
    if not isinstance(data, Mapping):
        return {}
    symbols = data.get("symbols")
    if not isinstance(symbols, list):
        return {}
    candidates: Dict[str, List[str]] = {}
    for item in symbols:
        if not isinstance(item, Mapping):
            continue
        if _clean_text(item.get("status")).upper() != "TRADING":
            continue
        base = _clean_text(item.get("baseAsset")).upper()
        symbol = _clean_text(item.get("symbol")).upper()
        if not base or not symbol or not base.startswith("ALPHA_"):
            continue
        candidates.setdefault(base, []).append(symbol)
    preferred: Dict[str, str] = {}
    for base, market_symbols in candidates.items():
        ranked = sorted(
            set(market_symbols),
            key=lambda symbol: (0 if symbol.endswith("USDT") else 1 if symbol.endswith("USDC") else 2, symbol),
        )
        if ranked:
            preferred[base] = ranked[0]
    return preferred


def _parse_kline_rows(
    alpha_id: str,
    official_symbol: str,
    timeframe: str,
    data: Any,
    *,
    captured_at: str,
) -> List[Dict[str, Any]]:
    if not isinstance(data, list):
        return []
    now_ms = int(time.time() * 1000)
    result: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, (list, tuple)) or len(row) < 9:
            continue
        open_time = _as_int(row[0], -1)
        if open_time < 0:
            continue
        close_time = _as_int(row[6], open_time)
        open_price = _as_float(row[1], 0.0)
        high_price = _as_float(row[2], 0.0)
        low_price = _as_float(row[3], 0.0)
        close_price = _as_float(row[4], 0.0)
        if min(open_price, high_price, low_price, close_price) <= 0:
            continue
        result.append(
            {
                "alpha_id": alpha_id,
                "official_symbol": official_symbol,
                "timeframe": timeframe,
                "open_time_ms": open_time,
                "close_time_ms": close_time,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": _as_float(row[5], 0.0),
                "quote_volume": _as_float(row[7], 0.0),
                "trade_count": _as_int(row[8], 0),
                "taker_buy_volume": _as_float(row[9], 0.0) if len(row) > 9 else 0.0,
                "taker_buy_quote_volume": _as_float(row[10], 0.0) if len(row) > 10 else 0.0,
                "is_closed": int(close_time <= now_ms),
                "captured_at": captured_at,
                "raw_json": _json_dumps(list(row)),
            }
        )
    return result


def _parse_trade_rows(
    alpha_id: str,
    official_symbol: str,
    data: Any,
    *,
    captured_at: str,
) -> List[Dict[str, Any]]:
    if not isinstance(data, list):
        return []
    result: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, Mapping):
            continue
        trade_id = _as_int(row.get("a"), -1)
        if trade_id < 0:
            continue
        result.append(
            {
                "alpha_id": alpha_id,
                "official_symbol": official_symbol,
                "trade_id": trade_id,
                "price": _as_float(row.get("p")),
                "quantity": _as_float(row.get("q")),
                "first_trade_id": _as_int(row.get("f")),
                "last_trade_id": _as_int(row.get("l")),
                "trade_time_ms": _as_int(row.get("T")),
                "buyer_is_maker": int(bool(row.get("m"))),
                "captured_at": captured_at,
                "raw_json": _json_dumps(dict(row)),
            }
        )
    return result


def _parse_orderbook_row(
    alpha_id: str,
    official_symbol: str,
    data: Any,
    *,
    captured_at: str,
) -> Optional[Dict[str, Any]]:
    if not isinstance(data, Mapping):
        return None
    if not isinstance(data.get("bids"), list) and not isinstance(data.get("asks"), list):
        return None
    return {
        "alpha_id": alpha_id,
        "official_symbol": official_symbol,
        "event_time_ms": _as_int(data.get("E")),
        "transaction_time_ms": _as_int(data.get("T")),
        "last_update_id": _as_int(data.get("lastUpdateId")),
        "bids_json": _json_dumps(data.get("bids") or []),
        "asks_json": _json_dumps(data.get("asks") or []),
        "captured_at": captured_at,
        "raw_json": _json_dumps(dict(data)),
    }


_SCHEMA = """
CREATE TABLE IF NOT EXISTS market_snapshots (
    captured_at TEXT NOT NULL,
    alpha_id TEXT NOT NULL,
    official_symbol TEXT NOT NULL,
    display_symbol TEXT,
    name TEXT,
    chain_id TEXT,
    chain_name TEXT,
    contract_address TEXT,
    price REAL,
    price_change_24h REAL,
    volume_24h REAL,
    market_cap REAL,
    fdv REAL,
    liquidity REAL,
    holders INTEGER,
    total_supply REAL,
    circulating_supply REAL,
    hot_tag INTEGER,
    directory_score INTEGER,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (captured_at, alpha_id)
);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_alpha_time
    ON market_snapshots (alpha_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_retention
    ON market_snapshots (captured_at);

CREATE TABLE IF NOT EXISTS alpha_klines (
    alpha_id TEXT NOT NULL,
    official_symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open_time_ms INTEGER NOT NULL,
    close_time_ms INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    quote_volume REAL NOT NULL,
    trade_count INTEGER NOT NULL,
    taker_buy_volume REAL NOT NULL,
    taker_buy_quote_volume REAL NOT NULL,
    is_closed INTEGER NOT NULL,
    captured_at TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (alpha_id, timeframe, open_time_ms)
);
CREATE INDEX IF NOT EXISTS idx_alpha_klines_time
    ON alpha_klines (timeframe, open_time_ms);

CREATE TABLE IF NOT EXISTS alpha_agg_trades (
    alpha_id TEXT NOT NULL,
    official_symbol TEXT NOT NULL,
    trade_id INTEGER NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    first_trade_id INTEGER NOT NULL,
    last_trade_id INTEGER NOT NULL,
    trade_time_ms INTEGER NOT NULL,
    buyer_is_maker INTEGER NOT NULL,
    captured_at TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (alpha_id, trade_id)
);
CREATE INDEX IF NOT EXISTS idx_alpha_trades_time
    ON alpha_agg_trades (alpha_id, trade_time_ms);
CREATE INDEX IF NOT EXISTS idx_alpha_trades_retention
    ON alpha_agg_trades (trade_time_ms);

CREATE TABLE IF NOT EXISTS alpha_orderbook_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alpha_id TEXT NOT NULL,
    official_symbol TEXT NOT NULL,
    event_time_ms INTEGER NOT NULL,
    transaction_time_ms INTEGER NOT NULL,
    last_update_id INTEGER NOT NULL,
    bids_json TEXT NOT NULL,
    asks_json TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alpha_orderbook_time
    ON alpha_orderbook_snapshots (alpha_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_alpha_orderbook_retention
    ON alpha_orderbook_snapshots (captured_at);

CREATE TABLE IF NOT EXISTS collector_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    summary_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_collector_runs_retention
    ON collector_runs (started_at);
"""


class _SQLiteStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=30.0)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.executescript(_SCHEMA)
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        connection.close()

    def latest_kline_times(self, timeframe: str) -> Dict[str, int]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT alpha_id, MAX(open_time_ms) FROM alpha_klines WHERE timeframe = ? GROUP BY alpha_id",
                (timeframe,),
            ).fetchall()
            return {str(alpha_id): int(value) for alpha_id, value in rows if value is not None}
        finally:
            connection.close()

    def latest_trade_ids(self) -> Dict[str, int]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT alpha_id, MAX(trade_id) FROM alpha_agg_trades GROUP BY alpha_id"
            ).fetchall()
            return {str(alpha_id): int(value) for alpha_id, value in rows if value is not None}
        finally:
            connection.close()

    def counts(self) -> Dict[str, int]:
        connection = self._connect()
        try:
            queries = {
                "market_snapshots": "SELECT COUNT(*) FROM market_snapshots",
                "klines": "SELECT COUNT(*) FROM alpha_klines",
                "agg_trades": "SELECT COUNT(*) FROM alpha_agg_trades",
                "orderbook_snapshots": "SELECT COUNT(*) FROM alpha_orderbook_snapshots",
                "collector_runs": "SELECT COUNT(*) FROM collector_runs",
            }
            return {
                name: int(connection.execute(query).fetchone()[0] or 0)
                for name, query in queries.items()
            }
        finally:
            connection.close()

    def prune(self, config: AlphaCollectorConfig, *, now: Optional[datetime] = None) -> Dict[str, int]:
        """Delete expired high-volume history while preserving recent research data.

        SQLite keeps freed pages for later inserts, so this bounds ongoing file
        growth without an expensive blocking VACUUM in the live worker.
        """
        current = now or _utc_now()
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        current = current.astimezone(timezone.utc)
        cutoffs = {
            "market_snapshots": (current - timedelta(hours=config.market_retention_hours)).isoformat(),
            "alpha_agg_trades": int(
                (current - timedelta(hours=config.trade_retention_hours)).timestamp() * 1000
            ),
            "alpha_orderbook_snapshots": (
                current - timedelta(hours=config.orderbook_retention_hours)
            ).isoformat(),
            "collector_runs": (current - timedelta(days=config.run_retention_days)).isoformat(),
        }
        connection = self._connect()
        deleted: Dict[str, int] = {}
        try:
            with connection:
                statements = {
                    "market_snapshots": ("captured_at", cutoffs["market_snapshots"]),
                    "alpha_agg_trades": ("trade_time_ms", cutoffs["alpha_agg_trades"]),
                    "alpha_orderbook_snapshots": (
                        "captured_at",
                        cutoffs["alpha_orderbook_snapshots"],
                    ),
                    "collector_runs": ("started_at", cutoffs["collector_runs"]),
                }
                for table, (column, cutoff) in statements.items():
                    cursor = connection.execute(
                        f"DELETE FROM {table} WHERE {column} < ?",
                        (cutoff,),
                    )
                    deleted[table] = max(0, int(cursor.rowcount or 0))

                kline_deleted = 0
                timeframes = [
                    str(row[0])
                    for row in connection.execute(
                        "SELECT DISTINCT timeframe FROM alpha_klines"
                    ).fetchall()
                ]
                for timeframe in timeframes:
                    date_cutoff_ms = int(
                        (current - timedelta(days=config.kline_retention_days)).timestamp() * 1000
                    )
                    minimum_bars = max(1, int(config.kline_min_bars))
                    alpha_ids = [
                        str(row[0])
                        for row in connection.execute(
                            "SELECT DISTINCT alpha_id FROM alpha_klines WHERE timeframe = ?",
                            (timeframe,),
                        ).fetchall()
                    ]
                    for alpha_id in alpha_ids:
                        retained_boundary = connection.execute(
                            """SELECT open_time_ms FROM alpha_klines
                               WHERE alpha_id = ? AND timeframe = ?
                               ORDER BY open_time_ms DESC LIMIT 1 OFFSET ?""",
                            (alpha_id, timeframe, minimum_bars - 1),
                        ).fetchone()
                        # Fewer than minimum_bars: keep the complete series.
                        if retained_boundary is None:
                            continue
                        cutoff_ms = min(date_cutoff_ms, int(retained_boundary[0]))
                        cursor = connection.execute(
                            """DELETE FROM alpha_klines
                               WHERE alpha_id = ? AND timeframe = ? AND open_time_ms < ?""",
                            (alpha_id, timeframe, cutoff_ms),
                        )
                        kline_deleted += max(0, int(cursor.rowcount or 0))
                deleted["alpha_klines"] = kline_deleted
            connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
            connection.execute("PRAGMA optimize")
            return deleted
        finally:
            connection.close()

    def persist_batch(
        self,
        *,
        captured_at: str,
        tokens: Sequence[Mapping[str, Any]],
        klines: Sequence[Mapping[str, Any]],
        trades: Sequence[Mapping[str, Any]],
        orderbooks: Sequence[Mapping[str, Any]],
        run_id: str,
        started_at: str,
        finished_at: str,
        run_status: str,
        summary: Mapping[str, Any],
    ) -> None:
        connection = self._connect()
        try:
            with connection:
                market_rows = []
                for item in tokens:
                    alpha_id = _clean_text(item.get("alphaId")).upper()
                    if not alpha_id:
                        continue
                    market_rows.append(
                        (
                            captured_at,
                            alpha_id,
                            _clean_text(item.get("officialSymbol")) or alpha_trade_symbol_from_id(alpha_id),
                            _clean_text(item.get("symbol")),
                            _clean_text(item.get("name")),
                            _clean_text(item.get("chainId")),
                            _clean_text(item.get("chainName")),
                            _clean_text(item.get("contractAddress")),
                            _as_float(item.get("price")),
                            _as_float(item.get("percentChange24h")),
                            _as_float(item.get("volume24h")),
                            _as_float(item.get("marketCap")),
                            _as_float(item.get("fdv")),
                            _as_float(item.get("liquidity")),
                            _as_int(item.get("holders")),
                            _as_float(item.get("totalSupply")),
                            _as_float(item.get("circulatingSupply")),
                            int(_as_bool(item.get("hotTag"))),
                            _as_int(item.get("score")),
                            _json_dumps(dict(item)),
                        )
                    )
                if market_rows:
                    connection.executemany(
                        """INSERT OR REPLACE INTO market_snapshots (
                            captured_at, alpha_id, official_symbol, display_symbol,
                            name, chain_id, chain_name, contract_address, price,
                            price_change_24h, volume_24h, market_cap, fdv,
                            liquidity, holders, total_supply, circulating_supply,
                            hot_tag, directory_score, raw_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        market_rows,
                    )

                if klines:
                    connection.executemany(
                        """INSERT OR REPLACE INTO alpha_klines (
                            alpha_id, official_symbol, timeframe, open_time_ms,
                            close_time_ms, open, high, low, close, volume,
                            quote_volume, trade_count, taker_buy_volume,
                            taker_buy_quote_volume, is_closed, captured_at, raw_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [
                            (
                                row["alpha_id"], row["official_symbol"], row["timeframe"],
                                row["open_time_ms"], row["close_time_ms"], row["open"],
                                row["high"], row["low"], row["close"], row["volume"],
                                row["quote_volume"], row["trade_count"], row["taker_buy_volume"],
                                row["taker_buy_quote_volume"], row["is_closed"],
                                row["captured_at"], row["raw_json"],
                            )
                            for row in klines
                        ],
                    )

                if trades:
                    connection.executemany(
                        """INSERT OR REPLACE INTO alpha_agg_trades (
                            alpha_id, official_symbol, trade_id, price, quantity,
                            first_trade_id, last_trade_id, trade_time_ms,
                            buyer_is_maker, captured_at, raw_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [
                            (
                                row["alpha_id"], row["official_symbol"], row["trade_id"],
                                row["price"], row["quantity"], row["first_trade_id"],
                                row["last_trade_id"], row["trade_time_ms"], row["buyer_is_maker"],
                                row["captured_at"], row["raw_json"],
                            )
                            for row in trades
                        ],
                    )

                if orderbooks:
                    connection.executemany(
                        """INSERT INTO alpha_orderbook_snapshots (
                            alpha_id, official_symbol, event_time_ms,
                            transaction_time_ms, last_update_id, bids_json,
                            asks_json, captured_at, raw_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [
                            (
                                row["alpha_id"], row["official_symbol"], row["event_time_ms"],
                                row["transaction_time_ms"], row["last_update_id"],
                                row["bids_json"], row["asks_json"], row["captured_at"],
                                row["raw_json"],
                            )
                            for row in orderbooks
                        ],
                    )

                connection.execute(
                    """INSERT OR REPLACE INTO collector_runs
                       (run_id, started_at, finished_at, status, summary_json)
                       VALUES (?, ?, ?, ?, ?)""",
                    (run_id, started_at, finished_at, run_status, _json_dumps(dict(summary))),
                )
        finally:
            connection.close()


def _default_status(config: AlphaCollectorConfig) -> Dict[str, Any]:
    return {
        "source": ALPHA_SOURCE_NAME,
        "state": "not_started",
        "enabled": bool(config.enabled),
        "last_started_at": None,
        "last_finished_at": None,
        "last_success_at": None,
        "last_error": None,
        "last_run": None,
        "catalog_path": str(alpha_catalog_path()),
        "catalog_history_path": str(alpha_snapshot_history_path()),
        "exchange_info_path": str(exchange_info_path()),
        "database_path": str(collector_database_path()),
        "status_path": str(collector_status_path()),
        "config": config.as_dict(),
    }


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(temp_path, path)


def _persist_exchange_info(payload: Any) -> None:
    if not isinstance(payload, Mapping):
        return
    with _STATUS_LOCK:
        _atomic_write_json(exchange_info_path(), dict(payload))


def load_collector_status() -> Dict[str, Any]:
    """Return the last persisted collector status without starting it."""
    config = AlphaCollectorConfig.from_settings()
    path = collector_status_path()
    with _STATUS_LOCK:
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    payload["config"] = {
                        **config.as_dict(),
                        **dict(payload.get("config") or {}),
                    }
                    payload.setdefault("exchange_info_path", str(exchange_info_path()))
                    payload.setdefault("database_path", str(collector_database_path()))
                    return payload
            except Exception:
                pass
    return _default_status(config)


class BinanceAlphaCollector:
    """Run the scheduled Alpha REST collection loop."""

    def __init__(self, config: Optional[AlphaCollectorConfig] = None) -> None:
        self.config = config or AlphaCollectorConfig.from_settings()
        self.store = _SQLiteStore(collector_database_path())
        self._tokens: List[Dict[str, Any]] = []
        self._market_symbols: Dict[str, str] = {}
        now = time.monotonic()
        self._next_catalog_due = 0.0
        self._next_aux_due = now
        self._next_prune_due = now
        self._next_kline_due: Dict[str, float] = {
            interval: now + index * max(1, self.config.interval_sec)
            for index, interval in enumerate(self.config.kline_intervals)
        }
        self._failure_until: Dict[Tuple[str, str], float] = {}
        self._status = load_collector_status()
        self._status.update({"enabled": bool(self.config.enabled), "config": self.config.as_dict()})
        self._status_lock = threading.RLock()

    def _write_status(self) -> None:
        with self._status_lock:
            payload = dict(self._status)
        try:
            with _STATUS_LOCK:
                _atomic_write_json(collector_status_path(), payload)
        except Exception as exc:
            logger.warning(f"Failed to persist Binance Alpha collector status: {exc}")

    def _update_status(self, **updates: Any) -> None:
        with self._status_lock:
            self._status.update(updates)
        self._write_status()

    async def _request(self, client: httpx.AsyncClient, url: str, params: Mapping[str, Any]) -> Any:
        response = await client.get(url, params=dict(params))
        response.raise_for_status()
        return _unwrap_api_payload(response.json())

    async def _refresh_market_symbols(self, client: httpx.AsyncClient) -> Optional[str]:
        try:
            raw = await self._request(client, ALPHA_EXCHANGE_INFO_URL, {})
            preferred = _preferred_market_symbols(raw)
            if preferred:
                self._market_symbols = preferred
                await asyncio.to_thread(_persist_exchange_info, raw)
                return None
            return "Binance Alpha exchange info returned no TRADING Alpha symbols"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"Binance Alpha exchange info refresh failed: {exc}")
            return f"Binance Alpha exchange info refresh failed: {type(exc).__name__}: {exc}"

    def _official_market_symbol(self, token: Mapping[str, Any]) -> str:
        alpha_id = _clean_text(token.get("alphaId")).upper()
        return self._market_symbols.get(alpha_id) or alpha_trade_symbol_from_id(alpha_id)

    def _due_kline_intervals(self, now: float) -> List[str]:
        due: List[str] = []
        for interval in self.config.kline_intervals:
            next_due = self._next_kline_due.get(interval, 0.0)
            if now >= next_due:
                due.append(interval)
                cadence = max(1, KLINE_INTERVAL_SECONDS.get(interval, self.config.interval_sec))
                next_value = next_due or now
                while next_value <= now:
                    next_value += cadence
                self._next_kline_due[interval] = next_value
        return due

    async def _collect_kline_interval(
        self,
        client: httpx.AsyncClient,
        timeframe: str,
        tokens: Sequence[Mapping[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], int, List[str]]:
        latest = await asyncio.to_thread(self.store.latest_kline_times, timeframe)
        semaphore = asyncio.Semaphore(self.config.request_concurrency)
        captured_at = _utc_iso()
        rows: List[Dict[str, Any]] = []
        errors: List[str] = []
        requests = 0

        async def collect_one(token: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
            alpha_id = _clean_text(token.get("alphaId")).upper()
            official_symbol = self._official_market_symbol(token)
            key = (official_symbol, timeframe)
            if not alpha_id or not official_symbol:
                return [], "missing_alpha_id", False
            if self._failure_until.get(key, 0.0) > time.monotonic():
                return [], None, False
            last_open = latest.get(alpha_id)
            params: Dict[str, Any] = {
                "symbol": official_symbol,
                "interval": timeframe,
                "limit": self.config.kline_limit if last_open is None else min(
                    self.config.incremental_kline_limit, self.config.kline_limit
                ),
            }
            if last_open is not None:
                step_ms = KLINE_INTERVAL_SECONDS.get(timeframe, 60) * 1000
                params["startTime"] = max(0, int(last_open) - step_ms)
            try:
                async with semaphore:
                    data = await self._request(client, ALPHA_KLINES_URL, params)
                parsed = _parse_kline_rows(
                    alpha_id,
                    official_symbol,
                    timeframe,
                    data,
                    captured_at=captured_at,
                )
                self._failure_until.pop(key, None)
                return parsed, None, True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._failure_until[key] = time.monotonic() + self.config.error_backoff_sec
                return [], f"{official_symbol}/{timeframe}: {type(exc).__name__}: {exc}", True

        results = await asyncio.gather(*(collect_one(token) for token in tokens))
        for parsed, error, attempted in results:
            if attempted:
                requests += 1
            rows.extend(parsed)
            if error:
                errors.append(error)
        return rows, requests, errors

    async def _collect_aux(
        self,
        client: httpx.AsyncClient,
        tokens: Sequence[Mapping[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, int, List[str]]:
        if not self.config.aux_enabled or self.config.aux_top_n <= 0:
            return [], [], 0, 0, []
        ranked = sorted(tokens, key=lambda item: _as_float(item.get("volume24h")), reverse=True)
        selected = ranked[: self.config.aux_top_n]
        cursors = await asyncio.to_thread(self.store.latest_trade_ids)
        semaphore = asyncio.Semaphore(self.config.request_concurrency)
        captured_at = _utc_iso()
        trades: List[Dict[str, Any]] = []
        orderbooks: List[Dict[str, Any]] = []
        errors: List[str] = []
        trade_requests = 0
        orderbook_requests = 0

        async def collect_trade(token: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
            alpha_id = _clean_text(token.get("alphaId")).upper()
            official_symbol = self._official_market_symbol(token)
            if not official_symbol:
                return [], "missing_alpha_id"
            params: Dict[str, Any] = {"symbol": official_symbol, "limit": self.config.trades_limit}
            cursor = cursors.get(alpha_id)
            if cursor is not None:
                params["fromId"] = int(cursor) + 1
            try:
                async with semaphore:
                    data = await self._request(client, ALPHA_AGG_TRADES_URL, params)
                return _parse_trade_rows(alpha_id, official_symbol, data, captured_at=captured_at), None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                return [], f"{official_symbol}/trades: {type(exc).__name__}: {exc}"

        async def collect_orderbook(token: Mapping[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
            alpha_id = _clean_text(token.get("alphaId")).upper()
            official_symbol = self._official_market_symbol(token)
            if not official_symbol:
                return None, "missing_alpha_id"
            params = {"symbol": official_symbol, "limit": self.config.orderbook_limit}
            try:
                async with semaphore:
                    data = await self._request(client, ALPHA_FULL_DEPTH_URL, params)
                return _parse_orderbook_row(alpha_id, official_symbol, data, captured_at=captured_at), None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                return None, f"{official_symbol}/orderbook: {type(exc).__name__}: {exc}"

        trade_results = await asyncio.gather(*(collect_trade(token) for token in selected))
        for parsed, error in trade_results:
            trade_requests += 1
            trades.extend(parsed)
            if error:
                errors.append(error)

        book_results = await asyncio.gather(*(collect_orderbook(token) for token in selected))
        for parsed, error in book_results:
            orderbook_requests += 1
            if parsed:
                orderbooks.append(parsed)
            if error:
                errors.append(error)

        return trades, orderbooks, trade_requests, orderbook_requests, errors

    async def run_once(self) -> Dict[str, Any]:
        started_at = _utc_iso()
        run_id = f"alpha-{int(time.time() * 1000)}"
        self._update_status(
            state="running",
            last_started_at=started_at,
            last_error=None,
        )
        run_started_monotonic = time.monotonic()
        catalog_refreshed = False
        catalog_warning = ""
        kline_rows: List[Dict[str, Any]] = []
        trade_rows: List[Dict[str, Any]] = []
        orderbook_rows: List[Dict[str, Any]] = []
        errors: List[str] = []
        kline_requests = 0
        trade_requests = 0
        orderbook_requests = 0
        tokens: List[Dict[str, Any]] = []
        market_symbol_coverage = 0

        try:
            now = time.monotonic()
            if not self._tokens or now >= self._next_catalog_due:
                catalog = await load_alpha_token_catalog(refresh=True)
                catalog_refreshed = True
                self._next_catalog_due = now + self.config.catalog_interval_sec
            else:
                catalog = await load_alpha_token_catalog(refresh=False)
            catalog_warning = _clean_text(catalog.get("warning"))
            tokens = _active_tokens(catalog.get("tokens") or [], self.config.max_tokens)
            if tokens:
                self._tokens = tokens
            elif self._tokens:
                tokens = list(self._tokens)
            if not tokens:
                raise RuntimeError(catalog_warning or "Binance Alpha catalog returned no active tokens")
            if catalog_warning:
                errors.append(catalog_warning)

            due_intervals = self._due_kline_intervals(now)
            if due_intervals:
                async with httpx.AsyncClient(
                    timeout=self.config.timeout_sec,
                    follow_redirects=True,
                    verify=get_shared_ssl_context(),
                    headers={"User-Agent": "crypto-trading-system/binance-alpha-collector"},
                    limits=httpx.Limits(
                        max_connections=max(8, self.config.request_concurrency * 2),
                        max_keepalive_connections=max(4, self.config.request_concurrency),
                    ),
                ) as client:
                    if catalog_refreshed or not self._market_symbols:
                        exchange_info_error = await self._refresh_market_symbols(client)
                        if exchange_info_error:
                            errors.append(exchange_info_error)
                    market_tokens = [
                        token
                        for token in tokens
                        if _clean_text(token.get("alphaId")).upper() in self._market_symbols
                    ]
                    # If exchange-info is unavailable, keep the token list as
                    # a best-effort fallback; individual API errors are still
                    # recorded and placed into backoff.
                    if not market_tokens:
                        market_tokens = list(tokens)
                    market_symbol_coverage = len(
                        [
                            token
                            for token in tokens
                            if _clean_text(token.get("alphaId")).upper() in self._market_symbols
                        ]
                    )
                    for timeframe in due_intervals:
                        parsed, attempted, interval_errors = await self._collect_kline_interval(
                            client, timeframe, market_tokens
                        )
                        kline_rows.extend(parsed)
                        kline_requests += attempted
                        errors.extend(interval_errors)

                    if self.config.aux_enabled and now >= self._next_aux_due:
                        (
                            parsed_trades,
                            parsed_orderbooks,
                            aux_trade_requests,
                            aux_orderbook_requests,
                            aux_errors,
                        ) = await self._collect_aux(client, market_tokens)
                        trade_rows.extend(parsed_trades)
                        orderbook_rows.extend(parsed_orderbooks)
                        trade_requests += aux_trade_requests
                        orderbook_requests += aux_orderbook_requests
                        errors.extend(aux_errors)
                        self._next_aux_due = now + self.config.aux_interval_sec
            else:
                market_symbol_coverage = len(
                    [
                        token
                        for token in tokens
                        if _clean_text(token.get("alphaId")).upper() in self._market_symbols
                    ]
                )

            finished_at = _utc_iso()
            summary: Dict[str, Any] = {
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": "degraded" if errors else "ok",
                "catalog_refreshed": catalog_refreshed,
                "catalog_count": len(catalog.get("tokens") or []),
                "active_count": len(tokens),
                "market_symbol_coverage": market_symbol_coverage,
                "kline_requests": kline_requests,
                "kline_rows": len(kline_rows),
                "trade_requests": trade_requests,
                "trade_rows": len(trade_rows),
                "orderbook_requests": orderbook_requests,
                "orderbook_rows": len(orderbook_rows),
                "errors": errors[:50],
                "duration_sec": round(max(0.0, time.monotonic() - run_started_monotonic), 3),
            }
            if time.monotonic() >= self._next_prune_due:
                try:
                    summary["pruned_rows"] = await asyncio.to_thread(
                        self.store.prune,
                        self.config,
                    )
                    self._next_prune_due = time.monotonic() + self.config.prune_interval_sec
                except Exception as prune_exc:
                    errors.append(f"retention prune: {type(prune_exc).__name__}: {prune_exc}")
                    summary["status"] = "degraded"
                    summary["errors"] = errors[:50]
                    self._next_prune_due = time.monotonic() + min(
                        300,
                        self.config.prune_interval_sec,
                    )
            await asyncio.to_thread(
                self.store.persist_batch,
                captured_at=finished_at,
                tokens=(
                    [
                        {
                            **token,
                            "officialSymbol": self._official_market_symbol(token),
                        }
                        for token in tokens
                    ]
                    if catalog_refreshed
                    else []
                ),
                klines=kline_rows,
                trades=trade_rows,
                orderbooks=orderbook_rows,
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                run_status=str(summary["status"]),
                summary=summary,
            )
            total_rows = await asyncio.to_thread(self.store.counts)
            summary["database_total_rows"] = total_rows
            self._update_status(
                state="degraded" if errors else "ready",
                last_finished_at=finished_at,
                last_success_at=finished_at,
                last_error=(errors[0] if errors else None),
                last_run=summary,
                catalog_count=summary["catalog_count"],
                active_count=summary["active_count"],
                market_symbol_coverage=summary["market_symbol_coverage"],
                database_rows={
                    **total_rows,
                },
            )
            return summary
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            finished_at = _utc_iso()
            summary = {
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": "failed",
                "catalog_refreshed": catalog_refreshed,
                "catalog_count": 0,
                "active_count": len(tokens),
                "market_symbol_coverage": market_symbol_coverage,
                "kline_requests": kline_requests,
                "kline_rows": len(kline_rows),
                "trade_requests": trade_requests,
                "trade_rows": len(trade_rows),
                "orderbook_requests": orderbook_requests,
                "orderbook_rows": len(orderbook_rows),
                "errors": [f"{type(exc).__name__}: {exc}"],
                "duration_sec": round(max(0.0, time.monotonic() - run_started_monotonic), 3),
            }
            self._update_status(
                state="failed",
                last_finished_at=finished_at,
                last_error=summary["errors"][0],
                last_run=summary,
            )
            logger.warning(f"Binance Alpha collector run failed: {exc}")
            return summary

    async def run(
        self,
        stop_event: asyncio.Event,
        *,
        heartbeat: Optional[Callable[[bool], Any]] = None,
    ) -> None:
        if not self.config.enabled:
            self._update_status(state="disabled")
            return
        if self.config.startup_delay_sec > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.config.startup_delay_sec)
                return
            except asyncio.TimeoutError:
                pass
        while not stop_event.is_set():
            summary = await self.run_once()
            if heartbeat:
                try:
                    result = heartbeat(str(summary.get("status")) in {"ok", "degraded"})
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    pass
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.config.interval_sec)
            except asyncio.TimeoutError:
                pass


__all__ = [
    "ALPHA_AGG_TRADES_PATH",
    "ALPHA_FULL_DEPTH_PATH",
    "ALPHA_KLINES_PATH",
    "AlphaCollectorConfig",
    "BinanceAlphaCollector",
    "collector_database_path",
    "collector_status_path",
    "load_collector_status",
    "_parse_kline_rows",
    "_parse_orderbook_row",
    "_parse_trade_rows",
]
