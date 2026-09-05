"""Complete and maintain every Binance market-data series used by the repo.

This maintainer deliberately keeps spot and USD-M perpetual data in separate
storage namespaces:

* ``binance``: spot data already used by the application and research tools.
* ``binance_futures``: USD-M perpetual data used for futures research.

The Parquet store remains the canonical bar database.  SQLite receives a
compact ``market_data_catalog`` describing coverage and freshness instead of a
second, multi-gigabyte copy of the same candles.  No order, strategy-manager,
or radar module is imported by this script.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from loguru import logger


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.data.data_storage import data_storage  # noqa: E402
from core.data.parquet_lock import parquet_partition_lock  # noqa: E402
from core.data.path_utils import (  # noqa: E402
    candidate_symbol_dirs,
    canonical_symbol_dir,
    symbol_from_storage_dirname,
)
from core.exchanges.base_exchange import Kline  # noqa: E402


SPOT_NAMESPACE = "binance"
FUTURES_NAMESPACE = "binance_futures"
SPOT_HOSTS = ("https://data-api.binance.vision", "https://api.binance.com")
FUTURES_HOSTS = ("https://fapi.binance.com",)
USER_AGENT = "crypto-trading-system/all-binance-data-maintainer/1.0"
SUPPORTED_TIMEFRAME = re.compile(r"^[1-9][0-9]*[smhdw]$")
DEFAULT_TIMEFRAMES = ("1m", "5m", "15m", "1h", "4h", "1d")
DEFAULT_RESEARCH_SYMBOLS = (
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "ADA/USDT", "DOGE/USDT", "TRX/USDT", "LINK/USDT", "AVAX/USDT",
    "DOT/USDT", "POL/USDT", "LTC/USDT", "BCH/USDT", "ETC/USDT",
    "ATOM/USDT", "NEAR/USDT", "APT/USDT", "ARB/USDT", "OP/USDT",
    "SUI/USDT", "INJ/USDT", "RUNE/USDT", "AAVE/USDT", "MKR/USDT",
    "UNI/USDT", "FIL/USDT", "HBAR/USDT", "ICP/USDT", "TON/USDT",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_ready(v) for v in value]
    return value


def timeframe_ms(timeframe: str) -> int:
    text = str(timeframe or "").strip().lower()
    if not SUPPORTED_TIMEFRAME.fullmatch(text):
        raise ValueError(f"Unsupported timeframe: {timeframe!r}")
    value = int(text[:-1])
    multiplier = {
        "s": 1_000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 7 * 86_400_000,
    }[text[-1]]
    return value * multiplier


def timeframe_alignment_offset_ms(timeframe: str) -> int:
    """Binance weekly candles start Monday 00:00 UTC, not at Unix epoch."""
    return 4 * 86_400_000 if str(timeframe).lower().endswith("w") else 0


def normalize_symbol(value: str) -> str:
    text = str(value or "").strip().upper().replace("-", "/").replace("_", "/")
    if "/" in text:
        base, quote = text.split("/", 1)
        return f"{base}/{quote}"
    if text.endswith("USDT") and len(text) > 4:
        return f"{text[:-4]}/USDT"
    return text


def raw_symbol(value: str) -> str:
    return normalize_symbol(value).replace("/", "")


@dataclass(frozen=True, order=True)
class SeriesKey:
    namespace: str
    market_type: str
    symbol: str
    timeframe: str


@dataclass
class LocalBounds:
    first: datetime | None = None
    last: datetime | None = None
    files: int = 0


@dataclass(frozen=True)
class FetchWindow:
    start_ms: int
    end_open_ms: int
    reason: str


@dataclass
class SeriesResult:
    namespace: str
    market_type: str
    symbol: str
    timeframe: str
    source_status: str
    target_start_utc: str
    target_end_utc: str
    before_first_utc: str | None
    before_last_utc: str | None
    requested_start_utc: str | None = None
    source_available_from_utc: str | None = None
    after_first_utc: str | None = None
    after_last_utc: str | None = None
    file_count: int = 0
    windows: int = 0
    downloaded_rows: int = 0
    complete_head: bool = False
    fresh_tail: bool = False
    status: str = "pending"
    error: str | None = None


class RequestRateLimiter:
    def __init__(self, max_requests_per_second: float):
        self._interval = 1.0 / max(float(max_requests_per_second), 0.1)
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self._interval
        if delay:
            time.sleep(delay)


_THREAD_LOCAL = threading.local()


def _session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        _THREAD_LOCAL.session = session
    return session


def request_json(
    hosts: Sequence[str],
    path: str,
    *,
    params: dict[str, Any] | None = None,
    limiter: RequestRateLimiter | None = None,
    attempts: int = 5,
    timeout: int = 30,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        for host in hosts:
            if limiter is not None:
                limiter.wait()
            try:
                response = _session().get(f"{host}{path}", params=params, timeout=timeout)
                if response.status_code in {418, 429}:
                    retry_after = float(response.headers.get("Retry-After") or 0)
                    time.sleep(max(retry_after, min(30.0, 1.5 * attempt)))
                    response.raise_for_status()
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # noqa: BLE001 - exhausted errors become audit data
                last_error = exc
        if attempt < attempts:
            time.sleep(min(20.0, 0.75 * (2 ** (attempt - 1))) + random.random() * 0.2)
    raise RuntimeError(f"{path} failed after {attempts} attempts: {last_error}")


def load_market_statuses(
    limiter: RequestRateLimiter,
) -> tuple[dict[str, str], dict[str, str]]:
    spot = request_json(SPOT_HOSTS, "/api/v3/exchangeInfo", limiter=limiter)
    futures = request_json(FUTURES_HOSTS, "/fapi/v1/exchangeInfo", limiter=limiter)
    spot_status = {
        str(item.get("symbol") or "").upper(): str(item.get("status") or "UNKNOWN").upper()
        for item in spot.get("symbols", [])
        if item.get("quoteAsset") == "USDT"
    }
    futures_status = {
        str(item.get("symbol") or "").upper(): str(item.get("status") or "UNKNOWN").upper()
        for item in futures.get("symbols", [])
        if item.get("quoteAsset") == "USDT" and item.get("contractType") == "PERPETUAL"
    }
    return spot_status, futures_status


def discover_local_series(storage_root: Path, namespace: str, market_type: str) -> set[SeriesKey]:
    root = Path(storage_root) / namespace
    output: set[SeriesKey] = set()
    if not root.exists():
        return output
    for symbol_dir in root.iterdir():
        if not symbol_dir.is_dir():
            continue
        symbol = symbol_from_storage_dirname(symbol_dir.name)
        if not symbol:
            continue
        for path in symbol_dir.glob("*.parquet"):
            timeframe = path.stem.lower()
            if SUPPORTED_TIMEFRAME.fullmatch(timeframe):
                output.add(SeriesKey(namespace, market_type, symbol, timeframe))
        for path in symbol_dir.glob("*_parts"):
            if not path.is_dir():
                continue
            timeframe = path.name.removesuffix("_parts").lower()
            if SUPPORTED_TIMEFRAME.fullmatch(timeframe):
                output.add(SeriesKey(namespace, market_type, symbol, timeframe))
    return output


def build_series_catalog(
    storage_root: Path,
    spot_status: dict[str, str],
    futures_status: dict[str, str],
) -> list[SeriesKey]:
    series = discover_local_series(storage_root, SPOT_NAMESPACE, "spot")
    series.update(discover_local_series(storage_root, FUTURES_NAMESPACE, "futures"))
    for symbol in DEFAULT_RESEARCH_SYMBOLS:
        for timeframe in DEFAULT_TIMEFRAMES:
            series.add(SeriesKey(SPOT_NAMESPACE, "spot", symbol, timeframe))
    for market, status in futures_status.items():
        if status == "TRADING":
            series.add(SeriesKey(FUTURES_NAMESPACE, "futures", normalize_symbol(market), "4h"))
    return sorted(series)


def _parquet_index_column(path: Path) -> str | None:
    schema = pq.read_schema(path)
    metadata = schema.metadata or {}
    pandas_meta = metadata.get(b"pandas")
    if pandas_meta:
        try:
            index_columns = json.loads(pandas_meta).get("index_columns") or []
            for column in index_columns:
                if isinstance(column, str) and column in schema.names:
                    return column
        except (TypeError, ValueError, UnicodeDecodeError):
            pass
    for candidate in ("timestamp", "__index_level_0__"):
        if candidate in schema.names:
            return candidate
    return None


def _as_utc_naive(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return stamp.to_pydatetime()


def parquet_bounds(path: Path) -> LocalBounds:
    parquet = pq.ParquetFile(path)
    column_name = _parquet_index_column(path)
    if not column_name:
        return LocalBounds(files=1)
    minima: list[datetime] = []
    maxima: list[datetime] = []
    for group_index in range(parquet.num_row_groups):
        group = parquet.metadata.row_group(group_index)
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            if column.path_in_schema != column_name:
                continue
            stats = column.statistics
            if stats is not None and stats.has_min_max:
                minima.append(_as_utc_naive(stats.min))
                maxima.append(_as_utc_naive(stats.max))
            break
    if not minima or not maxima:
        table = parquet.read(columns=[column_name])
        values = pd.to_datetime(table.column(column_name).to_pandas(), errors="coerce", utc=True)
        values = values.dropna()
        if len(values):
            minima.append(values.min().tz_localize(None).to_pydatetime())
            maxima.append(values.max().tz_localize(None).to_pydatetime())
    return LocalBounds(
        first=min(minima) if minima else None,
        last=max(maxima) if maxima else None,
        files=1,
    )


def local_bounds(storage_root: Path, key: SeriesKey) -> LocalBounds:
    first: list[datetime] = []
    last: list[datetime] = []
    file_count = 0
    for symbol_root in candidate_symbol_dirs(storage_root, key.namespace, key.symbol):
        legacy = symbol_root / f"{key.timeframe}.parquet"
        if legacy.exists():
            item = parquet_bounds(legacy)
            file_count += 1
            if item.first:
                first.append(item.first)
            if item.last:
                last.append(item.last)
        parts_dir = symbol_root / f"{key.timeframe}_parts"
        if not parts_dir.exists():
            continue
        files = sorted(parts_dir.glob("*.parquet"))
        file_count += len(files)
        edge_files = files[:1] + (files[-1:] if len(files) > 1 else [])
        for path in edge_files:
            item = parquet_bounds(path)
            if item.first:
                first.append(item.first)
            if item.last:
                last.append(item.last)
    return LocalBounds(
        first=min(first) if first else None,
        last=max(last) if last else None,
        files=file_count,
    )


def last_complete_open(now: datetime, timeframe: str) -> datetime:
    interval = timeframe_ms(timeframe)
    now_ms = int(now.timestamp() * 1000)
    offset = timeframe_alignment_offset_ms(timeframe)
    open_ms = ((now_ms - offset) // interval) * interval + offset - interval
    return datetime.fromtimestamp(open_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)


def target_start_for(key: SeriesKey, end: datetime, args: argparse.Namespace) -> datetime:
    interval = timeframe_ms(key.timeframe)
    if key.market_type == "futures":
        return end - timedelta(milliseconds=interval * max(0, int(args.futures_bars) - 1))
    unit = key.timeframe[-1]
    if unit == "s":
        days = int(args.second_days)
    elif unit == "m":
        days = int(args.minute_days)
    else:
        days = int(args.htf_days)
    return end - timedelta(days=max(1, days))


def source_available_start(
    key: SeriesKey,
    requested_start: datetime,
    bounds: LocalBounds,
    limiter: RequestRateLimiter,
) -> tuple[datetime, datetime | None]:
    """Resolve a lookback against the exchange's actual listing history."""
    interval = timedelta(milliseconds=timeframe_ms(key.timeframe))
    if bounds.first is not None and bounds.first <= requested_start + interval:
        return requested_start, None

    source_timeframe = "1s" if is_derived_seconds_timeframe(key.timeframe) else key.timeframe
    path = "/api/v3/klines" if key.market_type == "spot" else "/fapi/v1/klines"
    hosts = SPOT_HOSTS if key.market_type == "spot" else FUTURES_HOSTS
    payload = request_json(
        hosts,
        path,
        params={
            "symbol": raw_symbol(key.symbol),
            "interval": source_timeframe,
            "startTime": 0,
            "limit": 1,
        },
        limiter=limiter,
    )
    if not payload:
        return requested_start, None
    available = datetime.fromtimestamp(int(payload[0][0]) / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    return max(requested_start, available), available


def plan_fetch_windows(
    bounds: LocalBounds,
    target_start: datetime,
    target_end: datetime,
    timeframe: str,
    overlap_bars: int,
) -> list[FetchWindow]:
    interval = timeframe_ms(timeframe)
    start_ms = int(target_start.replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(target_end.replace(tzinfo=timezone.utc).timestamp() * 1000)
    if bounds.first is None or bounds.last is None:
        return [FetchWindow(start_ms, end_ms, "missing_full_window")]
    first_ms = int(bounds.first.replace(tzinfo=timezone.utc).timestamp() * 1000)
    last_ms = int(bounds.last.replace(tzinfo=timezone.utc).timestamp() * 1000)
    windows: list[FetchWindow] = []
    if first_ms > start_ms + interval:
        windows.append(FetchWindow(start_ms, min(end_ms, first_ms - interval), "missing_head"))
    if last_ms < end_ms:
        tail_start = max(start_ms, last_ms - interval * max(1, overlap_bars))
        windows.append(FetchWindow(tail_start, end_ms, "stale_tail"))
    return [window for window in windows if window.start_ms <= window.end_open_ms]


def fetch_window(
    key: SeriesKey,
    window: FetchWindow,
    limiter: RequestRateLimiter,
) -> list[list[Any]]:
    interval = timeframe_ms(key.timeframe)
    path = "/api/v3/klines" if key.market_type == "spot" else "/fapi/v1/klines"
    hosts = SPOT_HOSTS if key.market_type == "spot" else FUTURES_HOSTS
    cursor = int(window.start_ms)
    rows: dict[int, list[Any]] = {}
    while cursor <= window.end_open_ms:
        payload = request_json(
            hosts,
            path,
            params={
                "symbol": raw_symbol(key.symbol),
                "interval": key.timeframe,
                "startTime": cursor,
                "endTime": window.end_open_ms + interval - 1,
                "limit": 1000,
            },
            limiter=limiter,
        )
        if not payload:
            break
        for item in payload:
            if len(item) < 6:
                continue
            open_ms = int(item[0])
            if window.start_ms <= open_ms <= window.end_open_ms:
                rows[open_ms] = item
        last_ms = int(payload[-1][0])
        if last_ms < cursor:
            break
        next_cursor = last_ms + interval
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(payload) < 1000:
            break
    return [rows[key_ms] for key_ms in sorted(rows)]


def validate_rows(rows: Iterable[list[Any]], timeframe: str) -> list[list[Any]]:
    interval = timeframe_ms(timeframe)
    valid: list[list[Any]] = []
    seen: set[int] = set()
    for row in rows:
        open_ms = int(row[0])
        if open_ms in seen:
            continue
        values = [float(row[index]) for index in range(1, 6)]
        open_, high, low, close, volume = values
        if not all(math.isfinite(value) for value in values):
            raise ValueError(f"non-finite OHLCV at {open_ms}")
        if min(open_, high, low, close) <= 0 or volume < 0:
            raise ValueError(f"invalid OHLCV range at {open_ms}")
        if high < max(open_, low, close) or low > min(open_, high, close):
            raise ValueError(f"inconsistent OHLC at {open_ms}")
        offset = timeframe_alignment_offset_ms(timeframe)
        if (open_ms - offset) % interval != 0:
            raise ValueError(f"misaligned open time {open_ms} for {timeframe}")
        seen.add(open_ms)
        valid.append(row)
    return sorted(valid, key=lambda item: int(item[0]))


def rows_to_klines(key: SeriesKey, rows: Sequence[list[Any]]) -> list[Kline]:
    return [
        Kline(
            exchange=key.namespace,
            symbol=key.symbol,
            timeframe=key.timeframe,
            timestamp=datetime.fromtimestamp(int(row[0]) / 1000.0, tz=timezone.utc),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )
        for row in rows
    ]


async def save_klines_compact(key: SeriesKey, klines: Sequence[Kline]) -> str:
    """Atomically merge ordinary bars into one compact file per series.

    Existing daily partitions remain readable and are not rewritten.  Native
    sub-minute data keeps the daily-partition writer because those files can be
    materially larger and only the most recent day is refreshed here.
    """
    if not klines:
        return ""
    if key.timeframe.endswith("s"):
        return await data_storage.save_klines_to_parquet(
            list(klines), key.namespace, key.symbol, key.timeframe
        )

    rows = [
        {
            "timestamp": item.timestamp,
            "open": item.open,
            "high": item.high,
            "low": item.low,
            "close": item.close,
            "volume": item.volume,
        }
        for item in klines
    ]
    incoming = pd.DataFrame(rows)
    incoming["timestamp"] = pd.to_datetime(incoming["timestamp"], errors="coerce", utc=True)
    incoming = incoming.dropna(subset=["timestamp"]).set_index("timestamp")
    incoming.index = incoming.index.tz_localize(None)
    incoming = incoming[~incoming.index.duplicated(keep="last")].sort_index()
    target = canonical_symbol_dir(Path(data_storage.storage_path), key.namespace, key.symbol) / f"{key.timeframe}.parquet"

    def _merge_write() -> str:
        target.parent.mkdir(parents=True, exist_ok=True)
        with parquet_partition_lock(target, timeout_seconds=60):
            combined = incoming
            if target.exists():
                existing = pd.read_parquet(target)
                existing.index = pd.to_datetime(existing.index, errors="coerce", utc=True).tz_localize(None)
                existing = existing[~existing.index.isna()]
                combined = pd.concat([existing, incoming])
            combined = combined[~combined.index.duplicated(keep="last")].sort_index()
            temp = target.with_name(f"{target.name}.{os.getpid()}.{threading.get_ident()}.tmp")
            try:
                pq.write_table(
                    pa.Table.from_pandas(combined),
                    str(temp),
                    compression="zstd",
                    compression_level=6,
                )
                os.replace(temp, target)
            finally:
                if temp.exists():
                    temp.unlink(missing_ok=True)
        return str(target)

    return await asyncio.to_thread(_merge_write)


def source_status_for(
    key: SeriesKey,
    spot_status: dict[str, str],
    futures_status: dict[str, str],
) -> str:
    status = (spot_status if key.market_type == "spot" else futures_status).get(raw_symbol(key.symbol))
    return status or "NOT_LISTED"


def is_complete(bounds: LocalBounds, start: datetime, end: datetime, timeframe: str) -> tuple[bool, bool]:
    interval = timedelta(milliseconds=timeframe_ms(timeframe))
    head = bool(bounds.first is not None and bounds.first <= start + interval)
    tail = bool(bounds.last is not None and bounds.last >= end - interval)
    return head, tail


def fetch_key_windows(
    key: SeriesKey,
    windows: Sequence[FetchWindow],
    limiter: RequestRateLimiter,
) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for window in windows:
        rows.extend(fetch_window(key, window, limiter))
    return validate_rows(rows, key.timeframe)


def is_derived_seconds_timeframe(timeframe: str) -> bool:
    text = str(timeframe or "").lower()
    return text.endswith("s") and text != "1s"


async def derive_seconds_from_one_second(
    key: SeriesKey,
    windows: Sequence[FetchWindow],
) -> list[Kline]:
    """Build local sub-minute aggregates from the canonical Binance 1s bars."""
    seconds = int(key.timeframe[:-1])
    frames: list[pd.DataFrame] = []
    for window in windows:
        start = datetime.fromtimestamp(window.start_ms / 1000.0, tz=timezone.utc)
        end = datetime.fromtimestamp(
            (window.end_open_ms + timeframe_ms(key.timeframe) - 1) / 1000.0,
            tz=timezone.utc,
        )
        source = await data_storage.load_klines_from_parquet(
            exchange=key.namespace,
            symbol=key.symbol,
            timeframe="1s",
            start_time=start,
            end_time=end,
        )
        if source is None or source.empty:
            raise RuntimeError(f"missing 1s source for derived {key.symbol} {key.timeframe}")
        source = source[["open", "high", "low", "close", "volume"]].copy()
        source.index = pd.to_datetime(source.index, errors="coerce", utc=True).tz_localize(None)
        aggregate = source.resample(
            f"{seconds}s",
            origin="epoch",
            label="left",
            closed="left",
        ).agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        aggregate = aggregate.dropna(subset=["open", "high", "low", "close"])
        lower = pd.Timestamp(start).tz_convert("UTC").tz_localize(None)
        upper = datetime.fromtimestamp(window.end_open_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)
        aggregate = aggregate[(aggregate.index >= lower) & (aggregate.index <= upper)]
        frames.append(aggregate)
    if not frames:
        return []
    combined = pd.concat(frames).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    return [
        Kline(
            exchange=key.namespace,
            symbol=key.symbol,
            timeframe=key.timeframe,
            timestamp=pd.Timestamp(stamp).tz_localize("UTC").to_pydatetime(),
            open=float(row.open),
            high=float(row.high),
            low=float(row.low),
            close=float(row.close),
            volume=float(row.volume),
        )
        for stamp, row in combined.iterrows()
    ]


async def refresh_series(
    keys: Sequence[SeriesKey],
    spot_status: dict[str, str],
    futures_status: dict[str, str],
    args: argparse.Namespace,
    limiter: RequestRateLimiter,
) -> list[SeriesResult]:
    storage_root = Path(data_storage.storage_path)
    planned: list[tuple[SeriesKey, list[FetchWindow], SeriesResult]] = []
    now = utc_now()
    for key in keys:
        end = last_complete_open(now, key.timeframe)
        requested_start = target_start_for(key, end, args)
        before = local_bounds(storage_root, key)
        status = source_status_for(key, spot_status, futures_status)
        start = requested_start
        available_from: datetime | None = None
        availability_error: Exception | None = None
        if status == "TRADING":
            try:
                start, available_from = await asyncio.to_thread(
                    source_available_start,
                    key,
                    requested_start,
                    before,
                    limiter,
                )
            except Exception as exc:  # noqa: BLE001 - metadata failure is fail-closed
                availability_error = exc
        result = SeriesResult(
            namespace=key.namespace,
            market_type=key.market_type,
            symbol=key.symbol,
            timeframe=key.timeframe,
            source_status=status,
            target_start_utc=start.replace(tzinfo=timezone.utc).isoformat(),
            target_end_utc=end.replace(tzinfo=timezone.utc).isoformat(),
            before_first_utc=before.first.replace(tzinfo=timezone.utc).isoformat() if before.first else None,
            before_last_utc=before.last.replace(tzinfo=timezone.utc).isoformat() if before.last else None,
            requested_start_utc=requested_start.replace(tzinfo=timezone.utc).isoformat(),
            source_available_from_utc=(
                available_from.replace(tzinfo=timezone.utc).isoformat() if available_from else None
            ),
            file_count=before.files,
        )
        if availability_error is not None:
            result.status = "failed"
            result.error = (
                f"source availability lookup failed: {type(availability_error).__name__}: "
                f"{availability_error}"
            )
            planned.append((key, [], result))
            continue
        if status != "TRADING":
            result.status = "upstream_unavailable"
            result.after_first_utc = result.before_first_utc
            result.after_last_utc = result.before_last_utc
            result.complete_head, result.fresh_tail = is_complete(before, start, end, key.timeframe)
            planned.append((key, [], result))
            continue
        windows = plan_fetch_windows(before, start, end, key.timeframe, int(args.overlap_bars))
        result.windows = len(windows)
        if not bool(args.refresh) or not windows:
            result.status = "audit_only" if not bool(args.refresh) else "already_complete"
        planned.append((key, windows, result))

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=max(1, int(args.max_workers)))
    async def _run_fetch(
        key: SeriesKey,
        windows: Sequence[FetchWindow],
        result: SeriesResult,
    ) -> tuple[SeriesKey, SeriesResult, list[list[Any]] | None, Exception | None]:
        try:
            rows = await loop.run_in_executor(executor, fetch_key_windows, key, windows, limiter)
            return key, result, rows, None
        except Exception as exc:  # noqa: BLE001 - returned to the fail-closed catalog
            return key, result, None, exc

    tasks: list[asyncio.Task[tuple[SeriesKey, SeriesResult, list[list[Any]] | None, Exception | None]]] = []
    try:
        if args.refresh:
            for key, windows, result in planned:
                if not windows or result.source_status != "TRADING":
                    continue
                if is_derived_seconds_timeframe(key.timeframe):
                    continue
                tasks.append(asyncio.create_task(_run_fetch(key, windows, result)))
            completed = 0
            for task in asyncio.as_completed(tasks):
                key, result, rows, error = await task
                if error is None:
                    assert rows is not None
                    result.downloaded_rows = len(rows)
                    if rows:
                        await save_klines_compact(key, rows_to_klines(key, rows))
                    result.status = "refreshed" if rows else "source_returned_no_rows"
                else:
                    result.status = "failed"
                    result.error = f"{type(error).__name__}: {error}"
                completed += 1
                if completed % 50 == 0:
                    logger.info("Completed {}/{} planned downloads", completed, len(tasks))

            derived = [
                (key, windows, result)
                for key, windows, result in planned
                if windows
                and result.source_status == "TRADING"
                and is_derived_seconds_timeframe(key.timeframe)
            ]
            for key, windows, result in derived:
                try:
                    klines = await derive_seconds_from_one_second(key, windows)
                    result.downloaded_rows = len(klines)
                    if klines:
                        await save_klines_compact(key, klines)
                    result.status = "derived" if klines else "source_returned_no_rows"
                except Exception as exc:  # noqa: BLE001 - retained in fail-closed catalog
                    result.status = "failed"
                    result.error = f"{type(exc).__name__}: {exc}"
    finally:
        executor.shutdown(wait=True, cancel_futures=False)

    output: list[SeriesResult] = []
    for key, _, result in planned:
        after = local_bounds(storage_root, key)
        start = pd.Timestamp(result.target_start_utc).tz_convert("UTC").tz_localize(None).to_pydatetime()
        end = pd.Timestamp(result.target_end_utc).tz_convert("UTC").tz_localize(None).to_pydatetime()
        result.after_first_utc = after.first.replace(tzinfo=timezone.utc).isoformat() if after.first else None
        result.after_last_utc = after.last.replace(tzinfo=timezone.utc).isoformat() if after.last else None
        result.file_count = after.files
        result.complete_head, result.fresh_tail = is_complete(after, start, end, key.timeframe)
        if result.status in {"refreshed", "derived", "source_returned_no_rows", "already_complete", "audit_only"}:
            if result.complete_head and result.fresh_tail:
                result.status = "complete"
            elif result.status != "audit_only":
                result.status = "incomplete"
        output.append(result)
    return output


CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS market_data_catalog (
    exchange_namespace TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    source_status TEXT NOT NULL,
    storage_backend TEXT NOT NULL DEFAULT 'parquet',
    requested_start_utc TEXT,
    source_available_from_utc TEXT,
    target_start_utc TEXT,
    target_end_utc TEXT,
    first_timestamp_utc TEXT,
    last_timestamp_utc TEXT,
    file_count INTEGER NOT NULL DEFAULT 0,
    downloaded_rows INTEGER NOT NULL DEFAULT 0,
    complete_head INTEGER NOT NULL DEFAULT 0,
    fresh_tail INTEGER NOT NULL DEFAULT 0,
    refresh_status TEXT NOT NULL,
    last_error TEXT,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (exchange_namespace, symbol, timeframe)
)
"""


CATALOG_UPSERT = """
INSERT INTO market_data_catalog (
    exchange_namespace, market_type, symbol, timeframe, source_status,
    storage_backend, requested_start_utc, source_available_from_utc,
    target_start_utc, target_end_utc, first_timestamp_utc,
    last_timestamp_utc, file_count, downloaded_rows, complete_head,
    fresh_tail, refresh_status, last_error, updated_at_utc
) VALUES (?, ?, ?, ?, ?, 'parquet', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(exchange_namespace, symbol, timeframe) DO UPDATE SET
    market_type=excluded.market_type,
    source_status=excluded.source_status,
    storage_backend=excluded.storage_backend,
    requested_start_utc=excluded.requested_start_utc,
    source_available_from_utc=excluded.source_available_from_utc,
    target_start_utc=excluded.target_start_utc,
    target_end_utc=excluded.target_end_utc,
    first_timestamp_utc=excluded.first_timestamp_utc,
    last_timestamp_utc=excluded.last_timestamp_utc,
    file_count=excluded.file_count,
    downloaded_rows=excluded.downloaded_rows,
    complete_head=excluded.complete_head,
    fresh_tail=excluded.fresh_tail,
    refresh_status=excluded.refresh_status,
    last_error=excluded.last_error,
    updated_at_utc=excluded.updated_at_utc
"""


def update_sqlite_catalog(database: Path, results: Sequence[SeriesResult], updated_at: str) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(database), timeout=30)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute(CATALOG_DDL)
        existing_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(market_data_catalog)")
        }
        for column in ("requested_start_utc", "source_available_from_utc"):
            if column not in existing_columns:
                connection.execute(f"ALTER TABLE market_data_catalog ADD COLUMN {column} TEXT")
        rows = [
            (
                item.namespace, item.market_type, item.symbol, item.timeframe,
                item.source_status, item.requested_start_utc, item.source_available_from_utc,
                item.target_start_utc, item.target_end_utc,
                item.after_first_utc, item.after_last_utc, int(item.file_count),
                int(item.downloaded_rows), int(item.complete_head), int(item.fresh_tail),
                item.status, item.error, updated_at,
            )
            for item in results
        ]
        connection.executemany(CATALOG_UPSERT, rows)
        connection.commit()
    finally:
        connection.close()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, path)


def build_summary(results: Sequence[SeriesResult], started_at: str, finished_at: str) -> dict[str, Any]:
    actionable = [item for item in results if item.source_status == "TRADING"]
    complete = [item for item in actionable if item.complete_head and item.fresh_tail]
    failed = [item for item in actionable if item.status == "failed"]
    unavailable = [item for item in results if item.source_status != "TRADING"]
    namespaces: dict[str, dict[str, Any]] = {}
    for namespace in sorted({item.namespace for item in results}):
        group = [item for item in results if item.namespace == namespace]
        active_group = [item for item in group if item.source_status == "TRADING"]
        complete_group = [item for item in active_group if item.complete_head and item.fresh_tail]
        namespaces[namespace] = {
            "series": len(group),
            "active_series": len(active_group),
            "complete_active_series": len(complete_group),
            "complete_active_rate": len(complete_group) / len(active_group) if active_group else 1.0,
            "upstream_unavailable_series": len(group) - len(active_group),
            "downloaded_rows": sum(item.downloaded_rows for item in group),
        }
    return {
        "schema_version": "1.0",
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "canonical_bar_storage": "partitioned_parquet",
        "sqlite_role": "coverage_catalog_only; legacy klines is non-authoritative",
        "spot_namespace": SPOT_NAMESPACE,
        "futures_namespace": FUTURES_NAMESPACE,
        "series_total": len(results),
        "active_series": len(actionable),
        "complete_active_series": len(complete),
        "complete_active_rate": len(complete) / len(actionable) if actionable else 1.0,
        "upstream_unavailable_series": len(unavailable),
        "failed_active_series": len(failed),
        "downloaded_rows": sum(item.downloaded_rows for item in results),
        "source_limited_series": sum(
            1
            for item in actionable
            if item.source_available_from_utc
            and item.requested_start_utc
            and item.source_available_from_utc > item.requested_start_utc
        ),
        "namespaces": namespaces,
        "failure_reasons": [asdict(item) for item in failed],
        "upstream_unavailable": [
            {
                "namespace": item.namespace,
                "market_type": item.market_type,
                "symbol": item.symbol,
                "timeframe": item.timeframe,
                "source_status": item.source_status,
                "last_timestamp_utc": item.after_last_utc,
            }
            for item in unavailable
        ],
        "series": [asdict(item) for item in results],
        "read_only_trading": True,
        "order_modules_imported": False,
        "strategy_rules_changed": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="Download missing heads and stale tails; default is audit only.")
    parser.add_argument("--minute-days", type=int, default=30)
    parser.add_argument("--htf-days", type=int, default=365)
    parser.add_argument("--second-days", type=int, default=1)
    parser.add_argument("--futures-bars", type=int, default=300)
    parser.add_argument("--overlap-bars", type=int, default=48)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--max-requests-per-second", type=float, default=5.0)
    parser.add_argument(
        "--summary-out",
        type=Path,
        default=ROOT / "data" / "research" / "binance_data_catalog_latest.json",
    )
    parser.add_argument(
        "--sqlite-db",
        type=Path,
        default=ROOT / "data" / "crypto_trading.db",
    )
    parser.add_argument("--no-sqlite-catalog", action="store_true")
    parser.add_argument("--minimum-complete-rate", type=float, default=0.98)
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    started_at = utc_now().isoformat()
    limiter = RequestRateLimiter(float(args.max_requests_per_second))
    spot_status, futures_status = await asyncio.to_thread(load_market_statuses, limiter)
    keys = build_series_catalog(Path(data_storage.storage_path), spot_status, futures_status)
    logger.info(
        "Catalog contains {} series (spot statuses={}, futures statuses={})",
        len(keys), len(spot_status), len(futures_status),
    )
    results = await refresh_series(keys, spot_status, futures_status, args, limiter)
    finished_at = utc_now().isoformat()
    summary = build_summary(results, started_at, finished_at)
    write_json_atomic(Path(args.summary_out), summary)
    if not args.no_sqlite_catalog:
        update_sqlite_catalog(Path(args.sqlite_db), results, finished_at)
    print(json.dumps({key: value for key, value in summary.items() if key not in {"series", "upstream_unavailable", "failure_reasons"}}, ensure_ascii=False, indent=2))
    if summary["complete_active_rate"] < float(args.minimum_complete_rate):
        return 2
    if summary["failed_active_series"]:
        return 3
    return 0


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
