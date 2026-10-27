"""
数据存储管理模块
支持SQLite、Parquet、Redis等多种存储方式
"""
import json
import os
import asyncio
import time
from contextlib import ExitStack
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from uuid import uuid4
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
import redis.asyncio as redis
from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config.settings import settings
from config.database import (
    async_session_maker,
    Kline as KlineModel,
    init_db,
)
from core.exchanges import Kline
from core.data.path_utils import candidate_symbol_dirs, canonical_symbol_dir
from core.data.parquet_lock import parquet_partition_lock, partition_lock_path
from core.data.kline_partitions import (
    DAY,
    daily_part_path,
    legacy_pack_path,
    list_part_files,
    newest_first,
    overlapping_part_files,
    partition_granularity,
    partition_stems,
    read_order,
    rows_match_partition,
    series_lock_target,
    stem_period,
)
from core.data.kline_venue import SPOT, require_store_venue


PARQUET_WRITE_LOCK_TIMEOUT_SECONDS = float(
    os.getenv("PARQUET_WRITE_LOCK_TIMEOUT_SECONDS", "30")
)

# Shared with the explicit historical audit/repair tool. Naive pandas indexes
# alone do not establish a timezone; only a verified source may set this marker.
CTS_TIMESTAMP_TIMEZONE = "cts_timestamp_timezone"
CTS_TIMESTAMP_SCHEMA = "cts_timestamp_schema"
CTS_TIMESTAMP_ORIGIN = "cts_timestamp_origin"


def _has_utc_timestamp_metadata(frame: pd.DataFrame) -> bool:
    return (frame.attrs.get(CTS_TIMESTAMP_TIMEZONE) == "UTC"
            and frame.attrs.get(CTS_TIMESTAMP_SCHEMA) == 1)


def prepare_utc_kline_frame(frame: pd.DataFrame, *, origin: str = "writer",
                            timestamp_timezone: Optional[str] = None) -> pd.DataFrame:
    """Validate a writer's timestamps and explicitly mark its UTC-naive output.

    Connectors must supply aware datetimes. A caller that already proved a naive
    frame is UTC may declare ``timestamp_timezone='UTC'``; local wall clock is
    never guessed from its date or the host timezone. This function does not
    certify an old unmarked file merely because it is being read or merged.
    """
    if frame is None:
        raise ValueError("UTC writer requires a dataframe")
    out = frame.copy()
    stamps = [pd.Timestamp(value) for value in out.index]
    if any(pd.isna(stamp) for stamp in stamps):
        raise ValueError("Kline writer received an invalid timestamp")
    declared_utc = timestamp_timezone == "UTC" or _has_utc_timestamp_metadata(out)
    if any(stamp.tzinfo is None for stamp in stamps) and not declared_utc:
        raise ValueError("Kline writer requires timezone-aware timestamps or an explicit UTC declaration; naive local time is ambiguous")
    if timestamp_timezone not in (None, "UTC"):
        raise ValueError("Local timestamps must be explicitly localized and converted to UTC before writing")
    index = pd.to_datetime(stamps, utc=True).tz_localize(None)
    if index.has_duplicates:
        raise ValueError("Kline writer received duplicate timestamps; audit the source before merging")
    now_utc = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    if len(index) and index.max() > now_utc + pd.Timedelta(minutes=2):
        raise ValueError("Kline writer received future UTC timestamps; audit the source instead of guessing a timezone offset")
    out.index = pd.DatetimeIndex(index, name=frame.index.name)
    out.attrs.update({CTS_TIMESTAMP_TIMEZONE: "UTC", CTS_TIMESTAMP_SCHEMA: 1,
                      CTS_TIMESTAMP_ORIGIN: origin, "cts_timestamp_status": "verified_utc"})
    return out.sort_index()


def _quarantine_corrupted_parquet(file_path: Path, error: Exception) -> None:
    text = str(error or "").lower()
    markers = (
        "parquet magic bytes not found",
        "not a parquet file",
        "not a parquet",
    )
    if not any(marker in text for marker in markers):
        return
    if not file_path.exists():
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = file_path.with_name(f"{file_path.stem}.corrupt_{ts}{file_path.suffix}")
    try:
        file_path.rename(target)
        logger.warning(f"Quarantined corrupted parquet: {file_path} -> {target}")
    except Exception as rename_err:
        logger.warning(f"Failed to quarantine corrupted parquet {file_path}: {rename_err}")


def _normalize_parquet_boundary(dt: Optional[datetime]) -> Optional[datetime]:
    """Normalize tz-aware boundaries to the UTC-naive format used by parquet indexes."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _parquet_index_column(file_path: Path) -> Optional[str]:
    """Find the physical timestamp index column used by a pandas parquet file."""

    schema = pq.read_schema(file_path)
    metadata = schema.metadata or {}
    pandas_metadata = metadata.get(b"pandas")
    if pandas_metadata:
        try:
            index_columns = json.loads(pandas_metadata).get("index_columns") or []
            for item in index_columns:
                if isinstance(item, str) and item in schema.names:
                    return item
        except (TypeError, ValueError, UnicodeDecodeError):
            pass
    for candidate in ("timestamp", "__index_level_0__"):
        if candidate in schema.names:
            return candidate
    return None


def _read_parquet_frame(
    file_path: Path,
    *,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    """Read a parquet frame, pruning legacy monolith row groups when bounded."""

    if start_time is None and end_time is None:
        return pd.read_parquet(file_path)
    index_column = _parquet_index_column(file_path)
    if not index_column:
        logger.warning(
            f"Parquet file {file_path} has no discoverable timestamp index; "
            "falling back to a full compatibility read"
        )
        return pd.read_parquet(file_path)
    filters = []
    if start_time is not None:
        filters.append((index_column, ">=", start_time))
    if end_time is not None:
        filters.append((index_column, "<=", end_time))
    return pd.read_parquet(file_path, filters=filters)


def _parquet_max_timestamp(file_path: Path) -> Optional[datetime]:
    """Return a parquet file's latest timestamp from row-group metadata."""

    parquet_file = pq.ParquetFile(file_path)
    index_column = _parquet_index_column(file_path)
    if not index_column:
        return None
    maxima: List[pd.Timestamp] = []
    for row_group_idx in range(parquet_file.num_row_groups):
        row_group = parquet_file.metadata.row_group(row_group_idx)
        for column_idx in range(row_group.num_columns):
            column = row_group.column(column_idx)
            if column.path_in_schema != index_column:
                continue
            statistics = column.statistics
            if statistics is not None and statistics.has_min_max:
                maxima.append(pd.Timestamp(statistics.max))
            break
    if not maxima:
        # Some legacy writers omitted min/max statistics. Reading only the
        # newest row group remains bounded and avoids materializing the whole
        # monolith just to discover its final timestamp.
        if parquet_file.num_row_groups <= 0:
            return None
        table = parquet_file.read_row_group(
            parquet_file.num_row_groups - 1,
            columns=[index_column],
        )
        values = table.column(index_column).to_pandas()
        if values.empty:
            return None
        maxima.append(pd.Timestamp(values.max()))
    latest = max(maxima)
    if latest.tzinfo is not None:
        latest = latest.tz_convert("UTC").tz_localize(None)
    # A future maximum is not evidence for a particular timezone. In particular,
    # already repaired/marked UTC data must never be shifted a second time.
    return latest.to_pydatetime()


def _normalize_parquet_frame_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    normalized = df.copy()
    # utc=True coerces a mixed tz-aware/naive index uniformly (naive is read as
    # UTC, tz-aware is converted), avoiding the "cannot be converted to
    # datetime64 unless utc=True" crash when live UTC bars meet parquet bars.
    idx = pd.to_datetime(normalized.index, errors="coerce", utc=True).tz_localize(None)
    # Preserve compatibility reads of unmarked naive history, while exposing that
    # its timezone was not verified. Never infer -8h from a future maximum: old
    # CST history may be entirely in the past, and files can contain mixed eras.
    aware_index = isinstance(normalized.index, pd.DatetimeIndex) and normalized.index.tz is not None
    now_utc = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    valid = idx[~idx.isna()]
    if len(valid) and valid.max() > now_utc + pd.Timedelta(minutes=2):
        if bool(getattr(settings, "PARQUET_TZ_STRICT", False)):
            raise ValueError(
                f"Parquet index runs ahead of UTC (max={valid.max()} > "
                f"now={now_utc}); a non-UTC kline writer is still active"
            )
        logger.warning(f"Parquet index ahead of UTC (max={valid.max()} > now={now_utc}); leaving stored timestamps unchanged pending an explicit timezone audit")
    normalized.index = idx
    normalized = normalized[~normalized.index.isna()]
    if aware_index:
        normalized.attrs.update({CTS_TIMESTAMP_TIMEZONE: "UTC", CTS_TIMESTAMP_SCHEMA: 1,
                                 CTS_TIMESTAMP_ORIGIN: normalized.attrs.get(CTS_TIMESTAMP_ORIGIN, "timezone_aware_index")})
    normalized.attrs["cts_timestamp_status"] = "verified_utc" if _has_utc_timestamp_metadata(normalized) else "unverified_naive"
    return normalized.sort_index()


_CACHE_TYPE_KEY = "__cts_cache_type__"


def _json_cache_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return {_CACHE_TYPE_KEY: "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {_CACHE_TYPE_KEY: "date", "value": value.isoformat()}
    if isinstance(value, Path):
        return {_CACHE_TYPE_KEY: "path", "value": str(value)}
    if isinstance(value, set):
        return list(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "tolist"):
        return value.tolist()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable for cache")


def _json_cache_object_hook(value: Dict[str, Any]) -> Any:
    type_name = value.get(_CACHE_TYPE_KEY)
    if not type_name:
        return value
    raw = value.get("value")
    try:
        if type_name == "datetime":
            return datetime.fromisoformat(str(raw))
        if type_name == "date":
            return date.fromisoformat(str(raw))
        if type_name == "path":
            return str(raw)
    except Exception:
        return value
    return value


def _cache_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=_json_cache_default)


def _cache_loads(raw: Union[str, bytes, bytearray]) -> Any:
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
    return json.loads(text, object_hook=_json_cache_object_hook)


def _coerce_cache_datetime(value: Any, default: datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return default
    return default


def _cache_is_expired(expires_at: Any) -> bool:
    expiry = _coerce_cache_datetime(expires_at, datetime.max.replace(tzinfo=timezone.utc))
    now = datetime.now(timezone.utc) if expiry.tzinfo else datetime.now()
    return expiry < now


def _quarantine_unsafe_cache_file(cache_file: Path, reason: Union[Exception, str]) -> None:
    if not cache_file.exists():
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = cache_file.with_name(f"{cache_file.stem}.unsafe_{ts}{cache_file.suffix}")
    try:
        cache_file.rename(target)
        logger.warning(f"Quarantined unsafe cache file {cache_file}: {reason}")
    except Exception as exc:
        logger.warning(f"Failed to quarantine unsafe cache file {cache_file}: {exc}")


def _heal_local_existing_against_utc(
    existing_df: pd.DataFrame, incoming_df: pd.DataFrame
) -> pd.DataFrame:
    """Validate a merge boundary without silently migrating unknown history.

    The historical name is retained for callers. Partial OHLC matches, flat
    prices, and date age cannot certify a whole file or justify an index shift.
    Unmarked naive history is writable only when authoritative UTC incoming
    bars cover every existing row and its OHLCV agrees in full. Other cases
    require the explicit, backed-up audit/repair workflow.
    """
    if existing_df is None or existing_df.empty:
        return existing_df
    if pd.to_datetime(existing_df.index, errors="coerce", utc=True).isna().any():
        raise ValueError("Invalid stored kline timestamps require an explicit audit; refusing to discard rows")
    existing = _normalize_parquet_frame_index(existing_df)
    if incoming_df is None or incoming_df.empty:
        if _has_utc_timestamp_metadata(existing):
            return existing
        raise ValueError("Unverified parquet timezone: empty incoming bars cannot certify stored history; run an explicit timezone audit/repair before merging")
    incoming = _normalize_parquet_frame_index(incoming_df)
    if existing.index.has_duplicates or incoming.index.has_duplicates:
        raise ValueError("Duplicate kline timestamps require an explicit timezone/data audit; refusing to discard rows")
    if not _has_utc_timestamp_metadata(incoming):
        raise ValueError("Incoming bars must have verified UTC provenance before merging")
    if _has_utc_timestamp_metadata(existing):
        return existing
    cols = ["open", "high", "low", "close", "volume"]
    complete = (all(c in existing.columns and c in incoming.columns for c in cols)
                and existing.index.isin(incoming.index).all())
    if complete:
        authoritative = incoming.loc[existing.index, cols]
        complete = bool((existing[cols].to_numpy() == authoritative.to_numpy()).all())
        # Constant/repeating candles are not sufficient evidence for UTC labels.
        complete = complete and len(existing[["open", "high", "low", "close"]].drop_duplicates()) >= 3
    if complete:
        # Even a changing series can repeat every eight hours. When the UTC
        # source covers a competing local-clock interpretation in full, that
        # second complete match makes the timezone evidence ambiguous. This is
        # a rejection check only: writers never choose or apply an offset.
        for hours in (-8, 8):
            shifted_index = existing.index + pd.Timedelta(hours=hours)
            if shifted_index.isin(incoming.index).all():
                competing = incoming.loc[shifted_index, cols]
                if bool((existing[cols].to_numpy() == competing.to_numpy()).all()):
                    raise ValueError("Unverified parquet timezone: complete UTC and eight-hour-shifted OHLCV matches are ambiguous; run an explicit timezone audit/repair before merging")
    if not complete:
        raise ValueError("Unverified parquet timezone: incoming UTC bars do not uniquely verify every existing OHLCV row; run an explicit timezone audit/repair before merging")
    verified = existing.copy()
    verified.attrs.update({CTS_TIMESTAMP_TIMEZONE: "UTC", CTS_TIMESTAMP_SCHEMA: 1,
                           CTS_TIMESTAMP_ORIGIN: "verified_complete_utc_overlap", "cts_timestamp_status": "verified_utc"})
    return verified


def write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    """Write a kline part through a unique temp file and os.replace.

    Callers hold the partition's lock for the whole read-modify-write; the swap
    keeps readers from observing a partial file. Windows refuses the swap while
    a reader has the target open, so it is retried briefly.
    """
    table = pa.Table.from_pandas(frame)
    tmp_path = path.with_name(f"{path.name}.{uuid4().hex}.tmp")
    try:
        pq.write_table(table, str(tmp_path), compression="zstd", compression_level=9)
        for attempt in range(6):
            try:
                os.replace(tmp_path, path)
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.2 * (attempt + 1))
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise


def discard_partition_lock_file(partition_path: Path) -> None:
    """Delete the lock file of a daily part that has just been removed (Windows only).

    Windows refuses to delete a file another process has open, so a waiter can
    never be left holding a deleted lock (the race core.data.parquet_lock warns
    about); a refused delete leaves the lock file in place. Elsewhere lock files
    are kept.
    """
    if os.name != "nt":
        return
    try:
        partition_lock_path(partition_path).unlink()
    except OSError:
        pass


def _merge_into_part(part_path: Path, rows: pd.DataFrame, *,
                     absorbed: List[pd.DataFrame] = ()) -> bool:
    """Merge UTC rows into one part file under the caller's lock; True if it was rewritten.

    Stored history passes the shared UTC guard first. ``absorbed`` are verified
    daily frames being folded into a month/year file; they rank between the
    stored rows and the incoming ones.
    """
    incoming_df = _normalize_parquet_frame_index(rows)
    existing_df: Optional[pd.DataFrame] = None
    existing_was_verified = False
    sources: List[pd.DataFrame] = []
    if part_path.exists():
        try:
            existing_df = pd.read_parquet(part_path)
            existing_was_verified = _has_utc_timestamp_metadata(existing_df)
            existing_df = _heal_local_existing_against_utc(
                existing_df, incoming_df
            )
        except Exception as e:
            logger.warning(f"Failed to merge partition file {part_path}: {e}")
            # Never replace unknown/corrupt history with only the
            # latest batch after a failed merge validation.
            raise
        sources.append(existing_df)
    sources.extend(absorbed)
    sources.append(incoming_df)
    merged_df = pd.concat(sources)
    merged_df = merged_df[~merged_df.index.duplicated(keep="last")]
    merged_df = merged_df.sort_index()
    merged_df.attrs = {**(existing_df.attrs if existing_df is not None else {}), **incoming_df.attrs}
    if not rows_match_partition(merged_df.index, part_path):
        raise ValueError("Stored rows violate UTC partition date; explicit repair required")
    if (not absorbed and existing_df is not None and merged_df.equals(existing_df)
            and existing_was_verified):
        return False
    write_parquet_atomic(merged_df, part_path)
    return True


def _save_period_parts(parts_dir: Path, df: pd.DataFrame, granularity: str) -> List[Path]:
    """Merge UTC rows into month/year part files, folding in the old-layout days they touch.

    A verified daily file for a day being written is absorbed into the period
    file and removed, so each day lives in one file. An unverified daily file
    keeps receiving its own day through the UTC guard, as before this layout:
    it is refused unless the incoming bars verify every stored row. A day held
    in the period's legacy pack passes the same guard against that day's rows;
    once verified it moves into the period file.
    """
    written: List[Path] = []
    absorbed_paths: List[Path] = []
    with parquet_partition_lock(
        series_lock_target(parts_dir),
        timeout_seconds=PARQUET_WRITE_LOCK_TIMEOUT_SECONDS,
    ):
        for stem, period_df in df.groupby(partition_stems(df.index, granularity), sort=True):
            target = parts_dir / f"{stem}.parquet"
            days = pd.Index(period_df.index.date)
            with ExitStack() as day_locks:
                absorbed: List[tuple] = []
                daily_days = []
                for part_day in sorted(set(days)):
                    day_path = daily_part_path(parts_dir, part_day)
                    if not day_path.exists():
                        continue
                    day_locks.enter_context(parquet_partition_lock(
                        day_path,
                        timeout_seconds=PARQUET_WRITE_LOCK_TIMEOUT_SECONDS,
                    ))
                    if not day_path.exists():
                        continue
                    day_rows = period_df[days == part_day]
                    try:
                        stored = pd.read_parquet(day_path)
                    except Exception:
                        stored = None  # the legacy merge below reports and refuses it
                    if stored is not None and _has_utc_timestamp_metadata(stored):
                        absorbed.append((day_path, _heal_local_existing_against_utc(stored, day_rows)))
                        continue
                    if _merge_into_part(day_path, day_rows):
                        written.append(day_path)
                    daily_days.append(part_day)
                pack_path, pack, pack_days, taken = legacy_pack_path(parts_dir, stem), None, None, []
                if pack_path.exists():
                    pack_last = _parquet_max_timestamp(pack_path)
                    if pack_last is None or min(days) <= pack_last.date():
                        pack = pd.read_parquet(pack_path)
                        pack_days = pd.Index(pack.index.date)
                for part_day in sorted(set(days) - set(daily_days)) if pack is not None else []:
                    in_pack = pack_days == part_day
                    if not in_pack.any():
                        continue
                    stored = pack[in_pack]
                    stored.attrs = dict(pack.attrs)
                    absorbed.append((None, _heal_local_existing_against_utc(stored, period_df[days == part_day])))
                    taken.append(part_day)
                rows = period_df[~days.isin(daily_days)]
                if rows.empty:
                    continue
                if _merge_into_part(target, rows, absorbed=[frame for _, frame in absorbed]):
                    written.append(target)
                if taken:
                    # Only after the period file holds them: a crash here
                    # leaves the day in both files, never in neither.
                    remaining = pack[~pack_days.isin(taken)]
                    if remaining.empty:
                        pack_path.unlink()
                    else:
                        remaining.attrs = dict(pack.attrs)
                        write_parquet_atomic(remaining, pack_path)
                    written.append(pack_path)
                for day_path, _ in absorbed:
                    if day_path is not None:
                        day_path.unlink()
                        absorbed_paths.append(day_path)
    for day_path in absorbed_paths:
        discard_partition_lock_file(day_path)
    return written


class DataStorage:
    """数据存储管理器"""

    def __init__(self):
        self.storage_path = settings.DATA_STORAGE_PATH
        self.cache_path = settings.CACHE_PATH
        self._redis: Optional[redis.Redis] = None
        self._db_initialized = False

    def mark_db_initialized(self) -> None:
        self._db_initialized = True

    async def initialize(self, *, ensure_db: bool = True) -> None:
        """初始化存储"""
        # 创建目录
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.cache_path.mkdir(parents=True, exist_ok=True)

        # 初始化数据库
        if ensure_db and not self._db_initialized:
            await init_db()
            self._db_initialized = True

        # 初始化Redis连接
        try:
            self._redis = redis.from_url(settings.REDIS_URL)
            await self._redis.ping()
            logger.info("Redis connected")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
            self._redis = None

        logger.info("Data storage initialized")

    async def close(self) -> None:
        """关闭存储"""
        if self._redis:
            await self._redis.close()

    # ==================== K线数据存储 ====================

    async def save_klines_to_db(self, klines: List[Kline]) -> int:
        """Save klines in one conflict-safe batch instead of flushing each bar."""
        if not klines:
            return 0

        rows = [
            {
                "exchange": kline.exchange,
                "symbol": kline.symbol,
                "timeframe": kline.timeframe,
                "timestamp": kline.timestamp,
                "open": kline.open,
                "high": kline.high,
                "low": kline.low,
                "close": kline.close,
                "volume": kline.volume,
            }
            for kline in klines
        ]

        async with async_session_maker() as session:
            dialect = session.get_bind().dialect.name
            conflict_columns = ("exchange", "symbol", "timeframe", "timestamp")
            if dialect == "sqlite":
                statement = sqlite_insert(KlineModel).values(rows).on_conflict_do_nothing(
                    index_elements=conflict_columns
                )
            elif dialect == "postgresql":
                statement = postgresql_insert(KlineModel).values(rows).on_conflict_do_nothing(
                    index_elements=conflict_columns
                )
            else:
                # The supported deployments are SQLite and PostgreSQL. Keep a
                # portable bulk path for other engines; their unique constraint
                # still protects integrity, even if duplicate retries surface.
                statement = insert(KlineModel).values(rows)
            result = await session.execute(statement)
            await session.commit()
            rowcount = result.rowcount
            return len(rows) if rowcount is None or rowcount < 0 else int(rowcount)

    async def save_klines_to_parquet(
        self,
        klines: List[Kline],
        exchange: str,
        symbol: str,
        timeframe: str,
        *,
        timestamp_timezone: Optional[str] = None,
        market_type: str = SPOT,
    ) -> str:
        """淇濆瓨K绾挎暟鎹埌Parquet鏂囦欢"""
        if not klines:
            return ""
        # ``market_type`` is where the bars came from: perp bars never go into a
        # spot namespace such as ``binance`` (see core.data.kline_venue).
        require_store_venue(exchange, symbol, timeframe, market_type)

        data = [
            {
                "timestamp": k.timestamp,
                "open": k.open,
                "high": k.high,
                "low": k.low,
                "close": k.close,
                "volume": k.volume,
            }
            for k in klines
        ]
        df = pd.DataFrame(data)
        df = prepare_utc_kline_frame(df.set_index("timestamp"), timestamp_timezone=timestamp_timezone)

        symbol_dir = canonical_symbol_dir(self.storage_path, exchange, symbol)
        parts_dir = symbol_dir / f"{timeframe}_parts"
        parts_dir.mkdir(parents=True, exist_ok=True)
        granularity = partition_granularity(timeframe)

        def _save_sync() -> List[Path]:
            if granularity != DAY:
                return _save_period_parts(parts_dir, df, granularity)
            written_parts: List[Path] = []
            grouped = df.groupby(df.index.date, sort=True)
            for part_day, day_df in grouped:
                part_path = parts_dir / f"{part_day.isoformat()}.parquet"
                with parquet_partition_lock(
                    part_path,
                    timeout_seconds=PARQUET_WRITE_LOCK_TIMEOUT_SECONDS,
                ):
                    if _merge_into_part(part_path, day_df):
                        written_parts.append(part_path)
            return written_parts

        written_parts = await asyncio.to_thread(_save_sync)
        latest_path = written_parts[-1] if written_parts else parts_dir
        logger.info(
            f"Saved {len(df)} incremental klines to {len(written_parts)} parquet parts under {parts_dir}"
        )
        return str(latest_path)

    async def load_klines_from_parquet(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Load klines from parquet file and/or partitioned parquet directory."""
        start_time = _normalize_parquet_boundary(start_time)
        end_time = _normalize_parquet_boundary(end_time)
        def _load_sync() -> pd.DataFrame:
            frames: List[pd.DataFrame] = []
            # Concatenated in this order. Part rows are arranged as the daily
            # layout concatenated them, so the unstable sort below resolves
            # duplicate timestamps (main vs parts) exactly as it always did.
            ordered: List[pd.DataFrame] = []
            for symbol_root in candidate_symbol_dirs(self.storage_path, exchange, symbol):
                file_path = symbol_root / f"{timeframe}.parquet"
                parts_dir = symbol_root / f"{timeframe}_parts"

                if file_path.exists():
                    try:
                        single_df = _read_parquet_frame(
                            file_path,
                            start_time=start_time,
                            end_time=end_time,
                        )
                        if not single_df.empty:
                            single_df = _normalize_parquet_frame_index(single_df)
                            frames.append(single_df)
                            ordered.append(single_df)
                    except Exception as e:
                        logger.warning(f"Failed to load parquet file {file_path}: {e}")
                        _quarantine_corrupted_parquet(file_path, e)

                part_files = list_part_files(parts_dir)
                if not part_files:
                    continue

                lower = upper = None
                if start_time or end_time:
                    # Prune by the day/month/year named in each file, with the
                    # one-day margin the daily layout always used.
                    lower = (pd.Timestamp(start_time).date() - timedelta(days=1)) if start_time else None
                    upper = (pd.Timestamp(end_time).date() + timedelta(days=1)) if end_time else None
                    part_files = overlapping_part_files(part_files, lower, upper)

                part_frames: List[pd.DataFrame] = []
                part_paths: List[Path] = []
                for part_file in part_files:
                    try:
                        part_df = pd.read_parquet(part_file)
                        if part_df.empty:
                            continue
                        part_df = _normalize_parquet_frame_index(part_df)
                        period = stem_period(part_file.stem)
                        if period is not None and period[0] != period[1] and (lower or upper):
                            # Keep exactly the days the daily layout would have loaded.
                            days = part_df.index.normalize()
                            if lower:
                                part_df = part_df[days >= pd.Timestamp(lower)]
                                days = part_df.index.normalize()
                            if upper:
                                part_df = part_df[days <= pd.Timestamp(upper)]
                            if part_df.empty:
                                continue
                        frames.append(part_df)
                        part_frames.append(part_df)
                        part_paths.append(part_file)
                    except Exception as e:
                        logger.warning(f"Failed to load partition file {part_file}: {e}")
                if not part_frames:
                    continue
                parts_df = pd.concat(part_frames) if len(part_frames) > 1 else part_frames[0]
                order = read_order(part_frames, part_paths)
                if len(order) != len(parts_df) or not (order[1:] > order[:-1]).all():
                    parts_df = parts_df.iloc[order]
                ordered.append(parts_df)

            if not frames:
                return pd.DataFrame()

            df = pd.concat(ordered).sort_index()
            df = df[~df.index.duplicated(keep="last")]
            # Compatibility reads must not launder an unverified monolith into
            # a certified UTC frame merely by concatenating it with new parts.
            if all(_has_utc_timestamp_metadata(frame) for frame in frames):
                df.attrs.update({CTS_TIMESTAMP_TIMEZONE: "UTC", CTS_TIMESTAMP_SCHEMA: 1,
                                 CTS_TIMESTAMP_ORIGIN: df.attrs.get(CTS_TIMESTAMP_ORIGIN, "combined_verified_utc"),
                                 "cts_timestamp_status": "verified_utc"})
            else:
                df.attrs.pop(CTS_TIMESTAMP_TIMEZONE, None)
                df.attrs.pop(CTS_TIMESTAMP_SCHEMA, None)
                df.attrs["cts_timestamp_status"] = "unverified_naive"

            if start_time:
                df = df[df.index >= start_time]
            if end_time:
                df = df[df.index <= end_time]

            return df

        return await asyncio.to_thread(_load_sync)

    async def get_latest_kline_timestamp(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> Optional[datetime]:
        """Return the newest local bar timestamp without loading bar history."""

        def _latest_sync() -> Optional[datetime]:
            candidates: List[datetime] = []
            for symbol_root in candidate_symbol_dirs(self.storage_path, exchange, symbol):
                legacy_path = symbol_root / f"{timeframe}.parquet"
                if legacy_path.exists():
                    try:
                        latest = _parquet_max_timestamp(legacy_path)
                        if latest is not None:
                            candidates.append(latest)
                    except Exception as exc:
                        logger.warning(f"Failed to inspect parquet metadata {legacy_path}: {exc}")

                parts_dir = symbol_root / f"{timeframe}_parts"
                newest_part: Optional[datetime] = None
                for part_path, last_day in newest_first(list_part_files(parts_dir)):
                    if newest_part is not None and last_day < newest_part.date():
                        break  # this file and every later one end before the newest bar found
                    try:
                        latest = _parquet_max_timestamp(part_path)
                    except Exception as exc:
                        logger.warning(f"Failed to inspect parquet metadata {part_path}: {exc}")
                        continue
                    if latest is not None and (newest_part is None or latest > newest_part):
                        newest_part = latest
                if newest_part is not None:
                    candidates.append(newest_part)
            return max(candidates) if candidates else None

        return await asyncio.to_thread(_latest_sync)

    async def load_klines_from_db(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Kline]:
        """从数据库加载K线数据"""
        async with async_session_maker() as session:
            stmt = select(KlineModel).where(
                KlineModel.exchange == exchange,
                KlineModel.symbol == symbol,
                KlineModel.timeframe == timeframe,
            )

            if start_time:
                stmt = stmt.where(KlineModel.timestamp >= start_time)
            if end_time:
                stmt = stmt.where(KlineModel.timestamp <= end_time)

            stmt = stmt.order_by(KlineModel.timestamp.asc())

            if limit:
                stmt = stmt.limit(int(limit))

            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                Kline(
                    exchange=row.exchange,
                    symbol=row.symbol,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                    open=row.open,
                    high=row.high,
                    low=row.low,
                    close=row.close,
                    volume=row.volume,
                )
                for row in rows
            ]

    # ==================== 缓存操作 ====================

    async def cache_set(
        self,
        key: str,
        value: Any,
        ttl: int = 3600,
    ) -> bool:
        """设置缓存"""
        if not self._redis:
            return False

        try:
            serialized = _cache_dumps(value)
            await self._redis.setex(key, ttl, serialized)
            return True
        except TypeError as e:
            logger.warning(f"Cache value for key {key!r} is not JSON serializable: {e}")
            return False
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False

    async def cache_get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if not self._redis:
            return None

        try:
            serialized = await self._redis.get(key)
            if serialized:
                return _cache_loads(serialized)
            return None
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
            logger.warning(f"Ignoring non-JSON cache payload for key {key!r}: {e}")
            try:
                await self._redis.delete(key)
            except Exception:
                pass
            return None
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None

    async def cache_delete(self, key: str) -> bool:
        """删除缓存"""
        if not self._redis:
            return False

        try:
            await self._redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False

    async def cache_exists(self, key: str) -> bool:
        """检查缓存是否存在"""
        if not self._redis:
            return False

        try:
            return await self._redis.exists(key) > 0
        except Exception as e:
            logger.error(f"Cache exists error: {e}")
            return False

    # ==================== 文件缓存 ====================

    async def save_to_cache_file(
        self,
        key: str,
        data: Any,
        ttl: int = 3600,
    ) -> str:
        """保存到缓存文件"""
        cache_file = self.cache_path / f"{key}.cache"
        cache_data = {
            "data": data,
            "expires_at": datetime.now(timezone.utc) + timedelta(seconds=ttl),
        }

        self.cache_path.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            f.write(_cache_dumps(cache_data))

        return str(cache_file)

    async def load_from_cache_file(self, key: str) -> Optional[Any]:
        """从缓存文件加载"""
        cache_file = self.cache_path / f"{key}.cache"

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache_data = json.load(f, object_hook=_json_cache_object_hook)

            if not isinstance(cache_data, dict):
                raise ValueError("cache file root is not an object")

            if _cache_is_expired(cache_data.get("expires_at")):
                cache_file.unlink()
                return None

            return cache_data.get("data")

        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
            logger.warning(f"Ignoring unsafe cache file {cache_file}: {e}")
            _quarantine_unsafe_cache_file(cache_file, e)
            return None

        except Exception as e:
            logger.error(f"Cache file load error: {e}")
            return None

    # ==================== 数据管理 ====================

    async def get_storage_stats(self) -> Dict:
        """获取存储统计"""
        stats = {
            "parquet_files": 0,
            "total_size_mb": 0,
            "exchanges": [],
        }

        if self.storage_path.exists():
            for exchange_dir in self.storage_path.iterdir():
                if exchange_dir.is_dir():
                    stats["exchanges"].append(exchange_dir.name)
                    for symbol_dir in exchange_dir.iterdir():
                        if symbol_dir.is_dir():
                            for file in symbol_dir.glob("*.parquet"):
                                stats["parquet_files"] += 1
                                stats["total_size_mb"] += file.stat().st_size / (1024 * 1024)

        stats["total_size_mb"] = round(stats["total_size_mb"], 2)
        return stats

    async def cleanup_old_data(
        self,
        days: int = 365,
        dry_run: bool = True,
    ) -> Dict:
        """清理旧数据"""
        result = {
            "files_removed": 0,
            "space_freed_mb": 0,
        }

        # 清理缓存文件
        if self.cache_path.exists():
            for cache_file in self.cache_path.glob("*.cache"):
                try:
                    with open(cache_file, "r", encoding="utf-8") as f:
                        data = json.load(f, object_hook=_json_cache_object_hook)
                    if isinstance(data, dict) and _cache_is_expired(data.get("expires_at")):
                        if not dry_run:
                            size = cache_file.stat().st_size
                            cache_file.unlink()
                            result["files_removed"] += 1
                            result["space_freed_mb"] += size / (1024 * 1024)
                except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
                    if not dry_run:
                        _quarantine_unsafe_cache_file(cache_file, e)
                except Exception:
                    pass

        result["space_freed_mb"] = round(result["space_freed_mb"], 2)
        return result


# 全局数据存储实例
data_storage = DataStorage()
