"""
数据存储管理模块
支持SQLite、Parquet、Redis等多种存储方式
"""
import json
import os
import asyncio
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from uuid import uuid4
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
import redis.asyncio as redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from config.settings import settings
from config.database import (
    async_session_maker,
    Kline as KlineModel,
    init_db,
)
from core.exchanges import Kline
from core.data.path_utils import candidate_symbol_dirs, canonical_symbol_dir
from core.data.parquet_lock import parquet_partition_lock


PARQUET_WRITE_LOCK_TIMEOUT_SECONDS = float(
    os.getenv("PARQUET_WRITE_LOCK_TIMEOUT_SECONDS", "30")
)


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
    now_utc = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    if latest > now_utc + pd.Timedelta(minutes=2):
        latest -= _LOCAL_OFFSET
    return latest.to_pydatetime()


def _normalize_parquet_frame_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    normalized = df.copy()
    # utc=True coerces a mixed tz-aware/naive index uniformly (naive is read as
    # UTC, tz-aware is converted), avoiding the "cannot be converted to
    # datetime64 unless utc=True" crash when live UTC bars meet parquet bars.
    idx = pd.to_datetime(normalized.index, errors="coerce", utc=True).tz_localize(None)
    # Legacy partitions were written in Asia/Shanghai wall-clock (UTC+8). Such
    # an index runs ~8h ahead of real UTC; shift it back so downstream
    # consumers (which assume UTC-naive) align with live data. Genuine UTC data
    # is never in the future, so it is left untouched.
    now_utc = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    valid = idx[~idx.isna()]
    if len(valid) and valid.max() > now_utc + pd.Timedelta(minutes=2):
        if bool(getattr(settings, "PARQUET_TZ_STRICT", False)):
            raise ValueError(
                f"Parquet index runs ahead of UTC (max={valid.max()} > "
                f"now={now_utc}); a non-UTC kline writer is still active"
            )
        logger.warning(
            f"Parquet index ahead of UTC (max={valid.max()} > now={now_utc}); "
            f"applying -8h legacy local->UTC correction. Fix the writer / run "
            f"scripts/migrate_parquet_klines_to_utc.py to remove this fallback."
        )
        idx = idx - pd.Timedelta(hours=8)
    normalized.index = idx
    normalized = normalized[~normalized.index.isna()]
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


_LOCAL_OFFSET = pd.Timedelta(hours=8)  # Asia/Shanghai, no DST


def _heal_local_existing_against_utc(
    existing_df: pd.DataFrame, incoming_df: pd.DataFrame
) -> pd.DataFrame:
    """Shift a legacy local-stamped (UTC+8) partition onto UTC.

    Incoming klines come from the connectors / fixed writer and are
    authoritative UTC. A legacy partition written by the old maintain
    script carries the *same* OHLC values but timestamps labelled +8h.
    If shifting the existing index back 8h makes its OHLC line up exactly
    with the authoritative incoming bars (and the unshifted index does
    not), the partition is provably local and is migrated in place.

    Deterministic and self-validating: it acts only on exact OHLC
    equality across multiple bars, which cannot occur by chance, so it
    never corrupts a genuinely-UTC partition.
    """
    if existing_df is None or existing_df.empty or incoming_df is None or incoming_df.empty:
        return existing_df
    cols = ["open", "high", "low", "close"]
    if not all(c in existing_df.columns and c in incoming_df.columns for c in cols):
        return existing_df

    def _match_count(idx_shift: pd.Timedelta) -> int:
        probe = existing_df.copy()
        probe.index = probe.index + idx_shift
        join = probe[cols].join(incoming_df[cols], how="inner", lsuffix="_a", rsuffix="_b")
        if join.empty:
            return 0
        same = (
            (join["open_a"] == join["open_b"])
            & (join["high_a"] == join["high_b"])
            & (join["low_a"] == join["low_b"])
            & (join["close_a"] == join["close_b"])
        )
        return int(same.sum())

    shifted_matches = _match_count(-_LOCAL_OFFSET)
    asis_matches = _match_count(pd.Timedelta(0))
    if shifted_matches >= 3 and shifted_matches > asis_matches:
        healed = existing_df.copy()
        healed.index = healed.index - _LOCAL_OFFSET
        logger.warning(
            f"Healed legacy local-time partition to UTC: shifted -8h "
            f"({shifted_matches} OHLC-anchored bars matched vs {asis_matches} as-is)"
        )
        return healed
    return existing_df


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
        """保存K线数据到数据库"""
        if not klines:
            return 0

        async with async_session_maker() as session:
            count = 0
            for kline in klines:
                db_kline = KlineModel(
                    exchange=kline.exchange,
                    symbol=kline.symbol,
                    timeframe=kline.timeframe,
                    timestamp=kline.timestamp,
                    open=kline.open,
                    high=kline.high,
                    low=kline.low,
                    close=kline.close,
                    volume=kline.volume,
                )
                session.add(db_kline)
                try:
                    await session.flush()
                    count += 1
                except IntegrityError:
                    # Duplicate bar (concurrent writer or re-fetch) — skip silently.
                    await session.rollback()

            await session.commit()
            return count

    async def save_klines_to_parquet(
        self,
        klines: List[Kline],
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> str:
        """淇濆瓨K绾挎暟鎹埌Parquet鏂囦欢"""
        if not klines:
            return ""

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
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = _normalize_parquet_frame_index(df.set_index("timestamp"))

        symbol_dir = canonical_symbol_dir(self.storage_path, exchange, symbol)
        parts_dir = symbol_dir / f"{timeframe}_parts"
        parts_dir.mkdir(parents=True, exist_ok=True)

        def _save_sync() -> List[Path]:
            written_parts: List[Path] = []
            grouped = df.groupby(df.index.date, sort=True)
            for part_day, day_df in grouped:
                part_path = parts_dir / f"{part_day.isoformat()}.parquet"
                with parquet_partition_lock(
                    part_path,
                    timeout_seconds=PARQUET_WRITE_LOCK_TIMEOUT_SECONDS,
                ):
                    incoming_df = _normalize_parquet_frame_index(day_df)
                    merged_df = incoming_df
                    existing_df: Optional[pd.DataFrame] = None
                    if part_path.exists():
                        try:
                            existing_df = pd.read_parquet(part_path)
                            existing_df = _normalize_parquet_frame_index(existing_df)
                            existing_df = _heal_local_existing_against_utc(
                                existing_df, incoming_df
                            )
                            merged_df = pd.concat([existing_df, incoming_df])
                        except Exception as e:
                            logger.warning(f"Failed to merge partition file {part_path}: {e}")
                            _quarantine_corrupted_parquet(part_path, e)
                    merged_df = merged_df[~merged_df.index.duplicated(keep="last")]
                    merged_df = merged_df.sort_index()
                    if existing_df is not None and merged_df.equals(existing_df):
                        continue
                    table = pa.Table.from_pandas(merged_df)
                    # The lock protects the full read-modify-write transaction;
                    # os.replace additionally keeps readers from observing a
                    # partially-written file.
                    tmp_path = part_path.with_name(f"{part_path.name}.{uuid4().hex}.tmp")
                    try:
                        pq.write_table(
                            table,
                            str(tmp_path),
                            compression="zstd",
                            compression_level=9,
                        )
                        os.replace(tmp_path, part_path)
                    except Exception:
                        try:
                            if tmp_path.exists():
                                tmp_path.unlink()
                        except OSError:
                            pass
                        raise
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
                    except Exception as e:
                        logger.warning(f"Failed to load parquet file {file_path}: {e}")
                        _quarantine_corrupted_parquet(file_path, e)

                if not parts_dir.exists():
                    continue

                part_files = sorted(parts_dir.glob("*.parquet"))
                if start_time or end_time:
                    # Partition pruning by daily file name (YYYY-MM-DD.parquet) to avoid loading the full history.
                    lower = (pd.Timestamp(start_time).date() - timedelta(days=1)) if start_time else None
                    upper = (pd.Timestamp(end_time).date() + timedelta(days=1)) if end_time else None
                    filtered_files: List[Path] = []
                    for part_file in part_files:
                        try:
                            part_day = pd.Timestamp(part_file.stem).date()
                        except Exception:
                            filtered_files.append(part_file)
                            continue
                        if lower and part_day < lower:
                            continue
                        if upper and part_day > upper:
                            continue
                        filtered_files.append(part_file)
                    part_files = filtered_files

                for part_file in part_files:
                    try:
                        part_df = pd.read_parquet(part_file)
                        if part_df.empty:
                            continue
                        part_df = _normalize_parquet_frame_index(part_df)
                        frames.append(part_df)
                    except Exception as e:
                        logger.warning(f"Failed to load partition file {part_file}: {e}")

            if not frames:
                return pd.DataFrame()

            df = pd.concat(frames).sort_index()
            df = df[~df.index.duplicated(keep="last")]

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
                if not parts_dir.exists():
                    continue
                for part_path in sorted(parts_dir.glob("*.parquet"), reverse=True):
                    try:
                        latest = _parquet_max_timestamp(part_path)
                    except Exception as exc:
                        logger.warning(f"Failed to inspect parquet metadata {part_path}: {exc}")
                        continue
                    if latest is not None:
                        candidates.append(latest)
                        break
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
