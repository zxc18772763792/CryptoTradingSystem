"""
数据存储管理模块
支持SQLite、Parquet、Redis等多种存储方式
"""
import json
import pickle
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
import redis.asyncio as redis
from sqlalchemy import select

from config.settings import settings
from config.database import (
    async_session_maker,
    Kline as KlineModel,
    init_db,
)
from core.exchanges import Kline
from core.data.path_utils import candidate_symbol_dirs, canonical_symbol_dir


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


def _normalize_parquet_frame_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    normalized = df.copy()
    normalized.index = pd.to_datetime(normalized.index, errors="coerce")
    if getattr(normalized.index, "tz", None) is not None:
        normalized.index = normalized.index.tz_convert(timezone.utc).tz_localize(None)
    normalized = normalized[~normalized.index.isna()]
    return normalized.sort_index()


class DataStorage:
    """数据存储管理器"""

    def __init__(self):
        self.storage_path = settings.DATA_STORAGE_PATH
        self.cache_path = settings.CACHE_PATH
        self._redis: Optional[redis.Redis] = None
        self._db_initialized = False

    async def initialize(self) -> None:
        """初始化存储"""
        # 创建目录
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.cache_path.mkdir(parents=True, exist_ok=True)

        # 初始化数据库
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
                # 检查是否已存在
                existing = await session.execute(
                    select(KlineModel.id).where(
                        KlineModel.exchange == kline.exchange,
                        KlineModel.symbol == kline.symbol,
                        KlineModel.timeframe == kline.timeframe,
                        KlineModel.timestamp == kline.timestamp,
                    ).limit(1)
                )
                if existing.scalar_one_or_none() is not None:
                    continue

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
                count += 1

            await session.commit()
            return count

    async def save_klines_to_parquet(
        self,
        klines: List[Kline],
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> str:
        """保存K线数据到Parquet文件"""
        if not klines:
            return ""

        # 转换为DataFrame
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

        # 创建存储路径
        file_path = canonical_symbol_dir(self.storage_path, exchange, symbol) / f"{timeframe}.parquet"
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # 如果文件已存在，追加数据
        for symbol_root in candidate_symbol_dirs(self.storage_path, exchange, symbol):
            legacy_file = symbol_root / f"{timeframe}.parquet"
            if not legacy_file.exists():
                continue
            try:
                existing_df = pd.read_parquet(legacy_file)
                existing_df = _normalize_parquet_frame_index(existing_df)
                df = pd.concat([existing_df, df])
            except Exception as e:
                logger.warning(f"Failed to merge parquet file {legacy_file}: {e}")
                _quarantine_corrupted_parquet(legacy_file, e)
        df = df[~df.index.duplicated(keep="last")]
        df = df.sort_index()

        # 保存
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            str(file_path),
            compression="zstd",
            compression_level=9,
        )

        logger.info(f"Saved {len(df)} klines to {file_path}")
        return str(file_path)

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
                        single_df = pd.read_parquet(file_path)
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
            serialized = pickle.dumps(value)
            await self._redis.setex(key, ttl, serialized)
            return True
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
                return pickle.loads(serialized)
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
            "expires_at": datetime.now() + timedelta(seconds=ttl),
        }

        with open(cache_file, "wb") as f:
            pickle.dump(cache_data, f)

        return str(cache_file)

    async def load_from_cache_file(self, key: str) -> Optional[Any]:
        """从缓存文件加载"""
        cache_file = self.cache_path / f"{key}.cache"

        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "rb") as f:
                cache_data = pickle.load(f)

            if cache_data["expires_at"] < datetime.now():
                cache_file.unlink()
                return None

            return cache_data["data"]

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
        cutoff = datetime.now() - timedelta(days=days)
        result = {
            "files_removed": 0,
            "space_freed_mb": 0,
        }

        # 清理缓存文件
        if self.cache_path.exists():
            for cache_file in self.cache_path.glob("*.cache"):
                try:
                    with open(cache_file, "rb") as f:
                        data = pickle.load(f)
                    if data.get("expires_at", datetime.max) < datetime.now():
                        if not dry_run:
                            size = cache_file.stat().st_size
                            cache_file.unlink()
                            result["files_removed"] += 1
                            result["space_freed_mb"] += size / (1024 * 1024)
                except Exception:
                    pass

        result["space_freed_mb"] = round(result["space_freed_mb"], 2)
        return result


# 全局数据存储实例
data_storage = DataStorage()
