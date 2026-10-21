import asyncio
import importlib
import multiprocessing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.data.data_storage import DataStorage
from core.data.parquet_lock import ParquetPartitionLockTimeout, parquet_partition_lock
from core.data.path_utils import canonical_symbol_dir
from config.database import Kline as KlineModel
from core.exchanges.base_exchange import Kline

data_storage_module = importlib.import_module("core.data.data_storage")


def _concurrent_partition_writer(
    storage_path: str,
    minute: int,
    start_event,
) -> None:
    storage = DataStorage()
    storage.storage_path = Path(storage_path)
    start_event.wait(10)
    kline = Kline(
        exchange="binance",
        symbol="ETH/USDT",
        timeframe="1m",
        timestamp=datetime(2026, 1, 1, 0, minute, tzinfo=timezone.utc),
        open=100.0 + minute,
        high=101.0 + minute,
        low=99.0 + minute,
        close=100.5 + minute,
        volume=10.0 + minute,
    )
    asyncio.run(
        storage.save_klines_to_parquet(
            [kline], "binance", "ETH/USDT", "1m"
        )
    )


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.deleted = []

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)


def test_load_klines_from_parquet_uses_utc_naive_boundaries(tmp_path: Path):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    storage.cache_path = tmp_path / "cache"
    storage.storage_path.mkdir(parents=True, exist_ok=True)
    storage.cache_path.mkdir(parents=True, exist_ok=True)

    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    klines = [
        Kline(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe="15m",
            timestamp=start + timedelta(minutes=15 * idx),
            open=100.0 + idx,
            high=100.2 + idx,
            low=99.8 + idx,
            close=100.1 + idx,
            volume=10.0 + idx,
        )
        for idx in range(20)
    ]

    asyncio.run(
        storage.save_klines_to_parquet(
            klines=klines,
            exchange="binance",
            symbol="ETH/USDT",
            timeframe="15m",
        )
    )

    loaded = asyncio.run(
        storage.load_klines_from_parquet(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe="15m",
            start_time=datetime(2026, 1, 1, 2, 30, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 1, 4, 45, tzinfo=timezone.utc),
        )
    )

    assert len(loaded) == 10
    assert loaded.index.min().isoformat() == "2026-01-01T02:30:00"
    assert loaded.index.max().isoformat() == "2026-01-01T04:45:00"


def test_initialize_only_bootstraps_db_once(tmp_path: Path, monkeypatch):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    storage.cache_path = tmp_path / "cache"

    init_db_mock = AsyncMock(return_value=None)
    redis_client = AsyncMock()
    redis_client.ping = AsyncMock(return_value=True)

    monkeypatch.setattr(data_storage_module, "init_db", init_db_mock)
    monkeypatch.setattr(data_storage_module.redis, "from_url", lambda *_args, **_kwargs: redis_client)

    asyncio.run(storage.initialize())
    asyncio.run(storage.initialize())

    assert init_db_mock.await_count == 1


def test_save_klines_to_db_batches_and_ignores_duplicates(tmp_path: Path, monkeypatch):
    """A repeated live refresh should be one bulk insert, not N flushes."""
    db_path = tmp_path / "klines.sqlite"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(data_storage_module, "async_session_maker", sessions)

    async def _run() -> None:
        async with engine.begin() as conn:
            await conn.run_sync(KlineModel.metadata.create_all)
        storage = DataStorage()
        bars = [
            Kline(
                exchange="binance", symbol="ETH/USDT", timeframe="1m",
                timestamp=datetime(2026, 1, 1, 0, minute, tzinfo=timezone.utc),
                open=100.0 + minute, high=101.0 + minute, low=99.0 + minute,
                close=100.5 + minute, volume=10.0,
            )
            for minute in range(3)
        ]
        assert await storage.save_klines_to_db(bars) == 3
        assert await storage.save_klines_to_db(bars) == 0
        async with sessions() as session:
            rows = (await session.execute(select(KlineModel))).scalars().all()
        assert len(rows) == 3
        await engine.dispose()

    asyncio.run(_run())


def test_save_klines_to_parquet_writes_incremental_daily_parts(tmp_path: Path):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    storage.cache_path = tmp_path / "cache"
    storage.storage_path.mkdir(parents=True, exist_ok=True)
    storage.cache_path.mkdir(parents=True, exist_ok=True)

    start = datetime(2026, 1, 1, 23, 45, tzinfo=timezone.utc)
    klines = [
        Kline(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe="15m",
            timestamp=start + timedelta(minutes=15 * idx),
            open=200.0 + idx,
            high=201.0 + idx,
            low=199.0 + idx,
            close=200.5 + idx,
            volume=20.0 + idx,
        )
        for idx in range(6)
    ]

    asyncio.run(
        storage.save_klines_to_parquet(
            klines=klines,
            exchange="binance",
            symbol="ETH/USDT",
            timeframe="15m",
        )
    )

    symbol_dir = canonical_symbol_dir(storage.storage_path, "binance", "ETH/USDT")
    parts_dir = symbol_dir / "15m_parts"
    assert (parts_dir / "2026-01-01.parquet").exists()
    assert (parts_dir / "2026-01-02.parquet").exists()
    assert not (symbol_dir / "15m.parquet").exists()

    loaded = asyncio.run(
        storage.load_klines_from_parquet(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe="15m",
        )
    )

    assert len(loaded) == 6


def test_concurrent_process_writers_do_not_lose_partition_updates(tmp_path: Path):
    storage_path = tmp_path / "historical"
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    processes = [
        context.Process(
            target=_concurrent_partition_writer,
            args=(str(storage_path), minute, start_event),
        )
        for minute in (1, 2)
    ]
    for process in processes:
        process.start()
    start_event.set()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0

    storage = DataStorage()
    storage.storage_path = storage_path
    loaded = asyncio.run(
        storage.load_klines_from_parquet("binance", "ETH/USDT", "1m")
    )
    assert list(loaded.index.minute) == [1, 2]


def test_partition_lock_timeout_has_actionable_error(tmp_path: Path):
    partition = tmp_path / "2026-01-01.parquet"
    with parquet_partition_lock(partition, timeout_seconds=1):
        with pytest.raises(ParquetPartitionLockTimeout) as exc_info:
            with parquet_partition_lock(
                partition, timeout_seconds=0.05, poll_seconds=0.01
            ):
                pass
    message = str(exc_info.value)
    assert "2026-01-01.parquet.lock" in message
    assert "pid=" in message


def test_bounded_legacy_load_uses_parquet_predicate_filters(tmp_path: Path, monkeypatch):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    symbol_dir = canonical_symbol_dir(
        storage.storage_path, "binance", "ETH/USDT"
    )
    symbol_dir.mkdir(parents=True, exist_ok=True)
    index = pd.date_range("2026-01-01", periods=12, freq="h")
    pd.DataFrame({"close": range(12)}, index=index).to_parquet(
        symbol_dir / "1h.parquet", row_group_size=2
    )

    real_read_parquet = data_storage_module.pd.read_parquet
    captured_filters = []

    def _capturing_read(*args, **kwargs):
        captured_filters.append(kwargs.get("filters"))
        return real_read_parquet(*args, **kwargs)

    monkeypatch.setattr(data_storage_module.pd, "read_parquet", _capturing_read)
    loaded = asyncio.run(
        storage.load_klines_from_parquet(
            "binance",
            "ETH/USDT",
            "1h",
            start_time=datetime(2026, 1, 1, 4, 0),
            end_time=datetime(2026, 1, 1, 6, 0),
        )
    )

    assert list(loaded["close"]) == [4, 5, 6]
    assert captured_filters == [
        [
            ("__index_level_0__", ">=", datetime(2026, 1, 1, 4, 0)),
            ("__index_level_0__", "<=", datetime(2026, 1, 1, 6, 0)),
        ]
    ]


def test_latest_timestamp_reads_metadata_without_loading_history(tmp_path: Path, monkeypatch):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    symbol_dir = canonical_symbol_dir(
        storage.storage_path, "binance", "ETH/USDT"
    )
    symbol_dir.mkdir(parents=True, exist_ok=True)
    index = pd.date_range("2026-01-01", periods=8, freq="h")
    pd.DataFrame({"close": range(8)}, index=index).to_parquet(
        symbol_dir / "1h.parquet", row_group_size=2
    )

    monkeypatch.setattr(
        data_storage_module.pd,
        "read_parquet",
        lambda *_args, **_kwargs: pytest.fail("history should not be loaded"),
    )
    latest = asyncio.run(
        storage.get_latest_kline_timestamp("binance", "ETH/USDT", "1h")
    )
    assert latest == datetime(2026, 1, 1, 7, 0)


def test_identical_overlap_does_not_rewrite_partition(tmp_path: Path, monkeypatch):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    kline = Kline(
        exchange="binance",
        symbol="ETH/USDT",
        timeframe="1d",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10.0,
    )
    asyncio.run(storage.save_klines_to_parquet([kline], "binance", "ETH/USDT", "1d"))
    replace_calls = []
    real_replace = data_storage_module.os.replace

    def _tracking_replace(source, target):
        replace_calls.append((source, target))
        return real_replace(source, target)

    monkeypatch.setattr(data_storage_module.os, "replace", _tracking_replace)
    asyncio.run(storage.save_klines_to_parquet([kline], "binance", "ETH/USDT", "1d"))
    assert replace_calls == []


def test_redis_cache_uses_json_and_rejects_non_json_payload():
    storage = DataStorage()
    fake_redis = _FakeRedis()
    storage._redis = fake_redis
    when = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    stored = asyncio.run(storage.cache_set("demo", {"when": when, "value": 3}, ttl=60))
    assert stored is True
    raw = fake_redis.store["demo"]
    assert isinstance(raw, str)
    assert not raw.encode("utf-8").startswith(b"\x80")

    loaded = asyncio.run(storage.cache_get("demo"))
    assert loaded == {"when": when, "value": 3}

    fake_redis.store["legacy"] = b"\x80\x04not-json"
    loaded = asyncio.run(storage.cache_get("legacy"))
    assert loaded is None
    assert "legacy" in fake_redis.deleted


def test_cache_file_uses_json_and_quarantines_unsafe_payload(tmp_path: Path):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    storage.cache_path = tmp_path / "cache"
    storage.cache_path.mkdir(parents=True, exist_ok=True)

    when = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    cache_path = Path(asyncio.run(storage.save_to_cache_file("demo", {"when": when, "value": 3}, ttl=60)))
    raw = cache_path.read_bytes()
    assert raw.startswith(b"{")
    assert not raw.startswith(b"\x80")

    loaded = asyncio.run(storage.load_from_cache_file("demo"))
    assert loaded == {"when": when, "value": 3}

    unsafe_path = storage.cache_path / "legacy.cache"
    unsafe_path.write_bytes(b"\x80\x04not-json")
    loaded = asyncio.run(storage.load_from_cache_file("legacy"))
    assert loaded is None
    assert not unsafe_path.exists()
    assert list(storage.cache_path.glob("legacy.unsafe_*.cache"))


def _timezone_test_bars():
    return [Kline(exchange="binance",symbol="ETH/USDT",timeframe="1h",
                  timestamp=datetime(2026,1,1,hour,tzinfo=timezone.utc),
                  open=100.+hour,high=101.+hour,low=99.+hour,close=100.5+hour,volume=10.+hour)
            for hour in range(5)]


def _frame_for_timezone_bars(bars):
    return pd.DataFrame([{'open':bar.open,'high':bar.high,'low':bar.low,'close':bar.close,'volume':bar.volume}
                         for bar in bars],index=pd.DatetimeIndex([bar.timestamp.replace(tzinfo=None) for bar in bars],name='timestamp'))


def test_writer_metadata_roundtrip_and_complete_utc_overlap_upgrade(tmp_path):
    storage=DataStorage();storage.storage_path=tmp_path/'historical'
    bars=_timezone_test_bars()
    target=canonical_symbol_dir(storage.storage_path,'binance','ETH/USDT')/'1h_parts/2026-01-01.parquet'
    target.parent.mkdir(parents=True)
    original=_frame_for_timezone_bars(bars)
    original.to_parquet(target)
    assert not pd.read_parquet(target).attrs
    asyncio.run(storage.save_klines_to_parquet(bars,'binance','ETH/USDT','1h'))
    loaded=pd.read_parquet(target)
    pd.testing.assert_frame_equal(loaded,original,check_freq=False)
    assert loaded.attrs['cts_timestamp_timezone']=='UTC'
    assert loaded.attrs['cts_timestamp_schema']==1
    assert loaded.attrs['cts_timestamp_origin']=='writer'


@pytest.mark.parametrize('problem',['partial','duplicates','nat','corrupt'])
def test_failed_legacy_merge_preserves_all_source_bytes(tmp_path,problem):
    storage=DataStorage();storage.storage_path=tmp_path/'historical'
    bars=_timezone_test_bars()
    target=canonical_symbol_dir(storage.storage_path,'binance','ETH/USDT')/'1h_parts/2026-01-01.parquet'
    target.parent.mkdir(parents=True)
    original=_frame_for_timezone_bars(bars)
    if problem=='duplicates': original=pd.concat([original,original.iloc[[0]]])
    if problem=='nat': original.index=pd.DatetimeIndex([pd.NaT]+list(original.index[1:]))
    if problem=='corrupt': target.write_bytes(b'not parquet: preserve evidence')
    else: original.to_parquet(target)
    before=target.read_bytes()
    with pytest.raises(Exception):
        asyncio.run(storage.save_klines_to_parquet(bars[-1:],'binance','ETH/USDT','1h'))
    assert target.read_bytes()==before
    assert not list(target.parent.glob('*.tmp'))
    assert not list(target.parent.glob('*.corrupt_*'))


def test_naive_new_klines_require_explicit_utc_caller_declaration(tmp_path):
    storage=DataStorage();storage.storage_path=tmp_path/'historical'
    bars=_timezone_test_bars()
    for bar in bars: bar.timestamp=bar.timestamp.replace(tzinfo=None)
    with pytest.raises(ValueError,match='timezone-aware'):
        asyncio.run(storage.save_klines_to_parquet(bars,'binance','ETH/USDT','1h'))
    assert not storage.storage_path.exists()
    asyncio.run(storage.save_klines_to_parquet(bars,'binance','ETH/USDT','1h',timestamp_timezone='UTC'))
    target=canonical_symbol_dir(storage.storage_path,'binance','ETH/USDT')/'1h_parts/2026-01-01.parquet'
    assert pd.read_parquet(target).attrs['cts_timestamp_timezone']=='UTC'


def test_aware_non_utc_writer_partitions_by_actual_utc_date(tmp_path):
    storage=DataStorage();storage.storage_path=tmp_path/'historical'
    bars=_timezone_test_bars()[:1]
    bars[0].timestamp=datetime(2026,1,1,2,tzinfo=timezone(timedelta(hours=8)))
    asyncio.run(storage.save_klines_to_parquet(bars,'binance','ETH/USDT','1h'))
    parts=canonical_symbol_dir(storage.storage_path,'binance','ETH/USDT')/'1h_parts'
    assert (parts/'2025-12-31.parquet').exists()
    assert not (parts/'2026-01-01.parquet').exists()
    assert pd.read_parquet(parts/'2025-12-31.parquet').index[0]==pd.Timestamp('2025-12-31 18:00')


def test_combined_compatibility_read_remains_unverified_when_any_source_is_unknown(tmp_path):
    storage=DataStorage();storage.storage_path=tmp_path/'historical'
    symbol_dir=canonical_symbol_dir(storage.storage_path,'binance','ETH/USDT')
    symbol_dir.mkdir(parents=True)
    old=_frame_for_timezone_bars(_timezone_test_bars()[:3])
    old.to_parquet(symbol_dir/'1h.parquet')
    asyncio.run(storage.save_klines_to_parquet(_timezone_test_bars()[3:],'binance','ETH/USDT','1h'))
    loaded=asyncio.run(storage.load_klines_from_parquet('binance','ETH/USDT','1h'))
    assert len(loaded)==5
    assert loaded.attrs['cts_timestamp_status']=='unverified_naive'
    assert 'cts_timestamp_timezone' not in loaded.attrs
