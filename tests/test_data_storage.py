import asyncio
import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

from core.data.data_storage import DataStorage
from core.data.path_utils import canonical_symbol_dir
from core.exchanges.base_exchange import Kline

data_storage_module = importlib.import_module("core.data.data_storage")


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
