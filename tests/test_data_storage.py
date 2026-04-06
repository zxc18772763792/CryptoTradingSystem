import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.data.data_storage import DataStorage
from core.exchanges.base_exchange import Kline


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
