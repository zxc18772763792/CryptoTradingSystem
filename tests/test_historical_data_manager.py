from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from core.data.historical_data import HistoricalDataManager
from core.data import historical_data as historical_data_module


class _AlwaysFailConnector:
    async def get_klines(self, **kwargs):
        raise RuntimeError("upstream timeout")


def test_download_historical_klines_fails_after_bounded_retries(monkeypatch):
    manager = HistoricalDataManager()
    connector = _AlwaysFailConnector()

    monkeypatch.setattr(historical_data_module.exchange_manager, "get_exchange", lambda exchange: connector)

    async def _save_stub(*args, **kwargs):
        return None

    monkeypatch.setattr(historical_data_module.data_storage, "save_klines_to_parquet", _save_stub)

    with pytest.raises(RuntimeError, match="连续重试 6 次"):
        asyncio.run(
            manager.download_historical_klines(
                exchange="binance",
                symbol="RENDER/USDT",
                timeframe="1h",
                start_time=datetime(2026, 3, 1, 0, 0, 0),
                end_time=datetime(2026, 3, 2, 0, 0, 0),
            )
        )

    progress = manager.get_download_progress("binance_RENDER/USDT_1h")
    assert progress is not None
    assert progress.status == "failed"
    assert progress.consecutive_errors == 6
    assert progress.retry_count == 6
    assert "upstream timeout" in progress.last_error
