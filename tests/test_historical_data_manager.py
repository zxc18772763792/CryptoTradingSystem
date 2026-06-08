from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.data.historical_data import HistoricalDataManager
from core.data import historical_data as historical_data_module
from core.exchanges.base_exchange import Kline


class _AlwaysFailConnector:
    async def get_klines(self, **kwargs):
        raise RuntimeError("upstream timeout")


class _BadSymbolConnector:
    async def get_klines(self, **kwargs):
        raise RuntimeError("binance does not have market symbol 1000RENDER/USDT")


class _AwareKlineConnector:
    async def get_klines(self, **kwargs):
        since = kwargs["since"]
        base = since.replace(tzinfo=timezone.utc)
        return [
            Kline(
                exchange="binance",
                symbol=kwargs["symbol"],
                timeframe=kwargs["timeframe"],
                timestamp=base + timedelta(hours=idx),
                open=100.0 + idx,
                high=101.0 + idx,
                low=99.0 + idx,
                close=100.5 + idx,
                volume=10.0 + idx,
            )
            for idx in range(2)
        ]


async def test_download_historical_klines_accepts_utc_aware_exchange_timestamps(monkeypatch):
    manager = HistoricalDataManager()
    connector = _AwareKlineConnector()
    saved = {}

    monkeypatch.setattr(historical_data_module.exchange_manager, "get_exchange", lambda exchange: connector)

    async def _save_stub(klines, exchange, symbol, timeframe):
        saved["klines"] = list(klines)
        saved["exchange"] = exchange
        saved["symbol"] = symbol
        saved["timeframe"] = timeframe
        return len(klines)

    monkeypatch.setattr(historical_data_module.data_storage, "save_klines_to_parquet", _save_stub)

    klines = await manager.download_historical_klines(
        exchange="binance",
        symbol="FET/USDT",
        timeframe="1h",
        start_time=datetime(2026, 5, 17, 0, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 5, 17, 1, 0, 0, tzinfo=timezone.utc),
    )

    progress = manager.get_download_progress("binance_FET/USDT_1h")
    assert len(klines) == 2
    assert len(saved["klines"]) == 2
    assert saved["symbol"] == "FET/USDT"
    assert progress is not None
    assert progress.status == "completed"
    assert progress.retry_count == 0
    assert progress.current_time.tzinfo is None


async def test_download_historical_klines_fails_after_bounded_retries(monkeypatch):
    manager = HistoricalDataManager()
    connector = _AlwaysFailConnector()

    monkeypatch.setattr(historical_data_module.exchange_manager, "get_exchange", lambda exchange: connector)

    async def _save_stub(*args, **kwargs):
        return None

    monkeypatch.setattr(historical_data_module.data_storage, "save_klines_to_parquet", _save_stub)

    with pytest.raises(RuntimeError, match="连续重试 6 次"):
        await manager.download_historical_klines(
            exchange="binance",
            symbol="RENDER/USDT",
            timeframe="1h",
            start_time=datetime(2026, 3, 1, 0, 0, 0),
            end_time=datetime(2026, 3, 2, 0, 0, 0),
        )

    progress = manager.get_download_progress("binance_RENDER/USDT_1h")
    assert progress is not None
    assert progress.status == "failed"
    assert progress.consecutive_errors == 6
    assert progress.retry_count == 6
    assert "upstream timeout" in progress.last_error


async def test_download_historical_klines_does_not_retry_bad_symbol(monkeypatch):
    manager = HistoricalDataManager()
    connector = _BadSymbolConnector()

    monkeypatch.setattr(historical_data_module.exchange_manager, "get_exchange", lambda exchange: connector)

    async def _save_stub(*args, **kwargs):
        return None

    monkeypatch.setattr(historical_data_module.data_storage, "save_klines_to_parquet", _save_stub)

    with pytest.raises(RuntimeError, match="download failed without retry"):
        await manager.download_historical_klines(
            exchange="binance",
            symbol="RENDER/USDT",
            timeframe="1h",
            start_time=datetime(2026, 3, 1, 0, 0, 0),
            end_time=datetime(2026, 3, 2, 0, 0, 0),
        )

    progress = manager.get_download_progress("binance_RENDER/USDT_1h")
    assert progress is not None
    assert progress.status == "failed"
    assert progress.consecutive_errors == 0
    assert progress.retry_count == 0
    assert "1000RENDER/USDT" in progress.last_error
