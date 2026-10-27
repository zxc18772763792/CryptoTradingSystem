"""Perp bars must never land in the spot kline store.

With BINANCE_DEFAULT_TYPE=future the shared Binance connector resolves
"BTC/USDT" to the USD-M perpetual. On 2026-10-26 the web chart refresh saved
1,000 such bars into data/historical/binance/BTC_USDT/1m_parts, and on
10/13-10/18 the autonomous agent saved 242-bar perp batches into the spot
5m/15m stores of ~30 coins.
"""
from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from core.data import historical_data as historical_data_module
from core.data import kline_venue
from core.data.data_storage import DataStorage
from core.data.path_utils import canonical_symbol_dir
from core.exchanges.base_exchange import Kline


def _futures_connector(**methods):
    return SimpleNamespace(config=SimpleNamespace(default_type="future"), **methods)


def _spot_connector(**methods):
    return SimpleNamespace(config=SimpleNamespace(default_type="spot"), **methods)


def _klines(start: datetime, count: int, *, step=timedelta(minutes=1), price=100.0):
    return [
        Kline(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1m",
            timestamp=start + step * idx,
            open=price + idx,
            high=price + idx + 0.5,
            low=price + idx - 0.5,
            close=price + idx + 0.25,
            volume=1.0 + idx,
        )
        for idx in range(count)
    ]


def _frame(start: str, count: int, freq: str = "1min", price: float = 100.0) -> pd.DataFrame:
    idx = pd.date_range(start, periods=count, freq=freq)
    close = [price + i for i in range(count)]
    return pd.DataFrame(
        {
            "open": close,
            "high": [v + 0.5 for v in close],
            "low": [v - 0.5 for v in close],
            "close": close,
            "volume": [1.0] * count,
        },
        index=idx,
    )


def test_market_type_helpers():
    assert kline_venue.connector_market_type(_futures_connector()) == "future"
    assert kline_venue.connector_market_type(SimpleNamespace(default_type="swap")) == "swap"
    assert kline_venue.connector_market_type(SimpleNamespace()) == "spot"
    assert kline_venue.normalize_market_type("Futures") == "future"

    assert kline_venue.venue_matches_store("binance", "spot")
    assert not kline_venue.venue_matches_store("binance", "future")
    assert not kline_venue.venue_matches_store("gate", "swap")
    assert kline_venue.venue_matches_store("binance_futures", "future")
    assert not kline_venue.venue_matches_store("binance_futures", "spot")


def test_exchange_market_type_falls_back_to_setting(monkeypatch):
    from config.settings import settings

    monkeypatch.setattr(settings, "BINANCE_DEFAULT_TYPE", "future")
    assert kline_venue.exchange_market_type("binance") == "future"
    assert kline_venue.exchange_market_type("binance", _spot_connector()) == "spot"


def test_store_writer_refuses_perp_bars_in_spot_namespace(tmp_path):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    bars = _klines(datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc), 5)

    with pytest.raises(kline_venue.KlineVenueError):
        asyncio.run(storage.save_klines_to_parquet(bars, "binance", "BTC/USDT", "1m", market_type="future"))
    assert not canonical_symbol_dir(storage.storage_path, "binance", "BTC/USDT").exists()

    asyncio.run(storage.save_klines_to_parquet(bars, "binance", "BTC/USDT", "1m"))
    loaded = asyncio.run(storage.load_klines_from_parquet("binance", "BTC/USDT", "1m"))
    assert len(loaded) == 5


def test_public_client_is_spot_even_when_trading_connector_is_futures(monkeypatch):
    monkeypatch.setattr(historical_data_module.settings, "BINANCE_DEFAULT_TYPE", "future")
    client = historical_data_module._PublicCCXTKlineConnector("binance")
    assert client.default_type == "spot"


def test_spot_symbol_resolution_never_falls_back_to_contract():
    markets = {
        "BTC/USDT": {"symbol": "BTC/USDT", "base": "BTC", "quote": "USDT", "spot": True},
        "BTC/USDT:USDT": {"symbol": "BTC/USDT:USDT", "base": "BTC", "quote": "USDT", "swap": True},
        "AKE/USDT:USDT": {"symbol": "AKE/USDT:USDT", "base": "AKE", "quote": "USDT", "swap": True},
    }
    client = SimpleNamespace(markets=markets)
    resolve = historical_data_module._resolve_public_market_symbol
    assert resolve(client, "BTC/USDT", "spot") == "BTC/USDT"
    # Perp-only coin: no spot market, so ccxt gets the spot name and reports it missing.
    assert resolve(client, "AKE/USDT", "spot") == "AKE/USDT"
    assert resolve(client, "AKE/USDT", "future") == "AKE/USDT:USDT"


def test_download_uses_spot_public_client_when_trading_connector_is_futures(monkeypatch):
    start = datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc)
    created = []

    class FakePublic:
        def __init__(self, exchange):
            created.append(exchange)
            self.default_type = "spot"

        async def get_klines(self, **kwargs):
            return _klines(start, 3, step=timedelta(hours=1))

        async def close(self):
            return None

    futures = _futures_connector(get_klines=AsyncMock(side_effect=AssertionError("perp source used")))
    saved = {}

    async def fake_save(klines, exchange, symbol, timeframe, **kwargs):
        saved.update(exchange=exchange, count=len(klines), **kwargs)
        return "saved"

    monkeypatch.setattr(historical_data_module.exchange_manager, "get_exchange", lambda name: futures)
    monkeypatch.setattr(historical_data_module, "_PublicCCXTKlineConnector", FakePublic)
    monkeypatch.setattr(historical_data_module.data_storage, "save_klines_to_parquet", fake_save)

    klines = asyncio.run(
        historical_data_module.HistoricalDataManager().download_historical_klines(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1h",
            start_time=start,
            end_time=start + timedelta(hours=2),
        )
    )

    assert created == ["binance"]
    assert len(klines) == 3
    assert saved == {"exchange": "binance", "count": 3}
    futures.get_klines.assert_not_awaited()


def test_strategy_manager_keeps_futures_connector_bars_out_of_the_store(monkeypatch):
    module = importlib.import_module("core.strategies.strategy_manager")

    manager = module.StrategyManager()
    local_df = _frame("2026-04-02 10:00", 2, freq="15min")
    live_df = _frame("2026-04-02 10:00", 4, freq="15min", price=200.0)
    save_mock = AsyncMock()
    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=local_df))
    monkeypatch.setattr(module.data_storage, "save_klines_to_parquet", save_mock)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda exchange: _futures_connector())
    monkeypatch.setattr(manager, "_load_live_market_data", AsyncMock(return_value=live_df))

    result = asyncio.run(manager._load_market_data("binance", "BTC/USDT", "15m", limit=4))

    # Strategies still see the live bars; only persistence is skipped.
    assert result.index.max() == pd.Timestamp("2026-04-02 10:45")
    save_mock.assert_not_awaited()


def test_strategy_manager_persists_spot_connector_bars(monkeypatch):
    module = importlib.import_module("core.strategies.strategy_manager")

    manager = module.StrategyManager()
    local_df = _frame("2026-04-02 10:00", 2, freq="15min")
    live_df = _frame("2026-04-02 10:00", 4, freq="15min")
    save_mock = AsyncMock()
    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=local_df))
    monkeypatch.setattr(module.data_storage, "save_klines_to_parquet", save_mock)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda exchange: _spot_connector())
    monkeypatch.setattr(manager, "_load_live_market_data", AsyncMock(return_value=live_df))

    asyncio.run(manager._load_market_data("binance", "BTC/USDT", "15m", limit=4))

    saved = save_mock.await_args.args[0]
    assert [k.timestamp for k in saved] == [
        datetime(2026, 4, 2, 10, 30, tzinfo=timezone.utc),
        datetime(2026, 4, 2, 10, 45, tzinfo=timezone.utc),
    ]


def test_agent_keeps_futures_connector_bars_out_of_the_store(monkeypatch, tmp_path):
    import core.ai.autonomous_agent as module

    agent = module.AutonomousTradingAgent(cache_root=tmp_path / "agent")
    stale = _frame("2026-01-01", 120, freq="15min")
    live_start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=75)
    live = [
        SimpleNamespace(
            timestamp=(live_start + timedelta(minutes=15 * i)).isoformat(),
            open=100.0 + i, high=100.2 + i, low=99.8 + i, close=100.1 + i, volume=10.0,
        )
        for i in range(6)
    ]
    save_mock = AsyncMock(return_value="saved")
    connector = _futures_connector(get_klines=AsyncMock(return_value=live))
    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", AsyncMock(return_value=stale))
    monkeypatch.setattr(module.data_storage, "save_klines_to_parquet", save_mock)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda name: connector)

    result = asyncio.run(
        agent._load_market_data(
            {"exchange": "binance", "symbol": "BTC/USDT", "timeframe": "15m", "lookback_bars": 60}
        )
    )

    assert not result.empty
    connector.get_klines.assert_awaited()
    save_mock.assert_not_awaited()


@pytest.fixture
def data_api():
    from web.api import data as module

    return module


def _run_chart_refresh(data_api, monkeypatch, connector):
    """Request a stale BTC 1m chart, then run the background refresh it schedules."""
    local = _frame("2026-01-01", 400)
    spot = _frame("2026-01-01 06:40", 5, price=80198.0)
    scheduled = []
    saved = []

    async def fake_load(**kwargs):
        return local if kwargs.get("exchange") == "binance" else pd.DataFrame()

    async def fake_public(symbol, timeframe, limit=500):
        return spot

    async def fake_exchange_call(exchange, method, *args, **kwargs):
        return _klines(datetime(2026, 1, 1, 6, 40, tzinfo=timezone.utc), 5, price=80261.3)

    async def fake_save(klines, exchange, symbol, timeframe, **kwargs):
        saved.append((exchange, [k.close for k in klines]))

    monkeypatch.setattr(data_api.data_storage, "load_klines_from_parquet", fake_load)
    monkeypatch.setattr(data_api.data_storage, "save_klines_to_parquet", fake_save)
    monkeypatch.setattr(data_api, "_fetch_binance_public_klines", fake_public)
    monkeypatch.setattr(data_api, "_safe_exchange_call", fake_exchange_call)
    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda name: connector)
    monkeypatch.setattr(
        data_api, "_schedule_live_cache_refresh", lambda key, build: scheduled.append(build) or True
    )

    asyncio.run(data_api.get_klines(exchange="binance", symbol="BTC/USDT", timeframe="1m", limit=120))
    assert len(scheduled) == 1
    asyncio.run(scheduled[0]())
    return saved


def test_chart_refresh_saves_spot_rest_bars_when_connector_is_futures(data_api, monkeypatch):
    saved = _run_chart_refresh(data_api, monkeypatch, _futures_connector())
    assert saved == [("binance", [80198.0, 80199.0, 80200.0, 80201.0, 80202.0])]


def test_chart_refresh_uses_connector_when_it_is_spot(data_api, monkeypatch):
    saved = _run_chart_refresh(data_api, monkeypatch, _spot_connector())
    assert saved and saved[0][1][0] == pytest.approx(80261.55)


def test_trade_aggregation_skips_futures_connector(data_api, monkeypatch):
    fetch_trades = AsyncMock(side_effect=AssertionError("perp trades fetched"))
    connector = _futures_connector(_client=SimpleNamespace(fetch_trades=fetch_trades))
    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda name: connector)
    now = datetime.now(timezone.utc)

    trades = asyncio.run(data_api._fetch_public_trades("binance", "BTC/USDT", now - timedelta(minutes=5), now))

    assert trades == []
    fetch_trades.assert_not_awaited()


def test_web_save_helper_refuses_perp_frames(data_api, monkeypatch):
    save_mock = AsyncMock()
    monkeypatch.setattr(data_api.data_storage, "save_klines_to_parquet", save_mock)
    with pytest.raises(kline_venue.KlineVenueError):
        asyncio.run(data_api._save_df_to_parquet("binance", "BTC/USDT", "1m", _frame("2026-01-01", 3), "future"))
    save_mock.assert_not_awaited()


def test_second_level_backfill_refuses_futures_connector(monkeypatch, tmp_path):
    from core.data import second_level_backfill as module

    monkeypatch.setattr(module.settings, "CACHE_PATH", str(tmp_path))
    manager = module.SecondLevelBackfillManager()
    start = datetime(2026, 10, 25, tzinfo=timezone.utc)
    task = module.SecondLevelBackfillTask(
        task_id="t1",
        exchange="binance",
        symbol="BTC/USDT",
        start_time=start,
        end_time=start + timedelta(days=1),
        current_time=start,
        window_days=1,
        total_days=1,
        total_windows=1,
    )
    manager._tasks[task.task_id] = task
    fetch = AsyncMock(side_effect=AssertionError("perp data fetched"))
    monkeypatch.setattr(manager, "_fetch_1s_klines_window", fetch)
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda name: _futures_connector())

    asyncio.run(manager._run("t1"))

    assert task.status == "failed"
    assert "future" in task.last_error
    fetch.assert_not_awaited()
