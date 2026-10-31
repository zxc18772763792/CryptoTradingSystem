"""Behavioral regressions from the October 10-28 change audit."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import importlib
from pathlib import Path
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from core.data.data_storage import DataStorage, prepare_utc_kline_frame
from core.data.kline_partitions import series_lock_target
from core.data.parquet_lock import parquet_partition_lock
from core.data.path_utils import canonical_symbol_dir
from core.exchanges.base_exchange import Kline


def _frame(start="2026-10-01", periods=48, *, price=100.0):
    index = pd.date_range(start, periods=periods, freq="h", name="timestamp")
    return pd.DataFrame({"open": price, "high": price + 1, "low": price - 1,
                         "close": price, "volume": 10.0}, index=index)


@pytest.mark.parametrize("operation", ["load", "latest"])
def test_read_during_part_consolidation_keeps_every_bar(tmp_path, monkeypatch, operation):
    module = importlib.import_module("core.data.data_storage")
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    parts = canonical_symbol_dir(storage.storage_path, "binance", "ETH/USDT") / "1h_parts"
    parts.mkdir(parents=True)
    frame = prepare_utc_kline_frame(_frame(), timestamp_timezone="UTC")
    paths = [parts / "2026-10-01.parquet", parts / "2026-10-02.parquet"]
    for n, path in enumerate(paths):
        frame.iloc[n * 24:(n + 1) * 24].to_parquet(path)

    listed, moved, started = threading.Event(), threading.Event(), threading.Event()
    original_list = module.list_part_files

    def list_before_move(directory):
        files = original_list(directory)
        if Path(directory) == parts and not moved.is_set():
            listed.set()
            assert moved.wait(5)
        return files

    monkeypatch.setattr(module, "list_part_files", list_before_move)

    def read():
        started.set()
        method = storage.load_klines_from_parquet if operation == "load" else storage.get_latest_kline_timestamp
        return asyncio.run(method("binance", "ETH/USDT", "1h"))

    with ThreadPoolExecutor(max_workers=1) as pool:
        with parquet_partition_lock(series_lock_target(parts)):
            future = pool.submit(read)
            assert started.wait(5)
            # Before the fix a reader enumerates old paths while this
            # transaction is still moving them. A synchronized reader waits.
            listed.wait(1)
            try:
                frame.to_parquet(parts / "2026.parquet")
                for path in paths:
                    path.unlink()
            finally:
                moved.set()
        result = future.result(timeout=10)
    if operation == "load":
        pd.testing.assert_frame_equal(result, frame, check_freq=False)
    else:
        assert result == frame.index.max().to_pydatetime()


@pytest.mark.parametrize("consumer", ["agent", "strategy"])
@pytest.mark.parametrize("connected", [True, False])
def test_futures_consumers_never_blend_spot_history(tmp_path, monkeypatch, consumer, connected):
    name = "core.ai.autonomous_agent" if consumer == "agent" else "core.strategies.strategy_manager"
    module = importlib.import_module(name)
    spot, perp = _frame(price=100.0), _frame(price=200.0)
    seen = []

    async def load(**kwargs):
        seen.append(kwargs["exchange"])
        return perp.copy() if kwargs["exchange"] == "binance_futures" else spot.copy()

    connector = SimpleNamespace(config=SimpleNamespace(default_type="future")) if connected else None
    monkeypatch.setattr(module.settings, "BINANCE_DEFAULT_TYPE", "future")
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda _: connector)
    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", load)
    monkeypatch.setattr(module.data_storage, "save_klines_to_parquet", AsyncMock())
    if consumer == "agent":
        obj = module.AutonomousTradingAgent(cache_root=tmp_path)
        result = asyncio.run(obj._load_market_data({"exchange": "binance", "symbol": "ETH/USDT",
                                                   "timeframe": "1h", "lookback_bars": 48,
                                                   "_scan_skip_live_market": True}))
    else:
        obj = module.StrategyManager()
        monkeypatch.setattr(obj, "_load_live_market_data", AsyncMock(return_value=perp.iloc[-2:].copy()))
        result = asyncio.run(obj._load_market_data("binance", "ETH/USDT", "1h", limit=48))
    assert seen[0] == "binance_futures"
    assert len(result) == 48
    assert (result["close"] == 200.0).all()


def test_strategy_cache_does_not_survive_a_change_of_venue(monkeypatch):
    module = importlib.import_module("core.strategies.strategy_manager")
    manager = module.StrategyManager()
    connector = SimpleNamespace(config=SimpleNamespace(default_type="future"))
    monkeypatch.setattr(module.exchange_manager, "get_exchange", lambda _: connector)
    monkeypatch.setattr(manager, "_load_live_market_data", AsyncMock(return_value=pd.DataFrame()))
    calls = []

    async def load(**kwargs):
        calls.append(kwargs["exchange"])
        return _frame(price=200. if kwargs["exchange"] == "binance_futures" else 100.)

    monkeypatch.setattr(module.data_storage, "load_klines_from_parquet", load)
    futures = asyncio.run(manager._load_market_data("binance", "ETH/USDT", "1h", limit=40))
    connector.config.default_type = "spot"
    spot = asyncio.run(manager._load_market_data("binance", "ETH/USDT", "1h", limit=40))
    assert calls == ["binance_futures", "binance"]
    assert (futures["close"] == 200.).all()
    assert (spot["close"] == 100.).all()


@pytest.mark.parametrize("market,url", [("spot", "https://api.binance.com/api/v3/klines"),
                                        ("future", "https://fapi.binance.com/fapi/v1/klines")])
def test_public_strategy_fallback_uses_the_requested_venue(monkeypatch, market, url):
    module = importlib.import_module("core.strategies.strategy_manager")
    requested = []

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, endpoint, **kwargs):
            requested.append(endpoint)
            return SimpleNamespace(raise_for_status=lambda: None,
                                   json=lambda: [[1790812800000, "100", "101", "99", "100", "10"]])

    monkeypatch.setattr(module.httpx, "AsyncClient", Client)
    frame = asyncio.run(module.StrategyManager()._load_binance_public_market_data(
        exchange="binance", symbol="ETH/USDT", timeframe="1h", limit=40, market_type=market))
    assert requested == [url]
    assert len(frame) == 1


def test_saving_the_last_legacy_pack_day_returns_the_written_file(tmp_path):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    parts = canonical_symbol_dir(storage.storage_path, "binance", "ETH/USDT") / "1h_parts"
    parts.mkdir(parents=True)
    frame = _frame(periods=24)
    # Every bar needs distinct OHLCV so the legacy UTC guard can prove its
    # clock instead of rejecting an ambiguous constant-price fixture.
    frame = frame.add(pd.Series(range(len(frame)), index=frame.index), axis=0)
    frame.to_parquet(parts / "2026.legacy.parquet")
    klines = [Kline(exchange="binance", symbol="ETH/USDT", timeframe="1h", timestamp=stamp,
                    **row.to_dict()) for stamp, row in frame.tz_localize("UTC").iterrows()]
    saved = asyncio.run(storage.save_klines_to_parquet(klines, "binance", "ETH/USDT", "1h"))
    assert Path(saved).is_file()
    pd.testing.assert_frame_equal(pd.read_parquet(saved)[list(frame.columns)], frame, check_freq=False)


def test_benchmark_beta_cache_refreshes_after_a_minute(tmp_path, monkeypatch):
    from core.market_state import benchmark_beta as module

    monkeypatch.setattr(module.settings, "DATA_STORAGE_PATH", tmp_path)
    import time
    clock = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    index = pd.date_range("2026-10-01", periods=80, freq="h")
    returns = pd.Series([.004, -.002, .006, -.003] * 20, index=index)

    def write(symbol, beta):
        path = canonical_symbol_dir(tmp_path, "binance", symbol)
        path.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"close": 100. * (1. + beta * returns).cumprod()}).to_parquet(path / "1h.parquet")

    write("BTC/USDT", 1.)
    write("ETH/USDT", .5)
    module.clear_benchmark_beta_cache()
    try:
        before = module.resolve_benchmark_beta(symbol="ETH/USDT")
        write("ETH/USDT", 1.5)
        clock[0] += 61.
        after = module.resolve_benchmark_beta(symbol="ETH/USDT")
        assert before["beta"] == pytest.approx(.5)
        assert after["beta"] == pytest.approx(1.5)
    finally:
        module.clear_benchmark_beta_cache()


@pytest.mark.parametrize("field,value", [("exchange", "gate"), ("timeframe", "1h"),
                                         ("trading_mode", "live")])
def test_model_decision_cache_isolated_by_feed_and_trading_mode(tmp_path, field, value):
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path)
    context = {"exchange": "binance", "symbol": "ETH/USDT", "timeframe": "15m",
               "trading_mode": "paper", "market_structure": {"last_bar_at": "2026-10-28T01:00:00Z"}}
    agent._remember_bar_decision(context, {"action": "buy", "confidence": .8})
    assert agent._bar_cached_decision(context)["action"] == "buy"
    assert agent._bar_cached_decision({**context, field: value}) is None


def test_model_decision_cache_invalidated_when_model_changes(tmp_path):
    from core.ai.autonomous_agent import AutonomousTradingAgent

    agent = AutonomousTradingAgent(cache_root=tmp_path)
    context = {"symbol": "ETH/USDT", "market_structure": {"last_bar_at": "2026-10-28T01:00:00Z"}}
    agent._remember_bar_decision(context, {"action": "buy", "confidence": .8})
    asyncio.run(agent.update_runtime_config(model="audit-another-model"))
    assert agent._bar_cached_decision(context) is None
