import asyncio
import importlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pandas as pd

from core.strategies.strategy_base import StrategyBase
from core.strategies.strategy_manager import StrategyConfig, StrategyManager


def _sample_bars(index_values: list[str]) -> pd.DataFrame:
    idx = pd.to_datetime(index_values)
    base = pd.Series(range(1, len(idx) + 1), index=idx, dtype=float)
    df = pd.DataFrame(
        {
            "open": base.values,
            "high": (base + 0.5).values,
            "low": (base - 0.5).values,
            "close": (base + 0.25).values,
            "volume": 100.0,
        },
        index=idx,
    )
    df["symbol"] = "BTC/USDT"
    return df


class _CountingStrategy(StrategyBase):
    def __init__(self, name: str = "counting"):
        super().__init__(name=name, params={})
        self.generate_calls = 0

    def generate_signals(self, data: pd.DataFrame):
        self.generate_calls += 1
        return []

    def get_required_data(self):
        return {"type": "kline", "columns": ["close"], "min_length": 1}


def test_drop_incomplete_last_bar_for_runtime_data():
    manager = StrategyManager()
    df = _sample_bars(
        [
            "2026-04-02 10:00:00",
            "2026-04-02 10:01:00",
            "2026-04-02 10:02:00",
        ]
    )

    trimmed = manager._drop_incomplete_last_bar(
        df,
        "1m",
        now=datetime(2026, 4, 2, 10, 2, 30),
    )
    assert list(trimmed.index) == list(df.index[:-1])

    kept = manager._drop_incomplete_last_bar(
        df,
        "1m",
        now=datetime(2026, 4, 2, 10, 3, 0),
    )
    assert list(kept.index) == list(df.index)


def test_market_data_cache_ttl_scales_with_timeframe():
    manager = StrategyManager()

    assert manager._market_data_cache_ttl_for_timeframe("1s") == 1.0
    assert manager._market_data_cache_ttl_for_timeframe("1m") == 10.0
    assert manager._market_data_cache_ttl_for_timeframe("5m") == 30.0


def test_run_strategy_once_processes_each_completed_bar_only_once():
    async def _run() -> None:
        manager = StrategyManager()
        strategy = _CountingStrategy(name="runtime_once")
        strategy.start()

        manager._strategies["runtime_once"] = strategy
        manager._configs["runtime_once"] = StrategyConfig(
            name="runtime_once",
            strategy_class=_CountingStrategy,
            params={},
            symbols=["BTC/USDT"],
            timeframe="1m",
            exchange="binance",
        )

        same_bar_df = _sample_bars(
            [
                "2026-04-02 10:00:00",
                "2026-04-02 10:01:00",
            ]
        )
        next_bar_df = _sample_bars(
            [
                "2026-04-02 10:00:00",
                "2026-04-02 10:01:00",
                "2026-04-02 10:02:00",
            ]
        )

        manager._load_market_data = AsyncMock(return_value=same_bar_df)  # type: ignore[method-assign]
        await manager._run_strategy_once("runtime_once")
        await manager._run_strategy_once("runtime_once")

        assert strategy.generate_calls == 1

        manager._load_market_data = AsyncMock(return_value=next_bar_df)  # type: ignore[method-assign]
        await manager._run_strategy_once("runtime_once")

        assert strategy.generate_calls == 2

    asyncio.run(_run())


def test_run_strategy_once_timeout_records_error(monkeypatch):
    async def _run() -> None:
        manager = StrategyManager()
        strategy = _CountingStrategy(name="runtime_timeout")
        strategy.start()

        manager._strategies["runtime_timeout"] = strategy
        manager._configs["runtime_timeout"] = StrategyConfig(
            name="runtime_timeout",
            strategy_class=_CountingStrategy,
            params={},
            symbols=["BTC/USDT"],
            timeframe="15m",
            exchange="binance",
        )

        async def _slow_once(name: str) -> None:
            await asyncio.sleep(60)

        strategy_manager_module = importlib.import_module("core.strategies.strategy_manager")
        monkeypatch.setattr(strategy_manager_module, "_STRATEGY_CYCLE_TIMEOUT_SEC", 0.01)
        manager._run_strategy_once = _slow_once  # type: ignore[method-assign]

        await manager._run_strategy_once_with_timeout("runtime_timeout")
        stats = manager.get_strategy_runtime("runtime_timeout")
        assert stats["error_count"] == 1
        assert "timeout" in stats["last_error"]

    asyncio.run(_run())


def test_dashboard_stale_threshold_ignores_wedged_average_cycle(monkeypatch):
    manager = StrategyManager()
    strategy = _CountingStrategy(name="wedged_runtime")
    strategy.start()

    manager._strategies["wedged_runtime"] = strategy
    manager._configs["wedged_runtime"] = StrategyConfig(
        name="wedged_runtime",
        strategy_class=_CountingStrategy,
        params={},
        symbols=["BTC/USDT"],
        timeframe="15m",
        exchange="binance",
    )
    stats = manager._stats_for("wedged_runtime")
    stats.run_count = 5
    stats.last_run_at = datetime.now(timezone.utc) - timedelta(seconds=4000)
    stats.avg_cycle_ms = 3_600_000.0
    monkeypatch.setattr(manager, "get_strategy_runtime_mode", lambda name: "paper")

    summary = manager.get_dashboard_summary(signal_limit=5)
    stale = summary["stale_running"]
    assert stale
    assert stale[0]["strategy"] == "wedged_runtime"
    assert stale[0]["stale_threshold_seconds"] == 2400
