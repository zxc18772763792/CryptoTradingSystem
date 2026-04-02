import asyncio
from datetime import datetime
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
