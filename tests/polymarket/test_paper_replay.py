from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.paper_replay import (
    HoldReplayStrategy,
    PolymarketPaperReplay,
    ReplayConfig,
    ReplayDecision,
    build_replay_strategy,
    list_replay_strategies,
)


async def _setup_db(tmp_path: Path):
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'pm_replay.db').as_posix()}")
    await pm_db.init_pm_db()


def _quote(minute: int, bid: float, ask: float):
    midpoint = (bid + ask) / 2.0
    return {
        "ts": datetime(2026, 3, 3, 0, minute, tzinfo=timezone.utc),
        "market_id": "m1",
        "token_id": "tok_yes",
        "outcome": "YES",
        "price": midpoint,
        "bid": bid,
        "ask": ask,
        "midpoint": midpoint,
        "spread": ask - bid,
        "depth1": 100.0,
        "depth5": 500.0,
        "fetched_at": datetime(2026, 3, 3, 0, minute, tzinfo=timezone.utc),
    }


def test_paper_replay_buys_low_sells_high_from_quotes(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            replay = PolymarketPaperReplay(
                ReplayConfig(
                    account_id="replay-test",
                    initial_cash=100.0,
                    order_size=10.0,
                    buy_below=0.45,
                    sell_above=0.60,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                )
            )
            result = await replay.run_quotes(
                [
                    _quote(0, 0.39, 0.41),
                    _quote(1, 0.50, 0.52),
                    _quote(2, 0.62, 0.64),
                ]
            )

            assert result["quotes_seen"] == 3
            assert result["fills_count"] == 2
            assert [item["action"] for item in result["decisions"]] == ["BUY", "SELL"]
            assert result["summary"]["realized_pnl"] == 2.1
            assert result["summary"]["equity"] == 102.1
            assert result["summary"]["positions"] == []
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_replay_can_load_token_quotes_from_db(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes([_quote(0, 0.39, 0.41), _quote(1, 0.62, 0.64)])
            replay = PolymarketPaperReplay(
                ReplayConfig(
                    account_id="replay-db",
                    initial_cash=100.0,
                    order_size=10.0,
                    buy_below=0.45,
                    sell_above=0.60,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                )
            )
            result = await replay.run_token(
                "tok_yes",
                datetime(2026, 3, 3, tzinfo=timezone.utc),
                datetime(2026, 3, 3, 0, 2, tzinfo=timezone.utc),
            )

            assert result["quotes_seen"] == 2
            assert result["fills_count"] == 2
            assert result["summary"]["equity"] == 102.1
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_replay_hold_strategy_makes_no_trades(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            replay = PolymarketPaperReplay(
                ReplayConfig(account_id="hold", initial_cash=100.0),
                strategy=HoldReplayStrategy(),
            )
            result = await replay.run_quotes([_quote(0, 0.39, 0.41), _quote(1, 0.62, 0.64)])

            assert result["fills_count"] == 0
            assert result["decisions"] == []
            assert result["summary"]["equity"] == 100.0
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_replay_accepts_custom_strategy(tmp_path: Path):
    class BuyFirstStrategy:
        def __init__(self):
            self.done = False

        async def decide(self, *, quote, position, config):
            if self.done:
                return None
            self.done = True
            return ReplayDecision(action="BUY", price=float(quote["ask"]), size=5.0, reason="custom_buy")

    async def run():
        await _setup_db(tmp_path)
        try:
            replay = PolymarketPaperReplay(
                ReplayConfig(account_id="custom", initial_cash=100.0, max_order_notional=100.0),
                strategy=BuyFirstStrategy(),
            )
            result = await replay.run_quotes([_quote(0, 0.49, 0.51), _quote(1, 0.52, 0.54)])

            assert result["fills_count"] == 1
            assert result["decisions"][0]["reason"] == "custom_buy"
            assert result["summary"]["positions"][0]["size"] == 5.0
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_replay_strategy_registry_exposes_named_strategies():
    assert list_replay_strategies() == ["hold", "momentum", "threshold"]
    assert build_replay_strategy("threshold").__class__.__name__ == "ThresholdReplayStrategy"
    assert build_replay_strategy("momentum").__class__.__name__ == "MomentumReplayStrategy"


def test_paper_replay_momentum_strategy_buys_rising_sells_falling(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            replay = PolymarketPaperReplay(
                ReplayConfig(
                    account_id="momentum",
                    initial_cash=100.0,
                    order_size=10.0,
                    momentum_window=2,
                    momentum_buy_delta=0.04,
                    momentum_sell_delta=0.04,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                ),
                strategy=build_replay_strategy("momentum"),
            )
            result = await replay.run_quotes(
                [
                    _quote(0, 0.39, 0.41),
                    _quote(1, 0.41, 0.43),
                    _quote(2, 0.45, 0.47),
                    _quote(3, 0.44, 0.46),
                    _quote(4, 0.39, 0.41),
                ]
            )

            assert [item["reason"] for item in result["decisions"]] == ["momentum_buy", "momentum_sell"]
            assert result["fills_count"] == 2
            assert result["summary"]["positions"] == []
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())
