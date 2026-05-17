from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.paper_trading import PaperRiskLimits, PolymarketPaperTrader


async def _setup_db(tmp_path: Path):
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'pm_paper.db').as_posix()}")
    await pm_db.init_pm_db()
    await pm_db.reset_paper_account("test", initial_cash=100.0)


def test_paper_limit_buy_fills_against_latest_ask(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes(
                [
                    {
                        "ts": datetime(2026, 3, 3, tzinfo=timezone.utc),
                        "market_id": "m1",
                        "token_id": "tok_yes",
                        "outcome": "YES",
                        "price": 0.49,
                        "bid": 0.48,
                        "ask": 0.50,
                        "midpoint": 0.49,
                        "spread": 0.02,
                        "depth1": 100.0,
                        "depth5": 500.0,
                        "fetched_at": datetime(2026, 3, 3, tzinfo=timezone.utc),
                    }
                ]
            )
            trader = PolymarketPaperTrader(
                account_id="test",
                limits=PaperRiskLimits(initial_cash=100.0, max_order_notional=100.0, max_position_notional=100.0),
            )
            order = await trader.place_limit(
                market_id="m1",
                token_id="tok_yes",
                outcome="YES",
                side="BUY",
                price=0.51,
                size=10,
            )
            account = await trader.get_account()
            positions = await trader.get_positions()

            assert order["status"] == "FILLED"
            assert order["avg_fill_price"] == 0.5
            assert account["cash"] == 95.0
            assert positions[0]["size"] == 10
            assert positions[0]["avg_price"] == 0.5
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_limit_order_stays_open_until_sweep(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes(
                [
                    {
                        "ts": datetime(2026, 3, 3, tzinfo=timezone.utc),
                        "market_id": "m1",
                        "token_id": "tok_yes",
                        "outcome": "YES",
                        "price": 0.50,
                        "bid": 0.49,
                        "ask": 0.55,
                        "midpoint": 0.52,
                        "spread": 0.06,
                        "depth1": 100.0,
                        "depth5": 500.0,
                        "fetched_at": datetime(2026, 3, 3, tzinfo=timezone.utc),
                    }
                ]
            )
            trader = PolymarketPaperTrader(
                account_id="test",
                limits=PaperRiskLimits(initial_cash=100.0, max_order_notional=100.0, max_position_notional=100.0),
            )
            order = await trader.place_limit(
                market_id="m1",
                token_id="tok_yes",
                outcome="YES",
                side="BUY",
                price=0.51,
                size=10,
            )
            assert order["status"] == "OPEN"

            await pm_db.insert_quotes(
                [
                    {
                        "ts": datetime(2026, 3, 3, 0, 1, tzinfo=timezone.utc),
                        "market_id": "m1",
                        "token_id": "tok_yes",
                        "outcome": "YES",
                        "price": 0.49,
                        "bid": 0.48,
                        "ask": 0.50,
                        "midpoint": 0.49,
                        "spread": 0.02,
                        "depth1": 100.0,
                        "depth5": 500.0,
                        "fetched_at": datetime(2026, 3, 3, 0, 1, tzinfo=timezone.utc),
                    }
                ]
            )
            sweep = await trader.sweep_open_orders()
            updated = await pm_db.get_paper_order(order["order_id"])

            assert sweep["filled"] == 1
            assert updated["status"] == "FILLED"
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_order_risk_limit_rejects_large_notional(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            trader = PolymarketPaperTrader(
                account_id="test",
                limits=PaperRiskLimits(initial_cash=100.0, max_order_notional=5.0, max_position_notional=100.0),
            )
            try:
                await trader.place_limit(market_id="m1", token_id="tok_yes", price=0.5, size=20)
            except ValueError as exc:
                assert "max_order_notional" in str(exc)
            else:
                raise AssertionError("expected ValueError")
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_rejects_sell_without_position(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            trader = PolymarketPaperTrader(
                account_id="test",
                limits=PaperRiskLimits(initial_cash=100.0, max_order_notional=100.0, max_position_notional=100.0),
            )
            try:
                await trader.place_limit(market_id="m1", token_id="tok_yes", side="SELL", price=0.5, size=1)
            except ValueError as exc:
                assert "insufficient paper position" in str(exc)
            else:
                raise AssertionError("expected ValueError")
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_open_buy_orders_reserve_cash(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            trader = PolymarketPaperTrader(
                account_id="test",
                limits=PaperRiskLimits(initial_cash=100.0, max_order_notional=100.0, max_position_notional=100.0),
            )
            order = await trader.place_limit(
                market_id="m1",
                token_id="tok_yes",
                side="BUY",
                price=0.6,
                size=100,
                fill_immediately=False,
            )
            summary = await trader.get_summary()

            assert order["status"] == "OPEN"
            assert summary["reserved_cash"] == 60.0
            assert summary["available_cash"] == 40.0
            try:
                await trader.place_limit(
                    market_id="m1",
                    token_id="tok_no",
                    side="BUY",
                    price=0.5,
                    size=90,
                    fill_immediately=False,
                )
            except ValueError as exc:
                assert "insufficient paper cash" in str(exc)
            else:
                raise AssertionError("expected ValueError")
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_summary_marks_positions_to_latest_quote(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes(
                [
                    {
                        "ts": datetime(2026, 3, 3, tzinfo=timezone.utc),
                        "market_id": "m1",
                        "token_id": "tok_yes",
                        "outcome": "YES",
                        "price": 0.49,
                        "bid": 0.48,
                        "ask": 0.50,
                        "midpoint": 0.49,
                        "spread": 0.02,
                        "depth1": 100.0,
                        "depth5": 500.0,
                        "fetched_at": datetime(2026, 3, 3, tzinfo=timezone.utc),
                    }
                ]
            )
            trader = PolymarketPaperTrader(
                account_id="test",
                limits=PaperRiskLimits(initial_cash=100.0, max_order_notional=100.0, max_position_notional=100.0),
            )
            await trader.place_limit(market_id="m1", token_id="tok_yes", side="BUY", price=0.51, size=10)
            await pm_db.insert_quotes(
                [
                    {
                        "ts": datetime(2026, 3, 3, 0, 1, tzinfo=timezone.utc),
                        "market_id": "m1",
                        "token_id": "tok_yes",
                        "outcome": "YES",
                        "price": 0.59,
                        "bid": 0.58,
                        "ask": 0.60,
                        "midpoint": 0.59,
                        "spread": 0.02,
                        "depth1": 100.0,
                        "depth5": 500.0,
                        "fetched_at": datetime(2026, 3, 3, 0, 1, tzinfo=timezone.utc),
                    }
                ]
            )
            summary = await trader.get_summary()

            assert summary["positions_value"] == 5.9
            assert summary["unrealized_pnl"] == 0.9
            assert summary["equity"] == 100.9
            assert summary["positions"][0]["mark_price"] == 0.59
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())
