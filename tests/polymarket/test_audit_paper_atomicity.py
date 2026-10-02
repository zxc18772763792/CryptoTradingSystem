import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from prediction_markets.polymarket import db
from prediction_markets.polymarket.paper_trading import PolymarketPaperTrader, PaperRiskLimits


def quote(**updates):
    return {"ts": datetime.now(timezone.utc) - timedelta(seconds=1), "token_id": "token", "market_id": "market",
            "bid": .49, "ask": .5, "outcome": "YES", "payload": {"source": "clob_rest"}, **updates}


async def setup(tmp_path, *, max_position=100):
    db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'atomic.db').as_posix()}")
    await db.init_pm_db()
    await db.reset_paper_account("a", initial_cash=100)
    return PolymarketPaperTrader("a", PaperRiskLimits(initial_cash=100, max_order_notional=100, max_position_notional=max_position))


async def place(trader, size=10):
    return await trader.place_limit(market_id="market", token_id="token", price=.5, size=size, fill_immediately=False)


def test_concurrent_fill_is_exactly_once(tmp_path):
    async def run():
        trader = await setup(tmp_path)
        try:
            order = await place(trader)
            results = await asyncio.gather(*[trader.try_fill_order(order["order_id"], quote=quote()) for _ in range(8)])
            assert all(r["status"] == "FILLED" for r in results)
            assert (await trader.get_account())["cash"] == 95
            assert len(await db.list_paper_fills("a")) == 1
            assert (await trader.get_positions())[0]["size"] == 10
        finally:
            await db.close_pm_db()
    asyncio.run(run())


def test_fill_failure_rolls_back_and_retry_succeeds(tmp_path, monkeypatch):
    async def run():
        trader = await setup(tmp_path)
        try:
            order = await place(trader)
            apply = db._apply_paper_fill
            async def fail_after_ledger(session, fill):
                await apply(session, fill)
                raise RuntimeError("injected after ledger mutation")
            with monkeypatch.context() as patch:
                patch.setattr(db, "_apply_paper_fill", fail_after_ledger)
                with pytest.raises(RuntimeError, match="injected"):
                    await trader.try_fill_order(order["order_id"], quote=quote())
            assert (await trader.get_account())["cash"] == 100
            assert await db.list_paper_fills("a") == []
            assert (await db.get_paper_order(order["order_id"]))["status"] == "OPEN"
            assert (await trader.try_fill_order(order["order_id"], quote=quote()))["status"] == "FILLED"
        finally:
            await db.close_pm_db()
    asyncio.run(run())


def test_concurrent_pending_orders_reserve_position_budget(tmp_path):
    async def run():
        trader = await setup(tmp_path, max_position=7)
        try:
            results = await asyncio.gather(place(trader), place(trader), return_exceptions=True)
            assert sum(isinstance(r, ValueError) for r in results) == 1
            assert len(await trader.get_orders()) == 1
        finally:
            await db.close_pm_db()
    asyncio.run(run())


@pytest.mark.parametrize("invalid", [
    {"ts": datetime(2020, 1, 1, tzinfo=timezone.utc)}, {"ts": None}, {"token_id": "wrong"},
    {"ask": None, "price": .4}, {"payload": {"source": "gamma_market_snapshot"}},
])
def test_stale_or_non_executable_quote_never_fills(tmp_path, invalid):
    async def run():
        trader = await setup(tmp_path)
        try:
            order = await place(trader)
            assert (await trader.try_fill_order(order["order_id"], quote=quote(**invalid)))["status"] == "OPEN"
            assert (await trader.get_account())["cash"] == 100
        finally:
            await db.close_pm_db()
    asyncio.run(run())


def test_explicit_replay_clock_and_account_isolation(tmp_path):
    async def run():
        trader = await setup(tmp_path)
        try:
            order = await place(trader)
            other = PolymarketPaperTrader("other")
            with pytest.raises(ValueError, match="another account"):
                await other.try_fill_order(order["order_id"], quote=quote())
            ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
            result = await trader.try_fill_order(order["order_id"], quote=quote(ts=ts), simulation_time=ts)
            assert result["status"] == "FILLED"
        finally:
            await db.close_pm_db()
    asyncio.run(run())


def test_gamma_fallback_maps_tokens_and_preserves_source_time():
    from prediction_markets.polymarket.worker import _fallback_quote_from_market_snapshot
    market = {"payload": {"market": {"clobTokenIds": '["yes-token", "no-token"]', "outcomes": '["Yes", "No"]',
        "outcomePrices": '["0.8", "0.2"]', "updatedAt": "2020-01-01T00:00:00Z", "bestBid": .79, "bestAsk": .81}}}
    sub = {"market_id": "m", "token_id": "no-token", "outcome": "NO"}
    q = _fallback_quote_from_market_snapshot(sub, market)
    assert q["price"] == .2
    assert q["bid"] is q["ask"] is None
    assert q["ts"].year == 2020
    assert _fallback_quote_from_market_snapshot({**sub, "token_id": "unknown"}, market) is None
