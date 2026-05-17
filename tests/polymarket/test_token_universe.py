from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from prediction_markets.polymarket import db as pm_db


async def _setup_db(tmp_path: Path):
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'pm_universe.db').as_posix()}")
    await pm_db.init_pm_db()


def _quote(token_id: str, minute: int, bid: float = 0.4, ask: float = 0.42):
    midpoint = (bid + ask) / 2.0
    return {
        "ts": datetime(2026, 3, 3, 0, minute, tzinfo=timezone.utc),
        "market_id": f"m-{token_id}",
        "token_id": token_id,
        "outcome": "YES",
        "price": midpoint,
        "bid": bid,
        "ask": ask,
        "midpoint": midpoint,
        "spread": ask - bid,
        "depth1": 10.0 + minute,
        "depth5": 100.0 + minute,
        "fetched_at": datetime(2026, 3, 3, 0, minute, tzinfo=timezone.utc),
    }


def test_list_active_quote_tokens_orders_by_quote_count(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes(
                [
                    _quote("tok_a", 0),
                    _quote("tok_a", 1),
                    _quote("tok_a", 2),
                    _quote("tok_b", 0),
                    _quote("tok_b", 1),
                    _quote("tok_c", 0),
                ]
            )
            rows = await pm_db.list_active_quote_tokens(
                datetime(2026, 3, 3, tzinfo=timezone.utc),
                datetime(2026, 3, 3, 0, 3, tzinfo=timezone.utc),
                limit=2,
                min_quotes=2,
            )

            assert [row["token_id"] for row in rows] == ["tok_a", "tok_b"]
            assert rows[0]["quotes_count"] == 3
            assert round(rows[0]["avg_spread"], 8) == 0.02
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())
