from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.replay_report import ReplayBatchConfig, run_replay_batch, write_replay_batch_report


async def _setup_db(tmp_path: Path):
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'pm_replay_report.db').as_posix()}")
    await pm_db.init_pm_db()


def _quote(token_id: str, minute: int, bid: float, ask: float):
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
        "depth1": 100.0,
        "depth5": 500.0,
        "fetched_at": datetime(2026, 3, 3, 0, minute, tzinfo=timezone.utc),
    }


def test_replay_batch_ranks_tokens_and_writes_reports(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes(
                [
                    _quote("tok_a", 0, 0.39, 0.41),
                    _quote("tok_a", 1, 0.62, 0.64),
                    _quote("tok_b", 0, 0.49, 0.51),
                    _quote("tok_b", 1, 0.52, 0.54),
                ]
            )
            report = await run_replay_batch(
                token_ids=["tok_a", "tok_b"],
                since=datetime(2026, 3, 3, tzinfo=timezone.utc),
                until=datetime(2026, 3, 3, 0, 2, tzinfo=timezone.utc),
                config=ReplayBatchConfig(
                    strategy="threshold",
                    account_prefix="testbatch",
                    initial_cash=100.0,
                    order_size=10.0,
                    buy_below=0.45,
                    sell_above=0.60,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                ),
            )
            paths = write_replay_batch_report(report, tmp_path / "reports", name="batch_test")

            assert report["best"]["token_id"] == "tok_a"
            assert report["rows"][0]["net_pnl"] == 2.1
            assert report["rows"][1]["fills_count"] == 0
            md_text = Path(paths["markdown_path"]).read_text(encoding="utf-8")
            json_payload = json.loads(Path(paths["json_path"]).read_text(encoding="utf-8"))
            assert "Polymarket Replay Batch" in md_text
            assert json_payload["best"]["token_id"] == "tok_a"
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())
