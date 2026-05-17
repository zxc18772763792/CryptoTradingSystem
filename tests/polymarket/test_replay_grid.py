from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.replay_report import (
    ReplayBatchConfig,
    momentum_param_grid,
    run_replay_grid,
    threshold_param_grid,
    write_replay_grid_report,
)


async def _setup_db(tmp_path: Path):
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'pm_replay_grid.db').as_posix()}")
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


def test_replay_param_grid_builders():
    assert threshold_param_grid("0.40,0.45", "0.42,0.60") == [
        {"buy_below": 0.4, "sell_above": 0.42},
        {"buy_below": 0.4, "sell_above": 0.6},
        {"buy_below": 0.45, "sell_above": 0.6},
    ]
    assert momentum_param_grid("2,3", "0.04", "0.05") == [
        {"momentum_window": 2, "momentum_buy_delta": 0.04, "momentum_sell_delta": 0.05},
        {"momentum_window": 3, "momentum_buy_delta": 0.04, "momentum_sell_delta": 0.05},
    ]


def test_replay_grid_ranks_parameter_sets_and_writes_report(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes(
                [
                    _quote("tok_a", 0, 0.39, 0.41),
                    _quote("tok_a", 1, 0.62, 0.64),
                ]
            )
            report = await run_replay_grid(
                token_ids=["tok_a"],
                since=datetime(2026, 3, 3, tzinfo=timezone.utc),
                until=datetime(2026, 3, 3, 0, 2, tzinfo=timezone.utc),
                base_config=ReplayBatchConfig(
                    strategy="threshold",
                    account_prefix="gridtest",
                    initial_cash=100.0,
                    order_size=10.0,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                ),
                param_grid=[
                    {"buy_below": 0.35, "sell_above": 0.60},
                    {"buy_below": 0.45, "sell_above": 0.60},
                ],
            )
            paths = write_replay_grid_report(report, tmp_path / "reports", name="grid_test")

            assert report["best"]["params"] == {"buy_below": 0.45, "sell_above": 0.6}
            assert report["rows"][0]["total_net_pnl"] == 2.1
            assert report["rows"][1]["total_fills"] == 0
            md_text = Path(paths["markdown_path"]).read_text(encoding="utf-8")
            json_payload = json.loads(Path(paths["json_path"]).read_text(encoding="utf-8"))
            assert "Polymarket Replay Grid Search" in md_text
            assert json_payload["best"]["total_net_pnl"] == 2.1
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())
