from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.replay_report import (
    ReplayBatchConfig,
    ReplayWalkForwardConfig,
    run_replay_walk_forward,
    write_replay_walk_forward_report,
)


async def _setup_db(tmp_path: Path):
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'pm_replay_walk_forward.db').as_posix()}")
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


def test_replay_walk_forward_selects_train_params_and_scores_test(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes(
                [
                    _quote("tok_a", 0, 0.39, 0.41),
                    _quote("tok_a", 1, 0.62, 0.64),
                    _quote("tok_a", 2, 0.39, 0.41),
                    _quote("tok_a", 3, 0.62, 0.64),
                    _quote("tok_a", 4, 0.39, 0.41),
                    _quote("tok_a", 5, 0.62, 0.64),
                ]
            )
            report = await run_replay_walk_forward(
                token_ids=["tok_a"],
                since=datetime(2026, 3, 3, 0, 0, tzinfo=timezone.utc),
                until=datetime(2026, 3, 3, 0, 6, tzinfo=timezone.utc),
                base_config=ReplayBatchConfig(
                    strategy="threshold",
                    account_prefix="wftest",
                    initial_cash=100.0,
                    order_size=10.0,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                ),
                param_grid=[
                    {"buy_below": 0.35, "sell_above": 0.60},
                    {"buy_below": 0.45, "sell_above": 0.60},
                ],
                walk_config=ReplayWalkForwardConfig(train_minutes=2, test_minutes=2, step_minutes=2),
            )
            paths = write_replay_walk_forward_report(report, tmp_path / "reports", name="wf_test")

            assert report["summary"]["segments"] == 2
            assert report["rows"][0]["selected_params"] == {"buy_below": 0.45, "sell_above": 0.6}
            assert report["rows"][0]["train_total_net_pnl"] == 2.1
            assert report["rows"][0]["test_total_net_pnl"] == 2.1
            assert report["rows"][1]["test_total_net_pnl"] == 2.1
            assert report["summary"]["total_test_net_pnl"] == 4.2
            assert report["summary"]["positive_segments"] == 2
            md_text = Path(paths["markdown_path"]).read_text(encoding="utf-8")
            json_payload = json.loads(Path(paths["json_path"]).read_text(encoding="utf-8"))
            assert "Polymarket Replay Walk Forward" in md_text
            assert json_payload["summary"]["segments"] == 2
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())
