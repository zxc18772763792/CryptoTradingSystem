from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.paper_strategy import (
    PaperStrategyConfig,
    ProfileGuardrails,
    load_paper_strategy_profile,
    profile_from_walk_forward_report,
    run_paper_strategy_once,
    save_paper_strategy_profile,
)


async def _setup_db(tmp_path: Path):
    pm_db.configure_pm_db(f"sqlite+aiosqlite:///{(tmp_path / 'pm_paper_strategy.db').as_posix()}")
    await pm_db.init_pm_db()


def _quote(token_id: str, minute: int, bid: float, ask: float):
    midpoint = (bid + ask) / 2.0
    return {
        "ts": (datetime.now(timezone.utc) - timedelta(seconds=max(0, 30 - minute))),
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
        "fetched_at": (datetime.now(timezone.utc) - timedelta(seconds=max(0, 30 - minute))),
    }


def test_paper_strategy_dry_run_plans_threshold_buy_without_order(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes([_quote("tok_a", 0, 0.39, 0.41)])
            result = await run_paper_strategy_once(
                token_ids=["tok_a"],
                config=PaperStrategyConfig(
                    account_id="strategy",
                    initial_cash=100.0,
                    order_size=10.0,
                    buy_below=0.45,
                    sell_above=0.60,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                    dry_run=True,
                ),
            )

            assert result["dry_run"] is True
            assert result["decisions"][0]["action"] == "BUY"
            assert result["orders"] == []
            assert await pm_db.list_paper_orders("strategy") == []
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_strategy_execute_places_and_fills_buy(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes([_quote("tok_a", 0, 0.39, 0.41)])
            result = await run_paper_strategy_once(
                token_ids=["tok_a"],
                config=PaperStrategyConfig(
                    account_id="strategy",
                    initial_cash=100.0,
                    order_size=10.0,
                    buy_below=0.45,
                    sell_above=0.60,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                    dry_run=False,
                ),
            )

            assert result["orders"][0]["status"] == "FILLED"
            assert result["summary"]["cash"] == 95.9
            positions = await pm_db.list_paper_positions("strategy")
            assert positions[0]["size"] == 10
            assert positions[0]["avg_price"] == 0.41
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_strategy_sells_existing_position_on_threshold_exit(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes([_quote("tok_a", 0, 0.39, 0.41)])
            await run_paper_strategy_once(
                token_ids=["tok_a"],
                config=PaperStrategyConfig(
                    account_id="strategy",
                    initial_cash=100.0,
                    order_size=10.0,
                    buy_below=0.45,
                    sell_above=0.60,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                    dry_run=False,
                ),
            )
            await pm_db.insert_quotes([_quote("tok_a", 1, 0.62, 0.64)])
            result = await run_paper_strategy_once(
                token_ids=["tok_a"],
                config=PaperStrategyConfig(
                    account_id="strategy",
                    initial_cash=100.0,
                    order_size=10.0,
                    buy_below=0.45,
                    sell_above=0.60,
                    max_order_notional=100.0,
                    max_position_notional=100.0,
                    dry_run=False,
                ),
            )

            assert result["decisions"][0]["action"] == "SELL"
            assert result["orders"][0]["status"] == "FILLED"
            assert result["summary"]["cash"] == 102.1
            assert result["summary"]["realized_pnl"] == 2.1
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_strategy_profile_from_walk_forward_report_can_execute(tmp_path: Path):
    async def run():
        await _setup_db(tmp_path)
        try:
            await pm_db.insert_quotes([_quote("tok_a", 0, 0.39, 0.41)])
            report = {
                "base_config": {
                    "strategy": "threshold",
                    "initial_cash": 100.0,
                    "order_size": 10.0,
                    "max_order_notional": 100.0,
                    "max_position_notional": 100.0,
                    "fee_rate": 0.0,
                },
                "token_ids": ["tok_a"],
                "summary": {
                    "segments": 2,
                    "total_test_net_pnl": 4.2,
                    "avg_test_net_pnl": 2.1,
                    "worst_test_net_pnl": 2.1,
                    "positive_segments": 2,
                    "selection_counts": [{"params": {"buy_below": 0.45, "sell_above": 0.60}, "segments": 2}],
                },
            }
            profile = profile_from_walk_forward_report(report, token_ids=report["token_ids"], account_id="profile", dry_run=True)
            paths = save_paper_strategy_profile(profile, tmp_path / "profile.json")
            loaded = load_paper_strategy_profile(Path(paths["profile_path"]))
            result = await run_paper_strategy_once(
                config=PaperStrategyConfig(account_id="ignored", dry_run=False),
                profile=loaded,
                dry_run_override=False,
            )

            assert loaded["params"] == {"buy_below": 0.45, "sell_above": 0.6}
            assert loaded["safe_to_execute"] is True
            assert loaded["validation"]["ok"] is True
            assert result["config"]["account_id"] == "profile"
            assert result["tokens"] == ["tok_a"]
            assert result["orders"][0]["status"] == "FILLED"
        finally:
            await pm_db.close_pm_db()

    asyncio.run(run())


def test_paper_strategy_profile_guardrails_reject_bad_walk_forward_report():
    report = {
        "base_config": {"strategy": "threshold", "order_size": 10.0},
        "summary": {
            "segments": 1,
            "positive_segments": 0,
            "total_test_net_pnl": -2.0,
            "worst_test_net_pnl": -2.0,
            "selection_counts": [{"params": {"buy_below": 0.45, "sell_above": 0.60}, "segments": 1}],
        },
    }

    try:
        profile_from_walk_forward_report(
            report,
            token_ids=["tok_a"],
            account_id="bad",
            guardrails=ProfileGuardrails(min_segments=2, min_positive_segments=1, min_total_test_net_pnl=0.0),
        )
        raised = False
    except ValueError as exc:
        raised = True
        assert "profile promotion rejected" in str(exc)

    assert raised is True


def test_paper_strategy_profile_allow_unsafe_marks_profile():
    report = {
        "base_config": {"strategy": "threshold", "order_size": 10.0},
        "summary": {
            "segments": 1,
            "positive_segments": 0,
            "total_test_net_pnl": -2.0,
            "worst_test_net_pnl": -2.0,
            "selection_counts": [{"params": {"buy_below": 0.45, "sell_above": 0.60}, "segments": 1}],
        },
    }

    profile = profile_from_walk_forward_report(
        report,
        token_ids=["tok_a"],
        account_id="bad",
        guardrails=ProfileGuardrails(min_segments=2, min_positive_segments=1, min_total_test_net_pnl=0.0),
        allow_unsafe=True,
    )

    assert profile["safe_to_execute"] is False
    assert profile["validation"]["ok"] is False
    assert profile["validation"]["failures"]
