from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.paper_replay import list_replay_strategies
from prediction_markets.polymarket.replay_report import (
    ReplayBatchConfig,
    ReplayWalkForwardConfig,
    momentum_param_grid,
    run_replay_walk_forward,
    threshold_param_grid,
    write_replay_walk_forward_report,
)
from prediction_markets.polymarket.utils import parse_ts_any


async def _main(args) -> None:
    if args.database_url:
        pm_db.configure_pm_db(args.database_url)
    await pm_db.init_pm_db()
    try:
        token_ids = [token.strip() for token in str(args.token_ids or "").split(",") if token.strip()]
        if not token_ids and int(args.top_n or 0) > 0:
            universe = await pm_db.list_active_quote_tokens(
                parse_ts_any(args.since),
                parse_ts_any(args.until),
                limit=int(args.top_n),
                min_quotes=int(args.min_quotes),
            )
            token_ids = [str(item.get("token_id") or "") for item in universe if str(item.get("token_id") or "").strip()]
        if not token_ids:
            raise RuntimeError("--token-ids must contain at least one token, or use --top-n with stored quotes")
        if args.strategy == "momentum":
            grid = momentum_param_grid(args.momentum_windows, args.momentum_buy_deltas, args.momentum_sell_deltas)
        else:
            grid = threshold_param_grid(args.buy_below_grid, args.sell_above_grid)
        if not grid:
            raise RuntimeError("parameter grid is empty")
        report = await run_replay_walk_forward(
            token_ids=token_ids,
            since=parse_ts_any(args.since),
            until=parse_ts_any(args.until),
            base_config=ReplayBatchConfig(
                strategy=args.strategy,
                account_prefix=args.account_prefix,
                initial_cash=args.initial_cash,
                order_size=args.order_size,
                max_order_notional=args.max_order_notional,
                max_position_notional=args.max_position_notional,
                fee_rate=args.fee_rate,
            ),
            param_grid=grid,
            walk_config=ReplayWalkForwardConfig(
                train_minutes=args.train_minutes,
                test_minutes=args.test_minutes,
                step_minutes=args.step_minutes,
            ),
        )
        paths = write_replay_walk_forward_report(report, Path(args.output_dir), name=args.name)
        print(json.dumps({"paths": paths, "token_ids": token_ids, "summary": report.get("summary"), "rows": report.get("rows", [])}, ensure_ascii=False, indent=2, default=str))
    finally:
        await pm_db.close_pm_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run offline Polymarket paper replay walk-forward grid evaluation.")
    parser.add_argument("--token-ids", default="", help="Comma-separated token ids")
    parser.add_argument("--top-n", type=int, default=0, help="Auto-select top N tokens by quote count in the requested window")
    parser.add_argument("--min-quotes", type=int, default=1)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--strategy", default="threshold", choices=list_replay_strategies())
    parser.add_argument("--account-prefix", default="wf")
    parser.add_argument("--initial-cash", type=float, default=1000.0)
    parser.add_argument("--order-size", type=float, default=10.0)
    parser.add_argument("--buy-below-grid", default="0.40,0.45,0.50")
    parser.add_argument("--sell-above-grid", default="0.55,0.60,0.65")
    parser.add_argument("--momentum-windows", default="2,3,5")
    parser.add_argument("--momentum-buy-deltas", default="0.03,0.04,0.05")
    parser.add_argument("--momentum-sell-deltas", default="0.03,0.04,0.05")
    parser.add_argument("--train-minutes", type=int, default=120)
    parser.add_argument("--test-minutes", type=int, default=60)
    parser.add_argument("--step-minutes", type=int, default=60)
    parser.add_argument("--max-order-notional", type=float, default=100.0)
    parser.add_argument("--max-position-notional", type=float, default=500.0)
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument("--database-url", default="")
    parser.add_argument("--output-dir", default="data/reports")
    parser.add_argument("--name", default="polymarket_replay_walk_forward")
    asyncio.run(_main(parser.parse_args()))
