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
from prediction_markets.polymarket.replay_report import ReplayBatchConfig, run_replay_batch, write_replay_batch_report
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
            token_ids = [str(item.get("token_id") or "") for item in universe]
        if not token_ids:
            raise RuntimeError("--token-ids must contain at least one token, or use --top-n with stored quotes")
        report = await run_replay_batch(
            token_ids=token_ids,
            since=parse_ts_any(args.since),
            until=parse_ts_any(args.until),
            config=ReplayBatchConfig(
                strategy=args.strategy,
                account_prefix=args.account_prefix,
                initial_cash=args.initial_cash,
                order_size=args.order_size,
                buy_below=args.buy_below,
                sell_above=args.sell_above,
                momentum_window=args.momentum_window,
                momentum_buy_delta=args.momentum_buy_delta,
                momentum_sell_delta=args.momentum_sell_delta,
                max_order_notional=args.max_order_notional,
                max_position_notional=args.max_position_notional,
                fee_rate=args.fee_rate,
            ),
        )
        paths = write_replay_batch_report(report, Path(args.output_dir), name=args.name)
        print(json.dumps({"paths": paths, "token_ids": token_ids, "best": report.get("best"), "rows": report.get("rows", [])}, ensure_ascii=False, indent=2, default=str))
    finally:
        await pm_db.close_pm_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run offline Polymarket paper replay for multiple tokens.")
    parser.add_argument("--token-ids", default="", help="Comma-separated token ids")
    parser.add_argument("--top-n", type=int, default=0, help="Auto-select top N tokens by quote count in the requested window")
    parser.add_argument("--min-quotes", type=int, default=1)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--strategy", default="threshold", choices=list_replay_strategies())
    parser.add_argument("--account-prefix", default="batch")
    parser.add_argument("--initial-cash", type=float, default=1000.0)
    parser.add_argument("--order-size", type=float, default=10.0)
    parser.add_argument("--buy-below", type=float, default=0.45)
    parser.add_argument("--sell-above", type=float, default=0.60)
    parser.add_argument("--momentum-window", type=int, default=3)
    parser.add_argument("--momentum-buy-delta", type=float, default=0.04)
    parser.add_argument("--momentum-sell-delta", type=float, default=0.04)
    parser.add_argument("--max-order-notional", type=float, default=100.0)
    parser.add_argument("--max-position-notional", type=float, default=500.0)
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument("--database-url", default="")
    parser.add_argument("--output-dir", default="data/reports")
    parser.add_argument("--name", default="polymarket_replay_batch")
    asyncio.run(_main(parser.parse_args()))
