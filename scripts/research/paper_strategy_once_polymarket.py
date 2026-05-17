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
from prediction_markets.polymarket.paper_strategy import PaperStrategyConfig, load_paper_strategy_profile, run_paper_strategy_once


async def _main(args) -> None:
    if args.database_url:
        pm_db.configure_pm_db(args.database_url)
    await pm_db.init_pm_db()
    try:
        token_ids = [token.strip() for token in str(args.token_ids or "").split(",") if token.strip()]
        profile = load_paper_strategy_profile(Path(args.profile)) if args.profile else None
        result = await run_paper_strategy_once(
            token_ids=token_ids,
            config=PaperStrategyConfig(
                account_id=args.account_id,
                strategy=args.strategy,
                order_size=args.order_size,
                buy_below=args.buy_below,
                sell_above=args.sell_above,
                momentum_window=args.momentum_window,
                momentum_buy_delta=args.momentum_buy_delta,
                momentum_sell_delta=args.momentum_sell_delta,
                initial_cash=args.initial_cash,
                max_order_notional=args.max_order_notional,
                max_position_notional=args.max_position_notional,
                fee_rate=args.fee_rate,
                max_tokens=args.max_tokens,
                min_quotes=args.min_quotes,
                dry_run=not args.execute,
            ),
            profile=profile,
            dry_run_override=not args.execute,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        await pm_db.close_pm_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one offline Polymarket paper strategy pass from stored quotes.")
    parser.add_argument("--token-ids", default="", help="Comma-separated token ids. If empty, auto-selects active tokens from stored quotes.")
    parser.add_argument("--account-id", default="default")
    parser.add_argument("--strategy", default="threshold", choices=list_replay_strategies())
    parser.add_argument("--order-size", type=float, default=10.0)
    parser.add_argument("--buy-below", type=float, default=0.45)
    parser.add_argument("--sell-above", type=float, default=0.60)
    parser.add_argument("--momentum-window", type=int, default=3)
    parser.add_argument("--momentum-buy-delta", type=float, default=0.04)
    parser.add_argument("--momentum-sell-delta", type=float, default=0.04)
    parser.add_argument("--initial-cash", type=float, default=1000.0)
    parser.add_argument("--max-order-notional", type=float, default=50.0)
    parser.add_argument("--max-position-notional", type=float, default=200.0)
    parser.add_argument("--fee-rate", type=float, default=0.0)
    parser.add_argument("--max-tokens", type=int, default=20)
    parser.add_argument("--min-quotes", type=int, default=1)
    parser.add_argument("--database-url", default="")
    parser.add_argument("--profile", default="", help="Load a promoted paper strategy profile JSON")
    parser.add_argument("--execute", action="store_true", help="Place paper orders. Default is dry-run.")
    asyncio.run(_main(parser.parse_args()))
