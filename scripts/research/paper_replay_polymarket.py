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
from prediction_markets.polymarket.paper_replay import (
    PolymarketPaperReplay,
    ReplayConfig,
    build_replay_strategy,
    list_replay_strategies,
)
from prediction_markets.polymarket.utils import parse_ts_any


async def _main(args) -> None:
    if args.database_url:
        pm_db.configure_pm_db(args.database_url)
    await pm_db.init_pm_db()
    try:
        replay = PolymarketPaperReplay(
            ReplayConfig(
                account_id=args.account_id,
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
            strategy=build_replay_strategy(args.strategy),
        )
        result = await replay.run_token(
            args.token_id,
            parse_ts_any(args.since),
            parse_ts_any(args.until),
            reset=not args.no_reset,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        await pm_db.close_pm_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an offline Polymarket paper replay from stored pm_quotes.")
    parser.add_argument("--token-id", required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--account-id", default="replay")
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
    parser.add_argument("--strategy", default="threshold", choices=list_replay_strategies())
    parser.add_argument("--database-url", default="")
    parser.add_argument("--no-reset", action="store_true")
    asyncio.run(_main(parser.parse_args()))
