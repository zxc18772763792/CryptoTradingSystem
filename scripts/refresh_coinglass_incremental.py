from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from core.data.coinglass_feature_builder import update_coinglass_cache


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh CoinGlass premium cache incrementally.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols such as BTC/USDT,ETH/USDT")
    parser.add_argument("--datasets", default="", help="Comma-separated CoinGlass dataset names. Defaults to the light core set.")
    parser.add_argument("--manual", action="store_true", help="Use manual refresh budget path.")
    parser.add_argument("--max-symbols", type=int, default=1, help="Maximum symbols to refresh in this run.")
    return parser


async def _main() -> None:
    args = _build_parser().parse_args()
    symbols = [item.strip() for item in str(args.symbols or "").split(",") if item.strip()]
    datasets = [item.strip() for item in str(args.datasets or "").split(",") if item.strip()]
    result = await update_coinglass_cache(
        symbols=symbols or None,
        datasets=datasets or None,
        manual=bool(args.manual),
        max_symbols_per_run=max(1, int(args.max_symbols or 1)),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
