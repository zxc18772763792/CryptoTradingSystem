"""Pull and cache Binance perpetual funding history for research/backtests."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.backtest.funding_provider import FundingProviderConfig, FundingRateProvider  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pull Binance funding history into local parquet cache.")
    p.add_argument("--symbols", default="BTC/USDT,ETH/USDT", help="Comma-separated symbols")
    p.add_argument("--days", type=int, default=180, help="History lookback days")
    p.add_argument("--cache-dir", default="data/funding")
    p.add_argument("--exchange", default="binance")
    p.add_argument("--source", default="binance_http", choices=["binance_http", "auto"])
    return p.parse_args()


def main() -> None:
    args = parse_args()
    provider = FundingRateProvider(
        FundingProviderConfig(
            source=args.source,
            exchange=args.exchange,
            cache_dir=args.cache_dir,
        )
    )
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=max(1, int(args.days)))
    syms = [s.strip() for s in str(args.symbols).split(",") if s.strip()]

    rows = []
    for sym in syms:
        try:
            series = provider.ensure_history(sym, start_time=start_dt, end_time=end_dt, source=args.source, save=True)
            path = provider._cache_path(sym, exchange=args.exchange)  # internal helper, fine for script
            rows.append(
                {
                    "symbol": sym,
                    "points": int(len(series)),
                    "start": series.index.min().isoformat() if len(series) else None,
                    "end": series.index.max().isoformat() if len(series) else None,
                    "cache_path": str(path),
                }
            )
        except Exception as e:
            rows.append({"symbol": sym, "error": str(e)})

    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "exchange": args.exchange,
        "source": args.source,
        "days": int(args.days),
        "symbols": syms,
        "results": rows,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

