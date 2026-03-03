from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from prediction_markets.polymarket.config import load_polymarket_config
from prediction_markets.polymarket import db as pm_db
from prediction_markets.polymarket.utils import parse_ts_any
from prediction_markets.polymarket.worker import run_worker_once
from prediction_markets.polymarket.utils import utc_now


def _p95(values: List[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=20)[-1]


async def _main(minutes: int, categories: List[str], markets_per_category: int, symbol: str) -> None:
    cfg = load_polymarket_config()
    for cat in categories:
        item = (cfg.get("categories") or {}).get(cat) or {}
        item["max_markets"] = max(1, int(markets_per_category))
        (cfg.get("categories") or {})[cat] = item
    await pm_db.init_pm_db()
    try:
        await run_worker_once(cfg, refresh_markets=True, refresh_quotes=True, categories=categories)
        end_at = utc_now() + timedelta(minutes=minutes)
        latencies: List[float] = []
        while utc_now() < end_at:
            result = await run_worker_once(cfg, refresh_markets=False, refresh_quotes=True, categories=categories)
            for item in ((result.get("status") or {}).get("source_states") or []):
                _ = item
            quotes = await pm_db.get_quotes_for_subscriptions(utc_now() - timedelta(minutes=5), utc_now())
            for quote in quotes[-50:]:
                try:
                    ts = quote["ts"]
                    fetched = quote["fetched_at"]
                    latencies.append((parse_ts_any(fetched) - parse_ts_any(ts)).total_seconds())
                except Exception:
                    continue
            await asyncio.sleep(5)
        status = await pm_db.get_pm_status()
        features_1m = await pm_db.get_features_range(symbol=symbol, since=utc_now() - timedelta(hours=2), until=utc_now(), timeframe="1m")
        features_5m = await pm_db.get_features_range(symbol=symbol, since=utc_now() - timedelta(hours=8), until=utc_now(), timeframe="5m")
        alerts = await pm_db.list_alerts(since=utc_now() - timedelta(hours=6), limit=10)
        print(json.dumps({
            "categories": categories,
            "status": status,
            "quotes_latency_p95_sec": round(_p95(latencies), 3),
            "top_alerts": alerts[:5],
            "features_1m_sample": features_1m[-3:],
            "features_5m_sample": features_5m[-3:],
        }, ensure_ascii=False, indent=2))
    finally:
        await pm_db.close_pm_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket self-check")
    parser.add_argument("--minutes", type=int, default=2)
    parser.add_argument("--categories", default="PRICE,MACRO,REG_ETF,ELECTION_GEO")
    parser.add_argument("--markets-per-category", type=int, default=3)
    parser.add_argument("--symbol", default="BTCUSDT")
    args = parser.parse_args()
    cats = [x.strip().upper() for x in str(args.categories or "").split(",") if x.strip()]
    asyncio.run(_main(args.minutes, cats, args.markets_per_category, str(args.symbol or "BTCUSDT").upper()))
