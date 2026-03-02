from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.news.collectors.manager import MultiSourceNewsCollector
from core.news.service.api import load_service_config
from core.news.storage import db as news_db


async def main_async(sources: list[str], max_records: int) -> None:
    cfg = load_service_config()
    await news_db.init_news_db()
    try:
        collector = MultiSourceNewsCollector(cfg)
        bundle = await collector.pull_latest_incremental(max_records=max_records, since_minutes=240, source_names=sources or None)
        print(json.dumps({
            "pulled_total": bundle.get("pulled_total"),
            "kept_total": bundle.get("kept_total"),
            "errors": bundle.get("errors"),
            "source_stats": bundle.get("source_stats"),
        }, ensure_ascii=False, indent=2))
    finally:
        await news_db.close_news_db()


def main() -> None:
    parser = argparse.ArgumentParser(description="News source self-check")
    parser.add_argument("--sources", type=str, default="chaincatcher_flash,binance_announcements,cryptocompare_news")
    parser.add_argument("--max-records", type=int, default=30)
    args = parser.parse_args()
    sources = [x.strip().lower() for x in args.sources.split(",") if x.strip()]
    asyncio.run(main_async(sources, args.max_records))


if __name__ == "__main__":
    main()
