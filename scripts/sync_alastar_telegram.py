"""Archive the latest public preview snapshot, preserving original post dates.

This is a one-off bootstrap, not a complete-history exporter or event trigger.
The regular news worker handles newly published posts after setup.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import settings
from core.news.collectors.telegram import TelegramPublicChannelCollector
from core.news.service.worker import load_service_config
from core.news.storage import db as news_db
from core.utils.proxy_env import ensure_proxy_env


async def sync_snapshot(days: int = 30) -> dict:
    ensure_proxy_env(settings)
    collector = TelegramPublicChannelCollector(load_service_config())
    try:
        items = await asyncio.to_thread(
            collector.pull_latest, max_records=100, since_minutes=max(1, min(days, 365)) * 1440,
        )
    finally:
        collector.close()
    await news_db.init_news_db()
    try:
        result = await news_db.save_news_raw(
            items,
            ingest_meta={"mode": "telegram_public_snapshot", "purpose": "historical_archive"},
        )
        await news_db.set_source_state("telegram_alastar", mark_success=True)
        return {
            "source": "telegram_alastar",
            "channel": "https://t.me/alastartrades",
            "captured_count": len(items),
            "inserted_count": len(result.get("inserted") or []),
            "deduped_count": result.get("deduped_count", 0),
            "latest_published_at": max((item["published_at"] for item in items), default=None),
            "earliest_published_at": min((item["published_at"] for item in items), default=None),
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "historical_tasks_queued": 0,
            "full_history": False,
        }
    finally:
        await news_db.close_news_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30, help="Publication age limit for the visible preview snapshot")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(sync_snapshot(args.days)), ensure_ascii=False, indent=2))
