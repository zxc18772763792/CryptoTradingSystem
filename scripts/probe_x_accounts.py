"""Read-only check of the X account collector: fetch the newest posts of one handle.

Needs X_AUTH_TOKEN (process environment or .env.local). Nothing is written to the news
database; the Scweet session state stays in data/x_accounts/. Run this once before
turning on NEWS_ENABLE_X_ACCOUNTS, and keep --limit small: every run is a real
request on the logged-in account.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import settings
from core.news.collectors.x_accounts import XAccountsCollector
from core.utils.proxy_env import ensure_proxy_env


def probe(handle: str, limit: int, days: int) -> dict:
    ensure_proxy_env(settings)
    collector = XAccountsCollector(
        {"defaults": {"x_accounts_handles": [handle], "x_accounts_tweets_per_account": limit}}
    )
    try:
        items = collector.pull_latest(max_records=limit, since_minutes=max(1, days) * 1440)
    finally:
        collector.close()
    return {
        "handle": handle,
        "count": len(items),
        "posts": [
            {
                "published_at": item["published_at"],
                "url": item["url"],
                "symbols": item["symbols"],
                "text": item["content"][:200],
                "likes": item["payload"]["likes"],
            }
            for item in items
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("handle", help="X handle to read, without @")
    parser.add_argument("--limit", type=int, default=5, help="posts to request (1-100)")
    parser.add_argument("--days", type=int, default=30, help="ignore posts older than this")
    args = parser.parse_args()
    try:
        print(json.dumps(probe(args.handle.strip().removeprefix("@"), max(1, min(args.limit, 100)), args.days),
                         ensure_ascii=False, indent=2))
    except Exception as exc:  # noqa: BLE001 - a probe reports any failure and exits non-zero
        print(f"x_accounts probe failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
