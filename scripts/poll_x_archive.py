"""Poll the tracked X accounts into the research archive, without the news worker.

The news worker is the normal place for X polling (x_accounts in config/news_rules.yaml), but it only reads
X after it is restarted with the X code. This script does the archive half on its own, from a scheduled task,
so the forward sample (posts the collector saw in real time) starts growing now. It reads the same handle list,
creates no news item and never touches news.db except to read whether the worker is already polling X: if it
polled within the last 150 minutes this script does nothing, so turning the worker on later does not double the
requests. Every handle read is one request and goes through the shared ledger (core/news/x_pacing.py).
"""
from __future__ import annotations

import argparse
import glob
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import settings
from core.news import x_archive, x_pacing
from core.news.collectors.x_accounts import XAccountsCollector, setting
from core.utils.proxy_env import ensure_proxy_env

NEWS_DB = ROOT / "data" / "news.db"
WORKER_FRESH_MINUTES = 150


def load_handles() -> List[str]:
    import yaml

    cfg = yaml.safe_load((ROOT / "config" / "news_rules.yaml").read_text(encoding="utf-8-sig"))
    return list((cfg.get("defaults") or {}).get("x_accounts_handles") or [])


def worker_polled_recently(now: Optional[datetime] = None, db_path: Optional[Path] = None,
                           window_minutes: int = WORKER_FRESH_MINUTES) -> Optional[str]:
    """Last x_accounts success in the news worker's state table if it is recent, else None (read-only)."""
    moment = now or datetime.now(timezone.utc)
    try:
        con = sqlite3.connect(f"file:{(db_path or NEWS_DB).as_posix()}?mode=ro", uri=True, timeout=10)
        try:
            row = con.execute("select last_success_at from news_source_state where source = 'x_accounts'").fetchone()
        finally:
            con.close()
    except sqlite3.Error:
        return None
    if not row or not row[0]:
        return None
    try:
        last = datetime.fromisoformat(str(row[0]).replace(" ", "T")).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return str(row[0]) if moment - last < timedelta(minutes=window_minutes) else None


def archived_posts(directory: Optional[Path] = None) -> int:
    base = directory or x_archive.archive_dir()
    total = 0
    for path in glob.glob(str(base / "*.jsonl")):
        with open(path, "r", encoding="utf-8") as handle:
            total += sum(1 for _ in handle)
    return total


def live_status(handles: List[str], directory: Optional[Path] = None, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Read-only health of the real-time archive: who has live posts, how fresh, and what the worker/ledger saw."""
    moment = now or datetime.now(timezone.utc)
    records = [r for r in x_archive.load_archive(directory=directory) if r.get("provenance") == "live"]
    per: Dict[str, Dict[str, Any]] = {h: {"live_posts": 0, "newest_post": None, "last_seen": None} for h in handles}
    for r in records:
        row = per.setdefault(r["handle"], {"live_posts": 0, "newest_post": None, "last_seen": None})
        row["live_posts"] += 1
        row["newest_post"] = max(filter(None, [row["newest_post"], r.get("created_at")]), default=None)
        row["last_seen"] = max(filter(None, [row["last_seen"], r.get("first_seen_at")]), default=None)
    worker = worker_polled_recently(moment, window_minutes=10**6)
    return {
        "now_utc": moment.isoformat(timespec="minutes"),
        "worker_last_x_success_utc": worker,
        "worker_polling_now": bool(worker_polled_recently(moment)),
        "live_posts_total": len(records),
        "ledger_requests_last_15min": x_pacing.recent_total(),
        "per_handle": per,
    }


def poll(handles: List[str], tweets_per_account: int = 20) -> Dict[str, Any]:
    collector = XAccountsCollector({"defaults": {
        "x_accounts_handles": handles,
        "x_accounts_archive": True,
        "x_accounts_archive_only_handles": handles,      # archive only: no news item, no AI extraction
        "x_accounts_tweets_per_account": tweets_per_account,
    }})
    before = archived_posts()
    x_pacing.wait_for_room(len(handles), announce=lambda m: print(m, flush=True))
    try:
        collector.pull_latest(max_records=1)
    finally:
        collector.close()
    return {"handles": len(handles), "failed": dict(collector.last_account_errors),
            "new_posts_archived": archived_posts() - before}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--per-account", type=int, default=20, help="newest posts to read per account (1-100)")
    parser.add_argument("--ignore-worker", action="store_true", help="poll even if the news worker polled X recently")
    parser.add_argument("--dry-run", action="store_true", help="show the plan; send no request")
    parser.add_argument("--status", action="store_true", help="read-only health of the real-time archive; no request")
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    handles = load_handles()
    if args.status:
        print(json.dumps(live_status(handles), ensure_ascii=False, indent=2))
        return 0
    recent = None if args.ignore_worker else worker_polled_recently()
    plan = {"handles": len(handles), "requests_at_most": len(handles), "news_worker_polled_recently": recent}
    if args.dry_run or recent:
        print(json.dumps({**plan, "action": "none (dry run)" if args.dry_run else "skipped: the news worker is polling X"}))
        return 0
    if not setting("X_AUTH_TOKEN"):
        print("X_AUTH_TOKEN is not set", file=sys.stderr)
        return 1
    ensure_proxy_env(settings)
    try:
        result = poll(handles, max(1, min(args.per_account, 100)))
    except Exception as exc:  # noqa: BLE001 - report and exit non-zero; the next scheduled run tries again
        print(f"x archive poll failed: {type(exc).__name__}: {str(exc)[:300]}", file=sys.stderr)
        return 1
    print(json.dumps({**plan, **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
