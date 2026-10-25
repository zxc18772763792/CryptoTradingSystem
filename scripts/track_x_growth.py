"""Phase 0 of the KOL-growth idea: a candidate pool and daily follower snapshots. Records only.

Nothing here selects accounts for trading or reads their posts; see docs/X_KOL_GROWTH_TRACKER.md.
Every command is safe to re-run. Each real request goes through the shared ledger
(core/news/x_pacing.py), so it waits for a free slot in X's 15-minute window whoever else is using
the account, and a run stops at its own request cap (``--max-requests``, default 60).

  status          pool size, queue, snapshot days, ledger usage (no request)
  seed-mentions   queue accounts the archived KOL posts mention, then resolve the most-mentioned
                  first (1 request per account); each resolution also stores its first snapshot
  seed-search     look up accounts for a fixed list of search phrases (1 request per page of ~20)
  snapshot        follower counts for the active pool: 100 accounts per request by id, or, while X blocks that
                  (HTTP 403), one request per account by username for the accounts not seen for a week
  report          follower growth over the last N days (descriptive; no selection rule is frozen yet)
  daily           what to schedule: snapshot the pool, then (until the queue is empty) resolve more
                  mentioned accounts, and once a week run the search phrases; one run cap for all of it
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import settings
from core.news import x_growth, x_pacing
from core.news.collectors.x_accounts import XRateLimited, _failure_kind, setting
from core.news.x_archive import load_archive
from core.news.x_batch import guarded_call, make_client
from core.utils.proxy_env import ensure_proxy_env

# Frozen with the tracker: changing it changes which accounts can enter the pool, so record any change.
SEARCH_QUERIES = [
    "crypto trader", "bitcoin analyst", "on-chain analyst", "altcoin trader", "crypto research",
    "DeFi analyst", "crypto macro", "加密 交易员", "币圈 分析", "链上 分析师",
]
USERNAME_CHUNK = 5          # accounts per lookup call; each costs one request
SNAPSHOT_BATCH = 100        # ids per UsersByRestIds request
REFRESH_DAYS = 7            # username fallback: refresh an account when its newest snapshot is this old
NAMES_PER_RUN = 40          # username fallback: most accounts (= requests) refreshed in one run
ID_BLOCK_HOURS = 24         # after X answers 403 to the id lookup, do not try it again for this long
SLEEP: Callable[[float], None] = time.sleep


class BudgetSpent(RuntimeError):
    """The run reached its --max-requests cap."""


class Budget:
    def __init__(self, cap: int) -> None:
        self.cap = cap
        self.used = 0

    def take(self, requests: int, who: str = "tracker") -> None:
        if self.used + requests > self.cap:
            raise BudgetSpent(f"run cap of {self.cap} requests reached")
        x_pacing.wait_for_room(requests, announce=lambda m: print(f"  {m}", flush=True))
        x_pacing.record(requests, who)  # reserve before asking, so the other jobs see it
        self.used += requests


def _pause(delay: float) -> None:
    SLEEP(delay + random.uniform(0, 1.5))


def mention_counts(records: List[Dict[str, Any]], skip: set) -> Dict[str, int]:
    """How many archived KOL posts mention each account (once per post), minus the accounts we already follow."""
    counts: Dict[str, int] = {}
    for record in records:
        for name in {str(m).lower() for m in record.get("mentions") or []}:
            if name and name not in skip:
                counts[name] = counts.get(name, 0) + 1
    return counts


def resolve_usernames(conn, client, names: List[str], now: datetime, mentions: Dict[str, int]) -> Dict[str, int]:
    """Look up one chunk of usernames. Found -> candidate plus first snapshot; not found -> missing; else retry."""
    rows, messages = guarded_call(client, lambda: client.aget_user_info(users=names), debug=True)
    found = {str(r.get("username") or "").lower(): r for r in rows}
    result = {"resolved": 0, "missing": 0, "retry": 0, "new": 0}
    for name in names:
        record = found.get(name.lower())
        if record:
            result["new"] += int(x_growth.upsert_candidate(conn, record, "mention", now, mentions.get(name)))
            x_growth.save_snapshot(conn, record, now)
            x_growth.mark_pending(conn, name, "done", now)
            result["resolved"] += 1
        elif any(f"username={name}" in m.lower() and "no user" in m.lower() for m in messages):
            x_growth.mark_pending(conn, name, "missing", now, "user not found")
            result["missing"] += 1
        else:
            x_growth.mark_pending(conn, name, "retry", now, "no answer")
            result["retry"] += 1
    conn.commit()
    return result


def queue_mentions(conn, min_mentions: int) -> int:
    """Local only: queue the archive's mentioned accounts that are not followed, tracked or queued yet."""
    counts = mention_counts(load_archive(), {h.lower() for h in _tracked_handles()})
    queued = x_growth.add_pending(conn, {n: c for n, c in counts.items() if c >= min_mentions})
    conn.commit()
    return queued


def seed_mentions(conn, client, budget: Budget, min_mentions: int, limit: int, now: datetime) -> Dict[str, Any]:
    queued = queue_mentions(conn, min_mentions)
    totals = {"queued_now": queued, "resolved": 0, "missing": 0, "retry": 0, "new": 0}
    todo = x_growth.pending_batch(conn, limit)
    for start in range(0, len(todo), USERNAME_CHUNK):
        chunk = todo[start:start + USERNAME_CHUNK]
        try:
            budget.take(len(chunk))
        except BudgetSpent:
            totals["stopped"] = "run cap reached"
            break
        part = resolve_usernames(conn, client, [n for n, _ in chunk], now, dict(chunk))
        for key, value in part.items():
            totals[key] += value
        _pause(2.0)
    return totals


def seed_search(conn, client, budget: Budget, pages: int, now: datetime) -> Dict[str, Any]:
    totals = {"queries": 0, "found": 0, "new": 0}
    for query in SEARCH_QUERIES:
        try:
            budget.take(pages)
        except BudgetSpent:
            totals["stopped"] = "run cap reached"
            break
        rows, _ = guarded_call(client, lambda q=query: client.asearch_users(q, limit=20 * pages))
        for record in rows:
            totals["new"] += int(x_growth.upsert_candidate(conn, record, f"search:{query}", now))
            x_growth.save_snapshot(conn, record, now)
        totals["queries"] += 1
        totals["found"] += len(rows)
        conn.commit()
        _pause(2.0)
    return totals


def refresh_usernames(conn, client, rows: List[tuple], now: datetime) -> Dict[str, int]:
    """Snapshot accounts by username (one request each). Only a clear "not found" counts against an account."""
    names = [username for _, username in rows]
    items, messages = guarded_call(client, lambda: client.aget_user_info(users=names), debug=True)
    by_id = {str(r.get("user_id")): r for r in items}
    by_name = {str(r.get("username") or "").lower(): r for r in items}
    result = {"saved": 0, "gone": 0, "unanswered": 0}
    for user_id, username in rows:
        record = by_id.get(user_id) or by_name.get(username.lower())
        if record and str(record.get("user_id")) == user_id:
            result["saved"] += int(x_growth.save_snapshot(conn, record, now))
            x_growth.upsert_candidate(conn, record, "snapshot", now)
        elif record:  # the name now belongs to another account: ours was renamed or removed
            result["gone"] += x_growth.note_lookup(conn, [user_id], [])
        elif any(f"username={username}" in m.lower() and "no user" in m.lower() for m in messages):
            result["gone"] += x_growth.note_lookup(conn, [user_id], [])
        else:
            result["unanswered"] += 1
    conn.commit()
    return result


def take_snapshots(conn, client, budget: Budget, now: datetime, force: bool = False, mode: str = "auto") -> Dict[str, Any]:
    """Today's follower counts. 100 accounts per request by id; if X blocks that (403) fall back to usernames,
    which costs one request per account, so only accounts not seen for REFRESH_DAYS are asked, oldest first."""
    day = x_growth.day_of(now)
    totals: Dict[str, Any] = {"mode": "ids", "due": 0, "saved": 0, "gone": 0}
    blocked_until = x_growth.get_meta(conn, "id_lookup_blocked_until")
    ids_blocked = bool(blocked_until and datetime.fromisoformat(blocked_until) > now)
    if mode != "names" and not (ids_blocked and mode == "auto"):
        ids = [r["user_id"] for r in conn.execute("select user_id from candidates where active = 1")] if force             else x_growth.due_ids(conn, day)
        totals["due"] = len(ids)
        for start in range(0, len(ids), SNAPSHOT_BATCH):
            batch = ids[start:start + SNAPSHOT_BATCH]
            try:
                budget.take(1)
            except BudgetSpent:
                totals["stopped"] = "run cap reached"
                return totals
            try:
                rows, _ = guarded_call(client, lambda b=batch: client.aget_user_info(user_ids=b))
            except Exception as exc:  # noqa: BLE001 - X refuses this lookup on some connections (HTTP 403)
                if mode == "auto" and "id lookup" in str(exc) and "403" in str(exc):
                    x_growth.set_meta(conn, "id_lookup_blocked_until", (now + timedelta(hours=ID_BLOCK_HOURS)).isoformat())
                    conn.commit()
                    totals["id_lookup"] = "blocked (HTTP 403); using usernames"
                    ids_blocked = True
                    break
                raise
            for record in rows:
                if x_growth.save_snapshot(conn, record, now, replace=force):
                    totals["saved"] += 1
                x_growth.upsert_candidate(conn, record, "snapshot", now)
            totals["gone"] += x_growth.note_lookup(conn, batch, [r.get("user_id") for r in rows])
            conn.commit()
            _pause(2.0)
        else:
            return totals
    if mode == "ids":
        return totals
    # username fallback: a bounded, rotating refresh
    totals["mode"] = "usernames"
    stale = x_growth.stale_accounts(conn, day, REFRESH_DAYS, min(NAMES_PER_RUN, budget.cap - budget.used))
    totals["due"] = len(stale)
    for start in range(0, len(stale), USERNAME_CHUNK):
        chunk = stale[start:start + USERNAME_CHUNK]
        try:
            budget.take(len(chunk))
        except BudgetSpent:
            totals["stopped"] = "run cap reached"
            break
        part = refresh_usernames(conn, client, chunk, now)
        totals["saved"] += part["saved"]
        totals["gone"] += part["gone"]
        totals["unanswered"] = totals.get("unanswered", 0) + part["unanswered"]
        _pause(2.0)
    return totals


def run_daily(conn, client, budget: Budget, args, now: datetime) -> Dict[str, Any]:
    out: Dict[str, Any] = {"snapshot": take_snapshots(conn, client, budget, now)}
    queue_mentions(conn, args.min_mentions)  # new mentions from the live archive join the queue
    queue = x_growth.status(conn)["pending"]
    if queue.get("new", 0) + queue.get("retry", 0):
        out["mentions"] = seed_mentions(conn, client, budget, args.min_mentions, args.limit, now)
    last = x_growth.get_meta(conn, "last_search_seed")
    if not last or (now - datetime.fromisoformat(last)).days >= 7:
        out["search"] = seed_search(conn, client, budget, 1, now)
        if not out["search"].get("stopped"):
            x_growth.set_meta(conn, "last_search_seed", now.isoformat())
            conn.commit()
    return out


def _tracked_handles() -> List[str]:
    import yaml

    cfg = yaml.safe_load((ROOT / "config" / "news_rules.yaml").read_text(encoding="utf-8-sig"))
    return list((cfg.get("defaults") or {}).get("x_accounts_handles") or [])


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["status", "seed-mentions", "seed-search", "snapshot", "report", "daily"])
    parser.add_argument("--max-requests", type=int, default=60, help="stop after this many requests in this run")
    parser.add_argument("--min-mentions", type=int, default=2, help="seed-mentions: minimum posts mentioning an account")
    parser.add_argument("--limit", type=int, default=60, help="seed-mentions: most accounts to resolve in this run")
    parser.add_argument("--pages", type=int, default=2, help="seed-search: pages (about 20 accounts) per phrase")
    parser.add_argument("--force", action="store_true", help="snapshot: redo today's snapshot for everyone")
    parser.add_argument("--mode", choices=["auto", "ids", "names"], default="auto",
                        help="snapshot: ids (100 per request), names (1 per request), auto = ids then names on a 403")
    parser.add_argument("--days", type=int, default=7, help="report: growth window in days")
    parser.add_argument("--top", type=int, default=25, help="report: rows to show")
    parser.add_argument("--min-followers", type=int, default=1000, help="report: ignore accounts below this at the start")
    parser.add_argument("--dry-run", action="store_true", help="show what would be asked; send no request")
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    conn = x_growth.connect()
    now = x_growth.now_utc()
    if args.command == "status":
        print(json.dumps({**x_growth.status(conn), "ledger_requests_last_15min": x_pacing.recent_total()},
                         ensure_ascii=False, indent=2))
        return 0
    if args.command == "report":
        rows = x_growth.growth_rows(conn, args.days, args.min_followers)[:args.top]
        print(f"follower growth over {args.days}d, accounts with >= {args.min_followers} followers at the start "
              f"(descriptive only; {len(rows)} shown)")
        for r in rows:
            print(f"  {r['pct']:+7.1f}%  {r['change']:+8d}  {r['followers']:>9d}  {r['kind_guess']:10s} "
                  f"@{r['username']}  {r['from_day']}..{r['to_day']}")
        return 0
    if args.command == "seed-mentions":
        counts = mention_counts(load_archive(), {h.lower() for h in _tracked_handles()})
        eligible = {n: c for n, c in counts.items() if c >= args.min_mentions}
        print(json.dumps({"mentioned_accounts": len(counts), "eligible": len(eligible),
                          "requests_this_run_at_most": min(args.limit, args.max_requests)}))
    snapshot_requests = -(-len(x_growth.due_ids(conn, x_growth.day_of(now))) // SNAPSHOT_BATCH)
    waiting = x_growth.status(conn)["pending"]
    plan = {
        "seed-search": len(SEARCH_QUERIES) * args.pages,
        "snapshot": snapshot_requests,
        "daily": snapshot_requests + min(args.limit, waiting.get("new", 0) + waiting.get("retry", 0))
        + (0 if x_growth.get_meta(conn, "last_search_seed") else len(SEARCH_QUERIES)),
    }.get(args.command)
    if plan is not None:
        print(json.dumps({"command": args.command, "estimated_requests": plan, "run_cap": args.max_requests}))
    if args.dry_run:
        return 0
    if not setting("X_AUTH_TOKEN"):
        print("X_AUTH_TOKEN is not set", file=sys.stderr)
        return 1

    ensure_proxy_env(settings)
    client = make_client()
    budget = Budget(args.max_requests)
    try:
        if args.command == "seed-mentions":
            result = seed_mentions(conn, client, budget, args.min_mentions, args.limit, now)
        elif args.command == "seed-search":
            result = seed_search(conn, client, budget, args.pages, now)
        elif args.command == "daily":
            result = run_daily(conn, client, budget, args, now)
        else:
            result = take_snapshots(conn, client, budget, now, force=args.force, mode=args.mode)
    except Exception as exc:  # noqa: BLE001 - report, then stop: a refusal must not be retried in a loop
        kind = _failure_kind(exc)
        print(f"{args.command} stopped after {budget.used} requests: {type(exc).__name__}: {str(exc)[:200]}"
              + (" (X is refusing the session or the window is spent; wait about 15 minutes)" if kind != "other" else ""),
              file=sys.stderr)
        return 1
    finally:
        conn.commit()
    print(json.dumps({"command": args.command, "requests_used": budget.used, **result}, ensure_ascii=False))
    print(json.dumps(x_growth.status(conn), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
