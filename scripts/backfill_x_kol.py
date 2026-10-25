"""Backfill old timeline pages of allowlisted X accounts into the research archive.

Every page is a real request on the logged-in account (about 20 posts each), and the
account is the user's dedicated throwaway: a ban ends live collection too. So this
script is budgeted and conservative:

* ``--dry-run`` prints the plan and the request estimate without touching X;
* it refuses a plan above ``--max-requests`` (default 220);
* it uses its own Scweet state file and a sliding window of its own: at most 40 requests in any 15
  minutes (Scweet's limiter restarts with every read, and X answered 429 at 49 in a window);
* it stops at the first rejected login, rate limit or exhausted budget instead of
  pushing on, and an empty answer that came with warnings counts as a failure.

Handles take an optional depth: ``watcherguru:600 cryptokaleo:300``. Depth is the
number of newest posts to read (X exposes about 3,200 per account). Posts are archived
as ``provenance=backfill``; re-running only adds what is not archived yet.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import settings
from core.news.collectors.x_accounts import (
    XAccountsCollector, XRateLimited, _ScweetWarnings, _failure_kind, _run_async, refusal_code, setting,
)
from core.news.x_archive import observe, oldest_created
from core.utils.proxy_env import ensure_proxy_env

BACKFILL_DB = ROOT / "data" / "x_accounts" / "scweet_backfill.db"
POSTS_PER_PAGE = 20
# X refused the account after 49 requests inside 15 minutes, and again when two 26-request reads sat 13 minutes
# apart. Scweet's limiter restarts with every read, so the script keeps its own sliding window: no more than
# WINDOW_MAX_REQUESTS requests inside any WINDOW_SECONDS, whatever the reads are.
WINDOW_SECONDS = 900.0
WINDOW_MAX_REQUESTS = 40


def parse_plan(items: list[str], default_depth: int) -> list[tuple[str, int]]:
    plan = []
    for item in items:
        handle, _, depth = item.partition(":")
        handle = handle.strip().removeprefix("@").lower()
        plan.append((handle, max(20, min(int(depth) if depth else default_depth, 3200))))
    return plan


def seconds_to_wait(history: list[tuple[float, int]], needed: int, now: float) -> float:
    """How long until `needed` more requests fit under the window cap, given (finished_at, requests) history."""
    live = [(at, n) for at, n in history if now - at < WINDOW_SECONDS]
    total = sum(n for _, n in live) + needed
    wait = 0.0
    for at, n in sorted(live):  # drop the oldest reads until the rest fit
        if total <= WINDOW_MAX_REQUESTS:
            break
        wait = at + WINDOW_SECONDS - now
        total -= n
    return max(0.0, wait)


def estimate_requests(plan: list[tuple[str, int]]) -> int:
    # one timeline request per page, plus one user lookup per account on first sight
    return sum(math.ceil(depth / POSTS_PER_PAGE) + 1 for _handle, depth in plan)


def make_client(daily_limit: int):
    from Scweet import Scweet, ScweetConfig

    BACKFILL_DB.parent.mkdir(parents=True, exist_ok=True)
    return Scweet(
        auth_token=setting("X_AUTH_TOKEN"),
        db_path=str(BACKFILL_DB),
        proxy=setting("X_PROXY") or setting("HTTPS_PROXY") or setting("HTTP_PROXY") or None,
        manifest_scrape_on_init=True,
        # X returned 429 after 49 requests inside one window on 2026-10-25 (Scweet's measured ceiling is 50 per
        # 15 minutes), so pace to well under it: 30 per window, at least 2 seconds apart.
        config=ScweetConfig(daily_requests_limit=daily_limit, window_request_limit=30, min_delay_s=2.0),
    )


def read_depth(client, handle: str, depth: int) -> list:
    """One handle, newest `depth` posts. Raises when Scweet answered an empty list with warnings."""
    scweet_logger = logging.getLogger("Scweet")
    for attempt in (1, 2, 3):
        capture = _ScweetWarnings()
        scweet_logger.addHandler(capture)
        try:
            tweets = _run_async(lambda: client.aget_profile_tweets([handle], limit=depth))
        finally:
            scweet_logger.removeHandler(capture)
        if tweets:
            return tweets
        if not capture.messages:
            code = refusal_code(client)  # Scweet answers a 429 with an empty list and no warning
            if code:
                raise XRateLimited(code)
            return tweets
        if attempt < 3 and any("status=599" in m or "curl:" in m for m in capture.messages):
            time.sleep(4.0 * attempt)  # a dropped connection never reached X
            continue
        raise RuntimeError("X request failed: " + capture.messages[0][:200])
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("handles", nargs="+", help="handle or handle:depth, without @")
    parser.add_argument("--depth", type=int, default=300, help="default newest posts per account")
    parser.add_argument("--max-requests", type=int, default=220, help="refuse a plan estimated above this")
    parser.add_argument("--pause", type=float, default=10.0, help="seconds between accounts (plus jitter)")
    parser.add_argument("--dry-run", action="store_true", help="print the plan only; no request is sent")
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    plan = parse_plan(args.handles, args.depth)
    estimate = estimate_requests(plan)
    print(json.dumps({"plan": plan, "estimated_requests": estimate, "max_requests": args.max_requests}))
    if estimate > args.max_requests:
        print(f"Refusing: estimate {estimate} is above --max-requests {args.max_requests}.", file=sys.stderr)
        return 2
    if args.dry_run:
        return 0

    ensure_proxy_env(settings)
    # Also validates the token and proxy settings the same way the news collector does.
    XAccountsCollector({"defaults": {"x_accounts_handles": [h for h, _ in plan]}})
    client = make_client(daily_limit=args.max_requests + 40)
    results = {}
    history: list[tuple[float, int]] = []
    try:
        for index, (handle, depth) in enumerate(plan):
            pause = max(args.pause if index else 0.0,
                        seconds_to_wait(history, math.ceil(depth / POSTS_PER_PAGE) + 1, time.time()))
            if pause:
                print(f"waiting {pause:.0f}s before {handle} to stay under X's window", flush=True)
                time.sleep(pause + random.uniform(0, 4))
            try:
                tweets = read_depth(client, handle, depth)
            except Exception as exc:  # noqa: BLE001 - classify, then decide whether to go on
                kind = _failure_kind(exc)
                results[handle] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
                print(f"{handle}: FAILED {results[handle]['error']}")
                if kind in {"auth", "rate_limit"}:
                    print("Stopping: X is refusing the session or the budget is spent.", file=sys.stderr)
                    break
                continue
            history.append((time.time(), math.ceil(len(tweets) / POSTS_PER_PAGE) + 1))
            saved = observe(handle, tweets, provenance="backfill")
            results[handle] = {**saved, "oldest": oldest_created(handle)}
            print(f"{handle}: read {len(tweets)} new_in_archive={saved['new']} oldest={results[handle]['oldest']}")
            if refusal_code(client) == 429:
                # The read may have stopped short on a 429; keep what came, and do not ask again now.
                results[handle]["partial"] = True
                print("Stopping: X answered 429 during this read; wait about 15 minutes before resuming.",
                      file=sys.stderr)
                break
    finally:
        print(json.dumps(results, ensure_ascii=False))
    return 1 if any("error" in r for r in results.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
