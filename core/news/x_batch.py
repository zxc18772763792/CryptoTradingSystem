"""Scweet access for batch jobs (backfill, growth tracker) on the one logged-in X account.

Batch jobs use their own Scweet state file so their per-day counters do not get in the way of the
news worker, and pace themselves with core.news.x_pacing. Scweet turns many failures into an
empty list instead of raising, so ``guarded_call`` reads what it logged and what the state file
says about the last refusal before it believes an empty answer.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

from core.news.collectors.x_accounts import XRateLimited, _run_async, refusal_code, setting

ROOT = Path(__file__).resolve().parents[2]
BATCH_DB = ROOT / "data" / "x_accounts" / "scweet_backfill.db"
TRANSIENT = ("status=599", "curl:")
DAILY_LIMIT = 300  # Scweet's own per-day guard for the batch account; the counter spans every batch run of the day


class DailyBudgetSpent(XRateLimited):
    """Scweet refused to lease the account: its per-day request guard is used up (resets at UTC midnight)."""

    def __init__(self, detail: str = "") -> None:
        super().__init__(429)
        self.detail = detail

    def __str__(self) -> str:
        return "the batch account's daily request budget is spent; it resets at UTC midnight"


class Capture(logging.Handler):
    """Keep (level, message) of everything Scweet logs during one call."""

    def __init__(self, level: int) -> None:
        super().__init__(level=level)
        self.records: List[Tuple[int, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append((record.levelno, record.getMessage()))


def make_client(daily_limit: int = DAILY_LIMIT) -> Any:
    from Scweet import Scweet, ScweetConfig

    BATCH_DB.parent.mkdir(parents=True, exist_ok=True)
    return Scweet(
        auth_token=setting("X_AUTH_TOKEN"),
        db_path=str(BATCH_DB),
        proxy=setting("X_PROXY") or setting("HTTPS_PROXY") or setting("HTTP_PROXY") or None,
        manifest_scrape_on_init=True,
        # window_request_limit is Scweet's own pacing inside one read; x_pacing paces across reads and jobs.
        config=ScweetConfig(daily_requests_limit=daily_limit, window_request_limit=30, min_delay_s=2.0),
    )


def guarded_call(client: Any, factory: Callable[[], Any], *, debug: bool = False, retries: int = 2,
                 sleep: Callable[[float], None] = time.sleep) -> Tuple[Any, List[str]]:
    """Run one Scweet coroutine; return (rows, every message it logged).

    An empty answer is believed only when Scweet logged no problem and the state file records no
    401/403/429. A dropped connection (status=599 / curl) is retried; a refusal raises XRateLimited.
    With ``debug`` the Scweet logger is lowered to DEBUG for the call so per-user "not found" and
    "lookup failed" lines (logged at debug level) reach the caller.
    """
    scweet_logger = logging.getLogger("Scweet")
    previous = scweet_logger.level
    attempt = 0
    while True:
        attempt += 1
        capture = Capture(logging.DEBUG if debug else logging.WARNING)
        scweet_logger.addHandler(capture)
        if debug:
            scweet_logger.setLevel(logging.DEBUG)
        try:
            rows = _run_async(factory)
        finally:
            scweet_logger.removeHandler(capture)
            scweet_logger.setLevel(previous)
        messages = [m for _, m in capture.records]
        problems = [m for level, m in capture.records if level >= logging.WARNING or "lookup failed" in m]
        if rows:
            return rows, messages
        if any("lease unavailable" in m and "daily_limit" in m for m in problems):
            raise DailyBudgetSpent(problems[0][:200])
        if problems:
            if attempt <= retries and any(t in m for m in problems for t in TRANSIENT):
                sleep(4.0 * attempt)  # a dropped connection never reached X
                continue
            raise RuntimeError("X request failed: " + problems[0][:200])
        code: Optional[int] = refusal_code(client)
        if code:
            raise XRateLimited(code)
        return rows, messages
