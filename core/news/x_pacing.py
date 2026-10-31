"""Shared request ledger for everything that uses the one logged-in X account.

The news worker, the backfill and the growth tracker are separate processes but share one
account, and X answers 429 once an account passes roughly 50 requests in 15 minutes (seen
on 2026-10-25 at 49). Each job records the requests it is about to make here, and batch jobs
wait until a read fits under ``WINDOW_MAX_REQUESTS`` for the whole window, whoever made the
earlier requests. The ledger is a small append-only JSONL file in the gitignored data/.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Callable, List, Optional, Tuple
from uuid import uuid4

from core.utils.file_lock import advisory_file_lock

ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "data" / "x_accounts" / "request_ledger.jsonl"
WINDOW_SECONDS = 900.0
WINDOW_MAX_REQUESTS = 40
_KEEP_SECONDS = 6 * 3600.0
_COMPACT_BYTES = 64 * 1024


class RequestWindowFull(RuntimeError):
    """The local shared request window has no room; no remote request was sent."""

    def __init__(self, retry_after: float) -> None:
        self.retry_after = retry_after
        super().__init__(f"shared X request window full; retry after {retry_after:.0f}s")


def _request_count(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= WINDOW_MAX_REQUESTS:
        raise ValueError(f"requests must be an integer from 1 to {WINDOW_MAX_REQUESTS}")
    return value


def _rows(path: Path, now: float, horizon: float) -> list[dict]:
    if not path.exists():
        return []
    found = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
                at, count = float(row["t"]), int(row["n"])
            except (ValueError, KeyError, TypeError):
                continue
            if now - at < horizon:
                found.append({**row, "t": at, "n": count})
    return found


def _entries(path: Path, now: float, horizon: float) -> List[Tuple[float, int]]:
    return [(row["t"], row["n"]) for row in _rows(path, now, horizon)]


def _append(target: Path, requests: int, who: str, at: float) -> None:
    """Append/compact while the caller holds the shared transaction lock."""
    with target.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"t": round(at, 3), "n": requests, "who": str(who)}) + "\n")
    if target.stat().st_size > _COMPACT_BYTES:
        keep = _rows(target, at, _KEEP_SECONDS)
        temporary = target.with_name(f"{target.name}.{uuid4().hex}.tmp")
        try:
            temporary.write_text("".join(json.dumps(row) + "\n" for row in keep), encoding="utf-8")
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)


def record(requests: int, who: str, *, path: Optional[Path] = None, now: Optional[float] = None) -> None:
    """Log `requests` about to be (or just) made. Never raises: a ledger problem must not stop a job."""
    target = path or LEDGER
    at = time.time() if now is None else now
    try:
        with advisory_file_lock(target):
            _append(target, int(requests), who, at)
    except OSError:
        pass


def recent_total(*, path: Optional[Path] = None, now: Optional[float] = None) -> int:
    at = time.time() if now is None else now
    return sum(n for _, n in _entries(path or LEDGER, at, WINDOW_SECONDS))


def seconds_to_wait(needed: int, *, path: Optional[Path] = None, now: Optional[float] = None) -> float:
    """Seconds until `needed` more requests fit under the cap given what the ledger shows."""
    needed = _request_count(needed)
    at = time.time() if now is None else now
    live = sorted(_entries(path or LEDGER, at, WINDOW_SECONDS))
    total = sum(n for _, n in live) + needed
    wait = 0.0
    for entry_time, count in live:  # drop the oldest entries until the rest fit
        if total <= WINDOW_MAX_REQUESTS:
            break
        wait = entry_time + WINDOW_SECONDS - at
        total -= count
    return max(0.0, wait)


def try_reserve(needed: int, who: str, *, path: Optional[Path] = None, now: Optional[float] = None) -> float:
    """Atomically check and reserve, or return the delay without spending room.

    Unlike observational ``record``, a failed ledger write is surfaced: callers
    cannot send a request whose shared reservation was not saved.
    """
    needed = _request_count(needed)
    target = Path(path) if path is not None else LEDGER
    with advisory_file_lock(target):
        at = time.time() if now is None else now
        pause = seconds_to_wait(needed, path=target, now=at)
        if pause > 0:
            return pause
        _append(target, needed, who, at)
    return 0.0


def reserve_requests(needed: int, who: str, *, path: Optional[Path] = None,
                     sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.time,
                     announce: Optional[Callable[[str], None]] = None) -> float:
    """Wait for an atomic reservation without holding the lock while sleeping."""
    waited = 0.0
    while True:
        pause = try_reserve(needed, who, path=path, now=clock())
        if pause <= 0:
            return waited
        pause = min(pause + 1.0, 120.0)
        if announce:
            announce(f"waiting {pause:.0f}s for X's shared request window")
        sleep(pause)
        waited += pause


def wait_for_room(needed: int, *, path: Optional[Path] = None, sleep: Callable[[float], None] = time.sleep,
                  clock: Callable[[], float] = time.time,
                  announce: Optional[Callable[[str], None]] = None) -> float:
    """Block until `needed` requests fit; re-checks after each sleep because other jobs may add entries."""
    waited = 0.0
    while True:
        pause = seconds_to_wait(needed, path=path, now=clock())
        if pause <= 0:
            return waited
        pause = min(pause + 1.0, 120.0)
        if announce:
            announce(f"waiting {pause:.0f}s for X's request window ({recent_total(path=path, now=clock())} used)")
        sleep(pause)
        waited += pause
