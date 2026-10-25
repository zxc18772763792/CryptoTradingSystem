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

ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "data" / "x_accounts" / "request_ledger.jsonl"
WINDOW_SECONDS = 900.0
WINDOW_MAX_REQUESTS = 40
_KEEP_SECONDS = 6 * 3600.0
_COMPACT_BYTES = 64 * 1024


def _entries(path: Path, now: float, horizon: float) -> List[Tuple[float, int]]:
    if not path.exists():
        return []
    found: List[Tuple[float, int]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
                at, count = float(row["t"]), int(row["n"])
            except (ValueError, KeyError, TypeError):
                continue
            if now - at < horizon:
                found.append((at, count))
    return found


def record(requests: int, who: str, *, path: Optional[Path] = None, now: Optional[float] = None) -> None:
    """Log `requests` about to be (or just) made. Never raises: a ledger problem must not stop a job."""
    target = path or LEDGER
    at = time.time() if now is None else now
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps({"t": round(at, 3), "n": int(requests), "who": str(who)}) + "\n")
        if target.stat().st_size > _COMPACT_BYTES:
            keep = _entries(target, at, _KEEP_SECONDS)
            target.write_text("".join(json.dumps({"t": t, "n": n}) + "\n" for t, n in keep), encoding="utf-8")
    except OSError:
        pass


def recent_total(*, path: Optional[Path] = None, now: Optional[float] = None) -> int:
    at = time.time() if now is None else now
    return sum(n for _, n in _entries(path or LEDGER, at, WINDOW_SECONDS))


def seconds_to_wait(needed: int, *, path: Optional[Path] = None, now: Optional[float] = None) -> float:
    """Seconds until `needed` more requests fit under the cap given what the ledger shows."""
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
