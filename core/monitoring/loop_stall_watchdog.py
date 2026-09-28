"""Event-loop stall watchdog: when the web loop stops responding, log what it was running.

Every freeze this service has had (2026-07 TLS bundle read, 2026-09 research
workbench disk IO, journal full reads, tracker schedule parsing) was a
synchronous call inside an async path, and each one took a hand-built probe
to find. This makes the probe permanent and cheap:

* a heartbeat coroutine on the loop stamps the time every HEARTBEAT_SEC;
* a daemon thread checks the stamp; once it is older than STALL_SEC it samples
  the loop thread's stack (``sys._current_frames``) every SAMPLE_SEC until
  the heartbeat resumes;
* the stall is then logged once, with its duration and the project frames
  seen most often across the samples, i.e. the code actually holding the loop.

Overhead: one sleep per HEARTBEAT_SEC on the loop and one idle check in a thread;
stack sampling happens only during a stall. ``recent_stalls()`` exposes the
last few for diagnostics endpoints.
"""
from __future__ import annotations

import asyncio
import collections
import os
import sys
import threading
import time
import traceback
from typing import Any, Deque, Dict, List, Optional

from loguru import logger

HEARTBEAT_SEC = 0.25
STALL_SEC = 1.0
SAMPLE_SEC = 0.2
MAX_SAMPLES = 200
KEEP_STALLS = 20
PROJECT_MARKERS = (f"{os.sep}core{os.sep}", f"{os.sep}web{os.sep}", f"{os.sep}strategies{os.sep}", f"{os.sep}scripts{os.sep}")

_state: Dict[str, Any] = {"beat": None, "loop_thread": None, "thread": None, "task": None}
_stalls: Deque[Dict[str, Any]] = collections.deque(maxlen=KEEP_STALLS)
_stop = threading.Event()


def _project_frames(frame) -> List[str]:
    """Innermost-last list of 'file:line fn' for frames inside this repository."""
    out = []
    for fs in traceback.extract_stack(frame):
        path = fs.filename
        if "site-packages" in path or not any(m in path for m in PROJECT_MARKERS):
            continue
        rel = path.split(f"crypto_trading_system{os.sep}", 1)[-1]
        out.append(f"{rel}:{fs.lineno} {fs.name}")
    return out


def _innermost(frame) -> str:
    fs = traceback.extract_stack(frame)[-1]
    return f"{os.path.basename(fs.filename)}:{fs.lineno} {fs.name}"


def summarize(samples: List[Dict[str, Any]], duration: float) -> Dict[str, Any]:
    """Collapse stack samples into the frames that held the loop most often."""
    leaf = collections.Counter(s["project"][-1] for s in samples if s["project"])
    native = collections.Counter(s["innermost"] for s in samples)
    first_with_project = next((s["project"] for s in samples if s["project"]), [])
    return {
        "at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_sec": round(duration, 2),
        "samples": len(samples),
        "top_project_frames": leaf.most_common(3),
        "top_innermost": native.most_common(3),
        "stack": first_with_project[-8:],
    }


def _watch() -> None:
    samples: List[Dict[str, Any]] = []
    stall_started: Optional[float] = None
    while not _stop.wait(SAMPLE_SEC if stall_started else HEARTBEAT_SEC):
        beat = _state["beat"]
        if beat is None:
            continue
        late = time.monotonic() - beat
        if late > STALL_SEC:
            if stall_started is None:
                stall_started, samples = beat, []
            frame = sys._current_frames().get(_state["loop_thread"])  # noqa: SLF001 - diagnostics only
            if frame is not None and len(samples) < MAX_SAMPLES:
                samples.append({"project": _project_frames(frame), "innermost": _innermost(frame)})
        elif stall_started is not None:
            record = summarize(samples, beat - stall_started)
            _stalls.append(record)
            logger.warning(
                "event loop stalled {:.1f}s | held by {} | innermost {} | stack: {}",
                record["duration_sec"], record["top_project_frames"], record["top_innermost"],
                " <- ".join(reversed(record["stack"])),
            )
            stall_started, samples = None, []


async def _heartbeat() -> None:
    while not _stop.is_set():
        _state["beat"] = time.monotonic()
        await asyncio.sleep(HEARTBEAT_SEC)


def start() -> None:
    """Start on the running loop (idempotent)."""
    if _state["task"] is not None and not _state["task"].done():
        return
    _stop.clear()
    _state["loop_thread"] = threading.get_ident()
    _state["beat"] = time.monotonic()
    _state["task"] = asyncio.get_running_loop().create_task(_heartbeat())
    if _state["thread"] is None or not _state["thread"].is_alive():
        _state["thread"] = threading.Thread(target=_watch, name="loop-stall-watchdog", daemon=True)
        _state["thread"].start()


def stop() -> None:
    _stop.set()
    task = _state.get("task")
    if task is not None:
        task.cancel()
    _state["task"] = None


def recent_stalls() -> List[Dict[str, Any]]:
    return list(_stalls)
