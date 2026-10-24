"""Supervised runner for the Telegram forward paper cycle.

``scripts/telegram_forward_research.py cycle`` was driven by a chat-session
heartbeat, so the books silently stopped whenever that session did. This worker
runs the same command every few minutes under the web runtime-task supervisor.

The cycle runs in a child process on purpose: the script removes every loguru
handler when imported, which would blind the web process. The runner never edits
the research directory or the hashed protocol files; the CLI keeps its own
study/session locks and its FloodWait cooldown (``last_error.json``), and a busy
lock or active cooldown is reported as a skip, not as a failure.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable, Dict, Optional

from core.research.telegram_forward_tracker import DEFAULT_DIR, ROOT, research_dir

# Frozen CLI imported unchanged, with Binance funding requests paged (see the wrapper's docstring).
SCRIPT = ROOT / "scripts" / "telegram_forward_cycle.py"
STATE_PATH = ROOT / "runtime" / "telegram_forward_runner.json"
POLL_SECONDS = 300
SKIP_IF_SUCCESS_WITHIN = 120
CYCLE_TIMEOUT_SECONDS = 420


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _read(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_state(state: Dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = STATE_PATH.with_name(STATE_PATH.name + ".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(STATE_PATH)


def _seconds_since(value: Any, now: datetime) -> Optional[float]:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return (now - stamp).total_seconds()


def should_run(now: Optional[datetime] = None, base: Optional[Path] = None) -> str:
    """Return ``"run"`` or the reason to skip this tick."""
    now = now or _now()
    base = base or research_dir()
    if not (base / "activation.json").exists():
        return "not_activated"
    recent = _seconds_since(_read(base / "forward_status.json").get("checked_at"), now)
    if recent is not None and 0 <= recent < SKIP_IF_SUCCESS_WITHIN:
        return "recent_success"
    cooldown = _read(base / "last_error.json").get("retry_at")
    remaining = _seconds_since(cooldown, now) if cooldown else None
    if remaining is not None and remaining < 0:
        return "cooldown"
    return "run"


def _error_type(output: str) -> str:
    for line in reversed(output.strip().splitlines()):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            return str(value.get("error_type") or "")
    return ""


def _classify(returncode: int, output: str) -> str:
    if returncode == 0:
        return "ok"
    kind = _error_type(output)
    if kind in ("CooldownActive", "FloodWaitError"):
        return "cooldown"
    if kind == "PermissionError":  # study/session lock held elsewhere (or authorization record missing)
        return "busy"
    return "failed"


async def run_cycle(base: Optional[Path] = None) -> Dict[str, Any]:
    base = base or research_dir()
    command = [sys.executable, str(SCRIPT), "cycle"]
    if base != DEFAULT_DIR:
        command += ["--output", str(base)]
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    proc = await asyncio.create_subprocess_exec(
        *command, cwd=str(ROOT), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        creationflags=flags)
    try:
        raw, _ = await asyncio.wait_for(proc.communicate(), timeout=CYCLE_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return {"returncode": -1, "outcome": "timeout", "error_type": "timeout"}
    except asyncio.CancelledError:
        proc.kill()
        raise
    text = (raw or b"").decode("utf-8", "replace")
    return {"returncode": proc.returncode, "outcome": _classify(proc.returncode, text),
            "error_type": _error_type(text) if proc.returncode else ""}


async def worker(stop_event: asyncio.Event, heartbeat: Optional[Callable[[bool], None]] = None) -> None:
    state: Dict[str, Any] = {"enabled": True, "started_at": _now().isoformat()}
    _write_state(state)
    try:
        while not stop_event.is_set():
            verdict = should_run()
            if verdict == "run":
                result = await run_cycle()
                busy = state.get("busy_streak", 0) + 1 if result["outcome"] == "busy" else 0
                state.update(last_run_at=_now().isoformat(), last_exit_code=result["returncode"],
                             last_outcome=result["outcome"], last_error_type=result["error_type"], busy_streak=busy)
                # A held lock is normal for a tick or two; a lock that never frees (or a missing
                # authorization record, which raises the same error type) must show as unhealthy.
                ok = result["outcome"] in ("ok", "cooldown") or (result["outcome"] == "busy" and busy < 3)
            else:
                state.update(last_skip_at=_now().isoformat(), last_outcome=verdict)
                ok = verdict != "not_activated"
            _write_state(state)
            if heartbeat:
                heartbeat(ok)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
            except asyncio.TimeoutError:
                pass
    finally:
        state.update(enabled=False, stopped_at=_now().isoformat())
        _write_state(state)
