from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Callable, Dict, Optional


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


ClearCallback = Callable[[], Any]
InspectCallback = Callable[[], Dict[str, Any]]


@dataclass
class CacheRegistration:
    name: str
    clear: ClearCallback
    inspect: Optional[InspectCallback] = None
    scope: str = "global"
    registered_at: datetime = field(default_factory=_utc_now)
    last_cleared_at: Optional[datetime] = None
    last_clear_error: Optional[str] = None


@dataclass
class TaskDiagnostics:
    task_name: str
    running: bool = False
    state: str = "stopped"
    restart_on_failure: bool = False
    restarts: int = 0
    last_started_at: Optional[datetime] = None
    last_stopped_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_error: Optional[str] = None


class RuntimeState:
    def __init__(self) -> None:
        self._lock = RLock()
        self._trading_mode = "paper"
        self._account_scope = "paper"
        self._visible_mode = "paper"
        self._mode_switch_in_progress = False
        self._last_mode_switch_at: Optional[datetime] = None
        self._last_mode_switch_reason = ""
        self._last_mode_switch_error = ""
        self._equity_value = 0.0
        self._equity_updated_at: Optional[datetime] = None
        self._task_diagnostics: Dict[str, TaskDiagnostics] = {}
        self._caches: Dict[str, CacheRegistration] = {}

    def initialize_mode(self, mode: str, *, reason: str = "startup") -> None:
        normalized = self._normalize_mode(mode)
        with self._lock:
            self._trading_mode = normalized
            self._visible_mode = normalized
            self._account_scope = normalized
            self._mode_switch_in_progress = False
            self._last_mode_switch_at = _utc_now()
            self._last_mode_switch_reason = str(reason or "")
            self._last_mode_switch_error = ""

    def begin_mode_switch(self, target_mode: str, *, reason: str = "") -> Dict[str, Any]:
        normalized = self._normalize_mode(target_mode)
        with self._lock:
            previous = self._visible_mode
            self._mode_switch_in_progress = True
            self._trading_mode = normalized
            self._account_scope = normalized
            self._last_mode_switch_reason = str(reason or "")
            self._last_mode_switch_error = ""
            return {"previous_mode": previous, "target_mode": normalized}

    def finish_mode_switch(self, target_mode: str, *, reason: str = "") -> None:
        normalized = self._normalize_mode(target_mode)
        with self._lock:
            self._trading_mode = normalized
            self._visible_mode = normalized
            self._account_scope = normalized
            self._mode_switch_in_progress = False
            self._last_mode_switch_at = _utc_now()
            self._last_mode_switch_reason = str(reason or self._last_mode_switch_reason or "")
            self._last_mode_switch_error = ""

    def fail_mode_switch(self, previous_mode: str, *, error: str) -> None:
        normalized = self._normalize_mode(previous_mode)
        with self._lock:
            self._trading_mode = normalized
            self._visible_mode = normalized
            self._account_scope = normalized
            self._mode_switch_in_progress = False
            self._last_mode_switch_at = _utc_now()
            self._last_mode_switch_error = str(error or "unknown error")

    def get_trading_mode(self) -> str:
        with self._lock:
            return self._visible_mode

    def get_account_scope(self) -> str:
        with self._lock:
            return self._account_scope

    def is_paper_mode(self) -> bool:
        return self.get_trading_mode() != "live"

    def update_equity_snapshot(self, equity_value: float, *, updated_at: Optional[datetime] = None) -> None:
        with self._lock:
            self._equity_value = float(equity_value or 0.0)
            self._equity_updated_at = updated_at or _utc_now()

    def register_cache(
        self,
        name: str,
        *,
        clear: ClearCallback,
        inspect: Optional[InspectCallback] = None,
        scope: str = "global",
    ) -> None:
        cache_name = str(name or "").strip()
        if not cache_name:
            raise ValueError("cache name is required")
        with self._lock:
            current = self._caches.get(cache_name)
            registered_at = current.registered_at if current else _utc_now()
            self._caches[cache_name] = CacheRegistration(
                name=cache_name,
                clear=clear,
                inspect=inspect,
                scope=str(scope or "global"),
                registered_at=registered_at,
                last_cleared_at=current.last_cleared_at if current else None,
                last_clear_error=current.last_clear_error if current else None,
            )

    def clear_registered_caches(self, *, scope: Optional[str] = None) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        with self._lock:
            items = list(self._caches.items())
        for cache_name, reg in items:
            if scope and reg.scope not in {scope, "global"}:
                continue
            try:
                clear_result = reg.clear()
                reg.last_cleared_at = _utc_now()
                reg.last_clear_error = None
                results[cache_name] = {"cleared": True, "result": clear_result}
            except Exception as exc:
                reg.last_clear_error = str(exc)
                results[cache_name] = {"cleared": False, "error": str(exc)}
        return results

    def inspect_registered_caches(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        with self._lock:
            items = list(self._caches.items())
        for cache_name, reg in items:
            payload: Dict[str, Any] = {
                "scope": reg.scope,
                "registered_at": reg.registered_at.isoformat(),
                "last_cleared_at": reg.last_cleared_at.isoformat() if reg.last_cleared_at else None,
                "last_clear_error": reg.last_clear_error,
            }
            if reg.inspect is not None:
                try:
                    payload.update(reg.inspect() or {})
                except Exception as exc:
                    payload["inspect_error"] = str(exc)
            out[cache_name] = payload
        return out

    def register_task(self, task_name: str, *, restart_on_failure: bool = False) -> None:
        name = str(task_name or "").strip()
        if not name:
            raise ValueError("task_name is required")
        with self._lock:
            existing = self._task_diagnostics.get(name)
            self._task_diagnostics[name] = TaskDiagnostics(
                task_name=name,
                running=existing.running if existing else False,
                state=existing.state if existing else "stopped",
                restart_on_failure=bool(restart_on_failure),
                restarts=existing.restarts if existing else 0,
                last_started_at=existing.last_started_at if existing else None,
                last_stopped_at=existing.last_stopped_at if existing else None,
                last_heartbeat_at=existing.last_heartbeat_at if existing else None,
                last_success_at=existing.last_success_at if existing else None,
                last_error=existing.last_error if existing else None,
            )

    def mark_task_started(self, task_name: str) -> None:
        with self._lock:
            diag = self._task_diagnostics.setdefault(task_name, TaskDiagnostics(task_name=task_name))
            diag.running = True
            diag.state = "running"
            diag.last_started_at = _utc_now()
            diag.last_heartbeat_at = diag.last_started_at
            diag.last_error = None

    def touch_task(self, task_name: str, *, success: bool = False) -> None:
        with self._lock:
            diag = self._task_diagnostics.setdefault(task_name, TaskDiagnostics(task_name=task_name))
            now = _utc_now()
            diag.last_heartbeat_at = now
            if success:
                diag.last_success_at = now

    def mark_task_stopped(self, task_name: str) -> None:
        with self._lock:
            diag = self._task_diagnostics.setdefault(task_name, TaskDiagnostics(task_name=task_name))
            diag.running = False
            diag.state = "stopped"
            diag.last_stopped_at = _utc_now()

    def mark_task_failed(self, task_name: str, error: str, *, will_restart: bool = False) -> None:
        with self._lock:
            diag = self._task_diagnostics.setdefault(task_name, TaskDiagnostics(task_name=task_name))
            diag.running = False
            diag.state = "restarting" if will_restart else "failed"
            diag.last_error = str(error or "unknown error")
            diag.last_stopped_at = _utc_now()
            if will_restart:
                diag.restarts += 1

    def get_task_diagnostics(self) -> Dict[str, Any]:
        with self._lock:
            items = list(self._task_diagnostics.items())
        return {
            name: {
                "task_name": diag.task_name,
                "running": diag.running,
                "state": diag.state,
                "restart_on_failure": diag.restart_on_failure,
                "restarts": diag.restarts,
                "last_started_at": diag.last_started_at.isoformat() if diag.last_started_at else None,
                "last_stopped_at": diag.last_stopped_at.isoformat() if diag.last_stopped_at else None,
                "last_heartbeat_at": diag.last_heartbeat_at.isoformat() if diag.last_heartbeat_at else None,
                "last_success_at": diag.last_success_at.isoformat() if diag.last_success_at else None,
                "last_error": diag.last_error,
            }
            for name, diag in items
        }

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "trading_mode": self._visible_mode,
                "account_scope": self._account_scope,
                "switch_in_progress": self._mode_switch_in_progress,
                "last_mode_switch_at": self._last_mode_switch_at.isoformat() if self._last_mode_switch_at else None,
                "last_mode_switch_reason": self._last_mode_switch_reason,
                "last_mode_switch_error": self._last_mode_switch_error or None,
                "equity_snapshot": {
                    "value": round(float(self._equity_value or 0.0), 8),
                    "updated_at": self._equity_updated_at.isoformat() if self._equity_updated_at else None,
                },
                "tasks": self.get_task_diagnostics(),
                "caches": self.inspect_registered_caches(),
            }

    @staticmethod
    def _normalize_mode(mode: str) -> str:
        return "live" if str(mode or "").strip().lower() == "live" else "paper"


runtime_state = RuntimeState()
