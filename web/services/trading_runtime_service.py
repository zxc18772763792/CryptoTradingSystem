from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import HTTPException

from core.realtime import event_bus
from core.runtime import runtime_state
from core.strategies import strategy_manager
from core.trading import (
    account_manager,
    account_snapshot_manager,
    execution_engine,
    order_manager,
    position_manager,
)
from core.risk.risk_manager import risk_manager


_MODE_CONFIRM_TEXT = "CONFIRM LIVE TRADING"
_mode_switch_pending: Dict[str, Dict[str, Any]] = {}


def get_mode_confirm_text() -> str:
    return _MODE_CONFIRM_TEXT


def list_pending_mode_switches(*, include_token: bool = False) -> list[Dict[str, Any]]:
    pending: list[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for token, item in list(_mode_switch_pending.items()):
        expires_at = item.get("expires_at")
        if expires_at and expires_at < now:
            _mode_switch_pending.pop(token, None)
            continue
        payload = {
            "target_mode": item.get("target_mode"),
            "reason": item.get("reason"),
            "created_at": item.get("created_at"),
            "expires_at": expires_at.isoformat() if expires_at else None,
        }
        if include_token:
            payload["token"] = token
        pending.append(payload)
    return pending


def request_mode_switch(*, target_mode: str, current_mode: str, reason: str = "") -> Dict[str, Any]:
    target = "live" if str(target_mode or "").strip().lower() == "live" else "paper"
    current = "live" if str(current_mode or "").strip().lower() == "live" else "paper"
    if target == current:
        return {"success": True, "mode": target, "message": "Already in target mode."}

    token = uuid4().hex
    created_at = datetime.now(timezone.utc)
    expires_at = created_at + timedelta(minutes=5)
    _mode_switch_pending[token] = {
        "target_mode": target,
        "reason": str(reason or ""),
        "created_at": created_at.isoformat(),
        "expires_at": expires_at,
    }
    return {
        "success": True,
        "token": token,
        "target_mode": target,
        "confirm_text": _MODE_CONFIRM_TEXT,
        "expires_at": expires_at.isoformat(),
        "warning": "Switching to live trading is high risk. Verify API permissions and risk settings.",
    }


def cancel_mode_switch(token: str) -> bool:
    if token in _mode_switch_pending:
        _mode_switch_pending.pop(token, None)
        return True
    return False


async def clear_local_trading_runtime(
    *,
    clear_paper_snapshots: bool = False,
    mode: str = "paper",
) -> Dict[str, Any]:
    normalized_mode = "live" if str(mode or "").strip().lower() == "live" else "paper"
    current_default_mode = execution_engine.get_trading_mode()
    runtime_reset = execution_engine.clear_paper_runtime() if normalized_mode == "paper" else {
        "conditional_orders_cleared": 0,
        "queued_signals_cleared": 0,
        "paper_fee_orders_cleared": 0,
        "paper_fee_total_cleared": 0.0,
    }
    order_reset = order_manager.clear_paper_history(mode=normalized_mode)
    position_reset = position_manager.clear_scope(normalized_mode)
    risk_manager.set_account_scope(normalized_mode, reset_baseline=False)
    risk_reset = risk_manager.clear_runtime_history()
    if current_default_mode != normalized_mode:
        risk_manager.set_account_scope(current_default_mode, reset_baseline=False)
    snapshots_deleted = 0
    if clear_paper_snapshots and normalized_mode == "paper":
        with contextlib.suppress(Exception):
            snapshots_deleted = int(await account_snapshot_manager.clear_history(mode="paper"))

    strategy_signal_cleared = 0
    strategy_position_cleared = 0
    for strategy in strategy_manager.get_all_strategies(runtime_mode=normalized_mode).values():
        try:
            strategy_signal_cleared += len(getattr(strategy, "signals_history", []) or [])
            strategy_position_cleared += len(getattr(strategy, "positions", {}) or {})
            strategy.signals_history.clear()
            strategy.positions.clear()
        except Exception:
            continue

    return {
        "runtime": runtime_reset,
        "orders": order_reset,
        "positions": position_reset,
        "risk": risk_reset,
        "snapshots_deleted": snapshots_deleted,
        "strategy_signal_cleared": strategy_signal_cleared,
        "strategy_position_cleared": strategy_position_cleared,
    }


async def _restart_runtime_workers(app: Any) -> Dict[str, Any]:
    supervisor = getattr(app.state, "runtime_supervisor", None)
    factories = dict(getattr(app.state, "runtime_task_factories", {}) or {})
    if supervisor is None:
        return {"stopped": [], "started": []}

    stopped = list(factories.keys())
    await supervisor.stop_all(timeout_sec=6.0)
    app.state.analytics_history_stop_events = {}
    app.state.analytics_history_tasks = {}
    started: list[str] = []
    for name, item in factories.items():
        managed = supervisor.start_task(
            name,
            item["factory"],
            restart_on_failure=bool(item.get("restart_on_failure", False)),
        )
        started.append(name)
        setattr(app.state, f"{name}_task", managed.task)
        setattr(app.state, f"{name}_stop_event", managed.stop_event)
        if str(name).startswith("analytics_history_"):
            collector = str(name).replace("analytics_history_", "", 1)
            app.state.analytics_history_stop_events[collector] = managed.stop_event
            app.state.analytics_history_tasks[collector] = managed.task
    return {"stopped": stopped, "started": started}


async def switch_trading_mode(
    *,
    token: Optional[str],
    confirm_text: str,
    app: Any,
    reason: str = "",
    clear_paper_snapshots: bool = True,
) -> Dict[str, Any]:
    pending = _mode_switch_pending.get(str(token or ""))
    if not pending:
        raise HTTPException(status_code=404, detail="Mode switch token not found")
    if pending.get("expires_at") and pending["expires_at"] < datetime.now(timezone.utc):
        _mode_switch_pending.pop(str(token or ""), None)
        raise HTTPException(status_code=400, detail="Mode switch token expired")
    if str(confirm_text or "").strip() != _MODE_CONFIRM_TEXT:
        raise HTTPException(status_code=400, detail="Confirmation text mismatch")

    target_mode = str(pending.get("target_mode") or "paper").strip().lower()
    previous_mode = runtime_state.get_trading_mode()
    was_running = bool(execution_engine.is_running)
    runtime_state.begin_mode_switch(target_mode, reason=reason or pending.get("reason") or "")

    cleanup_result: Dict[str, Any] = {}
    restart_result: Dict[str, Any] = {"stopped": [], "started": []}
    updated_accounts = 0
    strategies_stopped = 0
    try:
        if app is not None:
            restart_result = await _restart_runtime_workers(app)

        execution_engine.set_paper_trading(target_mode != "live", sync_runtime_state=False)
        try:
            updated_accounts = 1 if account_manager.set_mode("main", target_mode) else 0
        except Exception:
            updated_accounts = 0
        cleanup_result = {
            "skipped": True,
            "reason": "mode_switch_preserves_registered_and_running_strategies",
        }
        cache_reset = runtime_state.clear_registered_caches(scope=target_mode)

        if was_running and not execution_engine.is_running:
            await execution_engine.start()
        elif was_running and target_mode == "live":
            with contextlib.suppress(Exception):
                await execution_engine.prime_live_equity()

        runtime_state.finish_mode_switch(target_mode, reason=reason or pending.get("reason") or "")
        _mode_switch_pending.pop(str(token or ""), None)
        await event_bus.publish_nowait_safe(
            event="mode_changed",
            payload={
                "mode": runtime_state.get_trading_mode(),
                "updated_accounts": updated_accounts,
                "cleanup": cleanup_result,
                "cache_reset": cache_reset,
                "restart": restart_result,
            },
        )
        return {
            "success": True,
            "mode": runtime_state.get_trading_mode(),
            "paper_trading": runtime_state.is_paper_mode(),
            "updated_accounts": updated_accounts,
            "cleanup": cleanup_result,
            "cache_reset": cache_reset,
            "restart": restart_result,
            "strategies_stopped": strategies_stopped,
        }
    except Exception as exc:
        execution_engine.set_paper_trading(previous_mode != "live", sync_runtime_state=False)
        with contextlib.suppress(Exception):
            account_manager.set_mode("main", previous_mode)
        runtime_state.clear_registered_caches(scope=previous_mode)
        runtime_state.fail_mode_switch(previous_mode, error=str(exc))
        raise


async def ensure_trading_mode_started(target_mode: str) -> Dict[str, Any]:
    normalized = "live" if str(target_mode or "").strip().lower() == "live" else "paper"
    execution_engine.set_paper_trading(normalized != "live")
    with contextlib.suppress(Exception):
        account_manager.set_mode("main", normalized)
    runtime_state.clear_registered_caches(scope=normalized)
    if not execution_engine.is_running:
        await execution_engine.start()
    elif normalized == "live":
        with contextlib.suppress(Exception):
            await execution_engine.prime_live_equity()
    risk_scope = risk_manager.get_risk_report().get("scope")
    return {
        "running": bool(execution_engine.is_running),
        "mode": execution_engine.get_trading_mode(),
        "queue_size": int(execution_engine.get_queue_size()),
        "risk_scope": str(risk_scope or execution_engine.get_trading_mode()),
    }


def build_runtime_diagnostics() -> Dict[str, Any]:
    return runtime_state.snapshot()
