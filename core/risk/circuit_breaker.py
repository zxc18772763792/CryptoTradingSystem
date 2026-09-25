"""Portfolio / per-strategy drawdown circuit breaker (Phase 4.2).

Provides a singleton ``circuit_breaker`` that the execution engine consults
before dispatching any signal. State transitions:

    allow       -> normal trade flow
    close_only  -> only reduce-only / close orders permitted
    block       -> all orders rejected

Strategy-level trip uses daily / weekly drawdown thresholds against a
strategy-scoped equity curve. Portfolio-level trip uses the global equity
timeline from :mod:`core.risk.risk_manager`.

Trips are recorded persistently in ``data/cache/runtime_state/circuit_breaker.json``
so a process restart will not silently re-arm a strategy that was tripped.

The module is intentionally light on dependencies — it imports
``risk_manager`` lazily so the monitor task can be unit-tested with a fake
trade history.
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from loguru import logger

from config.settings import settings


# ── decision tokens ──
DECISION_ALLOW = "allow"
DECISION_CLOSE_ONLY = "close_only"
DECISION_BLOCK = "block"
_DECISIONS = (DECISION_ALLOW, DECISION_CLOSE_ONLY, DECISION_BLOCK)
_FALSE_TRIP_AUTO_CLEAR_MIN_RECORDED_DD = 0.20


@dataclass
class Decision:
    """Result of a circuit-breaker check.

    ``action`` is one of ``allow`` / ``close_only`` / ``block``.
    ``scope`` is ``strategy`` or ``portfolio`` (whichever was the binding
    constraint). For ``allow`` results, ``scope`` is ``ok``.
    """

    action: str
    scope: str = "ok"
    reason: str = ""
    strategy_name: Optional[str] = None
    tripped_at: Optional[str] = None

    @property
    def is_allow(self) -> bool:
        return self.action == DECISION_ALLOW

    @property
    def is_close_only(self) -> bool:
        return self.action == DECISION_CLOSE_ONLY

    @property
    def is_block(self) -> bool:
        return self.action == DECISION_BLOCK

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "scope": self.scope,
            "reason": self.reason,
            "strategy_name": self.strategy_name,
            "tripped_at": self.tripped_at,
        }


@dataclass
class _StrategyState:
    tripped: bool = False
    tripped_at: Optional[str] = None
    reason: str = ""
    daily_dd: float = 0.0
    weekly_dd: float = 0.0
    last_reset_at: Optional[str] = None
    last_reset_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class _PortfolioState:
    tripped: bool = False
    tripped_at: Optional[str] = None
    reason: str = ""
    daily_dd: float = 0.0
    weekly_dd: float = 0.0
    last_reset_at: Optional[str] = None
    last_reset_by: Optional[str] = None
    # Manual-reset latch: while active, the SAME persisting drawdown does not
    # immediately re-trip (the pre-2026-07-07 behavior made the reset button a
    # lie — re-tripped within minutes on the unchanged rolling window). The
    # latch pierces if the dd WORSENS beyond the reset-time level, and re-arms
    # once the dd first recovers below thresholds.
    manual_override_active: bool = False
    manual_override_daily_dd: float = 0.0
    manual_override_weekly_dd: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# Best-effort hook so the monitor task can request position-flatten without
# importing strategy_manager at module load time.
_close_positions_hook: Optional[Callable[[str, str], Any]] = None
# Main event loop captured at hook registration time. Used to schedule async
# hooks from worker threads (e.g. `asyncio.to_thread(run_circuit_breaker_checks)`).
_main_loop: Optional[asyncio.AbstractEventLoop] = None


def register_close_positions_hook(hook: Callable[[str, str], Any]) -> None:
    """Register a callable ``hook(strategy_name, reason)`` that closes positions.

    The hook may be sync or async. If async, the coroutine will be scheduled
    onto the running event loop (best-effort across thread / sync / async
    contexts).
    """
    global _close_positions_hook, _main_loop
    _close_positions_hook = hook
    try:
        _main_loop = asyncio.get_running_loop()
    except RuntimeError:
        _main_loop = None


class CircuitBreaker:
    """Thread-safe singleton state machine."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._strategies: Dict[str, _StrategyState] = {}
        self._portfolio = _PortfolioState()
        self._store_path: Path = (
            Path(getattr(settings, "CACHE_PATH", Path("./data/cache"))) / "runtime_state" / "circuit_breaker.json"
        )
        self._listeners: List[Callable[[str, Dict[str, Any]], None]] = []
        # Strong references to fire-and-forget close-position tasks. Without this
        # the event loop only holds a weak reference and may garbage-collect the
        # task mid-flight, silently dropping a breaker-triggered position close.
        self._bg_tasks: set = set()
        self._load_from_disk()

    # ── persistence ──
    def _load_from_disk(self) -> None:
        try:
            if not self._store_path.exists():
                return
            with self._store_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh) or {}
        except Exception as exc:
            logger.debug(f"circuit_breaker: could not load persisted state: {exc}")
            return
        try:
            for name, row in (data.get("strategies") or {}).items():
                if isinstance(row, dict):
                    self._strategies[str(name)] = _StrategyState(**{k: v for k, v in row.items() if k in _StrategyState.__dataclass_fields__})
            port = data.get("portfolio") or {}
            if isinstance(port, dict):
                self._portfolio = _PortfolioState(**{k: v for k, v in port.items() if k in _PortfolioState.__dataclass_fields__})
        except Exception as exc:
            logger.debug(f"circuit_breaker: could not deserialise persisted state: {exc}")

    def _persist(self) -> None:
        try:
            self._store_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "portfolio": self._portfolio.to_dict(),
                "strategies": {name: state.to_dict() for name, state in self._strategies.items()},
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }
            tmp = self._store_path.with_suffix(self._store_path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            tmp.replace(self._store_path)
        except Exception as exc:
            logger.debug(f"circuit_breaker: could not persist state: {exc}")

    # ── listeners (for notifications, audit) ──
    def add_listener(self, callback: Callable[[str, Dict[str, Any]], None]) -> None:
        with self._lock:
            self._listeners.append(callback)

    def _notify(self, event: str, payload: Dict[str, Any]) -> None:
        for cb in list(self._listeners):
            try:
                cb(event, payload)
            except Exception as exc:
                logger.debug(f"circuit_breaker listener {cb} raised: {exc}")

    # ── threshold getters ──
    @property
    def enabled(self) -> bool:
        return bool(getattr(settings, "CIRCUIT_BREAKER_ENABLED", True))

    @property
    def strategy_daily_threshold(self) -> float:
        return abs(float(getattr(settings, "CB_STRATEGY_DAILY_DD_PCT", 0.05) or 0.0))

    @property
    def strategy_weekly_threshold(self) -> float:
        return abs(float(getattr(settings, "CB_STRATEGY_WEEKLY_DD_PCT", 0.10) or 0.0))

    @property
    def portfolio_daily_threshold(self) -> float:
        return abs(float(getattr(settings, "CB_PORTFOLIO_DAILY_DD_PCT", 0.03) or 0.0))

    @property
    def portfolio_weekly_threshold(self) -> float:
        return abs(float(getattr(settings, "CB_PORTFOLIO_WEEKLY_DD_PCT", 0.06) or 0.0))

    # ── decision API ──
    def check_strategy(self, name: str) -> Decision:
        if not self.enabled:
            return Decision(action=DECISION_ALLOW)
        if not name:
            return Decision(action=DECISION_ALLOW)
        with self._lock:
            state = self._strategies.get(str(name))
            if state and state.tripped:
                return Decision(
                    action=DECISION_CLOSE_ONLY,
                    scope="strategy",
                    reason=state.reason,
                    strategy_name=name,
                    tripped_at=state.tripped_at,
                )
        return Decision(action=DECISION_ALLOW)

    def check_portfolio(self) -> Decision:
        if not self.enabled:
            return Decision(action=DECISION_ALLOW)
        with self._lock:
            if self._portfolio.tripped:
                return Decision(
                    action=DECISION_CLOSE_ONLY,
                    scope="portfolio",
                    reason=self._portfolio.reason,
                    tripped_at=self._portfolio.tripped_at,
                )
        return Decision(action=DECISION_ALLOW)

    def evaluate(self, *, strategy_name: Optional[str], is_reduce_only: bool) -> Decision:
        """Combined check used by execution engine.

        Reduce-only / close orders are *always* allowed — the breaker exists
        to stop the bleeding, not to lock positions in.
        """
        if is_reduce_only:
            return Decision(action=DECISION_ALLOW)
        portfolio = self.check_portfolio()
        if not portfolio.is_allow:
            return portfolio
        if strategy_name:
            return self.check_strategy(strategy_name)
        return Decision(action=DECISION_ALLOW)

    # ── trip / reset ──
    def trip_strategy(
        self,
        name: str,
        reason: str,
        *,
        daily_dd: float = 0.0,
        weekly_dd: float = 0.0,
    ) -> bool:
        """Trip a single strategy. Returns True if a state transition occurred."""
        if not name:
            return False
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            state = self._strategies.setdefault(str(name), _StrategyState())
            already = state.tripped
            state.tripped = True
            state.tripped_at = state.tripped_at if already else now_iso
            state.reason = reason
            state.daily_dd = float(daily_dd)
            state.weekly_dd = float(weekly_dd)
            self._persist()
        if not already:
            logger.warning(
                f"circuit_breaker: tripped strategy={name} reason={reason} "
                f"daily_dd={daily_dd:.4f} weekly_dd={weekly_dd:.4f}"
            )
            self._notify("strategy_tripped", {
                "strategy": name,
                "reason": reason,
                "daily_dd": daily_dd,
                "weekly_dd": weekly_dd,
                "tripped_at": now_iso,
            })
            # Best-effort fire-and-forget close. Resilient against thread /
            # sync / async contexts.
            self._fire_close_positions(name, reason)
        return not already

    def trip_portfolio(
        self,
        reason: str,
        *,
        daily_dd: float = 0.0,
        weekly_dd: float = 0.0,
    ) -> bool:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            already = self._portfolio.tripped
            self._portfolio.tripped = True
            self._portfolio.tripped_at = self._portfolio.tripped_at if already else now_iso
            self._portfolio.reason = reason
            self._portfolio.daily_dd = float(daily_dd)
            self._portfolio.weekly_dd = float(weekly_dd)
            self._persist()
        if not already:
            logger.warning(
                f"circuit_breaker: tripped PORTFOLIO reason={reason} "
                f"daily_dd={daily_dd:.4f} weekly_dd={weekly_dd:.4f}"
            )
            self._notify("portfolio_tripped", {
                "reason": reason,
                "daily_dd": daily_dd,
                "weekly_dd": weekly_dd,
                "tripped_at": now_iso,
            })
        return not already

    def reset_strategy(self, name: str, operator: str) -> bool:
        """Manual reset. Returns True if state was actually cleared."""
        if not name:
            return False
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            state = self._strategies.get(str(name))
            if not state or not state.tripped:
                return False
            state.tripped = False
            state.last_reset_at = now_iso
            state.last_reset_by = str(operator or "unknown")
            self._persist()
        logger.info(f"circuit_breaker: strategy {name} reset by {operator}")
        self._notify("strategy_reset", {
            "strategy": name,
            "operator": operator,
            "reset_at": now_iso,
        })
        return True

    def reset_portfolio(self, operator: str) -> bool:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            if not self._portfolio.tripped:
                return False
            self._portfolio.tripped = False
            self._portfolio.last_reset_at = now_iso
            self._portfolio.last_reset_by = str(operator or "unknown")
            # Arm the manual-override latch at the CURRENT dd level: the same
            # persisting rolling-window condition must not instantly undo the
            # operator's decision; only a worsening beyond this level trips.
            self._portfolio.manual_override_active = True
            self._portfolio.manual_override_daily_dd = float(self._portfolio.daily_dd or 0.0)
            self._portfolio.manual_override_weekly_dd = float(self._portfolio.weekly_dd or 0.0)
            self._persist()
        logger.info(
            f"circuit_breaker: portfolio reset by {operator}; manual-override latch armed "
            f"(daily_dd<={self._portfolio.manual_override_daily_dd:.4f}, "
            f"weekly_dd<={self._portfolio.manual_override_weekly_dd:.4f} suppressed until recovery)"
        )
        self._notify("portfolio_reset", {
            "operator": operator,
            "reset_at": now_iso,
        })
        return True

    # Piercing margin: the latch only suppresses the SAME condition; a further
    # 0.5pp drawdown beyond the reset-time level is a NEW deterioration and
    # trips normally (a real crash always pierces within one evaluation).
    MANUAL_OVERRIDE_PIERCE_MARGIN = 0.005

    def is_portfolio_trip_suppressed(self, daily_dd: float, weekly_dd: float) -> bool:
        """True when a breach should be swallowed by the manual-reset latch."""
        with self._lock:
            st = self._portfolio
            if st.tripped or not st.manual_override_active:
                return False
            margin = float(self.MANUAL_OVERRIDE_PIERCE_MARGIN)
            if float(daily_dd) > st.manual_override_daily_dd + margin:
                return False
            if float(weekly_dd) > st.manual_override_weekly_dd + margin:
                return False
            return True

    def rearm_portfolio_if_recovered(self) -> bool:
        """Clear the manual-override latch once the condition has cleared.

        Called on evaluation passes with NO portfolio breach: the drawdown has
        recovered below thresholds, so future breaches are genuinely new and
        the breaker re-arms to full strictness.
        """
        with self._lock:
            if not self._portfolio.manual_override_active:
                return False
            self._portfolio.manual_override_active = False
            self._portfolio.manual_override_daily_dd = 0.0
            self._portfolio.manual_override_weekly_dd = 0.0
            self._persist()
        logger.info("circuit_breaker: portfolio manual-override latch cleared (condition recovered); breaker re-armed")
        return True

    def tripped_strategy_states(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                name: state.to_dict()
                for name, state in self._strategies.items()
                if state.tripped
            }

    def portfolio_state(self) -> Dict[str, Any]:
        with self._lock:
            return self._portfolio.to_dict()

    def clear_strategy_false_trip(
        self,
        name: str,
        *,
        reason: str,
        daily_dd: float,
        weekly_dd: float,
        operator: str = "auto_false_trip_recalc",
    ) -> bool:
        if not name:
            return False
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            state = self._strategies.get(str(name))
            if not state or not state.tripped:
                return False
            state.tripped = False
            state.reason = reason
            state.daily_dd = float(daily_dd or 0.0)
            state.weekly_dd = float(weekly_dd or 0.0)
            state.last_reset_at = now_iso
            state.last_reset_by = str(operator or "auto_false_trip_recalc")
            self._persist()
        logger.info(f"circuit_breaker: auto-cleared false strategy trip {name}: {reason}")
        return True

    def clear_portfolio_false_trip(
        self,
        *,
        reason: str,
        daily_dd: float,
        weekly_dd: float,
        operator: str = "auto_false_trip_recalc",
    ) -> bool:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            if not self._portfolio.tripped:
                return False
            self._portfolio.tripped = False
            self._portfolio.reason = reason
            self._portfolio.daily_dd = float(daily_dd or 0.0)
            self._portfolio.weekly_dd = float(weekly_dd or 0.0)
            self._portfolio.last_reset_at = now_iso
            self._portfolio.last_reset_by = str(operator or "auto_false_trip_recalc")
            self._persist()
        logger.info(f"circuit_breaker: auto-cleared false portfolio trip: {reason}")
        return True

    def _fire_close_positions(self, name: str, reason: str) -> None:
        """Sync, best-effort fire of the close-positions hook.

        Handles three call contexts:
          (A) Sync context with no running loop (tests) → ``asyncio.run`` the
              coroutine on a fresh loop.
          (B) Inside the running event loop → schedule via
              ``loop.create_task``.
          (C) From a worker thread spawned by ``asyncio.to_thread`` while the
              main loop is still running → ``run_coroutine_threadsafe`` onto
              the captured ``_main_loop``.

        Sync hooks are simply invoked.
        """
        hook = _close_positions_hook
        if hook is None:
            return
        try:
            result = hook(name, f"circuit_breaker:{reason}")
        except Exception as exc:
            logger.debug(f"circuit_breaker: close-positions hook raised for {name}: {exc}")
            return
        if not asyncio.iscoroutine(result):
            return
        # Async hook → schedule it on whatever loop is reachable.
        try:
            running = asyncio.get_running_loop()
            task = running.create_task(result)
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
            return
        except RuntimeError:
            pass  # no loop in this thread
        loop = _main_loop
        if loop is not None and loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(result, loop)
                return
            except Exception as exc:
                logger.debug(f"circuit_breaker: run_coroutine_threadsafe failed for {name}: {exc}")
        try:
            asyncio.run(result)
        except Exception as exc:
            logger.debug(f"circuit_breaker: asyncio.run fallback failed for {name}: {exc}")

    # ── snapshots ──
    def snapshot(self, *, active_strategy_names: Optional[Iterable[Any]] = None) -> Dict[str, Any]:
        active_names = _normalize_strategy_names(active_strategy_names)
        with self._lock:
            strategies = {name: state.to_dict() for name, state in self._strategies.items()}
            active_strategies = None
            inactive_tripped_count = None
            if active_names is not None:
                active_strategies = {
                    name: state
                    for name, state in strategies.items()
                    if name in active_names
                }
                inactive_tripped_count = len([
                    state
                    for name, state in strategies.items()
                    if state.get("tripped") and name not in active_names
                ])
            return {
                "enabled": self.enabled,
                "portfolio": self._portfolio.to_dict(),
                "strategies": strategies,
                "active_strategy_names": sorted(active_names) if active_names is not None else None,
                "active_strategies": active_strategies,
                "inactive_tripped_count": inactive_tripped_count,
                "thresholds": {
                    "strategy_daily_pct": self.strategy_daily_threshold,
                    "strategy_weekly_pct": self.strategy_weekly_threshold,
                    "portfolio_daily_pct": self.portfolio_daily_threshold,
                    "portfolio_weekly_pct": self.portfolio_weekly_threshold,
                },
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }


circuit_breaker = CircuitBreaker()


# ──────────────────────────────────────────────────────────────────────────
# Monitoring / threshold evaluation
# ──────────────────────────────────────────────────────────────────────────

def _parse_iso(ts: Any) -> Optional[datetime]:
    text = str(ts or "").strip()
    if not text:
        return None
    try:
        out = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if out.tzinfo is None:
        out = out.replace(tzinfo=timezone.utc)
    return out.astimezone(timezone.utc)


def _resolve_account_equity() -> float:
    """Best-effort fetch of live account equity for drawdown anchoring."""
    def _safe_positive(value: Any) -> float:
        try:
            numeric = float(value or 0.0)
        except Exception:
            return 0.0
        return numeric if numeric > 0 else 0.0

    def _report_equity(report: Dict[str, Any]) -> float:
        equity = report.get("equity") if isinstance(report, dict) else {}
        if not isinstance(equity, dict):
            return 0.0
        for key in ("current", "risk_current", "risk_equity_input", "day_start"):
            numeric = _safe_positive(equity.get(key))
            if numeric > 0:
                return numeric
        return 0.0

    try:
        from core.risk.risk_manager import risk_manager  # noqa: PLC0415

        runtime_mode = _current_runtime_mode()
        if runtime_mode == "live":
            try:
                report = risk_manager.get_risk_report(scope="live") or {}
            except TypeError:
                report = risk_manager.get_risk_report() or {}
            equity = _report_equity(report)
            if equity >= _credible_equity_floor():
                return equity

            cached_equity = _resolve_cached_live_account_equity()
            if cached_equity > 0:
                return cached_equity

            report = risk_manager.get_risk_report() or {}
            equity = _report_equity(report)
            if equity >= _credible_equity_floor():
                return equity

        active_scope = ""
        try:
            active_scope = str(risk_manager.get_account_scope() or "").strip().lower()
        except Exception:
            active_scope = ""
        if runtime_mode != "live" or active_scope == "live":
            for attr in ("_current_equity", "_day_start_equity"):
                value = _safe_positive(getattr(risk_manager, attr, 0.0))
                if value > 0:
                    return value
        report = risk_manager.get_risk_report() or {}
        equity = _report_equity(report)
        if equity > 0:
            return equity
    except Exception:
        pass
    return 0.0


def _resolve_cached_live_account_equity() -> float:
    """Read the live balance baseline persisted by the trading balance view."""
    candidates = [
        Path(getattr(settings, "CACHE_PATH", "data/cache")) / "analytics" / "live_equity_baseline.json",
        Path("data/cache/analytics/live_equity_baseline.json"),
    ]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    floor = _credible_equity_floor()
    for path in candidates:
        try:
            if not path.exists():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue
            if str(payload.get("day") or "") != today:
                continue
            equity = float(payload.get("portfolio_total_usd") or 0.0)
            if equity >= floor:
                return equity
        except Exception as exc:
            logger.debug(f"circuit_breaker: failed to read live equity baseline {path}: {exc}")
    return 0.0


def _credible_equity_floor() -> float:
    return max(10.0, float(getattr(settings, "MIN_STRATEGY_ORDER_USD", 100.0) or 100.0))


def _should_auto_clear_false_trip(
    stored_state: Dict[str, Any],
    *,
    current_daily_dd: float,
    current_weekly_dd: float,
    daily_threshold: float,
    weekly_threshold: float,
    account_equity: float,
) -> bool:
    """Conservatively clear trips caused by a clearly bad equity denominator."""
    if account_equity < _credible_equity_floor():
        return False
    try:
        stored_daily = float((stored_state or {}).get("daily_dd") or 0.0)
        stored_weekly = float((stored_state or {}).get("weekly_dd") or 0.0)
    except Exception:
        return False
    if max(stored_daily, stored_weekly) < _FALSE_TRIP_AUTO_CLEAR_MIN_RECORDED_DD:
        return False
    if current_daily_dd >= daily_threshold or current_weekly_dd >= weekly_threshold:
        return False
    return (
        current_daily_dd <= max(daily_threshold * 0.5, 1e-9)
        and current_weekly_dd <= max(weekly_threshold * 0.5, 1e-9)
    )


def _normalize_strategy_names(names: Optional[Iterable[Any]]) -> Optional[Set[str]]:
    if names is None:
        return None
    normalized = {str(name or "").strip() for name in names}
    normalized.discard("")
    return normalized


def _runtime_row_scope(row: Dict[str, Any]) -> str:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    candidates = [
        row.get("runtime_mode"),
        row.get("trading_mode"),
        row.get("mode"),
        row.get("scope"),
        metadata.get("runtime_mode"),
        metadata.get("trading_mode"),
        metadata.get("mode"),
    ]
    for candidate in candidates:
        text = str(candidate or "").strip().lower()
        if text in {"paper", "live"}:
            return text
    return ""


def _trade_row_is_runtime_eligible(
    row: Dict[str, Any],
    *,
    active_names: Optional[Set[str]] = None,
    runtime_mode: Optional[str] = None,
) -> Tuple[bool, str]:
    """Return whether a trade-history row should be allowed to trip runtime CB.

    The risk manager keeps a mixed audit ledger. Backtests, stopped strategy
    imports, and old paper/live rows can all coexist there, so the circuit
    breaker must require a known active strategy scope before treating a row as
    runtime risk.
    """
    if not isinstance(row, dict):
        return False, "not_dict"
    name = str(row.get("strategy") or row.get("strategy_name") or "").strip()
    if not name:
        return False, "missing_strategy"
    if active_names is not None and name not in active_names:
        return False, "inactive_strategy"
    row_scope = _runtime_row_scope(row)
    order_id = str(row.get("order_id") or "").strip()
    if order_id.startswith("paper_") and (runtime_mode == "live" or row_scope == "live"):
        return False, "paper_order_in_live"
    if runtime_mode and row_scope and row_scope != runtime_mode:
        return False, "scope_mismatch"
    return True, ""


def _current_runtime_mode() -> Optional[str]:
    """Best-effort current execution mode for scoping runtime monitors."""
    try:
        from core.trading.execution_engine import execution_engine  # noqa: PLC0415

        for attr in ("_current_trading_mode", "get_trading_mode"):
            getter = getattr(execution_engine, attr, None)
            if not callable(getter):
                continue
            mode = str(getter() or "").strip().lower()
            if mode in {"paper", "live"}:
                return mode
    except Exception:
        pass
    try:
        from core.runtime.state import runtime_state  # noqa: PLC0415

        mode = str(runtime_state.get_trading_mode() or "").strip().lower()
        if mode in {"paper", "live"}:
            return mode
    except Exception:
        pass
    return None


def _resolve_active_strategy_names() -> Set[str]:
    """Return strategies that are currently running or have open exposure."""
    names: Set[str] = set()
    runtime_mode = _current_runtime_mode()

    try:
        from core.strategies.strategy_manager import strategy_manager  # noqa: PLC0415

        if runtime_mode:
            running = strategy_manager.get_running_strategies(runtime_mode=runtime_mode)
        else:
            running = strategy_manager.get_running_strategies()
        for strategy in running or []:
            name = str(getattr(strategy, "name", "") or "").strip()
            if name:
                names.add(name)
    except Exception as exc:
        logger.debug(f"circuit_breaker: failed to resolve running strategies: {exc}")

    try:
        from core.trading.position_manager import position_manager  # noqa: PLC0415

        positions = position_manager.get_all_positions(scope=runtime_mode) if runtime_mode else position_manager.get_all_positions()
        for position in positions or []:
            name = str(getattr(position, "strategy", "") or "").strip()
            if name:
                names.add(name)
    except Exception as exc:
        logger.debug(f"circuit_breaker: failed to resolve open-position strategies: {exc}")

    # The autonomous agent is not a strategy_manager strategy; while flat it
    # would otherwise drop out of the active set and a trip on it becomes
    # invisible in the risk view even though every fresh entry is refused.
    # Only consult it if already loaded — never import the agent from here.
    agent_module = sys.modules.get("core.ai.autonomous_agent")
    agent = getattr(agent_module, "autonomous_trading_agent", None) if agent_module else None
    if agent is not None:
        try:
            if agent.is_running():
                cfg = agent.get_runtime_config()
                names.add(str(cfg.get("strategy_name") or "AI_AutonomousAgent").strip())
        except Exception as exc:
            logger.debug(f"circuit_breaker: failed to resolve autonomous agent strategy: {exc}")

    return names


def _drawdown_from_pnl(
    rows: List[Dict[str, Any]],
    *,
    hours: int,
    base_capital_override: Optional[float] = None,
) -> float:
    """Compute peak-to-trough drawdown from a chronological trade list within window.

    ``rows`` should be dicts with ``timestamp`` (ISO string) and ``pnl`` numeric.
    The drawdown denominator is the live account equity if available, with
    trade-row ``capital_after`` / ``equity`` as fallbacks. Raw trade notional is
    deliberately not used as a denominator because it can hide leveraged losses.
    Returns drawdown as a *positive* float (e.g. 0.04 for -4%).
    """
    if not rows:
        return 0.0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(hours or 1)))
    cumulative = 0.0
    # Anchor curve at 0 so a single losing trade still registers a drawdown
    equity_curve: List[float] = [0.0]
    row_capital: Optional[float] = None
    for row in rows:
        ts = _parse_iso(row.get("timestamp"))
        if ts is None or ts < cutoff:
            continue
        try:
            pnl = float(row.get("pnl") or 0.0)
        except Exception:
            pnl = 0.0
        cumulative += pnl
        if row_capital is None:
            row_capital = float(row.get("capital_after") or row.get("equity") or 0.0) or None
        equity_curve.append(cumulative)
    if len(equity_curve) <= 1:
        return 0.0

    # Pick the most credible base capital reference. Order:
    # 1. explicit override (caller-supplied / risk_manager equity at call time)
    # 2. row-level capital snapshot
    base: float = 0.0
    if base_capital_override and base_capital_override > 0:
        base = float(base_capital_override)
    elif row_capital and row_capital > 0:
        base = float(row_capital)
    if base <= 0:
        fallback_loss = max(abs(min(equity_curve)), abs(max(equity_curve)))
        if fallback_loss < 1.0:
            # Tiny cost-only rows without an equity snapshot should not become
            # synthetic full drawdowns, but material losses must not be diluted
            # by high leveraged notional either.
            return 0.0
        base = fallback_loss

    peak = equity_curve[0]
    worst_dd = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        dd_abs = peak - value
        dd_pct = dd_abs / base if base > 0 else 0.0
        if dd_pct > worst_dd:
            worst_dd = dd_pct
    return float(worst_dd)


def _system_portfolio_drawdown_from_trade_history(
    trade_history: List[Dict[str, Any]],
    *,
    runtime_mode: Optional[str],
    account_equity: float,
    current_system_unrealized_pnl: float,
) -> Dict[str, Any]:
    def _is_manual_strategy_label(value: Any) -> bool:
        return str(value or "").strip().lower().startswith("manual")

    rows: List[Dict[str, Any]] = []
    for row in trade_history or []:
        if not isinstance(row, dict):
            continue
        strategy = str(row.get("strategy") or row.get("strategy_name") or "").strip()
        if _is_manual_strategy_label(strategy):
            continue
        action = str(row.get("action") or "").strip().lower()
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        source = str(metadata.get("source") or row.get("source") or "").strip().lower()
        if action == "manual_order" or source in {"manual", "external", "exchange_live"}:
            continue
        eligible, _ = _trade_row_is_runtime_eligible(
            row,
            active_names=None,
            runtime_mode=runtime_mode,
        )
        if eligible:
            rows.append(row)

    floating_loss = min(0.0, float(current_system_unrealized_pnl or 0.0))
    if abs(floating_loss) > 1e-9:
        rows.append(
            {
                "strategy": "__open_system_unrealized__",
                "pnl": floating_loss,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "mode": runtime_mode or "",
            }
        )

    return {
        "daily_dd": _drawdown_from_pnl(
            rows,
            hours=24,
            base_capital_override=account_equity if account_equity > 0 else None,
        ),
        "weekly_dd": _drawdown_from_pnl(
            rows,
            hours=24 * 7,
            base_capital_override=account_equity if account_equity > 0 else None,
        ),
        "source": "system_owned_pnl",
        "system_trade_rows": len(rows),
        "system_unrealized_pnl_usd": round(float(current_system_unrealized_pnl or 0.0), 4),
    }


def evaluate_strategy_drawdowns(
    trade_history: List[Dict[str, Any]],
    *,
    base_capital: Optional[float] = None,
    active_strategy_names: Optional[Iterable[Any]] = None,
    runtime_mode: Optional[str] = None,
) -> Dict[str, Dict[str, float]]:
    """Group ``trade_history`` by strategy and return per-strategy daily/weekly drawdowns.

    ``base_capital`` (if positive) is used as the drawdown denominator instead
    of relying on per-row fields. Typically callers pass
    ``risk_manager._current_equity``.
    """
    active_names = _normalize_strategy_names(active_strategy_names)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in trade_history or []:
        if not isinstance(row, dict):
            continue
        name = str(row.get("strategy") or row.get("strategy_name") or "").strip()
        if not name:
            continue
        eligible, _ = _trade_row_is_runtime_eligible(
            row,
            active_names=active_names,
            runtime_mode=runtime_mode,
        )
        if not eligible:
            continue
        grouped.setdefault(name, []).append(row)
    result: Dict[str, Dict[str, float]] = {}
    for name, rows in grouped.items():
        result[name] = {
            "daily_dd": _drawdown_from_pnl(rows, hours=24, base_capital_override=base_capital),
            "weekly_dd": _drawdown_from_pnl(rows, hours=24 * 7, base_capital_override=base_capital),
        }
    return result


def evaluate_portfolio_drawdown() -> Dict[str, Any]:
    """Use ``risk_manager`` equity timeline for portfolio-level drawdown.

    Besides the bare ratios, expose the binding peak/trough (value+timestamp)
    snapshots so an operator can see at a glance WHICH equity reading produced
    the drawdown — a phantom warmup reading becomes a one-glance diagnosis.
    """
    try:
        from core.risk.risk_manager import risk_manager  # noqa: PLC0415
    except Exception:
        return {"daily_dd": 0.0, "weekly_dd": 0.0}
    runtime_mode = _current_runtime_mode()
    if runtime_mode == "live":
        try:
            account_equity = _resolve_account_equity()
            trade_history = list(risk_manager.get_trade_history(limit=50000, scope="live") or [])
            current_system_unrealized = float(getattr(risk_manager, "_current_unrealized_pnl", 0.0) or 0.0)
            return _system_portfolio_drawdown_from_trade_history(
                trade_history,
                runtime_mode=runtime_mode,
                account_equity=account_equity,
                current_system_unrealized_pnl=current_system_unrealized,
            )
        except Exception as exc:
            logger.debug(f"circuit_breaker: system-owned portfolio drawdown failed: {exc}")
    try:
        daily_snap = risk_manager.get_rolling_drawdown_snapshot(hours=24)
        weekly_snap = risk_manager.get_rolling_drawdown_snapshot(hours=24 * 7)
    except Exception as exc:
        logger.debug(f"circuit_breaker: portfolio drawdown computation failed: {exc}")
        return {"daily_dd": 0.0, "weekly_dd": 0.0}
    return {
        "daily_dd": float(daily_snap.get("drawdown") or 0.0),
        "weekly_dd": float(weekly_snap.get("drawdown") or 0.0),
        "daily_snapshot": daily_snap,
        "weekly_snapshot": weekly_snap,
    }


def run_circuit_breaker_checks(
    *,
    trade_history: Optional[List[Dict[str, Any]]] = None,
    portfolio_drawdown: Optional[Dict[str, float]] = None,
    account_equity: Optional[float] = None,
    active_strategy_names: Optional[Iterable[Any]] = None,
    auto_clear_false_trips: bool = False,
) -> Dict[str, Any]:
    """Single evaluation pass. Returns a report dict.

    All inputs are optional so the function is unit-testable with synthetic
    data. When omitted we read from the live risk_manager.
    """
    history_was_supplied = trade_history is not None
    runtime_mode = _current_runtime_mode()
    if trade_history is None:
        try:
            from core.risk.risk_manager import risk_manager  # noqa: PLC0415
            trade_history = list(getattr(risk_manager, "_trade_history", []) or [])
        except Exception:
            trade_history = []
    active_names = _normalize_strategy_names(active_strategy_names)
    if active_names is None and not history_was_supplied:
        active_names = _resolve_active_strategy_names()
    if portfolio_drawdown is None:
        portfolio_drawdown = evaluate_portfolio_drawdown()
    if account_equity is None:
        account_equity = _resolve_account_equity()

    # Per-strategy drawdown is measured against the capital the strategy is
    # actually allocated (equity x allocation), NOT the whole portfolio. Using
    # total equity made the per-strategy breaker ~1/alloc too loose: a strategy
    # sized at DEFAULT_STRATEGY_ALLOCATION had to lose a large multiple of its own
    # book before hitting the threshold. Mirror the allocation risk_manager uses
    # for position sizing (allocated_capital = equity * allocation).
    try:
        _strategy_alloc = float(getattr(settings, "DEFAULT_STRATEGY_ALLOCATION", 0.15) or 0.15)
    except Exception:
        _strategy_alloc = 0.15
    _strategy_alloc = min(1.0, max(0.01, _strategy_alloc))
    per_strategy_base_capital = (
        float(account_equity) * _strategy_alloc
        if account_equity and float(account_equity) > 0
        else None
    )

    breaker = circuit_breaker
    if not breaker.enabled:
        return {"enabled": False, "skipped": True}

    report: Dict[str, Any] = {
        "enabled": True,
        "strategy_trips": [],
        "portfolio_trip": None,
        "strategy_dds": {},
        "portfolio_dd": portfolio_drawdown,
        "account_equity": float(account_equity or 0.0),
        "active_strategy_names": sorted(active_names) if active_names is not None else None,
        "runtime_mode": runtime_mode,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    strat_dds = evaluate_strategy_drawdowns(
        trade_history,
        base_capital=per_strategy_base_capital,
        active_strategy_names=active_names,
        runtime_mode=runtime_mode if not history_was_supplied else None,
    )
    report["strategy_dds"] = strat_dds
    daily_thr = breaker.strategy_daily_threshold
    weekly_thr = breaker.strategy_weekly_threshold
    strategy_auto_clears: List[Dict[str, Any]] = []

    tripped_strategy_states = breaker.tripped_strategy_states()
    if auto_clear_false_trips and tripped_strategy_states:
        full_strategy_dds = strat_dds
        if active_names is not None:
            full_strategy_dds = evaluate_strategy_drawdowns(
                trade_history,
                base_capital=per_strategy_base_capital,
                active_strategy_names=None,
                runtime_mode=runtime_mode if not history_was_supplied else None,
            )
        for name, stored_state in tripped_strategy_states.items():
            dds = full_strategy_dds.get(name) or {"daily_dd": 0.0, "weekly_dd": 0.0}
            current_daily = float(dds.get("daily_dd") or 0.0)
            current_weekly = float(dds.get("weekly_dd") or 0.0)
            if not _should_auto_clear_false_trip(
                stored_state,
                current_daily_dd=current_daily,
                current_weekly_dd=current_weekly,
                daily_threshold=daily_thr,
                weekly_threshold=weekly_thr,
                account_equity=float(account_equity or 0.0),
            ):
                continue
            reason = (
                "auto-cleared false trip after credible equity recalculation; "
                f"recomputed 24h_dd={current_daily:.4f}, 7d_dd={current_weekly:.4f}, "
                f"account_equity={float(account_equity or 0.0):.4f}"
            )
            if breaker.clear_strategy_false_trip(
                name,
                reason=reason,
                daily_dd=current_daily,
                weekly_dd=current_weekly,
            ):
                strategy_auto_clears.append({
                    "strategy": name,
                    "reason": reason,
                    "daily_dd": current_daily,
                    "weekly_dd": current_weekly,
                })
    report["strategy_auto_clears"] = strategy_auto_clears

    for name, dds in strat_dds.items():
        daily = float(dds.get("daily_dd") or 0.0)
        weekly = float(dds.get("weekly_dd") or 0.0)
        breaches: List[str] = []
        if daily_thr > 0 and daily >= daily_thr:
            breaches.append(f"24h_dd {daily:.4f} >= {daily_thr:.4f}")
        if weekly_thr > 0 and weekly >= weekly_thr:
            breaches.append(f"7d_dd {weekly:.4f} >= {weekly_thr:.4f}")
        if breaches:
            reason = "; ".join(breaches)
            transitioned = breaker.trip_strategy(name, reason, daily_dd=daily, weekly_dd=weekly)
            report["strategy_trips"].append({
                "strategy": name,
                "reason": reason,
                "daily_dd": daily,
                "weekly_dd": weekly,
                "new_trip": transitioned,
            })

    port_daily = float((portfolio_drawdown or {}).get("daily_dd") or 0.0)
    port_weekly = float((portfolio_drawdown or {}).get("weekly_dd") or 0.0)
    report["portfolio_auto_clear"] = None
    portfolio_state = breaker.portfolio_state()
    if auto_clear_false_trips and bool(portfolio_state.get("tripped")) and _should_auto_clear_false_trip(
        portfolio_state,
        current_daily_dd=port_daily,
        current_weekly_dd=port_weekly,
        daily_threshold=breaker.portfolio_daily_threshold,
        weekly_threshold=breaker.portfolio_weekly_threshold,
        account_equity=float(account_equity or 0.0),
    ):
        reason = (
            "auto-cleared false trip after credible equity recalculation; "
            f"recomputed 24h_dd={port_daily:.4f}, 7d_dd={port_weekly:.4f}, "
            f"account_equity={float(account_equity or 0.0):.4f}"
        )
        if breaker.clear_portfolio_false_trip(
            reason=reason,
            daily_dd=port_daily,
            weekly_dd=port_weekly,
        ):
            report["portfolio_auto_clear"] = {
                "reason": reason,
                "daily_dd": port_daily,
                "weekly_dd": port_weekly,
            }

    port_breaches: List[str] = []
    if breaker.portfolio_daily_threshold > 0 and port_daily >= breaker.portfolio_daily_threshold:
        port_breaches.append(f"24h_dd {port_daily:.4f} >= {breaker.portfolio_daily_threshold:.4f}")
    if breaker.portfolio_weekly_threshold > 0 and port_weekly >= breaker.portfolio_weekly_threshold:
        port_breaches.append(f"7d_dd {port_weekly:.4f} >= {breaker.portfolio_weekly_threshold:.4f}")
    if port_breaches:
        reason = "; ".join(port_breaches)
        if breaker.is_portfolio_trip_suppressed(port_daily, port_weekly):
            # Manual-reset latch: same persisting condition after an operator
            # reset — honor the human decision until it worsens or recovers.
            report["portfolio_trip"] = {
                "reason": reason,
                "daily_dd": port_daily,
                "weekly_dd": port_weekly,
                "new_trip": False,
                "suppressed_by_manual_reset": True,
            }
        else:
            transitioned = breaker.trip_portfolio(reason, daily_dd=port_daily, weekly_dd=port_weekly)
            report["portfolio_trip"] = {
                "reason": reason,
                "daily_dd": port_daily,
                "weekly_dd": port_weekly,
                "new_trip": transitioned,
            }
    else:
        # No breach -> the condition has cleared; re-arm the breaker to full
        # strictness if a manual-override latch was active.
        breaker.rearm_portfolio_if_recovered()

    return report


__all__ = [
    "CircuitBreaker",
    "Decision",
    "DECISION_ALLOW",
    "DECISION_CLOSE_ONLY",
    "DECISION_BLOCK",
    "circuit_breaker",
    "register_close_positions_hook",
    "run_circuit_breaker_checks",
    "evaluate_strategy_drawdowns",
    "evaluate_portfolio_drawdown",
]
