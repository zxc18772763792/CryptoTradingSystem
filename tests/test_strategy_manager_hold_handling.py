"""HOLD signal handling regression tests.

Structural strategies (LiquidationOICrowdingStrategy, SupplyEventStrategy,
OnChainFlowRegimeStrategy) emit HOLD signals every bar to expose gate
context. Two ways those HOLDs used to break the runtime:

  1. ``_emit_signals`` wrote every HOLD into ``_recent_signal_by_symbol``,
     replacing a recent BUY/SELL. The next opposite-side entry then
     wouldn't trip conflict detection.
  2. ``execution_engine.submit_signal`` queued HOLDs into the signal queue
     even though ``execute_signal`` returns None for HOLD. The queue
     worker still paid for the circuit breaker + structural risk gate +
     position lookup per bar.

These tests lock the fixed behavior so a future change can't regress.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from core.strategies.strategy_base import Signal, SignalType, StrategyBase
from core.strategies.strategy_manager import StrategyManager


class _StubStrategy(StrategyBase):
    """Minimal strategy that records signals routed through it."""

    mutates_input = False

    def __init__(self, name: str = "stub"):
        super().__init__(name=name, params={})

    def generate_signals(self, data):  # noqa: ANN001 - test stub
        return []

    def get_required_data(self) -> Dict[str, Any]:
        return {"type": "kline", "columns": ["close"], "min_length": 1}


def _sig(symbol: str, kind: SignalType, strength: float, ts: datetime, name: str = "stub", account: str = "acct_A") -> Signal:
    return Signal(
        symbol=symbol,
        signal_type=kind,
        price=100.0,
        timestamp=ts,
        strategy_name=name,
        strength=strength,
        metadata={"account_id": account, "exchange": "binance"},
    )


def _make_manager() -> StrategyManager:
    """A bare StrategyManager with one registered strategy, no callbacks."""
    m = StrategyManager()
    stub = _StubStrategy("stub")
    m._strategies["stub"] = stub
    return m


def _run(coro):
    """Run a coroutine on a fresh loop without polluting the active one."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_hold_signal_does_not_overwrite_conflict_window():
    """A HOLD signal arriving after a BUY must not replace the BUY as the
    conflict-window incumbent — otherwise a subsequent opposite SELL
    would slip past conflict detection."""
    m = _make_manager()
    now = datetime.now(timezone.utc)
    buy = _sig("BTC/USDT", SignalType.BUY, 0.9, now)
    hold = _sig("BTC/USDT", SignalType.HOLD, 0.5, now + timedelta(seconds=5))

    with patch.object(m, "_dispatch_signal_callbacks", return_value=False) as _:
        _run(m._emit_signals("stub", [buy]))
        # Capture incumbent after the BUY.
        key = ("acct_A", "BTC/USDT", "binance")
        assert m._recent_signal_by_symbol[key] is buy

        _run(m._emit_signals("stub", [hold]))
        # The HOLD must NOT have overwritten the BUY incumbent.
        assert m._recent_signal_by_symbol[key] is buy, (
            "HOLD signal poisoned the conflict-window — a subsequent opposite "
            "entry would no longer trip conflict detection"
        )


def test_hold_signal_still_recorded_in_strategy_history():
    """HOLDs lose conflict + execution side-effects but their diagnostic
    value (gate status, structural reasons) must survive — strategies
    rely on signals_history for replay and UI."""
    m = _make_manager()
    hold = _sig("BTC/USDT", SignalType.HOLD, 0.5, datetime.now(timezone.utc))

    with patch.object(m, "_dispatch_signal_callbacks", return_value=False):
        _run(m._emit_signals("stub", [hold]))

    stub = m._strategies["stub"]
    assert any(s.signal_type == SignalType.HOLD for s in stub.signals_history), (
        "HOLD signals must still land in strategy.signals_history for "
        "diagnostic/UI consumption"
    )


def test_hold_signal_dispatches_to_callbacks():
    """HOLDs must still reach notify callbacks (logging, UI streaming,
    AI signal aggregator). Only execution dispatch is suppressed."""
    m = _make_manager()
    hold = _sig("BTC/USDT", SignalType.HOLD, 0.5, datetime.now(timezone.utc))

    seen: List[Signal] = []

    async def _dispatch(sig: Signal) -> bool:
        seen.append(sig)
        return False

    with patch.object(m, "_dispatch_signal_callbacks", side_effect=_dispatch):
        _run(m._emit_signals("stub", [hold]))

    assert len(seen) == 1 and seen[0].signal_type == SignalType.HOLD


def test_buy_signal_still_writes_conflict_window():
    """Regression guard: the HOLD short-circuit must not also short-circuit
    BUY/SELL — those still need to populate the conflict window."""
    m = _make_manager()
    now = datetime.now(timezone.utc)
    buy = _sig("BTC/USDT", SignalType.BUY, 0.9, now)

    with patch.object(m, "_dispatch_signal_callbacks", return_value=True):
        _run(m._emit_signals("stub", [buy]))

    key = ("acct_A", "BTC/USDT", "binance")
    assert m._recent_signal_by_symbol[key] is buy


def test_execution_engine_submit_signal_skips_hold_queue():
    """``submit_signal(HOLD)`` must accept the signal but not queue it.
    Locks bug B: HOLDs were filling the queue worker with no-op work."""
    from core.trading.execution_engine import execution_engine

    hold = _sig("BTC/USDT", SignalType.HOLD, 0.5, datetime.now(timezone.utc))

    async def _check() -> Dict[str, Any]:
        # Snapshot diagnostics + queue size inside the event loop so
        # ``_ensure_signal_queue`` can read ``asyncio.get_running_loop``.
        before_submitted = int(execution_engine._signal_diagnostics.get("submitted", 0))
        before_hold = int(execution_engine._signal_diagnostics.get("hold_skipped", 0))
        queue = execution_engine._ensure_signal_queue()
        qsize_before = queue.qsize()

        ok = await execution_engine.submit_signal(hold)
        return {
            "ok": ok,
            "qsize_before": qsize_before,
            "qsize_after": queue.qsize(),
            "submitted_before": before_submitted,
            "submitted_after": int(execution_engine._signal_diagnostics.get("submitted", 0)),
            "hold_before": before_hold,
            "hold_after": int(execution_engine._signal_diagnostics.get("hold_skipped", 0)),
        }

    result = _run(_check())

    assert result["ok"] is True, "HOLD should be accepted (not treated as a failure)"
    assert result["qsize_after"] == result["qsize_before"], (
        "HOLD must not be enqueued for execution"
    )
    assert result["submitted_after"] == result["submitted_before"] + 1
    assert result["hold_after"] == result["hold_before"] + 1
