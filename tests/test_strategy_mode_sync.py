"""Regression tests for the strategy-mode/global-mode desync.

Live incident 2026-05-25:
- 22 strategies persisted with ``params['runtime_mode']='paper'``.
- Operator switched the system to live via /trading/mode/confirm.
- New signals still routed to the paper queue because each strategy
  instance's ``_runtime_mode`` was burned in at registration time, and
  ``_resolve_signal_trading_mode`` treats it as an explicit per-strategy
  override that wins over the new global mode.

These tests pin down (a) register_strategy no longer freezes the current
global mode into ``params`` unless the caller explicitly asked for it,
and (b) ``sync_runtime_mode_to_global`` updates already-running strategies
so the operator's mode switch takes effect without restart.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from core.strategies.strategy_base import Signal, StrategyBase
from core.strategies.strategy_manager import StrategyManager


class _DummyStrategy(StrategyBase):
    mutates_input = False

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        return []

    def get_required_data(self) -> Dict[str, Any]:
        return {"type": "kline", "columns": ["close"], "min_length": 10}


def _fresh_manager() -> StrategyManager:
    return StrategyManager()


def test_register_without_explicit_mode_does_not_freeze_runtime_mode_in_params(monkeypatch):
    """If the caller didn't pass a mode, params/metadata must not end up with
    a runtime_mode key — the strategy should track the global mode dynamically."""
    mgr = _fresh_manager()
    # Force the resolver to return 'live' as if global mode is live now.
    monkeypatch.setattr(mgr, "_resolve_strategy_runtime_mode", lambda *_a, **_k: "live")
    monkeypatch.setattr(mgr, "_sync_strategy_account", lambda *a, **k: None)

    ok = mgr.register_strategy(
        name="dummy_no_mode",
        strategy_class=_DummyStrategy,
        params={},
        symbols=["BTC/USDT"],
        timeframe="1h",
    )
    assert ok is True

    cfg = mgr._configs["dummy_no_mode"]
    assert "runtime_mode" not in cfg.params, (
        f"runtime_mode must not be frozen into params when caller did not "
        f"specify it (got params={cfg.params!r})"
    )
    assert "runtime_mode" not in cfg.metadata, (
        f"runtime_mode must not be frozen into metadata when caller did not "
        f"specify it (got metadata={cfg.metadata!r})"
    )


def test_register_with_explicit_mode_is_preserved(monkeypatch):
    """If the operator deliberately pins a strategy to e.g. paper for safe
    testing, that pin must survive — sync_runtime_mode_to_global must not
    silently flip it to live."""
    mgr = _fresh_manager()
    monkeypatch.setattr(mgr, "_resolve_strategy_runtime_mode", lambda *_a, **_k: "paper")
    monkeypatch.setattr(mgr, "_sync_strategy_account", lambda *a, **k: None)

    ok = mgr.register_strategy(
        name="dummy_explicit_paper",
        strategy_class=_DummyStrategy,
        params={"runtime_mode": "paper"},
        symbols=["BTC/USDT"],
        timeframe="1h",
    )
    assert ok is True
    assert mgr._configs["dummy_explicit_paper"].params["runtime_mode"] == "paper"

    # Global switches to live → pinned paper stays paper.
    updated = mgr.sync_runtime_mode_to_global("live")
    assert mgr._strategies["dummy_explicit_paper"].runtime_mode == "paper"
    # No mode change for this strategy → not counted in updated.
    assert updated == 0


def test_sync_runtime_mode_to_global_treats_empty_mode_key_as_pin(monkeypatch):
    """Presence of a mode key is a pin even if the stored value is empty."""
    mgr = _fresh_manager()
    monkeypatch.setattr(mgr, "_resolve_strategy_runtime_mode", lambda *_a, **_k: "paper")
    monkeypatch.setattr(mgr, "_sync_strategy_account", lambda *a, **k: None)

    ok = mgr.register_strategy(
        name="dummy_empty_mode_pin",
        strategy_class=_DummyStrategy,
        params={"runtime_mode": ""},
        symbols=["BTC/USDT"],
        timeframe="1h",
    )
    assert ok is True

    updated = mgr.sync_runtime_mode_to_global("live")
    assert updated == 0
    assert mgr._strategies["dummy_empty_mode_pin"].runtime_mode == "paper"


def test_sync_runtime_mode_to_global_updates_unpinned_strategies(monkeypatch):
    """The whole point: when global flips paper→live, every strategy that
    didn't pin its mode must follow the global without a restart."""
    mgr = _fresh_manager()
    # Register two strategies: one pinned, one not pinned.
    monkeypatch.setattr(mgr, "_resolve_strategy_runtime_mode", lambda *_a, **_k: "paper")
    monkeypatch.setattr(mgr, "_sync_strategy_account", lambda *a, **k: None)

    mgr.register_strategy(
        name="unpinned",
        strategy_class=_DummyStrategy,
        params={},
        symbols=["BTC/USDT"],
        timeframe="1h",
    )
    mgr.register_strategy(
        name="pinned_paper",
        strategy_class=_DummyStrategy,
        params={"runtime_mode": "paper"},
        symbols=["BTC/USDT"],
        timeframe="1h",
    )
    # Sanity: both currently 'paper' because resolver returned paper at registration.
    assert mgr._strategies["unpinned"].runtime_mode == "paper"
    assert mgr._strategies["pinned_paper"].runtime_mode == "paper"

    updated = mgr.sync_runtime_mode_to_global("live")
    # Unpinned strategy moves to live.
    assert mgr._strategies["unpinned"].runtime_mode == "live"
    # Pinned strategy stays paper.
    assert mgr._strategies["pinned_paper"].runtime_mode == "paper"
    assert updated == 1


def test_sync_runtime_mode_to_global_is_idempotent(monkeypatch):
    """Calling sync repeatedly with the same target must not log or count
    each strategy on every call — only when the mode actually changes."""
    mgr = _fresh_manager()
    monkeypatch.setattr(mgr, "_resolve_strategy_runtime_mode", lambda *_a, **_k: "live")
    monkeypatch.setattr(mgr, "_sync_strategy_account", lambda *a, **k: None)
    mgr.register_strategy(
        name="x",
        strategy_class=_DummyStrategy,
        params={},
        symbols=["BTC/USDT"],
        timeframe="1h",
    )
    # First sync may or may not flip (already live); a redundant second sync
    # must report 0.
    mgr.sync_runtime_mode_to_global("live")
    second = mgr.sync_runtime_mode_to_global("live")
    assert second == 0
