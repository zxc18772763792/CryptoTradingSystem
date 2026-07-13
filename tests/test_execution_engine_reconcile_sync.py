"""Reconcile sync: only quantity/entry divergence is a material change.

Live incident: `_sync_local_position_from_exchange` returned True whenever the
MARK PRICE moved, so every ~12s reconcile pass logged the
"Synchronized local live position" WARNING, fired the position_reconciled
callback ("exchange_position_size_changed" — a lie for price drift), and
force-persisted scope state to the exFAT volume around the clock. These tests
pin the fixed semantics: price-only drift refreshes fields silently with a
throttled persist; quantity/entry divergence stays a loud, force-persisted
material event.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from core.trading.execution_engine import ExecutionEngine
from core.trading.position_manager import PositionSide, position_manager


def _local_pos(quantity=95.0, entry=1.895, current=1.900, side=PositionSide.LONG):
    return SimpleNamespace(
        symbol="NEAR/USDT",
        exchange="binance",
        account_id="strategy_x",
        side=side,
        quantity=quantity,
        entry_price=entry,
        current_price=current,
        value=current * quantity,
        leverage=1.0,
        margin=current * quantity,
        unrealized_pnl=0.0,
        unrealized_pnl_pct=0.0,
        updated_at=datetime.now(timezone.utc),
        metadata={},
    )


def _snapshot(quantity=95.0, entry=1.895, current=1.900, upnl=0.5):
    return {
        "quantity": quantity,
        "entry_price": entry,
        "current_price": current,
        "unrealized_pnl": upnl,
        "leverage": 1.0,
    }


@pytest.fixture()
def persist_spy(monkeypatch):
    calls = []
    monkeypatch.setattr(
        position_manager,
        "_persist_scope_state",
        lambda force=False: calls.append(bool(force)),
    )
    return calls


@pytest.fixture()
def engine():
    return ExecutionEngine()


def test_price_only_drift_is_not_material(engine, persist_spy):
    pos = _local_pos(current=1.900)
    snap = _snapshot(current=1.955, upnl=5.7)  # same qty/entry, price moved

    material = engine._sync_local_position_from_exchange(pos, snap)

    assert material is False, "price drift must not be a material reconcile event"
    # Fields still refreshed so the local book stays current.
    assert pos.current_price == 1.955
    assert pos.unrealized_pnl == 5.7
    # Persisted, but throttled (force=False), not force-written.
    assert persist_spy == [False]


def test_quantity_divergence_is_material_and_force_persisted(engine, persist_spy):
    pos = _local_pos(quantity=95.0)
    snap = _snapshot(quantity=80.0)  # exchange shows a different size

    material = engine._sync_local_position_from_exchange(pos, snap)

    assert material is True
    assert pos.quantity == 80.0
    assert persist_spy == [True]


def test_entry_divergence_is_material(engine, persist_spy):
    pos = _local_pos(entry=1.895)
    snap = _snapshot(entry=2.100)

    material = engine._sync_local_position_from_exchange(pos, snap)

    assert material is True
    assert pos.entry_price == 2.100
    assert persist_spy == [True]


def test_no_change_at_all_is_a_noop(engine, persist_spy):
    pos = _local_pos()
    snap = _snapshot(upnl=0.0)

    material = engine._sync_local_position_from_exchange(pos, snap)

    assert material is False
    assert persist_spy == [], "identical snapshot must not touch persistence"
