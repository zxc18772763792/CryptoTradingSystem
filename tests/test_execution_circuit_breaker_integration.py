"""Integration: circuit breaker gate inside ExecutionEngine.

The execution engine is heavy (account_manager, position_manager, exchange
connectors, etc.). These tests exercise only the very top of
``_execute_signal_in_active_mode`` — the new CB gate — by stubbing the
downstream paths and asserting the gate's three states.
"""
from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

cb_mod = importlib.import_module("core.risk.circuit_breaker")
from core.strategies.strategy_base import Signal, SignalType
from core.trading.execution_engine import ExecutionEngine


@pytest.fixture
def fresh_breaker(tmp_path, monkeypatch):
    monkeypatch.setattr(cb_mod.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(cb_mod.settings, "CIRCUIT_BREAKER_ENABLED", True, raising=False)
    breaker = cb_mod.CircuitBreaker()
    breaker._store_path = tmp_path / "runtime_state" / "circuit_breaker.json"
    monkeypatch.setattr(cb_mod, "circuit_breaker", breaker)
    return breaker


@pytest.fixture
def engine():
    return ExecutionEngine()


def _signal(strategy: str, signal_type: SignalType, *, close_only: bool = False) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        signal_type=signal_type,
        price=50000.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name=strategy,
        strength=0.8,
        metadata={"close_only": close_only, "account_id": "main"},
    )


def test_signal_allowed_when_no_trip(engine, fresh_breaker):
    """Without a trip, the gate falls through to downstream dispatch."""
    sentinel = {"called": False}

    async def _stub_close(signal, side):
        sentinel["called"] = True
        return {"status": "stub_close"}

    async def _run():
        with patch.object(engine, "_close_position_in_active_mode", side_effect=_stub_close):
            sig = _signal("StratA", SignalType.CLOSE_LONG)
            await engine._execute_signal_in_active_mode(sig)

    asyncio.run(_run())
    assert sentinel["called"] is True


def test_strategy_trip_blocks_open_signal(engine, fresh_breaker):
    fresh_breaker.trip_strategy("StratA", "test trip", daily_dd=0.06)

    close_mock = AsyncMock()
    resolve_mock = AsyncMock()

    async def _run():
        with patch.object(engine, "_close_position_in_active_mode", close_mock), \
             patch.object(engine, "_resolve_existing_position", resolve_mock):
            sig = _signal("StratA", SignalType.BUY)
            return await engine._execute_signal_in_active_mode(sig)

    result = asyncio.run(_run())
    assert result is None
    assert close_mock.await_count == 0
    assert resolve_mock.await_count == 0
    diag = engine._signal_diagnostics.get("last_result") or {}
    assert diag.get("status") == "circuit_breaker_blocked"
    assert diag.get("scope") == "strategy"


def test_portfolio_trip_blocks_signal_for_any_strategy(engine, fresh_breaker):
    fresh_breaker.trip_portfolio("portfolio trip", daily_dd=0.04)

    close_mock = AsyncMock()

    async def _run():
        with patch.object(engine, "_close_position_in_active_mode", close_mock):
            sig = _signal("StratXYZ", SignalType.SELL)
            return await engine._execute_signal_in_active_mode(sig)

    result = asyncio.run(_run())
    assert result is None
    assert close_mock.await_count == 0
    diag = engine._signal_diagnostics.get("last_result") or {}
    assert diag.get("scope") == "portfolio"


def test_close_signal_passes_through_even_when_tripped(engine, fresh_breaker):
    """Reduce-only / close orders must never be blocked by the breaker."""
    fresh_breaker.trip_portfolio("portfolio trip", daily_dd=0.04)
    fresh_breaker.trip_strategy("StratA", "strat trip", daily_dd=0.06)

    sentinel = {"called": False}

    async def _stub_close(signal, side):
        sentinel["called"] = True
        return {"status": "ok"}

    async def _run():
        with patch.object(engine, "_close_position_in_active_mode", side_effect=_stub_close):
            sig = _signal("StratA", SignalType.CLOSE_LONG)
            await engine._execute_signal_in_active_mode(sig)

    asyncio.run(_run())
    assert sentinel["called"] is True


def test_close_only_metadata_allows_dispatch(engine, fresh_breaker):
    """Signals with ``close_only=True`` metadata should pass the gate."""
    fresh_breaker.trip_strategy("StratA", "strat trip", daily_dd=0.06)

    async def _run():
        with patch.object(engine, "_resolve_existing_position", new=AsyncMock(return_value=None)), \
             patch.object(engine, "_resolve_strategy_trade_policy", return_value={
                 "allow_long": True, "allow_short": True,
                 "allow_pyramiding": False, "reverse_on_signal": True,
             }), \
             patch.object(engine, "_resolve_signal_exchange", return_value="binance"):
            sig = _signal("StratA", SignalType.BUY, close_only=True)
            return await engine._execute_signal_in_active_mode(sig)

    asyncio.run(_run())
    diag = engine._signal_diagnostics.get("last_result") or {}
    assert diag.get("status") != "circuit_breaker_blocked"


def test_disabled_breaker_does_not_block(engine, fresh_breaker, monkeypatch):
    fresh_breaker.trip_strategy("StratA", "trip", daily_dd=0.06)
    monkeypatch.setattr(cb_mod.settings, "CIRCUIT_BREAKER_ENABLED", False, raising=False)

    async def _run():
        with patch.object(engine, "_resolve_existing_position", new=AsyncMock(return_value=None)), \
             patch.object(engine, "_resolve_strategy_trade_policy", return_value={
                 "allow_long": True, "allow_short": True,
                 "allow_pyramiding": False, "reverse_on_signal": True,
             }), \
             patch.object(engine, "_resolve_signal_exchange", return_value="binance"):
            sig = _signal("StratA", SignalType.BUY)
            await engine._execute_signal_in_active_mode(sig)

    asyncio.run(_run())
    diag = engine._signal_diagnostics.get("last_result") or {}
    assert diag.get("status") != "circuit_breaker_blocked"
