"""End-to-end: feed synthetic trade history → run check → assert trip + close hook."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, List

import pytest

import importlib

cb_mod = importlib.import_module("core.risk.circuit_breaker")
CircuitBreaker = cb_mod.CircuitBreaker
register_close_positions_hook = cb_mod.register_close_positions_hook
run_circuit_breaker_checks = cb_mod.run_circuit_breaker_checks


@pytest.fixture
def isolated_breaker(tmp_path, monkeypatch):
    monkeypatch.setattr(cb_mod.settings, "CACHE_PATH", tmp_path, raising=False)
    breaker = CircuitBreaker()
    breaker._store_path = tmp_path / "runtime_state" / "circuit_breaker.json"
    monkeypatch.setattr(cb_mod, "circuit_breaker", breaker)
    # Default thresholds (5% / 10% strategy, 3% / 6% portfolio)
    monkeypatch.setattr(cb_mod.settings, "CIRCUIT_BREAKER_ENABLED", True, raising=False)
    monkeypatch.setattr(cb_mod.settings, "CB_STRATEGY_DAILY_DD_PCT", 0.05, raising=False)
    monkeypatch.setattr(cb_mod.settings, "CB_STRATEGY_WEEKLY_DD_PCT", 0.10, raising=False)
    monkeypatch.setattr(cb_mod.settings, "CB_PORTFOLIO_DAILY_DD_PCT", 0.03, raising=False)
    monkeypatch.setattr(cb_mod.settings, "CB_PORTFOLIO_WEEKLY_DD_PCT", 0.06, raising=False)
    return breaker


def _trade(strategy: str, pnl: float, *, hours_ago: float, capital: float = 10000.0) -> dict:
    return {
        "strategy": strategy,
        "pnl": pnl,
        "capital_after": capital,
        "timestamp": (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat(),
    }


def test_pnl_trigger_breaches_daily_threshold(isolated_breaker):
    """A losing streak that drains 6% in 24h on a $10k base should trip."""
    history = [
        _trade("StratA", -200.0, hours_ago=3.0),
        _trade("StratA", -300.0, hours_ago=2.0),
        _trade("StratA", -200.0, hours_ago=1.0),
    ]
    report = run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0},
    )
    trips = report["strategy_trips"]
    assert any(t["strategy"] == "StratA" for t in trips)
    assert isolated_breaker.check_strategy("StratA").is_close_only


def test_pnl_trigger_invokes_close_positions_hook(isolated_breaker):
    captured: List[tuple] = []

    def hook(name: str, reason: str) -> Any:
        captured.append((name, reason))
        return None

    register_close_positions_hook(hook)
    history = [
        _trade("StratClosable", -700.0, hours_ago=2.0, capital=10000.0),
    ]
    run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0},
    )
    assert captured, "close-positions hook should fire when a strategy is tripped"
    name, reason = captured[-1]
    assert name == "StratClosable"
    assert "circuit_breaker" in reason


def test_pnl_trigger_runs_async_close_positions_hook(isolated_breaker):
    captured: List[tuple] = []

    async def hook(name: str, reason: str) -> Any:
        await asyncio.sleep(0)
        captured.append((name, reason))
        return None

    register_close_positions_hook(hook)
    history = [
        _trade("StratAsyncClose", -700.0, hours_ago=2.0, capital=10000.0),
    ]

    run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0},
    )

    assert captured, "async close-positions hook should run even outside an active event loop"
    name, reason = captured[-1]
    assert name == "StratAsyncClose"
    assert "circuit_breaker" in reason


def test_pnl_trigger_only_fires_on_first_breach(isolated_breaker):
    """Second pass over the same history should NOT re-fire the hook."""
    fired: List[tuple] = []
    register_close_positions_hook(lambda n, r: fired.append((n, r)))
    history = [_trade("StratA", -700.0, hours_ago=1.0, capital=10000.0)]

    run_circuit_breaker_checks(trade_history=history,
                               portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0})
    run_circuit_breaker_checks(trade_history=history,
                               portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0})
    assert len(fired) == 1, "trip is idempotent — hook fires once per transition"


def test_portfolio_threshold_independent_of_strategy(isolated_breaker):
    """Portfolio trip can occur even without any single strategy breach."""
    history = [
        _trade("StratA", -50.0, hours_ago=1.0, capital=10000.0),
        _trade("StratB", -60.0, hours_ago=1.0, capital=10000.0),
    ]
    report = run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.045, "weekly_dd": 0.02},
    )
    assert report["portfolio_trip"] is not None
    assert isolated_breaker.check_portfolio().is_close_only
    # Individual strategies untouched
    assert isolated_breaker.check_strategy("StratA").is_allow
    assert isolated_breaker.check_strategy("StratB").is_allow


def test_weekly_threshold_independent_of_daily(isolated_breaker):
    """A slow 7-day bleed > weekly threshold should trip even if daily looks ok."""
    history = [
        _trade("StratSlowBleed", -200.0, hours_ago=24 * 6, capital=10000.0),
        _trade("StratSlowBleed", -300.0, hours_ago=24 * 4, capital=10000.0),
        _trade("StratSlowBleed", -300.0, hours_ago=24 * 2, capital=10000.0),
        _trade("StratSlowBleed", -300.0, hours_ago=24 * 1, capital=10000.0),
    ]
    report = run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0},
    )
    breached = [t for t in report["strategy_trips"] if t["strategy"] == "StratSlowBleed"]
    assert breached, "weekly drawdown should fire even if daily window is light"


def test_manual_reset_then_re_trip(isolated_breaker):
    """After a manual reset, a subsequent breach in fresh history re-trips."""
    history1 = [_trade("StratA", -700.0, hours_ago=1.0, capital=10000.0)]
    run_circuit_breaker_checks(trade_history=history1,
                               portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0})
    assert isolated_breaker.check_strategy("StratA").is_close_only

    isolated_breaker.reset_strategy("StratA", operator="test_ops")
    assert isolated_breaker.check_strategy("StratA").is_allow

    # New loss series re-trips
    history2 = history1 + [_trade("StratA", -100.0, hours_ago=0.1, capital=9300.0)]
    run_circuit_breaker_checks(trade_history=history2,
                               portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0})
    assert isolated_breaker.check_strategy("StratA").is_close_only
