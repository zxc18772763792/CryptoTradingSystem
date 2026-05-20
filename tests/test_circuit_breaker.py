"""Unit tests for the Phase 4.2 portfolio/per-strategy circuit breaker."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import importlib

cb_mod = importlib.import_module("core.risk.circuit_breaker")
CircuitBreaker = cb_mod.CircuitBreaker
DECISION_ALLOW = cb_mod.DECISION_ALLOW
DECISION_CLOSE_ONLY = cb_mod.DECISION_CLOSE_ONLY
Decision = cb_mod.Decision
evaluate_strategy_drawdowns = cb_mod.evaluate_strategy_drawdowns
run_circuit_breaker_checks = cb_mod.run_circuit_breaker_checks


@pytest.fixture
def cb(tmp_path, monkeypatch):
    """Fresh CircuitBreaker with persistence redirected to tmp_path."""
    monkeypatch.setattr(
        cb_mod.settings, "CACHE_PATH", tmp_path, raising=False
    )
    fresh = CircuitBreaker()
    fresh._store_path = tmp_path / "runtime_state" / "circuit_breaker.json"
    return fresh


def test_initial_state_allows(cb):
    assert cb.check_portfolio().is_allow
    assert cb.check_strategy("StratA").is_allow
    snap = cb.snapshot()
    assert snap["portfolio"]["tripped"] is False
    assert snap["strategies"] == {}


def test_trip_strategy_returns_close_only(cb):
    transitioned = cb.trip_strategy("StratA", "daily_dd 0.06 >= 0.05", daily_dd=0.06)
    assert transitioned is True
    decision = cb.check_strategy("StratA")
    assert decision.is_close_only
    assert decision.scope == "strategy"
    assert "daily_dd" in decision.reason
    # Other strategies still allowed
    assert cb.check_strategy("StratB").is_allow


def test_trip_strategy_idempotent(cb):
    cb.trip_strategy("StratA", "first", daily_dd=0.06)
    second = cb.trip_strategy("StratA", "second", daily_dd=0.07)
    assert second is False  # no new transition
    decision = cb.check_strategy("StratA")
    assert decision.reason == "second"  # reason updated, single trip event


def test_trip_portfolio_overrides_per_strategy(cb):
    cb.trip_portfolio("portfolio breach", daily_dd=0.04)
    # Even an un-tripped strategy is held in close-only via evaluate()
    decision = cb.evaluate(strategy_name="StratX", is_reduce_only=False)
    assert decision.is_close_only
    assert decision.scope == "portfolio"


def test_evaluate_close_orders_always_allowed(cb):
    cb.trip_portfolio("portfolio breach", daily_dd=0.04)
    cb.trip_strategy("StratA", "strategy breach", daily_dd=0.06)
    decision = cb.evaluate(strategy_name="StratA", is_reduce_only=True)
    assert decision.is_allow


def test_reset_strategy_clears_trip(cb):
    cb.trip_strategy("StratA", "breach", daily_dd=0.06)
    assert cb.check_strategy("StratA").is_close_only
    changed = cb.reset_strategy("StratA", operator="ops_user")
    assert changed is True
    assert cb.check_strategy("StratA").is_allow
    # Reset again is no-op
    assert cb.reset_strategy("StratA", operator="ops_user") is False


def test_reset_portfolio_clears_trip(cb):
    cb.trip_portfolio("breach", daily_dd=0.04)
    assert cb.check_portfolio().is_close_only
    assert cb.reset_portfolio("ops_user") is True
    assert cb.check_portfolio().is_allow


def test_listener_fires_on_trip(cb):
    events = []
    cb.add_listener(lambda event, payload: events.append((event, payload)))
    cb.trip_strategy("StratA", "breach", daily_dd=0.06)
    cb.trip_portfolio("portfolio breach", daily_dd=0.04)
    cb.reset_strategy("StratA", operator="ops_user")
    kinds = [e[0] for e in events]
    assert "strategy_tripped" in kinds
    assert "portfolio_tripped" in kinds
    assert "strategy_reset" in kinds


def test_persistence_survives_reload(cb, tmp_path):
    cb.trip_strategy("StratA", "breach", daily_dd=0.06)
    cb.trip_portfolio("portfolio breach", daily_dd=0.04)
    # New instance reads from disk
    from core.risk.circuit_breaker import CircuitBreaker
    fresh = CircuitBreaker()
    fresh._store_path = cb._store_path
    fresh._strategies.clear()
    fresh._portfolio.tripped = False
    fresh._load_from_disk()
    assert fresh.check_strategy("StratA").is_close_only
    assert fresh.check_portfolio().is_close_only


def test_disabled_circuit_breaker_short_circuits(cb, monkeypatch):
    monkeypatch.setattr(cb_mod.settings, "CIRCUIT_BREAKER_ENABLED", False, raising=False)
    cb.trip_strategy("StratA", "breach", daily_dd=0.06)
    # While disabled, decisions always allow
    assert cb.check_strategy("StratA").is_allow
    assert cb.evaluate(strategy_name="StratA", is_reduce_only=False).is_allow


def _trade(strategy: str, pnl: float, hours_ago: float, capital: float = 10000.0) -> dict:
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return {
        "strategy": strategy,
        "pnl": pnl,
        "capital_after": capital,
        "timestamp": ts.isoformat(),
    }


def test_evaluate_strategy_drawdowns_grouping():
    history = [
        _trade("StratA", -100.0, 1.0),
        _trade("StratA", -50.0, 0.5),
        _trade("StratB", 50.0, 2.0),
    ]
    grouped = evaluate_strategy_drawdowns(history)
    assert "StratA" in grouped and "StratB" in grouped
    assert grouped["StratA"]["daily_dd"] > 0
    # StratB was profitable — drawdown should be 0
    assert grouped["StratB"]["daily_dd"] == 0.0


def test_run_checks_trips_breaching_strategy(cb, monkeypatch):
    monkeypatch.setattr(cb_mod, "circuit_breaker", cb)
    history = [
        _trade("StratA", -700.0, 1.0, capital=10000.0),
    ]
    report = run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.0, "weekly_dd": 0.0},
    )
    assert report["enabled"] is True
    assert any(t["strategy"] == "StratA" for t in report["strategy_trips"])
    assert cb.check_strategy("StratA").is_close_only


def test_run_checks_trips_portfolio(cb, monkeypatch):
    monkeypatch.setattr(cb_mod, "circuit_breaker", cb)
    report = run_circuit_breaker_checks(
        trade_history=[],
        portfolio_drawdown={"daily_dd": 0.05, "weekly_dd": 0.0},
    )
    assert report["portfolio_trip"] is not None
    assert cb.check_portfolio().is_close_only


def test_run_checks_does_not_trip_under_threshold(cb, monkeypatch):
    monkeypatch.setattr(cb_mod, "circuit_breaker", cb)
    history = [_trade("StratA", -50.0, 1.0, capital=10000.0)]
    report = run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.005, "weekly_dd": 0.01},
    )
    assert report["strategy_trips"] == []
    assert report["portfolio_trip"] is None
    assert cb.check_portfolio().is_allow
    assert cb.check_strategy("StratA").is_allow
