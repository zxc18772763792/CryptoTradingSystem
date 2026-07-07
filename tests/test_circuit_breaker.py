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


def test_drawdown_fallback_does_not_dilute_loss_by_notional():
    ts = datetime.now(timezone.utc)
    history = [
        {
            "strategy": "Levered",
            "pnl": -500.0,
            "notional": 50000.0,
            "timestamp": ts.isoformat(),
        }
    ]

    grouped = evaluate_strategy_drawdowns(history)

    assert grouped["Levered"]["daily_dd"] == pytest.approx(1.0)


def test_drawdown_without_denominator_does_not_create_synthetic_full_loss():
    ts = datetime.now(timezone.utc)
    history = [
        {
            "strategy": "TinyCostOnly",
            "pnl": -0.2,
            "timestamp": ts.isoformat(),
        }
    ]

    grouped = evaluate_strategy_drawdowns(history)

    assert grouped["TinyCostOnly"]["daily_dd"] == 0.0


def test_live_drawdown_ignores_paper_order_ids():
    ts = datetime.now(timezone.utc)
    history = [
        {
            "strategy": "LiveStrat",
            "pnl": -0.38,
            "order_id": "paper_abc123",
            "mode": "live",
            "timestamp": ts.isoformat(),
        }
    ]

    grouped = evaluate_strategy_drawdowns(
        history,
        base_capital=1.1574,
        active_strategy_names={"LiveStrat"},
        runtime_mode="live",
    )

    assert grouped == {}


def test_evaluate_strategy_drawdowns_filters_to_active_names():
    history = [
        _trade("StoppedStrat", -700.0, 1.0, capital=10000.0),
        _trade("RunningStrat", -50.0, 1.0, capital=10000.0),
    ]

    grouped = evaluate_strategy_drawdowns(history, active_strategy_names={"RunningStrat"})

    assert set(grouped) == {"RunningStrat"}


def test_system_portfolio_drawdown_excludes_manual_rows_and_external_unrealized():
    history = [
        {
            "strategy": "ManualDesk",
            "action": "manual_order",
            "pnl": -500.0,
            "mode": "live",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        {
            "strategy": "manual_demo",
            "action": "close",
            "pnl": -250.0,
            "mode": "live",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    ]

    dds = cb_mod._system_portfolio_drawdown_from_trade_history(
        history,
        runtime_mode="live",
        account_equity=10000.0,
        current_system_unrealized_pnl=0.0,
    )

    assert dds["source"] == "system_owned_pnl"
    assert dds["daily_dd"] == 0.0
    assert dds["weekly_dd"] == 0.0


def test_system_portfolio_drawdown_counts_system_unrealized_loss():
    dds = cb_mod._system_portfolio_drawdown_from_trade_history(
        [],
        runtime_mode="live",
        account_equity=10000.0,
        current_system_unrealized_pnl=-400.0,
    )

    assert dds["daily_dd"] == pytest.approx(0.04)
    assert dds["weekly_dd"] == pytest.approx(0.04)


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


def test_run_checks_does_not_auto_clear_false_trip_by_default(cb, monkeypatch):
    monkeypatch.setattr(cb_mod, "circuit_breaker", cb)
    cb.trip_portfolio("24h_dd 0.9998 >= 0.0300", daily_dd=0.9998, weekly_dd=0.9998)
    cb.trip_strategy("LiveStrat", "24h_dd 0.3280 >= 0.0500", daily_dd=0.3280, weekly_dd=0.3280)
    history = [
        {
            "strategy": "LiveStrat",
            "pnl": -0.38,
            "order_id": "paper_abc123",
            "mode": "live",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    ]

    report = run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.0025, "weekly_dd": 0.0025},
        account_equity=5038.0,
        active_strategy_names={"LiveStrat"},
    )

    assert report["portfolio_auto_clear"] is None
    assert report["strategy_auto_clears"] == []
    assert cb.check_portfolio().is_close_only
    assert cb.check_strategy("LiveStrat").is_close_only


def test_run_checks_auto_clear_false_trip_requires_explicit_opt_in(cb, monkeypatch):
    monkeypatch.setattr(cb_mod, "circuit_breaker", cb)
    cb.trip_portfolio("24h_dd 0.9998 >= 0.0300", daily_dd=0.9998, weekly_dd=0.9998)
    cb.trip_strategy("LiveStrat", "24h_dd 0.3280 >= 0.0500", daily_dd=0.3280, weekly_dd=0.3280)
    history = [
        {
            "strategy": "LiveStrat",
            "pnl": -0.38,
            "order_id": "paper_abc123",
            "mode": "live",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    ]

    report = run_circuit_breaker_checks(
        trade_history=history,
        portfolio_drawdown={"daily_dd": 0.0025, "weekly_dd": 0.0025},
        account_equity=5038.0,
        active_strategy_names={"LiveStrat"},
        auto_clear_false_trips=True,
    )

    assert report["portfolio_auto_clear"] is not None
    assert report["strategy_auto_clears"][0]["strategy"] == "LiveStrat"
    assert cb.check_portfolio().is_allow
    assert cb.check_strategy("LiveStrat").is_allow


def test_manual_reset_suppresses_same_persisting_drawdown(cb, monkeypatch):
    """Regression (2026-07-07): the rolling 24h dd persists after a manual
    reset, so the next evaluation re-tripped within minutes and the reset
    button was effectively a lie. Same condition -> suppressed."""
    monkeypatch.setattr(cb_mod, "circuit_breaker", cb)
    dd = {"daily_dd": 0.05, "weekly_dd": 0.0}
    run_circuit_breaker_checks(trade_history=[], portfolio_drawdown=dd)
    assert cb.check_portfolio().is_close_only
    assert cb.reset_portfolio("test_operator") is True

    report = run_circuit_breaker_checks(trade_history=[], portfolio_drawdown=dd)
    assert report["portfolio_trip"]["suppressed_by_manual_reset"] is True
    assert report["portfolio_trip"]["new_trip"] is False
    assert not cb.check_portfolio().is_close_only


def test_manual_reset_latch_pierced_by_worsening_drawdown(cb, monkeypatch):
    monkeypatch.setattr(cb_mod, "circuit_breaker", cb)
    run_circuit_breaker_checks(trade_history=[], portfolio_drawdown={"daily_dd": 0.05, "weekly_dd": 0.0})
    cb.reset_portfolio("test_operator")

    # +0.6pp beyond the reset-time level (> 0.5pp margin) = new deterioration.
    report = run_circuit_breaker_checks(trade_history=[], portfolio_drawdown={"daily_dd": 0.056, "weekly_dd": 0.0})
    assert report["portfolio_trip"]["new_trip"] is True
    assert cb.check_portfolio().is_close_only


def test_manual_reset_latch_rearms_after_recovery(cb, monkeypatch):
    monkeypatch.setattr(cb_mod, "circuit_breaker", cb)
    run_circuit_breaker_checks(trade_history=[], portfolio_drawdown={"daily_dd": 0.05, "weekly_dd": 0.0})
    cb.reset_portfolio("test_operator")

    # Condition clears -> latch removed, breaker back to full strictness.
    run_circuit_breaker_checks(trade_history=[], portfolio_drawdown={"daily_dd": 0.01, "weekly_dd": 0.0})
    assert cb.portfolio_state()["manual_override_active"] is False

    # The SAME 5% dd later is now a genuinely new event -> trips again.
    report = run_circuit_breaker_checks(trade_history=[], portfolio_drawdown={"daily_dd": 0.05, "weekly_dd": 0.0})
    assert report["portfolio_trip"]["new_trip"] is True
    assert cb.check_portfolio().is_close_only


def test_drawdown_snapshot_reports_binding_peak_and_trough_timestamps():
    from core.risk.risk_manager import RiskManager

    rm = RiskManager.__new__(RiskManager)  # snapshot helper is self-contained
    points = [
        {"timestamp": "2026-07-07T02:50:00+00:00", "equity": 10550.0},
        {"timestamp": "2026-07-07T02:56:00+00:00", "equity": 11126.0},  # phantom peak
        {"timestamp": "2026-07-07T03:10:00+00:00", "equity": 10540.0},  # binding trough
        {"timestamp": "2026-07-07T03:20:00+00:00", "equity": 10545.0},
    ]
    snap = rm._drawdown_snapshot_for_points(points, hours=24)
    assert snap["peak_equity"] == 11126.0
    assert snap["peak_ts"] == "2026-07-07T02:56:00+00:00"
    assert snap["trough_equity"] == 10540.0
    assert snap["trough_ts"] == "2026-07-07T03:10:00+00:00"
    assert round(snap["drawdown"], 4) == round((11126.0 - 10540.0) / 11126.0, 4)
