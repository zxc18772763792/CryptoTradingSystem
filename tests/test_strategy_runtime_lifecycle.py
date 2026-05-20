"""runtime_limit lifecycle supervision.

The legacy behaviour silently logged at INFO when a strategy auto-stopped
on runtime_limit expiry, and never restarted it — strategies could
"disappear" mid-trading with no obvious signal. The new behaviour:

  * default: still stop, but log at WARNING with a hint about how to opt
    into renewal (so an operator scanning the log sees what happened).
  * auto_renew_on_runtime_limit=True: extend the deadline by the
    original interval and keep the strategy (and its open positions)
    alive.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pandas as pd

from core.strategies.strategy_manager import StrategyConfig, StrategyManager
from strategies.technical.rsi_strategy import RSIStrategy


def _config(*, runtime_min: int, auto_renew: bool) -> StrategyConfig:
    return StrategyConfig(
        name="lc_probe",
        strategy_class=RSIStrategy,
        params={},
        symbols=["BTC/USDT"],
        timeframe="1m",
        runtime_limit_minutes=runtime_min,
        auto_renew_on_runtime_limit=auto_renew,
    )


def test_strategy_config_defaults_to_no_auto_renew():
    cfg = _config(runtime_min=60, auto_renew=False)
    assert cfg.auto_renew_on_runtime_limit is False


def test_strategy_config_carries_auto_renew_flag():
    cfg = _config(runtime_min=60, auto_renew=True)
    assert cfg.auto_renew_on_runtime_limit is True


def test_auto_renew_extends_deadline_by_runtime_limit_minutes():
    """The renewal arithmetic the runner relies on: a deadline already in
    the past gets pushed forward by the configured interval."""
    cfg = _config(runtime_min=30, auto_renew=True)
    expired = datetime.now(timezone.utc) - timedelta(minutes=1)
    new_deadline = expired + pd.Timedelta(minutes=cfg.runtime_limit_minutes)
    assert new_deadline > datetime.now(timezone.utc)
    # Renewing twice keeps moving forward — i.e., always strictly later.
    again = new_deadline + pd.Timedelta(minutes=cfg.runtime_limit_minutes)
    assert again > new_deadline


def test_register_strategy_accepts_auto_renew_kwarg():
    """register_strategy must surface the flag so callers can opt in
    without poking at StrategyConfig directly."""
    mgr = StrategyManager()
    ok = mgr.register_strategy(
        name="lc_register_probe",
        strategy_class=RSIStrategy,
        params={"period": 14},
        symbols=["BTC/USDT"],
        timeframe="1h",
        runtime_limit_minutes=60,
        auto_renew_on_runtime_limit=True,
    )
    assert ok
    cfg = mgr._configs["lc_register_probe"]
    assert cfg.auto_renew_on_runtime_limit is True


def test_stop_strategy_can_skip_position_close(monkeypatch):
    mgr = StrategyManager()
    ok = mgr.register_strategy(
        name="lc_shutdown_probe",
        strategy_class=RSIStrategy,
        params={"period": 14},
        symbols=["BTC/USDT"],
        timeframe="1h",
    )
    assert ok
    strategy = mgr._strategies["lc_shutdown_probe"]
    strategy.start()

    called = []

    async def fail_if_called(*args, **kwargs):
        called.append((args, kwargs))
        raise AssertionError("position close should be skipped")

    async def noop_stop_task(name):
        return None

    monkeypatch.setattr(mgr, "_close_positions_for_strategy_stop", fail_if_called)
    monkeypatch.setattr(mgr, "_stop_task_for_strategy", noop_stop_task)

    assert asyncio.run(
        mgr.stop_strategy(
            "lc_shutdown_probe",
            close_positions=False,
            reason="service_shutdown",
        )
    )
    assert called == []
    assert not strategy.is_running
    assert mgr.pop_last_stop_close_summary("lc_shutdown_probe") == {
        "requested": 0,
        "closed": 0,
        "failed": 0,
        "results": [],
        "skipped": True,
        "reason": "service_shutdown",
    }


def test_stop_all_passes_close_positions_policy(monkeypatch):
    mgr = StrategyManager()
    for name in ("lc_all_a", "lc_all_b"):
        ok = mgr.register_strategy(
            name=name,
            strategy_class=RSIStrategy,
            params={"period": 14},
            symbols=["BTC/USDT"],
            timeframe="1h",
        )
        assert ok

    calls = []

    async def fake_stop_strategy(name, *, close_positions=True, reason="strategy_stopped"):
        calls.append((name, close_positions, reason))
        return True

    monkeypatch.setattr(mgr, "stop_strategy", fake_stop_strategy)

    asyncio.run(mgr.stop_all(close_positions=False, reason="service_shutdown"))

    assert calls == [
        ("lc_all_a", False, "service_shutdown"),
        ("lc_all_b", False, "service_shutdown"),
    ]
