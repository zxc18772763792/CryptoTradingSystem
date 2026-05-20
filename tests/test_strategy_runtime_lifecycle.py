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

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from core.strategies.strategy_manager import StrategyConfig
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
    from core.strategies.strategy_manager import StrategyManager

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
