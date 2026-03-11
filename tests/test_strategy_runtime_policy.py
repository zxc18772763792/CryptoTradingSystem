from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from core.strategies.runtime_policy import (
    build_runtime_limit_policy,
    infer_effective_interval_minutes,
    parse_timeframe_minutes,
)


def test_parse_timeframe_minutes_basic():
    assert parse_timeframe_minutes("1m") == 1
    assert parse_timeframe_minutes("15m") == 15
    assert parse_timeframe_minutes("2h") == 120
    assert parse_timeframe_minutes("1d") == 1440
    assert parse_timeframe_minutes("bad-value") == 60


def test_infer_effective_interval_respects_cooldown():
    interval = infer_effective_interval_minutes(
        "5m",
        {"cooldown_min": 45, "rebalance_interval_minutes": 30},
    )
    assert interval == 45


def test_runtime_policy_observed_frequency():
    policy = build_runtime_limit_policy(
        timeframe="1h",
        params={"cooldown_min": 60},
        observed_trades_per_day=24.0,
        target_trade_samples=96,
    )
    assert policy["source"] == "observed"
    assert policy["runtime_limit_minutes"] >= 720
    assert policy["runtime_limit_minutes"] <= 10080
    assert policy["estimated_trades_per_day"] == 24.0


def test_runtime_policy_caps_for_very_low_frequency():
    policy = build_runtime_limit_policy(
        timeframe="4h",
        params={},
        observed_trades_per_day=0.3,
        target_trade_samples=120,
    )
    assert policy["runtime_limit_minutes"] == 10080
    assert policy["source"] == "observed"


def test_strategy_register_endpoint_applies_runtime_policy(monkeypatch):
    from web.api import strategies as strategies_api

    register_mock = MagicMock(return_value=True)
    monkeypatch.setattr(strategies_api, "_get_strategy_classes", lambda: {"MAStrategy": object})
    monkeypatch.setattr(strategies_api.strategy_manager, "register_strategy", register_mock)
    monkeypatch.setattr(strategies_api, "_persist_if_exists", AsyncMock(return_value=None))
    monkeypatch.setattr(strategies_api.asyncio, "create_task", lambda coro: coro.close())
    monkeypatch.setattr(strategies_api.audit_logger, "log", AsyncMock(return_value=None))

    request = strategies_api.StrategyRegisterRequest(
        name="runtime_policy_test",
        strategy_type="MAStrategy",
        params={"cooldown_min": 30},
        symbols=["BTC/USDT"],
        timeframe="15m",
        exchange="binance",
        allocation=0.1,
        runtime_limit_minutes=None,
    )

    result = asyncio.run(strategies_api.register_strategy(request))
    assert result["success"] is True
    assert isinstance(result["runtime_limit_minutes"], int)
    assert result["runtime_limit_minutes"] > 0
    assert isinstance(result["runtime_policy"], dict)
    assert register_mock.call_args.kwargs["runtime_limit_minutes"] == result["runtime_limit_minutes"]
