"""Isolated money-path regressions from the 2026-10-23 team audit."""
from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.exchanges.base_exchange import Order, OrderSide, OrderStatus, OrderType
from core.risk.risk_manager import RiskManager
from core.trading.order_manager import OrderManager

risk_module = importlib.import_module("core.risk.risk_manager")


def _cached_order(manager, status, mode):
    order = Order(
        id="synthetic-order", symbol="BTC/USDT", exchange="binance",
        side=OrderSide.BUY, type=OrderType.LIMIT, price=100.0, amount=1.0,
        filled=1.0 if status == OrderStatus.CLOSED else 0.0,
        remaining=0.0 if status == OrderStatus.CLOSED else 1.0,
        cost=100.0 if status == OrderStatus.CLOSED else 0.0,
        status=status, timestamp=datetime.now(timezone.utc),
    )
    manager._orders[order.id] = order
    manager._order_meta[order.id] = {"mode": mode, "account_id": "main"}
    return order


@pytest.mark.parametrize("mode", ["paper", "live"])
@pytest.mark.parametrize("status", [
    OrderStatus.CLOSED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED,
])
def test_terminal_order_cannot_be_canceled_again(monkeypatch, mode, status):
    manager = OrderManager()
    order = _cached_order(manager, status, mode)
    connector = SimpleNamespace(cancel_order=AsyncMock(return_value=True))
    monkeypatch.setattr(manager, "_resolve_cached_exchange", lambda *a, **kw: connector)
    callback = AsyncMock()
    manager.register_callback(callback)

    assert asyncio.run(manager.cancel_order(order.id, order.symbol, trading_mode=mode)) is False
    assert order.status == status
    assert order.filled == (1.0 if status == OrderStatus.CLOSED else 0.0)
    assert order.cost == (100.0 if status == OrderStatus.CLOSED else 0.0)
    connector.cancel_order.assert_not_awaited()
    callback.assert_not_awaited()


def test_open_paper_cancel_transitions_once_and_preserves_partial_fill():
    manager = OrderManager()
    order = _cached_order(manager, OrderStatus.OPEN, "paper")
    order.filled, order.remaining, order.cost = 0.4, 0.6, 40.0
    callback = AsyncMock()
    manager.register_callback(callback)

    assert asyncio.run(manager.cancel_order(order.id, order.symbol)) is True
    assert asyncio.run(manager.cancel_order(order.id, order.symbol)) is False
    assert order.status == OrderStatus.CANCELED
    assert (order.filled, order.remaining, order.cost) == (0.4, 0.6, 40.0)
    callback.assert_awaited_once_with(order, "canceled")


@pytest.fixture
def risk(monkeypatch, tmp_path):
    monkeypatch.setattr(risk_module, "_position_manager", lambda: SimpleNamespace(
        get_position_count=lambda: 0, get_all_positions=lambda: [],
        get_positions_by_strategy=lambda strategy: [],
    ))
    manager = RiskManager(use_persisted_overlay=False)
    manager.configure_storage(tmp_path / "risk")
    manager.max_position_size = 0.1
    manager.max_gross_exposure_ratio = 0.8
    return manager


@pytest.mark.parametrize("allocation", [0.0, "0"])
@pytest.mark.parametrize("order_value", [10.0, 0.01])
def test_zero_strategy_allocation_blocks_every_new_entry(risk, allocation, order_value):
    assert risk.pre_trade_check(
        symbol="BTC/USDT", side="buy", strategy_name="disabled-strategy",
        account_equity=1000.0, order_value=order_value,
        strategy_allocation=allocation,
    ) is False


def test_zero_allocation_still_permits_exit_and_positive_allocation_entry(risk):
    args = dict(symbol="BTC/USDT", side="sell", strategy_name="disabled-strategy",
                account_equity=1000.0, order_value=10.0)
    assert risk.pre_trade_check(**args, strategy_allocation=0, allow_close=True) is True
    assert risk.pre_trade_check(**args, strategy_allocation=0.05) is True


@pytest.mark.parametrize("key", [
    "max_position_size", "max_daily_loss_ratio", "max_daily_loss_usd",
    "max_daily_trades", "max_open_positions", "max_leverage",
    "balance_volatility_alert_pct", "autonomy_daily_stop_buffer_ratio",
    "autonomy_max_drawdown_reduce_only", "autonomy_rolling_3d_drawdown_reduce_only",
    "autonomy_rolling_7d_drawdown_reduce_only", "daily_stop_buffer_ratio",
    "max_drawdown_reduce_only", "rolling_3d_drawdown_reduce_only",
    "rolling_7d_drawdown_reduce_only", "max_daily_loss",
])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), "invalid"])
def test_invalid_risk_update_is_rejected_atomically(risk, key, value):
    before = {name: getattr(risk, name) for name in (
        "max_position_size", "max_daily_loss_ratio", "max_daily_loss_usd", "max_daily_trades",
        "max_open_positions", "max_leverage", "balance_volatility_alert_pct",
        "autonomy_daily_stop_buffer_ratio", "autonomy_max_drawdown_reduce_only",
        "autonomy_rolling_3d_drawdown_reduce_only", "autonomy_rolling_7d_drawdown_reduce_only",
    )}
    payload = {"max_position_size": 0.5, "max_daily_loss_ratio": 0.1, key: value}

    with pytest.raises(ValueError):
        risk.update_parameters(payload)

    assert {name: getattr(risk, name) for name in before} == before
    assert risk.pre_trade_check(
        symbol="BTC/USDT", side="buy", strategy_name=None,
        account_equity=1000.0, order_value=400.0,
    ) is False


def test_valid_risk_update_retains_alias_and_clamp_behavior(risk):
    risk.update_parameters({"max_position_size": 2.0, "max_daily_trades": "12.9",
                            "max_daily_loss": 0.1, "daily_stop_buffer_ratio": 0.5})
    assert risk.max_position_size == 1.0
    assert risk.max_daily_trades == 12
    assert risk.max_daily_loss_ratio == 0.1
    assert risk.autonomy_daily_stop_buffer_ratio == 0.1


@pytest.mark.parametrize("key,attribute,expected", [
    ("max_position_size", "max_position_size", 0.001),
    ("max_daily_loss_ratio", "max_daily_loss_ratio", 0.001),
    ("max_daily_loss_usd", "max_daily_loss_usd", 0.0),
    ("max_daily_trades", "max_daily_trades", 1),
    ("max_open_positions", "max_open_positions", 1),
    ("max_leverage", "max_leverage", 1.0),
    ("balance_volatility_alert_pct", "balance_volatility_alert_pct", 0.01),
    ("autonomy_daily_stop_buffer_ratio", "autonomy_daily_stop_buffer_ratio", 0.0),
    ("autonomy_max_drawdown_reduce_only", "autonomy_max_drawdown_reduce_only", 0.0),
    ("autonomy_rolling_3d_drawdown_reduce_only", "autonomy_rolling_3d_drawdown_reduce_only", 0.0),
    ("autonomy_rolling_7d_drawdown_reduce_only", "autonomy_rolling_7d_drawdown_reduce_only", 0.0),
    ("daily_stop_buffer_ratio", "autonomy_daily_stop_buffer_ratio", 0.0),
    ("max_drawdown_reduce_only", "autonomy_max_drawdown_reduce_only", 0.0),
    ("rolling_3d_drawdown_reduce_only", "autonomy_rolling_3d_drawdown_reduce_only", 0.0),
    ("rolling_7d_drawdown_reduce_only", "autonomy_rolling_7d_drawdown_reduce_only", 0.0),
    ("max_daily_loss", "max_daily_loss_ratio", 0.001),
])
def test_zero_risk_parameter_preserves_legacy_clamp_or_disable_semantics(risk, key, attribute, expected):
    risk.update_parameters({key: 0.0})
    assert getattr(risk, attribute) == expected
