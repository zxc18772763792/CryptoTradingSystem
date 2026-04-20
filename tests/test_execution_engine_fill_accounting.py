from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.exchanges.base_exchange import OrderStatus
from core.strategies import Signal, SignalType
from core.trading.execution_engine import ExecutionEngine
from core.trading.position_manager import PositionSide, position_manager

execution_engine_module = importlib.import_module("core.trading.execution_engine")


@pytest.fixture(autouse=True)
def _clear_positions():
    position_manager.clear_all()
    yield
    position_manager.clear_all()


def test_resolved_order_fill_qty_only_falls_back_for_closed_orders():
    engine = ExecutionEngine()

    assert engine._resolved_order_fill_qty(
        SimpleNamespace(filled=0.0, amount=1.0, status=OrderStatus.OPEN),
        1.0,
    ) == pytest.approx(0.0)
    assert engine._resolved_order_fill_qty(
        SimpleNamespace(filled=0.25, amount=1.0, status=OrderStatus.OPEN),
        1.0,
    ) == pytest.approx(0.25)
    assert engine._resolved_order_fill_qty(
        SimpleNamespace(filled=0.0, amount=1.0, status=OrderStatus.CLOSED),
        1.0,
    ) == pytest.approx(1.0)


def test_manual_order_does_not_open_local_position_when_live_order_is_unfilled(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    record_trade_calls: list[dict] = []
    notify_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(execution_engine_module.account_manager, "is_enabled", lambda account_id: True)
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, trace_id="trace-manual-open")),
    )
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(
        execution_engine_module.risk_manager,
        "record_trade",
        lambda payload: record_trade_calls.append(dict(payload)),
    )
    monkeypatch.setattr(
        execution_engine_module.order_manager,
        "create_order",
        AsyncMock(
            return_value=SimpleNamespace(
                id="manual-open-1",
                status=OrderStatus.OPEN,
                price=100.0,
                amount=1.0,
                filled=0.0,
                fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 100.0)))
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(engine, "_notify_callbacks", notify_mock)

    result = asyncio.run(
        engine._execute_manual_order_single_in_active_mode(
            exchange="binance",
            symbol="BTC/USDT",
            side="buy",
            order_type="limit",
            amount=1.0,
            price=100.0,
            leverage=2.0,
            stop_loss=None,
            take_profit=None,
            trailing_stop_pct=None,
            trailing_stop_distance=None,
            trigger_price=None,
            order_mode="normal",
            iceberg_parts=1,
            algo_slices=1,
            algo_interval_sec=0,
            account_id="main",
            reduce_only=False,
            strategy="manual_demo",
            params={},
        )
    )

    assert result is not None
    assert result["status"] == "open"
    assert result["filled"] == pytest.approx(0.0)
    assert result["executed_quantity"] == pytest.approx(0.0)
    assert position_manager.get_position("binance", "BTC/USDT", account_id="main", strategy="manual_demo") is None
    assert record_trade_calls == []
    assert notify_mock.await_args.args[0] == "manual_order_submitted"


def test_close_position_uses_actual_filled_quantity_for_partial_live_close(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    record_trade_calls: list[dict] = []
    notify_mock = AsyncMock(return_value=None)
    live_trade_mock = AsyncMock(return_value=None)
    audit_mock = AsyncMock(return_value=None)

    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.0,
        leverage=2.0,
        strategy="demo_close",
        account_id="main",
        metadata={"source": "strategy"},
    )
    position = position_manager.get_position("binance", "BTC/USDT", account_id="main", strategy="demo_close")
    assert position is not None

    signal = Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.CLOSE_LONG,
        price=105.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name="demo_close",
        strength=0.6,
        metadata={"account_id": "main", "exchange": "binance"},
    )

    monkeypatch.setattr(engine, "_resolve_existing_position", AsyncMock(return_value=position))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(105.0, 105.0)))
    monkeypatch.setattr(engine, "_resolve_strategy_trade_policy", lambda *args, **kwargs: {})
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(
        execution_engine_module.risk_manager,
        "record_trade",
        lambda payload: record_trade_calls.append(dict(payload)),
    )
    monkeypatch.setattr(
        execution_engine_module.order_manager,
        "create_order",
        AsyncMock(
            return_value=SimpleNamespace(
                id="close-1",
                status=OrderStatus.OPEN,
                price=105.0,
                amount=1.0,
                filled=0.25,
                fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(engine, "_record_live_strategy_trade", live_trade_mock)
    monkeypatch.setattr(engine, "_notify_callbacks", notify_mock)
    monkeypatch.setattr(execution_engine_module.audit_logger, "log", audit_mock)

    result = asyncio.run(engine._close_position_in_active_mode(signal, PositionSide.LONG))

    assert result is not None
    assert result["quantity"] == pytest.approx(0.25)
    assert record_trade_calls[-1]["quantity"] == pytest.approx(0.25)
    remaining = position_manager.get_position("binance", "BTC/USDT", account_id="main", strategy="demo_close")
    assert remaining is not None
    assert remaining.quantity == pytest.approx(0.75)
    assert live_trade_mock.await_args.kwargs["quantity"] == pytest.approx(0.25)
    assert notify_mock.await_args.args[0] == "order_executed"
