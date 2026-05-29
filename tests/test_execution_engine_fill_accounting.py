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
from core.trading.order_manager import OrderType
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


def test_live_reconcile_syncs_local_position_size_from_exchange(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=0.10,
        leverage=2.0,
        account_id="main",
        strategy="sync_demo",
    )
    connector = SimpleNamespace(
        config=SimpleNamespace(default_type="future"),
        get_positions=AsyncMock(
            return_value=[
                {
                    "symbol": "BTCUSDT",
                    "side": "long",
                    "amount": 0.25,
                    "entry_price": 105.0,
                    "current_price": 110.0,
                    "unrealizedPnl": 1.23,
                    "leverage": 3.0,
                }
            ]
        ),
    )
    notify_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(engine, "_ensure_exchange_connector", AsyncMock(return_value=connector))
    monkeypatch.setattr(engine, "_notify_callbacks", notify_mock)

    asyncio.run(engine._reconcile_local_positions_with_exchange())

    updated = position_manager.get_position(
        "binance",
        "BTC/USDT",
        account_id="main",
        strategy="sync_demo",
    )
    assert updated is not None
    assert updated.quantity == pytest.approx(0.25)
    assert updated.entry_price == pytest.approx(105.0)
    assert updated.current_price == pytest.approx(110.0)
    assert updated.unrealized_pnl == pytest.approx(1.23)
    assert updated.leverage == pytest.approx(3.0)
    assert updated.metadata["last_exchange_sync_source"] == "live_position_reconcile"
    assert notify_mock.await_count == 1


def test_manual_live_order_records_backfilled_fee_and_slippage(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    record_trade_calls: list[dict] = []
    notify_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(execution_engine_module.account_manager, "is_enabled", lambda account_id: True)
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, trace_id="trace-manual-cost")),
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
                id="manual-cost-1",
                status=OrderStatus.CLOSED,
                price=101.0,
                amount=0.5,
                filled=0.5,
                fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 50.0)))
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(execution_engine_module, "binance_has_credentials", lambda account_id=None: True)
    monkeypatch.setattr(
        execution_engine_module,
        "binance_signed_request",
        AsyncMock(
            return_value=[
                {
                    "orderId": "manual-cost-1",
                    "price": "101.0",
                    "qty": "0.5",
                    "quoteQty": "50.5",
                    "commission": "0.0202",
                    "commissionAsset": "USDT",
                }
            ]
        ),
    )
    monkeypatch.setattr(engine, "_notify_callbacks", notify_mock)

    result = asyncio.run(
        engine._execute_manual_order_single_in_active_mode(
            exchange="binance",
            symbol="BTC/USDT",
            side="buy",
            order_type="market",
            amount=0.5,
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
            strategy="manual_cost_demo",
            params={},
        )
    )

    assert result is not None
    assert result["fee_usd"] == pytest.approx(0.0202)
    assert result["slippage_cost_usd"] == pytest.approx(0.5)
    assert result["slippage_bps"] == pytest.approx(100.0)
    assert record_trade_calls[-1]["fee_source"] == "exchange_trades"
    assert record_trade_calls[-1]["slippage_source"] == "fill_vs_reference"
    assert record_trade_calls[-1]["pnl"] == pytest.approx(-0.5202)
    assert notify_mock.await_args.args[0] == "manual_order_executed"


def test_manual_open_revalidates_take_profit_against_actual_fill(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    record_trade_calls: list[dict] = []

    monkeypatch.setattr(execution_engine_module.account_manager, "is_enabled", lambda account_id: True)
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, trace_id="trace-manual-fill-protection")),
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
                id="manual-fill-protection-1",
                status=OrderStatus.CLOSED,
                price=105.0,
                amount=0.5,
                filled=0.5,
                fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 50.0)))
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))

    result = asyncio.run(
        engine._execute_manual_order_single_in_active_mode(
            exchange="binance",
            symbol="BTC/USDT",
            side="buy",
            order_type="market",
            amount=0.5,
            price=100.0,
            leverage=2.0,
            stop_loss=96.0,
            take_profit=101.0,
            trailing_stop_pct=None,
            trailing_stop_distance=None,
            trigger_price=None,
            order_mode="normal",
            iceberg_parts=1,
            algo_slices=1,
            algo_interval_sec=0,
            account_id="main",
            reduce_only=False,
            strategy="manual_fill_protection",
            params={},
        )
    )

    assert result is not None
    assert result["price"] == pytest.approx(105.0)
    assert result["stop_loss"] == pytest.approx(96.0)
    assert result["take_profit"] is None
    assert record_trade_calls[-1]["take_profit"] is None
    position = position_manager.get_position("binance", "BTC/USDT", account_id="main", strategy="manual_fill_protection")
    assert position is not None
    assert position.entry_price == pytest.approx(105.0)
    assert position.take_profit is None


def test_manual_reduce_order_records_close_reason(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False
    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.0,
        leverage=2.0,
        strategy="manual_close_demo",
        account_id="main",
    )

    record_trade_calls: list[dict] = []
    notify_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(execution_engine_module.account_manager, "is_enabled", lambda account_id: True)
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, trace_id="trace-manual-close")),
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
                id="manual-close-1",
                status=OrderStatus.CLOSED,
                price=99.0,
                amount=1.0,
                filled=1.0,
                fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(99.0, 99.0)))
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(
        engine,
        "_resolve_execution_costs",
        AsyncMock(return_value={"fee_usd": 0.0, "slippage_cost_usd": 0.0, "cost_usd": 0.0}),
    )
    monkeypatch.setattr(engine, "_notify_callbacks", notify_mock)

    result = asyncio.run(
        engine._execute_manual_order_single_in_active_mode(
            exchange="binance",
            symbol="BTC/USDT",
            side="sell",
            order_type="market",
            amount=1.0,
            price=99.0,
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
            reduce_only=True,
            strategy="manual_close_demo",
            params={"close_reason": "stop_loss"},
        )
    )

    assert result is not None
    assert result["close_reason"] == "stop_loss"
    assert record_trade_calls[-1]["close_reason"] == "stop_loss"
    assert notify_mock.await_args.args[1]["close_reason"] == "stop_loss"


def test_live_execution_costs_fall_back_to_live_defaults(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    monkeypatch.setattr(execution_engine_module.settings, "LIVE_FEE_RATE", 0.0004, raising=False)
    monkeypatch.setattr(execution_engine_module.settings, "LIVE_SLIPPAGE_BPS", 2.0, raising=False)
    monkeypatch.setattr(execution_engine_module, "binance_has_credentials", lambda account_id=None: False)

    result = asyncio.run(
        engine._resolve_execution_costs(
            order=SimpleNamespace(id="default-cost-1", fee=0.0, fee_currency="", status=OrderStatus.CLOSED),
            exchange="binance",
            symbol="BTC/USDT",
            account_id="main",
            fill_price=100.0,
            quantity=2.0,
            reference_price=100.0,
            paper_cost={"fee_usd": 0.0, "slippage_cost_usd": 0.0},
        )
    )

    assert result["fee_usd"] == pytest.approx(0.08)
    assert result["slippage_cost_usd"] == pytest.approx(0.04)
    assert result["cost_usd"] == pytest.approx(0.12)
    assert result["fee_source"] == "live_default_fee_rate"
    assert result["slippage_source"] == "live_default_slippage_bps"
    assert result["slippage_bps"] == pytest.approx(2.0)


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
            side_effect=[
                SimpleNamespace(
                    id="close-1",
                    status=OrderStatus.OPEN,
                    price=105.0,
                    amount=1.0,
                    filled=0.25,
                    fee=0.0,
                ),
                SimpleNamespace(
                    id="close-2",
                    status=OrderStatus.CLOSED,
                    price=105.0,
                    amount=0.75,
                    filled=0.75,
                    fee=0.0,
                ),
            ]
        ),
    )
    cancel_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(execution_engine_module.order_manager, "cancel_order", cancel_mock)
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(engine, "_record_live_strategy_trade", live_trade_mock)
    monkeypatch.setattr(engine, "_notify_callbacks", notify_mock)
    monkeypatch.setattr(execution_engine_module.audit_logger, "log", audit_mock)

    result = asyncio.run(engine._close_position_in_active_mode(signal, PositionSide.LONG))

    assert result is not None
    assert result["quantity"] == pytest.approx(1.0)
    assert result["close_order_mode"] == "limit_first_partial_market_fallback"
    assert record_trade_calls[-1]["quantity"] == pytest.approx(1.0)
    remaining = position_manager.get_position("binance", "BTC/USDT", account_id="main", strategy="demo_close")
    assert remaining is None
    assert live_trade_mock.await_args.kwargs["quantity"] == pytest.approx(1.0)
    assert notify_mock.await_args.args[0] == "order_executed"
    cancel_mock.assert_awaited_once_with("close-1", "BTC/USDT", "binance")


def test_close_position_uses_post_only_limit_before_market_fallback(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    record_trade_calls: list[dict] = []
    captured_requests = []
    notify_mock = AsyncMock(return_value=None)
    live_trade_mock = AsyncMock(return_value=None)

    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.0,
        leverage=2.0,
        strategy="demo_limit_fallback",
        account_id="main",
        metadata={"source": "strategy"},
    )
    position = position_manager.get_position("binance", "BTC/USDT", account_id="main", strategy="demo_limit_fallback")
    assert position is not None

    signal = Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.CLOSE_LONG,
        price=105.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name="demo_limit_fallback",
        strength=0.6,
        metadata={"account_id": "main", "exchange": "binance", "close_limit_wait_sec": 0},
    )

    async def _create_order(request):
        captured_requests.append(request)
        if len(captured_requests) == 1:
            return SimpleNamespace(
                id="limit-close-1",
                status=OrderStatus.OPEN,
                price=request.price,
                amount=request.amount,
                filled=0.0,
                fee=0.0,
            )
        return SimpleNamespace(
            id="market-close-1",
            status=OrderStatus.CLOSED,
            price=105.0,
            amount=request.amount,
            filled=request.amount,
            fee=0.0,
        )

    monkeypatch.setattr(engine, "_resolve_existing_position", AsyncMock(return_value=position))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(105.0, 105.0)))
    monkeypatch.setattr(engine, "_resolve_strategy_trade_policy", lambda *args, **kwargs: {"market_type": "future"})
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(
        execution_engine_module.risk_manager,
        "record_trade",
        lambda payload: record_trade_calls.append(dict(payload)),
    )
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", AsyncMock(side_effect=_create_order))
    monkeypatch.setattr(execution_engine_module.order_manager, "cancel_order", AsyncMock(return_value=True))
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(engine, "_record_live_strategy_trade", live_trade_mock)
    monkeypatch.setattr(engine, "_notify_callbacks", notify_mock)
    monkeypatch.setattr(execution_engine_module.audit_logger, "log", AsyncMock(return_value=None))

    result = asyncio.run(engine._close_position_in_active_mode(signal, PositionSide.LONG))

    assert result is not None
    assert len(captured_requests) == 2
    assert captured_requests[0].order_type == OrderType.LIMIT
    assert captured_requests[0].price == pytest.approx(105.0 * 1.0005)
    assert captured_requests[0].reduce_only is True
    assert captured_requests[0].params["post_only"] is True
    assert captured_requests[1].order_type == OrderType.MARKET
    assert captured_requests[1].params["close_order_mode"] == "market_fallback"
    assert captured_requests[1].params["fallback_from_order_id"] == "limit-close-1"
    assert result["order"]["id"] == "market-close-1"
    assert result["close_order_mode"] == "market_fallback"
    assert result["limit_first_order_id"] == "limit-close-1"
    assert record_trade_calls[-1]["close_order_mode"] == "market_fallback"
    assert live_trade_mock.await_args.kwargs["cost_details"]["close_order_mode"] == "market_fallback"


def test_close_position_accepts_filled_post_only_limit_without_fallback(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    record_trade_calls: list[dict] = []
    captured_requests = []

    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.0,
        leverage=2.0,
        strategy="demo_limit_fill",
        account_id="main",
        metadata={"source": "strategy"},
    )
    position = position_manager.get_position("binance", "BTC/USDT", account_id="main", strategy="demo_limit_fill")
    assert position is not None

    signal = Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.CLOSE_LONG,
        price=105.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name="demo_limit_fill",
        strength=0.6,
        metadata={"account_id": "main", "exchange": "binance", "close_limit_wait_sec": 0},
    )

    async def _create_order(request):
        captured_requests.append(request)
        return SimpleNamespace(
            id="limit-close-filled",
            status=OrderStatus.CLOSED,
            price=request.price,
            amount=request.amount,
            filled=request.amount,
            fee=0.0,
        )

    monkeypatch.setattr(engine, "_resolve_existing_position", AsyncMock(return_value=position))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(105.0, 105.0)))
    monkeypatch.setattr(engine, "_resolve_strategy_trade_policy", lambda *args, **kwargs: {"market_type": "future"})
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(
        execution_engine_module.risk_manager,
        "record_trade",
        lambda payload: record_trade_calls.append(dict(payload)),
    )
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", AsyncMock(side_effect=_create_order))
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(engine, "_record_live_strategy_trade", AsyncMock(return_value=None))
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module.audit_logger, "log", AsyncMock(return_value=None))

    result = asyncio.run(engine._close_position_in_active_mode(signal, PositionSide.LONG))

    assert result is not None
    assert len(captured_requests) == 1
    assert captured_requests[0].order_type == OrderType.LIMIT
    assert captured_requests[0].params["post_only"] is True
    assert result["order"]["id"] == "limit-close-filled"
    assert result["close_order_mode"] == "limit_first"
    assert record_trade_calls[-1]["close_order_mode"] == "limit_first"


def test_live_close_backfills_fee_from_binance_trades_and_records_slippage(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    record_trade_calls: list[dict] = []
    notify_mock = AsyncMock(return_value=None)
    live_trade_mock = AsyncMock(return_value=None)

    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.0,
        leverage=2.0,
        strategy="demo_costs",
        account_id="main",
        metadata={"source": "strategy"},
    )
    position = position_manager.get_position("binance", "BTC/USDT", account_id="main", strategy="demo_costs")
    assert position is not None

    signal = Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.CLOSE_LONG,
        price=105.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name="demo_costs",
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
            side_effect=[
                SimpleNamespace(
                    id="close-cost-1",
                    status=OrderStatus.CLOSED,
                    price=106.0,
                    amount=1.0,
                    filled=0.4,
                    fee=0.0,
                ),
                SimpleNamespace(
                    id="close-cost-2",
                    status=OrderStatus.CLOSED,
                    price=106.0,
                    amount=0.6,
                    filled=0.6,
                    fee=0.0,
                ),
            ]
        ),
    )
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(execution_engine_module, "binance_has_credentials", lambda account_id=None: True)
    monkeypatch.setattr(
        execution_engine_module,
        "binance_signed_request",
        AsyncMock(
            return_value=[
                {
                    "orderId": "close-cost-1",
                    "price": "106.0",
                    "qty": "0.4",
                    "quoteQty": "42.4",
                    "commission": "0.01696",
                    "commissionAsset": "USDT",
                }
            ]
        ),
    )
    monkeypatch.setattr(engine, "_record_live_strategy_trade", live_trade_mock)
    monkeypatch.setattr(engine, "_notify_callbacks", notify_mock)
    monkeypatch.setattr(execution_engine_module.audit_logger, "log", AsyncMock(return_value=None))

    result = asyncio.run(engine._close_position_in_active_mode(signal, PositionSide.LONG))

    assert result is not None
    assert result["fee_usd"] == pytest.approx(0.01696)
    assert result["slippage_cost_usd"] == pytest.approx(1.0)
    assert result["order"]["slippage_bps"] == pytest.approx((1.0 / 105.0) * 10000.0)
    assert record_trade_calls[-1]["fee_usd"] == pytest.approx(0.01696)
    assert record_trade_calls[-1]["slippage_cost_usd"] == pytest.approx(1.0)
    assert record_trade_calls[-1]["fee_source"] == "exchange_trades"
    assert record_trade_calls[-1]["slippage_source"] == "fill_vs_reference"
    assert live_trade_mock.await_args.kwargs["fee_usd"] == pytest.approx(0.01696)
    assert live_trade_mock.await_args.kwargs["slippage_cost_usd"] == pytest.approx(1.0)
    assert live_trade_mock.await_args.kwargs["cost_details"]["fee_source"] == "exchange_trades"
