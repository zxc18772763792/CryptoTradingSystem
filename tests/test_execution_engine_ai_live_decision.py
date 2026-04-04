from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.exchanges.base_exchange import OrderStatus, OrderType
from core.strategies import Signal, SignalType
from core.trading.execution_engine import ExecutionEngine
from core.trading.position_manager import PositionSide, position_manager


execution_engine_module = importlib.import_module("core.trading.execution_engine")


def _make_signal(*, signal_type: SignalType = SignalType.BUY) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        signal_type=signal_type,
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name="ai_gate_strategy",
        strength=0.9,
        metadata={"account_id": "main", "exchange": "binance"},
    )


def _make_position(side: PositionSide, quantity: float, entry_price: float = 100.0) -> SimpleNamespace:
    position = SimpleNamespace(
        symbol="BTC/USDT",
        exchange="binance",
        side=side,
        entry_price=entry_price,
        current_price=entry_price,
        quantity=quantity,
        value=entry_price * quantity,
        realized_pnl=0.0,
        leverage=2.0,
        margin=(entry_price * quantity) / 2.0,
        stop_loss=None,
        take_profit=None,
        trailing_stop_pct=None,
        trailing_stop_distance=None,
    )

    def _update_price(price: float) -> None:
        position.current_price = float(price)
        position.value = position.current_price * float(position.quantity or 0.0)

    position.update_price = _update_price
    return position


def test_execute_signal_respects_ai_live_decision_block(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal()
    rejected_order = SimpleNamespace(id="rej-ai-1")

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(engine, "_resolve_strategy_trade_policy", lambda strategy_name, exchange: {"allow_long": True, "allow_short": True})
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=10000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(engine, "_calculate_quantity", AsyncMock(return_value=0.01))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 1.0)))
    monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (99.0, 102.0))
    monkeypatch.setattr(
        engine,
        "_evaluate_live_ai_decision",
        AsyncMock(
            return_value={
                "action": "block",
                "applied": True,
                "provider": "openai",
                "model": "gpt-5.4",
                "reason": "model_block",
            }
        ),
    )
    monkeypatch.setattr(execution_engine_module.order_manager, "record_rejected_order", AsyncMock(return_value=rejected_order))
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module, "write_audit", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))
    assert result is None
    assert int(engine.get_signal_diagnostics().get("ai_rejected") or 0) == 1


def test_execute_signal_rejects_same_direction_when_ai_enforces_reduce_only(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal(signal_type=SignalType.BUY)
    existing_position = _make_position(PositionSide.LONG, quantity=0.02)
    rejected_order = SimpleNamespace(id="rej-ai-reduce-only")
    create_order_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(
        engine,
        "_resolve_strategy_trade_policy",
        lambda strategy_name, exchange: {"allow_long": True, "allow_short": True, "allow_pyramiding": True},
    )
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: existing_position)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=10000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(engine, "_calculate_quantity", AsyncMock(return_value=0.01))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 1.0)))
    monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (99.0, 102.0))
    monkeypatch.setattr(
        engine,
        "_evaluate_live_ai_decision",
        AsyncMock(
            return_value={
                "action": "reduce_only",
                "applied": True,
                "provider": "openai",
                "model": "gpt-5.4",
                "reason": "only reduce risk",
            }
        ),
    )
    rejected_mock = AsyncMock(return_value=rejected_order)
    monkeypatch.setattr(execution_engine_module.order_manager, "record_rejected_order", rejected_mock)
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", create_order_mock)
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module, "write_audit", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))

    assert result is None
    assert create_order_mock.await_count == 0
    assert rejected_mock.await_count == 1
    assert int(engine.get_signal_diagnostics().get("ai_reduce_only_rejected") or 0) == 1
    assert engine.get_signal_diagnostics()["last_result"]["status"] == "ai_reduce_only_rejected"


def test_execute_signal_enforces_ai_reduce_only_on_opposite_side_fill(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal(signal_type=SignalType.SELL)
    existing_position = _make_position(PositionSide.LONG, quantity=0.02)
    captured_requests = []
    closed_quantities = []
    opened_positions = []
    trade_records = []

    async def _create_order(request):
        captured_requests.append(request)
        return SimpleNamespace(
            id="ord-ai-reduce-only",
            price=100.0,
            amount=request.amount,
            filled=request.amount,
            status=OrderStatus.CLOSED,
        )

    def _close_position(exchange, symbol, close_price, quantity=None, account_id=None):
        close_qty = float(quantity or 0.0)
        closed_quantities.append(close_qty)
        existing_position.realized_pnl = float(existing_position.realized_pnl or 0.0)
        existing_position.quantity = max(0.0, float(existing_position.quantity or 0.0) - close_qty)
        existing_position.update_price(close_price)
        return existing_position

    def _open_position(**kwargs):
        opened_positions.append(kwargs)
        return None

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(
        engine,
        "_resolve_strategy_trade_policy",
        lambda strategy_name, exchange: {"allow_long": True, "allow_short": True, "reverse_on_signal": False},
    )
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: existing_position)
    monkeypatch.setattr(execution_engine_module.position_manager, "close_position", _close_position)
    monkeypatch.setattr(execution_engine_module.position_manager, "open_position", _open_position)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=10000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(engine, "_calculate_quantity", AsyncMock(return_value=0.05))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 5.0)))
    monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (101.0, 96.0))
    monkeypatch.setattr(
        engine,
        "_evaluate_live_ai_decision",
        AsyncMock(
            return_value={
                "action": "reduce_only",
                "applied": True,
                "provider": "openai",
                "model": "gpt-5.4",
                "reason": "de-risk first",
            }
        ),
    )
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, reason="", trace_id="trace-ai-ro")),
    )
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(execution_engine_module.risk_manager, "record_trade", lambda payload: trade_records.append(payload))
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", AsyncMock(side_effect=_create_order))
    monkeypatch.setattr(engine, "_record_live_strategy_trade", AsyncMock(return_value=None))
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module, "write_audit", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))

    assert result is not None
    assert len(captured_requests) == 1
    request = captured_requests[0]
    assert request.reduce_only is True
    assert request.amount == 0.02
    assert request.params["ai_reduce_only"] is True
    assert request.params["ai_reduce_only_reason"] == "de-risk first"
    assert request.params["ai_reduce_only_adjusted_amount"] == 0.02
    assert closed_quantities == [0.02]
    assert opened_positions == []
    assert existing_position.quantity == 0.0
    assert int(engine.get_signal_diagnostics().get("ai_reduce_only_adjusted") or 0) == 1
    assert trade_records[-1]["notional"] == 2.0


def test_execute_signal_skips_same_direction_when_exchange_live_position_exists(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal(signal_type=SignalType.SELL)
    create_order_mock = AsyncMock(return_value=None)
    fake_connector = SimpleNamespace(
        config=SimpleNamespace(default_type="future"),
        get_positions=AsyncMock(
            return_value=[
                {
                    "symbol": "BTC/USDT:USDT",
                    "side": "short",
                    "amount": -0.02,
                    "entry_price": 100.0,
                    "current_price": 99.0,
                    "unrealized_pnl": 0.02,
                    "leverage": 2.0,
                }
            ]
        ),
    )

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(
        engine,
        "_resolve_strategy_trade_policy",
        lambda strategy_name, exchange: {"allow_long": True, "allow_short": True, "allow_pyramiding": False},
    )
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(execution_engine_module.exchange_manager, "get_exchange", lambda exchange: fake_connector)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=10000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", create_order_mock)

    result = asyncio.run(engine.execute_signal(signal))

    assert result is None
    assert create_order_mock.await_count == 0
    assert engine.get_signal_diagnostics()["last_result"]["status"] == "existing_position_blocked"
    assert engine.get_signal_diagnostics()["last_result"]["position_source"] == "exchange_live"


def test_execute_signal_allows_same_direction_add_below_half_cap_and_caps_quantity(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal(signal_type=SignalType.SELL)
    signal.metadata["same_direction_max_exposure_ratio"] = 0.5
    existing_position = _make_position(PositionSide.SHORT, quantity=0.3)
    captured_requests = []
    trade_records = []

    async def _create_order(request):
        captured_requests.append(request)
        return SimpleNamespace(
            id="ord-same-dir-half-cap",
            price=100.0,
            amount=request.amount,
            filled=request.amount,
            status=OrderStatus.CLOSED,
        )

    async def _resolve_order_context(exchange, symbol, quantity, preferred_price):
        price = float(preferred_price or 100.0)
        return price, float(quantity or 0.0) * price

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(
        engine,
        "_resolve_strategy_trade_policy",
        lambda strategy_name, exchange: {
            "allow_long": True,
            "allow_short": True,
            "allow_pyramiding": False,
            "market_type": "future",
        },
    )
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: existing_position)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.0)
    monkeypatch.setattr(engine, "_resolve_price", AsyncMock(return_value=100.0))
    monkeypatch.setattr(engine, "_get_exchange_amount_rules", AsyncMock(return_value=(0.0, 8)))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(side_effect=_resolve_order_context))
    monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (101.0, 96.0))
    monkeypatch.setattr(
        engine,
        "_evaluate_live_ai_decision",
        AsyncMock(return_value={"action": "allow", "applied": False, "reason": ""}),
    )
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, reason="", trace_id="trace-half-cap")),
    )
    monkeypatch.setattr(execution_engine_module.risk_manager, "max_position_size", 0.1)
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(execution_engine_module.risk_manager, "record_trade", lambda payload: trade_records.append(payload))
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", AsyncMock(side_effect=_create_order))
    monkeypatch.setattr(engine, "_record_live_strategy_trade", AsyncMock(return_value=None))
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module, "write_audit", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))

    assert result is not None
    assert len(captured_requests) == 1
    request = captured_requests[0]
    assert request.amount == 0.1996
    assert existing_position.quantity == 0.4996
    assert trade_records[-1]["notional"] == 19.96
    assert engine.get_signal_diagnostics()["last_result"]["status"] == "executed"


def test_close_signal_uses_exchange_live_position_when_local_missing(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal(signal_type=SignalType.CLOSE_SHORT)
    trade_records = []
    create_order_mock = AsyncMock(
        return_value=SimpleNamespace(
            id="ord-close-live",
            price=100.0,
            amount=0.02,
            filled=0.02,
            status=OrderStatus.CLOSED,
        )
    )
    fake_connector = SimpleNamespace(
        config=SimpleNamespace(default_type="future"),
        get_positions=AsyncMock(
            return_value=[
                {
                    "symbol": "BTCUSDT",
                    "side": "short",
                    "amount": -0.02,
                    "entry_price": 101.0,
                    "current_price": 100.5,
                    "unrealizedPnl": 0.02,
                    "leverage": 2.0,
                }
            ]
        ),
    )

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(engine, "_resolve_strategy_trade_policy", lambda strategy_name, exchange: {"market_type": "future"})
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(execution_engine_module.position_manager, "close_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(execution_engine_module.exchange_manager, "get_exchange", lambda exchange: fake_connector)
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 2.0)))
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=10000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(execution_engine_module.risk_manager, "record_trade", lambda payload: trade_records.append(payload))
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", create_order_mock)
    monkeypatch.setattr(engine, "_record_live_strategy_trade", AsyncMock(return_value=None))
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module.audit_logger, "log", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))

    assert result is not None
    assert result["action"] == "close_position"
    assert result["order"]["id"] == "ord-close-live"
    assert trade_records[-1]["side"] == "close_short"
    assert trade_records[-1]["notional"] == 2.0


def test_execute_signal_keeps_market_order_with_explicit_quantity(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal(signal_type=SignalType.SELL)
    signal.symbol = "ETH/USDT"
    signal.price = 2000.0
    signal.quantity = 0.05
    captured_requests = []

    async def _create_order(request):
        captured_requests.append(request)
        return SimpleNamespace(
            id="ord-secondary-market",
            price=2000.0,
            amount=request.amount,
            filled=request.amount,
            status=OrderStatus.CLOSED,
        )

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(
        engine,
        "_resolve_strategy_trade_policy",
        lambda strategy_name, exchange: {
            "allow_long": True,
            "allow_short": True,
            "reverse_on_signal": True,
            "allow_pyramiding": False,
            "market_type": "future",
        },
    )
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=5000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.3)
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(2000.0, 100.0)))
    monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (2020.0, 1940.0))
    monkeypatch.setattr(
        engine,
        "_evaluate_live_ai_decision",
        AsyncMock(return_value={"action": "allow", "applied": False, "reason": ""}),
    )
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, reason="", trace_id="trace-secondary-market")),
    )
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(execution_engine_module.risk_manager, "record_trade", lambda payload: None)
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", AsyncMock(side_effect=_create_order))
    monkeypatch.setattr(engine, "_record_live_strategy_trade", AsyncMock(return_value=None))
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module, "write_audit", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))

    assert result is not None
    assert len(captured_requests) == 1
    assert captured_requests[0].amount == 0.05
    assert captured_requests[0].order_type == OrderType.MARKET


def test_execute_signal_can_still_request_explicit_limit_order(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal(signal_type=SignalType.BUY)
    signal.symbol = "ETH/USDT"
    signal.price = 1995.0
    signal.quantity = 0.08
    signal.metadata["order_type"] = "limit"
    captured_requests = []

    async def _create_order(request):
        captured_requests.append(request)
        return SimpleNamespace(
            id="ord-explicit-limit",
            price=1995.0,
            amount=request.amount,
            filled=request.amount,
            status=OrderStatus.CLOSED,
        )

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(
        engine,
        "_resolve_strategy_trade_policy",
        lambda strategy_name, exchange: {
            "allow_long": True,
            "allow_short": True,
            "reverse_on_signal": True,
            "allow_pyramiding": False,
            "market_type": "future",
        },
    )
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=5000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.3)
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(1995.0, 159.6)))
    monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (1970.0, 2050.0))
    monkeypatch.setattr(
        engine,
        "_evaluate_live_ai_decision",
        AsyncMock(return_value={"action": "allow", "applied": False, "reason": ""}),
    )
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, reason="", trace_id="trace-explicit-limit")),
    )
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(execution_engine_module.risk_manager, "record_trade", lambda payload: None)
    monkeypatch.setattr(execution_engine_module.order_manager, "create_order", AsyncMock(side_effect=_create_order))
    monkeypatch.setattr(engine, "_record_live_strategy_trade", AsyncMock(return_value=None))
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module, "write_audit", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))

    assert result is not None
    assert len(captured_requests) == 1
    assert captured_requests[0].order_type == OrderType.LIMIT
    assert captured_requests[0].price == 1995.0


def test_execute_signal_sizes_pair_legs_with_shared_hedge_ratio(monkeypatch):
    position_manager.clear_all()
    try:
        engine = ExecutionEngine()
        engine._paper_trading = False

        common_metadata = {
            "account_id": "main",
            "exchange": "binance",
            "pair_group_id": "pair-balance-1",
            "pair_unit_notional": 200.0,
            "pair_min_leg_notional_fraction": 0.5,
        }
        primary_signal = Signal(
            symbol="AAA/USDT",
            signal_type=SignalType.BUY,
            price=100.0,
            timestamp=datetime.now(timezone.utc),
            strategy_name="pairs_balance_strategy",
            strength=1.0,
            metadata={**common_metadata, "pair_quantity_scale": 1.0, "pair_leg_notional_fraction": 0.5},
        )
        secondary_signal = Signal(
            symbol="BBB/USDT",
            signal_type=SignalType.SELL,
            price=200.0,
            timestamp=datetime.now(timezone.utc),
            strategy_name="pairs_balance_strategy",
            strength=1.0,
            metadata={**common_metadata, "pair_quantity_scale": 0.5, "pair_leg_notional_fraction": 0.5},
        )

        captured_requests = []
        trade_records = []

        async def _create_order(request):
            captured_requests.append(request)
            return SimpleNamespace(
                id=f"ord-{request.symbol}",
                price=float(request.price or (100.0 if request.symbol == "AAA/USDT" else 200.0)),
                amount=request.amount,
                filled=request.amount,
                status=OrderStatus.CLOSED,
            )

        async def _resolve_order_context(exchange, symbol, quantity, preferred_price):
            price = float(preferred_price or (100.0 if symbol == "AAA/USDT" else 200.0))
            return price, float(quantity or 0.0) * price

        async def _resolve_price(exchange, symbol, preferred_price=None):
            return float(preferred_price or (100.0 if symbol == "AAA/USDT" else 200.0))

        monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
        monkeypatch.setattr(
            engine,
            "_resolve_strategy_trade_policy",
            lambda strategy_name, exchange: {
                "allow_long": True,
                "allow_short": True,
                "reverse_on_signal": True,
                "allow_pyramiding": False,
                "market_type": "future",
            },
        )
        monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000.0))
        monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
        monkeypatch.setattr(execution_engine_module.settings, "MIN_STRATEGY_ORDER_USD", 50.0, raising=False)
        monkeypatch.setattr(engine, "_resolve_price", AsyncMock(side_effect=_resolve_price))
        monkeypatch.setattr(engine, "_get_exchange_amount_rules", AsyncMock(return_value=(0.0, 8)))
        monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(side_effect=_resolve_order_context))
        monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (None, None))
        monkeypatch.setattr(
            engine,
            "_evaluate_live_ai_decision",
            AsyncMock(return_value={"action": "allow", "applied": False, "reason": ""}),
        )
        monkeypatch.setattr(
            execution_engine_module.decision_engine,
            "evaluate_order_intent",
            AsyncMock(return_value=SimpleNamespace(allowed=True, reason="", trace_id="trace-pair-balance")),
        )
        monkeypatch.setattr(execution_engine_module.risk_manager, "max_position_size", 0.1)
        monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
        monkeypatch.setattr(execution_engine_module.risk_manager, "record_trade", lambda payload: trade_records.append(payload))
        monkeypatch.setattr(execution_engine_module.order_manager, "create_order", AsyncMock(side_effect=_create_order))
        monkeypatch.setattr(engine, "_record_live_strategy_trade", AsyncMock(return_value=None))
        monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
        monkeypatch.setattr(execution_engine_module, "write_audit", AsyncMock(return_value=None))

        primary_result = asyncio.run(engine.execute_signal(primary_signal))
        secondary_result = asyncio.run(engine.execute_signal(secondary_signal))

        assert primary_result is not None
        assert secondary_result is not None
        assert len(captured_requests) == 2
        assert captured_requests[0].amount == pytest.approx(0.499)
        assert captured_requests[1].amount == pytest.approx(0.2495)

        pos_primary = position_manager.get_position("binance", "AAA/USDT", account_id="main")
        pos_secondary = position_manager.get_position("binance", "BBB/USDT", account_id="main")
        assert pos_primary is not None
        assert pos_secondary is not None
        assert pos_primary.value == pytest.approx(49.9)
        assert pos_secondary.value == pytest.approx(49.9)
        assert trade_records[-2]["notional"] == pytest.approx(49.9)
        assert trade_records[-1]["notional"] == pytest.approx(49.9)
    finally:
        position_manager.clear_all()
