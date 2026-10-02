"""Deterministic regressions for the 2026-10-01 trading audit."""
import asyncio
import importlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.exchanges.base_exchange import Order, OrderSide, OrderStatus, OrderType
from core.trading.execution_engine import ExecutionEngine
from core.trading.order_manager import OrderManager
from core.trading.position_manager import position_manager, PositionSide

engine_module = importlib.import_module("core.trading.execution_engine")


@pytest.fixture(autouse=True)
def clean_positions():
    position_manager.clear_all()
    yield
    position_manager.clear_all()


def test_cross_account_venue_id_is_ambiguous_and_scoped_cancel_is_correct(monkeypatch):
    manager = OrderManager()
    connectors = {a: SimpleNamespace(cancel_order=AsyncMock(return_value=True)) for a in ("A", "B")}
    monkeypatch.setattr(manager, "_resolve_cached_exchange", lambda exchange, account_id: connectors[account_id])
    orders = []
    for account in ("A", "B"):
        order = Order(id="42", symbol="BTC/USDT", exchange="binance", side=OrderSide.BUY,
                      type=OrderType.LIMIT, price=100, amount=1, status=OrderStatus.OPEN,
                      timestamp=datetime.now(timezone.utc))
        orders.append(manager._cache_live_order(order, {"account_id": account}))
    assert len(manager._orders) == 2
    assert manager.get_order_by_id("42") is None
    assert not asyncio.run(manager.cancel_order("42", "BTC/USDT", trading_mode="live"))
    assert asyncio.run(manager.cancel_order(orders[0].cache_key, "BTC/USDT", trading_mode="live"))
    connectors["A"].cancel_order.assert_awaited_once_with("42", "BTC/USDT")
    connectors["B"].cancel_order.assert_not_awaited()
    assert "/" not in orders[0].cache_key  # safe as one URL path segment


@pytest.mark.parametrize("venue_quantity,blocked", [(3.0, False), (2.5, True)])
def test_reconciliation_preserves_strategy_allocations(monkeypatch, venue_quantity, blocked):
    engine = ExecutionEngine()
    engine._paper_trading = False
    positions = [position_manager.open_position("binance", "BTC/USDT", PositionSide.LONG, 100, qty,
                 strategy=name, metadata={"trading_mode": "live"}) for name, qty in [("A", 1), ("B", 2)]]
    connector = SimpleNamespace(config=SimpleNamespace(default_type="swap"), get_positions=AsyncMock(return_value=[
        dict(symbol="BTC/USDT", amount=venue_quantity, side="long", entry_price=100, current_price=110)]))
    monkeypatch.setattr(engine, "_ensure_exchange_connector", AsyncMock(return_value=connector))
    monkeypatch.setattr(engine, "list_pending_live_order_intents", lambda **kwargs: [])
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock())
    asyncio.run(engine._reconcile_local_positions_with_exchange())
    assert [p.quantity for p in positions] == [1, 2]
    assert engine._has_unallocated_venue_position("binance", "BTC/USDT", "main") is blocked


def test_oversized_reversal_cannot_use_close_risk_exemption(monkeypatch):
    engine = ExecutionEngine()
    position_manager.open_position("binance", "BTC/USDT", PositionSide.LONG, 100, 1, strategy="manual")
    monkeypatch.setattr(engine_module.account_manager, "is_enabled", lambda account: True)
    governance = AsyncMock(return_value=SimpleNamespace(allowed=True, trace_id="test"))
    monkeypatch.setattr(engine_module.decision_engine, "evaluate_order_intent", governance)
    checks = []
    monkeypatch.setattr(engine_module.risk_manager, "pre_trade_check", lambda **kw: checks.append(kw) or False)
    submit = AsyncMock()
    monkeypatch.setattr(engine_module.order_manager, "create_order", submit)
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100, 10000)))
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000))
    result = asyncio.run(engine._execute_manual_order_single_in_active_mode(
        exchange="binance", symbol="BTC/USDT", side="sell", order_type="market", amount=100,
        price=100, leverage=100, stop_loss=None, take_profit=None, trailing_stop_pct=None,
        trailing_stop_distance=None, trigger_price=None, order_mode="normal", iceberg_parts=1,
        algo_slices=1, algo_interval_sec=0, account_id="main", reduce_only=False, strategy="manual",
    ))
    assert result is None
    assert governance.await_args.kwargs["allow_close"] is False
    assert checks[0]["allow_close"] is False
    submit.assert_not_awaited()


def test_partial_take_profit_keeps_protection_until_fill(monkeypatch):
    engine = ExecutionEngine()
    position = position_manager.open_position("binance", "BTC/USDT", PositionSide.LONG, 100, 4,
        strategy="manual", take_profit=120, metadata={"partial_take_profit_fraction": 0.5})
    submit = AsyncMock(return_value={"order_id": "pending", "status": "open", "filled": 0.0})
    monkeypatch.setattr(engine, "_execute_manual_order_single", submit)
    result = asyncio.run(engine._execute_position_partial_take_profit(position, current_price=110))
    assert not result["applied"]
    assert not position.metadata.get("partial_take_profit_done")
    assert position.take_profit == 120
    monkeypatch.setattr(engine_module.order_manager, "get_order", AsyncMock(return_value=SimpleNamespace(status=OrderStatus.OPEN, filled=0)))
    asyncio.run(engine._execute_position_partial_take_profit(position, current_price=110))
    submit.assert_awaited_once()
    # Venue reports completion; only after local ledger reconciliation consume TP.
    position_manager.close_position("binance", "BTC/USDT", 110, 2, "main", "manual")
    monkeypatch.setattr(engine_module.order_manager, "get_order", AsyncMock(return_value=SimpleNamespace(status=OrderStatus.CLOSED, filled=2)))
    result = asyncio.run(engine._execute_position_partial_take_profit(position, current_price=110))
    assert result["applied"]
    assert position.metadata["partial_take_profit_done"]
    assert position.take_profit is None
    submit.assert_awaited_once()


@pytest.mark.parametrize("connector_name", ["okx", "bybit", "gate", "binance"])
def test_derivative_quantities_round_trip_in_base_units(connector_name):
    from config.exchanges import ExchangeConfig, ExchangeType
    classes = {"okx": "OKXConnector", "bybit": "BybitConnector", "gate": "GateConnector", "binance": "BinanceConnector"}
    cls = getattr(importlib.import_module(f"core.exchanges.{connector_name}_connector"), classes[connector_name])
    connector = cls(ExchangeConfig(name=connector_name, exchange_type=ExchangeType.CEX, default_type="swap"))
    connector._client = SimpleNamespace(markets={"BTC/USDT:USDT": {"contract": True, "linear": True, "contractSize": 0.01}})
    assert connector._to_contract_amount("BTC/USDT:USDT", 0.2) == 20
    parsed = connector._parse_order(dict(id="unit-test", symbol="BTC/USDT:USDT", side="buy", type="market",
        amount=20, filled=10, remaining=10, cost=5000, average=None, price=None, status="open", timestamp=1700000000000))
    assert parsed.amount == pytest.approx(0.2)
    assert parsed.filled == pytest.approx(0.1)
    assert parsed.price == pytest.approx(50000)
    payload = dict(id="submitted", symbol="BTC/USDT:USDT", side="buy", type="limit", amount=20,
                   filled=0, remaining=20, cost=0, price=50000, status="open", timestamp=1700000000000)
    client = connector._client
    client.create_order = AsyncMock(return_value=payload)
    client.amount_to_precision = lambda symbol, value: str(value)
    client.price_to_precision = lambda symbol, value: str(value)
    connector._ensure_client = AsyncMock(return_value=client)
    created = asyncio.run(connector.create_order("BTC/USDT:USDT", OrderSide.BUY, OrderType.LIMIT, .2, 50000))
    assert float(client.create_order.await_args.kwargs["amount"]) == 20
    assert created.amount == pytest.approx(.2)
    connector._client.markets["BTC/USDT:USDT"]["inverse"] = True
    with pytest.raises(ValueError, match="inverse"):
        connector._to_contract_amount("BTC/USDT:USDT", 0.2)


def test_fifo_partial_close_allocates_entry_cost_exactly_once():
    from core.accounting.pnl_decomposer import PnLDecomposer
    pnl = PnLDecomposer()
    pnl.on_fill("BTC/USDT", "buy", 2, 100, fee=4, slippage_cost=2)
    pnl.on_fill("BTC/USDT", "sell", 1, 110, fee=1, slippage_cost=1)
    pnl.on_fill("BTC/USDT", "sell", 1, 120, fee=1, slippage_cost=1)
    assert pnl.portfolio_breakdown()["net_pnl"] == pytest.approx(20)


def test_sandbox_fast_rest_cannot_create_an_http_client(monkeypatch):
    from core.trading import binance_rest
    monkeypatch.setattr(binance_rest.account_manager, "get_exchange_credentials", lambda *args: {
        "api_key": "fake", "api_secret": "fake", "sandbox": True,
    })
    def forbidden(**kwargs):
        pytest.fail("sandbox request tried to construct production HTTP client")
    monkeypatch.setattr(binance_rest.httpx, "AsyncClient", forbidden)
    assert not binance_rest.binance_has_credentials("test")
    with pytest.raises(RuntimeError, match="sandbox"):
        asyncio.run(binance_rest.binance_signed_request("POST", "/fapi/v1/order", account_id="test"))

    from core.trading.order_manager import OrderRequest
    manager = OrderManager()
    client = SimpleNamespace(set_leverage=AsyncMock())
    connector = SimpleNamespace(config=SimpleNamespace(sandbox=True), _ensure_client=AsyncMock(return_value=client),
        create_order=AsyncMock(return_value=Order(id="sandbox", symbol="BTC/USDT", exchange="binance", side=OrderSide.BUY,
            type=OrderType.LIMIT, price=100, amount=1, status=OrderStatus.OPEN, timestamp=datetime.now(timezone.utc))))
    monkeypatch.setattr(manager, "_ensure_exchange_connector", AsyncMock(return_value=connector))
    monkeypatch.setattr(manager, "_evaluate_order_governance", AsyncMock(return_value=SimpleNamespace(allowed=True, reduce_only=False, trace_id="dummy")))
    result = asyncio.run(manager._create_real_order(OrderRequest(symbol="BTC/USDT", side=OrderSide.BUY,
        order_type=OrderType.LIMIT, amount=1, price=100, exchange="binance", account_id="test",
        params={"trading_mode": "live", "market_type": "future", "leverage": 3})))
    assert result is not None
    client.set_leverage.assert_awaited_once_with(3, "BTC/USDT")
    connector.create_order.assert_awaited_once()


def test_unfilled_algo_children_do_not_count_as_fills(monkeypatch):
    engine = ExecutionEngine()
    monkeypatch.setattr(engine, "_execute_manual_order_single", AsyncMock(return_value={
        "order_id": "test", "status": "open", "amount": 1, "filled": 0, "price": 100,
    }))
    result = asyncio.run(engine.execute_manual_order("binance", "BTC/USDT", "buy", "limit", 2,
        price=100, order_mode="twap", algo_slices=2))
    assert result["filled"] == 0
    assert result["status"] == "open"


def test_close_signal_records_incremental_realized_pnl(monkeypatch):
    from core.strategies import Signal, SignalType
    engine = ExecutionEngine()
    position_manager.open_position("binance", "ETH/USDT", PositionSide.LONG, 100, 2, strategy="S")
    position_manager.close_position("binance", "ETH/USDT", 110, 1, "main", "S")
    monkeypatch.setattr(engine, "_resolve_strategy_trade_policy", lambda *args: {})
    monkeypatch.setattr(engine, "_requires_strategy_position_isolation", lambda *args: True)
    monkeypatch.setattr(engine, "_close_limit_first_enabled", lambda *args: False)
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda *args: {})
    monkeypatch.setattr(engine, "_resolve_execution_costs", AsyncMock(return_value={"fee_usd": 0, "slippage_cost_usd": 0}))
    monkeypatch.setattr(engine, "_record_live_strategy_trade", AsyncMock())
    monkeypatch.setattr(engine_module.audit_logger, "log", AsyncMock())
    monkeypatch.setattr(engine_module.order_manager, "create_order", AsyncMock(return_value=Order(
        id="close", symbol="ETH/USDT", side=OrderSide.SELL, type=OrderType.MARKET, price=120,
        amount=1, filled=1, status=OrderStatus.CLOSED, timestamp=datetime.now(timezone.utc), exchange="binance")))
    recorded = []
    monkeypatch.setattr(engine_module.risk_manager, "record_trade", recorded.append)
    signal = Signal(symbol="ETH/USDT", signal_type=SignalType.CLOSE_LONG, price=120, strength=1,
                    timestamp=datetime.now(timezone.utc), strategy_name="S", metadata={"account_id": "main"})
    asyncio.run(engine._close_position_in_active_mode(signal, PositionSide.LONG))
    assert recorded[0]["pnl"] == 20
