from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.exchanges.base_exchange import Order, OrderStatus
from core.marketdata.ws_client import WSClient, WSClientConfig
from core.trading.order_manager import OrderManager, OrderRequest, OrderSide, OrderType

order_manager_module = __import__("core.trading.order_manager", fromlist=[""])


class _FakeExchange:
    def __init__(self) -> None:
        self.calls = []

    async def create_order(self, symbol, side, order_type, amount, price=None, params=None):
        self.calls.append(
            {
                "symbol": symbol,
                "side": side,
                "order_type": order_type,
                "amount": amount,
                "price": price,
                "params": dict(params or {}),
            }
        )
        return Order(
            id=f"ex-{len(self.calls)}",
            symbol=symbol,
            side=side,
            type=order_type,
            price=float(price or 0.0),
            amount=float(amount or 0.0),
            filled=float(amount or 0.0),
            remaining=0.0,
            cost=float(price or 0.0) * float(amount or 0.0),
            status=OrderStatus.CLOSED,
            timestamp=datetime.now(timezone.utc),
            exchange="fake",
        )


def test_real_order_reuses_generated_client_order_id_for_request_retry(monkeypatch):
    manager = OrderManager()
    manager.set_paper_trading(False)
    fake_exchange = _FakeExchange()

    monkeypatch.setattr(manager, "_ensure_exchange_connector", AsyncMock(return_value=fake_exchange))
    monkeypatch.setattr(
        order_manager_module.risk_manager,
        "get_risk_report",
        lambda: {"equity": {"current": 10_000.0}},
    )
    monkeypatch.setattr(
        order_manager_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(
            return_value=SimpleNamespace(
                allowed=True,
                reason="",
                reduce_only=False,
                trace_id="trace-id",
            )
        ),
    )

    request = OrderRequest(
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        amount=0.01,
        price=50_000.0,
        exchange="okx",
        strategy="strategy_name_longer_than_8",
    )

    first = asyncio.run(manager._create_real_order(request))
    second = asyncio.run(manager._create_real_order(request))

    assert first is not None
    assert second is None
    assert len(fake_exchange.calls) == 1
    client_order_id = fake_exchange.calls[0]["params"]["newClientOrderId"]
    assert fake_exchange.calls[0]["params"]["clientOrderId"] == client_order_id
    assert request.params["newClientOrderId"] == client_order_id
    assert request.params["clientOrderId"] == client_order_id
    prefix, ts_ms, seq = client_order_id.split("-")
    assert prefix == "strategy"
    assert ts_ms.isdigit()
    assert seq.isdigit()
    assert manager.get_last_error().startswith("duplicate client_order_id detected")


class _RaisingExchange:
    """Exchange stub whose create_order always raises a configured error."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls = 0

    async def create_order(self, symbol, side, order_type, amount, price=None, params=None):
        self.calls += 1
        raise self.error


def _make_real_manager(monkeypatch, exchange):
    manager = OrderManager()
    manager.set_paper_trading(False)
    monkeypatch.setattr(manager, "_ensure_exchange_connector", AsyncMock(return_value=exchange))
    monkeypatch.setattr(
        order_manager_module.risk_manager,
        "get_risk_report",
        lambda: {"equity": {"current": 10_000.0}},
    )
    monkeypatch.setattr(
        order_manager_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, reason="", reduce_only=False, trace_id="t")),
    )
    return manager


def _limit_request():
    # exchange="okx" keeps us off the binance-futures fast path → goes straight
    # to exchange.create_order, which the raising stub intercepts.
    return OrderRequest(
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        amount=0.01,
        price=50_000.0,
        exchange="okx",
        strategy="strat",
    )


def test_ambiguous_timeout_holds_client_order_id_and_blocks_blind_retry(monkeypatch):
    """A submit timeout leaves fill-state unknown → the clientOrderId must stay
    reserved so an upstream retry of the same request cannot double-fill."""
    ex = _RaisingExchange(asyncio.TimeoutError("read timed out"))
    manager = _make_real_manager(monkeypatch, ex)
    request = _limit_request()

    first = asyncio.run(manager._create_real_order(request))
    assert first is None
    coid = request.params["newClientOrderId"]
    # Held, NOT released — the order may have reached the exchange.
    assert manager._is_client_order_id_active(coid) is True

    # Retrying the same request is rejected as a duplicate → no second submit.
    second = asyncio.run(manager._create_real_order(request))
    assert second is None
    assert ex.calls == 1
    assert manager.get_last_error().startswith("duplicate client_order_id")


def test_binance_fast_path_ambiguous_error_skips_ccxt_fallback(monkeypatch):
    """A Binance raw submit timeout must not be converted into a ccxt retry."""
    fallback_exchange = _FakeExchange()
    manager = _make_real_manager(monkeypatch, fallback_exchange)
    monkeypatch.setattr(manager, "_sync_binance_futures_leverage", AsyncMock(return_value=True))
    monkeypatch.setattr(
        order_manager_module,
        "binance_signed_request",
        AsyncMock(side_effect=asyncio.TimeoutError("raw submit timed out")),
    )

    request = OrderRequest(
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        amount=0.01,
        price=50_000.0,
        exchange="binance",
        strategy="strat",
        params={"market_type": "future"},
    )

    first = asyncio.run(manager._create_real_order(request))
    assert first is None
    assert fallback_exchange.calls == []
    coid = request.params["newClientOrderId"]
    assert manager._is_client_order_id_active(coid) is True
    assert "raw submit timed out" in manager.get_last_error()

    second = asyncio.run(manager._create_real_order(request))
    assert second is None
    assert fallback_exchange.calls == []
    assert manager.get_last_error().startswith("duplicate client_order_id")


def test_definitive_rejection_releases_client_order_id_and_allows_retry(monkeypatch):
    """A definitive pre-execution rejection (order never executed) releases the
    clientOrderId so an honest retry is allowed."""
    ex = _RaisingExchange(ValueError("invalid order: -1111 precision over maximum"))
    manager = _make_real_manager(monkeypatch, ex)
    request = _limit_request()

    first = asyncio.run(manager._create_real_order(request))
    assert first is None
    coid = request.params["newClientOrderId"]
    # Released — safe to retry.
    assert manager._is_client_order_id_active(coid) is False

    second = asyncio.run(manager._create_real_order(request))
    assert second is None
    assert ex.calls == 2  # both honest attempts reached the exchange


class _RequestTimeout(Exception):
    """Stands in for ccxt.RequestTimeout (matched by class name)."""


@pytest.mark.parametrize(
    "error,expected",
    [
        (asyncio.TimeoutError(), True),
        (TimeoutError("x"), True),
        (ConnectionError("connection reset by peer"), True),
        (_RequestTimeout("binance POST timed out"), True),
        (RuntimeError("Request timeout after 8s"), True),
        (RuntimeError("server disconnected without response"), True),
        (ValueError("insufficient funds"), False),
        (ValueError("-1111 precision over maximum"), False),
        (RuntimeError("invalid order: reduceOnly rejected"), False),
        (RuntimeError("-2010 NEW_ORDER_REJECTED"), False),
    ],
)
def test_is_ambiguous_submit_error_classifier(error, expected):
    assert OrderManager._is_ambiguous_submit_error(error) is expected


def test_ws_client_skeleton_methods_raise_not_implemented():
    client = WSClient(WSClientConfig(url="wss://example.invalid", name="test_ws"))

    with pytest.raises(NotImplementedError):
        asyncio.run(client.connect())
    with pytest.raises(NotImplementedError):
        asyncio.run(client.subscribe({"channel": "ticker"}))
    with pytest.raises(NotImplementedError):
        asyncio.run(client.run_forever())
