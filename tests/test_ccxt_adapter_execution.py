"""Tests for CCXTExchangeAdapter create_order/cancel_order/fetch_order.

These were previously NotImplementedError stubs. The implementations route
through ``_call`` which wraps the synchronous ccxt client via asyncio.to_thread,
so the tests stub the client and ``_call`` directly to keep the unit boundary
crisp.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.exchange_adapters.base import ExchangeOrderRequest
from core.exchange_adapters.ccxt_adapter import CCXTExchangeAdapter


def _make_adapter(*, supports_execution: bool) -> CCXTExchangeAdapter:
    adapter = CCXTExchangeAdapter(exchange="binance", market_type="swap")
    adapter._client = SimpleNamespace(has={"createOrder": True, "cancelOrder": True, "fetchOrder": True})
    adapter.supports_execution = supports_execution
    return adapter


def _sample_order_payload(**overrides) -> dict:
    base = {
        "id": "order-1",
        "clientOrderId": "client-7",
        "symbol": "BTC/USDT",
        "status": "open",
        "side": "buy",
        "type": "limit",
        "amount": 0.25,
        "filled": 0.10,
        "remaining": 0.15,
        "price": 50000.0,
        "average": 50001.0,
        "fee": {"cost": 0.0125, "currency": "USDT"},
        "timestamp": 1_700_000_000_000,
    }
    base.update(overrides)
    return base


def test_create_order_rejected_when_supports_execution_false():
    """Defense in depth: a direct caller bypassing OrderIntentRouter must not
    silently flip the adapter into live."""
    adapter = _make_adapter(supports_execution=False)
    adapter._call = AsyncMock()
    request = ExchangeOrderRequest(symbol="BTC/USDT", side="buy", order_type="market", amount=0.1)

    with pytest.raises(RuntimeError, match="not enabled for order execution"):
        asyncio.run(adapter.create_order(request))

    adapter._call.assert_not_called()


def test_create_order_passes_through_to_ccxt_when_enabled():
    """Verify the ccxt call shape: (symbol, type, side, amount, price, params)."""
    adapter = _make_adapter(supports_execution=True)
    adapter._call = AsyncMock(return_value=_sample_order_payload())
    request = ExchangeOrderRequest(
        symbol="BTCUSDT",  # un-normalized; adapter should normalize to BTC/USDT
        side="buy",
        order_type="limit",
        amount=0.25,
        price=50000.0,
        reduce_only=True,
        client_order_id="client-7",
        params={"timeInForce": "GTC"},
    )

    snapshot = asyncio.run(adapter.create_order(request))

    adapter._call.assert_awaited_once()
    call_args = adapter._call.await_args.args
    assert call_args[0] == "create_order"
    assert call_args[1] == "BTC/USDT"  # normalized
    assert call_args[2] == "limit"
    assert call_args[3] == "buy"
    assert call_args[4] == 0.25
    assert call_args[5] == 50000.0
    params = call_args[6]
    assert params["timeInForce"] == "GTC"
    assert params["reduceOnly"] is True
    assert params["clientOrderId"] == "client-7"

    assert snapshot.order_id == "order-1"
    assert snapshot.symbol == "BTC/USDT"
    assert snapshot.status == "open"
    assert snapshot.amount == 0.25
    assert snapshot.filled == 0.10
    assert snapshot.remaining == 0.15
    assert snapshot.price == 50000.0
    assert snapshot.fee == pytest.approx(0.0125)
    assert snapshot.fee_currency == "USDT"
    assert snapshot.timestamp == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)


def test_create_order_market_drops_price_field():
    """ccxt rejects market orders that carry an explicit price; we must not
    forward the strategy's reference price for ``type=market``."""
    adapter = _make_adapter(supports_execution=True)
    adapter._call = AsyncMock(return_value=_sample_order_payload(type="market"))
    request = ExchangeOrderRequest(
        symbol="BTC/USDT", side="sell", order_type="market", amount=0.5, price=99999.0
    )

    asyncio.run(adapter.create_order(request))
    call_args = adapter._call.await_args.args
    assert call_args[2] == "market"
    assert call_args[5] is None  # price suppressed for market orders


def test_cancel_order_passes_id_and_symbol():
    adapter = _make_adapter(supports_execution=True)
    adapter._call = AsyncMock(return_value=_sample_order_payload(status="canceled"))

    snapshot = asyncio.run(adapter.cancel_order("BTCUSDT", "order-1", {"recvWindow": 5000}))

    adapter._call.assert_awaited_once()
    call_args = adapter._call.await_args.args
    assert call_args[0] == "cancel_order"
    assert call_args[1] == "order-1"
    assert call_args[2] == "BTC/USDT"
    assert call_args[3] == {"recvWindow": 5000}
    assert snapshot.status == "canceled"


def test_cancel_order_blocked_when_supports_execution_false():
    adapter = _make_adapter(supports_execution=False)
    adapter._call = AsyncMock()
    with pytest.raises(RuntimeError, match="not enabled for order execution"):
        asyncio.run(adapter.cancel_order("BTC/USDT", "order-1"))
    adapter._call.assert_not_called()


def test_fetch_order_works_even_when_execution_disabled():
    """fetch_order is read-only and used for reconciliation. It must work
    regardless of the execution gate so monitoring stays available."""
    adapter = _make_adapter(supports_execution=False)
    adapter._call = AsyncMock(return_value=_sample_order_payload(status="closed", filled=0.25, remaining=0.0))

    snapshot = asyncio.run(adapter.fetch_order("BTC/USDT", "order-1"))

    adapter._call.assert_awaited_once_with("fetch_order", "order-1", "BTC/USDT")
    assert snapshot.status == "closed"
    assert snapshot.filled == 0.25
    assert snapshot.remaining == 0.0


def test_snapshot_handles_missing_optional_fields():
    """Some venues omit fee/timestamp/clientOrderId. The mapper must produce a
    snapshot with sensible defaults rather than crashing."""
    adapter = _make_adapter(supports_execution=True)
    raw = {"id": "x", "symbol": "ETH/USDT", "status": "open", "side": "buy", "type": "market"}
    snapshot = adapter._order_to_snapshot(raw, fallback_symbol="ETH/USDT")
    assert snapshot.order_id == "x"
    assert snapshot.client_order_id is None
    assert snapshot.fee is None
    assert snapshot.fee_currency is None
    assert snapshot.timestamp is None
    assert snapshot.amount == 0.0
    assert snapshot.remaining == 0.0
