"""Focused tests for OKX and Bybit connector lifecycle and precision handling."""

from __future__ import annotations

import asyncio
import importlib
from typing import Any

import pytest

from config.exchanges import ExchangeConfig, ExchangeType
from core.exchanges.base_exchange import OrderSide, OrderType
from core.exchanges.bybit_connector import BybitConnector
from core.exchanges.okx_connector import OKXConnector


CONNECTOR_CASES = [
    (OKXConnector, "core.exchanges.okx_connector", "okx", "okx"),
    (BybitConnector, "core.exchanges.bybit_connector", "bybit", "bybit"),
]


def _config(name: str) -> ExchangeConfig:
    return ExchangeConfig(
        name=name,
        exchange_type=ExchangeType.CEX,
        api_key="test_key",
        api_secret="test_secret",
        passphrase="test_passphrase" if name == "okx" else None,
        sandbox=True,
    )


def _order_payload(**overrides: Any) -> dict:
    payload = {
        "id": "order-1",
        "symbol": "BTC/USDT",
        "side": "buy",
        "type": "limit",
        "price": 27123.4,
        "amount": 1.2345,
        "filled": 0,
        "remaining": 1.2345,
        "cost": 0,
        "status": "open",
        "timestamp": 1609459200000,
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize("connector_cls,module_path,factory_attr,name", CONNECTOR_CASES)
def test_connect_is_serialized_and_replaces_clients(
    connector_cls,
    module_path,
    factory_attr,
    name,
    monkeypatch,
):
    module = importlib.import_module(module_path)
    created = []

    class SlowClient:
        active_loads = 0
        max_active_loads = 0

        def __init__(self, config):
            self.config = config
            self.closed = False

        async def load_markets(self):
            type(self).active_loads += 1
            type(self).max_active_loads = max(
                type(self).max_active_loads,
                type(self).active_loads,
            )
            await asyncio.sleep(0.01)
            type(self).active_loads -= 1

        async def close(self):
            self.closed = True

    def factory(config):
        client = SlowClient(config)
        created.append(client)
        return client

    monkeypatch.setattr(module.ccxt, factory_attr, factory)
    connector = connector_cls(_config(name))

    async def run_connects():
        return await asyncio.gather(*(connector.connect() for _ in range(3)))

    results = asyncio.run(run_connects())

    assert results == [True, True, True]
    assert SlowClient.max_active_loads == 1
    assert len(created) == 3
    assert all(client.closed for client in created[:-1])
    assert created[-1].closed is False
    assert connector._client is created[-1]
    assert connector.is_connected is True


@pytest.mark.parametrize("connector_cls,module_path,factory_attr,name", CONNECTOR_CASES)
def test_connect_failure_keeps_existing_client(
    connector_cls,
    module_path,
    factory_attr,
    name,
    monkeypatch,
):
    module = importlib.import_module(module_path)

    class ExistingClient:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    class BrokenClient:
        def __init__(self, config):
            self.config = config
            self.closed = False

        async def load_markets(self):
            raise RuntimeError("load_markets failed")

        async def close(self):
            self.closed = True

    broken = BrokenClient({})
    monkeypatch.setattr(module.ccxt, factory_attr, lambda _: broken)
    connector = connector_cls(_config(name))
    existing = ExistingClient()
    connector._client = existing
    connector._connected = True

    with pytest.raises(RuntimeError, match="load_markets failed"):
        asyncio.run(connector.connect())

    assert connector._client is existing
    assert connector.is_connected is True
    assert existing.closed is False
    assert broken.closed is True


@pytest.mark.parametrize("connector_cls,_,__,name", CONNECTOR_CASES)
def test_create_order_uses_ccxt_precision_helpers(connector_cls, _, __, name):
    class PrecisionClient:
        def __init__(self):
            self.amount_args = None
            self.price_args = None
            self.create_order_kwargs = None

        def amount_to_precision(self, symbol, amount):
            self.amount_args = (symbol, amount)
            return "1.2345"

        def price_to_precision(self, symbol, price):
            self.price_args = (symbol, price)
            return "27123.4"

        async def create_order(self, **kwargs):
            self.create_order_kwargs = kwargs
            return _order_payload(
                amount=kwargs["amount"],
                price=kwargs["price"],
                remaining=kwargs["amount"],
            )

    connector = connector_cls(_config(name))
    client = PrecisionClient()
    connector._client = client
    connector._connected = True

    order = asyncio.run(
        connector.create_order(
            "BTC/USDT",
            OrderSide.BUY,
            OrderType.LIMIT,
            amount=1.23456789,
            price=27123.45678,
            params={"reduceOnly": True},
        )
    )

    assert client.amount_args == ("BTC/USDT", 1.23456789)
    assert client.price_args == ("BTC/USDT", 27123.45678)
    assert client.create_order_kwargs == {
        "symbol": "BTC/USDT",
        "type": "limit",
        "side": "buy",
        "amount": "1.2345",
        "price": "27123.4",
        "params": {"reduceOnly": True},
    }
    assert order.amount == 1.2345
    assert order.price == 27123.4


@pytest.mark.parametrize("connector_cls,_,__,name", CONNECTOR_CASES)
def test_query_and_account_methods_reconnect_through_ensure_client(connector_cls, _, __, name, monkeypatch):
    class QueryClient:
        def __init__(self):
            self.calls = []

        async def fetch_order_book(self, symbol, limit):
            self.calls.append(("fetch_order_book", symbol, limit))
            return {"bids": [[1, 2]], "asks": [[3, 4]]}

        async def fetch_balance(self):
            self.calls.append(("fetch_balance",))
            return {"USDT": {"free": 1, "used": 2, "total": 3}}

        async def cancel_order(self, order_id, symbol):
            self.calls.append(("cancel_order", order_id, symbol))

        async def fetch_order(self, order_id, symbol):
            self.calls.append(("fetch_order", order_id, symbol))
            return _order_payload(id=order_id, symbol=symbol)

        async def fetch_open_orders(self, symbol):
            self.calls.append(("fetch_open_orders", symbol))
            return [_order_payload(symbol=symbol)]

        async def fetch_positions(self):
            self.calls.append(("fetch_positions",))
            return [
                {
                    "symbol": "BTC/USDT",
                    "side": "long",
                    "contracts": 0.5,
                    "entryPrice": 27000,
                    "markPrice": 27100,
                    "unrealizedPnl": 50,
                    "leverage": 2,
                    "liquidationPrice": 20000,
                }
            ]

        async def fetch_my_trades(self, symbol, since=None, limit=None):
            self.calls.append(("fetch_my_trades", symbol, since, limit))
            return [{"id": "trade-1"}]

    connector = connector_cls(_config(name))
    client = QueryClient()
    reconnects = {"count": 0}

    async def fake_connect():
        reconnects["count"] += 1
        connector._client = client
        connector._connected = True
        return True

    monkeypatch.setattr(connector, "connect", fake_connect)

    async def invoke_with_disconnected_client(method):
        connector._client = None
        connector._connected = False
        return await method()

    async def run_methods():
        results = []
        results.append(await invoke_with_disconnected_client(lambda: connector.get_order_book("BTC/USDT")))
        results.append(await invoke_with_disconnected_client(lambda: connector.get_balance()))
        results.append(await invoke_with_disconnected_client(lambda: connector.cancel_order("order-1", "BTC/USDT")))
        results.append(await invoke_with_disconnected_client(lambda: connector.get_order("order-1", "BTC/USDT")))
        results.append(await invoke_with_disconnected_client(lambda: connector.get_open_orders("BTC/USDT")))
        results.append(await invoke_with_disconnected_client(lambda: connector.get_positions()))
        results.append(await invoke_with_disconnected_client(lambda: connector.get_trades("BTC/USDT")))
        return results

    results = asyncio.run(run_methods())

    assert reconnects["count"] == 7
    assert results[0]["bids"] == [[1, 2]]
    assert results[1][0].currency == "USDT"
    assert results[2] is True
    assert results[3].id == "order-1"
    assert results[4][0].symbol == "BTC/USDT"
    assert results[5][0].amount == 0.5
    assert results[6] == [{"id": "trade-1"}]
    assert client.calls == [
        ("fetch_order_book", "BTC/USDT", 20),
        ("fetch_balance",),
        ("cancel_order", "order-1", "BTC/USDT"),
        ("fetch_order", "order-1", "BTC/USDT"),
        ("fetch_open_orders", "BTC/USDT"),
        ("fetch_positions",),
        ("fetch_my_trades", "BTC/USDT", None, 100),
    ]
