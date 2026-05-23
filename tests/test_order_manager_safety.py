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


def test_ws_client_skeleton_methods_raise_not_implemented():
    client = WSClient(WSClientConfig(url="wss://example.invalid", name="test_ws"))

    with pytest.raises(NotImplementedError):
        asyncio.run(client.connect())
    with pytest.raises(NotImplementedError):
        asyncio.run(client.subscribe({"channel": "ticker"}))
    with pytest.raises(NotImplementedError):
        asyncio.run(client.run_forever())
