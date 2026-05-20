from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.execution.order_intent_router import OrderIntentRouter
from core.strategies.strategy_base import Signal, SignalType


def _signal(signal_type: SignalType = SignalType.BUY) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        signal_type=signal_type,
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name="router_test",
        strength=0.8,
        quantity=0.5,
    )


def test_order_intent_router_blocks_staged_read_only_adapter_before_submit():
    adapter = SimpleNamespace(
        exchange="ccxt-binance",
        supports_execution=False,
        create_order=AsyncMock(),
    )
    router = OrderIntentRouter(adapter)
    intent = router.build_order_intent(_signal(), {"amount": 0.5})

    with pytest.raises(RuntimeError, match="not enabled for order execution"):
        asyncio.run(router.submit_intent(intent))

    adapter.create_order.assert_not_called()


def test_order_intent_router_submits_when_adapter_explicitly_supports_execution():
    adapter = SimpleNamespace(
        exchange="execution-test",
        supports_execution=True,
        create_order=AsyncMock(return_value={"order_id": "ok"}),
    )
    router = OrderIntentRouter(adapter)
    intent = router.build_order_intent(
        _signal(SignalType.CLOSE_LONG),
        {"amount": 0.25, "order_type": "limit"},
    )

    result = asyncio.run(router.submit_intent(intent))

    assert result == {"order_id": "ok"}
    request = adapter.create_order.await_args.args[0]
    assert request.symbol == "BTC/USDT"
    assert request.side == "sell"
    assert request.order_type == "limit"
    assert request.amount == 0.25
    assert request.price == 100.0
    assert request.reduce_only is True
