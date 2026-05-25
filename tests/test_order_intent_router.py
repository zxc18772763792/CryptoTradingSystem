from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.execution.order_intent_router import OrderIntentRouter
from core.execution.rate_limit_and_reconnect import (
    RateLimitAndReconnectPolicy,
    RateLimitExceeded,
)
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


def test_order_intent_router_rate_limit_acquires_before_submit():
    """Submit must consume rate-limit tokens *before* hitting the exchange so
    bursty strategies don't blow through the venue's per-window cap."""
    adapter = SimpleNamespace(
        exchange="rate-limited-venue",
        supports_execution=True,
        create_order=AsyncMock(return_value={"order_id": "ok"}),
    )
    policy = RateLimitAndReconnectPolicy()
    policy.configure_bucket("order_10s", capacity=2, refill_per_sec=0.1)
    policy.configure_bucket("order_1m", capacity=10, refill_per_sec=0.5)

    router = OrderIntentRouter(adapter, policy=policy)
    intent = router.build_order_intent(_signal(), {"amount": 0.5})

    result = asyncio.run(router.submit_intent(intent))
    assert result == {"order_id": "ok"}

    stats = policy.stats()
    # Capacity was 2 / 10; one acquire each leaves 1 / 9.
    assert stats["buckets"]["order_10s"]["tokens"] == pytest.approx(1.0, abs=0.01)
    assert stats["buckets"]["order_1m"]["tokens"] == pytest.approx(9.0, abs=0.01)
    assert stats["buckets"]["order_10s"]["success_count"] == 1
    assert stats["buckets"]["order_1m"]["success_count"] == 1


def test_order_intent_router_raises_rate_limit_when_bucket_starved():
    """When the bucket is empty and won't refill within the acquire deadline,
    the router must raise ``RateLimitExceeded`` before calling the adapter —
    silently sleeping past the deadline would mask exchange-side overload."""
    adapter = SimpleNamespace(
        exchange="rate-limited-venue",
        supports_execution=True,
        create_order=AsyncMock(return_value={"order_id": "should-not-call"}),
    )
    policy = RateLimitAndReconnectPolicy()
    # Capacity 0 = always starved; refill 0 = never refills.
    policy.configure_bucket("order_10s", capacity=1, refill_per_sec=0.0, initial_tokens=0.0)
    policy.configure_bucket("order_1m", capacity=10, refill_per_sec=10.0)

    router = OrderIntentRouter(adapter, policy=policy, acquire_timeout_ms=20)
    intent = router.build_order_intent(_signal(), {"amount": 0.5})

    with pytest.raises(RateLimitExceeded):
        asyncio.run(router.submit_intent(intent))

    adapter.create_order.assert_not_called()


def test_order_intent_router_records_failure_on_adapter_exception():
    """Adapter exceptions should tighten the bucket so the next attempt does
    not pile on while the venue is degraded."""

    class _Boom(RuntimeError):
        pass

    adapter = SimpleNamespace(
        exchange="rate-limited-venue",
        supports_execution=True,
        create_order=AsyncMock(side_effect=_Boom("network blip")),
    )
    policy = RateLimitAndReconnectPolicy()
    policy.configure_bucket("order_10s", capacity=5, refill_per_sec=1.0)
    policy.configure_bucket("order_1m", capacity=20, refill_per_sec=1.0)

    router = OrderIntentRouter(adapter, policy=policy)
    intent = router.build_order_intent(_signal(), {"amount": 0.5})

    with pytest.raises(_Boom):
        asyncio.run(router.submit_intent(intent))

    stats = policy.stats()
    # record_failure increments per bucket; success_count stays 0.
    assert stats["buckets"]["order_10s"]["failures"] >= 1
    assert stats["buckets"]["order_1m"]["failures"] >= 1
    assert stats["buckets"]["order_10s"]["success_count"] == 0
    assert stats["buckets"]["order_1m"]["success_count"] == 0


def test_order_intent_router_works_without_policy():
    """Backwards-compatibility: a router constructed without a policy must
    still submit, so existing callers don't need to wire rate-limit config."""
    adapter = SimpleNamespace(
        exchange="legacy-venue",
        supports_execution=True,
        create_order=AsyncMock(return_value={"order_id": "ok"}),
    )
    router = OrderIntentRouter(adapter)  # policy=None
    intent = router.build_order_intent(_signal(), {"amount": 0.5})

    result = asyncio.run(router.submit_intent(intent))
    assert result == {"order_id": "ok"}
    adapter.create_order.assert_awaited_once()
