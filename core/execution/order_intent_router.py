"""Order intent router skeleton for staged adapter integration."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from core.exchange_adapters.base import ExchangeAdapter, ExchangeOrderRequest
from core.execution.rate_limit_and_reconnect import (
    RateLimitAndReconnectPolicy,
    RateLimitExceeded,
)
from core.strategies.strategy_base import Signal


@dataclass
class OrderIntent:
    strategy_name: str
    symbol: str
    side: str
    order_type: str
    amount: float
    price: Optional[float] = None
    reduce_only: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class OrderIntentRouter:
    """Bridge `Signal` -> `ExchangeOrderRequest` without touching current execution engine."""

    # Buckets we consult before submitting an order. Exchange protections fire on
    # both per-minute and 10-second windows, so we acquire both — whichever is
    # tighter wins. Buckets are looked up by name; missing buckets return True
    # (no limit), so callers that haven't configured a policy stay functional.
    _DEFAULT_ORDER_BUCKETS = ("order_10s", "order_1m")

    def __init__(
        self,
        adapter: ExchangeAdapter,
        policy: Optional[RateLimitAndReconnectPolicy] = None,
        *,
        order_buckets: Optional[tuple[str, ...]] = None,
        acquire_timeout_ms: int = 2_000,
    ):
        self.adapter = adapter
        self.policy = policy
        self.order_buckets = tuple(order_buckets or self._DEFAULT_ORDER_BUCKETS)
        self.acquire_timeout_ms = int(max(0, acquire_timeout_ms))

    def build_order_intent(self, signal: Signal, context: Optional[Dict[str, Any]] = None) -> OrderIntent:
        context = dict(context or {})
        side = "buy" if signal.signal_type.value in {"buy", "close_short"} else "sell"
        reduce_only = signal.signal_type.value in {"close_long", "close_short"}
        amount = float(context.get("amount", signal.quantity or 0.0))
        return OrderIntent(
            strategy_name=signal.strategy_name,
            symbol=signal.symbol,
            side=side,
            order_type=str(context.get("order_type", "market")),
            amount=amount,
            price=signal.price if context.get("order_type", "market") == "limit" else None,
            reduce_only=reduce_only,
            metadata={"signal": signal.to_dict(), **context},
        )

    async def _acquire_rate_limit(self) -> None:
        if self.policy is None:
            return
        # `reduce_only` orders are risk-reducing; the cooldown gate only blocks NEW
        # exposure, so we let exits through even when the bot is in reduce_only mode.
        # That decision is left to higher layers (we don't see reduce_only here).
        # Acquire each configured order bucket. `acquire_async` raises
        # RateLimitExceeded on timeout — let it bubble so the caller sees the
        # bucket name and retry-after.
        for bucket in self.order_buckets:
            await self.policy.acquire_async(bucket, cost=1.0, timeout_ms=self.acquire_timeout_ms)

    async def submit_intent(self, intent: OrderIntent):
        if not bool(getattr(self.adapter, "supports_execution", False)):
            exchange = str(getattr(self.adapter, "exchange", "unknown") or "unknown")
            raise RuntimeError(
                f"Exchange adapter {exchange} is not enabled for order execution"
            )
        req = ExchangeOrderRequest(
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            amount=intent.amount,
            price=intent.price,
            reduce_only=intent.reduce_only,
            params={"strategy_name": intent.strategy_name, **dict(intent.metadata or {})},
        )
        await self._acquire_rate_limit()
        try:
            result = await self.adapter.create_order(req)
        except RateLimitExceeded:
            # Surface upstream — caller decides whether to backoff/retry.
            raise
        except Exception:
            if self.policy is not None:
                # Failures (network/server-side rejection) tighten our backoff so
                # the next attempt doesn't pile on. Penalize each bucket we used.
                for bucket in self.order_buckets:
                    self.policy.record_failure(bucket)
            raise
        if self.policy is not None:
            for bucket in self.order_buckets:
                self.policy.record_success(bucket)
        return result
