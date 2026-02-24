"""Order intent router skeleton for staged adapter integration."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from core.exchange_adapters.base import ExchangeAdapter, ExchangeOrderRequest
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

    def __init__(self, adapter: ExchangeAdapter):
        self.adapter = adapter

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

    async def submit_intent(self, intent: OrderIntent):
        req = ExchangeOrderRequest(
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            amount=intent.amount,
            price=intent.price,
            reduce_only=intent.reduce_only,
            params={"strategy_name": intent.strategy_name, **dict(intent.metadata or {})},
        )
        # TODO: add rate-limit policy + state machine hooks
        return await self.adapter.create_order(req)

