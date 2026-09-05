"""Shared helpers for normalizing exchange order payloads."""

from __future__ import annotations

import math
from typing import Any, Mapping


def _positive_float(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) and number > 0 else 0.0


def resolve_ccxt_order_fill_price(order: Mapping[str, Any]) -> float:
    """Resolve the best available executed price from a CCXT order payload.

    Market-order responses commonly leave ``price`` empty while populating
    ``average``. Prefer execution-derived fields, then weighted fills, and use
    the submitted/limit price only as the last fallback.
    """
    average = _positive_float(order.get("average"))
    if average:
        return average

    cost = _positive_float(order.get("cost"))
    filled = _positive_float(order.get("filled"))
    if cost and filled:
        return cost / filled

    fills = order.get("trades") or order.get("fills") or []
    weighted_notional = 0.0
    weighted_quantity = 0.0
    if isinstance(fills, list):
        for fill in fills:
            if not isinstance(fill, Mapping):
                continue
            price = _positive_float(fill.get("price") or fill.get("average"))
            quantity = _positive_float(
                fill.get("amount") or fill.get("filled") or fill.get("qty") or fill.get("quantity")
            )
            if price and quantity:
                weighted_notional += price * quantity
                weighted_quantity += quantity
    if weighted_quantity:
        return weighted_notional / weighted_quantity

    return _positive_float(order.get("price"))
