from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from core.exchanges.base_exchange import Order, OrderSide, OrderStatus, OrderType
from core.trading.order_manager import OrderManager


def _order(order_id: str, *, status: OrderStatus = OrderStatus.OPEN) -> Order:
    return Order(
        id=order_id,
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        type=OrderType.LIMIT,
        price=50_000.0,
        amount=0.01,
        remaining=0.01,
        status=status,
        timestamp=datetime.now(timezone.utc),
        exchange="binance",
    )


def test_paper_cancel_uses_order_metadata_after_global_mode_changes(monkeypatch):
    manager = OrderManager()
    paper = _order("paper-1")
    manager._orders[paper.id] = paper
    manager._order_meta[paper.id] = {"mode": "paper", "account_id": "paper-account"}
    manager.set_paper_trading(False)
    connector = AsyncMock()
    monkeypatch.setattr(manager, "_resolve_cached_exchange", lambda *args, **kwargs: connector)

    assert asyncio.run(manager.cancel_order(paper.id, paper.symbol)) is True
    assert paper.status == OrderStatus.CANCELED
    connector.cancel_order.assert_not_awaited()


def test_conflicting_explicit_mode_fails_closed(monkeypatch):
    manager = OrderManager()
    paper = _order("paper-2")
    manager._orders[paper.id] = paper
    manager._order_meta[paper.id] = {"mode": "paper", "account_id": "paper-account"}
    connector = AsyncMock()
    monkeypatch.setattr(manager, "_resolve_cached_exchange", lambda *args, **kwargs: connector)

    assert asyncio.run(
        manager.cancel_order(
            paper.id,
            paper.symbol,
            trading_mode="live",
        )
    ) is False
    assert paper.status == OrderStatus.OPEN
    connector.cancel_order.assert_not_awaited()


def test_paper_open_order_query_isolated_from_live_cached_orders():
    manager = OrderManager()
    paper = _order("paper-3")
    live = _order("live-1")
    manager._orders.update({paper.id: paper, live.id: live})
    manager._order_meta.update(
        {
            paper.id: {"mode": "paper"},
            live.id: {"mode": "live"},
        }
    )
    manager.set_paper_trading(False)

    rows = asyncio.run(manager.get_open_orders(trading_mode="paper"))

    assert [row.id for row in rows] == [paper.id]

