from __future__ import annotations

import asyncio

import pytest

from core.exchanges.base_exchange import OrderSide, OrderStatus, OrderType
from core.marketdata.runtime_price_provider import PriceReadResult
from core.trading.account_manager import account_manager
from core.trading.order_manager import OrderManager, OrderRequest

order_manager_module = __import__("core.trading.order_manager", fromlist=["get_realtime_price"])


@pytest.fixture
def manager(monkeypatch):
    out = OrderManager()
    out.set_paper_trading(True)
    monkeypatch.setattr(account_manager, "get_account_mode", lambda account_id, default="paper": "paper")
    monkeypatch.setattr(out, "_resolve_cached_exchange", lambda *args, **kwargs: None)
    return out


def _live(price, *, bid=None, ask=None):
    return PriceReadResult(exchange="binance", symbol="FIL/USDT", price=price, bid=bid, ask=ask, source="ws",
                           age_ms=800, is_stale=False, fallback_required=False, reason="hub_fresh")


def _patch_live(monkeypatch, result):
    calls = []

    async def fake(exchange, symbol, **kwargs):
        calls.append((exchange, symbol))
        if result is None:
            return PriceReadResult(exchange=exchange, symbol=symbol, price=None, reason="hub_missing")
        return result

    monkeypatch.setattr(order_manager_module, "get_realtime_price", fake)
    return calls


def _submit(manager, *, side=OrderSide.BUY, order_type=OrderType.MARKET, price=3.30, slippage_bps=2.0):
    return asyncio.run(manager.create_order(OrderRequest(
        symbol="FIL/USDT", side=side, order_type=order_type, amount=100.0, price=price,
        exchange="binance", account_id="paper_acc",
        params={"trace_id": "t", "governance_prechecked": True,
                "paper_fee_rate": 0.001, "paper_slippage_bps": slippage_bps},
    )))


def test_market_order_fills_at_the_live_price_not_the_stale_request_price(manager, monkeypatch):
    calls = _patch_live(monkeypatch, _live(3.15, bid=3.149, ask=3.151))
    order = _submit(manager, price=3.30)  # caller carried a bar close 4.8% above the market

    assert order.status == OrderStatus.CLOSED
    assert order.price == pytest.approx(3.15 * 1.0002)
    meta = manager.get_order_metadata(order.id)
    assert meta["paper_fill_source"] == "live"
    assert meta["paper_reference_price"] == pytest.approx(3.15)
    assert meta["paper_requested_price"] == pytest.approx(3.30)
    assert meta["paper_request_vs_live_bps"] == pytest.approx((3.30 / 3.15 - 1) * 1e4, abs=0.01)
    assert (meta["paper_live_bid"], meta["paper_live_ask"]) == (3.149, 3.151)
    assert calls == [("binance", "FIL/USDT")]


def test_market_sell_takes_the_live_price_minus_slippage(manager, monkeypatch):
    _patch_live(monkeypatch, _live(3.15))
    order = _submit(manager, side=OrderSide.SELL, price=3.00)

    assert order.price == pytest.approx(3.15 * 0.9998)


def test_market_order_without_a_live_price_falls_back_to_the_request_price(manager, monkeypatch):
    _patch_live(monkeypatch, None)
    order = _submit(manager, side=OrderSide.SELL, price=3.30)

    assert order.price == pytest.approx(3.30 * 0.9998)
    meta = manager.get_order_metadata(order.id)
    assert meta["paper_fill_source"] == "request_fallback"
    assert meta["paper_request_vs_live_bps"] is None


def test_implausible_live_price_keeps_the_request_price(manager, monkeypatch):
    _patch_live(monkeypatch, _live(3300.0))  # e.g. a 1000x contract or the wrong symbol
    order = _submit(manager, price=3.30, slippage_bps=0)

    assert order.price == pytest.approx(3.30)
    assert manager.get_order_metadata(order.id)["paper_fill_source"] == "request_live_implausible"


def test_market_order_without_a_request_price_uses_the_live_price(manager, monkeypatch):
    _patch_live(monkeypatch, _live(3.15))
    order = _submit(manager, price=None, slippage_bps=0)

    assert order.price == pytest.approx(3.15)
    assert manager.get_order_metadata(order.id)["paper_fill_source"] == "live"


def test_limit_order_keeps_its_limit_price_without_reading_the_market(manager, monkeypatch):
    calls = _patch_live(monkeypatch, _live(3.15))
    order = _submit(manager, order_type=OrderType.LIMIT, price=3.10, slippage_bps=0)

    assert order.price == pytest.approx(3.10)
    assert calls == []
    assert manager.get_order_metadata(order.id)["paper_fill_source"] == "request"
