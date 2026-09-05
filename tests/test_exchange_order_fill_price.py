from __future__ import annotations

import pytest

from core.exchanges.binance_connector import BinanceConnector
from core.exchanges.bybit_connector import BybitConnector
from core.exchanges.gate_connector import GateConnector
from core.exchanges.okx_connector import OKXConnector
from core.exchanges.order_parsing import resolve_ccxt_order_fill_price


def _order_payload(**overrides):
    payload = {
        "id": "order-1",
        "symbol": "BTC/USDT",
        "side": "buy",
        "type": "market",
        "price": None,
        "average": 101.25,
        "amount": 2.0,
        "filled": 2.0,
        "remaining": 0.0,
        "cost": 202.5,
        "status": "closed",
        "timestamp": 1_700_000_000_000,
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    ("connector_class", "name"),
    [
        (BinanceConnector, "binance"),
        (BybitConnector, "bybit"),
        (GateConnector, "gate"),
        (OKXConnector, "okx"),
    ],
)
def test_connectors_use_ccxt_average_for_market_fill(connector_class, name):
    connector = object.__new__(connector_class)
    connector.name = name

    parsed = connector._parse_order(_order_payload())

    assert parsed.price == pytest.approx(101.25)


def test_fill_price_falls_back_to_cost_over_filled_then_weighted_fills():
    assert resolve_ccxt_order_fill_price(
        _order_payload(average=None, cost=201.0, filled=2.0)
    ) == pytest.approx(100.5)
    assert resolve_ccxt_order_fill_price(
        _order_payload(
            average=None,
            cost=None,
            filled=None,
            fills=[{"price": 100.0, "amount": 1.0}, {"price": 102.0, "amount": 3.0}],
        )
    ) == pytest.approx(101.5)
