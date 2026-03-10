"""Exchange connector tests."""

from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from config.exchanges import ExchangeConfig, ExchangeType
from core.exchanges.base_exchange import (
    Balance,
    Kline,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Ticker,
)
from core.exchanges.binance_connector import BinanceConnector


class TestBaseExchange:
    def test_order_side_enum(self):
        assert OrderSide.BUY.value == "buy"
        assert OrderSide.SELL.value == "sell"

    def test_order_type_enum(self):
        assert OrderType.MARKET.value == "market"
        assert OrderType.LIMIT.value == "limit"

    def test_ticker_dataclass(self):
        ticker = Ticker(
            symbol="BTC/USDT",
            last=50000.0,
            bid=49999.0,
            ask=50001.0,
            high_24h=51000.0,
            low_24h=49000.0,
            volume_24h=1000000.0,
            timestamp=datetime.now(),
            exchange="binance",
        )
        assert ticker.symbol == "BTC/USDT"
        assert ticker.last == 50000.0

    def test_kline_dataclass(self):
        kline = Kline(
            symbol="BTC/USDT",
            timeframe="1h",
            timestamp=datetime.now(),
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=1000.0,
            exchange="binance",
        )
        assert kline.symbol == "BTC/USDT"
        assert kline.timeframe == "1h"
        assert kline.high > kline.low

    def test_order_dataclass(self):
        order = Order(
            id="12345",
            symbol="BTC/USDT",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=50000.0,
            amount=0.1,
            status=OrderStatus.OPEN,
            exchange="binance",
        )
        assert order.id == "12345"
        assert order.side == OrderSide.BUY
        assert order.status == OrderStatus.OPEN

    def test_balance_dataclass(self):
        balance = Balance(
            currency="USDT",
            free=10000.0,
            used=5000.0,
            total=15000.0,
        )
        assert balance.currency == "USDT"
        assert balance.total == balance.free + balance.used


class TestBinanceConnector:
    @pytest.fixture
    def config(self):
        return ExchangeConfig(
            name="binance",
            exchange_type=ExchangeType.CEX,
            api_key="test_key",
            api_secret="test_secret",
            sandbox=True,
        )

    @pytest.fixture
    def connector(self, config):
        return BinanceConnector(config)

    def test_connector_initialization(self, connector, config):
        assert connector.name == "binance"
        assert connector.config == config
        assert not connector.is_connected

    def test_parse_order(self, connector):
        ccxt_order = {
            "id": "12345",
            "symbol": "BTC/USDT",
            "side": "buy",
            "type": "limit",
            "price": 50000.0,
            "amount": 0.1,
            "filled": 0.05,
            "remaining": 0.05,
            "cost": 2500.0,
            "status": "open",
            "timestamp": 1609459200000,
        }

        order = connector._parse_order(ccxt_order)

        assert order.id == "12345"
        assert order.symbol == "BTC/USDT"
        assert order.side == OrderSide.BUY
        assert order.type == OrderType.LIMIT
        assert order.status == OrderStatus.OPEN


class TestExchangeManager:
    def test_exchange_manager_initialization(self):
        async def _run():
            from core.exchanges import ExchangeManager

            manager = ExchangeManager()
            assert not manager.is_connected
            assert len(manager.get_all_exchanges()) == 0

        asyncio.run(_run())

    def test_get_nonexistent_exchange(self):
        async def _run():
            from core.exchanges import ExchangeManager

            manager = ExchangeManager()
            exchange = manager.get_exchange("nonexistent")
            assert exchange is None

        asyncio.run(_run())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
