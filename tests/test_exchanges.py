"""
交易所连接器测试
"""
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from core.exchanges.base_exchange import (
    BaseExchange,
    Ticker,
    Kline,
    Order,
    Balance,
    OrderSide,
    OrderType,
    OrderStatus,
)
from core.exchanges.binance_connector import BinanceConnector
from config.exchanges import ExchangeConfig, ExchangeType


class TestBaseExchange:
    """BaseExchange测试"""

    def test_order_side_enum(self):
        """测试订单方向枚举"""
        assert OrderSide.BUY.value == "buy"
        assert OrderSide.SELL.value == "sell"

    def test_order_type_enum(self):
        """测试订单类型枚举"""
        assert OrderType.MARKET.value == "market"
        assert OrderType.LIMIT.value == "limit"

    def test_ticker_dataclass(self):
        """测试Ticker数据类"""
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
        """测试Kline数据类"""
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
        """测试Order数据类"""
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
        """测试Balance数据类"""
        balance = Balance(
            currency="USDT",
            free=10000.0,
            used=5000.0,
            total=15000.0,
        )
        assert balance.currency == "USDT"
        assert balance.total == balance.free + balance.used


class TestBinanceConnector:
    """Binance连接器测试"""

    @pytest.fixture
    def config(self):
        """测试配置"""
        return ExchangeConfig(
            name="binance",
            exchange_type=ExchangeType.CEX,
            api_key="test_key",
            api_secret="test_secret",
            sandbox=True,
        )

    @pytest.fixture
    def connector(self, config):
        """创建连接器实例"""
        return BinanceConnector(config)

    def test_connector_initialization(self, connector, config):
        """测试连接器初始化"""
        assert connector.name == "binance"
        assert connector.config == config
        assert not connector.is_connected

    def test_parse_order(self, connector):
        """测试订单解析"""
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


@pytest.mark.asyncio
class TestExchangeManager:
    """交易所管理器测试"""

    async def test_exchange_manager_initialization(self):
        """测试交易所管理器初始化"""
        from core.exchanges import ExchangeManager

        manager = ExchangeManager()
        assert not manager.is_connected
        assert len(manager.get_all_exchanges()) == 0

    async def test_get_nonexistent_exchange(self):
        """测试获取不存在的交易所"""
        from core.exchanges import ExchangeManager

        manager = ExchangeManager()
        exchange = manager.get_exchange("nonexistent")
        assert exchange is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
