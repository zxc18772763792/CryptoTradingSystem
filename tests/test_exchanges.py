"""Exchange connector tests."""

from __future__ import annotations

import asyncio
import importlib
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from config.exchanges import ExchangeConfig, ExchangeType
from core.exchanges.base_exchange import (
    BaseExchange,
    Balance,
    ExchangeThrottled,
    Kline,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Ticker,
)
from core.exchanges.binance_connector import BinanceConnector

exchange_manager_module = importlib.import_module("core.exchanges.exchange_manager")


class DummyExchange(BaseExchange):
    async def connect(self):
        return True

    async def disconnect(self):
        return None

    async def get_ticker(self, symbol):
        raise NotImplementedError

    async def get_klines(self, symbol, timeframe, since=None, limit=None):
        raise NotImplementedError

    async def get_order_book(self, symbol, limit=20):
        raise NotImplementedError

    async def get_balance(self):
        raise NotImplementedError

    async def create_order(self, symbol, side, order_type, amount, price=None, params=None):
        raise NotImplementedError

    async def cancel_order(self, order_id, symbol):
        raise NotImplementedError

    async def get_order(self, order_id, symbol):
        raise NotImplementedError

    async def get_open_orders(self, symbol=None):
        raise NotImplementedError

    async def get_positions(self):
        raise NotImplementedError

    async def get_trades(self, symbol, since=None, limit=None):
        raise NotImplementedError


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

    def test_get_ticker_auto_reconnects_when_client_missing(self, connector, monkeypatch):
        class FakeClient:
            async def fetch_ticker(self, symbol):
                assert symbol == "BTC/USDT"
                return {
                    "last": 50000.0,
                    "bid": 49990.0,
                    "ask": 50010.0,
                    "high": 51000.0,
                    "low": 49000.0,
                    "baseVolume": 1234.5,
                    "timestamp": 1609459200000,
                }

        calls = {"count": 0}

        async def fake_connect():
            calls["count"] += 1
            connector._client = FakeClient()
            connector._connected = True
            return True

        connector._client = None
        connector._connected = False
        monkeypatch.setattr(connector, "connect", fake_connect)

        ticker = asyncio.run(connector.get_ticker("BTC/USDT"))

        assert calls["count"] == 1
        assert ticker.last == 50000.0
        assert ticker.exchange == "binance"

    def test_get_ticker_fast_fails_during_throttle_cooldown(self, connector):
        class FakeClient:
            def __init__(self):
                self.fetch_calls = 0

            async def fetch_ticker(self, symbol):
                self.fetch_calls += 1
                return {}

        fake_client = FakeClient()
        connector._client = fake_client
        connector._connected = True
        connector._note_throttle()

        with pytest.raises(ExchangeThrottled, match="get_ticker skipped"):
            asyncio.run(connector.get_ticker("BTC/USDT"))

        assert fake_client.fetch_calls == 0

    def test_get_ticker_success_clears_throttle_cooldown(self, connector):
        class FakeClient:
            markets = {}

            async def fetch_ticker(self, symbol):
                assert symbol == "BTC/USDT"
                return {
                    "last": 50000.0,
                    "bid": 49990.0,
                    "ask": 50010.0,
                    "high": 51000.0,
                    "low": 49000.0,
                    "baseVolume": 1234.5,
                    "timestamp": 1609459200000,
                }

        connector._client = FakeClient()
        connector._connected = True
        connector._note_throttle()
        connector._throttle_until = 0.0

        ticker = asyncio.run(connector.get_ticker("BTC/USDT"))

        assert ticker.last == 50000.0
        assert connector._throttle_failures == 0
        assert connector._throttle_remaining() == 0.0

    def test_transient_error_marks_exchange_disconnected(self):
        exchange = DummyExchange(ExchangeConfig(name="dummy", exchange_type=ExchangeType.CEX))
        exchange._connected = True

        with pytest.raises(TimeoutError):
            exchange._handle_error(TimeoutError("request timed out"), "fetch_ticker")

        assert exchange.is_connected is False

    def test_throttle_error_keeps_exchange_connected_and_starts_cooldown(self):
        class RateLimitExceeded(Exception):
            pass

        exchange = DummyExchange(ExchangeConfig(name="dummy", exchange_type=ExchangeType.CEX))
        exchange._connected = True

        with pytest.raises(RateLimitExceeded):
            exchange._handle_error(
                RateLimitExceeded("Too many requests; request weight limit -1003"),
                "fetch_ticker",
            )

        assert exchange.is_connected is True
        assert exchange._throttle_failures == 1
        assert exchange._throttle_remaining() > 0

        with pytest.raises(ExchangeThrottled, match="throttle cooldown"):
            exchange._raise_if_throttled("get_ticker")

    def test_throttle_fast_fail_does_not_extend_cooldown(self):
        exchange = DummyExchange(ExchangeConfig(name="dummy", exchange_type=ExchangeType.CEX))
        exchange._note_throttle()
        until = exchange._throttle_until
        failures = exchange._throttle_failures

        with pytest.raises(ExchangeThrottled):
            exchange._handle_error(ExchangeThrottled("cooling down"), "get_ticker")

        assert exchange._throttle_until == until
        assert exchange._throttle_failures == failures

    def test_get_klines_uses_existing_futures_market_before_1000_alias(self):
        config = ExchangeConfig(
            name="binance",
            exchange_type=ExchangeType.CEX,
            default_type="future",
            sandbox=True,
        )
        connector = BinanceConnector(config)

        class FakeClient:
            markets = {
                "TAO/USDT:USDT": {"symbol": "TAO/USDT:USDT"},
                "1000PEPE/USDT:USDT": {"symbol": "1000PEPE/USDT:USDT"},
            }

            def __init__(self):
                self.last_symbol = ""

            async def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None):
                self.last_symbol = symbol
                return [[1609459200000, 10.0, 11.0, 9.0, 10.5, 123.0]]

        fake_client = FakeClient()
        connector._client = fake_client
        connector._connected = True

        klines = asyncio.run(connector.get_klines("TAO/USDT", "1h"))

        assert fake_client.last_symbol == "TAO/USDT:USDT"
        assert klines[0].symbol == "TAO/USDT"
        assert klines[0].close == 10.5

    def test_get_klines_keeps_valid_1000_futures_alias_and_normalizes_price(self):
        config = ExchangeConfig(
            name="binance",
            exchange_type=ExchangeType.CEX,
            default_type="future",
            sandbox=True,
        )
        connector = BinanceConnector(config)

        class FakeClient:
            markets = {
                "PEPE/USDT:USDT": {"symbol": "PEPE/USDT:USDT"},
                "1000PEPE/USDT:USDT": {"symbol": "1000PEPE/USDT:USDT"},
            }

            def __init__(self):
                self.last_symbol = ""

            async def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None):
                self.last_symbol = symbol
                return [[1609459200000, 1000.0, 1200.0, 900.0, 1100.0, 123.0]]

        fake_client = FakeClient()
        connector._client = fake_client
        connector._connected = True

        klines = asyncio.run(connector.get_klines("PEPE/USDT", "1h"))

        assert fake_client.last_symbol == "1000PEPE/USDT:USDT"
        assert klines[0].symbol == "PEPE/USDT"
        assert klines[0].close == 1.1

    def test_connect_failure_keeps_existing_client(self, connector, monkeypatch):
        class ExistingClient:
            def __init__(self):
                self.closed = False

            async def close(self):
                self.closed = True

        class BrokenClient:
            def __init__(self):
                self.options = {}
                self.closed = False

            async def load_time_difference(self):
                return 0

            async def fetch_time(self):
                return 0

            async def load_markets(self):
                raise RuntimeError("load_markets failed")

            async def close(self):
                self.closed = True

        existing = ExistingClient()
        broken = BrokenClient()

        connector._client = existing
        connector._connected = True
        monkeypatch.setattr("core.exchanges.binance_connector.ccxt.binance", lambda _: broken)

        with pytest.raises(RuntimeError, match="load_markets failed"):
            asyncio.run(connector.connect())

        assert connector._client is existing
        assert connector._connected is True
        assert existing.closed is False
        assert broken.closed is True

    def test_connect_cancellation_closes_candidate_client(self, connector, monkeypatch):
        class CandidateClient:
            def __init__(self):
                self.options = {}
                self.close_started = False
                self.closed = False

            async def close(self):
                self.close_started = True
                await asyncio.sleep(0)
                self.closed = True

        candidate = CandidateClient()

        async def cancel_prepare(client):
            assert client is candidate
            raise asyncio.CancelledError()

        monkeypatch.setattr("core.exchanges.binance_connector.ccxt.binance", lambda _: candidate)
        monkeypatch.setattr(connector, "_prepare_client", cancel_prepare)

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(connector.connect())

        assert candidate.close_started is True
        assert candidate.closed is True
        assert connector._client is None
        assert connector._connected is False

    def test_connect_cancellation_closes_existing_disconnected_client(self, connector, monkeypatch):
        class Client:
            def __init__(self):
                self.options = {}
                self.closed = False

            async def close(self):
                self.closed = True

        existing = Client()
        candidate = Client()
        connector._client = existing
        connector._connected = False

        async def cancel_prepare(client):
            assert client is candidate
            raise asyncio.CancelledError()

        monkeypatch.setattr("core.exchanges.binance_connector.ccxt.binance", lambda _: candidate)
        monkeypatch.setattr(connector, "_prepare_client", cancel_prepare)

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(connector.connect())

        assert candidate.closed is True
        assert existing.closed is True
        assert connector._client is None
        assert connector._connected is False

    def test_get_positions_default_type_fallback_does_not_pollute_balance_fetch(self):
        config = ExchangeConfig(
            name="binance",
            exchange_type=ExchangeType.CEX,
            api_key="test_key",
            api_secret="test_secret",
            default_type="spot",
            sandbox=True,
        )
        connector = BinanceConnector(config)

        class SharedOptionsClient:
            def __init__(self):
                self.options = {"defaultType": "spot"}
                self.balance_default_types = []

            async def fetch_positions(self):
                assert self.options.get("defaultType") in {"future", "swap"}
                await asyncio.sleep(0.02)
                return [
                    {
                        "symbol": "BTC/USDT",
                        "side": "long",
                        "contracts": 1,
                        "entryPrice": 100,
                        "markPrice": 101,
                        "unrealizedPnl": 1,
                        "leverage": 1,
                        "liquidationPrice": 50,
                    }
                ]

            async def fetch_balance(self, params=None):
                params = params or {}
                self.balance_default_types.append(
                    (dict(params), self.options.get("defaultType"))
                )
                await asyncio.sleep(0.005)
                balance_type = params.get("type") or "spot"
                if balance_type == "spot":
                    return {"USDT": {"free": 10, "used": 0, "total": 10}}
                return {}

        client = SharedOptionsClient()
        connector._client = client
        connector._connected = True

        async def run_concurrently():
            positions_task = asyncio.create_task(connector.get_positions())
            await asyncio.sleep(0.001)
            balances_task = asyncio.create_task(connector.get_balance())
            return await asyncio.gather(positions_task, balances_task)

        positions, balances = asyncio.run(run_concurrently())

        assert positions[0].amount == 1
        assert balances[0].currency == "USDT"
        assert client.options["defaultType"] == "spot"
        assert all(default_type == "spot" for _, default_type in client.balance_default_types)


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

    def test_initialize_runs_connector_creation_in_parallel(self, monkeypatch):
        async def _run():
            manager = exchange_manager_module.ExchangeManager()
            started = set()
            both_started = asyncio.Event()

            async def fake_create_connector(name, config, *, timeout_sec=None):
                started.add(name)
                if len(started) == 2:
                    both_started.set()
                await asyncio.wait_for(both_started.wait(), timeout=0.2)
                return SimpleNamespace(is_connected=True, name=name, config=config)

            monkeypatch.setattr(manager, "_create_connector", fake_create_connector)

            ok = await manager.initialize(["gate", "binance"])

            assert ok is True
            assert started == {"gate", "binance"}
            assert set(manager.get_all_exchanges().keys()) == {"gate", "binance"}

        asyncio.run(_run())

    def test_create_connector_timeout_cleans_up_partial_client(self, monkeypatch):
        async def _run():
            manager = exchange_manager_module.ExchangeManager()
            cleanup = {"disconnect_calls": 0}

            class SlowConnector:
                def __init__(self, config):
                    self.config = config
                    self.name = config.name
                    self._connected = False
                    self._client = object()

                @property
                def is_connected(self):
                    return self._connected

                async def connect(self):
                    await asyncio.sleep(0.2)
                    self._connected = True
                    return True

                async def disconnect(self):
                    cleanup["disconnect_calls"] += 1
                    self._connected = False
                    self._client = None

            monkeypatch.setattr(exchange_manager_module, "GateConnector", SlowConnector)

            connector = await manager._create_connector(
                "gate",
                ExchangeConfig(name="gate", exchange_type=ExchangeType.CEX),
                timeout_sec=0.05,
            )

            assert connector is None
            assert cleanup["disconnect_calls"] == 1

        asyncio.run(_run())

    def test_ensure_exchange_reconnects_disconnected_connector(self, monkeypatch):
        async def _run():
            manager = exchange_manager_module.ExchangeManager()
            connector = SimpleNamespace(
                is_connected=False,
                connect=AsyncMock(return_value=True),
            )
            manager._exchanges["binance"] = connector

            reconnect = AsyncMock(return_value=True)
            monkeypatch.setattr(manager, "reconnect_exchange", reconnect)

            resolved = await manager.ensure_exchange("binance")

            assert resolved is connector
            reconnect.assert_awaited_once_with("binance", account_id=None, timeout_sec=20.0)

        asyncio.run(_run())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
