from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from core.marketdata.hub import market_data_hub
from core.ai.autonomous_agent import AutonomousTradingAgent
from core.backtest.paper_trading import PaperTradingEngine
from core.ops.service import api as ops_api
from web.api import data as data_api
from web.api import notifications as notifications_api
from web.api import strategies as strategies_api
from web.api import trading as trading_api


class _RestMustNotBeCalled:
    def __init__(self) -> None:
        self.calls = 0

    async def get_ticker(self, symbol: str):
        self.calls += 1
        raise AssertionError(f"REST ticker fallback should not be called for {symbol}")


class _RestTickerConnector:
    def __init__(self, *, last=66000.0, low_24h=64000.0, volume_24h=123.0) -> None:
        self.calls = 0
        self.ticker = SimpleNamespace(
            last=last,
            bid=last - 1.0,
            ask=last + 1.0,
            high_24h=last + 100.0,
            low_24h=low_24h,
            volume_24h=volume_24h,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    async def get_ticker(self, symbol: str):
        self.calls += 1
        return self.ticker


def test_notifications_price_loader_reads_fresh_market_ws_hub_without_rest(monkeypatch):
    market_data_hub.clear()
    connector = _RestMustNotBeCalled()
    monkeypatch.setattr(
        notifications_api.exchange_manager,
        "get_exchange",
        lambda exchange: connector,
    )

    try:
        market_data_hub.upsert_ws_tick(
            "binance",
            "BTC/USDT:USDT",
            {"last": 65000.0, "bid": 64999.0, "ask": 65001.0},
        )

        prices = asyncio.run(notifications_api._load_prices("binance", ["BTC/USDT"]))

        assert prices == {"BTC/USDT": 65000.0}
        assert connector.calls == 0
    finally:
        market_data_hub.clear()


def test_trading_rule_prices_read_fresh_market_ws_hub_without_rest(monkeypatch):
    market_data_hub.clear()
    trading_api._RULE_PRICE_CACHE["ts"] = 0.0
    trading_api._RULE_PRICE_CACHE["prices"] = {}
    trading_api._RULE_PRICE_IN_FLIGHT = None
    connector = _RestMustNotBeCalled()

    def fake_get_exchange(exchange: str):
        return connector if exchange == "gate" else None

    monkeypatch.setattr(trading_api.exchange_manager, "get_exchange", fake_get_exchange)

    try:
        market_data_hub.upsert_ws_tick("gate", "BTC/USDT", {"last": 65000.0})
        market_data_hub.upsert_ws_tick("gate", "ETH/USDT", {"last": 3500.0})
        market_data_hub.upsert_ws_tick("gate", "SOL/USDT", {"last": 180.0})

        prices = asyncio.run(trading_api._load_rule_prices())

        assert prices == {
            "BTC/USDT": 65000.0,
            "ETH/USDT": 3500.0,
            "SOL/USDT": 180.0,
        }
        assert connector.calls == 0
    finally:
        market_data_hub.clear()
        trading_api._RULE_PRICE_CACHE["ts"] = 0.0
        trading_api._RULE_PRICE_CACHE["prices"] = {}
        trading_api._RULE_PRICE_IN_FLIGHT = None


def test_strategy_sizing_preview_reads_fresh_market_ws_hub_without_rest(monkeypatch):
    market_data_hub.clear()
    connector = _RestMustNotBeCalled()

    async def fake_load_klines_from_parquet(*args, **kwargs):
        return None

    async def fake_get_exchange_amount_rules(exchange: str, symbol: str):
        return 0.001, 6

    monkeypatch.setattr(
        strategies_api.strategy_manager,
        "get_strategy_info",
        lambda name: {
            "name": name,
            "exchange": "binance",
            "symbols": ["BTC/USDT"],
            "timeframe": "1h",
            "params": {},
            "allocation": 0.1,
        },
    )
    monkeypatch.setattr(
        strategies_api.risk_manager,
        "get_risk_report",
        lambda: {"equity": {"current": 1000.0}},
    )
    monkeypatch.setattr(strategies_api.risk_manager, "max_position_size", 0.1, raising=False)
    monkeypatch.setattr(strategies_api.execution_engine, "_cached_equity", 0.0)
    monkeypatch.setattr(
        strategies_api.execution_engine,
        "_get_exchange_amount_rules",
        fake_get_exchange_amount_rules,
    )
    monkeypatch.setattr(
        strategies_api.data_storage,
        "load_klines_from_parquet",
        fake_load_klines_from_parquet,
    )
    monkeypatch.setattr(
        strategies_api.exchange_manager,
        "get_exchange",
        lambda exchange: connector,
    )

    try:
        market_data_hub.upsert_ws_tick(
            "binance",
            "BTC/USDT",
            {"last": 50000.0, "bid": 49999.0, "ask": 50001.0},
        )

        payload = asyncio.run(strategies_api._build_strategy_sizing_preview("unit"))

        assert payload["price"] == 50000.0
        assert payload["price_source"] == "hub:ws"
        assert payload["price_meta"]["source"] == "ws"
        assert payload["price_meta"]["is_stale"] is False
        assert connector.calls == 0
    finally:
        market_data_hub.clear()


def test_data_ticker_route_reads_fresh_market_ws_hub_without_rest(monkeypatch):
    market_data_hub.clear()
    connector = _RestMustNotBeCalled()
    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda exchange: connector)

    try:
        market_data_hub.upsert_ws_tick(
            "binance",
            "BTC/USDT:USDT",
            {"last": 65123.0, "bid": 65122.0, "ask": 65124.0},
        )

        payload = asyncio.run(data_api.get_ticker("binance", "BTC/USDT"))

        assert payload["last"] == 65123.0
        assert payload["bid"] == 65122.0
        assert payload["ask"] == 65124.0
        assert payload["source"] == "ws"
        assert payload["price_meta"]["source"] == "ws"
        assert connector.calls == 0
    finally:
        market_data_hub.clear()


def test_data_ticker_route_uses_full_rest_payload_when_hub_missing(monkeypatch):
    market_data_hub.clear()
    connector = _RestTickerConnector(last=66000.0, low_24h=64000.0, volume_24h=321.0)
    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda exchange: connector)

    payload = asyncio.run(data_api.get_ticker("binance", "BTC/USDT"))

    assert payload["last"] == 66000.0
    assert payload["low_24h"] == 64000.0
    assert payload["volume_24h"] == 321.0
    assert payload["source"] == "rest"
    assert "price_meta" not in payload
    assert connector.calls == 1
    market_data_hub.clear()


def test_data_tickers_route_reads_fresh_market_ws_hub_without_rest(monkeypatch):
    market_data_hub.clear()
    connector = _RestMustNotBeCalled()
    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda exchange: connector)
    monkeypatch.setattr(
        data_api.exchange_manager,
        "get_supported_symbols",
        lambda exchange: ["BTC/USDT", "ETH/USDT"],
    )

    try:
        market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 65000.0})
        market_data_hub.upsert_ws_tick("binance", "ETH/USDT", {"last": 3500.0})

        payload = asyncio.run(data_api.get_tickers("binance"))

        assert payload["tickers"] == [
            {"symbol": "BTC/USDT", "last": 65000.0, "change_24h": 0, "volume_24h": 0.0, "source": "ws"},
            {"symbol": "ETH/USDT", "last": 3500.0, "change_24h": 0, "volume_24h": 0.0, "source": "ws"},
        ]
        assert connector.calls == 0
    finally:
        market_data_hub.clear()


def test_ops_ticker_price_reads_fresh_market_ws_hub_without_rest(monkeypatch):
    market_data_hub.clear()
    connector = _RestMustNotBeCalled()
    monkeypatch.setattr(ops_api.exchange_manager, "get_exchange", lambda exchange: connector)

    try:
        market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 65010.0})

        price = asyncio.run(ops_api._get_ticker_price("BTC/USDT", "binance"))

        assert price == 65010.0
        assert connector.calls == 0
    finally:
        market_data_hub.clear()


def test_autonomous_agent_last_price_reads_fresh_market_ws_hub_without_rest(monkeypatch, tmp_path):
    market_data_hub.clear()
    connector = _RestMustNotBeCalled()
    agent = AutonomousTradingAgent(cache_root=tmp_path / "agent_ws_price")
    monkeypatch.setattr(
        "core.ai.autonomous_agent.exchange_manager.get_exchange",
        lambda exchange: connector,
    )

    try:
        market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 65020.0})

        price = asyncio.run(agent._resolve_last_price({"exchange": "binance", "symbol": "BTC/USDT"}, None))

        assert price == 65020.0
        assert connector.calls == 0
    finally:
        market_data_hub.clear()


def test_paper_trading_price_refresh_reads_fresh_market_ws_hub_without_rest(monkeypatch):
    market_data_hub.clear()
    connector = _RestMustNotBeCalled()
    engine = PaperTradingEngine()
    updated = {}
    monkeypatch.setattr(
        "core.backtest.paper_trading.exchange_manager.get_connected_exchanges",
        lambda: ["binance"],
    )
    monkeypatch.setattr(
        "core.backtest.paper_trading.exchange_manager.get_exchange",
        lambda exchange: connector,
    )
    monkeypatch.setattr(
        "core.backtest.paper_trading.exchange_manager.get_supported_symbols",
        lambda exchange: ["BTC/USDT"],
    )
    monkeypatch.setattr(
        "core.backtest.paper_trading.position_manager.update_all_prices",
        lambda prices: updated.update(prices),
    )

    try:
        market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 65030.0})

        prices = asyncio.run(engine._update_prices())

        assert prices == {"binance": {"BTC/USDT": 65030.0}}
        assert updated == {"binance": {"BTC/USDT": 65030.0}}
        assert connector.calls == 0
    finally:
        market_data_hub.clear()


def test_binance_futures_precheck_reads_fresh_market_ws_hub_without_rest(monkeypatch):
    market_data_hub.clear()
    connector = _RestMustNotBeCalled()
    connector.config = SimpleNamespace(default_type="future")

    async def fake_get_balance():
        return [SimpleNamespace(currency="USDT", free=1000.0)]

    connector.get_balance = fake_get_balance
    monkeypatch.setattr(trading_api.execution_engine, "is_paper_mode", lambda: False)
    monkeypatch.setattr(
        trading_api.exchange_manager,
        "get_exchange",
        lambda exchange, account_id=None: connector,
    )

    try:
        market_data_hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 65040.0})
        request = trading_api.OrderRequest(
            exchange="binance",
            symbol="BTC/USDT",
            side="buy",
            order_type="market",
            amount=0.001,
            leverage=1.0,
        )

        asyncio.run(trading_api._precheck_binance_futures_order(request))

        assert connector.calls == 0
    finally:
        market_data_hub.clear()
