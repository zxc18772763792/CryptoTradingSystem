from __future__ import annotations

import asyncio
import importlib
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from core.marketdata.hub import MarketDataHub, market_data_hub
from core.marketdata.runtime_price_provider import (
    PriceUnavailableError,
    get_realtime_price,
    require_realtime_price,
)
from core.trading.execution_engine import ExecutionEngine


@dataclass
class _Ticker:
    symbol: str
    last: float
    bid: float = 0.0
    ask: float = 0.0
    exchange: str = "binance"
    timestamp: object = None


class _Connector:
    def __init__(self, ticker):
        self.ticker = ticker
        self.calls = 0

    async def get_ticker(self, symbol):
        self.calls += 1
        return self.ticker


def test_realtime_price_provider_reads_fresh_hub_tick_without_rest_call():
    hub = MarketDataHub(symbol_max_age_sec=10)
    hub.upsert_ws_tick("binance", "BTC/USDT:USDT", {"last": 50000.0, "bid": 49999.0, "ask": 50001.0})
    connector = _Connector(_Ticker(symbol="BTC/USDT", last=51000.0))

    result = asyncio.run(
        get_realtime_price(
            "BINANCE",
            "BTCUSDT",
            hub=hub,
            connector=connector,
            max_age_sec=10,
        )
    )

    assert result.ok is True
    assert result.price == 50000.0
    assert result.source == "ws"
    assert result.reason == "hub_fresh"
    assert result.age_ms is not None
    assert connector.calls == 0


def test_realtime_price_provider_falls_back_to_rest_and_records_reason():
    hub = MarketDataHub(symbol_max_age_sec=0.5)
    connector = _Connector(_Ticker(symbol="BTC/USDT", last=51000.0, bid=50999.0, ask=51001.0))

    result = asyncio.run(
        get_realtime_price(
            "binance",
            "BTC/USDT",
            hub=hub,
            connector=connector,
            max_age_sec=0.5,
        )
    )

    snapshot = hub.snapshot(include_symbols=True)
    assert result.ok is True
    assert result.price == 51000.0
    assert result.source == "rest_fallback"
    assert result.reason == "rest_fallback:hub_missing"
    assert snapshot["rest_fallback_count"] == 1
    assert snapshot["fallback_reasons"] == {"hub_missing": 1}
    assert snapshot["symbols"]["binance"]["BTC/USDT"]["source"] == "rest_fallback"


def test_realtime_price_provider_fail_closed_when_no_fresh_price():
    hub = MarketDataHub()

    with pytest.raises(PriceUnavailableError) as exc_info:
        asyncio.run(
            require_realtime_price(
                "binance",
                "BTC/USDT",
                hub=hub,
                connector=None,
                allow_rest_fallback=False,
            )
        )

    assert "market_data_unavailable:binance:BTC/USDT:hub_missing" in str(exc_info.value)


def test_realtime_price_provider_rejects_rest_exchange_mismatch():
    hub = MarketDataHub()
    connector = _Connector(_Ticker(symbol="BTC/USDT", last=50000.0, exchange="okx"))

    result = asyncio.run(
        get_realtime_price(
            "binance",
            "BTC/USDT",
            hub=hub,
            connector=connector,
            fail_closed=False,
        )
    )

    assert result.ok is False
    assert result.reason == "exchange_mismatch"
    assert hub.snapshot()["rest_fallback_count"] == 0


def test_execution_engine_live_strategy_primary_ignores_unverified_preferred_price(monkeypatch):
    market_data_hub.clear()
    engine = ExecutionEngine()
    execution_engine_module = importlib.import_module("core.trading.execution_engine")
    monkeypatch.setattr(engine, "_paper_trading", False)
    monkeypatch.setattr(
        execution_engine_module,
        "settings",
        SimpleNamespace(
            MARKET_WS_MODE="strategy_primary",
            MARKET_WS_FAIL_CLOSED_FOR_LIVE=True,
            MARKET_WS_SYMBOL_MAX_AGE_SEC=10.0,
        ),
    )
    monkeypatch.setattr(engine, "_resolve_cached_exchange", lambda exchange, account_id=None: None)

    with pytest.raises(PriceUnavailableError):
        asyncio.run(engine._resolve_price("binance", "BTC/USDT", preferred_price=50000.0))
    market_data_hub.clear()


def test_realtime_price_provider_falls_back_to_rest_when_ws_distrusted():
    hub = MarketDataHub(symbol_max_age_sec=10)
    hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 50000.0, "bid": 49999.0, "ask": 50001.0})
    hub.set_ws_trust(False, reason="ws_quality_guard")
    connector = _Connector(_Ticker(symbol="BTC/USDT", last=51000.0, bid=50999.0, ask=51001.0))

    result = asyncio.run(
        get_realtime_price(
            "binance",
            "BTC/USDT",
            hub=hub,
            connector=connector,
        )
    )

    # The fresh-but-distrusted WS tick must not be served; REST wins.
    assert connector.calls == 1
    assert result.ok
    assert result.price == 51000.0
    assert result.source == "rest_fallback"

    # Once trust is restored the (now newer) hub tick is served again.
    hub.set_ws_trust(True)
    hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 50500.0, "bid": 50499.0, "ask": 50501.0})
    result2 = asyncio.run(
        get_realtime_price("binance", "BTC/USDT", hub=hub, connector=connector)
    )
    assert connector.calls == 1
    assert result2.ok
    assert result2.source == "ws"
