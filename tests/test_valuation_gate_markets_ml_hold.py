"""Spot-only valuation in futures mode, Gate's market scope, and the ML hold on the strategy catalog."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import ccxt.async_support as ccxt_async
import pytest

from config.exchanges import ExchangeConfig, ExchangeType
from config.settings import settings
from core.ai.ml_signal import MLSignalModel
from core.exchanges.binance_connector import BinanceConnector
from core.exchanges.gate_connector import GateConnector
from core.utils import asset_valuation
from strategies.ai.ml_xgboost_strategy import MLXGBoostStrategy


def _market(symbol: str, *, spot: bool) -> dict:
    pair = symbol.split(":")[0]
    base, quote = pair.split("/")
    return {"id": pair.replace("/", ""), "symbol": symbol, "base": base, "quote": quote, "settle": None if spot else quote,
            "baseId": base, "quoteId": quote, "settleId": None if spot else quote, "spot": spot, "swap": not spot, "future": False,
            "option": False, "margin": False, "contract": not spot, "linear": None if spot else True, "inverse": None, "active": True,
            "type": "spot" if spot else "swap", "subType": None if spot else "linear", "precision": {}, "limits": {},
            "taker": 0.001, "maker": 0.001, "contractSize": None if spot else 1, "expiry": None, "expiryDatetime": None, "strike": None,
            "optionType": None, "info": {}}


MARKETS = [_market("LUNA/USDT", spot=True), _market("SOL/USDT", spot=True), _market("SOL/USDT:USDT", spot=False),
           _market("PEPE/USDT", spot=True), _market("1000PEPE/USDT:USDT", spot=False)]


def _connector(default_type: str = "future") -> BinanceConnector:
    connector = BinanceConnector(ExchangeConfig(name="binance", exchange_type=ExchangeType.CEX, api_key="test_key",
                                                api_secret="test_secret", default_type=default_type))
    client = ccxt_async.binance(connector._build_client_config())
    client.set_markets(MARKETS)
    client.publicGetTickerPrice = AsyncMock(return_value={"symbol": "LUNAUSDT", "price": "0.25"})
    connector._client, connector._connected = client, True
    return connector


def _run(connector: BinanceConnector, coroutine_factory):
    async def runner():
        try:
            return await coroutine_factory()
        finally:
            await connector._client.close()
    return asyncio.run(runner())


def test_ccxt_cannot_resolve_a_spot_only_coin_in_futures_mode():
    """The reason get_spot_only_last_price exists: if ccxt ever stops doing this, the method can go."""
    connector = _connector()
    with pytest.raises(Exception, match="does not have market symbol LUNA/USDT"):
        connector._client.market("LUNA/USDT")
    _run(connector, AsyncMock())


def test_spot_only_coin_is_priced_from_the_spot_ticker_in_futures_mode():
    connector = _connector()
    assert _run(connector, lambda: connector.get_spot_only_last_price("LUNA/USDT")) == 0.25
    connector._client.publicGetTickerPrice.assert_awaited_once_with({"symbol": "LUNAUSDT"})


@pytest.mark.parametrize("symbol", ["SOL/USDT", "PEPE/USDT", "NOSUCH/USDT"])
def test_coins_with_a_perpetual_or_no_market_keep_the_normal_path(symbol):
    connector = _connector()
    assert _run(connector, lambda: connector.get_spot_only_last_price(symbol)) is None
    connector._client.publicGetTickerPrice.assert_not_awaited()


def test_spot_mode_never_needs_the_spot_only_path():
    connector = _connector("spot")
    assert _run(connector, lambda: connector.get_spot_only_last_price("LUNA/USDT")) is None
    connector._client.publicGetTickerPrice.assert_not_awaited()


def test_valuation_prices_a_spot_only_coin_without_asking_the_futures_ticker(monkeypatch):
    realtime = AsyncMock(side_effect=AssertionError("a spot-only coin must not reach the futures ticker"))
    monkeypatch.setattr(asset_valuation, "get_realtime_price", realtime)
    connector = _connector()
    price = _run(connector, lambda: asset_valuation.resolve_currency_usd_price(connector, "LUNA", bridge_usd={"BTC": 1.0}))
    assert price == 0.25
    realtime.assert_not_awaited()


def test_valuation_keeps_the_normal_path_for_a_coin_with_a_perpetual(monkeypatch):
    realtime = AsyncMock(return_value=SimpleNamespace(ok=True, price=150.0))
    monkeypatch.setattr(asset_valuation, "get_realtime_price", realtime)
    connector = _connector()
    assert _run(connector, lambda: asset_valuation.resolve_currency_usd_price(connector, "SOL", bridge_usd={"BTC": 1.0})) == 150.0
    connector._client.publicGetTickerPrice.assert_not_awaited()


def test_a_failed_spot_ticker_values_the_coin_at_zero_for_this_pass(monkeypatch):
    monkeypatch.setattr(asset_valuation, "get_realtime_price", AsyncMock(side_effect=AssertionError("not this path")))
    connector = _connector()
    connector._client.publicGetTickerPrice = AsyncMock(side_effect=OSError("offline"))
    assert _run(connector, lambda: asset_valuation.resolve_currency_usd_price(connector, "LUNA", bridge_usd={})) == 0.0


def test_gate_load_markets_asks_only_for_spot_and_swap():
    connector = GateConnector(ExchangeConfig(name="gate", exchange_type=ExchangeType.CEX, api_key="test_key", api_secret="test_secret"))
    client = ccxt_async.gate(connector._client_config())
    called = []

    def endpoint(name):
        async def _call(params={}):
            called.append(name)
            return []
        return _call

    for name in ("publicSpotGetCurrencyPairs", "publicMarginGetCurrencyPairs", "publicFuturesGetSettleContracts",
                 "publicDeliveryGetSettleContracts", "publicOptionsGetUnderlyings", "publicSpotGetCurrencies"):
        setattr(client, name, endpoint(name))
    client.load_unified_status = AsyncMock()

    async def runner():
        try:
            await client.load_markets()
        finally:
            await client.close()

    asyncio.run(runner())
    assert "publicSpotGetCurrencyPairs" in called and "publicFuturesGetSettleContracts" in called
    assert not {"publicDeliveryGetSettleContracts", "publicOptionsGetUnderlyings", "publicSpotGetCurrencies"} & set(called)


def test_ml_hold_skips_the_blocking_model_load(monkeypatch):
    monkeypatch.setattr(settings, "ML_SIGNAL_ENABLED", False)
    monkeypatch.setattr(settings, "ML_SIGNAL_DISABLED_REASON", "quality_gate_rejected")
    monkeypatch.setattr(MLSignalModel, "load", lambda self: pytest.fail("the catalog must not load a held model"))
    strategy = MLXGBoostStrategy(name="defaults", params={})
    assert not strategy._model.is_loaded()
    assert strategy._model.load_error == "quality_gate_rejected"


def test_ml_enabled_or_an_explicit_model_still_loads(monkeypatch):
    loads = []
    monkeypatch.setattr(MLSignalModel, "load", lambda self: loads.append(self._model_path))
    monkeypatch.setattr(settings, "ML_SIGNAL_ENABLED", True)
    MLXGBoostStrategy(name="enabled", params={})
    monkeypatch.setattr(settings, "ML_SIGNAL_ENABLED", False)
    MLXGBoostStrategy(name="explicit", params={"model_path": "custom.json"})
    assert len(loads) == 2 and loads[1] == "custom.json"
