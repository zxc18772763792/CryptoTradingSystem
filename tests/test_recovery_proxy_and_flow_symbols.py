from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from config.exchanges import ExchangeConfig, ExchangeType
from core.exchanges import gate_connector as gate
from core.exchanges import okx_connector as okx
from core.research import coinglass_daily_store as store


class Client:
    def __init__(self, config):
        self.config = config
    async def fetch_time(self):
        return 1
    async def load_markets(self):
        return {}
    async def close(self):
        pass


def test_gate_explicit_route_overrides_bypass_and_reaches_async_transport(monkeypatch):
    monkeypatch.setattr(gate, "settings", SimpleNamespace(
        GATE_PROXY="http://127.0.0.1:7890", HTTP_PROXY="http://global:8080", HTTPS_PROXY=None,
        GATE_API_KEY="test", GATE_API_SECRET="test"))
    monkeypatch.setattr(gate, "bypasses_proxy", lambda url: True)
    monkeypatch.setattr(gate.ccxt, "gate", Client)
    connector = gate.GateConnector(ExchangeConfig(name="gate", exchange_type=ExchangeType.CEX))
    assert asyncio.run(connector.connect())
    assert connector._client.aiohttp_proxy == "http://127.0.0.1:7890"


def test_gate_still_respects_bypass_without_an_explicit_route(monkeypatch):
    monkeypatch.setattr(gate, "settings", SimpleNamespace(
        GATE_PROXY="", HTTP_PROXY="http://global:8080", HTTPS_PROXY=None,
        GATE_API_KEY="test", GATE_API_SECRET="test"))
    monkeypatch.setattr(gate, "bypasses_proxy", lambda url: True)
    monkeypatch.setattr(gate.ccxt, "gate", Client)
    connector = gate.GateConnector(ExchangeConfig(name="gate", exchange_type=ExchangeType.CEX))
    assert asyncio.run(connector.connect())
    assert not getattr(connector._client, "aiohttp_proxy", None)


def test_okx_proxy_is_applied_to_async_transport(monkeypatch):
    monkeypatch.setattr(okx, "settings", SimpleNamespace(HTTP_PROXY="http://127.0.0.1:7890", HTTPS_PROXY=None))
    connector = okx.OKXConnector(ExchangeConfig(name="okx", exchange_type=ExchangeType.CEX))
    client = Client({})
    connector._apply_proxy(client)
    assert client.aiohttp_proxy == "http://127.0.0.1:7890"


def test_xec_usd_flow_alias_leaves_other_datasets_and_contracts_unchanged():
    day = datetime(2026, 11, 1, tzinfo=timezone.utc)
    for dataset in ("liquidation", "taker"):
        requests = store.requests_for({}, "XEC", "1000XECUSDT", dataset, day)
        assert requests[0][1]["symbol"] == "1000XEC"
        assert requests[0][1]["exchange_list"] == "Binance,OKX,Bybit"
        unchanged = store.requests_for({}, "PEPE", "1000PEPEUSDT", dataset, day)
        assert unchanged[0][1]["symbol"] == "PEPE"
    for dataset in ("oi", "funding"):
        assert store.requests_for({}, "XEC", "1000XECUSDT", dataset, day)[0][1]["symbol"] == "XEC"
    for dataset in store.SNAPSHOT_AT_STAMP:
        assert store.requests_for({}, "XEC", "1000XECUSDT", dataset, day)[0][1]["symbol"] == "1000XECUSDT"
