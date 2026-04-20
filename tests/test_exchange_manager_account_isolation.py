import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.exchanges.exchange_manager import ExchangeManager

exchange_manager_module = importlib.import_module("core.exchanges.exchange_manager")


def test_exchange_manager_scopes_connectors_by_account(monkeypatch):
    manager = ExchangeManager()
    created = []

    async def _fake_create_connector(name, config, timeout_sec=None):
        del timeout_sec
        connector = SimpleNamespace(
            name=name,
            config=config,
            is_connected=True,
            disconnect=AsyncMock(return_value=None),
            connect=AsyncMock(return_value=True),
        )
        created.append((name, config.api_key, config.api_secret, config.default_type))
        return connector

    monkeypatch.setattr(manager, "_create_connector", _fake_create_connector)
    monkeypatch.setattr(exchange_manager_module.account_manager, "requires_live_connector_isolation", lambda account_id: True)
    monkeypatch.setattr(
        exchange_manager_module.account_manager,
        "get_exchange_credentials",
        lambda account_id, exchange: {
            "api_key": f"{account_id}_{exchange}_key",
            "api_secret": f"{account_id}_{exchange}_secret",
            "default_type": "future",
        },
    )

    async def _run():
        ok_a = await manager.initialize(["binance"], account_id="acct_a")
        ok_b = await manager.initialize(["binance"], account_id="acct_b")
        return ok_a, ok_b

    ok_a, ok_b = asyncio.run(_run())

    connector_a = manager.get_exchange("binance", account_id="acct_a")
    connector_b = manager.get_exchange("binance", account_id="acct_b")

    assert ok_a is True
    assert ok_b is True
    assert connector_a is not None
    assert connector_b is not None
    assert connector_a is not connector_b
    assert connector_a.config.api_key == "acct_a_binance_key"
    assert connector_b.config.api_key == "acct_b_binance_key"
    assert created == [
        ("binance", "acct_a_binance_key", "acct_a_binance_secret", "future"),
        ("binance", "acct_b_binance_key", "acct_b_binance_secret", "future"),
    ]


def test_exchange_manager_blocks_isolated_live_account_without_credentials(monkeypatch):
    manager = ExchangeManager()
    monkeypatch.setattr(exchange_manager_module.account_manager, "requires_live_connector_isolation", lambda account_id: True)
    monkeypatch.setattr(exchange_manager_module.account_manager, "get_exchange_credentials", lambda account_id, exchange: {})

    ok = asyncio.run(manager.initialize(["binance"], account_id="isolated_live"))

    assert ok is False
    assert manager.get_exchange("binance", account_id="isolated_live") is None
