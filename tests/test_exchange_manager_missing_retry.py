from __future__ import annotations

import asyncio

from core.exchanges.exchange_manager import ExchangeManager


class _Conn:
    is_connected = True

    async def health_check(self):
        return True

    async def disconnect(self):
        return None


def test_connector_that_failed_at_startup_is_reported_and_reconnected(monkeypatch):
    manager = ExchangeManager()
    attempts = {"binance": 0}

    async def create(name, config, *, timeout_sec=None):
        if name == "binance":
            attempts["binance"] += 1
            return None if attempts["binance"] == 1 else _Conn()  # startup connect times out
        return _Conn()

    monkeypatch.setattr(manager, "_resolve_exchange_config", lambda name, account_id=None: {"name": name})
    monkeypatch.setattr(manager, "_create_connector", create)

    async def scenario():
        await manager.initialize(["gate", "binance"])
        assert manager.get_exchange("binance") is None
        health = await manager.health_check()
        assert health == {"gate": True, "binance": False}  # visible to the watchdog now
        assert await manager.reconnect_exchange("binance", timeout_sec=1.0) is True
        assert await manager.health_check() == {"gate": True, "binance": True}

    asyncio.run(scenario())


def test_exchange_without_config_is_not_reported(monkeypatch):
    manager = ExchangeManager()
    monkeypatch.setattr(manager, "_resolve_exchange_config", lambda name, account_id=None: {"name": name} if name == "gate" else None)

    async def create(name, config, *, timeout_sec=None):
        return _Conn()

    monkeypatch.setattr(manager, "_create_connector", create)
    asyncio.run(manager.initialize(["gate", "okx"]))
    assert asyncio.run(manager.health_check()) == {"gate": True}  # okx was never intended
