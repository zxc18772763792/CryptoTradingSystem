"""_handle_error log throttling: identical repeats demote to DEBUG.

Live incident: the gate connector's "Request IP not in whitelist" failed every
balances/positions poll, emitting an identical ERROR line each time around the
clock. _handle_error now logs a given (operation, message) signature at ERROR
once per window and at DEBUG in between. Pure logging behavior — the exception
must always propagate unchanged.
"""
from __future__ import annotations

from unittest.mock import patch

import loguru
import pytest

from core.exchanges.base_exchange import BaseExchange, ExchangeConfig


class _StubExchange(BaseExchange):
    async def connect(self):  # pragma: no cover - stubs
        return True

    async def disconnect(self):  # pragma: no cover
        pass

    async def get_ticker(self, symbol):  # pragma: no cover
        pass

    async def get_klines(self, *a, **k):  # pragma: no cover
        pass

    async def get_balance(self):  # pragma: no cover
        pass

    async def get_positions(self):  # pragma: no cover
        pass

    async def create_order(self, *a, **k):  # pragma: no cover
        pass

    async def cancel_order(self, *a, **k):  # pragma: no cover
        pass

    async def get_order(self, *a, **k):  # pragma: no cover
        pass

    async def get_open_orders(self, *a, **k):  # pragma: no cover
        pass

    async def get_order_book(self, *a, **k):  # pragma: no cover
        pass

    async def get_trades(self, *a, **k):  # pragma: no cover
        pass


def _stub() -> _StubExchange:
    return _StubExchange(ExchangeConfig(name="gate", exchange_type=None, api_key="", api_secret=""))


def test_repeated_identical_error_demotes_to_debug():
    exchange = _stub()
    levels = []
    with patch.object(loguru.logger, "error", lambda m: levels.append("E")), patch.object(
        loguru.logger, "debug", lambda m: levels.append("D")
    ):
        for _ in range(4):
            with pytest.raises(ValueError):
                exchange._handle_error(ValueError("Request IP not in whitelist: 1.2.3.4"), "get_balance")
    assert levels == ["E", "D", "D", "D"]


def test_distinct_errors_and_operations_log_at_error():
    exchange = _stub()
    levels = []
    with patch.object(loguru.logger, "error", lambda m: levels.append("E")), patch.object(
        loguru.logger, "debug", lambda m: levels.append("D")
    ):
        with pytest.raises(ValueError):
            exchange._handle_error(ValueError("whitelist"), "get_balance")
        with pytest.raises(ValueError):
            exchange._handle_error(ValueError("whitelist"), "get_positions")  # different op
        with pytest.raises(ValueError):
            exchange._handle_error(ValueError("something else"), "get_balance")  # different msg
    assert levels == ["E", "E", "E"]


def test_error_always_propagates():
    exchange = _stub()
    with patch.object(loguru.logger, "error", lambda m: None), patch.object(
        loguru.logger, "debug", lambda m: None
    ):
        for _ in range(3):
            with pytest.raises(RuntimeError):
                exchange._handle_error(RuntimeError("boom"), "op")
