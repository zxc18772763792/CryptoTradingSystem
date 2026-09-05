from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException


def _request(strategies_api, name: str):
    return strategies_api.StrategyRegisterRequest(
        name=name,
        strategy_type="MAStrategy",
        params={},
        symbols=["BTC/USDT"],
        timeframe="15m",
        exchange="binance",
        allocation=0.1,
    )


def test_register_strategy_waits_for_durability_acknowledgement(monkeypatch):
    from web.api import strategies as strategies_api

    persist = AsyncMock(return_value=True)
    unregister = MagicMock(return_value=True)
    monkeypatch.setattr(strategies_api, "_get_strategy_classes", lambda: {"MAStrategy": object})
    monkeypatch.setattr(strategies_api.strategy_manager, "register_strategy", MagicMock(return_value=True))
    monkeypatch.setattr(strategies_api.strategy_manager, "unregister_strategy", unregister)
    monkeypatch.setattr(strategies_api, "_persist_if_exists", persist)
    monkeypatch.setattr(strategies_api, "_schedule_audit_log", MagicMock())

    result = asyncio.run(strategies_api.register_strategy(_request(strategies_api, "durable")))

    assert result["success"] is True
    persist.assert_awaited_once_with("durable", state_override="idle")
    unregister.assert_not_called()


def test_register_strategy_rolls_back_when_persistence_fails(monkeypatch):
    from web.api import strategies as strategies_api

    persist = AsyncMock(return_value=False)
    unregister = MagicMock(return_value=True)
    audit = MagicMock()
    monkeypatch.setattr(strategies_api, "_get_strategy_classes", lambda: {"MAStrategy": object})
    monkeypatch.setattr(strategies_api.strategy_manager, "register_strategy", MagicMock(return_value=True))
    monkeypatch.setattr(strategies_api.strategy_manager, "unregister_strategy", unregister)
    monkeypatch.setattr(strategies_api, "_persist_if_exists", persist)
    monkeypatch.setattr(strategies_api, "_schedule_audit_log", audit)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(strategies_api.register_strategy(_request(strategies_api, "not_durable")))

    assert exc_info.value.status_code == 503
    unregister.assert_called_once_with("not_durable")
    assert audit.call_args.kwargs["status"] == "failed"
    assert audit.call_args.kwargs["details"]["reason"] == "persistence_failed"


def test_persist_if_exists_preserves_false_acknowledgement(monkeypatch):
    from web.api import strategies as strategies_api

    persist = AsyncMock(return_value=False)
    monkeypatch.setattr(strategies_api, "persist_strategy_snapshot", persist)

    result = asyncio.run(strategies_api._persist_if_exists("missing"))

    assert result is False
    persist.assert_awaited_once_with("missing", state_override=None)
