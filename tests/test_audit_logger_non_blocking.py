from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock


def test_audit_logger_sanitizes_sensitive_details():
    from core.audit.audit_logger import _sanitize_audit_details

    payload = {
        "api_key": "key-secret",
        "token_present": True,
        "nested": {
            "password": "pw",
            "safe": "ok",
            "items": [{"x-api-key": "nested-key"}],
        },
    }

    sanitized = _sanitize_audit_details(payload)

    assert sanitized["api_key"] == "[REDACTED]"
    assert sanitized["token_present"] is True
    assert sanitized["nested"]["password"] == "[REDACTED]"
    assert sanitized["nested"]["safe"] == "ok"
    assert sanitized["nested"]["items"][0]["x-api-key"] == "[REDACTED]"


async def _wait_until(predicate, *, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition not reached before timeout")
        await asyncio.sleep(0.01)


def test_trading_runtime_risk_update_does_not_wait_for_audit(monkeypatch):
    from web.api import trading_runtime

    audit_started = asyncio.Event()
    release_audit = asyncio.Event()

    async def blocked_audit_log(**_kwargs):
        audit_started.set()
        await release_audit.wait()

    async def fake_report(*, force_live_refresh=False):
        return {"force_live_refresh": force_live_refresh}

    async def scenario():
        monkeypatch.setattr(trading_runtime.audit_logger, "log", blocked_audit_log)
        monkeypatch.setattr(trading_runtime.risk_manager, "update_parameters", lambda payload: None)
        monkeypatch.setattr(trading_runtime, "_build_effective_risk_report", fake_report)

        result = await asyncio.wait_for(
            trading_runtime.update_risk_params(
                trading_runtime.RiskUpdateRequest(max_open_positions=3)
            ),
            timeout=0.2,
        )

        assert result["success"] is True
        await _wait_until(audit_started.is_set)
        release_audit.set()
        await asyncio.sleep(0)

    asyncio.run(scenario())


def test_strategy_start_does_not_wait_for_audit(monkeypatch):
    from web.api import strategies

    audit_started = asyncio.Event()
    release_audit = asyncio.Event()

    async def blocked_audit_log(**_kwargs):
        audit_started.set()
        await release_audit.wait()

    async def scenario():
        monkeypatch.setattr(strategies.audit_logger, "log", blocked_audit_log)
        monkeypatch.setattr(strategies.strategy_manager, "start_strategy", AsyncMock(return_value=True))
        monkeypatch.setattr(strategies, "_persist_if_exists", AsyncMock(return_value=None))

        result = await asyncio.wait_for(strategies.start_strategy("non_blocking"), timeout=0.2)

        assert result == {"success": True, "name": "non_blocking", "status": "running"}
        await _wait_until(audit_started.is_set)
        release_audit.set()
        await asyncio.sleep(0)

    asyncio.run(scenario())


def test_trading_create_order_does_not_wait_for_audit(monkeypatch):
    from web.api import trading

    audit_started = asyncio.Event()
    release_audit = asyncio.Event()

    async def blocked_audit_log(**_kwargs):
        audit_started.set()
        await release_audit.wait()

    async def fake_execute_manual_order(**_kwargs):
        return {"order_id": "ord-1", "status": "filled", "price": 100.0, "amount": 1.0, "filled": 1.0}

    async def scenario():
        monkeypatch.setattr(trading.audit_logger, "log", blocked_audit_log)
        monkeypatch.setattr(trading, "_precheck_binance_futures_order", AsyncMock(return_value=None))
        monkeypatch.setattr(trading.execution_engine, "execute_manual_order", fake_execute_manual_order)

        result = await asyncio.wait_for(
            trading.create_order(
                trading.OrderRequest(
                    exchange="binance",
                    symbol="BTC/USDT",
                    side="buy",
                    order_type="market",
                    amount=1.0,
                )
            ),
            timeout=0.2,
        )

        assert result.order_id == "ord-1"
        assert datetime.fromisoformat(result.timestamp).tzinfo == timezone.utc
        await _wait_until(audit_started.is_set)
        release_audit.set()
        await asyncio.sleep(0)

    asyncio.run(scenario())
