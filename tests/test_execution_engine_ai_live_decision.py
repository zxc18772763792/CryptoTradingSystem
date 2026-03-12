from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.strategies import Signal, SignalType
from core.trading.execution_engine import ExecutionEngine


execution_engine_module = importlib.import_module("core.trading.execution_engine")


def _make_signal() -> Signal:
    return Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.BUY,
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name="ai_gate_strategy",
        strength=0.9,
        metadata={"account_id": "main", "exchange": "binance"},
    )


def test_execute_signal_respects_ai_live_decision_block(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False

    signal = _make_signal()
    rejected_order = SimpleNamespace(id="rej-ai-1")

    monkeypatch.setattr(execution_engine_module.account_manager, "resolve_exchange", lambda account_id, exchange: "binance")
    monkeypatch.setattr(engine, "_resolve_strategy_trade_policy", lambda strategy_name, exchange: {"allow_long": True, "allow_short": True})
    monkeypatch.setattr(execution_engine_module.position_manager, "get_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=10000.0))
    monkeypatch.setattr(execution_engine_module.strategy_manager, "get_strategy_allocation", lambda name: 0.1)
    monkeypatch.setattr(engine, "_calculate_quantity", AsyncMock(return_value=0.01))
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 1.0)))
    monkeypatch.setattr(engine, "_ensure_signal_protection_levels", lambda **kwargs: (99.0, 102.0))
    monkeypatch.setattr(
        engine,
        "_evaluate_live_ai_decision",
        AsyncMock(
            return_value={
                "action": "block",
                "applied": True,
                "provider": "glm",
                "model": "GLM-4.5-Air",
                "reason": "model_block",
            }
        ),
    )
    monkeypatch.setattr(execution_engine_module.order_manager, "record_rejected_order", AsyncMock(return_value=rejected_order))
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))
    monkeypatch.setattr(execution_engine_module, "write_audit", AsyncMock(return_value=None))

    result = asyncio.run(engine.execute_signal(signal))
    assert result is None
    assert int(engine.get_signal_diagnostics().get("ai_rejected") or 0) == 1
