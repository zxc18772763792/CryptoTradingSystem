from __future__ import annotations

import asyncio
import importlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.strategies.strategy_base import Signal, SignalType, StrategyBase
from core.strategies.strategy_manager import StrategyConfig, StrategyManager
from core.trading.execution_engine import ExecutionEngine

execution_engine_module = importlib.import_module("core.trading.execution_engine")


class _NoopStrategy(StrategyBase):
    def generate_signals(self, data):
        return []

    def get_required_data(self):
        return {"type": "kline", "min_length": 1}


def _signal(signal_type: SignalType, *, exchange: str = "binance", strategy_name: str = "route_test") -> Signal:
    return Signal(
        symbol="XRP/USDT",
        signal_type=signal_type,
        price=1.35,
        timestamp=datetime.now(timezone.utc),
        strategy_name=strategy_name,
        strength=0.9,
        metadata={"exchange": exchange},
    )


def test_submit_signal_executes_inline_when_queue_worker_unavailable(monkeypatch):
    engine = ExecutionEngine()
    execute_mock = AsyncMock(return_value={"order_id": "ord-inline"})
    monkeypatch.setattr(engine, "execute_signal", execute_mock)

    accepted = asyncio.run(engine.submit_signal(_signal(SignalType.BUY)))

    assert accepted is True
    assert execute_mock.await_count == 1


def test_emit_signals_keeps_opposite_legs_on_different_exchanges(monkeypatch):
    async def _run() -> None:
        manager = StrategyManager()
        strategy = _NoopStrategy("cex_route")
        strategy.start()
        manager._strategies["cex_route"] = strategy
        manager._configs["cex_route"] = StrategyConfig(
            name="cex_route",
            strategy_class=_NoopStrategy,
            params={"account_id": "strategy_cex_route"},
            symbols=["XRP/USDT"],
            timeframe="1m",
            exchange="binance",
        )

        submitted = []

        async def _submit_signal(signal):
            submitted.append(signal)
            return True

        fake_engine = SimpleNamespace(submit_signal=_submit_signal)
        monkeypatch.setattr(execution_engine_module, "execution_engine", fake_engine)

        await manager._emit_signals(
            "cex_route",
            [
                _signal(SignalType.BUY, exchange="binance", strategy_name="cex_route"),
                _signal(SignalType.SELL, exchange="gate", strategy_name="cex_route"),
            ],
        )

        assert len(submitted) == 2
        assert [sig.signal_type for sig in submitted] == [SignalType.BUY, SignalType.SELL]
        assert [sig.metadata["exchange"] for sig in submitted] == ["binance", "gate"]

    asyncio.run(_run())


def test_emit_signals_still_drops_weaker_opposite_signal_on_same_exchange(monkeypatch):
    async def _run() -> None:
        manager = StrategyManager()
        strategy = _NoopStrategy("conflict_route")
        strategy.start()
        manager._strategies["conflict_route"] = strategy
        manager._configs["conflict_route"] = StrategyConfig(
            name="conflict_route",
            strategy_class=_NoopStrategy,
            params={"account_id": "strategy_conflict_route"},
            symbols=["XRP/USDT"],
            timeframe="1m",
            exchange="binance",
        )

        submitted = []

        async def _submit_signal(signal):
            submitted.append(signal)
            return True

        fake_engine = SimpleNamespace(submit_signal=_submit_signal)
        monkeypatch.setattr(execution_engine_module, "execution_engine", fake_engine)

        first = _signal(SignalType.BUY, exchange="binance", strategy_name="conflict_route")
        first.strength = 0.9
        second = _signal(SignalType.SELL, exchange="binance", strategy_name="conflict_route")
        second.timestamp = first.timestamp
        second.strength = 0.6

        await manager._emit_signals("conflict_route", [first, second])

        assert len(submitted) == 1
        assert submitted[0].signal_type == SignalType.BUY

    asyncio.run(_run())


def test_auto_created_strategy_account_uses_signal_exchange(monkeypatch):
    engine = ExecutionEngine()
    signal = _signal(SignalType.SELL, exchange="gate")
    account = {
        "account_id": "strategy_route_test",
        "exchange": "binance",
        "metadata": {"auto_created": True, "strategy_name": "route_test"},
    }

    monkeypatch.setattr(execution_engine_module.account_manager, "get_account", lambda account_id: account)
    monkeypatch.setattr(
        execution_engine_module.account_manager,
        "resolve_exchange",
        lambda account_id, default: "binance",
    )

    assert engine._resolve_signal_exchange(signal, "strategy_route_test") == "gate"


def test_manual_account_still_uses_account_exchange(monkeypatch):
    engine = ExecutionEngine()
    signal = _signal(SignalType.SELL, exchange="gate")
    account = {
        "account_id": "main",
        "exchange": "binance",
        "metadata": {},
    }

    monkeypatch.setattr(execution_engine_module.account_manager, "get_account", lambda account_id: account)
    monkeypatch.setattr(
        execution_engine_module.account_manager,
        "resolve_exchange",
        lambda account_id, default: "binance",
    )

    assert engine._resolve_signal_exchange(signal, "main") == "binance"
