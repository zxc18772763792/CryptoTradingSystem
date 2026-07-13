from __future__ import annotations

import asyncio
import importlib

from core.trading.execution_engine import ExecutionEngine


execution_engine_module = importlib.import_module("core.trading.execution_engine")


def test_per_account_mode_is_task_local_and_does_not_flip_process_defaults(monkeypatch):
    engine = ExecutionEngine()
    engine.set_paper_trading(True, sync_runtime_state=False)
    scope_switches: list[str] = []
    risk_switches: list[str] = []
    order_mode_switches: list[bool] = []

    monkeypatch.setattr(
        execution_engine_module.position_manager,
        "set_scope",
        lambda mode: scope_switches.append(mode),
    )
    monkeypatch.setattr(
        execution_engine_module.risk_manager,
        "set_account_scope",
        lambda mode, reset_baseline=False: risk_switches.append(mode),
    )
    monkeypatch.setattr(
        execution_engine_module.order_manager,
        "set_paper_trading",
        lambda enabled: order_mode_switches.append(enabled),
    )

    async def _run():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def _guarded():
            async with engine._mode_guard("live"):
                assert engine._current_trading_mode() == "live"
                assert engine._paper_trading is True
                entered.set()
                await release.wait()

        task = asyncio.create_task(_guarded())
        await entered.wait()
        # ContextVar state must not leak into an unrelated request task.
        assert engine._current_trading_mode() == "paper"
        assert engine.get_trading_mode() == "paper"
        release.set()
        await task

    asyncio.run(_run())

    assert engine._current_trading_mode() == "paper"
    assert order_mode_switches == []
    assert scope_switches == ["live", "paper"]
    assert risk_switches == ["live", "paper"]
