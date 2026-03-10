import asyncio

from core.runtime.state import RuntimeState
from core.runtime.supervisor import RuntimeTaskSupervisor


def test_runtime_state_mode_switch_and_cache_snapshot():
    state = RuntimeState()
    cleared = {"count": 0}

    def _clear():
        cleared["count"] += 1
        return {"cleared": True}

    def _inspect():
        return {"entries": 3}

    state.initialize_mode("paper", reason="test_start")
    switch_info = state.begin_mode_switch("live", reason="switch_to_live")
    state.register_cache("demo_cache", clear=_clear, inspect=_inspect, scope="global")
    cache_reset = state.clear_registered_caches(scope="live")
    state.update_equity_snapshot(1234.56)
    state.finish_mode_switch("live", reason="switch_done")

    snapshot = state.snapshot()

    assert switch_info["previous_mode"] == "paper"
    assert state.get_trading_mode() == "live"
    assert state.get_account_scope() == "live"
    assert cleared["count"] == 1
    assert cache_reset["demo_cache"]["cleared"] is True
    assert snapshot["equity_snapshot"]["value"] == 1234.56
    assert snapshot["caches"]["demo_cache"]["entries"] == 3
    assert snapshot["last_mode_switch_reason"] == "switch_done"


def test_runtime_task_supervisor_restarts_failed_worker():
    async def _run() -> None:
        state = RuntimeState()
        supervisor = RuntimeTaskSupervisor(state)
        attempts = {"count": 0}

        async def flaky_worker(stop_event: asyncio.Event) -> None:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("boom")
            state.touch_task("flaky", success=True)
            stop_event.set()

        supervisor.start_task("flaky", flaky_worker, restart_on_failure=True)
        await asyncio.sleep(1.3)
        await supervisor.stop_all(timeout_sec=1.0)

        diagnostics = state.get_task_diagnostics()["flaky"]
        assert attempts["count"] >= 2
        assert diagnostics["restarts"] >= 1
        assert diagnostics["last_success_at"] is not None

    asyncio.run(_run())
