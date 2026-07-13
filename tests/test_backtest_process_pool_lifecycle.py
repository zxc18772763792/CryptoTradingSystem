from __future__ import annotations

import asyncio
import threading

import pytest

from web.api import backtest


class _FakeProcess:
    def __init__(self) -> None:
        self.alive = True
        self.terminated = False
        self.killed = False

    def is_alive(self) -> bool:
        return self.alive

    def terminate(self) -> None:
        self.terminated = True
        self.alive = False

    def kill(self) -> None:
        self.killed = True
        self.alive = False

    def join(self, timeout=None) -> None:
        return None


class _FakePool:
    def __init__(self) -> None:
        self.process = _FakeProcess()
        self._processes = {1: self.process}
        self.shutdown_calls = []

    def shutdown(self, *, wait, cancel_futures) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


def test_shutdown_helper_cancels_and_reaps_registered_pools():
    backtest.shutdown_optimize_process_pools()
    pool = _FakePool()
    cancel_event = threading.Event()
    backtest._register_optimize_pool(pool, cancel_event)

    assert backtest.shutdown_optimize_process_pools(wait=False) == 1
    assert cancel_event.is_set()
    assert pool.shutdown_calls == [(False, True)]
    assert pool.process.terminated is True


def test_worker_initializer_starts_daemon_parent_watch(monkeypatch):
    captured = {}

    class FakeThread:
        def __init__(self, *, target, name, daemon):
            captured.update(target=target, name=name, daemon=daemon)

        def start(self):
            captured["started"] = True

    monkeypatch.setattr(backtest.threading, "Thread", FakeThread)
    backtest._optimize_worker_initializer(1234)

    assert captured["daemon"] is True
    assert captured["started"] is True
    assert "1234" in captured["name"]


def test_async_cancellation_terminates_its_pool(monkeypatch):
    pool = _FakePool()
    started = threading.Event()

    def fake_optimize(**kwargs):
        cancel_event = kwargs["_cancel_event"]
        backtest._register_optimize_pool(pool, cancel_event)
        started.set()
        cancel_event.wait(timeout=5)
        return {}

    monkeypatch.setattr(backtest, "_optimize_strategy_on_df", fake_optimize)

    async def run():
        task = asyncio.create_task(backtest._optimize_strategy_async(value=1))
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert pool.process.terminated is True
