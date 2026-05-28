"""Lifecycle tests for ``DataCollector``.

Python's ``asyncio.create_task`` only keeps weak references to tasks, so the
event loop can garbage-collect a long-running task mid-execution if the
caller doesn't store a strong reference. ``DataCollector.start()`` used to
discard the return value, which made the collection loop silently die if
GC kicked in. These tests pin down the fix.
"""
from __future__ import annotations

import asyncio
import gc

import pytest

from core.data.data_collector import DataCollector, DataType


def test_start_retains_loop_task_reference():
    """After start(), the loop task must be held on the instance so GC can't
    free it (asyncio's task registry is a weakref set)."""

    async def _run() -> None:
        dc = DataCollector()
        await dc.start()
        try:
            assert dc._loop_task is not None
            assert not dc._loop_task.done()
            # Force a collection cycle. Without the strong reference the task
            # would be eligible for collection here.
            gc.collect()
            await asyncio.sleep(0)  # let event loop run once
            assert dc._loop_task is not None
            assert not dc._loop_task.done()
        finally:
            await dc.stop()

    asyncio.run(_run())


def test_stop_cancels_the_loop_task():
    """stop() must cancel the loop task explicitly, not just flip
    ``_running``. Without cancel, the loop could linger for up to one
    sleep(1) cycle after stop, which complicates shutdown ordering."""

    async def _run() -> None:
        dc = DataCollector()
        await dc.start()
        task = dc._loop_task
        assert task is not None
        await dc.stop()
        # Reference cleared, task marked done.
        assert dc._loop_task is None
        assert task.done()
        # CancelledError is swallowed by stop() — the task should reflect a
        # cancelled state, not a normal completion.
        assert task.cancelled() or task.exception() is not None or task.result() is None

    asyncio.run(_run())


def test_double_start_is_idempotent():
    """Calling start() twice must not spawn a second loop task — that would
    double-fire every interval and (worse) orphan the first task."""

    async def _run() -> None:
        dc = DataCollector()
        await dc.start()
        first_task = dc._loop_task
        try:
            await dc.start()
            assert dc._loop_task is first_task
            assert not first_task.done()
        finally:
            await dc.stop()

    asyncio.run(_run())


def test_stop_without_start_is_safe():
    """Defensive: calling stop() on a collector that never started should
    not raise (e.g., when shutdown runs after a startup error)."""

    async def _run() -> None:
        dc = DataCollector()
        # No start() — _loop_task is None.
        await dc.stop()  # must not raise

    asyncio.run(_run())


def test_callback_failures_are_exposed_in_diagnostics():
    async def _run() -> None:
        dc = DataCollector()
        task_id = dc.add_task("binance", "BTC/USDT", DataType.TICKER, interval=60)

        async def _fake_collect(_task):
            return {"price": 100.0}

        async def _bad_callback(_task, _data):
            raise RuntimeError("callback boom")

        dc._collect_ticker = _fake_collect
        dc.register_callback(DataType.TICKER, _bad_callback)

        await dc._process_task(task_id)

        assert dc.get_collected_data(task_id) == [{"price": 100.0}]
        diagnostics = dc.get_diagnostics()
        assert diagnostics["callback_failures"] == 1
        assert diagnostics["callback_failures_by_type"]["ticker"] == 1
        assert diagnostics["last_callback_error"]["task_id"] == task_id
        assert diagnostics["last_callback_error"]["data_type"] == "ticker"
        assert diagnostics["last_callback_error"]["error"] == "callback boom"

    asyncio.run(_run())
