from __future__ import annotations

import asyncio
import time

import pytest

from core.ai.research_scheduler import ResearchScheduler


def test_scheduler_sleep_wakes_when_stop_event_is_set():
    async def _run() -> None:
        scheduler = ResearchScheduler(interval_seconds=30)
        scheduler._stop_event = asyncio.Event()

        started = time.perf_counter()
        sleep_task = asyncio.create_task(scheduler._sleep_until_stopped(30))
        await asyncio.sleep(0)
        scheduler._stop_event.set()

        assert await sleep_task is True
        assert time.perf_counter() - started < 0.2

    asyncio.run(_run())


def test_scheduler_stop_cancels_startup_sleep_and_cleans_task():
    async def _run() -> None:
        scheduler = ResearchScheduler(interval_seconds=30)
        scheduler.start()

        started = time.perf_counter()
        await scheduler.stop()

        assert scheduler._task is None
        assert time.perf_counter() - started < 0.2

    asyncio.run(_run())


def test_scheduler_stop_does_not_swallow_completed_task_exception():
    async def _run() -> None:
        scheduler = ResearchScheduler(interval_seconds=30)
        scheduler._stop_event = asyncio.Event()
        scheduler._task = asyncio.create_task(_fail())
        await asyncio.sleep(0)

        with pytest.raises(RuntimeError, match="scheduler boom"):
            await scheduler.stop()

        assert scheduler._task is None

    async def _fail() -> None:
        raise RuntimeError("scheduler boom")

    asyncio.run(_run())
