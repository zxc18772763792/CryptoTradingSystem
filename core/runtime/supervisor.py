from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional

from core.runtime.state import RuntimeState


TaskFactory = Callable[[asyncio.Event], Awaitable[None]]


@dataclass
class ManagedTask:
    name: str
    stop_event: asyncio.Event
    task: asyncio.Task
    restart_on_failure: bool = False


class RuntimeTaskSupervisor:
    def __init__(self, runtime_state: RuntimeState) -> None:
        self._runtime_state = runtime_state
        self._tasks: Dict[str, ManagedTask] = {}

    def start_task(
        self,
        name: str,
        factory: TaskFactory,
        *,
        restart_on_failure: bool = False,
    ) -> ManagedTask:
        existing = self._tasks.get(name)
        if existing and not existing.task.done():
            return existing

        stop_event = asyncio.Event()
        self._runtime_state.register_task(name, restart_on_failure=restart_on_failure)
        task = asyncio.create_task(
            self._run_managed(name, factory, stop_event, restart_on_failure=restart_on_failure),
            name=f"managed::{name}",
        )
        managed = ManagedTask(
            name=name,
            stop_event=stop_event,
            task=task,
            restart_on_failure=restart_on_failure,
        )
        self._tasks[name] = managed
        return managed

    async def _run_managed(
        self,
        name: str,
        factory: TaskFactory,
        stop_event: asyncio.Event,
        *,
        restart_on_failure: bool,
    ) -> None:
        backoff_sec = 1.0
        while not stop_event.is_set():
            self._runtime_state.mark_task_started(name)
            try:
                await factory(stop_event)
                self._runtime_state.touch_task(name, success=True)
                self._runtime_state.mark_task_stopped(name)
                return
            except asyncio.CancelledError:
                self._runtime_state.mark_task_stopped(name)
                raise
            except Exception as exc:
                self._runtime_state.mark_task_failed(name, str(exc), will_restart=restart_on_failure)
                if not restart_on_failure or stop_event.is_set():
                    return
                await asyncio.sleep(backoff_sec)
                backoff_sec = min(backoff_sec * 2.0, 30.0)

    def touch(self, name: str, *, success: bool = False) -> None:
        self._runtime_state.touch_task(name, success=success)

    async def stop_task(self, name: str, *, timeout_sec: float = 5.0) -> None:
        managed = self._tasks.get(name)
        if not managed:
            self._runtime_state.mark_task_stopped(name)
            return
        managed.stop_event.set()
        managed.task.cancel()
        try:
            await asyncio.wait_for(managed.task, timeout=timeout_sec)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        self._runtime_state.mark_task_stopped(name)

    async def stop_all(self, *, timeout_sec: float = 5.0) -> None:
        names = list(self._tasks.keys())
        for name in names:
            await self.stop_task(name, timeout_sec=timeout_sec)

    def get_task(self, name: str) -> Optional[ManagedTask]:
        return self._tasks.get(name)
