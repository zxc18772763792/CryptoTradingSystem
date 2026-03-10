"""Background research scheduler.

Polls the proposal registry every N seconds, finds proposals in
``research_queued`` status that have no active job, and dispatches
them via the orchestrator's ``run_proposal()`` in background mode.

Usage in lifespan::

    from core.ai.research_scheduler import research_scheduler
    research_scheduler.set_app(app)
    research_scheduler.start()
    ...
    await research_scheduler.stop()
"""
from __future__ import annotations

import asyncio
import contextlib
import os
from typing import Any, Optional

from loguru import logger


_INTERVAL_DEFAULT = max(30, int(os.getenv("RESEARCH_SCHEDULER_INTERVAL_SEC", "300")))


class ResearchScheduler:
    """Asyncio background task that auto-dispatches queued research proposals."""

    def __init__(self, interval_seconds: int = _INTERVAL_DEFAULT):
        self._interval_seconds = max(30, int(interval_seconds))
        self._app: Optional[Any] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._task: Optional[asyncio.Task] = None  # type: ignore[type-arg]

    def set_app(self, app: Any) -> None:
        self._app = app

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="ai_research_scheduler")
        logger.info(f"ResearchScheduler started (interval={self._interval_seconds}s)")

    async def stop(self) -> None:
        if self._stop_event:
            self._stop_event.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        logger.info("ResearchScheduler stopped")

    async def _loop(self) -> None:
        # brief startup delay so the rest of lifespan can finish first
        await asyncio.sleep(30)
        while not self._is_stopped():
            try:
                await self._tick()
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"ResearchScheduler tick error: {exc}")
            # interruptible sleep
            for _ in range(self._interval_seconds):
                if self._is_stopped():
                    break
                await asyncio.sleep(1)

    def _is_stopped(self) -> bool:
        return bool(self._stop_event and self._stop_event.is_set())

    async def _tick(self) -> None:
        if self._app is None:
            return

        from core.research.orchestrator import (
            ensure_ai_research_runtime_state,
            list_proposals,
            run_proposal,
        )

        app = self._app
        ensure_ai_research_runtime_state(app)

        all_proposals = list_proposals(app, limit=50)
        queued = [p for p in all_proposals if p.status == "research_queued"]
        if not queued:
            return

        # collect proposal IDs that already have a running/pending job
        running_ids: set[str] = set()
        for job in list((getattr(app.state, "research_jobs", {}) or {}).values()):
            if job.get("status") in {"pending", "running"} and job.get("proposal_id"):
                running_ids.add(str(job["proposal_id"]))

        dispatched = 0
        for proposal in queued:
            if proposal.proposal_id in running_ids:
                continue
            try:
                # reuse parameters from the last research request if available
                last_req: dict = dict(
                    (proposal.metadata or {}).get("last_research_request") or {}
                )
                await run_proposal(
                    app,
                    proposal_id=proposal.proposal_id,
                    actor="scheduler",
                    exchange=str(last_req.get("exchange") or "binance"),
                    symbol=str(last_req.get("symbol") or ""),
                    days=int(last_req.get("days") or 30),
                    commission_rate=float(last_req.get("commission_rate") or 0.0004),
                    slippage_bps=float(last_req.get("slippage_bps") or 2.0),
                    initial_capital=float(last_req.get("initial_capital") or 10000.0),
                    background=True,
                    timeframes=list(last_req.get("timeframes") or []),
                    strategies=list(last_req.get("strategies") or []),
                )
                dispatched += 1
                logger.info(
                    f"ResearchScheduler dispatched proposal {proposal.proposal_id}"
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"ResearchScheduler: failed to dispatch "
                    f"{proposal.proposal_id}: {exc}"
                )

        if dispatched:
            logger.info(f"ResearchScheduler: dispatched {dispatched} queued proposal(s)")


# module-level singleton – shared with lifespan
research_scheduler = ResearchScheduler(interval_seconds=_INTERVAL_DEFAULT)
