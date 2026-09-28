from __future__ import annotations

import asyncio
import time

from core.monitoring import loop_stall_watchdog as wd


def _block_the_loop_synchronously(seconds):
    time.sleep(seconds)


def test_stall_is_recorded_with_the_blocking_frame(monkeypatch):
    monkeypatch.setattr(wd, "HEARTBEAT_SEC", 0.05)
    monkeypatch.setattr(wd, "STALL_SEC", 0.3)
    monkeypatch.setattr(wd, "SAMPLE_SEC", 0.05)
    monkeypatch.setattr(wd, "PROJECT_MARKERS", ("test_loop_stall_watchdog",))
    wd._stalls.clear()

    async def scenario():
        wd.start()
        await asyncio.sleep(0.2)
        _block_the_loop_synchronously(0.9)
        await asyncio.sleep(0.4)  # heartbeat resumes; the watchdog closes the stall
        wd.stop()
        await asyncio.sleep(0.1)

    asyncio.run(scenario())
    stalls = wd.recent_stalls()
    assert len(stalls) == 1
    assert 0.6 <= stalls[0]["duration_sec"] <= 2.0
    assert "_block_the_loop_synchronously" in stalls[0]["top_project_frames"][0][0]
