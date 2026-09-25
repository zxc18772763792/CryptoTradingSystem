from __future__ import annotations

import asyncio

from web.api import research


def _module(status: str, tag: str):
    return {"module": "market_state", "status": status, "payload": {"tag": tag}, "warnings": []}


def test_workbench_module_cache_serves_fresh_then_stale_and_skips_errors(monkeypatch):
    calls = {"n": 0}
    statuses = ["degraded", "degraded", "error"]

    async def fake_uncached(module_name, profile):
        calls["n"] += 1
        return _module(statuses[calls["n"] - 1], f"run{calls['n']}")

    clock = {"now": 1000.0}
    monkeypatch.setattr(research, "_capture_module_build_uncached", fake_uncached)
    monkeypatch.setattr(research.time, "time", lambda: clock["now"])
    profile = research.ResearchProfile()

    async def scenario():
        first = await research._capture_module_build("market_state", profile)
        assert first["payload"]["tag"] == "run1"
        assert first["cache"]["served_mode"] == "live_compute"

        clock["now"] += 30
        hit = await research._capture_module_build("market_state", profile)
        assert hit["cache"]["served_mode"] == "cache_hit" and calls["n"] == 1

        clock["now"] += 120  # past fresh window: stale served, refresh runs behind
        stale = await research._capture_module_build("market_state", profile)
        assert stale["payload"]["tag"] == "run1"
        assert stale["cache"]["served_mode"] == "stale_refresh"
        await asyncio.gather(*research._WORKBENCH_MODULE_REFRESH_TASKS.values())
        assert calls["n"] == 2

        clock["now"] += 1000  # past stale window: recompute; an error result is not cached
        errored = await research._capture_module_build("market_state", profile)
        assert errored["status"] == "error"
        key = research._workbench_module_cache_key("market_state", profile)
        assert research._WORKBENCH_MODULE_CACHE[key]["result"]["payload"]["tag"] == "run2"

    asyncio.run(scenario())
    research._clear_workbench_module_cache()
