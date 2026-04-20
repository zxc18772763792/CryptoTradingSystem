from __future__ import annotations

import asyncio
import time

import pandas as pd

from web.api import data as data_api


async def _sleep_forever() -> None:
    await asyncio.sleep(60)


def test_clear_data_api_runtime_caches_clears_registered_runtime_state():
    async def _run() -> None:
        data_api._clear_data_api_runtime_caches()

        download_task = asyncio.create_task(_sleep_forever())
        live_refresh_task = asyncio.create_task(_sleep_forever())
        onchain_task = asyncio.create_task(_sleep_forever())
        factor_task = asyncio.create_task(_sleep_forever())
        fama_task = asyncio.create_task(_sleep_forever())

        data_api._REPLAY_SESSIONS["replay-1"] = {
            "created_monotonic": time.monotonic(),
            "last_access_monotonic": time.monotonic(),
        }
        data_api._DOWNLOAD_TASKS["download-1"] = {"status": "running"}
        data_api._DOWNLOAD_TASKS["download-2"] = {"status": "completed"}
        data_api._DOWNLOAD_BACKGROUND_TASKS["download-1"] = download_task
        data_api._LIVE_CACHE_REFRESH_TASKS["binance|BTC/USDT|1h"] = live_refresh_task
        data_api._ONCHAIN_OVERVIEW_CACHE["binance|BTC/USDT|10|Ethereum"] = {"payload": {"ok": True}}
        data_api._ONCHAIN_OVERVIEW_REFRESH_TASKS["binance|BTC/USDT|10|Ethereum"] = onchain_task
        data_api._FACTOR_LIBRARY_CACHE["factor-key"] = {"payload": {"ok": True}}
        data_api._FACTOR_LIBRARY_REFRESH_TASKS["factor-key"] = factor_task
        data_api._FACTOR_LIBRARY_REFRESH_META["factor-key"] = {"started_at": "2026-04-20T00:00:00Z"}
        data_api._FAMA_CACHE["fama-key"] = {"payload": {"ok": True}}
        data_api._FAMA_REFRESH_TASKS["fama-key"] = fama_task
        data_api._RESEARCH_COVERAGE_CACHE.update(
            {
                "path": "coverage.csv",
                "mtime": 123.0,
                "df": pd.DataFrame({"symbol": ["BTC/USDT"]}),
            }
        )
        data_api._DOWNLOAD_TASK_SEMAPHORE = asyncio.Semaphore(1)
        data_api._DOWNLOAD_TASK_SEMAPHORE_LOOP_ID = 123

        result = data_api._clear_data_api_runtime_caches()
        await asyncio.sleep(0)

        assert result["replay_sessions_cleared"] == 1
        assert result["download_tasks_cleared"] == 2
        assert result["active_download_tasks_cleared"] == 1
        assert result["completed_download_tasks_cleared"] == 1
        assert result["onchain_cache_entries_cleared"] == 1
        assert result["factor_cache_entries_cleared"] == 1
        assert result["fama_cache_entries_cleared"] == 1
        assert result["research_coverage_loaded"] is True
        assert result["download_background_tasks_cancelled"] == 1
        assert result["live_cache_refresh_tasks_cancelled"] == 1
        assert result["onchain_refresh_tasks_cancelled"] == 1
        assert result["factor_refresh_tasks_cancelled"] == 1
        assert result["fama_refresh_tasks_cancelled"] == 1
        assert download_task.cancelled() is True
        assert live_refresh_task.cancelled() is True
        assert onchain_task.cancelled() is True
        assert factor_task.cancelled() is True
        assert fama_task.cancelled() is True

        inspect = data_api._inspect_data_api_runtime_caches()
        assert inspect["replay_sessions"] == 0
        assert inspect["download_tasks"] == 0
        assert inspect["active_download_tasks"] == 0
        assert inspect["download_background_tasks"] == 0
        assert inspect["live_cache_refresh_tasks"] == 0
        assert inspect["onchain_cache_entries"] == 0
        assert inspect["onchain_refresh_tasks"] == 0
        assert inspect["factor_cache_entries"] == 0
        assert inspect["factor_refresh_tasks"] == 0
        assert inspect["factor_refresh_meta_entries"] == 0
        assert inspect["fama_cache_entries"] == 0
        assert inspect["fama_refresh_tasks"] == 0
        assert inspect["research_coverage_rows"] == 0
        assert inspect["download_semaphore_initialized"] is False

    asyncio.run(_run())


def test_schedule_live_cache_refresh_deduplicates_same_key():
    async def _run() -> None:
        data_api._LIVE_CACHE_REFRESH_TASKS.clear()
        calls: list[str] = []
        release = asyncio.Event()

        async def _worker() -> None:
            calls.append("run")
            await release.wait()

        first = data_api._schedule_live_cache_refresh("binance|BTC/USDT|1h", _worker)
        second = data_api._schedule_live_cache_refresh("binance|BTC/USDT|1h", _worker)
        await asyncio.sleep(0)

        assert first is True
        assert second is False
        assert calls == ["run"]
        assert data_api._pending_tasks_count(data_api._LIVE_CACHE_REFRESH_TASKS) == 1

        release.set()
        task = data_api._LIVE_CACHE_REFRESH_TASKS.get("binance|BTC/USDT|1h")
        if task is not None:
            await task
        await asyncio.sleep(0)

        assert data_api._LIVE_CACHE_REFRESH_TASKS == {}

    asyncio.run(_run())
