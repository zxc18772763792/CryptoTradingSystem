from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import data as data_api


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    return app


def _reset_download_state() -> None:
    data_api._DOWNLOAD_TASKS.clear()
    data_api._DOWNLOAD_TASK_SEMAPHORE = None
    data_api._DOWNLOAD_TASK_SEMAPHORE_LOOP_ID = None


def test_run_download_task_marks_embedded_error_as_failed(monkeypatch):
    _reset_download_state()
    task_id = "task-embedded-error"
    data_api._DOWNLOAD_TASKS[task_id] = {
        "task_id": task_id,
        "status": "pending",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "timeframe": "1h",
        "days": 30,
        "start_time": None,
        "end_time": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
    }

    async def fake_run_download_historical_data(**kwargs):
        return {
            "exchange": kwargs["exchange"],
            "symbol": kwargs["symbol"],
            "timeframe": kwargs["timeframe"],
            "count": 0,
            "error": "simulated upstream failure",
        }

    monkeypatch.setattr(data_api, "run_download_historical_data", fake_run_download_historical_data)

    asyncio.run(
        data_api._run_download_task(
            task_id,
            {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "1h",
                "days": 30,
            },
        )
    )

    task = data_api._DOWNLOAD_TASKS[task_id]
    assert task["status"] == "failed"
    assert task["error"] == "simulated upstream failure"
    assert task["result"]["error"] == "simulated upstream failure"


def test_list_download_tasks_can_filter_specific_ids_beyond_default_limit():
    _reset_download_state()
    base = datetime(2026, 4, 1, tzinfo=timezone.utc)
    requested_ids = ["task-000", "task-149"]
    for idx in range(150):
        task_id = f"task-{idx:03d}"
        data_api._DOWNLOAD_TASKS[task_id] = {
            "task_id": task_id,
            "status": "completed",
            "batch_id": "batch-demo",
            "exchange": "binance",
            "symbol": f"SYM{idx}/USDT",
            "timeframe": "1h",
            "days": 30,
            "start_time": None,
            "end_time": None,
            "created_at": (base + timedelta(seconds=idx)).isoformat(),
            "started_at": None,
            "finished_at": None,
            "result": {"count": idx},
            "error": None,
        }

    with TestClient(_build_app()) as client:
        default_resp = client.get("/api/data/download/tasks")
        assert default_resp.status_code == 200
        assert default_resp.json()["count"] == 100

        filtered_resp = client.get(
            "/api/data/download/tasks",
            params={"task_ids": ",".join(requested_ids)},
        )

    assert filtered_resp.status_code == 200
    payload = filtered_resp.json()
    assert payload["count"] == 2
    assert [task["task_id"] for task in payload["tasks"]] == ["task-149", "task-000"]
