from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import data as data_api


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    return app


def _reset_download_state() -> None:
    data_api._DOWNLOAD_TASKS.clear()
    data_api._DOWNLOAD_BACKGROUND_TASKS.clear()
    data_api._DOWNLOAD_TASK_SEMAPHORE = None
    data_api._DOWNLOAD_TASK_SEMAPHORE_LOOP_ID = None


def test_download_route_honors_explicit_background_true_for_small_single_request(monkeypatch):
    _reset_download_state()
    queued_payloads: list[dict] = []

    def fake_queue(payload):
        queued_payloads.append(dict(payload))
        return {
            "task_id": "task-explicit-background",
            "status": "pending",
            "exchange": payload["exchange"],
            "symbol": payload["symbol"],
            "timeframe": payload["timeframe"],
            "days": payload["days"],
            "start_time": payload["start_time"].isoformat() if payload.get("start_time") else None,
            "end_time": payload["end_time"].isoformat() if payload.get("end_time") else None,
        }

    async def fake_run_download_historical_data(**kwargs):
        raise AssertionError(f"route should have queued instead of running inline: {kwargs}")

    monkeypatch.setattr(data_api, "_queue_download_task", fake_queue)
    monkeypatch.setattr(data_api, "run_download_historical_data", fake_run_download_historical_data)

    with TestClient(_build_app()) as client:
        response = client.post(
            "/api/data/download",
            params={
                "exchange": "binance",
                "symbol": "XML/USDT",
                "timeframe": "1h",
                "days": 30,
                "background": "true",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["queued"] is True
    assert payload["task_id"] == "task-explicit-background"
    assert payload["status"] == "pending"
    assert len(queued_payloads) == 1
    assert queued_payloads[0]["exchange"] == "binance"
    assert queued_payloads[0]["symbol"] == "XML/USDT"
    assert queued_payloads[0]["timeframe"] == "1h"
    assert queued_payloads[0]["days"] == 30


def test_download_route_runs_small_single_request_inline_when_background_unspecified(monkeypatch):
    _reset_download_state()
    run_calls: list[dict] = []

    def fake_queue(payload):
        raise AssertionError(f"route should have run inline instead of queuing: {payload}")

    async def fake_run_download_historical_data(**kwargs):
        run_calls.append(dict(kwargs))
        return {
            "exchange": kwargs["exchange"],
            "symbol": kwargs["symbol"],
            "timeframe": kwargs["timeframe"],
            "count": 180,
            "start": "2026-04-01T00:00:00",
            "end": "2026-04-08T11:00:00",
        }

    monkeypatch.setattr(data_api, "_queue_download_task", fake_queue)
    monkeypatch.setattr(data_api, "run_download_historical_data", fake_run_download_historical_data)

    with TestClient(_build_app()) as client:
        response = client.post(
            "/api/data/download",
            params={
                "exchange": "binance",
                "symbol": "XML/USDT",
                "timeframe": "1h",
                "days": 7,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 180
    assert len(run_calls) == 1
    assert run_calls[0]["exchange"] == "binance"
    assert run_calls[0]["symbol"] == "XML/USDT"
    assert run_calls[0]["timeframe"] == "1h"
    assert run_calls[0]["days"] == 7


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


def test_run_download_task_marks_timeout_as_failed(monkeypatch):
    _reset_download_state()
    task_id = "task-timeout"
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
        **data_api._download_task_progress_defaults(),
    }

    async def fake_run_download_historical_data(**kwargs):
        await asyncio.sleep(1.0)
        return {"count": 1}

    monkeypatch.setattr(data_api, "run_download_historical_data", fake_run_download_historical_data)
    monkeypatch.setattr(data_api, "_download_task_timeout_sec", lambda payload=None: 0.01)

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
    assert "timed out" in task["error"]
    assert task["result"]["error"] == task["error"]
    assert task["finished_at"] is not None
    assert task["progress"]["status"] == "failed"


def test_run_download_task_captures_live_progress(monkeypatch):
    _reset_download_state()
    task_id = "task-progress"
    data_api._DOWNLOAD_TASKS[task_id] = {
        "task_id": task_id,
        "status": "pending",
        "batch_id": "",
        "exchange": "binance",
        "symbol": "FET/USDT",
        "timeframe": "1h",
        "days": 30,
        "start_time": "2026-03-01T00:00:00",
        "end_time": "2026-03-31T23:59:59",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
        **data_api._download_task_progress_defaults(),
    }

    async def fake_run_download_historical_data(**kwargs):
        progress_callback = kwargs.get("progress_callback")
        assert callable(progress_callback)
        await progress_callback(
            SimpleNamespace(
                downloaded_candles=240,
                estimated_total_candles=720,
                total_candles=0,
                progress_pct=33.33,
                pages_fetched=2,
                retry_count=1,
                consecutive_errors=0,
                current_time=datetime(2026, 3, 10, 0, 0, 0),
                updated_at=datetime(2026, 3, 10, 0, 5, 0),
                last_success_at=datetime(2026, 3, 10, 0, 5, 0),
                last_error="",
                status="running",
                message="已抓取 240 根，继续下载",
                is_complete=False,
                started_at=datetime(2026, 3, 1, 0, 0, 0),
                finished_at=None,
            )
        )
        return {
            "exchange": kwargs["exchange"],
            "symbol": kwargs["symbol"],
            "timeframe": kwargs["timeframe"],
            "count": 720,
            "start": "2026-03-01T00:00:00",
            "end": "2026-03-31T23:00:00",
            "message": "下载完成",
        }

    monkeypatch.setattr(data_api, "run_download_historical_data", fake_run_download_historical_data)

    asyncio.run(
        data_api._run_download_task(
            task_id,
            {
                "exchange": "binance",
                "symbol": "FET/USDT",
                "timeframe": "1h",
                "days": 30,
            },
        )
    )

    task = data_api._DOWNLOAD_TASKS[task_id]
    assert task["status"] == "completed"
    assert task["error"] is None
    assert task["downloaded_candles"] == 720
    assert task["total_candles"] == 720
    assert task["estimated_total_candles"] == 720
    assert task["progress_pct"] == 100.0
    assert task["pages_fetched"] == 2
    assert task["retry_count"] == 1
    assert task["status_message"] == "下载完成"
    assert task["progress"]["status"] == "completed"


def test_run_download_historical_data_falls_back_to_coinglass(monkeypatch):
    captured = {}

    async def fake_download_historical_klines(**kwargs):
        raise RuntimeError(f"{kwargs['exchange']} upstream failed")

    async def fake_save_df_to_parquet(exchange, symbol, timeframe, df):
        captured["exchange"] = exchange
        captured["symbol"] = symbol
        captured["timeframe"] = timeframe
        captured["rows"] = int(len(df.index))

    monkeypatch.setattr(data_api.exchange_manager, "get_exchange", lambda exchange: object() if exchange in {"binance", "gate"} else None)
    monkeypatch.setattr(data_api.historical_data_manager, "download_historical_klines", fake_download_historical_klines)
    monkeypatch.setattr(data_api, "_save_df_to_parquet", fake_save_df_to_parquet)
    monkeypatch.setattr(data_api, "coinglass_enabled", lambda: True)

    class _FakeCoinglassClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None

        async def request_dataset(self, manifest, **kwargs):
            return {
                "params": dict(kwargs),
                "payload": {
                    "data": [
                        {"t": 1772323200000, "o": 1.0, "h": 1.2, "l": 0.9, "c": 1.1, "v": 100},
                        {"t": 1772326800000, "o": 1.1, "h": 1.3, "l": 1.0, "c": 1.25, "v": 120},
                    ]
                },
            }

    monkeypatch.setattr(data_api, "get_coinglass_manifest", lambda dataset: SimpleNamespace(dataset="price_history", routes=()))
    monkeypatch.setattr(data_api, "CoinglassClient", _FakeCoinglassClient)
    monkeypatch.setattr(
        data_api,
        "normalize_dataset_response",
        lambda **kwargs: {"rows": list(kwargs["response_payload"]["data"]), "error": "", "status": "ok"},
    )

    result = asyncio.run(
        data_api.run_download_historical_data(
            exchange="binance",
            symbol="RENDER/USDT",
            timeframe="1h",
            start_time=datetime(2026, 3, 1, 0, 0, 0),
            end_time=datetime(2026, 3, 1, 1, 0, 0),
        )
    )

    assert result["count"] == 2
    assert result["source"] == "coinglass"
    assert result["source_exchange"] == "binance"
    assert captured == {
        "exchange": "binance",
        "symbol": "RENDER/USDT",
        "timeframe": "1h",
        "rows": 2,
    }


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
