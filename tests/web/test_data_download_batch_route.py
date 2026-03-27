from __future__ import annotations

import asyncio
from datetime import datetime


def test_batch_download_route_normalizes_symbols_and_queues_tasks(monkeypatch):
    from web.api import data as data_api

    queued_payloads: list[dict] = []

    def fake_queue(payload):
        queued_payloads.append(dict(payload))
        return {
            "task_id": f"task-{len(queued_payloads)}",
            "status": "pending",
            "exchange": payload["exchange"],
            "symbol": payload["symbol"],
            "timeframe": payload["timeframe"],
            "days": payload["days"],
            "start_time": payload["start_time"].isoformat() if payload.get("start_time") else None,
            "end_time": payload["end_time"].isoformat() if payload.get("end_time") else None,
        }

    monkeypatch.setattr(data_api, "_queue_download_task", fake_queue)

    req = data_api.BatchDownloadRequest(
        exchange="BINANCE",
        symbols=[" btcusdt ", "ETH/USDT", "ethusdt", "", "BTC/USDT"],
        timeframe="4h",
        days=120,
        start_time=datetime(2024, 1, 1, 0, 0, 0),
        end_time=datetime(2024, 2, 1, 0, 0, 0),
        background=True,
    )

    payload = asyncio.run(data_api.download_historical_data_batch(req))

    assert payload["queued"] is True
    assert payload["exchange"] == "binance"
    assert payload["timeframe"] == "4h"
    assert payload["days"] == 120
    assert payload["start_time"] == "2024-01-01T00:00:00"
    assert payload["end_time"] == "2024-02-01T00:00:00"
    assert payload["symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert payload["task_count"] == 2
    assert payload["task_ids"] == ["task-1", "task-2"]
    assert [item["symbol"] for item in queued_payloads] == ["BTC/USDT", "ETH/USDT"]
    assert all(item["exchange"] == "binance" for item in queued_payloads)
    assert all(item["timeframe"] == "4h" for item in queued_payloads)
    assert all(item["days"] == 120 for item in queued_payloads)
    assert all(item["start_time"] == datetime(2024, 1, 1, 0, 0, 0) for item in queued_payloads)
    assert all(item["end_time"] == datetime(2024, 2, 1, 0, 0, 0) for item in queued_payloads)


def test_batch_download_route_runs_single_symbol_inline_when_background_disabled(monkeypatch):
    from web.api import data as data_api

    run_calls: list[dict] = []

    async def fake_run_download_historical_data(**kwargs):
        run_calls.append(dict(kwargs))
        return {
            "exchange": kwargs["exchange"],
            "symbol": kwargs["symbol"],
            "timeframe": kwargs["timeframe"],
            "count": 321,
            "start": "2024-03-01T00:00:00",
            "end": "2024-03-31T23:59:59",
        }

    monkeypatch.setattr(data_api, "run_download_historical_data", fake_run_download_historical_data)

    req = data_api.BatchDownloadRequest(
        exchange="OKX",
        symbols=[" solusdt "],
        timeframe="1h",
        days=45,
        background=False,
    )

    payload = asyncio.run(data_api.download_historical_data_batch(req))

    assert payload["queued"] is False
    assert payload["exchange"] == "okx"
    assert payload["symbols"] == ["SOL/USDT"]
    assert payload["task_count"] == 0
    assert payload["task_ids"] == []
    assert payload["results"][0]["count"] == 321
    assert len(run_calls) == 1
    assert run_calls[0]["exchange"] == "okx"
    assert run_calls[0]["symbol"] == "SOL/USDT"
    assert run_calls[0]["timeframe"] == "1h"
    assert run_calls[0]["days"] == 45

