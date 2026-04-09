from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import data as data_api


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(data_api.router, prefix="/api/data")
    return app


def test_research_refresh_status_route_returns_helper_payload(monkeypatch):
    def fake_status():
        return {
            "task": {"exists": True, "state": "Ready", "state_label": "待命"},
            "summary": {"exists": True, "downloaded_rows_total": 1234},
        }

    monkeypatch.setattr(data_api, "_get_research_universe_refresh_status_sync", fake_status)

    with TestClient(_build_app()) as client:
        response = client.get("/api/data/research/refresh/status")

    assert response.status_code == 200
    assert response.json()["task"]["state_label"] == "待命"
    assert response.json()["summary"]["downloaded_rows_total"] == 1234


def test_research_refresh_start_route_bridges_to_trigger(monkeypatch):
    async def fake_trigger(exchange: str = "binance", timeframes: str = "15m,1h", days: int = 90, overlap_bars: int = 48):
        return {
            "accepted": True,
            "message": "研究币池增量追平已触发",
            "task": {
                "exists": True,
                "state": "Running",
                "state_label": "运行中",
            },
            "summary": {
                "exists": True,
                "downloaded_rows_total": 4321,
            },
            "echo": {
                "exchange": exchange,
                "timeframes": timeframes,
                "days": days,
                "overlap_bars": overlap_bars,
            },
        }

    monkeypatch.setattr(data_api, "_trigger_research_universe_refresh_start", fake_trigger)

    with TestClient(_build_app()) as client:
        response = client.post(
            "/api/data/research/refresh/start",
            params={"exchange": "binance", "timeframes": "15m,1h", "days": 120, "overlap_bars": 64},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted"] is True
    assert payload["task"]["state_label"] == "运行中"
    assert payload["summary"]["downloaded_rows_total"] == 4321
    assert payload["echo"] == {
        "exchange": "binance",
        "timeframes": "15m,1h",
        "days": 120,
        "overlap_bars": 64,
    }


def test_research_refresh_start_route_defaults_include_1h(monkeypatch):
    async def fake_trigger(exchange: str = "binance", timeframes: str = "1m,5m,15m,1h", days: int = 90, overlap_bars: int = 48):
        return {
            "accepted": True,
            "task": {"exists": True, "state": "Running", "state_label": "运行中"},
            "summary": {"exists": True},
            "echo": {
                "exchange": exchange,
                "timeframes": timeframes,
                "days": days,
                "overlap_bars": overlap_bars,
            },
        }

    monkeypatch.setattr(data_api, "_trigger_research_universe_refresh_start", fake_trigger)

    with TestClient(_build_app()) as client:
        response = client.post("/api/data/research/refresh/start")

    assert response.status_code == 200
    payload = response.json()
    assert payload["echo"]["exchange"] == "binance"
    assert payload["echo"]["timeframes"] == "1m,5m,15m,1h"
    assert payload["echo"]["days"] == 90
    assert payload["echo"]["overlap_bars"] == 48
