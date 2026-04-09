from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import trading_runtime
from web.services import trading_runtime_service


def test_get_trading_mode_does_not_expose_pending_token(monkeypatch):
    app = FastAPI()
    app.include_router(trading_runtime.router, prefix="/api/trading")
    client = TestClient(app)

    monkeypatch.setattr(trading_runtime.execution_engine, "get_trading_mode", lambda: "live")
    monkeypatch.setattr(trading_runtime.execution_engine, "is_paper_mode", lambda: False)
    monkeypatch.setattr(
        trading_runtime,
        "list_pending_mode_switches",
        lambda: [
            {
                "target_mode": "live",
                "reason": "verify",
                "created_at": "2026-04-09T14:00:00+00:00",
                "expires_at": "2026-04-09T14:05:00+00:00",
            }
        ],
    )
    monkeypatch.setattr(trading_runtime, "get_mode_confirm_text", lambda: "CONFIRM LIVE TRADING")

    response = client.get("/api/trading/mode")
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "live"
    assert payload["pending_switches"] == [
        {
            "target_mode": "live",
            "reason": "verify",
            "created_at": "2026-04-09T14:00:00+00:00",
            "expires_at": "2026-04-09T14:05:00+00:00",
        }
    ]
    assert "token" not in payload["pending_switches"][0]


def test_list_pending_mode_switches_omits_token_by_default():
    result = trading_runtime_service.request_mode_switch(
        target_mode="live",
        current_mode="paper",
        reason="verify",
    )
    token = result["token"]
    try:
        default_payload = trading_runtime_service.list_pending_mode_switches()
        explicit_payload = trading_runtime_service.list_pending_mode_switches(include_token=True)

        assert len(default_payload) == 1
        assert "token" not in default_payload[0]
        assert explicit_payload[0]["token"] == token
    finally:
        trading_runtime_service.cancel_mode_switch(token)
