"""Tests for the pump-precursor watchlist radar routes."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

import web.api.altcoin as altcoin_api


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(altcoin_api.router, prefix="/api/altcoin")
    return TestClient(app)


def test_pump_watchlist_reports_missing_file(monkeypatch, tmp_path):
    monkeypatch.setattr(altcoin_api.pump, "_PUMP_WATCHLIST_DIR", tmp_path)
    response = _client().get("/api/altcoin/radar/pump-watchlist")
    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is False
    assert payload["reason"] == "not_generated"


def test_pump_watchlist_serves_fresh_payload(monkeypatch, tmp_path):
    monkeypatch.setattr(altcoin_api.pump, "_PUMP_WATCHLIST_DIR", tmp_path)
    generated = datetime.now(timezone.utc) - timedelta(days=1)
    (tmp_path / "latest.json").write_text(
        json.dumps(
            {
                "generated_at": generated.isoformat(),
                "top": [{"rank": 1, "base": "TEST", "score": 0.42}],
                "universe_size": 100,
            }
        ),
        encoding="utf-8",
    )
    payload = _client().get("/api/altcoin/radar/pump-watchlist").json()
    assert payload["available"] is True
    assert payload["stale"] is False
    assert 0.9 < payload["age_days"] < 1.1
    assert payload["data"]["top"][0]["base"] == "TEST"


def test_pump_watchlist_flags_stale_payload(monkeypatch, tmp_path):
    monkeypatch.setattr(altcoin_api.pump, "_PUMP_WATCHLIST_DIR", tmp_path)
    generated = datetime.now(timezone.utc) - timedelta(days=12)
    (tmp_path / "latest.json").write_text(
        json.dumps({"generated_at": generated.isoformat(), "top": []}),
        encoding="utf-8",
    )
    payload = _client().get("/api/altcoin/radar/pump-watchlist").json()
    assert payload["available"] is True
    assert payload["stale"] is True
