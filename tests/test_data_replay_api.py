from __future__ import annotations

import asyncio

import pandas as pd
import pytest
from fastapi import HTTPException

from web.api import data as data_api


def _reset_replay_state() -> None:
    data_api._REPLAY_SESSIONS.clear()


def _sample_replay_frame() -> pd.DataFrame:
    index = pd.date_range("2026-04-20T00:00:00", periods=3, freq="1H")
    return pd.DataFrame(
        {
            "open": [1.0, 1.1, 1.2],
            "high": [1.1, 1.2, 1.3],
            "low": [0.9, 1.0, 1.1],
            "close": [1.05, 1.15, 1.25],
            "volume": [100.0, 120.0, 140.0],
        },
        index=index,
    )


def test_prune_replay_sessions_removes_expired_and_oldest_entries(monkeypatch):
    _reset_replay_state()
    monkeypatch.setattr(data_api, "_REPLAY_SESSION_TTL_SEC", 60.0)
    monkeypatch.setattr(data_api, "_REPLAY_SESSION_MAX_ACTIVE", 2)
    data_api._REPLAY_SESSIONS.update(
        {
            "expired": {"created_monotonic": 0.0, "last_access_monotonic": -20.0},
            "oldest": {"created_monotonic": 10.0, "last_access_monotonic": 10.0},
            "recent": {"created_monotonic": 30.0, "last_access_monotonic": 30.0},
            "keep": {"created_monotonic": 40.0, "last_access_monotonic": 40.0},
        }
    )

    result = data_api._prune_replay_sessions(now_monotonic=50.0, keep_id="keep")

    assert result == {
        "expired_removed": ["expired"],
        "overflow_removed": ["oldest"],
    }
    assert set(data_api._REPLAY_SESSIONS) == {"recent", "keep"}


def test_get_replay_status_rejects_expired_session(monkeypatch):
    _reset_replay_state()
    monkeypatch.setattr(data_api, "_REPLAY_SESSION_TTL_SEC", 30.0)
    data_api._REPLAY_SESSIONS["expired-session"] = {
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "timeframe": "1h",
        "window": 100,
        "speed": 1.0,
        "data": _sample_replay_frame(),
        "cursor": 0,
        "started_at": "2026-04-20T00:00:00+00:00",
        "created_monotonic": 1.0,
        "last_access_monotonic": 1.0,
    }

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(data_api.get_replay_status("expired-session"))

    assert exc_info.value.status_code == 404
    assert "expired-session" not in data_api._REPLAY_SESSIONS
