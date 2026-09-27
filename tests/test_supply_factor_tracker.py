from __future__ import annotations

import asyncio
import json

import pandas as pd
import pytest

from core.research import supply_factor_tracker as sf
from core.research import unlock_short_tracker as ut


def _schedule(daily_unlock: float, start="2026-01-01", days=500):
    base = pd.Timestamp(start, tz="UTC")
    return {"documentedData": {"data": [{"label": "all", "data": [
        {"timestamp": int((base + pd.Timedelta(days=i)).timestamp()), "unlocked": 1_000_000 + daily_unlock * i} for i in range(days)
    ]}]}}


def test_supply_growth_and_tercile_legs():
    day = pd.Timestamp("2026-10-01", tz="UTC")
    g = sf.supply_growth(_schedule(1000.0), day)
    unlocked_now = 1_000_000 + 1000.0 * (day - pd.Timestamp("2026-01-01", tz="UTC")).days
    assert g == pytest.approx(90_000 / unlocked_now)
    assert sf.supply_growth(_schedule(1000.0, days=200), day) is None  # schedule does not cover the horizon
    legs = sf.assign_legs({f"T{i}": float(i) for i in range(9)})
    assert legs == {"long": ["T0", "T1", "T2"], "short": ["T6", "T7", "T8"]}


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code, self.text = payload, status, json.dumps(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class World:
    """12 tokens; T0..T11 release 0..11 x 1000 tokens/day. Low-supply coins rise 10%, high-supply fall 10%."""

    def __init__(self):
        self.tokens = [f"T{i}" for i in range(12)]

    def price(self, token, day):
        i = int(token[1:])
        drift = 0.10 if i < 4 else (-0.10 if i >= 8 else 0.0)
        return 1.0 * (1 + drift) if day >= pd.Timestamp("2026-10-31", tz="UTC") else 1.0

    async def get(self, url, params=None):
        params = params or {}
        if url.endswith("/emissionsIndex"):
            return _Resp({"data": [{"protocolSlug": t.lower(), "tokenPrice": [{"symbol": t, "price": 1.0}]} for t in self.tokens]})
        if "/emissions/" in url:
            i = int(url.rsplit("/", 1)[-1][1:])
            return _Resp(_schedule(1000.0 * i))
        if url.endswith("/ticker/price"):
            return _Resp([{"symbol": f"{t}USDT", "price": "1.0"} for t in self.tokens])
        if url.endswith("/klines"):
            day = pd.Timestamp(params["startTime"], unit="ms", tz="UTC")
            token = params["symbol"][:-4]
            return _Resp([[int(day.timestamp() * 1000), "0", "0", "0", str(self.price(token, day))]])
        raise AssertionError(url)


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setattr(ut, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(ut, "SCHEDULE_TTL_SEC", 0)
    monkeypatch.setattr(ut, "INDEX_TTL_SEC", 0)
    return World()


def test_month_opens_after_first_close_then_settles_with_the_spread(world, tmp_path):
    state_path = tmp_path / "state.json"
    sf.save_state({"started_at": "2026-09-27T00:00:00+00:00", "months": {}}, state_path)

    asyncio.run(sf.tick(world, state_path, now=pd.Timestamp("2026-10-01 12:00", tz="UTC")))
    assert json.loads(state_path.read_text(encoding="utf-8"))["months"] == {}  # Oct 1 close not final yet

    asyncio.run(sf.tick(world, state_path, now=pd.Timestamp("2026-10-02 01:00", tz="UTC")))
    month = json.loads(state_path.read_text(encoding="utf-8"))["months"]["2026-10-01"]
    assert month["backfill"] is False and month["status"] == "open"
    assert set(month["long"]) == {"T0", "T1", "T2", "T3"} and set(month["short"]) == {"T8", "T9", "T10", "T11"}

    summary = asyncio.run(sf.tick(world, state_path, now=pd.Timestamp("2026-11-01 01:00", tz="UTC")))
    month = json.loads(state_path.read_text(encoding="utf-8"))["months"]["2026-10-01"]
    assert month["status"] == "closed"
    assert month["spread_pct"] == pytest.approx((0.10 - (-0.10) - sf.MONTH_COST) * 100)
    assert summary["forward_completed"] == 1 and summary["forward_mean_spread_pct"] == pytest.approx(19.6)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["retirement_rule"]["min_n"] == 12  # rule registered on the first tick, before any result
    assert summary["retirement"]["verdict"] == "collecting" and summary["retirement"]["n"] == 1


def test_month_started_before_tracker_is_backfill(world, tmp_path):
    state_path = tmp_path / "state.json"
    sf.save_state({"started_at": "2026-10-15T00:00:00+00:00", "months": {}}, state_path)
    summary = asyncio.run(sf.tick(world, state_path, now=pd.Timestamp("2026-10-15 12:00", tz="UTC")))
    month = json.loads(state_path.read_text(encoding="utf-8"))["months"]["2026-10-01"]
    assert month["backfill"] is True
    assert summary["forward_open"] == 0
