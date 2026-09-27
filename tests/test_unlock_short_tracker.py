from __future__ import annotations

import asyncio
import json

import pandas as pd
import pytest

from core.research import unlock_short_tracker as ut
from core.research.unlock_events import cliff_events, unlocked_series

UNLOCK = pd.Timestamp("2026-11-15", tz="UTC")


def _schedule(cliff_ts=None):
    """Linear 1000/day for a year, plus one cliff of 200k (= ~50% of unlocked)."""
    start = pd.Timestamp("2026-01-01", tz="UTC")
    pts, total = [], 0.0
    for i in range(420):
        day = start + pd.Timedelta(days=i)
        total += 1000.0
        if cliff_ts is not None and day == cliff_ts:
            total += 200_000.0
        pts.append({"timestamp": int(day.timestamp()), "unlocked": total})
    return {"documentedData": {"data": [{"label": "all", "data": pts}]}}


def _index_entry(ticker, slug, price, cliff_ts=None):
    events = [{"timestamp": int(cliff_ts.timestamp()), "cliffAllocations": [{"category": "insiders", "amount": 200_000.0}]}] if cliff_ts is not None else []
    return {"protocolSlug": slug, "tokenPrice": [{"symbol": ticker, "price": price}], "unlockEvents": events}


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status
        self.text = json.dumps(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class FakeWorld:
    def __init__(self):
        self.now = pd.Timestamp("2026-10-01 01:00", tz="UTC")
        self.cliff = {"AAA": UNLOCK, "BBB": UNLOCK, "CCC": None}
        self.paths = {}  # symbol -> function(day) -> (high, close)

    async def get(self, url, params=None):
        params = params or {}
        if url.endswith("/emissionsIndex"):
            data = [_index_entry("AAA", "aaa", 1.0, self.cliff["AAA"]), _index_entry("BBB", "bbb", 1.0, self.cliff["BBB"])]
            data += [_index_entry(f"H{i}", f"h{i}", 1.0) for i in range(6)]
            return _Resp({"data": data})
        if "/emissions/" in url:
            slug = url.rsplit("/", 1)[-1]
            return _Resp(_schedule(self.cliff.get(slug.upper())))
        if url.endswith("/exchangeInfo"):
            syms = ["AAAUSDT", "1000BBBUSDT"] + [f"H{i}USDT" for i in range(6)]
            return _Resp({"symbols": [{"symbol": s, "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "TRADING"} for s in syms]})
        if url.endswith("/ticker/price"):
            return _Resp([{"symbol": "AAAUSDT", "price": "1.0"}, {"symbol": "1000BBBUSDT", "price": "1000.0"}]
                         + [{"symbol": f"H{i}USDT", "price": "1.0"} for i in range(6)])
        if url.endswith("/klines"):
            start = pd.Timestamp(params["startTime"], unit="ms", tz="UTC")
            path = self.paths.get(params["symbol"], lambda d: (1.0, 1.0))
            days = pd.date_range(start, self.now.normalize(), freq="D")  # today's bar is still forming
            return _Resp([[int(d.timestamp() * 1000), "0", str(path(d)[0]), "0", str(path(d)[1])] for d in days])
        if url.endswith("/fundingRate"):
            return _Resp([])
        raise AssertionError(url)


@pytest.fixture
def world(tmp_path, monkeypatch):
    w = FakeWorld()
    monkeypatch.setattr(ut, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(ut, "SCHEDULE_TTL_SEC", 0)  # re-read schedules every pass (tests change them)
    monkeypatch.setattr(ut, "INDEX_TTL_SEC", 0)
    monkeypatch.setattr(ut, "_now", lambda: w.now)
    return w


def run(world, tmp_path):
    return asyncio.run(ut.tick(world, state_path=tmp_path / "state.json"))


def trades(tmp_path):
    return json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))["trades"]


def test_full_lifecycle_entry_close_and_stop(world, tmp_path):
    entry_day = UNLOCK - pd.Timedelta(days=30)
    # AAA falls 20% after entry; 1000BBB spikes +50% intraday a week after entry.
    world.paths["AAAUSDT"] = lambda d: (1.0, 1.0 if d <= entry_day else 0.8)
    world.paths["1000BBBUSDT"] = lambda d: (1500.0 if d == entry_day + pd.Timedelta(days=7) else 1000.0, 1000.0)

    run(world, tmp_path)
    t = trades(tmp_path)
    assert set(t) == {"AAA|2026-11-15", "BBB|2026-11-15"}
    assert all(x["status"] == "scheduled" and x["late"] is False for x in t.values())
    assert t["BBB|2026-11-15"]["symbol"] == "1000BBBUSDT"  # 1000x contract matched via price scale

    world.now = entry_day + pd.Timedelta(hours=12)  # entry day not finished: no entry yet
    run(world, tmp_path)
    assert trades(tmp_path)["AAA|2026-11-15"]["status"] == "scheduled"

    world.now = entry_day + pd.Timedelta(days=1, hours=1)
    run(world, tmp_path)
    aaa = trades(tmp_path)["AAA|2026-11-15"]
    assert aaa["status"] == "open" and aaa["entry_price"] == 1.0
    assert len(aaa["basket"]) == 6  # both unlock tokens excluded from the hedge basket

    world.now = UNLOCK + pd.Timedelta(hours=1)  # exit day (unlock - 1) has closed
    summary = run(world, tmp_path)
    aaa, bbb = trades(tmp_path)["AAA|2026-11-15"], trades(tmp_path)["BBB|2026-11-15"]
    assert aaa["status"] == "closed"
    assert aaa["short_return_pct"] == pytest.approx((0.2 - ut.LEG_COST) * 100, abs=1e-6)
    assert aaa["hedged_return_pct"] == pytest.approx((0.2 - 2 * ut.LEG_COST) * 100, abs=1e-6)  # flat basket
    assert bbb["status"] == "stopped"
    assert bbb["exit_price"] == pytest.approx(1000 * 1.4 * 1.02)
    assert summary["forward_completed"] == 2


def test_schedule_change_closes_the_trade_and_late_discoveries_are_flagged(world, tmp_path):
    world.now = UNLOCK - pd.Timedelta(days=10)  # already past the t-30 entry day
    run(world, tmp_path)
    t = trades(tmp_path)["AAA|2026-11-15"]
    assert t["late"] is True and t["entry_day"] == str((UNLOCK - pd.Timedelta(days=10)).date())

    world.now += pd.Timedelta(days=1, hours=1)
    run(world, tmp_path)
    assert trades(tmp_path)["AAA|2026-11-15"]["status"] == "open"

    world.cliff["AAA"] = None  # DefiLlama no longer lists the unlock
    world.now += pd.Timedelta(days=2)
    summary = run(world, tmp_path)
    assert trades(tmp_path)["AAA|2026-11-15"]["status"] == "schedule_changed"
    assert summary["forward_completed"] == 0 and summary["late_trades"] == 2  # late trades never count


def test_shared_cliff_parser_matches_expected_size():
    events = cliff_events(_index_entry("AAA", "aaa", 1.0, UNLOCK), unlocked_series(_schedule(UNLOCK)), 10)
    assert len(events) == 1 and events[0]["insider"] is True
    unlocked_before = 1000.0 * (UNLOCK - pd.Timestamp("2026-01-01", tz="UTC")).days
    assert events[0]["size_pct"] == pytest.approx(200_000 / unlocked_before * 100)
