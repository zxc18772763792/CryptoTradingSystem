"""Failure handling of the paper trackers and their runner (2026-09-28 review).

Transient exchange failures must be retried, never booked as "no data" or zero
funding; heavy schedule parsing must be cached; the runner must isolate jobs
and never run two passes at once.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import pytest

from core.research import delist_risk
from core.research import exchange_research_runner as runner
from core.research import supply_factor_tracker as sf
from core.research import upbit_caution_tracker as uc
from core.research import unlock_short_tracker as ut


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status
        self.text = json.dumps(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_checked_json_separates_delisted_from_transient():
    assert ut._checked_json(_Resp({"code": -1121}, 400)) == []  # invalid symbol: genuinely no data
    with pytest.raises(RuntimeError):
        ut._checked_json(_Resp({}, 503))
    with pytest.raises(RuntimeError):
        ut._checked_json(_Resp({}, 429))


def test_schedule_sidecar_is_reused_and_rebuilt_when_raw_changes(tmp_path):
    raw = tmp_path / "tok.json"

    def write(total):
        raw.write_text(json.dumps({"documentedData": {"data": [{"label": "a", "data": [
            {"timestamp": 1_700_000_000 + i * 86400, "unlocked": total + i} for i in range(3)]}]}}), encoding="utf-8")

    write(100.0)
    first = ut._parsed_unlocked(raw)
    side = raw.with_suffix(".unlocked.json")
    assert side.exists() and list(first) == [100.0, 101.0, 102.0]
    raw.write_text("not json anymore", encoding="utf-8")
    os.utime(raw, (time.time() - 3600, time.time() - 3600))  # raw older than sidecar: sidecar is used
    assert list(ut._parsed_unlocked(raw)) == [100.0, 101.0, 102.0]
    write(500.0)
    os.utime(raw, (time.time() + 60, time.time() + 60))  # raw newer: rebuilt
    assert list(ut._parsed_unlocked(raw)) == [500.0, 501.0, 502.0]


def test_supply_month_stays_open_on_transient_failure_then_closes_after_grace():
    month = {"exit_day": "2026-10-31", "status": "open", "symbols": {"A": "AUSDT", "B": "BUSDT"},
             "long": {"A": {"entry": 1.0}}, "short": {"B": {"entry": 1.0}}}

    class Client:
        def __init__(self):
            self.fail_b = True

        async def get(self, url, params=None):
            if params["symbol"] == "BUSDT" and self.fail_b:
                return _Resp({}, 502)
            day = params["startTime"]
            return _Resp([[day, "1", "1", "1", "1.1" if params["symbol"] == "AUSDT" else "0.9"]])

    client = Client()
    asyncio.run(sf._close_month(client, month, pd.Timestamp("2026-11-01 02:00", tz="UTC")))
    assert month["status"] == "open" and month["long"]["A"]["exit"] == pytest.approx(1.1)  # kept, B retried later
    client.fail_b = False
    asyncio.run(sf._close_month(client, month, pd.Timestamp("2026-11-01 08:00", tz="UTC")))
    assert month["status"] == "closed" and month["spread_pct"] == pytest.approx((0.1 + 0.1 - sf.MONTH_COST) * 100)

    client.fail_b = True
    late = {**month, "status": "open", "symbols": {"A": "AUSDT", "B": "BUSDT", "C": "CUSDT"},
            "long": {"A": {"entry": 1.0}}, "short": {"B": {"entry": 1.0}, "C": {"entry": 1.0}}}
    asyncio.run(sf._close_month(client, late, pd.Timestamp("2026-11-06 00:00", tz="UTC")))  # past the grace period
    assert late["status"] == "closed" and late["short"]["B"]["exit"] is None  # closed with the prices that exist

    empty = {**month, "status": "open", "long": {"A": {"entry": 1.0}}, "short": {"B": {"entry": 1.0}}}
    asyncio.run(sf._close_month(client, empty, pd.Timestamp("2026-11-06 00:00", tz="UTC")))
    assert empty["status"] == "unresolved"  # a whole leg missing: stop polling, never invent a result


class _UpbitWorld:
    """One caution notice; klines/funding behaviour configurable per test."""

    def __init__(self, klines_status=200, funding_status=200, funding_rows=None):
        self.klines_status, self.funding_status = klines_status, funding_status
        self.funding_rows = funding_rows or []
        self.day0 = int(datetime(2026, 10, 5, tzinfo=timezone.utc).timestamp() * 1000)

    async def get(self, url, params=None, headers=None):
        params = params or {}
        if url.endswith("/exchangeInfo"):
            return _Resp({"symbols": [{"symbol": "AAAUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT",
                                       "onboardDate": 1, "status": "TRADING"}]})
        if url == uc.UPBIT:
            notices = [{"id": 1, "title": "에이(AAA) 거래 유의 종목 지정 안내", "first_listed_at": "2026-10-05T15:00:00+09:00"}]
            return _Resp({"data": {"notices": notices if params.get("page") == 1 else []}})
        if url.startswith(uc.SPOT):
            return _Resp([[params["startTime"], "1", "1", "1", "1"]])
        if url.endswith("/klines"):
            if self.klines_status != 200:
                return _Resp({}, self.klines_status)
            return _Resp([[self.day0 + k * uc.DAY_MS, "1", "1", "1", "1" if k == 0 else "0.9"] for k in range(9)])
        if url.endswith("/fundingRate"):
            return _Resp(self.funding_rows if self.funding_status == 200 else {}, self.funding_status)
        raise AssertionError(url)


def _run_upbit(world, tmp_path, monkeypatch):
    monkeypatch.setattr(uc, "_now", lambda: datetime(2026, 10, 14, tzinfo=timezone.utc))
    path = tmp_path / "state.json"
    uc.save_state({"started_at": "2026-10-01T00:00:00+00:00", "trades": {}}, path)
    asyncio.run(uc.tick(world, state_path=path))
    return json.loads(path.read_text(encoding="utf-8"))["trades"]["AAA|1"]


def test_upbit_failed_funding_request_never_books_zero_funding(tmp_path, monkeypatch):
    trade = _run_upbit(_UpbitWorld(funding_status=503), tmp_path, monkeypatch)
    assert trade["status"] == "waiting_entry" and "return_pct" not in trade  # retried next pass


def test_upbit_counts_every_hourly_funding_settlement(tmp_path, monkeypatch):
    world = _UpbitWorld()
    entry = world.day0 + uc.DAY_MS
    world.funding_rows = [{"fundingTime": entry + h * 3_600_000, "fundingRate": "-0.0001"} for h in range(1, 169)]
    trade = _run_upbit(world, tmp_path, monkeypatch)
    assert trade["status"] == "closed" and trade["funding"] == pytest.approx(-0.0168)


def test_upbit_delisted_contract_is_marked_not_polled_forever(tmp_path, monkeypatch):
    trade = _run_upbit(_UpbitWorld(klines_status=400), tmp_path, monkeypatch)
    assert trade["status"] == "delisted"


def test_run_job_isolates_errors_and_clears_them(monkeypatch):
    monkeypatch.setattr(runner, "_status", {})
    monkeypatch.setattr(runner, "_last_run", {})

    async def boom():
        raise ValueError("upstream down")

    async def fine():
        return None

    asyncio.run(runner._run_job("x", 60, 1000.0, False, boom))
    assert "upstream down" in runner._status["x_error"] and "x" in runner._status["durations_sec"]
    asyncio.run(runner._run_job("x", 60, 1030.0, False, fine))  # not due yet: nothing changes
    assert "x_error" in runner._status
    asyncio.run(runner._run_job("x", 60, 1070.0, False, fine))
    assert "x_error" not in runner._status


def test_background_tick_never_overlaps(monkeypatch):
    calls = []

    async def slow_tick(force=False):
        calls.append(1)
        await asyncio.sleep(0.05)
        return {}

    async def scenario():
        monkeypatch.setattr(runner, "tick", slow_tick)
        monkeypatch.setattr(runner, "_running", None)
        assert runner.start_background_tick() is True
        assert runner.start_background_tick() is False  # still running
        await asyncio.sleep(0.1)
        assert runner.start_background_tick() is True  # finished: a new pass may start
        await asyncio.sleep(0.1)

    asyncio.run(scenario())
    assert len(calls) == 2


def test_delist_scoring_refuses_a_half_fetched_universe(monkeypatch):
    symbols = [{"baseAsset": f"C{i}", "quoteAsset": "USDT", "status": "TRADING"} for i in range(10)]

    class Client:
        async def get(self, url, params=None):
            if url.endswith("/exchangeInfo"):
                return _Resp({"symbols": symbols + [{"baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING"}]})
            if params["symbol"] in {"C1USDT", "C2USDT", "C3USDT", "C4USDT"}:
                raise ConnectionError("proxy dropped the connection")
            rows = [[1_600_000_000_000 + d * 86_400_000, "1", "1", "1", "1", "1", "0", "1000"] for d in range(200)]
            return _Resp(rows)

    with pytest.raises(RuntimeError, match="keeping the previous scores"):
        asyncio.run(delist_risk.compute_live_scores(Client(), {"weights": [0] * 5, "bias": 0}))
