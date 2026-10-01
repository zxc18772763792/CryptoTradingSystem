from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from core.research import xs_reversal_tracker as xr

HOUR = 3_600_000
D0 = datetime(2026, 10, 5, tzinfo=timezone.utc)
D0_MS = int(D0.timestamp() * 1000)
BASES = [f"C{i:02d}" for i in range(40)]


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class Exchange:
    """40 perps. Coin i drifts down by i*0.1%/h for the last 10 hours (C39 falls most -> furthest below
    its EMA -> long); every coin then moves `next_day[i]` over the following 24h."""

    def __init__(self):
        self.now = D0 + timedelta(minutes=10)
        self.next_day = {f"C{i:02d}USDT": 1.0 for i in range(40)}
        self.funding = 0.0001
        self.gone = set()

    def close(self, symbol, open_ms):
        i = int(symbol[1:3])
        k = (open_ms - D0_MS) // HOUR  # 0 = the bar that opened at 00:00 on D0
        if k <= -1:  # history up to the 23:00 bar of D0-1
            drop = max(0, k + 11)  # bars -10..-1 fall: the 23:00 bar has fallen 10 times
            return 100.0 * (1 - 0.001 * i) ** drop
        base = 100.0 * (1 - 0.001 * i) ** 10
        return base * self.next_day[symbol] if k >= 23 else base

    async def get(self, url, params=None):
        params = params or {}
        if url.endswith("/exchangeInfo"):
            rows = [{"symbol": f"{b}USDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "TRADING"} for b in BASES]
            rows.append({"symbol": "XUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "SETTLING"})
            return _Resp({"symbols": rows})
        if url.endswith("/ticker/price"):
            return _Resp([{"symbol": f"{b}USDT", "price": "1"} for b in BASES])
        symbol = params.get("symbol")
        if url.endswith("/klines"):
            if symbol in self.gone:
                return _Resp({"code": -1121}, 400)
            now_ms = self.now.timestamp() * 1000
            if "endTime" in params:
                last = (int(params["endTime"]) + 1) // HOUR * HOUR - HOUR
                opens = [last - j * HOUR for j in range(params["limit"])][::-1]
            else:
                opens = [int(params["startTime"])]
            return _Resp([[t, "0", "0", "0", str(self.close(symbol, t))] for t in opens if t + HOUR <= now_ms])
        if url.endswith("/fundingRate"):
            lo, hi = int(params["startTime"]), int(params["endTime"])
            return _Resp([{"fundingTime": t, "fundingRate": str(self.funding)} for t in range(D0_MS, D0_MS + 25 * HOUR, 8 * HOUR)
                          if lo <= t <= hi])
        raise AssertionError(url)


@pytest.fixture
def ex(monkeypatch, tmp_path):
    e = Exchange()
    monkeypatch.setattr(xr, "_now", lambda: e.now)
    universe = tmp_path / "universe.json"
    universe.write_text(json.dumps({"bases": BASES + ["NOPERP"]}), encoding="utf-8")
    e.state = tmp_path / "state.json"
    xr.save_state({"started_at": (D0 - timedelta(days=1)).isoformat(), "days": {}}, e.state)
    e.run = lambda: asyncio.run(xr.tick(e, state_path=e.state, universe_path=universe))
    return e


def test_perp_map_prefers_plain_symbol_and_skips_dead_contracts():
    info = {"symbols": [
        {"symbol": "PEPEUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "SETTLING"},
        {"symbol": "1000PEPEUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "TRADING"},
        {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "TRADING"},
        {"symbol": "ETHUSDT_260327", "contractType": "CURRENT_QUARTER", "quoteAsset": "USDT", "status": "TRADING"},
    ]}
    assert xr.perp_symbols(info) == {"PEPE": "1000PEPEUSDT", "BTC": "BTCUSDT"}


def test_signal_and_legs_long_the_coins_furthest_below_their_ema():
    idx = range(60)
    closes = pd.DataFrame({f"C{i}": [100.0] * 50 + [100.0 * (1 - 0.01 * i) ** k for k in range(1, 11)] for i in range(30)}, index=idx)
    longs, shorts = xr.pick_legs(xr.reversal_signal(closes))
    assert longs == ["C27", "C28", "C29"] and shorts == ["C0", "C1", "C2"]
    assert xr.pick_legs(xr.reversal_signal(closes.iloc[:, :29])) is None  # fewer than 30 coins


def test_daily_cycle_forms_at_midnight_and_settles_with_funding(ex):
    summary = ex.run()
    day = json.loads(ex.state.read_text(encoding="utf-8"))["days"]["2026-10-05"]
    assert day["status"] == "open" and day["late"] is False and day["coins"] == 40
    assert sorted(day["longs"]) == ["C36USDT", "C37USDT", "C38USDT", "C39USDT"]  # 40 // 10 per side, the biggest fallers
    assert sorted(day["shorts"]) == ["C00USDT", "C01USDT", "C02USDT", "C03USDT"]
    assert day["longs"]["C39USDT"]["entry"] == pytest.approx(100.0 * 0.961 ** 10)
    assert summary["universe_size"] == 41 and summary["forward_open"] == 1

    for s in ex.next_day:
        ex.next_day[s] = 1.10 if s in day["longs"] else 0.95  # longs +10%, shorts -5%
    ex.now = D0 + timedelta(days=1, hours=1, minutes=10)
    summary = ex.run()
    day = json.loads(ex.state.read_text(encoding="utf-8"))["days"]["2026-10-05"]
    funding = 3 * 0.0001  # settlements at 08:00, 16:00, 00:00 inside (entry, exit]
    assert day["status"] == "closed"
    assert day["long_leg_pct"] == pytest.approx((0.10 - funding) * 100)
    assert day["short_leg_pct"] == pytest.approx((0.05 + funding) * 100)
    assert day["net_per_position_pct"] == pytest.approx(((0.10 + 0.05) / 2 - xr.ROUND_TRIP_COST) * 100)
    assert summary["forward_completed"] == 1 and summary["retirement"]["verdict"] == "collecting"
    assert json.loads(ex.state.read_text(encoding="utf-8"))["retirement_rule"]["min_n"] == 180


def test_late_formation_is_recorded_but_not_counted(ex):
    ex.now = D0 + timedelta(minutes=45)
    ex.run()
    ex.now = D0 + timedelta(days=1, hours=1, minutes=10)
    summary = ex.run()
    day = json.loads(ex.state.read_text(encoding="utf-8"))["days"]["2026-10-05"]
    assert day["late"] is True and day["status"] == "closed"
    # the 01:10 pass also forms 10-06, 70 minutes after 00:00: late as well
    assert summary["late"] == 2 and summary["forward_completed"] == 0


def test_missed_morning_and_mid_day_start(ex, tmp_path):
    ex.now = D0 + timedelta(hours=7)
    summary = ex.run()
    assert summary["missed"] == 1
    fresh = tmp_path / "fresh.json"
    xr.save_state({"started_at": (D0 + timedelta(hours=9)).isoformat(), "days": {}}, fresh)
    ex.now = D0 + timedelta(hours=10)
    asyncio.run(xr.tick(ex, state_path=fresh, universe_path=tmp_path / "universe.json"))
    assert json.loads(fresh.read_text(encoding="utf-8"))["days"] == {}  # first day starts tomorrow, not "missed"


def test_delisted_position_is_dropped_after_the_grace_period(ex):
    ex.run()
    ex.gone = {"C39USDT"}
    ex.now = D0 + timedelta(days=1, hours=1, minutes=10)
    ex.run()
    day = json.loads(ex.state.read_text(encoding="utf-8"))["days"]["2026-10-05"]
    assert day["status"] == "closed" and day["missing_positions"] == 1  # a vanished contract does not block settlement
    assert day["longs"]["C39USDT"]["gone"] is True
