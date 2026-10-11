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
    # the liquidation shadow's inputs: today's 24h totals (None = unavailable) and the stored prior days
    e.liquidations, e.store = None, {}

    async def today():
        return e.liquidations

    monkeypatch.setattr(xr, "_today_liquidations", today)
    monkeypatch.setattr("core.research.coinglass_daily_store.load_coin", lambda perp, root=None: e.store.get(perp, {}))
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
    assert day["liq_filter"]["status"] == "no_data"  # no liquidation lists: the shadow sits out, the main book does not

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


def test_liquidation_shadow_drops_the_shocks_from_the_longs_and_is_judged_on_the_difference(ex):
    bar_day = D0 - timedelta(days=1)  # the Coinglass bar that closes at the 00:00 formation
    prior_days = [int((bar_day - timedelta(days=k)).timestamp() * 1000) for k in range(1, 31)]
    shocked = {"C39", "C38", "C00", "C01", "C02", "C03", "C04", "C05"}  # the top fifth of 40
    ex.store = {f"{b}USDT": {"base": b, "liquidation": [
        {"time": t, "aggregated_long_liquidation_usd": 600.0 + (t // 86_400_000) % 7, "aggregated_short_liquidation_usd": 400.0}
        for t in prior_days]} for b in BASES}
    ex.liquidations = {b: (1e7 if b in shocked else 1000.0) for b in BASES}
    ex.run()
    day = json.loads(ex.state.read_text(encoding="utf-8"))["days"]["2026-10-05"]
    shadow = day["liq_filter"]
    assert sorted(day["longs"]) == ["C36USDT", "C37USDT", "C38USDT", "C39USDT"]  # the main book is untouched
    assert sorted(shadow["flagged"]) == sorted(f"{b}USDT" for b in shocked)
    assert sorted(shadow["longs"]) == ["C35USDT", "C36USDT", "C37USDT"]  # 32 candidates left: 3 per side

    for s in ex.next_day:
        ex.next_day[s] = 0.90 if s in ("C38USDT", "C39USDT") else 1.0  # the shocked longs keep falling
    ex.now = D0 + timedelta(days=1, hours=1, minutes=10)
    summary = ex.run()
    day = json.loads(ex.state.read_text(encoding="utf-8"))["days"]["2026-10-05"]
    funding = 3 * 0.0001
    main_long = (2 * (-funding) + 2 * (-0.10 - funding)) / 4
    shadow_long = -funding
    short_leg = funding
    assert day["net_per_position_pct"] == pytest.approx(((main_long + short_leg) / 2 - xr.ROUND_TRIP_COST) * 100)
    assert day["liq_filter"]["net_per_position_pct"] == pytest.approx(((shadow_long + short_leg) / 2 - xr.ROUND_TRIP_COST) * 100)
    assert day["liq_filter"]["diff_pct"] == pytest.approx((shadow_long - main_long) / 2 * 100)
    assert summary["liq_filter"]["completed"] == 1 and summary["liq_filter"]["mean_diff_pct"] == pytest.approx(2.5, abs=1e-3)
    assert json.loads(ex.state.read_text(encoding="utf-8"))["liq_filter_rule"]["min_n"] == 180


def test_liquidation_shock_needs_twenty_prior_days():
    day = D0 - timedelta(days=1)
    rows = [{"time": int((day - timedelta(days=k)).timestamp() * 1000), "aggregated_long_liquidation_usd": 100.0 + k,
             "aggregated_short_liquidation_usd": 50.0} for k in range(1, 20)]  # only 19 prior days
    assert xr.liquidation_shock({"A": 1e6}, {"AUSDT": {"base": "A", "liquidation": rows}}, day) == {}
    rows.append({"time": int((day - timedelta(days=20)).timestamp() * 1000), "aggregated_long_liquidation_usd": 120.0,
                 "aggregated_short_liquidation_usd": 50.0})
    shock = xr.liquidation_shock({"A": 1e6}, {"AUSDT": {"base": "A", "liquidation": rows}}, day)
    import numpy as np
    prior = np.log1p([r["aggregated_long_liquidation_usd"] + 50.0 for r in rows])
    assert shock["AUSDT"] == pytest.approx((np.log1p(1e6) - prior.mean()) / prior.std(ddof=1))


def test_coin_lists_keep_multiplier_contracts_apart():
    """The day's total must cover what the stored history covers: Coinglass's PEPE is OKX's PEPE only."""
    lists = {"Binance": [{"symbol": "1000PEPE", "liquidation_usd_24h": 100.0}, {"symbol": "BTC", "liquidation_usd_24h": 5.0}],
             "OKX": [{"symbol": "PEPE", "liquidation_usd_24h": 20.0}, {"symbol": "btc", "liquidation_usd_24h": 1.0}],
             "Bybit": [{"symbol": "1000PEPE", "liquidation_usd_24h": 3.0}, {"symbol": "SHIB1000", "liquidation_usd_24h": 7.0},
                       {"symbol": "BTC", "liquidation_usd_24h": "n/a"}]}
    assert xr.sum_coin_lists(lists) == {"1000PEPE": 103.0, "BTC": 6.0, "PEPE": 20.0, "SHIB1000": 7.0}


def test_the_days_lists_survive_a_slow_relay_and_fail_whole_on_a_refusal(monkeypatch):
    from core.research import coinglass_daily_store as store

    monkeypatch.setattr(store, "REQUEST_SPACING_SEC", 0.0)
    monkeypatch.setattr(store, "TIMEOUT_WAIT_SEC", 0.0)

    class Relay:
        def __init__(self, refuse=None):
            self.slow, self.refuse, self.calls = {"OKX"}, refuse, []

        async def request_json(self, path, params=None):
            exchange = params["exchange"]
            self.calls.append(exchange)
            if exchange in self.slow:
                self.slow.discard(exchange)
                raise asyncio.TimeoutError()
            if exchange == self.refuse:
                return {"payload": {"code": "40300", "msg": "no"}}
            return {"payload": {"code": "0", "data": [{"symbol": "BTC", "liquidation_usd_24h": 2.0},
                                                      {"symbol": f"X{exchange}", "liquidation_usd_24h": 1.0}]}}

    relay = Relay()
    totals = asyncio.run(xr._today_liquidations(relay))
    assert totals == {"BTC": 6.0, "XBINANCE": 1.0, "XOKX": 1.0, "XBYBIT": 1.0}
    assert relay.calls == ["Binance", "OKX", "OKX", "Bybit"]  # the timed-out OKX list was asked again
    assert asyncio.run(xr._today_liquidations(Relay(refuse="Bybit"))) is None
