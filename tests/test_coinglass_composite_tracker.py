from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from core.research import coinglass_composite_tracker as ct
from core.research import coinglass_daily_store as store
from scripts import coinglass_combination_study as combo
from scripts import coinglass_positioning_study as single

DAY_MS = 86_400_000
D = datetime(2025, 6, 30, tzinfo=timezone.utc)
D_MS = int(D.timestamp() * 1000)
PERPS = [f"C{i:02d}USDT" for i in range(40)]


def _coins(seed: int = 0, days: int = 90) -> dict:
    """Random daily bars for 40 coins up to day D (ratio snapshots up to D+1 00:00), some gaps."""
    rng = np.random.default_rng(seed)
    coins = {}
    for perp in PERPS:
        coin = {"base": perp[:-4], "perp": perp}
        stamps = [D_MS - k * DAY_MS for k in range(days)][::-1]
        coin["oi"] = [{"time": t, "close": str(1e6 * np.exp(rng.normal(0, 0.05)))} for t in stamps]
        coin["funding"] = [{"time": t, "close": str(rng.normal(0.01, 0.005))} for t in stamps]
        coin["liquidation"] = [{"time": t, "aggregated_long_liquidation_usd": float(rng.lognormal(10, 1)),
                                "aggregated_short_liquidation_usd": float(rng.lognormal(10, 1))} for t in stamps]
        coin["taker"] = [{"time": t, "aggregated_buy_volume_usd": float(rng.lognormal(15, 0.3)),
                          "aggregated_sell_volume_usd": float(rng.lognormal(15, 0.3))} for t in stamps]
        snaps = [t + DAY_MS for t in stamps]
        for dataset, prefix in (("global_ratio", "global_account"), ("top_ratio", "top_position")):
            rows = []
            for t in snaps:
                long_pct = float(rng.uniform(40, 70))
                rows.append({"time": t, f"{prefix}_long_percent": round(long_pct, 2), f"{prefix}_short_percent": round(100 - long_pct, 2),
                             f"{prefix}_long_short_ratio": round(long_pct / (100 - long_pct), 2)})
            coin[dataset] = rows
        if perp == "C05USDT":
            coin["oi"] = [r for r in coin["oi"] if r["time"] != D_MS - 2 * DAY_MS]  # a gap
        coins[perp] = coin
    return coins


def test_signals_and_composite_match_the_backtest_code_exactly():
    coins = _coins()
    study_frames = single.build_panel({perp: dict(coin) for perp, coin in coins.items()})
    study = single.build_signals(study_frames)
    mine = ct.signals(ct.frames_from_store(coins, D))
    day = pd.Timestamp(D)
    for name in ct.WEIGHTS:
        a = study[name].loc[day].reindex(PERPS)
        b = mine[name].loc[day].reindex(PERPS)
        assert a.notna().sum() >= 30, name
        pd.testing.assert_series_equal(a, b, check_names=False, rtol=1e-9, atol=1e-12)
    expected = combo.composite(study, ct.WEIGHTS).loc[day].dropna()
    got = ct.composite_row({name: mine[name].loc[day] for name in ct.WEIGHTS}, ct.WEIGHTS)
    pd.testing.assert_series_equal(expected.sort_index(), got.sort_index(), check_names=False, rtol=1e-9)


class Binance:
    def __init__(self):
        self.prices = {p: 10.0 for p in PERPS}
        self.funding_rate = 0.0001

    async def get(self, url, params=None):
        if url.endswith("/ticker/price"):
            return _Resp([{"symbol": p, "price": str(v)} for p, v in self.prices.items()])
        if url.endswith("/fundingRate"):
            lo, hi = int(params["startTime"]), int(params["endTime"])
            first = (lo // (8 * 3_600_000) + 1) * 8 * 3_600_000
            return _Resp([{"fundingTime": t, "fundingRate": str(self.funding_rate)} for t in range(first, hi + 1, 8 * 3_600_000)])
        raise AssertionError(url)


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


def test_a_book_is_settled_net_of_funding_and_turnover_costs():
    coins = _coins()
    binance = Binance()
    formed = D + timedelta(days=1, hours=3)
    state = {"books": {}}
    record = ct.form(state, D, formed, asyncio.run(ct._prices(binance)), coins, partial=False)
    book = record["main"]
    assert book["status"] == "open" and len(book["longs"]) == len(book["shorts"]) == 8  # 40 // 5
    assert book["turnover"] == 1.0  # nothing to carry over on the first day
    scores = [p["score"] for p in book["longs"].values()] + [p["score"] for p in book["shorts"].values()]
    assert min(p["score"] for p in book["longs"].values()) > max(p["score"] for p in book["shorts"].values())
    assert len(scores) == 16 and record["shadow"]["status"] == "open"

    for perp in book["longs"]:
        binance.prices[perp] = 10.5    # longs +5%
    for perp in book["shorts"]:
        binance.prices[perp] = 9.7     # shorts -3%
    now = formed + timedelta(days=7, minutes=5)
    asyncio.run(ct.settle(binance, record, now, asyncio.run(ct._prices(binance))))
    settlements = 21  # every 8h over the 7 days
    funding = settlements * 0.0001
    assert book["status"] == "closed"
    assert book["long_leg_pct"] == pytest.approx((0.05 - funding) * 100)
    assert book["short_leg_pct"] == pytest.approx((0.03 + funding) * 100)
    assert book["net_pct"] == pytest.approx((0.08 - 2 * 1.0 * ct.ROUND_TRIP_COST) * 100)

    # a book that replaces it pays only for the names it changes: same data, same names, no turnover
    state = {"books": {(D - timedelta(days=ct.HOLD_DAYS)).date().isoformat(): record}}
    nxt = ct.form(state, D, now, asyncio.run(ct._prices(binance)), coins, partial=False)
    assert nxt["main"]["turnover"] == 0.0


@pytest.fixture
def world(monkeypatch, tmp_path):
    coins = _coins()
    monkeypatch.setattr(store, "universe", lambda: [(perp[:-4], perp) for perp in PERPS])
    monkeypatch.setattr(store, "load_coin", lambda perp, root=None: coins.get(perp, {}))
    monkeypatch.setattr(store, "load_status", lambda path=None: {})
    w = type("World", (), {})()
    w.binance, w.state = Binance(), tmp_path / "composite.json"
    w.complete = set(PERPS)
    monkeypatch.setattr(store, "complete", lambda coin, day: coin.get("perp") in w.complete)
    w.run = lambda now: (monkeypatch.setattr(ct, "_now", lambda: now), asyncio.run(ct.tick(w.binance, state_path=w.state)))[1]
    ct.save_state({"started_at": (D - timedelta(days=2)).isoformat(), "books": {}}, w.state)
    return w


def test_forms_when_the_store_is_ready_misses_after_the_deadline(world):
    opens = D + timedelta(days=1)
    world.complete = set(PERPS[:20])            # half the coins updated: not ready yet
    world.run(opens + timedelta(hours=2))
    assert json.loads(world.state.read_text(encoding="utf-8"))["books"] == {}
    world.complete = set(PERPS)
    summary = world.run(opens + timedelta(hours=3))
    books = json.loads(world.state.read_text(encoding="utf-8"))["books"]
    assert books[D.date().isoformat()]["main"]["status"] == "open" and summary["forward_open"] == 1
    assert json.loads(world.state.read_text(encoding="utf-8"))["retirement_rule"]["min_n"] == 364

    # the next day's data never arrives: the first pass after deadline + grace marks it missed
    world.complete = set()
    world.run(opens + timedelta(days=1, hours=12, minutes=31))
    books = json.loads(world.state.read_text(encoding="utf-8"))["books"]
    assert books[(D + timedelta(days=1)).date().isoformat()]["status"] == "missed"


def test_no_book_for_a_day_that_opened_before_the_tracker_started(world):
    ct.save_state({"started_at": (D + timedelta(days=1, hours=1)).isoformat(), "books": {}}, world.state)
    world.run(D + timedelta(days=1, hours=3))
    assert json.loads(world.state.read_text(encoding="utf-8"))["books"] == {}


def test_a_final_store_day_forms_with_the_coins_it_has(world, monkeypatch):
    world.complete = set(PERPS[:32])  # 80%: below READY_SHARE, but nothing more will come today
    monkeypatch.setattr(store, "load_status", lambda path=None: {"day": D.date().isoformat(), "final": True})
    world.run(D + timedelta(days=1, hours=3))
    book = json.loads(world.state.read_text(encoding="utf-8"))["books"][D.date().isoformat()]
    assert book["partial"] is True and book["coins"] == 32 and book["main"]["status"] == "open"
