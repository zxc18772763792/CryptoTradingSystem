"""The coin x day panel study finds a planted attention effect and does not invent one from noise."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "research_x_panel.py"
spec = importlib.util.spec_from_file_location("research_x_panel", SCRIPT)
panel = importlib.util.module_from_spec(spec)
spec.loader.exec_module(panel)

DAYS = pd.date_range("2026-01-01", periods=140, freq="D", tz="UTC")
COINS = [f"C{i:02d}" for i in range(14)]
HANDLES = ["h1", "h2", "h3", "h4"]


def _frames(seed, plant=0.0, plant_mask=None):
    rng = np.random.default_rng(seed)
    daily = pd.DataFrame(rng.normal(0, 0.03, (len(DAYS), len(COINS) + 1)), index=DAYS, columns=["BTC"] + COINS)
    if plant_mask is not None:  # a coin flagged for entry day t earns `plant` more over t -> t+1
        for (t, coin) in plant_mask:
            if t + pd.Timedelta(days=1) in daily.index:
                daily.loc[t + pd.Timedelta(days=1), coin] += plant
    opens = 100 * np.exp(daily.cumsum())
    dvol = pd.DataFrame(5e7, index=DAYS, columns=opens.columns)
    return opens, dvol


def _events(seed, per_day=3):
    """Each covering handle names a few coins every day; direction is random."""
    rng = np.random.default_rng(seed)
    events, mentioned = [], {}
    for day in DAYS[:-1]:
        for handle in HANDLES:
            for coin in rng.choice(COINS, size=per_day if rng.random() < 0.7 else 0, replace=False):
                direction = int(rng.choice([-1, 1]))
                events.append({"handle": handle, "coin": str(coin), "created": day + pd.Timedelta(hours=10),
                               "dir": direction, "n_coins": 1})
                mentioned[(day + pd.Timedelta(days=1), str(coin))] = True
    return events, mentioned


COVER = {h: DAYS[0] for h in HANDLES}


def _run(seed, plant=0.0):
    events, mentioned = _events(seed)
    opens, dvol = _frames(seed + 1000, plant, mentioned if plant else None)
    return panel.analyze(events, COVER, opens, dvol, flips=2000)


def test_a_planted_attention_effect_is_found_and_costs_are_counted():
    result = _run(1, plant=0.03)
    t1 = result["tests"]["T1_mentioned_vs_not"]
    assert t1["days"] >= panel.MIN_DAYS and t1["mean_gross"] > 0.01 and t1["p_two_sided"] < 0.01
    assert t1["mean_net"] == pytest.approx(t1["mean_gross"] - panel.SPREAD_COST)
    assert t1["verdict"]["passes"] is True
    assert t1["min_detectable_gross_80pct_power"] == pytest.approx(2.8 * t1["se"])


def test_noise_is_rarely_called_a_signal():
    hits = 0
    for seed in range(20):
        result = _run(seed + 10)
        hits += any(v["p_two_sided"] < 0.05 for v in result["tests"].values())
    assert hits <= 7          # three tests per run at 5%: about 14% of runs expected to show at least one


def test_a_small_effect_below_the_cost_bar_does_not_pass():
    result = _run(2, plant=0.002)       # 20 bp a day is real, but a long-short pair costs 32 bp
    t1 = result["tests"]["T1_mentioned_vs_not"]
    assert t1["mean_net"] < 0 and t1["verdict"]["passes"] is False


def test_days_without_enough_covering_accounts_are_dropped():
    events, mentioned = _events(3)
    opens, dvol = _frames(1003)
    late = {h: DAYS[120] for h in HANDLES}         # all accounts only start 120 days in
    result = panel.analyze(events, late, opens, dvol, flips=500, window_days=90)
    assert result["days_with_enough_accounts"] <= 20 and result["tests"]["T1_mentioned_vs_not"]["days"] <= 20


def test_illiquid_coins_are_left_out_of_the_universe():
    events, _ = _events(4)
    opens, dvol = _frames(1004)
    dvol["C00"] = 1e6                              # below the dollar-volume floor
    result = panel.analyze(events, COVER, opens, dvol, flips=300)
    assert result["universe_coins_per_day_median"] == len(COINS) - 1


def test_coverage_start_ignores_a_single_pinned_old_post():
    recs = [{"handle": "a", "created_at": "2025-01-01T00:00:00+00:00"}] + [
        {"handle": "a", "created_at": f"2026-10-{d:02d}T12:00:00+00:00"} for d in range(10, 20)]
    assert panel.coverage_starts(recs)["a"] == pd.Timestamp("2026-10-12", tz="UTC")   # 3rd oldest post (Oct 11 noon), next midnight


def test_one_account_repeating_a_coin_counts_once():
    day = DAYS[50]
    events = [{"handle": "h1", "coin": "C01", "created": day + pd.Timedelta(hours=h), "dir": 1, "n_coins": 1}
              for h in (1, 2, 3)] + [{"handle": "h2", "coin": "C01", "created": day + pd.Timedelta(hours=5), "dir": -1,
                                      "n_coins": 1}]
    tables = panel.signal_tables(events, COVER, DAYS, COINS)
    t = day + pd.Timedelta(days=1)
    assert tables["s1"].at[t, "C01"] == 2.0 and tables["s2"].at[t, "C01"] == 2.0   # 2 accounts; directions 1+1+1-1


def test_always_named_and_always_rising_coins_are_a_coin_effect_not_a_signal():
    """The failure the placebo runs exposed: the same coins are named every day and they simply trend up."""
    rng = np.random.default_rng(5)
    daily = pd.DataFrame(rng.normal(0, 0.03, (len(DAYS), len(COINS) + 1)), index=DAYS, columns=["BTC"] + COINS)
    for coin in COINS[:4]:
        daily[coin] += 0.006                                   # +0.6% a day, all the time
    opens = 100 * np.exp(daily.cumsum())
    dvol = pd.DataFrame(5e7, index=DAYS, columns=opens.columns)
    events = [{"handle": h, "coin": c, "created": d + pd.Timedelta(hours=9), "dir": 1, "n_coins": 1}
              for d in DAYS[:-1] for h in HANDLES for c in COINS[:4]]
    result = panel.analyze(events, COVER, opens, dvol, flips=3000)
    t1 = result["tests"]["T1_mentioned_vs_not"]
    assert abs(t1["mean_gross"]) < 0.004 and t1["p_two_sided"] > 0.01
