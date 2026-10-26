import numpy as np
import pytest

from scripts.research_next10xgem1_fade import STOP_SLIP, short_paths, trigger_entries

COST = .002


def bars(opens, highs=None, lows=None):
    o = np.array(opens, float)
    h = np.array(highs if highs is not None else opens, float)
    lo = np.array(lows if lows is not None else opens, float)
    return o, h, lo, np.zeros(len(o)), np.ones(len(o), dtype=bool)


def test_held_to_time_is_the_arithmetic_short_return():
    o, h, lo, f, k = bars([100, 98, 95, 90])
    net, reason, held = short_paths(o, h, lo, f, k, hold=3)
    assert net[0] == pytest.approx((100 - 90) / 100 - COST)
    assert reason[0] == 0 and held[0] == 3
    assert np.isnan(net[1:]).all()  # later starts have no exit bar


def test_stop_fills_at_the_level_plus_slip_and_before_a_take_in_the_same_bar():
    o, h, lo, f, k = bars([100, 100, 100], highs=[100, 112, 100], lows=[100, 80, 100])
    net, reason, held = short_paths(o, h, lo, f, k, hold=2, stop=.10, take=.10)
    assert reason[0] == 1 and held[0] == 2
    assert net[0] == pytest.approx((100 - 110 * (1 + STOP_SLIP)) / 100 - COST)


def test_an_open_beyond_the_stop_fills_at_that_open():
    o, h, lo, f, k = bars([100, 130, 130], highs=[101, 135, 130], lows=[99, 125, 130])
    net, reason, _ = short_paths(o, h, lo, f, k, hold=2, stop=.10)
    assert reason[0] == 1
    assert net[0] == pytest.approx((100 - 130 * (1 + STOP_SLIP)) / 100 - COST)


def test_take_profit_and_trailing_stop():
    o, h, lo, f, k = bars([100, 95, 85, 90, 90], highs=[100, 96, 86, 96, 90], lows=[96, 84, 80, 89, 90])
    net, reason, held = short_paths(o, h, lo, f, k, hold=4, take=.15)
    assert reason[0] == 2 and held[0] == 2 and net[0] == pytest.approx(.15 - COST)
    # trail 10% above the lowest completed low (80 after bar 2) -> 88; bar 3 opens at 90, beyond it
    net, reason, held = short_paths(o, h, lo, f, k, hold=4, trail=.10)
    assert reason[0] == 1 and held[0] == 4
    assert net[0] == pytest.approx((100 - 90 * (1 + STOP_SLIP)) / 100 - COST)
    o[3] = 87  # opens below the trail, then the high 96 crosses it
    net, reason, held = short_paths(o, h, lo, f, k, hold=4, trail=.10)
    assert reason[0] == 1 and net[0] == pytest.approx((100 - 88 * (1 + STOP_SLIP)) / 100 - COST)


def test_no_stop_liquidates_at_twice_entry():
    o, h, lo, f, k = bars([100, 150, 120], highs=[110, 210, 130], lows=[95, 140, 110])
    net, reason, _ = short_paths(o, h, lo, f, k, hold=2)
    assert reason[0] == 3 and net[0] == -1.0


def test_funding_counts_only_bars_held_and_unknown_funding_is_missing():
    o, h, lo, f, k = bars([100, 100, 100, 100], highs=[100, 120, 100, 100])
    f[:] = [.001, .002, .004, .008]
    net, _, held = short_paths(o, h, lo, f, k, hold=3, stop=.10)
    assert held[0] == 2  # stopped in bar 1: settlements at bars 0 and 1 paid
    assert net[0] == pytest.approx((100 - 110 * (1 + STOP_SLIP)) / 100 - COST + .003)
    k[1] = False
    net, _, _ = short_paths(o, h, lo, f, k, hold=3, stop=.10)
    assert np.isnan(net[0])


def test_missing_bar_while_open_is_missing_but_after_exit_is_not():
    o, h, lo, f, k = bars([100, 100, np.nan, 100], highs=[100, 120, np.nan, 100])
    net, _, _ = short_paths(o, h, lo, f, k, hold=3, stop=.10)
    assert np.isfinite(net[0])  # stopped in bar 1, before the gap
    net, _, _ = short_paths(o, h, lo, f, k, hold=3)
    assert np.isnan(net[0])


def test_trigger_entries():
    o = np.array([100, 104, 111, 106, 99, 98], float)
    h = np.array([105, 112, 112, 107, 100, 99], float)
    assert trigger_entries(o, h, "delay", 2)[0] == 2
    assert trigger_entries(o, h, "delay", 2)[4] == -1
    # extension: first open >= 110 after bar 0 is bar 2
    assert trigger_entries(o, h, "extension", .10, window=4)[0] == 2
    # pullback 5% from the running high 112 (seen by bar 2) -> first open <= 106.4 is bar 3
    assert trigger_entries(o, h, "pullback", .05, window=4)[0] == 3
    assert trigger_entries(o, h, "pullback", .50, window=4)[0] == -1
