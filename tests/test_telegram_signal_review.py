from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.review_telegram_signals import (
    SymbolPanel, bh_fdr, dedup_independent, entry_hour, null_candidates, permutation_test, side_return,
)


def panel(opens, highs=None, funding=None):
    index = pd.date_range("2026-01-01", periods=len(opens), freq="h", tz="UTC")
    opens = np.asarray(opens, float)
    highs = opens * 1.01 if highs is None else np.asarray(highs, float)
    bars = pd.DataFrame({"open": opens, "high": highs, "low": opens * .99, "close": opens, "volume": 1.}, index=index)
    return SymbolPanel(bars, funding)


def test_short_return_is_entry_minus_exit_over_entry_and_floored():
    assert side_return(100, 80, -1) == pytest.approx(.20)
    assert side_return(100, 125, -1) == pytest.approx(-.25)
    assert side_return(100, 125, 1) == pytest.approx(.25)
    assert side_return(100, 350, -1) == -1.0


def test_entry_waits_a_minute_then_takes_next_full_hour():
    assert entry_hour("2026-01-01T00:59:30Z") == pd.Timestamp("2026-01-01T02:00:00Z")
    assert entry_hour("2026-01-01T00:10:00+08:00") == pd.Timestamp("2025-12-31T17:00:00Z")


def test_outcomes_use_future_open_never_fill_gaps_and_liquidate_short_on_doubling():
    p = panel([100, 110, 120, 130, 140])
    gross, _ = p.outcomes(2, 1)
    assert gross[0] == pytest.approx(.20)
    assert np.isnan(gross[3]) and np.isnan(gross[4])  # no future bar, no value

    gap = panel([100, np.nan, 120, 130])
    assert np.isnan(gap.outcomes(1, 1)[0][0]) and np.isnan(gap.outcomes(1, 1)[0][1])

    spike = panel([100, 100, 100], highs=[100, 250, 100])
    assert spike.outcomes(2, -1)[0][0] == -1.0


def test_funding_sign_follows_side_and_settlement_at_entry_hour_is_paid():
    index = pd.date_range("2026-01-01", periods=6, freq="h", tz="UTC")
    funding = pd.DataFrame({"funding_rate": [.001, .002], "mark_price": [100., 100.]},
                           index=[index[0] + pd.Timedelta(milliseconds=3), index[2] + pd.Timedelta(milliseconds=3)])
    p = panel([100] * 6, funding=funding)
    _, long_f = p.outcomes(2, 1)
    _, short_f = p.outcomes(2, -1)
    assert long_f[0] == pytest.approx(-.001)   # pays settlement at 00:00, not the one at 02:00
    assert short_f[1] == pytest.approx(.002)  # short receives positive funding


def test_null_candidates_exclude_own_window_and_invalid_starts():
    valid = np.ones(100, dtype=bool)
    valid[10] = False
    idx = null_candidates(50, 4, valid, window=45)
    assert idx.min() == 5 and idx.max() == 95
    assert not set(range(47, 54)) & set(idx)
    assert 10 not in idx


def test_dedup_keeps_first_position_per_coin_until_it_closes():
    t = pd.Timestamp("2026-01-01T00:00Z")
    rows = [{"signal_id": "a", "symbol": "X", "entry_at": t, "hours": 24},
            {"signal_id": "b", "symbol": "X", "entry_at": t + pd.Timedelta(hours=5), "hours": 24},
            {"signal_id": "c", "symbol": "Y", "entry_at": t + pd.Timedelta(hours=5), "hours": 24},
            {"signal_id": "d", "symbol": "X", "entry_at": t + pd.Timedelta(hours=24), "hours": 24}]
    assert [r["signal_id"] for r in dedup_independent(rows)] == ["a", "c", "d"]


def test_bh_fdr_is_monotone_and_matches_reference():
    q = bh_fdr([.01, .04, .03, .5])
    assert q == pytest.approx([.04, .0533333, .0533333, .5], rel=1e-4)


def test_permutation_detects_only_real_excess():
    nulls = [np.array([-.01, 0., .01])] * 30
    assert permutation_test(np.full(30, .02), nulls)["p_greater"] < .01
    assert permutation_test(np.zeros(30), nulls)["p_greater"] > .3
