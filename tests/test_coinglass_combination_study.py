from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts import coinglass_combination_study as combo

SPLIT = combo.SPLIT


def _frame(values: np.ndarray, start: str = "2024-06-01") -> pd.DataFrame:
    index = pd.date_range(start, periods=values.shape[0], freq="1D", tz="UTC")
    return pd.DataFrame(values, index=index, columns=[f"C{i:02d}USDT" for i in range(values.shape[1])])


def _panel(rng: np.random.Generator, days: int = 700, coins: int = 50) -> dict:
    panel = {name: _frame(rng.normal(size=(days, coins))) for name in combo.SIGNALS}
    for h in combo.HORIZONS:
        panel[f"fwd_{h}d"] = _frame(rng.normal(size=(days, coins)))
    return panel


def test_weights_come_from_training_days_only():
    rng = np.random.default_rng(1)
    panel = _panel(rng)
    signal, label = panel["liq_shock_z"], panel["fwd_1d"]
    before = signal.index < SPLIT
    # the signal predicts up before the split and down after it
    panel["fwd_1d"] = label + np.where(before[:, None], 1.0, -1.0) * signal
    weights = combo.training_weights(panel, 1)
    assert weights["liq_shock_z"] > 0.3 and abs(weights["funding_z"]) < 0.05


def test_composite_of_one_signal_is_its_signed_rank_score_and_needs_half_the_weight():
    rng = np.random.default_rng(2)
    panel = _panel(rng, days=50)
    weights = {name: 0.0 for name in combo.SIGNALS}
    weights["retail_z"] = -0.02
    score = combo.composite(panel, weights)
    pd.testing.assert_frame_equal(score, -combo.rank_score(panel["retail_z"]))  # a negative weight flips the score
    weights["top_z"] = 0.06  # three quarters of the weight: coins missing top_z drop out
    panel["top_z"].iloc[:, 0] = np.nan
    assert combo.composite(panel, weights).iloc[:, 0].isna().all()


def test_buffered_book_charges_only_the_names_it_replaces():
    ranking = np.tile(np.arange(40, dtype=float), (60, 1))  # a fixed ranking: one entry, then no trades
    fwd = np.tile(np.arange(40, dtype=float) * 0.001, (60, 1))
    out = combo.buffered_book(_frame(ranking), _frame(fwd), cost=0.001)
    spread = (np.arange(32, 40).mean() - np.arange(0, 8).mean()) * 0.001
    assert out["gross_per_day"] == pytest.approx(spread)
    assert out["net_per_day"] == pytest.approx(spread - 2 * 0.001 / 60)
    assert out["turnover"] == pytest.approx(1 / 60)


def test_pattern_permutation_finds_a_planted_effect_and_not_noise():
    rng = np.random.default_rng(3)
    events = _frame(rng.random(size=(300, 60)) < 0.05).astype(bool)
    rel = _frame(rng.normal(scale=0.02, size=(300, 60)))
    noise_p = combo.event_permutation(events, rel, reps=200)
    planted = rel + events.astype(float) * 0.02
    planted_p = combo.event_permutation(events, planted, reps=200)
    assert planted_p == pytest.approx(1 / 201) and noise_p > 0.05
    values, days = combo.event_values(events, planted)
    assert len(values) == int(events.to_numpy().sum()) and len(days) == len(values)


def test_filter_verdicts_judge_f2_on_test_days_only():
    days = pd.date_range("2025-02-22", "2026-09-30", freq="1D", tz="UTC")
    base = pd.Series(0.0005, index=days)
    replay = pd.DataFrame({"base": base, "f1": base + 0.001,
                           "f2": base + np.where(days < SPLIT, -0.05, 0.001)}, index=days)
    replay += np.random.default_rng(4).normal(scale=1e-4, size=replay.shape)
    rows = {r["filter"]: r for r in combo.filter_verdicts(replay)}
    assert rows["F1"]["passes"] and rows["F1"]["days"] == len(days)
    assert rows["F2"]["passes"] and rows["F2"]["days"] == int((days >= SPLIT).sum())  # the bad training days do not count


def test_daily_funding_counts_each_settlement_in_the_day_it_is_held():
    ms = lambda s: int(pd.Timestamp(s, tz="UTC").timestamp() * 1000)  # noqa: E731
    cache = {"AUSDT": [[ms("2025-06-02 00:00"), 0.001],    # ends holding day 06-01 -> bar 05-31
                       [ms("2025-06-02 08:00"), 0.002],    # held on 06-02 -> bar 06-01
                       [ms("2025-06-03 00:00"), 0.004]]}   # ends holding day 06-02 -> bar 06-01
    index = pd.date_range("2025-05-30", periods=5, freq="1D", tz="UTC")
    f = combo.daily_funding(cache, index, ["AUSDT", "BUSDT"])
    assert f.loc["2025-05-31", "AUSDT"] == pytest.approx(0.001)
    assert f.loc["2025-06-01", "AUSDT"] == pytest.approx(0.006)
    assert f["BUSDT"].sum() == 0.0  # no settlements: no funding
