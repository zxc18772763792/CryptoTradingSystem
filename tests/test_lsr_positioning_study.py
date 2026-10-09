from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.research.xs_evaluation import spearman
from scripts import lsr_positioning_study as study

HOURS = pd.date_range("2026-09-01", periods=400, freq="1h", tz="UTC")


def _frame(values: np.ndarray) -> pd.DataFrame:
    return pd.DataFrame(values, index=HOURS, columns=[f"C{i}USDT" for i in range(values.shape[1])])


def test_ratio_points_are_used_one_hour_after_their_stamp():
    stamps = pd.date_range("2026-09-01", periods=100, freq="1h", tz="UTC")
    hours = np.arange(100, dtype=float)
    payload = {
        "symbol": "AUSDT",
        "top_position": [{"timestamp": int(t.timestamp() * 1000), "longShortRatio": str(np.exp(h * h * 1e-4))}
                         for t, h in zip(stamps, hours)],
        "global_account": [{"timestamp": int(t.timestamp() * 1000), "longShortRatio": "1.0"} for t in stamps],
        "klines": [[int(t.timestamp() * 1000), 0, 0, 0, "1.0"] for t in stamps],
    }
    signals = study.build_signals(study.build_panel({"AUSDT": payload}))
    t = stamps[60]
    lag = 1  # the point stamped T is first usable at T + 1h
    expected = ((60 - lag) ** 2 - (60 - lag - 24) ** 2) * 1e-4
    assert signals["top_flow_24h"].loc[t, "AUSDT"] == pytest.approx(expected)


def test_ratio_history_pages_backward_past_the_latest_500():
    """The endpoint returns the latest 500 points of any longer window (as Binance does)."""
    import asyncio

    hour = 3_600_000
    available = [{"timestamp": t, "longShortRatio": "1.0"} for t in range(0, 720 * hour, hour)]

    class _Resp:
        status_code = 200

        def __init__(self, rows):
            self._rows = rows

        def raise_for_status(self):
            return None

        def json(self):
            return self._rows

    class _Client:
        calls = 0

        async def get(self, url, params):
            _Client.calls += 1
            window = [r for r in available if r["timestamp"] <= params["endTime"]]
            return _Resp(window[-params["limit"]:])

    rows = asyncio.run(study._ratio_history(_Client(), "/x", "AUSDT", 0, 719 * hour))
    assert len(rows) == 720 and rows[0]["timestamp"] == 0 and rows[-1]["timestamp"] == 719 * hour
    # a cache holding only the latest 500 is topped up with the older gap
    _Client.calls = 0
    topped = asyncio.run(study._ratio_history(_Client(), "/x", "AUSDT", 0, 719 * hour, rows=available[-500:]))
    assert len(topped) == 720 and _Client.calls == 1


def test_row_ic_matches_the_project_spearman():
    rng = np.random.default_rng(1)
    a, b = _frame(rng.normal(size=(400, 40))), _frame(rng.normal(size=(400, 40)))
    b.iloc[5, 3] = np.nan
    ic = study.row_ic(a, b)
    assert ic.iloc[5] == pytest.approx(spearman(a.iloc[5], b.iloc[5]))
    assert ic.iloc[0] == pytest.approx(spearman(a.iloc[0], b.iloc[0]))


def test_planted_signal_is_found_by_every_test():
    rng = np.random.default_rng(2)
    signal = _frame(rng.normal(size=(400, 60)))
    label = 0.5 * signal + _frame(rng.normal(size=(400, 60)))
    perm = study.coin_permutation(signal, label, reps=200)
    assert perm["ic"] > 0.3 and perm["p"] < study.ALPHA
    assert study.row_ic(signal, label).mean() > 0.3


def test_a_persistent_random_coin_trait_fools_hourly_ics_but_not_the_coin_permutation():
    """Lesson 1 of research loop v2: every hour repeats the same coin ranking, so a day bootstrap
    of hourly ICs looks significant; permuting whole coins gives the honest null."""
    rng = np.random.default_rng(5)
    trait = rng.normal(size=60)
    drift = rng.normal(size=60)  # coin-level trends over the window, unrelated to the trait
    signal = _frame(np.tile(trait, (400, 1)) + 0.05 * rng.normal(size=(400, 60)))
    label = _frame(np.tile(drift, (400, 1)) + 0.3 * rng.normal(size=(400, 60)))
    ic = study.row_ic(signal, label)
    observed = spearman(pd.Series(trait), pd.Series(drift))
    assert ic.mean() == pytest.approx(observed, abs=0.05)
    assert ic.std() < 0.1  # tight day-to-day, so a naive CI would call it "significant"
    assert study.coin_permutation(signal, label, reps=200)["p"] > study.ALPHA


def test_residualizing_removes_a_signal_that_is_only_past_return():
    rng = np.random.default_rng(3)
    past = _frame(rng.normal(size=(400, 60)))
    vol = _frame(np.abs(rng.normal(size=(400, 60))))
    label = -0.5 * past + _frame(rng.normal(size=(400, 60)))  # short-term reversal
    signal = past.copy()  # a "positioning" signal that merely re-encodes the past move
    assert study.row_ic(signal, label).mean() < -0.3
    residual = study.residualize(signal, [past, vol])
    assert abs(study.row_ic(residual.where(residual.abs() > 1e-9), label).mean()) < 0.05 or residual.abs().max().max() < 1e-6


def test_quintile_spread_charges_costs_on_turnover_only():
    signal = _frame(np.tile(np.arange(40, dtype=float), (400, 1)))  # fixed ranking: no turnover after day 1
    fwd = _frame(np.tile(np.arange(40, dtype=float) * 0.001, (400, 1)))
    out = study.quintile_spread(signal, fwd, direction=1.0, cost_round_trip=0.001)
    top, bottom = np.arange(32, 40).mean() * 0.001, np.arange(0, 8).mean() * 0.001
    assert out["gross"] == pytest.approx(top - bottom)
    days_per_hour = len(HOURS) / 24
    assert out["net"] == pytest.approx(top - bottom - 2 * 0.001 / days_per_hour, rel=0.05)
