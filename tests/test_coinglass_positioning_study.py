from __future__ import annotations

import asyncio

import numpy as np
import pandas as pd
import pytest

from scripts import coinglass_positioning_study as study

DAY_MS = 86_400_000


def _day_number(ts: pd.Timestamp) -> int:
    return int((ts - pd.Timestamp("2024-01-01", tz="UTC")) / pd.Timedelta(days=1))


def _payload() -> dict:
    """One coin whose log price is n^2 * 1e-4 at 00:00 of day n (and +0.002 at 04:00), and whose
    log global L/S on day n is n^2 * 1e-5."""
    start = study._ms(study.START) - 40 * DAY_MS
    klines = []
    for t in range(start, study._ms(study.END) + 8 * DAY_MS, 4 * 3_600_000):
        ts = pd.Timestamp(t, unit="ms", tz="UTC")
        n = _day_number(ts.floor("D"))
        log_p = n * n * 1e-4 + (0.002 if ts.hour == 4 else 0.0)
        klines.append([t, str(np.exp(log_p)), "0", "0", "0"])
    days = range(study._ms(study.START), study._ms(study.END), DAY_MS)
    glob = [{"time": t, "global_account_long_short_ratio": str(np.exp(_day_number(pd.Timestamp(t, unit="ms", tz="UTC")) ** 2 * 1e-5))}
            for t in days]
    return {"klines_4h": klines, "global_ratio": glob}


def test_bar_d_is_traded_from_the_next_day_open():
    frames = study.build_panel({"AUSDT": _payload()})
    panel = study.build_signals(frames)
    d = pd.Timestamp("2025-03-10", tz="UTC")
    n = _day_number(d)
    # entry at the D+1 00:00 open, exit at the D+1+h open
    for h in study.HORIZONS:
        assert panel[f"fwd_{h}d"].loc[d, "AUSDT"] == pytest.approx(((n + 1 + h) ** 2 - (n + 1) ** 2) * 1e-4)
    assert panel["past_1d"].loc[d, "AUSDT"] == pytest.approx(((n + 1) ** 2 - n ** 2) * 1e-4)  # day D's own move
    # a daily L/S bar is the snapshot at its stamp: day D ends with the one stamped D+1
    prior = np.array([(n + 1 - k) ** 2 * 1e-5 for k in range(1, 8)])  # snapshots closing days D-7 .. D-1
    expected = ((n + 1) ** 2 * 1e-5 - prior.mean()) / prior.std(ddof=1)
    assert panel["retail_z"].loc[d, "AUSDT"] == pytest.approx(expected)
    late = study.build_signals(frames, entry="open_04")
    assert late["fwd_1d"].loc[d, "AUSDT"] == pytest.approx(((n + 2) ** 2 - (n + 1) ** 2) * 1e-4)  # 04:00 to 04:00


def _frame(values: np.ndarray) -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=values.shape[0], freq="1D", tz="UTC")
    return pd.DataFrame(values, index=index, columns=[f"C{i:02d}USDT" for i in range(values.shape[1])])


def test_permutation_with_gaps_finds_a_planted_signal():
    rng = np.random.default_rng(4)
    signal = _frame(rng.normal(size=(300, 60)))
    label = 0.4 * signal + _frame(rng.normal(size=(300, 60)))
    late_listed = signal.columns[:15]
    signal.loc[signal.index < signal.index[120], late_listed] = np.nan  # coins listed later
    perm = study.coin_permutation(signal, label, reps=200)
    assert perm["ic"] > 0.25 and perm["p"] == pytest.approx(1 / 201)


def test_permutation_rejects_a_persistent_coin_trait():
    rng = np.random.default_rng(5)
    trait, drift = rng.normal(size=60), rng.normal(size=60)
    signal = _frame(np.tile(trait, (300, 1)) + 0.05 * rng.normal(size=(300, 60)))
    label = _frame(np.tile(drift, (300, 1)) + 0.3 * rng.normal(size=(300, 60)))
    ic = study.row_ic(signal, label)
    assert ic.std() < 0.1  # every day repeats the same ranking: a day bootstrap would call it significant
    assert study.coin_permutation(signal, label, reps=200)["p"] > 0.05


def test_quintile_spread_charges_turnover_once_per_offset():
    signal = _frame(np.tile(np.arange(40, dtype=float), (300, 1)))  # a fixed ranking never turns over
    fwd = _frame(np.tile(np.arange(40, dtype=float) * 0.003, (300, 1)))  # 3-day returns
    out = study.quintile_spread(signal, fwd, direction=1.0, horizon=3, cost_round_trip=0.001)
    spread = (np.arange(32, 40).mean() - np.arange(0, 8).mean()) * 0.003
    assert out["gross_per_day"] == pytest.approx(spread / 3)
    assert out["net_per_day"] == pytest.approx((spread - 2 * 0.001 * 3 / 300) / 3, rel=0.02)


class _Relay:
    """Serves 1100 daily rows but at most 1000 per response, from one end of the asked window."""

    def __init__(self, keep: str):
        self.keep, self.calls = keep, 0
        self.rows = [{"time": study._ms(study.START) + i * DAY_MS, "close": str(i)} for i in range(1100)]

    async def request_json(self, path, params=None, manual=False):
        self.calls += 1
        window = [r for r in self.rows if params["start_time"] <= r["time"] <= params["end_time"]]
        window = window[-1000:] if self.keep == "latest" else window[:1000]
        return {"payload": {"code": "0", "data": window}}


@pytest.mark.parametrize("keep", ["latest", "earliest"])
def test_history_fetches_both_ends_of_a_capped_range(monkeypatch, keep):
    monkeypatch.setattr(study, "REQUEST_SPACING_SEC", 0.0)
    monkeypatch.setattr(study, "END", study.START + pd.Timedelta(days=1100).to_pytimedelta())
    relay = _Relay(keep)
    rows = asyncio.run(study._history(relay, "oi", "AAA", "AAAUSDT", [0.0]))
    assert len(rows) == 1100 and relay.calls == 2
