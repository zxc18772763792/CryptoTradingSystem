from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.research_alastar_selection_timing import alt_index, forward_returns, paired_test, selection_rows


def wide(columns: dict[str, list[float]]) -> pd.DataFrame:
    n = len(next(iter(columns.values())))
    return pd.DataFrame(columns, index=pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC"), dtype=float)


def test_forward_returns_are_open_to_open_and_never_fill_gaps():
    opens = wide({"AUSDT": [100, 110, np.nan, 121]})
    fwd = forward_returns(opens, 1)
    assert fwd["AUSDT"].iloc[0] == pytest.approx(.10)
    assert np.isnan(fwd["AUSDT"].iloc[1]) and np.isnan(fwd["AUSDT"].iloc[2]) and np.isnan(fwd["AUSDT"].iloc[3])


def test_alt_index_excludes_majors_and_winsorises_one_pump():
    fwd = wide({"BTCUSDT": [.5], "ETHUSDT": [.5], **{f"C{i}USDT": [.01] for i in range(99)}, "PUMPUSDT": [50.0]})
    assert alt_index(fwd).iloc[0] < .02  # a 5000% listing pump and the majors do not drive the index


def test_paired_test_uses_the_difference_itself_and_clusters_by_week():
    stamps = list(pd.date_range("2026-01-05", periods=40, freq="D", tz="UTC"))
    flat = paired_test(np.zeros(40), stamps)
    assert flat["mean"] == 0 and flat["p_le_zero"] == pytest.approx(1.0)
    up = paired_test(np.full(40, .01), stamps)
    assert up["ci95_lo"] == pytest.approx(.01) and up["p_le_zero"] < .001
    assert up["weeks"] == 6


def test_selection_rules_read_only_the_past_and_sign_the_author_side():
    n = 600
    rise = 100 * 1.01 ** np.arange(n)  # +1%/h uptrend: 7d momentum says long, 48h reversal says short
    peers = {f"P{i}USDT": list(np.full(n, 10.0)) for i in range(6)}
    opens = wide({"XUSDT": list(rise), "BTCUSDT": list(np.full(n, 1.0)), **peers})
    qvol = opens.copy() * 0 + 1.0
    published = str(opens.index[400] - pd.Timedelta(minutes=30))
    rows = selection_rows([{"signal_id": "s", "kind": "fresh_call", "symbol": "XUSDT", "side": -1,
                            "published_at": published}], opens, qvol)
    one = next(r for r in rows if r["hours"] == 1)
    assert one["entry_at"] == opens.index[400]
    assert one["agree_mom7d"] is False and one["agree_rev48h"] is True
    assert one["net"] < 0 and one["net_mom7d"] > 0 and one["net_rev48h"] == pytest.approx(one["net"])
    assert one["peer_matched"] == pytest.approx(-.002)  # flat peers only pay the round-trip cost
