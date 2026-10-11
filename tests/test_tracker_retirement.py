from __future__ import annotations

import asyncio
import json
import sys
import types

import pytest

from core.research import exchange_research_runner as runner
from core.research import retirement

RULE = {"min_n": 12, "unit": "month", "backtest_mean_pct": 2.6}


@pytest.mark.parametrize("returns, expected", [
    ([], "collecting"),
    ([5.0, -1.0, 3.0], "collecting"),                 # below min_n // 2
    ([-3.0, -4.0, -2.5, -3.5, -5.0, -2.0], "retire"),  # clearly losing before min_n
    ([-3.0, 4.0, -2.5, 1.0, -5.0, 2.0], "watch"),      # negative mean, not significant
    ([4.0, -2.0] * 6, "on_track"),                       # positive, not significant, above decay bar
    ([3.0, 2.5, 3.5, 2.8, 3.1, 2.9] * 2, "confirmed"),
    ([1.0, -1.0] * 6, "retire"),                       # mean 0 after min_n
    ([0.3, 0.1, 0.2, 0.4, 0.2, 0.3] * 2, "retire"),    # significant but decayed below 1/4 of backtest
])
def test_verdicts(returns, expected):
    assert retirement.verdict(returns, RULE)["verdict"] == expected


def test_registered_rule_is_frozen_in_state(monkeypatch):
    state = {}
    retirement.register(state, "supply_factor", "2026-09-27T00:00:00+00:00")
    monkeypatch.setitem(retirement.RULES, "supply_factor", {"min_n": 99, "unit": "month", "backtest_mean_pct": 50})
    retirement.register(state, "supply_factor", "2026-10-01T00:00:00+00:00")
    assert state["retirement_rule"]["min_n"] == 12 and state["retirement_rule"]["registered_at"].startswith("2026-09-27")


def test_verdict_alert_fires_once_per_change(tmp_path, monkeypatch):
    sent = []

    class Manager:
        async def send_message(self, title, body):
            sent.append(title)

    monkeypatch.setitem(sys.modules, "core.notifications", types.SimpleNamespace(notification_manager=Manager()))
    monkeypatch.setattr(runner, "VERDICT_STATE_PATH", tmp_path / "verdicts.json")
    retire = {"retirement": retirement.verdict([-3.0, -4.0, -2.5, -3.5, -5.0, -2.0], RULE)}
    asyncio.run(runner._verdict_alert("supply_factor", {"retirement": retirement.verdict([], RULE)}))
    asyncio.run(runner._verdict_alert("supply_factor", retire))
    asyncio.run(runner._verdict_alert("supply_factor", retire))
    assert len(sent) == 1 and "供给通胀因子" in sent[0]
    assert json.loads((tmp_path / "verdicts.json").read_text(encoding="utf-8")) == {"supply_factor": "retire"}


def _width(v):
    return v["ci90_pct"][1] - v["ci90_pct"][0]


def test_clustered_interval_equals_the_plain_one_with_one_result_per_cluster():
    returns = [4.0, -1.0, 6.0, 2.5, -3.0, 5.0, 1.0, 3.5]
    plain = retirement.verdict(returns, RULE)
    singletons = retirement.verdict(returns, RULE, clusters=list(range(len(returns))))
    assert singletons["ci90_pct"] == plain["ci90_pct"] and singletons["clusters"] == len(returns)
    assert "clusters" not in plain


def test_results_repeated_inside_a_cluster_add_no_evidence():
    """Two coins per notice that move identically: the clustered interval is that of the notices alone."""
    notices = [4.0, -1.0, 6.0, 2.5]
    doubled = retirement.verdict(notices * 2, RULE, clusters=[0, 1, 2, 3] * 2)
    assert doubled["clusters"] == 4 and doubled["n"] == 8
    assert doubled["ci90_pct"] == retirement.verdict(notices, RULE)["ci90_pct"]
    assert _width(doubled) > _width(retirement.verdict(notices * 2, RULE))  # the naive one is too narrow


def test_a_single_cluster_gives_no_interval_and_cannot_confirm():
    returns = [3.0, 3.2, 2.9, 3.1] * 3
    assert retirement.verdict(returns, RULE)["verdict"] == "confirmed"  # as if 12 independent results
    one = retirement.verdict(returns, RULE, clusters=["notice-1"] * 12)
    assert one["ci90_pct"] is None and one["clusters"] == 1 and one["verdict"] == "on_track"


def test_cluster_keys_must_align_and_missing_keys_stand_alone():
    with pytest.raises(ValueError):
        retirement.verdict([1.0, 2.0], RULE, clusters=[1])
    v = retirement.verdict([1.0, 2.0, None, 4.0], RULE, clusters=[None, None, "x", "y"])
    assert v["n"] == 3 and v["clusters"] == 3  # the None return is dropped with its key
