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
