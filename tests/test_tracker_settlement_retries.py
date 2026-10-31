"""Settlement retries must finish the original exit, not create another trade."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from core.research import coinglass_composite_tracker as composite
from core.research import unlock_short_tracker as unlock


@pytest.mark.parametrize("failure", ["missing", "connection_error"])
def test_composite_funding_retry_keeps_first_observed_exit_and_funding_cutoff(monkeypatch, failure):
    now = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
    record = {"formed_at": (now - timedelta(days=7)).isoformat(),
              "main": {"status": "open", "turnover": 1.0,
                       "longs": {"AAA": {"entry": 100.0}}, "shorts": {"BBB": {"entry": 100.0}}}}
    calls = []
    async def funding(client, symbol, lo, hi):
        calls.append((symbol, hi))
        if symbol == "AAA" and sum(s == "AAA" for s, _ in calls) == 1:
            if failure == "connection_error":
                raise ConnectionError("temporary funding failure")
            return None
        return 0.0
    monkeypatch.setattr(composite, "_funding", funding)
    asyncio.run(composite.settle(None, record, now, {"AAA": 105.0, "BBB": 100.0}))
    assert record["main"]["status"] == "open"
    asyncio.run(composite.settle(None, record, now + timedelta(days=1), {"AAA": 200.0, "BBB": 100.0}))
    pos = record["main"]["longs"]["AAA"]
    assert pos["exit"] == 105.0 and pos["return"] == pytest.approx(.05)
    assert [hi for s, hi in calls if s == "AAA"] == [int(now.timestamp()*1000)] * 2


def test_composite_closed_book_is_immutable_during_retry(monkeypatch):
    record = {"formed_at": "2026-10-01T00:00:00+00:00", "main": {"status": "closed", "net_pct": 1.25}}
    original = copy.deepcopy(record)
    monkeypatch.setattr(composite, "_funding", AsyncMock(side_effect=AssertionError("No funding for a closed book")))
    asyncio.run(composite.settle(None, record, datetime.now(timezone.utc), {}))
    assert record == original


def test_new_unlock_closed_coin_can_finish_pending_hedge_without_repricing_coin(tmp_path, monkeypatch):
    day = pd.Timestamp("2026-10-20", tz="UTC")
    now = day + pd.Timedelta(days=6)
    trade = {"token": "AAA", "symbol": "AAAUSDT", "entry_day": str(day.date()),
             "exit_day": str((day + pd.Timedelta(days=4)).date()), "unlock_date": "2026-11-20",
             "entry_price": 1.0, "status": "open", "late": False,
             "basket": [f"H{i}" for i in range(6)]}
    complete = False
    funding = AsyncMock(return_value=0.0)
    async def leg(client, symbol, entry_day, exit_day, side):
        return .01 if complete or symbol in {"H0", "H1", "H2"} else None
    monkeypatch.setattr(unlock, "_now", lambda: now)
    monkeypatch.setattr(unlock, "_funding", funding)
    monkeypatch.setattr(unlock, "_leg", leg)
    bars = pd.DataFrame({"close": [.9]}, index=[day + pd.Timedelta(days=4)])
    asyncio.run(unlock._close(None, trade, day + pd.Timedelta(days=4), "closed", bars))
    assert trade["hedged_return_pct"] is None
    short_result = trade["short_return_pct"]
    state = {"started_at": day.isoformat(), "trades": {"AAA": trade}}
    path = tmp_path/"state.json"
    unlock.save_state(state, path)
    complete = True
    monkeypatch.setattr(unlock, "build_universe", AsyncMock(return_value={}))
    monkeypatch.setattr(unlock.event_risk_shadow, "apply_to_tracker", AsyncMock())
    result = asyncio.run(unlock.tick(None, state_path=path))
    after = unlock.load_state(path)["trades"]["AAA"]
    assert result["forward_completed"] == 1
    assert after["short_return_pct"] == short_result
    assert after["hedged_return_pct"] == pytest.approx(short_result + 1.0)
    assert funding.await_count == 1  # retry only the missing basket, never the exited coin


def test_legacy_unlock_terminal_record_is_not_rebuilt(tmp_path, monkeypatch):
    trade = {"token": "AAA", "symbol": "AAAUSDT", "status": "closed", "late": False,
             "entry_day": "2026-10-01", "exit_day": "2026-10-10", "hedged_return_pct": None}
    state = {"started_at": "2026-10-01T00:00:00+00:00", "trades": {"AAA": copy.deepcopy(trade)}}
    path = tmp_path/"state.json"
    unlock.save_state(state, path)
    monkeypatch.setattr(unlock, "build_universe", AsyncMock(return_value={}))
    monkeypatch.setattr(unlock.event_risk_shadow, "apply_to_tracker", AsyncMock())
    monkeypatch.setattr(unlock, "_leg", AsyncMock(side_effect=AssertionError("Legacy book immutable")))
    asyncio.run(unlock.tick(None, state_path=path))
    assert unlock.load_state(path)["trades"]["AAA"] == trade
