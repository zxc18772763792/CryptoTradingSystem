from __future__ import annotations

import pandas as pd
import pytest

from scripts.research_alastar_responses import available, trigger_entry, evaluate, funding_range, book, curve_stats, utc, horizon_events


def event(**changes):
    return {"event_id": "s:e1", "source_id": "s", "symbol": "BTCUSDT", "direction": 1,
            "published_at": "2026-01-01T00:03:00Z", "reaction_type": "immediate_view", **changes}


class Market:
    def __init__(self):
        index = pd.date_range("2025-12-31", periods=1000, freq="5min", tz="UTC")
        self.p = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100., "volume": 10.},index=index)
        self.f = pd.DataFrame({"funding_rate": 0., "mark_price": 100.},
                              index=pd.date_range("2025-12-31", periods=15, freq="8h", tz="UTC"))
    def prices_for(self, symbol): return self.p
    def funding_for(self, symbol): return self.f


def test_timezone_and_processing_latency_never_use_previous_bar():
    assert available(event(published_at="2026-01-01T08:03:00+08:00"), 1) == utc("2026-01-01T00:05:00Z")
    assert available(event(published_at="2026-01-01T00:04:30Z"), 1) == utc("2026-01-01T00:10:00Z")


def test_numeric_cross_only_trades_after_confirming_close_and_processing():
    m = Market()
    m.p.loc[utc("2026-01-01T00:10:00Z"), "close"] = 110.
    e = event(reaction_type="price_condition", condition_type="close_above", condition_level=105.)
    at, error = trigger_entry(e, m.p, 1)
    #00:10 bar closes00:15, +60s -> next00:20 open. Not00:10 or00:15.
    assert error is None and at == utc("2026-01-01T00:20:00Z")


def test_already_beyond_level_without_new_cross_does_not_trigger():
    m = Market(); m.p["close"] = 110.
    e = event(reaction_type="price_condition", condition_type="close_above", condition_level=105.)
    assert trigger_entry(e, m.p, 1)[1] == "numeric_cross_not_triggered_in_24h"


def test_gap_after_already_confirmed_trigger_does_not_remove_it():
    m = Market(); m.p.loc[utc("2026-01-01T00:10:00Z"), "close"] = 110.
    m.p = m.p.drop(utc("2026-01-01T18:00:00Z"))
    e = event(reaction_type="price_condition", condition_type="close_above", condition_level=105.)
    assert trigger_entry(e,m.p,1)[0] == utc("2026-01-01T00:20:00Z")


def test_state_policy_can_react_to_condition_already_true_at_message_time():
    m = Market(); m.p["close"] = 110.
    e = event(reaction_type="price_condition",condition_type="close_above",condition_level=105.)
    assert trigger_entry(e,m.p,1,policy="state")[0] == utc("2026-01-01T00:05:00Z")


def test_hold_confirmation_requires_two_closed_bars_then_processing():
    m = Market()
    m.p.loc[utc("2026-01-01T00:10:00Z"),"close"] = 110.
    m.p.loc[utc("2026-01-01T00:15:00Z"),"close"] = 110.
    e = event(reaction_type="price_condition",condition_type="close_above",condition_level=105.,
              condition_quote="If it breaks and holds above105.")
    assert trigger_entry(e,m.p,1)[0] == utc("2026-01-01T00:25:00Z")


def test_hold_confirmation_cannot_extend_past_trigger_expiry():
    m=Market()
    m.p.loc[utc("2026-01-02T00:00:00Z"),"close"]=110.
    m.p.loc[utc("2026-01-02T00:05:00Z"),"close"]=110.
    e=event(reaction_type="price_condition",condition_type="close_above",condition_level=105.,
            condition_quote="break and holds above105")
    assert trigger_entry(e,m.p,1)[1] == "numeric_cross_not_triggered_in_24h"


def test_official_complete_query_can_prove_no_payments_without_bracket_rows():
    f=pd.DataFrame(columns=["funding_rate","mark_price"])
    r=funding_range(f,utc("2026-01-01T00:00:00Z"),utc("2026-01-01T00:30:00Z"),100.,1,complete_query=True)
    assert r["lower"] == 0.
    assert funding_range(f,utc("2026-01-01T00:00:00Z"),utc("2026-01-01T00:30:00Z"),100.,1) is None


def test_same_horizon_opposed_views_are_all_counted_as_unresolved():
    a=event(explicit_horizon_quote="short-term")
    b=event(direction=-1,explicit_horizon_quote="short-term")
    selected,excluded=horizon_events([a,b],4)
    assert selected == [] and excluded == 2


def test_distinct_horizon_opposed_views_use_stated_horizon_bucket():
    a=event(explicit_horizon_quote="short-term")
    b=event(direction=-1,explicit_horizon_quote="higher timeframe")
    assert horizon_events([a,b],4)[0] == [a]
    assert horizon_events([a,b],72)[0] == [b]


def test_missing_bar_does_not_get_interpolated_from_hourly_data():
    m = Market(); m.p = m.p.drop(utc("2026-01-01T00:30:00Z"))
    assert evaluate(event(), m, 1, 1)["status"] == "holding_window_gap"


def test_missing_funding_remains_unknown_even_for_short_hold():
    m = Market(); m.f = pd.DataFrame()
    assert evaluate(event(), m, .5, 1)["status"] == "funding_uncovered"


def test_funding_edges_are_bounded_and_signed_mark_notional_used():
    f = pd.DataFrame({"funding_rate": [-.01, -.02, .03, 0.], "mark_price": [100.,110.,100.,100.]},
                     index=pd.to_datetime(["2026-01-01T00:00:00Z","2026-01-01T01:00:00Z",
                                           "2026-01-01T02:00:00.002Z","2026-01-01T08:00:00Z"],format="mixed"))
    r = funding_range(f, utc("2026-01-01T00:00:00Z"), utc("2026-01-01T02:00:00Z"), 100., 1)
    assert r["lower"] == pytest.approx(.022-.03)
    assert r["upper"] == pytest.approx(.022+.01)


def test_isolated_loss_is_capped_not_more_than_full_collateral():
    m = Market(); m.p.loc[utc("2026-01-01T00:20:00Z"), "high"] = 210.
    r = evaluate(event(direction=-1), m, 1, 1)
    assert r["liquidated_1x_approx"] and r["net_return"] == -1.
    assert r["exit_price"] == 199.
    assert r["net_100bps"] == -1.


def test_later_data_gap_cannot_remove_an_earlier_liquidated_loss():
    m = Market(); m.p.loc[utc("2026-01-01T00:20:00Z"), "high"] = 210.
    m.p = m.p.drop(utc("2026-01-01T00:50:00Z"))
    r = evaluate(event(direction=-1),m,1,1)
    assert r["status"] == "ok" and r["net_return"] == -1.


def test_opposite_update_exit_precedes_max_hold_and_same_side_cannot_stack():
    m = Market()
    a = evaluate(event(), m, 4, 1, utc("2026-01-01T01:00:00Z"))
    b = evaluate(event(event_id="s2:e1", published_at="2026-01-01T00:10:00Z"), m, 4, 1)
    chosen, skipped = book([a,b], m)
    assert a["until_update_exit"] and a["exit_at"] == str(utc("2026-01-01T01:00:00Z"))
    assert len(chosen) == 1 and skipped["overlap_same_symbol"] == 1
    summary, curve = curve_stats(chosen,m)
    assert curve[-1]["equity"] == pytest.approx(1-.2*.002)
    assert summary["max_5m_open_drawdown"] == pytest.approx(-.0004)
