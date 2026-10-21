from __future__ import annotations

import asyncio
import copy
import math
from datetime import datetime, timedelta, timezone
from statistics import stdev

import pandas as pd
import pytest

from core.research import xs_reversal_tracker as tracker
from core.research.paired_evaluation import evaluate_reversal_pairs


D0 = datetime(2026, 10, 5, tzinfo=timezone.utc)


def position(price_return=0., funding=0., quote=100.):
    return {"entry": 100., "exit": 100.*(1+price_return), "funding": funding,
            "return": price_return-funding, "mark_at_formation": quote}


def day_record(day=D0, *, main=0., shadow=1.):
    # One shared short earns zero; the long contributes half of total gross.
    record = {"status": "closed", "late": False, "formed_at": (day+timedelta(minutes=4)).isoformat(),
              "entry_bar_ms": int((day-timedelta(hours=1)).timestamp()*1000),
              "net_per_position_pct": main, "missing_positions": 0,
              "longs": {"OLD": position((main+.15)/50)},
              "shorts": {"SHORT": position()},
              "liq_filter": {"status": "closed", "date": day.date().isoformat(),
                             "net_per_position_pct": shadow, "diff_pct": shadow-main,
                             "longs": {"NEW": position((shadow+.15)/50)}}}
    return record


def many_days(n=8):
    return {(D0+timedelta(days=i)).date().isoformat(): day_record(D0+timedelta(days=i), main=i/10, shadow=1+i/5)
            for i in range(n)}


def test_only_same_common_dates_count_and_stored_delta_is_not_taken_on_trust():
    days = many_days(4)
    days["2026-10-05"]["liq_filter"]["status"] = "no_data"
    days["2026-10-06"]["liq_filter"]["diff_pct"] = 999.
    days["2026-10-08"]["late"] = True
    days["2026-10-09"] = day_record(D0+timedelta(days=4), main=100., shadow=200.)
    days["2026-10-09"]["status"] = "open"
    before = copy.deepcopy(days)
    result = evaluate_reversal_pairs(days)
    assert result["n_pairs"] == 2
    assert result["common_dates"] == ["2026-10-06", "2026-10-07"]
    assert result["baseline"]["mean_pct"] == pytest.approx(.15)
    assert result["shadow"]["mean_pct"] == pytest.approx(1.3)
    assert result["increment"]["mean_pct"] == pytest.approx(1.15)
    assert result["increment"]["positive_days"] == 2
    assert result["daily"][0]["recorded_increment_pp"] == 999.
    assert result["eligible_baseline_days"] == 3
    assert days == before


@pytest.mark.parametrize("change", ["date", "entry_bar_ms"])
def test_mismatched_shadow_date_or_holding_period_is_excluded(change):
    record = day_record()
    record["liq_filter"][change] = "2026-10-06" if change == "date" else record["entry_bar_ms"]+3600000
    result = evaluate_reversal_pairs({"2026-10-05": record})
    assert result["n_pairs"] == 0
    assert result["exclusions"]["shadow_date_or_interval_mismatch"] == 1
    assert result["baseline"]["mean_pct"] is None


def test_missing_return_is_excluded_and_missing_legs_are_visible_without_inventing_prices():
    days = many_days(3)
    days["2026-10-05"]["liq_filter"]["net_per_position_pct"] = float("nan")
    days["2026-10-06"]["longs"]["OLD"].pop("exit")
    days["2026-10-07"]["liq_filter"]["longs"] = {}
    result = evaluate_reversal_pairs(days)
    assert result["n_pairs"] == 2  # both are recorded ledger outcomes, with incomplete evidence flagged
    assert result["attribution"]["complete_days"] == 0
    assert result["attribution"]["missing_days"] == 2
    assert result["attribution"]["mean_removed_price_pp"] is None
    assert result["attribution"]["days"][0]["missing_main_longs"] == ["OLD"]
    assert result["execution_diagnostics"]["days"][1]["shadow_signed_entry_disadvantage_pp"] is None
    assert result["exclusions"]["missing_shadow_return"] == 1


def test_unequal_leg_sizes_reconcile_removed_replacement_and_shared_reweight_with_funding():
    record = day_record()
    record["longs"] = {"REMOVED": position(-.2, -.01), "SHARED": position(.1, .02)}
    record["liq_filter"]["longs"] = {"SHARED": position(.1, .02), "NEW": position(.3, .03),
                                         "NEW2": position(.2, .01)}
    baseline = ((-.2+.01+.1-.02)/2)/2*100-.15
    shadow = ((.1-.02+.3-.03+.2-.01)/3)/2*100-.15
    record["net_per_position_pct"] = baseline
    record["liq_filter"].update(net_per_position_pct=shadow, diff_pct=shadow-baseline)
    result = evaluate_reversal_pairs({"2026-10-05": record})
    attribution = result["attribution"]["days"][0]
    assert attribution["status"] == "complete"
    assert attribution["removed"] == ["REMOVED"]
    assert attribution["replacement"] == ["NEW", "NEW2"]
    assert attribution["shared"] == ["SHARED"]
    assert attribution["removed_price_pp"] == pytest.approx(5.)
    assert attribution["removed_funding_pp"] == pytest.approx(-.25)
    assert attribution["replacement_price_pp"] == pytest.approx(25/3)
    assert attribution["replacement_funding_pp"] == pytest.approx(-2/3)
    assert attribution["shared_reweight_price_pp"] == pytest.approx(-5/6)
    assert attribution["shared_reweight_funding_pp"] == pytest.approx(1/6)
    assert attribution["explained_increment_pp"] == pytest.approx(shadow-baseline)
    assert attribution["reconciliation_error_pp"] == pytest.approx(0.)


def test_bad_ledger_reconciliation_is_reported_not_silently_accepted():
    record = day_record()
    record["liq_filter"]["net_per_position_pct"] += 5.
    result = evaluate_reversal_pairs({"2026-10-05": record})
    assert result["attribution"]["unreconciled_days"] == 1
    assert result["attribution"]["complete_days"] == 0
    assert result["attribution"]["days"][0]["reconciliation_error_pp"] == pytest.approx(5.)


def test_eight_days_and_many_legs_never_become_confirmation_or_robust_intervals():
    days = many_days()
    # Adding duplicate economics as separate positions must not increase n.
    for record in days.values():
        record["longs"] = {str(i): position() for i in range(30)}
    result = evaluate_reversal_pairs(days)
    assert result["n_pairs"] == 8
    assert result["sample_unit"] == "common_calendar_day"
    assert result["status"] == "insufficient_data"
    assert result["confirmation_status"] == "not_assessed"
    for method in ("weekly", "hac"):
        assert result["sensitivity"][method]["status"] == "insufficient_data"
        assert result["sensitivity"][method]["ci90_pp"] is None
    assert result["sensitivity"]["weekly"]["n_week_groups"] == 2


def test_weekly_and_hac_are_sensitivity_only_and_use_calendar_lags():
    days = many_days(70)
    del days["2026-10-10"]
    result = evaluate_reversal_pairs(days)
    weekly, hac = result["sensitivity"]["weekly"], result["sensitivity"]["hac"]
    assert result["n_pairs"] == 69
    assert weekly["n_week_groups"] == 10
    assert weekly["status"] == hac["status"] == "sensitivity_only"
    assert weekly["ci90_pp"][0] < result["increment"]["mean_pct"] < weekly["ci90_pp"][1]
    assert hac["ci90_pp"][0] < result["increment"]["mean_pct"] < hac["ci90_pp"][1]
    plain_se = stdev(row["increment_pp"] for row in result["daily"])/math.sqrt(result["n_pairs"])
    assert hac["standard_error_pp"] > plain_se  # persistent consecutive returns carry less independent information
    assert hac["calendar_lag_pair_counts"]["1"] == 67  # missing day is not compressed away
    assert result["confirmation_status"] == "not_assessed"


def test_overlapping_holding_intervals_prevent_false_independent_sensitivity():
    days = many_days(70)
    days["2026-10-06"]["entry_bar_ms"] = days["2026-10-05"]["entry_bar_ms"]+12*3600000
    result = evaluate_reversal_pairs(days)
    assert ["2026-10-05", "2026-10-06"] in result["sensitivity"]["holding_overlap_pairs"]
    assert result["sensitivity"]["weekly"]["status"] == "insufficient_data"
    assert result["sensitivity"]["hac"]["ci90_pp"] is None


def test_leave_best_day_out_keeps_both_versions_on_the_same_remaining_days_and_costs_separate():
    result = evaluate_reversal_pairs(many_days(3))
    leave = result["sensitivity"]["leave_best_increment_day_out"]
    assert leave["excluded_date"] == "2026-10-07"
    assert leave["n_pairs"] == 2
    assert leave["baseline_mean_pct"] == pytest.approx(.05)
    assert leave["shadow_mean_pct"] == pytest.approx(1.1)
    assert leave["increment_mean_pp"] == pytest.approx(1.05)
    costs = result["sensitivity"]["cost"][-1]
    assert costs["baseline_mean_with_common_extra_cost_pct"] == pytest.approx(-.1)
    assert costs["shadow_mean_with_common_extra_cost_pct"] == pytest.approx(1.)
    assert costs["increment_with_common_extra_cost_pp"] == pytest.approx(1.1)
    assert costs["increment_with_shadow_only_extra_cost_pp"] == pytest.approx(.9)


def test_quotes_report_missing_replacements_and_actual_shadow_delay_without_executable_returns():
    record = day_record()
    record["longs"]["OLD"]["mark_at_formation"] = 102.
    record["shorts"]["SHORT"]["mark_at_formation"] = 99.
    record["liq_filter"]["longs"]["NEW"].pop("mark_at_formation")
    record["formation_audit"] = {"signal_ready_at": (D0+timedelta(minutes=10)).isoformat(),
                                  "price_snapshot_observed_at": (D0+timedelta(minutes=11)).isoformat(),
                                  "shadow_ready_at": (D0+timedelta(minutes=42)).isoformat()}
    result = evaluate_reversal_pairs({"2026-10-05": record})
    diag = result["execution_diagnostics"]["days"][0]
    assert diag["formation_lag_minutes"] == 4.
    assert diag["shadow_ready_lag_minutes"] == 42.
    assert diag["quote_age_at_shadow_ready_seconds"] == 31*60
    assert diag["quote_coverage"]["shadow_longs"]["missing_symbols"] == ["NEW"]
    assert diag["baseline_signed_entry_disadvantage_pp"] == pytest.approx(1.5)
    assert diag["shadow_signed_entry_disadvantage_pp"] is None
    assert diag["executable_return_available"] is False
    assert result["execution_diagnostics"]["shadow_ready_after_30min_days"] == 1


def test_summary_exposes_read_only_pairs_without_changing_frozen_verdict_or_history():
    days = many_days(8)
    state = {"started_at": D0.isoformat(), "days": days,
             "retirement_rule": {"min_n": 180, "backtest_mean_pct": .057},
             "liq_filter_rule": {"min_n": 180, "backtest_mean_pct": .031}}
    before = copy.deepcopy(state)
    summary = tracker.summary(state)
    assert summary["paired_evaluation"]["n_pairs"] == 8
    assert summary["liq_filter"]["paired_evaluation"] is summary["paired_evaluation"]
    assert summary["retirement"]["n"] == summary["liq_filter"]["retirement"]["n"] == 8
    assert summary["retirement"]["verdict"] == "collecting"
    assert summary["liq_filter"]["retirement"]["verdict"] == "collecting"
    assert state == before


def test_new_formations_record_shadow_quote_and_timing_without_changing_entries(monkeypatch):
    symbols = [f"C{i:02d}USDT" for i in range(40)]
    stamp = [D0+timedelta(minutes=4)]
    monkeypatch.setattr(tracker, "_now", lambda: stamp[0])
    async def closes(client, symbol, entry_ms):
        index = symbols.index(symbol)
        return pd.Series([100.] * 50+[100.-index/10])
    monkeypatch.setattr(tracker, "_closes_until", closes)
    async def shadow(*args):
        stamp[0] += timedelta(minutes=40)
        return {"status": "open", "longs": ["C35USDT", "C36USDT", "C37USDT"], "flagged": ["C39USDT"]}
    monkeypatch.setattr(tracker, "_liq_filter_legs", shadow)
    class Resp:
        status_code = 200
        def __init__(self, payload): self.payload = payload
        def json(self): return self.payload
        def raise_for_status(self): pass
    class Exchange:
        async def get(self, url):
            if url.endswith("/exchangeInfo"):
                return Resp({"symbols": [{"symbol": symbol, "contractType": "PERPETUAL", "quoteAsset": "USDT", "status": "TRADING"}
                                         for symbol in symbols]})
            return Resp([{"symbol": symbol, "price": "105"} for symbol in symbols])
    record = asyncio.run(tracker._form(Exchange(), {"universe": [symbol[:-4] for symbol in symbols]}, D0, stamp[0]))
    assert record["late"] is False  # registered tick-start convention remains intact
    assert record["longs"]["C39USDT"]["entry"] == pytest.approx(96.1)
    assert record["liq_filter"]["longs"]["C35USDT"]["mark_at_formation"] == 105.
    assert record["liq_filter"]["date"] == "2026-10-05"
    assert record["formation_audit"]["shadow_ready_at"] == (D0+timedelta(minutes=44)).isoformat()
    assert record["formation_audit"]["price_snapshot_observed_at"] == (D0+timedelta(minutes=4)).isoformat()
