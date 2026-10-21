from __future__ import annotations

import copy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.research import coinglass_attribution as ca


def book(net=1.0, turnover=1.0):
    return {"status": "closed", "net_pct": net, "cost_pct": 0.24 * turnover, "turnover": turnover,
            "missing_positions": 0, "scored": 40,
            "longs": {"AUSDT": {"entry": 100, "exit": 110, "funding": 0.01}},
            "shorts": {"BUSDT": {"entry": 100, "exit": 95, "funding": 0.02}}}


def record(day, main=1.0, shadow=3.0, hour=3):
    formed = datetime.combine(date.fromisoformat(day), datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1, hours=hour)
    result = {"formed_at": formed.isoformat(), "coins": 40, "partial": False,
              "main": book(main), "shadow": book(shadow)}
    for key in ("main", "shadow"):
        result[key]["settled_at"] = (formed + timedelta(days=7)).isoformat()
    return result


def states(days=("2026-10-01", "2026-10-02")):
    original = {"books": {d: record(d, main=1, shadow=3) for d in days}}
    # The wide tracker's main/shadow assignments are intentionally reversed.
    wide = {"books": {d: record(d, main=9, shadow=5, hour=5) for d in days}}
    return original, wide


def test_factorial_separates_weighting_and_expansion_on_the_same_four_cell_dates():
    original, wide = states()
    out = ca.load_attribution(original, wide)
    assert out["cells"]["original_ic"]["book_key"] == "main"
    assert out["cells"]["wide_ic"]["book_key"] == "shadow"
    assert out["comparisons"]["weighting_original"]["mean_pct"] == 2
    assert out["comparisons"]["weighting_wide"]["mean_pct"] == 4
    assert out["comparisons"]["expansion_ic"]["mean_pct"] == 4
    assert out["comparisons"]["expansion_equal"]["mean_pct"] == 6
    common = out["factorial_common"]
    assert common["signal_dates"] == ["2026-10-01", "2026-10-02"]
    assert common["effects"]["weighting"]["mean_pct"] == 3
    assert common["effects"]["expansion"]["mean_pct"] == 5
    assert common["effects"]["interaction"]["mean_pct"] == 2
    # Two books from one week cannot produce an uncertainty estimate.
    assert common["effects"]["weighting"]["weekly_groups"] == 1
    assert common["effects"]["weighting"]["ci90_pct"] is None
    assert out["status"] == "insufficient_data"


def test_pairing_is_by_signal_date_not_position_or_unmatched_cell_averages():
    original, wide = states(("2026-10-01", "2026-10-02", "2026-10-03"))
    wide["books"].pop("2026-10-01")
    wide["books"]["2026-10-04"] = record("2026-10-04", main=999, shadow=-999, hour=5)
    wide["books"]["2026-10-03"]["shadow"]["status"] = "open"
    out = ca.load_attribution(original, wide)
    assert out["comparisons"]["expansion_equal"]["signal_dates"] == ["2026-10-02", "2026-10-03"]
    assert out["comparisons"]["expansion_ic"]["signal_dates"] == ["2026-10-02"]
    assert out["factorial_common"]["signal_dates"] == ["2026-10-02"]
    assert out["factorial_common"]["comparisons"]["weighting_original"]["n_pairs"] == 1
    assert out["comparisons"]["weighting_original"]["n_pairs"] == 3


def test_unsettled_wide_books_leave_cross_universe_attribution_unavailable():
    original, wide = states()
    for value in wide["books"].values():
        for key in ("main", "shadow"):
            value[key]["status"] = "open"
    out = ca.load_attribution(original, wide)
    assert out["status"] == "insufficient_data"
    assert out["cells"]["wide_equal"]["n_completed"] == 0
    assert out["comparisons"]["weighting_original"]["n_pairs"] == 2
    assert out["comparisons"]["expansion_equal"]["n_pairs"] == 0
    assert out["comparisons"]["expansion_equal"]["mean_pct"] is None
    assert out["factorial_common"]["n_pairs"] == 0
    # Formation delays are still observable before either book settles.
    assert out["timing"]["n_dates"] == 2
    assert out["timing"]["mean_delay_minutes"] == 120


def test_price_funding_cost_units_and_stress_charge_both_legs():
    original = {"books": {"2026-10-01": record("2026-10-01", main=15.76, shadow=15.76)}}
    out = ca.load_attribution(original, {})
    row = out["cells"]["original_ic"]["rows"][0]
    assert row["long_price_pct"] == pytest.approx(10)
    assert row["short_price_pct"] == pytest.approx(5)
    assert row["long_funding_pct"] == pytest.approx(-1)
    assert row["short_funding_pct"] == pytest.approx(2)
    assert row["price_pct"] == pytest.approx(15)
    assert row["funding_pct"] == pytest.approx(1)
    assert row["long_cost_pct"] == pytest.approx(0.12)
    assert row["short_cost_pct"] == pytest.approx(0.12)
    assert row["reconciliation_error_pct"] == pytest.approx(0)
    assert row["stress_extra_cost_pct"] == pytest.approx(0.16)
    assert row["stress_net_pct"] == pytest.approx(15.60)
    assert out["cost_model"]["stress_round_trip_pct_per_leg"] == 0.20


def test_asymmetric_leg_turnover_is_recovered_without_inventing_equal_costs():
    previous = record("2026-10-01")
    previous["main"]["shorts"] = {"CUSDT": {"entry": 100, "exit": 100, "funding": 0}}
    current = record("2026-10-08")
    current["main"].update(turnover=0.5, cost_pct=0.12, net_pct=15.88)
    original = {"books": {"2026-10-01": previous, "2026-10-08": current}}
    row = ca.load_attribution(original, {})["cells"]["original_ic"]["rows"][1]
    assert row["long_cost_pct"] == 0
    assert row["short_cost_pct"] == pytest.approx(0.12)
    assert row["stress_extra_cost_pct"] == pytest.approx(0.08)
    assert row["stress_net_pct"] == pytest.approx(15.8)
    # If archived holdings cannot reproduce the frozen turnover, keep unknown
    # per-leg costs null while retaining the observed aggregate net and stress.
    current["main"]["turnover"] = 0.25
    row = ca.load_attribution(original, {})["cells"]["original_ic"]["rows"][1]
    assert row["long_cost_pct"] is None and row["short_cost_pct"] is None
    assert row["net_pct"] == 15.88


def test_paired_cost_stress_accounts_for_each_books_own_turnover():
    original, wide = states()
    for value in original["books"].values():
        value["shadow"].update(turnover=0.5, cost_pct=0.12)
    comparison = ca.load_attribution(original, wide)["comparisons"]["weighting_original"]
    assert comparison["mean_pct"] == 2
    assert comparison["stress_stats"]["mean_pct"] == pytest.approx(2.08)
    assert comparison["mean_contributions"]["turnover"] == -0.5


def test_partial_closed_and_nonfinite_books_are_explicitly_excluded():
    original, wide = states()
    wide["books"]["2026-10-01"]["main"]["missing_positions"] = 1
    wide["books"]["2026-10-02"]["main"]["net_pct"] = float("nan")
    out = ca.load_attribution(original, wide)
    cell = out["cells"]["wide_equal"]
    assert cell["n_completed"] == 0
    assert [r["reason"] for r in cell["excluded"]] == ["incomplete_positions", "invalid_net_return"]
    assert out["factorial_common"]["n_pairs"] == 0


def test_missing_detailed_components_are_not_substituted_with_zero():
    original, wide = states()
    original["books"]["2026-10-01"]["main"]["longs"]["AUSDT"].pop("funding")
    out = ca.load_attribution(original, wide)
    row = out["cells"]["original_ic"]["rows"][0]
    assert row["funding_pct"] is None
    assert row["component_status"] == "unavailable"
    assert row["net_pct"] == 1
    assert out["comparisons"]["weighting_original"]["mean_contributions"]["funding_pct"] is None


def test_weekly_uncertainty_and_time_confound_are_reported_separately():
    original, wide = states(("2026-10-01", "2026-10-02", "2026-10-08", "2026-10-09"))
    wide["books"]["2026-10-08"]["main"]["net_pct"] = 11
    wide["books"]["2026-10-09"]["main"]["net_pct"] = 11
    out = ca.load_attribution(original, wide)
    comparison = out["comparisons"]["expansion_equal"]
    assert comparison["weekly_groups"] == 2
    assert comparison["mean_pct"] == 7
    # Cluster errors sum within each week: 90% half-width = 1.64485.
    assert comparison["ci90_pct"] == pytest.approx([5.355146, 8.644854])
    assert comparison["timing_confounded"] is True
    assert comparison["rows"][0]["formation_delay_minutes"] == 120
    assert out["comparisons"]["weighting_original"]["timing_confounded"] is False
    assert out["status"] == "descriptive"
    assert out["timing"]["pure_universe_effect_identified"] is False


def test_supplied_snapshots_are_not_mutated_and_do_not_require_file_reads(monkeypatch):
    original, wide = states()
    expected = copy.deepcopy((original, wide))

    def forbidden(*args, **kwargs):
        raise AssertionError("supplied snapshots must not read or write disk")

    monkeypatch.setattr(Path, "read_text", forbidden)
    monkeypatch.setattr(Path, "write_text", forbidden)
    ca.load_attribution(original, wide)
    assert (original, wide) == expected


def test_missing_state_files_do_not_create_or_start_trackers(monkeypatch, tmp_path):
    from core.research import coinglass_composite_tracker as tracker

    monkeypatch.setattr(tracker, "STATE_PATH", tmp_path / "missing_original.json")
    monkeypatch.setattr(tracker, "WIDE_STATE_PATH", tmp_path / "missing_wide.json")
    before = sorted(tmp_path.iterdir())
    out = ca.load_attribution()
    assert out["status"] == "insufficient_data"
    assert out["factorial_common"]["n_pairs"] == 0
    assert sorted(tmp_path.iterdir()) == before


def test_different_settlement_times_are_not_misread_as_a_clean_weighting_effect():
    original, wide = states()
    original["books"]["2026-10-01"]["shadow"]["settled_at"] = "2026-10-09T04:00:00+00:00"
    out = ca.load_attribution(original, wide)
    comparison = out["comparisons"]["weighting_original"]
    assert comparison["timing_confounded"] is True
    assert comparison["rows"][0]["formation_delay_minutes"] == 0
    assert comparison["rows"][0]["settlement_delay_minutes"] == 60
