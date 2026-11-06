import json

import numpy as np
import pandas as pd
import pytest

from scripts import research_binance_chat_activity as study


BASE = pd.Timestamp("2026-10-24T00:00:00Z")


def row(mid, minute=0, received_minute=None, author="synthetic-author", bot=False):
    pub = BASE + pd.Timedelta(minutes=minute)
    receipt = BASE + pd.Timedelta(minutes=minute if received_minute is None else received_minute)
    return (mid, pub.isoformat(), receipt.isoformat(), author, True, False, bot, False, False, False, f"sha-{mid}")


def test_published_counts_and_information_available_at_hour_close_are_distinct():
    rows = [row(1, 10), row(2, 58, 63), row(3, 30, 65), row(4, 20, bot=True)]
    hours, _ = study.aggregate_chat(rows, [], BASE, BASE + pd.Timedelta(hours=1),
                                    [[BASE.isoformat(), (BASE + pd.Timedelta(hours=1)).isoformat()]])
    hour = hours[0]
    assert hour["raw_messages"] == 4 and hour["human_messages"] == 3
    assert hour["human_timely"] == 2 and hour["human_available"] == 1
    assert hour["authors_available"] == 1 and hour["late_or_invalid_receipt"] == 1
    assert hour["local_hour"] == 8 and hour["certified"]
    assert "synthetic-author" not in json.dumps(hours)


def test_zero_messages_requires_certified_coverage_and_does_not_certify_unknown_hour():
    hours, _ = study.aggregate_chat([], [], BASE, BASE + pd.Timedelta(hours=2),
                                    [[BASE.isoformat(), (BASE + pd.Timedelta(hours=1)).isoformat()]])
    assert hours[0]["human_messages"] == hours[1]["human_messages"] == 0
    assert hours[0]["certified"] is True and hours[1]["certified"] is False


def test_partial_hours_are_excluded_and_clock_is_china_time():
    grid = study.full_hours(BASE + pd.Timedelta(minutes=12), BASE + pd.Timedelta(hours=3, minutes=42))
    assert list(grid) == [BASE + pd.Timedelta(hours=1), BASE + pd.Timedelta(hours=2)]
    with pytest.raises(ValueError):
        study.full_hours("2026-10-24", "2026-10-25")


def test_overlapping_coverage_receipts_are_not_double_counted():
    intervals = [[BASE, BASE + pd.Timedelta(minutes=45)],
                 [BASE + pd.Timedelta(minutes=30), BASE + pd.Timedelta(hours=1)]]
    assert study.covered_minutes(BASE, intervals) == 60
    assert study.covered_minutes(BASE + pd.Timedelta(hours=1), intervals) == 0


def test_direction_labels_finished_after_close_are_not_predictive_information():
    views = [{"published_at": (BASE + pd.Timedelta(minutes=59)).isoformat(),
              "available_at": (BASE + pd.Timedelta(minutes=62)).isoformat(), "side": -1}]
    hours, _ = study.aggregate_chat([], views, BASE, BASE + pd.Timedelta(hours=1), [])
    assert hours[0]["btc_views"] == 1 and hours[0]["btc_available"] == 0


def test_hour_fixed_effects_remove_a_shared_daily_schedule():
    rows = [{"date": str(day), "local_hour": hour, "x": hour * 100,
             "y": hour * 100 + day} for day in range(7) for hour in range(4)]
    frame = pd.DataFrame(rows)
    assert study.rank_correlation(frame, "x", "y")["rho"] > .95
    adjusted = study.rank_correlation(frame, "x", "y", fixed_effects=True)
    assert adjusted["rho"] is None  # x contains no variation beyond clock effects
    assert adjusted["residual_dof"] > 5


def test_day_deletion_counts_only_dates_with_observed_pairs():
    frame = pd.DataFrame({"date": ["one"] * 8 + ["missing"] * 8,
                          "local_hour": [0] * 16, "x": list(range(16)),
                          "y": list(range(8)) + [None] * 8})
    result = study.association(frame, "x", "y")
    assert result["n"] == 8 and result["days"] == 1
    assert result["day_deletion_evaluations"] == 0


def test_future_horizon_starts_after_feature_hour_and_rejects_internal_gaps():
    hours = [(BASE + pd.Timedelta(hours=i)).isoformat() for i in range(5)]
    market = pd.DataFrame({"open": [50, 100, 110, 121, 133.1],
                           "next_open": [100, 110, 121, 133.1, 146.41],
                           "rv_pct": [1, 2, 3, 4, 5]}, index=hours)
    one = study.future_metrics(hours[0], market, 1)
    three = study.future_metrics(hours[0], market, 3)
    assert one["future_ret"] == pytest.approx(10)
    assert three["future_ret"] == pytest.approx(33.1)
    assert three["future_rv"] == pytest.approx(np.sqrt(29))
    gap = study.future_metrics(hours[0], market.drop(hours[2]), 3)
    assert gap == {"future_ret": None, "future_rv": None}


def test_walk_forward_expected_activity_uses_only_prior_same_clock_dates():
    frame = pd.DataFrame({"human_available": [10, 20, 30, 40, 50], "local_hour": [8] * 5,
                          "date": [str(i) for i in range(5)]},
                         index=[(BASE + pd.Timedelta(days=i)).isoformat() for i in range(5)])
    result = study.add_walk_forward_baseline(frame)
    assert result["activity_surprise"].iloc[:3].isna().all()
    assert result["expected_log_count"].iloc[3] == pytest.approx(np.mean(np.log1p([10, 20, 30])))
    frame.iloc[4, frame.columns.get_loc("human_available")] = 999999
    after = study.add_walk_forward_baseline(frame)
    pd.testing.assert_series_equal(after["expected_log_count"].iloc[:4], result["expected_log_count"].iloc[:4])


def test_5m_market_aggregation_preserves_missing_bars(tmp_path):
    plan = {"start": BASE.isoformat(), "as_of": (BASE + pd.Timedelta(hours=1, minutes=30)).isoformat()}
    (tmp_path / "protocol.json").write_text(json.dumps(plan), encoding="utf-8")
    rows = [[int((BASE + pd.Timedelta(minutes=5 * i)).timestamp() * 1000),
             100 + i, 101 + i, 99 + i, 100 + i, 1, 0, 10] for i in range(13)]
    page = {"tool": "get_futures_usds_kline_candlestick_data", "request": {"symbol": "BTCUSDT", "interval": "5m"},
            "response": {"result": rows}}
    path = tmp_path / "market_0.json"
    path.write_text(json.dumps(page), encoding="utf-8")
    frame, quality = study.market_hours(tmp_path)
    assert len(frame) == 1 and frame.iloc[0]["ret_pct"] == pytest.approx(12)
    assert frame.iloc[0]["quote_volume"] == 120
    del page["response"]["result"][5]
    path.write_text(json.dumps(page), encoding="utf-8")
    # A valid endpoint pair cannot stand in for the missing intrahour path.
    frame, quality = study.market_hours(tmp_path)
    assert frame.empty and len(quality["missing_5m_bars"]) == 37
