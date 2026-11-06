import json

from scripts import research_binance_chat_activity_followup as follow
import numpy as np
import pandas as pd
import pytest


BASE = pd.Timestamp("2026-10-30T00:00:00Z")


def message(mid, author, minute, sha=None, receipt=None):
    p = BASE + pd.Timedelta(minutes=minute)
    r = BASE + pd.Timedelta(minutes=minute if receipt is None else receipt)
    return (mid, p.isoformat(), r.isoformat(), author, True, False, False, False, False, False, sha or f"text-{mid}")


def test_concentration_is_known_at_close_and_does_not_export_authors():
    rows = [message(i, "a", i) for i in range(4)]
    rows += [message(4, "b", 10, "echo"), message(5, "b", 11, "echo"), message(6, "c", 12)]
    rows += [message(7, "a", 59, receipt=62)]
    result = follow.concentration_fields(rows, {BASE.isoformat()})[BASE.isoformat()]
    assert result["human_available"] == 7 and result["authors_available"] == 3
    assert result["without_top_author"] == 3 and result["capped_three"] == 6
    assert result["unique_texts_available"] == 6 and result["top_available_count"] == 4
    assert set(result) == {"human_available", "authors_available", "without_top_author", "capped_three",
                           "unique_texts_available", "top_available_count", "top_available_share"}


def test_shrinkage_uses_only_previous_hours_and_current_clock_support():
    times = pd.date_range(BASE, periods=40, freq="h")
    frame = pd.DataFrame({"human_available": range(1, 41), "local_hour": times.hour},
                         index=[t.isoformat() for t in times])
    result = follow.causal_baselines(frame, fields=("human_available",))
    assert result["human_available_shrink"].iloc[:24].isna().all()
    at = 30
    global_mean = np.mean(np.log1p(frame.human_available.iloc[:at]))
    previous = np.log1p(frame[frame.local_hour == times[at].hour].human_available.iloc[:1])
    expected = (previous.sum() + 3 * global_mean) / (len(previous) + 3)
    assert result["human_available_shrink"].iloc[at] == pytest.approx(expected)
    assert result["human_available_mean"].iloc[at] is np.nan or pd.isna(result["human_available_mean"].iloc[at])
    frame.loc[frame.index[35:], "human_available"] = 1e9
    changed = follow.causal_baselines(frame, fields=("human_available",))
    pd.testing.assert_series_equal(result["human_available_shrink"].iloc[:35], changed["human_available_shrink"].iloc[:35])


def forecast_frame():
    rng = np.random.default_rng(19)
    times = pd.date_range(BASE, periods=65, freq="h")
    frame = pd.DataFrame(rng.normal(size=(65, len(follow.MARKET_FEATURES))), columns=follow.MARKET_FEATURES,
                         index=[t.isoformat() for t in times])
    frame["log_rv"] = np.abs(frame["log_rv"]) / 10
    for model_fields in follow.MODELS.values():
        for field in model_fields:
            frame[field] = rng.normal(size=len(frame))
    frame["target_log_rv"] = rng.uniform(.01, .5, len(frame))
    frame["date"] = [str(t.tz_convert("Asia/Shanghai").date()) for t in times]
    return frame


def test_training_targets_are_mature_and_all_models_share_origins_and_rows():
    frame = forecast_frame()
    frame.loc[frame.index[3], "authors_available_surprise"] = np.nan
    records = follow.forecast(frame, min_train=4)
    assert records
    grouped = pd.DataFrame(records).groupby("hour")
    for hour, group in grouped:
        assert set(group.model) == {"persistence", *follow.MODELS}
        assert group.train_rows.nunique() == 1
        assert group.last_train_target_end.nunique() == 1
        assert pd.Timestamp(group.last_train_target_end.iloc[0]) <= pd.Timestamp(group.prediction_at.iloc[0])
        assert group.train_rows.iloc[0] == list(frame.index).index(hour) - 1  # one missing shared row


def test_current_and_future_outcomes_cannot_change_current_prediction():
    frame = forecast_frame()
    original = follow.forecast(frame, min_train=8)
    cutoff = frame.index[30]
    changed = frame.copy()
    changed.loc[changed.index >= cutoff, "target_log_rv"] = 1e5
    updated = follow.forecast(changed, min_train=8)
    before = {(r["hour"], r["model"]): r["prediction"] for r in original if r["hour"] <= cutoff}
    after = {(r["hour"], r["model"]): r["prediction"] for r in updated if r["hour"] <= cutoff}
    assert before == after


def test_ridge_standardization_and_intercept_use_training_only():
    x = np.array([[0.], [1.], [2.], [3.]])
    y = 2 + 3 * x[:, 0]
    result = follow.ridge_predict(x, y, np.array([3.]), alpha=1)
    assert result == pytest.approx(6.5 + 3 * 1.5 * 4 / 5)


def test_zero_benchmark_error_has_no_infinite_relative_gain():
    records = [{"hour": BASE.isoformat(), "local_date": "2026-11-01", "model": "market", "actual": .1, "prediction": .1},
               {"hour": BASE.isoformat(), "local_date": "2026-11-01", "model": "market_activity", "actual": .1, "prediction": .2}]
    cells = follow.forecast_metrics(records)
    actual = next(r for r in cells if r["period"] == "new_date" and r["model"] == "market_activity")
    assert actual["mae_change_vs_market_pct"] is None and actual["mae_log"] == pytest.approx(.1)
    assert follow.forecast_metrics([]) == []


def test_json_missing_evidence_stays_null_and_observed_zero_stays_zero():
    payload = follow.json_ready({"metric": np.nan, "events": np.int64(0)})
    assert json.dumps(payload, allow_nan=False) == '{"metric": null, "events": 0}'
