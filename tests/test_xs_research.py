from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.research import xs_panel
from core.research.xs_evaluation import (
    bonferroni_z, evaluate_formula_forward, evaluate_formula_on_panel, point_in_time_rows, score_period, spearman,
)
from core.research.xs_feature_dsl import FormulaError, evaluate_formula, fingerprint, validate_formula

COL = lambda name: {"col": name}  # noqa: E731


def synthetic_panel(n_coins=40, days=420, signal=True, seed=0):
    """Daily panel where 'volume' carries a planted pump signal (if signal=True)."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-07-07", periods=days, freq="D")
    rows = []
    for c in range(n_coins):
        coin_effect = rng.normal()
        noise = np.cumsum(rng.normal(scale=0.3, size=days)) * 0.1
        latent = coin_effect + noise
        prob = 1 / (1 + np.exp(-(latent * 2.0 - 2.5))) if signal else np.full(days, 0.12)
        pumped = rng.random(days) < prob
        rows.append(pd.DataFrame({
            "base": f"C{c:02d}", "date": dates,
            "close": 1.0 + rng.random(days), "volume": np.exp(latent),
            "oi": 5e6, "mcap": 1e8, "funding": 1e-4,
            "fwd30_maxret": np.where(pumped, 1.5, 0.1),
        }))
    return pd.concat(rows, ignore_index=True)


# ── DSL ──

def test_validate_formula_accepts_bounded_expressions_and_normalizes():
    f = validate_formula({
        "name": "vol_trend", "direction": "HIGH",
        "expr": {"op": "div", "args": [{"op": "mean", "window": 7, "args": [COL("volume")]},
                                       {"op": "mean", "window": 30, "args": [COL("volume")]}]},
    })
    assert f["direction"] == "high"
    assert validate_formula({"name": "ret1", "expr": {"op": "pct_change", "window": 1, "args": [COL("close")]}})


@pytest.mark.parametrize("payload, message", [
    ({"name": "x", "expr": {"op": "eval", "args": [COL("close")]}}, "unknown op"),
    ({"name": "x", "expr": {"op": "mean", "window": 1, "args": [COL("close")]}}, "window"),
    ({"name": "x", "expr": {"op": "mean", "window": 999, "args": [COL("close")]}}, "window"),
    ({"name": "x", "expr": {"col": "price_tomorrow"}}, "unknown column"),
    ({"name": "x", "expr": {"const": 3}}, "at least one base column"),
    ({"name": "x", "direction": "up", "expr": COL("close")}, "direction"),
    ({"name": "x", "expr": {"const": float("inf")}}, "finite"),
])
def test_validate_formula_rejects_unsafe_or_malformed(payload, message):
    with pytest.raises(FormulaError, match=message):
        validate_formula(payload)


def test_validate_formula_bounds_size():
    node = COL("close")
    for _ in range(6):
        node = {"op": "abs", "args": [node]}
    with pytest.raises(FormulaError, match="deeper"):
        validate_formula({"name": "deep", "expr": node})


def test_fingerprint_ignores_name_and_thesis():
    a = validate_formula({"name": "a", "thesis": "x", "expr": COL("oi")})
    b = validate_formula({"name": "b", "thesis": "y", "expr": COL("oi")})
    c = validate_formula({"name": "a", "direction": "low", "expr": COL("oi")})
    assert fingerprint(a) == fingerprint(b) != fingerprint(c)


def test_evaluate_formula_math_and_safe_division():
    daily = pd.DataFrame({"close": [1.0, 2.0, 4.0, 8.0], "volume": [0.0, 1.0, 1.0, 1.0]})
    ratio = evaluate_formula(validate_formula({"name": "r", "expr": {"op": "div", "args": [COL("close"), COL("volume")]}}), daily)
    assert np.isnan(ratio.iloc[0]) and ratio.iloc[3] == 8.0
    growth = evaluate_formula(validate_formula({"name": "g", "expr": {"op": "pct_change", "window": 1, "args": [COL("close")]}}), daily)
    assert growth.iloc[1:].tolist() == [1.0, 1.0, 1.0]
    rank = evaluate_formula(validate_formula({"name": "t", "expr": {"op": "ts_rank", "window": 3, "args": [COL("close")]}}), daily)
    assert rank.iloc[-1] == 1.0


# ── panel ──

def test_forward_labels_use_next_30_days_and_stay_nan_until_mature():
    daily = pd.DataFrame({"base": "A", "date": pd.date_range("2026-01-01", periods=40, freq="D"),
                          "close": [1.0] * 5 + [3.0] + [1.0] * 34})
    labelled = xs_panel.add_forward_labels(daily)
    assert labelled.loc[0, "fwd30_maxret"] == pytest.approx(2.0)   # day 5 is within days 1..30
    assert labelled.loc[5, "fwd30_maxret"] == pytest.approx(-2 / 3)  # the spike itself is not "forward"
    assert labelled["fwd30_maxret"].iloc[-30:].isna().all()


def test_forward_labels_reject_a_missing_day_inside_the_horizon():
    dates = pd.date_range("2026-01-01", periods=50, freq="D").delete(10)
    daily = pd.DataFrame({"base": "A", "date": dates, "close": [1.0] * 49})
    labelled = xs_panel.add_forward_labels(daily)
    assert pd.isna(labelled.loc[0, "fwd30_maxret"])
    assert labelled.loc[10, "fwd30_maxret"] == pytest.approx(0.0)


def test_forward_labels_reject_duplicate_that_masks_a_missing_day():
    dates = list(pd.date_range("2026-01-01", periods=50, freq="D"))
    dates[10] = dates[9]  # One duplicate and one missing date keep the endpoint span at 30 days.
    daily = pd.DataFrame({"base": "A", "date": dates, "close": [1.0] * len(dates)})
    labelled = xs_panel.add_forward_labels(daily)
    assert pd.isna(labelled.loc[0, "fwd30_maxret"])
    assert labelled.loc[11, "fwd30_maxret"] == pytest.approx(0.0)


def test_stitch_weekly_archive_latest_wins_and_mcap_only_on_snapshot_day(tmp_path):
    def snap(end, close, mcap):
        dates = pd.date_range(end=end, periods=3, freq="D")
        return pd.DataFrame({"base": "A", "date": dates, "close": close, "volume": 1.0, "oi": 1.0,
                             "mcap": mcap, "funding": 0.0})
    snap("2026-10-04", [1.0, 1.0, 1.0], 100.0).to_parquet(tmp_path / "2026-10-05.parquet")
    snap("2026-10-05", [9.0, 9.0, 9.0], 200.0).to_parquet(tmp_path / "2026-10-06.parquet")
    daily = xs_panel.stitch_weekly_archive(tmp_path).set_index("date")
    assert daily.loc["2026-10-04", "close"] == 9.0          # later snapshot overwrote the overlap
    assert daily.loc["2026-10-02", "close"] == 1.0          # older-only day kept
    assert daily.loc["2026-10-04", "mcap"] == 100.0          # first snapshot's own day
    assert daily.loc["2026-10-05", "mcap"] == 200.0
    assert pd.isna(daily.loc["2026-10-02", "mcap"])          # flat history mcap not trusted


def _archive(path, end, volume, *, captured_at=None, coins=("A", "B")):
    frames = []
    for coin in coins:
        dates = pd.date_range(end=end, periods=40, freq="D")
        frames.append(pd.DataFrame({"base": coin, "date": dates, "close": 1.0, "volume": volume, "oi": 1.0,
                                    "mcap": 1e8, "funding": 0.0}))
    frame = pd.concat(frames, ignore_index=True)
    if captured_at is not None:
        frame["captured_at"] = captured_at
    frame.to_parquet(path)


def test_archive_vintages_accept_provable_capture_and_reject_the_rest(tmp_path):
    _archive(tmp_path / "2026-10-05.parquet", "2026-10-04", 1.0)                       # schema 1: file name
    _archive(tmp_path / "2026-10-12.parquet", "2026-10-11", 1.0, captured_at="2026-10-12T00:20:00+00:00")
    _archive(tmp_path / "2026-10-19.parquet", "2026-10-16", 1.0)                       # last bar 3 days old
    _archive(tmp_path / "2026-10-26.parquet", "2026-10-25", 1.0, captured_at="2026-10-27T01:00:00+00:00")
    (tmp_path / "notes.parquet").write_bytes(b"")
    (tmp_path / "2026-11-02.parquet").write_bytes(b"")                                  # truncated write
    vintages, rejected = xs_panel.load_archive_vintages(tmp_path)
    assert [v["file"] for v in vintages] == ["2026-10-05.parquet", "2026-10-12.parquet"]
    assert vintages[0]["captured_at"] == pd.Timestamp("2026-10-05")
    assert vintages[1]["captured_at"] == pd.Timestamp("2026-10-12 00:20")
    assert vintages[1]["signal_date"] == pd.Timestamp("2026-10-11")
    assert {r["reason"] for r in rejected} == {"unparseable_name", "last_bar_not_previous_day", "capture_time_mismatch", "unreadable"}


def test_archive_vintage_drops_coins_whose_last_bar_lags(tmp_path):
    _archive(tmp_path / "2026-10-05.parquet", "2026-10-04", 1.0, coins=("A",))
    lagging = pd.read_parquet(tmp_path / "2026-10-05.parquet").assign(base="B")
    both = pd.concat([pd.read_parquet(tmp_path / "2026-10-05.parquet"), lagging[lagging["date"] < "2026-10-03"]])
    both.to_parquet(tmp_path / "2026-10-05.parquet")
    (vintage,), _ = xs_panel.load_archive_vintages(tmp_path)
    assert set(vintage["frame"]["base"]) == {"A"} and vintage["lagging_coins_dropped"] == 1


def test_forward_rows_use_capture_day_values_not_a_later_restatement(tmp_path):
    # The first archive saw volume 1 for A on 10-04; the next week's archive restates
    # that history as 50. The 10-04 signal must keep what was visible on 10-04.
    _archive(tmp_path / "2026-10-05.parquet", "2026-10-04", 1.0)
    _archive(tmp_path / "2026-10-12.parquet", "2026-10-11", 50.0)
    vintages, _ = xs_panel.load_archive_vintages(tmp_path)
    labels = pd.DataFrame({"base": ["A", "B", "A", "B"], "fwd30_maxret": [1.5, 0.0, 0.0, 0.0],
                           "date": pd.to_datetime(["2026-10-04"] * 2 + ["2026-10-11"] * 2)})
    formula = validate_formula({"name": "vol", "direction": "high", "expr": COL("volume")})
    rows = point_in_time_rows(formula, vintages, labels).set_index(["base", "date"])
    assert rows.loc[("A", pd.Timestamp("2026-10-04")), "feature"] == 1.0
    assert rows.loc[("A", pd.Timestamp("2026-10-11")), "feature"] == 50.0
    assert rows.loc[("A", pd.Timestamp("2026-10-04")), "pump"] == 1
    stitched = xs_panel.stitch_weekly_archive(tmp_path).set_index(["base", "date"])
    assert stitched.loc[("A", pd.Timestamp("2026-10-04")), "volume"] == 50.0  # why stitching is not point-in-time

    after_first = evaluate_formula_forward(formula, vintages, labels, after=pd.Timestamp("2026-10-05 12:00"), n_perm=10)
    assert after_first["vintages"] == 1 and after_first["method"] == "point_in_time_vintages"
    assert after_first["status"] == "insufficient_sample"


# ── evaluation ──

PERIODS = {"dev": (None, pd.Timestamp("2026-02-15")), "holdout": (pd.Timestamp("2026-02-15"), None)}


def test_planted_signal_is_found_and_reversed_direction_is_not():
    daily = synthetic_panel(signal=True)
    good = validate_formula({"name": "vol", "direction": "high", "expr": COL("volume")})
    bad = validate_formula({"name": "vol", "direction": "low", "expr": COL("volume")})
    r_good = evaluate_formula_on_panel(good, daily, periods=PERIODS, n_perm=60)
    r_bad = evaluate_formula_on_panel(bad, daily, periods=PERIODS, n_perm=60)
    for period in ("dev", "holdout"):
        assert r_good[period]["lift"] > 1.5 and r_good[period]["z"] > 2.5
        assert r_bad[period]["lift"] < 1.0


def test_unrelated_feature_is_not_significant():
    daily = synthetic_panel(signal=True)
    unrelated = validate_formula({"name": "close", "direction": "high", "expr": COL("close")})  # iid noise here
    result = evaluate_formula_on_panel(unrelated, daily, periods=PERIODS, n_perm=60)
    assert all(result[p]["z"] < 2.33 for p in ("dev", "holdout"))


def test_shortlist_test_separates_new_information_from_a_copy_of_the_model():
    daily = synthetic_panel(signal=True)
    feature = validate_formula({"name": "vol", "direction": "high", "expr": COL("volume")})
    weekly = daily[daily["date"].dt.dayofweek == 0][["base", "date"]].copy()
    rng = np.random.default_rng(3)
    # A model that already knows the signal: the feature adds nothing inside its shortlist.
    knows = weekly.assign(baseline_score=daily.loc[weekly.index, "volume"].to_numpy())
    copy = evaluate_formula_on_panel(feature, daily, periods=PERIODS, n_perm=60, baseline=knows)["holdout"]
    assert copy["corr_with_baseline"] > 0.99
    assert copy["shortlist_z"] < 1.645
    # A model that knows nothing: the same feature is clearly new information.
    blind = weekly.assign(baseline_score=rng.random(len(weekly)))
    new = evaluate_formula_on_panel(feature, daily, periods=PERIODS, n_perm=60, baseline=blind)["holdout"]
    assert new["shortlist_lift"] > 1.3 and new["shortlist_z"] > 2.0


def test_score_period_flags_small_samples():
    rows = pd.DataFrame({"base": ["A"] * 10, "date": pd.date_range("2026-01-05", periods=10, freq="W-MON"),
                         "feature": range(10), "pump": [0] * 10})
    assert score_period(rows, "high", n_perm=5)["status"] == "insufficient_sample"


def test_bonferroni_bar_and_spearman():
    assert bonferroni_z(1) == pytest.approx(1.645, abs=0.01)
    assert bonferroni_z(20) == pytest.approx(2.807, abs=0.01)
    assert bonferroni_z(100) > bonferroni_z(20)
    a = pd.Series([1.0, 2.0, 3.0, 4.0, np.nan])
    assert spearman(a, a * 10) == pytest.approx(1.0)
    assert spearman(a, -a) == pytest.approx(-1.0)


def test_archive_writer_output_is_accepted_as_a_point_in_time_vintage(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    import scripts.generate_pump_watchlist as watchlist

    monkeypatch.setattr(xs_panel, "WEEKLY_ARCHIVE_DIR", tmp_path)
    dates = pd.date_range(end="2026-10-04", periods=40, freq="D")
    part = pd.DataFrame({"base": "A", "date": dates, "close": 1.0, "volume": 1.0, "oi": 1.0, "mcap": 1e8,
                         "funding": 0.0, "oi_source": "binance", "mcap_source": "cache"})
    watchlist._write_weekly_archive([part], datetime(2026, 10, 4, 23, 59, 30, tzinfo=timezone.utc))
    _, rejected = xs_panel.load_archive_vintages(tmp_path)
    assert rejected == [{"file": "2026-10-04.parquet", "reason": "last_bar_not_previous_day"}]  # started before the bar closed

    (tmp_path / "2026-10-04.parquet").unlink()
    watchlist._write_weekly_archive([part], datetime(2026, 10, 5, 0, 5, tzinfo=timezone.utc))
    (vintage,), rejected = xs_panel.load_archive_vintages(tmp_path)
    assert rejected == [] and vintage["captured_at"] == pd.Timestamp("2026-10-05 00:05")
    saved = pd.read_parquet(tmp_path / "2026-10-05.parquet")
    assert set(xs_panel.ARCHIVE_META_COLUMNS) <= set(saved.columns)
    assert saved["oi_source"].eq("binance").all() and saved["schema_version"].eq(2).all()
