"""Explicit repair of perp bars stored in the spot kline store (core/data/kline_venue_repair.py)."""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.data.kline_venue_repair import (
    OHLCV,
    SeriesTarget,
    apply_series,
    classify,
    diagnose_blocked_day,
    fetch_archives,
    load_targets,
    plan_series,
    prepare_series,
    rollback_series,
    verify_series,
)
from core.data.parquet_lock import partition_lock_path
from core.data.parquet_timezone_repair import StalePlan, sha256

UTC_ATTRS = {"cts_timestamp_timezone": "UTC", "cts_timestamp_schema": 1,
             "cts_timestamp_origin": "writer", "cts_timestamp_status": "verified_utc"}
DAY_A, DAY_B, CLEAN_DAY = "2026-09-04", "2026-10-01", "2026-10-02"


def _bars(day: str, base: float, *, n: int = 8, volume: float = 10.0) -> pd.DataFrame:
    index = pd.date_range(day, periods=n, freq="15min", name="timestamp")
    opens = base + np.arange(n, dtype=float)
    return pd.DataFrame({"open": opens, "high": opens + 0.5, "low": opens - 0.5, "close": opens + 0.25,
                         "volume": np.full(n, volume)}, index=index)


def _spot(day: str) -> pd.DataFrame:
    return _bars(day, 100.0, volume=10.0)


def _perp(day: str) -> pd.DataFrame:
    return _bars(day, 100.125, volume=500.0)  # dyadic: survives the x1000 / 1000 round trip exactly


def _write(path: Path, frame: pd.DataFrame, attrs: dict | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = frame.copy()
    frame.attrs = dict(attrs or {})
    frame.to_parquet(path)


def _fetcher(multiplier: float = 1.0):
    def fetch(market, raw, timeframe, day):
        assert timeframe == "15m"
        if day not in (DAY_A, DAY_B):
            return None
        if market == "spot":
            assert raw == "BTCUSDT"
            return _spot(day)
        frame = _perp(day)
        frame[["open", "high", "low", "close"]] *= multiplier
        return frame
    return fetch


@pytest.fixture()
def store(tmp_path):
    """BTC 15m with perp rows on DAY_A (legacy pack + an old-code daily file) and DAY_B (marked period file)."""

    root = tmp_path / "historical"
    parts = root / "binance" / "BTC_USDT" / "15m_parts"
    pack = _spot(DAY_A)
    perp = _perp(DAY_A)
    pack.iloc[2:6] = perp.iloc[2:6].to_numpy()                      # perp rows
    pack.iloc[6, :] = [perp.iloc[6, 0], perp.iloc[6, 1] - 0.1, perp.iloc[6, 2] + 0.1, perp.iloc[6, 0], 50.0]  # perp-like, inexact
    pack.iloc[7, :] = [1.0, 2.0, 0.5, 1.5, 3.0]                       # neither: left alone
    _write(parts / "2026-09.legacy.parquet", pd.concat([pack, _spot("2026-09-05")]))
    daily = perp.iloc[2:4].copy()                                     # the same perp rows, recreated by an old writer
    _write(parts / f"{DAY_A}.parquet", daily)
    period = pd.concat([_perp(DAY_B), _spot(CLEAN_DAY)])
    _write(parts / "2026-10.parquet", period, UTC_ATTRS)
    _write(root / "binance" / "BTC_USDT" / "15m.parquet", _spot(DAY_A).iloc[:2])  # main file: spot, overlapping
    return root


def _target() -> SeriesTarget:
    return SeriesTarget("BTC_USDT", "15m", [DAY_A, DAY_B], "BTC_USDT")


def _plan(store):
    target = _target()
    return plan_series(store, target, fetch_archives(target, _fetcher(), workers=2))


def _hashes(store) -> dict:
    return {p.name: sha256(p) for p in sorted((store / "binance").rglob("*.parquet"))}


def test_classify_labels_each_kind_of_row():
    spot = _spot(DAY_A).to_numpy()[:1].repeat(7, axis=0)
    perp = _perp(DAY_A).to_numpy()[:1].repeat(7, axis=0)
    flat = np.array([[1.0, 1.0, 1.0, 1.0, 9.0]])
    values = np.vstack([
        spot[0], perp[0],
        [perp[0][0], perp[0][1] - 0.1, perp[0][2] + 0.1, perp[0][0], 1.0],   # forming perp bar
        [spot[0][0], spot[0][1] - 0.1, spot[0][2] + 0.1, spot[0][0], 1.0],   # forming spot bar
        [7.0, 8.0, 6.0, 7.5, 1.0], spot[0], flat[0],
    ])
    spot_ref = spot.copy()
    spot_ref[5] = np.nan
    perp_ref = perp.copy()
    spot_ref[6], perp_ref[6] = [1.0, 1.0, 1.0, 1.0, 2.0], [1.0, 1.0, 1.0, 1.0, 9.0]
    assert list(classify(values, spot_ref, perp_ref)) == [
        "spot", "perp", "perp_forming", "spot_forming", "neither", "no_spot_bar", "perp_volume"]


def test_plan_writes_nothing_and_replaces_only_perp_rows(store):
    hashes = _hashes(store)
    plan = _plan(store)
    assert _hashes(store) == hashes
    assert plan.status == "ready", plan.reason
    by_file = plan.changes.groupby("file")["class"].apply(list).to_dict()
    assert by_file == {
        "15m_parts/2026-09-04.parquet": ["perp", "perp"],
        "15m_parts/2026-09.legacy.parquet": ["perp"] * 4,
        "15m_parts/2026-10.parquet": ["perp"] * 8,
    }
    spot = pd.concat([_spot(DAY_A), _spot(DAY_B)])
    after = plan.changes.set_index("timestamp")[[f"after_{n}" for n in OHLCV]]
    assert (after.to_numpy() == spot.loc[after.index].to_numpy()).all()
    assert list(plan.unresolved["class"]) == ["perp_forming", "neither"]   # reported, not replaced
    assert not plan.unresolved["blocked"].any()
    files = {f["file"]: f for f in plan.files}
    assert files["15m.parquet"]["rows_changed"] == 0 and "planned_post_sha256" not in files["15m.parquet"]
    days = {d["day"]: d for d in plan.days}
    assert days[DAY_B]["reader_after_equals_spot_archive"] is True
    assert days[DAY_A]["reader_after_equals_spot_archive"] is False   # the inexact rows stay
    assert days[DAY_A]["before_perp"] == 4 and days[DAY_A]["after_perp"] == 0
    assert days[DAY_A]["reader_digest_before"] != days[DAY_A]["reader_digest_after"]
    assert set(plan.archives["market"]) == {"spot", "perp"}


def test_apply_writes_the_planned_bytes_and_keeps_layout(store):
    plan = _plan(store)
    parts = store / "binance" / "BTC_USDT" / "15m_parts"
    before = {name: pd.read_parquet(parts / name) for name in ("2026-09.legacy.parquet", "2026-10.parquet")}
    result = apply_series(store, "BTC_USDT", "15m", plan.changes, store.parent / "run" / "BTC_USDT__15m")
    assert result["status"] == "complete", result
    files = {f["file"]: f for f in plan.files}
    for rel, record in files.items():
        if record.get("rows_changed"):
            assert sha256(store / "binance" / "BTC_USDT" / rel) == record["planned_post_sha256"]
    pack, period = pd.read_parquet(parts / "2026-09.legacy.parquet"), pd.read_parquet(parts / "2026-10.parquet")
    assert pack.attrs == {} and period.attrs == UTC_ATTRS                # pack stays unmarked, period stays marked
    assert pack.index.equals(before["2026-09.legacy.parquet"].index)     # no row added, removed or moved
    pd.testing.assert_frame_equal(pack.loc["2026-09-05"], before["2026-09.legacy.parquet"].loc["2026-09-05"])
    pd.testing.assert_frame_equal(period.loc[DAY_B], _spot(DAY_B), check_freq=False)
    assert all(Path(f["backup"]).exists() for f in result["files"])
    if os.name == "nt":  # the engine's per-file locks beside period files and packs are removed again
        assert not partition_lock_path(parts / "2026-10.parquet").exists()
        assert not partition_lock_path(parts / "2026-09.legacy.parquet").exists()
    days = pd.DataFrame([d for d in plan.days if d["status"] == "checked"])
    checks = verify_series(store, "BTC_USDT", "15m", days, plan.archives)
    assert all(c["matches_planned_after"] for c in checks)
    assert all(c["replaceable_rows_left"] == 0 for c in checks)
    again = apply_series(store, "BTC_USDT", "15m", plan.changes, store.parent / "run2" / "BTC_USDT__15m")
    assert again["status"] == "already_applied"


def test_rows_added_after_the_plan_survive_the_apply(store):
    plan = _plan(store)
    path = store / "binance" / "BTC_USDT" / "15m_parts" / "2026-10.parquet"
    grown = pd.concat([pd.read_parquet(path), _spot("2026-10-03")])     # an hourly refresh appended a day
    _write(path, grown, UTC_ATTRS)
    result = apply_series(store, "BTC_USDT", "15m", plan.changes, store.parent / "run" / "BTC_USDT__15m")
    assert result["status"] == "complete", result
    repaired = pd.read_parquet(path)
    pd.testing.assert_frame_equal(repaired.loc["2026-10-03"], _spot("2026-10-03"), check_freq=False)
    pd.testing.assert_frame_equal(repaired.loc[DAY_B], _spot(DAY_B), check_freq=False)


def test_a_planned_row_changed_since_the_plan_leaves_the_series_untouched(store):
    plan = _plan(store)
    path = store / "binance" / "BTC_USDT" / "15m_parts" / "2026-10.parquet"
    frame = pd.read_parquet(path)
    frame.iloc[0, 0] = 123.0
    _write(path, frame, UTC_ATTRS)
    hashes = _hashes(store)
    with pytest.raises(StalePlan):
        prepare_series(store, "BTC_USDT", "15m", plan.changes)
    result = apply_series(store, "BTC_USDT", "15m", plan.changes, store.parent / "run" / "BTC_USDT__15m")
    assert result["status"] == "stale"
    assert _hashes(store) == hashes


def test_a_refused_swap_midway_is_finished_by_applying_again(store, monkeypatch):
    plan = _plan(store)
    real_replace, swaps = os.replace, []

    def flaky_replace(source, target):
        if str(source).endswith(".repair_tmp"):
            swaps.append(target)
            if len(swaps) == 2:
                raise PermissionError("target is open in another process")
        return real_replace(source, target)

    monkeypatch.setattr(os, "replace", flaky_replace)
    result = apply_series(store, "BTC_USDT", "15m", plan.changes, store.parent / "run" / "BTC_USDT__15m")
    monkeypatch.setattr(os, "replace", real_replace)
    assert result["status"] == "failed" and result["files_swapped"] == 1
    assert not list(store.rglob("*.repair_tmp"))
    again = apply_series(store, "BTC_USDT", "15m", plan.changes, store.parent / "run2" / "BTC_USDT__15m")
    assert again["status"] == "complete"
    assert sum(f["rows_already_repaired"] for f in again["files"]) == 0  # the swapped file is skipped whole
    days = pd.DataFrame([d for d in plan.days if d["status"] == "checked"])
    assert all(c["matches_planned_after"] for c in verify_series(store, "BTC_USDT", "15m", days, plan.archives))


def test_rollback_restores_the_original_files(store):
    hashes = _hashes(store)
    plan = _plan(store)
    run = store.parent / "run" / "BTC_USDT__15m"
    assert apply_series(store, "BTC_USDT", "15m", plan.changes, run)["status"] == "complete"
    assert _hashes(store) != hashes
    assert rollback_series(run)["status"] == "rolled_back"
    assert _hashes(store) == hashes


def test_contract_multiplier_scales_perp_prices_not_volume(store):
    target = SeriesTarget("BTC_USDT", "15m", [DAY_A, DAY_B], "1000BTC_USDT")
    archives = fetch_archives(target, _fetcher(multiplier=1000.0), workers=2)
    pd.testing.assert_frame_equal(archives["perp"][DAY_B], _perp(DAY_B).astype(float), check_freq=False)
    plan = plan_series(store, target, archives)
    assert plan.status == "ready" and len(plan.changes) == 14


def _day_bars(day: str, shift: float = 0.0) -> pd.DataFrame:
    """A full day of 15m bars whose prices identify their hour and quarter (distinct across the day)."""
    index = pd.date_range(day, periods=96, freq="15min", name="timestamp")
    opens = 1000.0 + index.dayofyear.to_numpy() * 100.0 + np.arange(96) * 0.5 + shift
    return pd.DataFrame({"open": opens, "high": opens + 0.25, "low": opens - 0.25, "close": opens + 0.125,
                         "volume": np.full(96, 7.0 if shift else 3.0)}, index=index)


def test_a_day_stored_eight_hours_off_is_left_alone_and_diagnosed(tmp_path):
    root = tmp_path / "historical"
    spot = pd.concat([_day_bars(d) for d in ("2026-04-06", "2026-04-07", "2026-04-08")])
    perp = pd.concat([_day_bars(d, shift=0.0625) for d in ("2026-04-06", "2026-04-07", "2026-04-08")])
    stored = spot.shift(32).loc["2026-04-07"].copy()        # each stamp holds the bar 8 hours earlier (local clock)
    stored.iloc[10] = perp.loc[stored.index[10]].to_numpy()  # one row equals the perp bar at its stamp by chance
    _write(root / "binance" / "BTC_USDT" / "15m_parts" / "2026-04.legacy.parquet", stored)
    hashes = _hashes(root)

    def fetch(market, raw, timeframe, day):
        frame = spot if market == "spot" else perp
        return frame.loc[day].copy()

    target = SeriesTarget("BTC_USDT", "15m", ["2026-04-07"], "BTC_USDT")
    plan = plan_series(root, target, fetch_archives(target, fetch, workers=2))
    assert plan.status == "nothing_to_do" and plan.changes is None
    assert _hashes(root) == hashes
    record = plan.files[0]
    assert record["blocked_days"] == "2026-04-07" and record["rows_blocked"] == 1 and record["rows_changed"] == 0
    assert plan.unresolved["blocked"].all() and len(plan.unresolved) == 96
    assert plan.days[0]["files_blocked"] == 1
    rows = dict(tuple(plan.unresolved.groupby("file")))
    diagnosis = diagnose_blocked_day(target, "2026-04-07", rows, fetch)
    assert diagnosis[0]["explained_by"] == "spot-8h" and diagnosis[0]["rows_blocked"] == 1


def test_load_targets_takes_confirmed_and_unpublished_days(tmp_path):
    records = [
        {"series": "BTC_USDT/15m", "day": DAY_A, "file": "a", "perp_reference": "BTC_USDT", "rows_perp_not_spot": 4},
        {"series": "BTC_USDT/15m", "day": DAY_B, "file": "b", "perp_reference": "BTC_USDT", "rows_perp_not_spot": 0},
        {"series": "KNC_USDT/1m", "day": "2026-10-26", "file": "c", "perp_reference": "KNC_USDT",
         "rows_equal_spot_archive": 1440, "rows_equal_perp_archive": None},
        {"series": "ETH_USDT/1s", "day": "2026-07-27", "file": "d", "perp_reference": "ETH_USDT",
         "rows_equal_spot_archive": 0, "rows_equal_perp_archive": None},
        {"series": "PEPE_USDT/5m", "day": DAY_A, "file": "e", "perp_reference": "1000PEPE_USDT", "rows_perp_not_spot": 7},
    ]
    path = tmp_path / "days.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    targets = {t.series: t for t in load_targets(path)}
    assert set(targets) == {"BTC_USDT/15m", "KNC_USDT/1m", "PEPE_USDT/5m"}
    assert targets["BTC_USDT/15m"].days == [DAY_A]
    assert targets["PEPE_USDT/5m"].multiplier == 1000.0
    assert [t.series for t in load_targets(path, only=["pepe"])] == ["PEPE_USDT/5m"]


# ---------------------------------------------------------------------------- remove mode


NO_SPOT_A, NO_SPOT_B = "2026-07-13", "2026-07-14"


@pytest.fixture()
def no_spot_store(tmp_path):
    """XYZ 15m, a coin with no spot bars on July 13/14: perp bars in the main file and in a legacy pack."""

    root = tmp_path / "historical"
    symbol = root / "binance" / "XYZ_USDT"
    main = _perp(NO_SPOT_A)
    main.iloc[7, :] = [main.iloc[7, 0], main.iloc[7, 1] - 0.1, main.iloc[7, 2] + 0.1, main.iloc[7, 0], 9.0]  # inexact
    _write(symbol / "15m.parquet", main)
    _write(symbol / "15m_parts" / "2026-07.legacy.parquet", pd.concat([_perp(NO_SPOT_B), _spot("2026-07-20")]))
    return root


def _no_spot_fetch(market, raw, timeframe, day):
    if market == "spot":
        return None if day in (NO_SPOT_A, NO_SPOT_B) else _spot(day)
    return _perp(day)


def _remove_plan(root):
    target = SeriesTarget("XYZ_USDT", "15m", [NO_SPOT_A, NO_SPOT_B], "XYZ_USDT")
    return plan_series(root, target, fetch_archives(target, _no_spot_fetch, workers=2), mode="remove")


def test_remove_plan_takes_perp_rows_out_and_keeps_the_rest(no_spot_store):
    hashes = _hashes(no_spot_store)
    plan = _remove_plan(no_spot_store)
    assert _hashes(no_spot_store) == hashes
    assert plan.status == "ready", plan.reason
    assert set(plan.changes["class"]) == {"perp_no_spot"} and len(plan.changes) == 15
    assert plan.changes[[f"after_{n}" for n in OHLCV]].isna().all().all()
    assert list(plan.unresolved["class"]) == ["no_spot_bar"]           # the inexact row stays, reported
    files = {f["file"]: f for f in plan.files}
    assert files["15m.parquet"]["rows_after"] == 1
    assert files["15m_parts/2026-07.legacy.parquet"]["rows_after"] == 8  # the July 20 spot bars stay
    days = {d["day"]: d for d in plan.days}
    assert days[NO_SPOT_A]["before_perp_no_spot"] == 7 and days[NO_SPOT_A]["after_perp_no_spot"] == 0
    assert days[NO_SPOT_A]["reader_rows_after"] == 1 and days[NO_SPOT_B]["reader_rows_after"] == 0
    assert days[NO_SPOT_A]["reader_after_equals_spot_archive"] is None


def test_remove_apply_is_idempotent_verifiable_and_reversible(no_spot_store):
    hashes = _hashes(no_spot_store)
    plan = _remove_plan(no_spot_store)
    run = no_spot_store.parent / "run" / "XYZ_USDT__15m"
    result = apply_series(no_spot_store, "XYZ_USDT", "15m", plan.changes, run)
    assert result["status"] == "complete", result
    files = {f["file"]: f for f in plan.files}
    for rel, record in files.items():
        if record.get("rows_changed"):
            assert sha256(no_spot_store / "binance" / "XYZ_USDT" / rel) == record["planned_post_sha256"]
    pack = pd.read_parquet(no_spot_store / "binance" / "XYZ_USDT" / "15m_parts" / "2026-07.legacy.parquet")
    pd.testing.assert_frame_equal(pack, _spot("2026-07-20"), check_freq=False)
    assert pack.attrs == {}
    days = pd.DataFrame([d for d in plan.days if d["status"] == "checked"])
    checks = verify_series(no_spot_store, "XYZ_USDT", "15m", days, plan.archives, acted=["perp_no_spot"])
    assert all(c["matches_planned_after"] and c["replaceable_rows_left"] == 0 for c in checks)
    again = apply_series(no_spot_store, "XYZ_USDT", "15m", plan.changes, no_spot_store.parent / "run2" / "XYZ_USDT__15m")
    assert again["status"] == "already_applied"
    assert rollback_series(run)["status"] == "rolled_back"
    assert _hashes(no_spot_store) == hashes


def test_a_row_to_remove_that_changed_leaves_the_series_alone(no_spot_store):
    plan = _remove_plan(no_spot_store)
    path = no_spot_store / "binance" / "XYZ_USDT" / "15m.parquet"
    frame = pd.read_parquet(path)
    frame.iloc[0, 3] = 555.0
    _write(path, frame)
    with pytest.raises(StalePlan):
        prepare_series(no_spot_store, "XYZ_USDT", "15m", plan.changes)


def test_remove_targets_come_from_days_without_spot_archive(tmp_path):
    records = [
        {"series": "HYPE_USDT/5m", "day": NO_SPOT_A, "file": "a", "perp_reference": "HYPE_USDT",
         "rows_equal_spot_archive": None, "rows_equal_perp_archive": 287, "rows_perp_no_spot_archive": 287},
        {"series": "BTC_USDT/5m", "day": NO_SPOT_A, "file": "b", "perp_reference": "BTC_USDT", "rows_perp_not_spot": 9},
    ]
    path = tmp_path / "days.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    assert [t.series for t in load_targets(path, mode="remove")] == ["HYPE_USDT/5m"]
    assert [t.series for t in load_targets(path)] == ["BTC_USDT/5m"]
