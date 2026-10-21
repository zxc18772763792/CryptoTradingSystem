"""Series-planner tests use temporary partitions only; no historical mutations."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.data.parquet_timezone_repair import RepairConflict, StalePlan, apply_plan, rollback_run, sha256
from scripts import repair_verified_partition_series as series


def bars(start="2026-05-01", hours=64, minutes=1):
    index = pd.date_range(start, periods=hours * 60 // minutes, freq=f"{minutes}min")
    price = 100.0 + np.arange(len(index)) / 1000
    return pd.DataFrame(dict(open=price, high=price + .002, low=price - .002,
                             close=price + .001, volume=np.ones(len(index))), index=index)


def hourly(frame):
    return frame.resample("h").agg(dict(open="first", high="max", low="min", close="last", volume="sum"))


def save(directory, date, frame):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{date}.parquet"
    frame.to_parquet(path)
    return path


def result(plans):
    return pd.concat([plan["result"] for plan in plans]).sort_index()


@pytest.mark.parametrize("minutes,interval", [(1, "1m"), (5, "5m"), (15, "15m"), (60, "1h")])
def test_entire_preceding_series_and_utc_seam_are_rebucketed_without_row_loss(tmp_path, minutes, interval):
    frame = bars(minutes=minutes)
    directory = tmp_path / f"{interval}_parts"
    local = frame.iloc[:40 * 60 // minutes].copy()
    local.index += pd.Timedelta(hours=8)
    for date, group in local.groupby(local.index.strftime("%Y-%m-%d")):
        save(directory, date, group)
    save(directory, "2026-05-03", frame.iloc[40 * 60 // minutes:])
    before = {path: path.read_bytes() for path in directory.glob("*.parquet")}
    plans = series.build_series_plans(directory, hourly(frame), {"test": "official"}, "2026-05-03")
    pd.testing.assert_frame_equal(result(plans), frame.rename_axis("timestamp"), check_freq=False)
    summary = plans[0]["evidence"]["series_summary"]
    assert summary["rows_before"] == summary["rows_after"] == len(frame)
    assert summary["shifted_rows"] == 40 * 60 // minutes
    assert summary["unknown_rows"] == summary["coalesced_rows"] == 0
    assert all(plan["utc_certified"] for plan in plans)
    assert all(path.read_bytes() == content for path, content in before.items())
    assert sum(len(plan["evidence"]["source_rows"]) for plan in plans) == len(frame)
    assert sum(len(plan["evidence"]["output_row_origins"]) for plan in plans) == len(frame)


def test_new_preceding_target_and_empty_source_have_guarded_transaction_and_rollback(tmp_path):
    frame = bars("2026-04-30 16:00", hours=8, minutes=5)
    local = frame.copy(); local.index += pd.Timedelta(hours=8)
    directory = tmp_path / "5m_parts"
    source = save(directory, "2026-05-01", local)
    before = source.read_bytes()
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    target = directory / "2026-04-30.parquet"
    by_name = {Path(plan["path"]).name: plan for plan in plans}
    assert by_name[target.name]["pre_hash"] is None
    assert by_name[source.name]["pre_hash"] == sha256(source)
    assert by_name[source.name]["rows_after"] == 0
    apply_plan(plans, tmp_path / "run")
    pd.testing.assert_frame_equal(pd.read_parquet(target), frame.rename_axis("timestamp"), check_freq=False)
    assert pd.read_parquet(source).empty
    rollback_run(tmp_path / "run")
    assert source.read_bytes() == before
    assert not target.exists()


def test_existing_destination_neighbor_after_cutoff_is_included_and_preserved(tmp_path):
    frame = bars("2026-05-02", hours=2, minutes=5)
    directory = tmp_path / "5m_parts"
    old = save(directory, "2026-05-01", frame.iloc[:12])
    neighbor = save(directory, "2026-05-02", frame.iloc[12:])
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    assert {plan["path"] for plan in plans} == {str(old), str(neighbor)}
    assert all(plan["pre_hash"] for plan in plans)
    pd.testing.assert_frame_equal(result(plans), frame.rename_axis("timestamp"), check_freq=False)


@pytest.mark.parametrize("unit", ["us", "ms", "s"])
def test_complete_grid_uses_instants_instead_of_storage_resolution(tmp_path, unit):
    frame = bars(hours=2, minutes=5)
    local = frame.copy()
    local.index = (local.index + pd.Timedelta(hours=8)).as_unit(unit)
    directory = tmp_path / "5m_parts"
    save(directory, "2026-05-01", local)
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    assert plans[0]["evidence"]["series_summary"]["unknown_rows"] == 0
    assert plans[0]["evidence"]["series_summary"]["shifted_rows"] == len(frame)
    pd.testing.assert_frame_equal(result(plans), frame.rename_axis("timestamp"), check_freq=False)


def test_incomplete_hour_is_preserved_at_original_timestamps_and_not_certified(tmp_path):
    frame = bars(hours=2, minutes=5)
    local = frame.copy(); local.index += pd.Timedelta(hours=8)
    local = local.drop(local.index[-1])
    directory = tmp_path / "5m_parts"
    save(directory, "2026-05-01", local)
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    output = result(plans)
    unknown = local.iloc[12:]
    pd.testing.assert_frame_equal(output.loc[unknown.index], unknown, check_freq=False)
    assert plans[0]["evidence"]["series_summary"]["unknown_rows"] == 11
    assert plans[0]["utc_certified"] is False
    assert "cts_timestamp_timezone" not in plans[0]["result"].attrs
    assert plans[0]["result"].attrs["cts_timestamp_status"] == "unverified_segments_remain"
    apply_plan(plans, tmp_path / "partial_run")
    persisted = pd.read_parquet(directory / "2026-05-01.parquet")
    assert "cts_timestamp_timezone" not in persisted.attrs
    pd.testing.assert_frame_equal(persisted.loc[unknown.index], unknown, check_freq=False)
    repeated = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    pd.testing.assert_frame_equal(result(repeated), persisted, check_freq=False)
    assert repeated[0]["evidence"]["series_summary"]["shifted_rows"] == 0


def test_unknown_collision_defers_the_entire_shifted_hour_and_preserves_bytes(tmp_path):
    frame = bars(hours=1, minutes=5)
    local = frame.copy(); local.index += pd.Timedelta(hours=8)
    source = pd.concat([frame.iloc[:1], local])
    directory = tmp_path / "5m_parts"
    path = save(directory, "2026-05-01", source)
    before = path.read_bytes()
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    pd.testing.assert_frame_equal(result(plans), source.sort_index().rename_axis("timestamp"), check_freq=False)
    assert plans[0]["evidence"]["series_summary"]["deferred_hours"] == 1
    assert plans[0]["evidence"]["series_summary"]["unknown_rows"] == len(source)
    assert path.read_bytes() == before


def test_identical_proven_overlaps_are_explicitly_accounted_and_all_origins_mapped(tmp_path):
    frame = bars(hours=1, minutes=5)
    local = frame.copy(); local.index += pd.Timedelta(hours=8)
    directory = tmp_path / "5m_parts"
    save(directory, "2026-05-01", local)
    save(directory, "2026-05-02", frame)
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-02")
    summary = plans[0]["evidence"]["series_summary"]
    assert summary["rows_before"] == 24
    assert summary["rows_after"] == summary["coalesced_rows"] == 12
    assert sum(len(row["origins"]) for plan in plans for row in plan["evidence"]["output_row_origins"]) == 24
    assert sum(len(plan["coalesced"]) for plan in plans) == 12


def test_hourly_reference_cannot_resolve_conflicting_minute_values(tmp_path):
    frame = bars(hours=1, minutes=5)
    local = frame.copy(); local.index += pd.Timedelta(hours=8)
    conflicting = frame.copy()
    conflicting.iloc[3, conflicting.columns.get_loc("close")] += .0001
    assert hourly(frame).equals(hourly(conflicting))  # same hourly candle, different minute
    directory = tmp_path / "5m_parts"
    save(directory, "2026-05-01", local)
    save(directory, "2026-05-02", conflicting)
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-02")
    assert plans[0]["evidence"]["series_summary"]["deferred_hours"] == 1
    assert plans[0]["evidence"]["series_summary"]["coalesced_rows"] == 0
    assert len(result(plans)) == 24
    pd.testing.assert_frame_equal(result(plans).loc[local.index], local, check_freq=False)


def test_unresolvable_original_unknown_duplicate_refuses_entire_series(tmp_path):
    frame = bars(hours=1, minutes=5)
    source = pd.concat([frame, frame.iloc[:1]])
    directory = tmp_path / "5m_parts"
    path = save(directory, "2026-05-01", source)
    before = path.read_bytes()
    with pytest.raises(RepairConflict, match="Unresolvable canonical collision") as caught:
        series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    assert len(caught.value.conflict_rows) == 2
    assert caught.value.conflict_rows[0]["source_path"] == str(path)
    assert caught.value.conflict_rows[0]["original_ohlcv"] == frame.iloc[0].tolist()
    assert path.read_bytes() == before


def test_collision_deferral_propagates_by_whole_hour_without_deleting_unknowns(tmp_path):
    frame = bars(hours=9, minutes=5)
    first = frame.iloc[:12].copy(); first.index += pd.Timedelta(hours=8)
    second = frame.iloc[8*12:9*12].copy(); second.index += pd.Timedelta(hours=8)
    source = pd.concat([frame.iloc[:1], first, second])
    directory = tmp_path / "5m_parts"
    save(directory, "2026-05-01", source)
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    assert plans[0]["evidence"]["series_summary"]["deferred_hours"] == 2
    assert plans[0]["evidence"]["series_summary"]["unknown_rows"] == len(source)
    pd.testing.assert_frame_equal(result(plans), source.rename_axis("timestamp"), check_freq=False)
    again = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    assert [plan["result_hash"] for plan in plans] == [plan["result_hash"] for plan in again]
    assert [plan["evidence"] for plan in plans] == [plan["evidence"] for plan in again]


def test_ambiguous_complete_hour_is_preserved_and_unmarked(tmp_path):
    frame = bars(hours=17, minutes=5)
    for pos in range(12, len(frame), 12):
        frame.iloc[pos:pos+12] = frame.iloc[:min(12, len(frame)-pos)].to_numpy()
    directory = tmp_path / "5m_parts"
    source = frame.iloc[8*12:9*12]
    save(directory, "2026-05-01", source)
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    assert plans[0]["evidence"]["series_summary"]["unknown_rows"] == 12
    assert plans[0]["evidence"]["series_summary"]["hour_status_counts"] == {"ambiguous": 1}
    assert not plans[0]["utc_certified"]
    pd.testing.assert_frame_equal(result(plans), source.rename_axis("timestamp"), check_freq=False)


def test_marked_utc_file_cannot_be_shifted_again_despite_reference(tmp_path):
    frame = bars(hours=1, minutes=5)
    local = frame.copy(); local.index += pd.Timedelta(hours=8)
    local.attrs = {"cts_timestamp_timezone": "UTC", "cts_timestamp_schema": 1}
    directory = tmp_path / "5m_parts"
    save(directory, "2026-05-01", local)
    with pytest.raises(RepairConflict, match="UTC marker contradicts"):
        series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")


def test_verified_utc_metadata_resolves_only_the_matching_zero_offset(tmp_path):
    frame = bars(hours=17, minutes=5)
    for pos in range(12, len(frame), 12):
        frame.iloc[pos:pos+12] = frame.iloc[:min(12, len(frame)-pos)].to_numpy()
    source = frame.iloc[8*12:9*12].copy()
    source.attrs = {"cts_timestamp_timezone": "UTC", "cts_timestamp_schema": 1}
    directory = tmp_path / "5m_parts"
    save(directory, "2026-05-01", source)
    plans = series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
    assert plans[0]["utc_certified"]
    assert plans[0]["evidence"]["series_summary"]["shifted_rows"] == 0


def test_source_change_during_series_planning_is_rejected(tmp_path, monkeypatch):
    frame = bars(hours=1, minutes=5)
    directory = tmp_path / "5m_parts"
    path = save(directory, "2026-05-01", frame)
    real_hash = series.sha256
    calls = 0
    def changing_hash(target):
        nonlocal calls
        calls += 1
        if calls == 3:
            path.write_bytes(path.read_bytes() + b"changed")
        return real_hash(target)
    monkeypatch.setattr(series, "sha256", changing_hash)
    with pytest.raises(StalePlan, match="changed during series planning"):
        series.build_series_plans(directory, hourly(frame), {}, "2026-05-01")
