"""Read-only idempotence validation against temporary applied series."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.data.parquet_timezone_repair import apply_plan, sha256
from scripts import verify_partition_series_idempotence as verify
from scripts.repair_verified_partition_series import build_series_plans


def prepared(tmp_path, *, partial=False):
    index = pd.date_range("2026-05-01", periods=24, freq="5min")
    value = 100. + np.arange(len(index)) / 100
    frame = pd.DataFrame(dict(open=value, high=value+.02, low=value-.02,
                              close=value+.01, volume=np.ones(len(index))), index=index)
    reference = frame.resample("h").agg(dict(open="first", high="max", low="min", close="last", volume="sum"))
    local = frame.copy(); local.index += pd.Timedelta(hours=8)
    if partial:
        local = local.iloc[:13]
    directory = tmp_path / "5m_parts"
    directory.mkdir()
    target = directory / "2026-05-01.parquet"
    local.to_parquet(target)
    plans = build_series_plans(directory, reference, {}, "2026-05-01")
    apply_plan(plans, tmp_path / "applied")
    return directory, target, reference


@pytest.mark.parametrize("partial", [False, True])
def test_applied_series_has_no_shift_no_value_or_attrs_change_and_no_source_write(tmp_path, partial):
    directory, target, reference = prepared(tmp_path, partial=partial)
    before = sha256(target)
    report = verify.verify_series(directory, reference, {}, "2026-05-01")
    assert report["passed"], report
    assert report["series_summary"]["shifted_rows"] == 0
    assert report["series_summary"]["coalesced_rows"] == 0
    assert report["files"][0]["attrs_equal"]
    assert report["files"][0]["index_and_ohlcv_equal"]
    assert sha256(target) == before


def test_equivalent_microsecond_index_storage_is_accepted(tmp_path):
    directory, target, reference = prepared(tmp_path)
    current = pd.read_parquet(target)
    current.index = current.index.as_unit("us")
    current.to_parquet(target)
    report = verify.verify_series(directory, reference, {}, "2026-05-01")
    assert report["passed"], report
    assert report["files"][0]["current_index_unit"] == "datetime64[us]"
    assert report["files"][0]["proposed_index_unit"] == "datetime64[ns]"


@pytest.mark.parametrize("change", ["value", "timestamp", "attrs"])
def test_changed_planned_output_is_rejected_even_if_plan_is_ready(tmp_path, monkeypatch, change):
    directory, target, reference = prepared(tmp_path)
    actual_build = verify.build_series_plans
    def bad_plan(*args):
        plans = actual_build(*args)
        output = plans[0]["result"]
        if change == "value": output.iloc[0, 0] += 1
        elif change == "timestamp": output.index = output.index + pd.Timedelta(seconds=1)
        else: output.attrs["cts_timestamp_origin"] = "changed"
        return plans
    monkeypatch.setattr(verify, "build_series_plans", bad_plan)
    before = target.read_bytes()
    report = verify.verify_series(directory, reference, {}, "2026-05-01")
    assert not report["passed"]
    assert not report["files"][0]["passed"]
    assert target.read_bytes() == before


def test_mutation_during_planning_fails_the_snapshot_hash_check(tmp_path, monkeypatch):
    directory, target, reference = prepared(tmp_path)
    actual_build = verify.build_series_plans
    def concurrent_writer(*args):
        plans = actual_build(*args)
        frame = pd.read_parquet(target)
        frame.attrs["external_writer"] = True
        frame.to_parquet(target)
        return plans
    monkeypatch.setattr(verify, "build_series_plans", concurrent_writer)
    report = verify.verify_series(directory, reference, {}, "2026-05-01")
    assert not report["passed"]
    assert report["files_changed_during_validation"] == [str(target)]


def test_unrepaired_local_series_does_not_pass_read_only_check(tmp_path):
    directory, target, reference = prepared(tmp_path)
    frame = pd.read_parquet(target)
    frame.index += pd.Timedelta(hours=8)
    frame.attrs = {}
    frame.to_parquet(target)
    before = target.read_bytes()
    report = verify.verify_series(directory, reference, {}, "2026-05-01")
    assert not report["passed"]
    assert report["series_summary"]["shifted_rows"] == len(frame)
    assert target.read_bytes() == before


def test_apply_report_selection_is_completed_deduplicated_and_cutoff_bound(tmp_path):
    path = tmp_path / "report.json"
    report = {"apply": True, "through": "2026-05-18", "series": [
        {"symbol": "BTC_USDT", "interval": "5m", "status": "complete"},
        {"symbol": "ETH_USDT", "interval": "5m", "status": "unchanged_skip"}]}
    path.write_text(json.dumps(report))
    targets, metadata = verify._targets([path, path], ["BTC_USDT"], "5m", "2026-05-18")
    assert targets == [("BTC_USDT", "5m")]
    assert metadata[0]["sha256"] == sha256(path)
    with pytest.raises(ValueError, match="same explicit cutoff"):
        verify._targets([path], [], "5m", "2026-05-19")
