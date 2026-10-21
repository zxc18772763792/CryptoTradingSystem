"""Independent timezone repair safety tests: all I/O uses pytest tmp_path.

No historical parquet, service, strategy ledger, or account is touched.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.data import parquet_timezone_repair as repair
from scripts import repair_verified_history_20261021 as driver
from scripts import verify_timezone_repair_run as acceptance
from scripts import repair_verified_partition_series as series


def candles(start="2025-12-01 00:00", periods=24, freq="1h"):
    index = pd.date_range(start, periods=periods, freq=freq)
    base = 100.0 + np.arange(periods, dtype=float)
    return pd.DataFrame({"open": base, "high": base + 1, "low": base - 1,
                         "close": base + 0.25, "volume": base * 10}, index=index)


def local_labels(frame):
    result = frame.copy()
    result.index = result.index + pd.Timedelta(hours=8)
    return result


def save(tmp_path, frame, relative="historical/binance/BTC_USDT/1h.parquet"):
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path)
    return path


def backup_path(run_dir, item):
    path = Path(item["backup"])
    return path if path.is_absolute() else run_dir / path


def assert_candles_equal(actual, expected):
    pd.testing.assert_frame_equal(actual.sort_index(), expected.sort_index(),
                                  check_freq=False, check_names=False)


def prepared(tmp_path):
    reference = candles()
    path = save(tmp_path, local_labels(reference))
    before = path.read_bytes()
    plan = repair.plan_repair(path, reference)
    assert plan["status"] == "ready"
    return reference, path, before, plan


@pytest.mark.parametrize("offset,classification", [(0, "UTC"), (8, "CST")])
def test_whole_utc_or_cst_is_verified_by_all_ohlcv_rows(offset, classification):
    reference = candles()
    source = reference.copy()
    source.index += pd.Timedelta(hours=offset)
    evidence = repair.verify_candidates(source, reference)
    assert evidence["classification"] == classification
    assert len(evidence["rows"]) == len(source)
    assert all(row["status"] == "unique" for row in evidence["rows"])
    assert {row["shift_hours"] for row in evidence["rows"]} == {-offset}


def test_mixed_row_encodings_are_repaired_by_position_not_whole_file(tmp_path):
    reference = candles(periods=40)
    source = pd.concat([local_labels(reference.iloc[:20]), reference.iloc[20:]])
    evidence = repair.verify_candidates(source, reference)
    assert evidence["classification"] == "MIXED"
    assert [row["shift_hours"] for row in evidence["rows"]] == [-8] * 20 + [0] * 20
    path = save(tmp_path, source)
    plan = repair.plan_repair(path, reference)
    assert plan["status"] == "ready"
    assert_candles_equal(plan["result"], reference)


def test_flat_two_hypotheses_are_ambiguous_not_proof(tmp_path):
    reference = candles()
    reference.loc[:, :] = [100, 101, 99, 100.25, 1000]
    source = reference.iloc[8:16].copy()
    evidence = repair.verify_candidates(source, reference)
    assert any(row["status"] == "ambiguous" for row in evidence["rows"])
    path = save(tmp_path, source)
    assert repair.plan_repair(path, reference)["status"] == "unresolved"


def test_partial_official_coverage_cannot_authorize_a_whole_file(tmp_path):
    source = local_labels(candles())
    path = save(tmp_path, source)
    before = path.read_bytes()
    plan = repair.plan_repair(path, candles().iloc[:-1])
    assert plan["status"] == "unresolved"
    assert any(row["status"] == "unmatched" for row in plan["evidence"]["rows"])
    with pytest.raises(repair.RepairConflict):
        repair.apply_plan(plan, tmp_path / "run")
    assert path.read_bytes() == before


def test_close_price_match_without_correct_volume_is_not_a_witness(tmp_path):
    reference = candles()
    source = local_labels(reference)
    source.iloc[0, source.columns.get_loc("volume")] *= 1.01
    path = save(tmp_path, source)
    assert repair.plan_repair(path, reference)["status"] == "unresolved"


def test_identical_corrected_overlaps_are_explicitly_accounted_for(tmp_path):
    reference = candles()
    source = pd.concat([local_labels(reference), reference.iloc[[0]]])
    path = save(tmp_path, source)
    before = path.read_bytes()
    plan = repair.plan_repair(path, reference)
    assert plan["status"] == "ready"
    assert_candles_equal(plan["result"], reference)
    run = tmp_path / "coalesced_run"
    manifest = repair.apply_plan(plan, run)
    item = manifest["files"][0]
    assert item["rows_before"] == 25 and item["rows_after"] == 24
    assert item["coalesced"] == [{"utc": "2025-12-01T00:00:00", "positions": [0, 24], "kept": 0}]
    assert backup_path(run, item).read_bytes() == before


def test_conflicting_mapped_rows_never_silently_keep_last(tmp_path):
    reference = candles()
    wrong = reference.iloc[[0]].copy()
    wrong.iloc[0, wrong.columns.get_loc("close")] += 0.1
    source = pd.concat([local_labels(reference), wrong])
    path = save(tmp_path, source)
    before = path.read_bytes()
    plan = repair.plan_repair(path, reference)
    assert plan["status"] == "unresolved"
    with pytest.raises(repair.RepairConflict):
        repair.apply_plan(plan, tmp_path / "conflict_run")
    assert path.read_bytes() == before


def test_invalid_datetime_is_not_silently_dropped(tmp_path):
    source = local_labels(candles())
    source.index = pd.DatetimeIndex([pd.NaT, *source.index[1:]])
    path = save(tmp_path, source)
    before = path.read_bytes()
    with pytest.raises(repair.RepairConflict):
        repair.plan_repair(path, candles())
    assert path.read_bytes() == before


def test_apply_preserves_values_and_creates_verified_immutable_backup(tmp_path):
    reference, path, before, plan = prepared(tmp_path)
    pre_hash = repair.sha256(path)
    run = tmp_path / "run"
    manifest = repair.apply_plan(plan, run)
    actual = pd.read_parquet(path)
    assert_candles_equal(actual, reference)
    assert actual.attrs["cts_timestamp_timezone"] == "UTC"
    assert actual.attrs["cts_timestamp_schema"] == 1
    assert actual.attrs["cts_timestamp_origin"] == "verified_timezone_repair"
    item = manifest["files"][0]
    assert item["pre_hash"] == pre_hash
    assert item["post_hash"] == repair.sha256(path)
    backup = backup_path(run, item)
    assert backup.read_bytes() == before and repair.sha256(backup) == pre_hash
    assert (run / "manifest.json").exists()


def test_utc_source_is_not_shifted_again(tmp_path):
    reference = candles()
    path = save(tmp_path, reference)
    before = path.read_bytes()
    plan = repair.plan_repair(path, reference)
    assert plan["status"] == "ready"  # correct timestamps still need durable UTC attrs
    assert {row["shift_hours"] for row in plan["evidence"]["rows"]} == {0}
    assert_candles_equal(plan["result"], reference)
    assert path.read_bytes() == before


def test_new_plan_after_apply_is_idempotent_and_stale_retry_never_double_shifts(tmp_path):
    reference, path, _, plan = prepared(tmp_path)
    repair.apply_plan(plan, tmp_path / "run")
    post = path.read_bytes()
    assert repair.plan_repair(path, reference)["status"] == "already_utc"
    try:
        repair.apply_plan(plan, tmp_path / "retry_run")
    except repair.StalePlan:
        pass  # A refused old pre-image is safe; it must never shift a second time.
    assert path.read_bytes() == post
    assert_candles_equal(pd.read_parquet(path), reference)


def test_plan_hash_refuses_a_writer_change_after_planning(tmp_path):
    _, path, _, plan = prepared(tmp_path)
    changed = pd.read_parquet(path)
    changed.iloc[0, 0] += 1
    changed.to_parquet(path)
    writer_bytes = path.read_bytes()
    with pytest.raises(repair.StalePlan):
        repair.apply_plan(plan, tmp_path / "stale_run")
    assert path.read_bytes() == writer_bytes


def test_hash_is_rechecked_after_the_shared_partition_lock_is_acquired(tmp_path, monkeypatch):
    _, path, _, plan = prepared(tmp_path)
    real_lock = repair.parquet_partition_lock
    changed_bytes = []

    @contextmanager
    def mutate_at_lock_entry(target, *args, **kwargs):
        with real_lock(target, *args, **kwargs):
            if Path(target).resolve() == path.resolve() and not changed_bytes:
                changed = pd.read_parquet(path)
                changed.iloc[-1, 0] += 1
                changed.to_parquet(path)
                changed_bytes.append(path.read_bytes())
            yield

    monkeypatch.setattr(repair, "parquet_partition_lock", mutate_at_lock_entry)
    with pytest.raises(repair.StalePlan):
        repair.apply_plan(plan, tmp_path / "lock_race")
    assert changed_bytes and path.read_bytes() == changed_bytes[0]


def test_existing_run_and_backup_cannot_be_overwritten(tmp_path):
    _, path, _, plan = prepared(tmp_path)
    run = tmp_path / "run"
    manifest = repair.apply_plan(plan, run)
    backup = backup_path(run, manifest["files"][0])
    original_backup, original_manifest, post = backup.read_bytes(), (run / "manifest.json").read_bytes(), path.read_bytes()
    with pytest.raises((FileExistsError, repair.RepairConflict)):
        repair.apply_plan(plan, run)
    assert backup.read_bytes() == original_backup
    assert (run / "manifest.json").read_bytes() == original_manifest
    assert path.read_bytes() == post


def test_atomic_replace_failure_keeps_source_bytes_and_valid_backup(tmp_path, monkeypatch):
    reference, path, before, plan = prepared(tmp_path)
    real_replace = repair.os.replace
    staged = []

    def fail_final_replace(source, destination):
        if Path(destination).resolve() == path.resolve():
            assert Path(source).resolve() != path.resolve()
            assert Path(source).parent.resolve() == path.parent.resolve()
            assert path.read_bytes() == before
            assert_candles_equal(pd.read_parquet(source), reference)
            staged.append(Path(source))
            raise OSError("injected atomic swap failure")
        return real_replace(source, destination)

    monkeypatch.setattr(repair.os, "replace", fail_final_replace)
    with pytest.raises((OSError, repair.RepairConflict)):
        repair.apply_plan(plan, tmp_path / "failed_run")
    assert staged and path.read_bytes() == before
    backups = [p for p in (tmp_path / "failed_run").rglob("*") if p.is_file() and p.read_bytes() == before]
    assert backups, "A verified original-byte backup must precede replacement"


def test_successful_rollback_restores_exact_original_bytes(tmp_path):
    _, path, before, plan = prepared(tmp_path)
    run = tmp_path / "run"
    repair.apply_plan(plan, run)
    result = repair.rollback_run(run)
    assert isinstance(result, dict)
    assert path.read_bytes() == before


def test_rollback_refuses_to_destroy_data_written_after_repair(tmp_path):
    _, path, _, plan = prepared(tmp_path)
    run = tmp_path / "run"
    repair.apply_plan(plan, run)
    changed = pd.read_parquet(path)
    changed.iloc[-1, 0] += 1
    changed.to_parquet(path)
    writer_bytes = path.read_bytes()
    with pytest.raises((repair.StalePlan, repair.RepairConflict)):
        repair.rollback_run(run)
    assert path.read_bytes() == writer_bytes


def test_rollback_refuses_corrupt_backup(tmp_path):
    _, path, _, plan = prepared(tmp_path)
    run = tmp_path / "run"
    manifest = repair.apply_plan(plan, run)
    post = path.read_bytes()
    backup_path(run, manifest["files"][0]).write_bytes(b"not the original parquet")
    with pytest.raises((repair.StalePlan, repair.RepairConflict)):
        repair.rollback_run(run)
    assert path.read_bytes() == post


def test_single_partition_repair_cannot_leave_corrected_rows_in_wrong_day_path(tmp_path):
    reference = candles("2025-12-01 16:00", periods=8)
    path = save(tmp_path, local_labels(reference), "historical/binance/BTC_USDT/1h_parts/2025-12-02.parquet")
    before = path.read_bytes()
    plan = repair.plan_repair(path, reference)
    if plan["status"] == "ready":
        with pytest.raises(repair.RepairConflict):
            repair.apply_plan(plan, tmp_path / "cross_day")
    else:
        assert plan["status"] == "unresolved"
    assert path.read_bytes() == before


def test_rollback_can_resume_after_copy_succeeds_but_atomic_restore_fails(tmp_path, monkeypatch):
    _, path, before, plan = prepared(tmp_path)
    run = tmp_path / "run"
    repair.apply_plan(plan, run)
    post = path.read_bytes()
    real_replace = repair.os.replace
    failures = []

    def fail_first_restore(source, destination):
        if Path(destination).resolve() == path.resolve() and not failures:
            failures.append(True)
            raise OSError("injected rollback swap failure after staging")
        return real_replace(source, destination)

    monkeypatch.setattr(repair.os, "replace", fail_first_restore)
    with pytest.raises(OSError):
        repair.rollback_run(run)
    assert path.read_bytes() == post
    repair.rollback_run(run)
    assert path.read_bytes() == before


def test_rollback_checks_actual_restore_stage_bytes_before_overwrite(tmp_path, monkeypatch):
    _, path, _, plan = prepared(tmp_path)
    run = tmp_path / "run"
    repair.apply_plan(plan, run)
    post = path.read_bytes()
    real_copy = repair._copy_exclusive
    injected = []

    def corrupt_rollback_stage(source, destination):
        real_copy(source, destination)
        if str(destination).endswith(".rollback_tmp"):
            Path(destination).write_bytes(b"injected restore-copy corruption")
            injected.append(True)

    monkeypatch.setattr(repair, "_copy_exclusive", corrupt_rollback_stage)
    with pytest.raises(repair.RepairConflict):
        repair.rollback_run(run)
    assert injected and path.read_bytes() == post


def test_official_plan_result_cannot_change_silently_before_apply(tmp_path):
    _, path, before, plan = prepared(tmp_path)
    plan["result"].iloc[0, plan["result"].columns.get_loc("close")] += 1
    with pytest.raises(repair.RepairConflict):
        repair.apply_plan(plan, tmp_path / "tampered_result")
    assert path.read_bytes() == before


def test_multipart_rollback_resumes_after_first_target_was_already_restored(tmp_path, monkeypatch):
    _, first, first_before, first_plan = prepared(tmp_path)
    second_ref = candles("2025-12-03", periods=24)
    second = save(tmp_path, local_labels(second_ref), "historical/binance/ETH_USDT/1h.parquet")
    second_before = second.read_bytes()
    second_plan = repair.plan_repair(second, second_ref)
    run = tmp_path / "two_file_run"
    repair.apply_plan([first_plan, second_plan], run)
    real_replace = repair.os.replace
    failed = []

    def fail_second_restore(source, destination):
        if Path(destination).resolve() == second.resolve() and not failed:
            failed.append(True)
            raise OSError("injected second-file restore failure")
        return real_replace(source, destination)

    monkeypatch.setattr(repair.os, "replace", fail_second_restore)
    with pytest.raises(OSError):
        repair.rollback_run(run)
    assert first.read_bytes() == first_before
    repair.rollback_run(run)
    assert first.read_bytes() == first_before and second.read_bytes() == second_before


def test_explicit_partition_rebucket_and_rollback_preserve_utc_day_membership(tmp_path):
    reference = candles("2025-12-01 16:00", periods=8)
    old_path = save(tmp_path, local_labels(reference), "historical/binance/BTC_USDT/1h_parts/2025-12-02.parquet")
    old_bytes = old_path.read_bytes()
    target = old_path.with_name("2025-12-01.parquet")
    evidence = {"method": "independent_official_all_rows_rebucket", "source_path": str(old_path)}
    emptied_source = repair.plan_explicit(old_path, reference.iloc[:0], evidence=evidence,
                                         expected_hash=repair.sha256(old_path), rows_before=8)
    target_plan = repair.plan_explicit(target, reference, evidence=evidence, expected_hash=None, rows_before=0)
    run = tmp_path / "rebucket_run"
    repair.apply_plan([emptied_source, target_plan], run)
    assert pd.read_parquet(old_path).empty
    assert_candles_equal(pd.read_parquet(target), reference)
    assert all(pd.read_parquet(target).index.strftime("%Y-%m-%d") == target.stem)
    repair.rollback_run(run)
    assert old_path.read_bytes() == old_bytes and not target.exists()


def test_explicit_new_target_cannot_overwrite_an_existing_neighbor(tmp_path):
    reference = candles("2025-12-01", periods=8)
    path = save(tmp_path, reference, "historical/binance/BTC_USDT/1h_parts/2025-12-01.parquet")
    original = path.read_bytes()
    plan = repair.plan_explicit(path, reference, evidence={"method": "official_rebucket"},
                                expected_hash=None, rows_before=0)
    with pytest.raises(repair.StalePlan):
        repair.apply_plan(plan, tmp_path / "new_target_collision")
    assert path.read_bytes() == original


def test_one_stale_member_prevents_every_file_in_group_from_changing(tmp_path):
    _, first, first_before, first_plan = prepared(tmp_path)
    second_ref = candles("2025-12-03", periods=24)
    second = save(tmp_path, local_labels(second_ref), "historical/binance/ETH_USDT/1h.parquet")
    second_plan = repair.plan_repair(second, second_ref)
    written_later = pd.read_parquet(second)
    written_later.iloc[0, 0] += 0.5
    written_later.to_parquet(second)
    second_writer_bytes = second.read_bytes()
    with pytest.raises(repair.StalePlan):
        repair.apply_plan([first_plan, second_plan], tmp_path / "group_stale")
    assert first.read_bytes() == first_before
    assert second.read_bytes() == second_writer_bytes


def test_partition_rebucket_holds_source_and_destination_locks_through_swap(tmp_path, monkeypatch):
    reference = candles("2025-12-01 16:00", periods=8)
    old = save(tmp_path, local_labels(reference), "historical/binance/BTC_USDT/1h_parts/2025-12-02.parquet")
    target = old.with_name("2025-12-01.parquet")
    evidence = {"method": "official_full_range_rebucket"}
    source_plan = repair.plan_explicit(old, reference.iloc[:0], evidence=evidence,
                                       expected_hash=repair.sha256(old), rows_before=8)
    target_plan = repair.plan_explicit(target, reference, evidence=evidence, expected_hash=None, rows_before=0)
    required = {old.resolve(), target.resolve()}
    held, acquired, replacements = set(), [], []
    real_lock, real_copy, real_replace = repair.parquet_partition_lock, repair._copy_exclusive, repair.os.replace

    @contextmanager
    def tracked_lock(path, *args, **kwargs):
        resolved = Path(path).resolve()
        with real_lock(path, *args, **kwargs):
            acquired.append(resolved)
            held.add(resolved)
            try:
                yield
            finally:
                held.remove(resolved)

    def locked_copy(source, destination):
        assert required.issubset(held), "All affected source/target locks must precede any backup"
        return real_copy(source, destination)

    def locked_swap(source, destination):
        if Path(destination).resolve() in required:
            assert required.issubset(held), "Do not release one target lock before the transaction ends"
            replacements.append(Path(destination).resolve())
        return real_replace(source, destination)

    monkeypatch.setattr(repair, "parquet_partition_lock", tracked_lock)
    monkeypatch.setattr(repair, "_copy_exclusive", locked_copy)
    monkeypatch.setattr(repair.os, "replace", locked_swap)
    repair.apply_plan([source_plan, target_plan], tmp_path / "locked_rebucket")
    assert acquired == sorted(required, key=lambda path: str(path).lower())
    assert set(replacements) == required and not held
    assert_candles_equal(pd.read_parquet(target), reference)


def test_explicit_partition_result_in_wrong_day_is_rejected_before_io(tmp_path):
    reference = candles("2025-12-01 16:00", periods=8)
    wrong_target = tmp_path / "1h_parts/2025-12-02.parquet"
    with pytest.raises(repair.RepairConflict):
        repair.plan_explicit(wrong_target, reference, evidence={"method": "official_rebucket"},
                             expected_hash=None, rows_before=0)
    assert not wrong_target.exists()


def test_utc_attrs_lost_during_stage_validation_block_actual_swap(tmp_path, monkeypatch):
    _, path, before, plan = prepared(tmp_path)
    real_read = repair.pd.read_parquet
    injected = []

    def wrong_stage_attrs(source, *args, **kwargs):
        frame = real_read(source, *args, **kwargs)
        if isinstance(source, (str, Path)) and str(source).endswith(".repair_tmp"):
            frame.attrs["cts_timestamp_timezone"] = "Asia/Shanghai"
            injected.append(True)
        return frame

    monkeypatch.setattr(repair.pd, "read_parquet", wrong_stage_attrs)
    with pytest.raises(repair.RepairConflict):
        repair.apply_plan(plan, tmp_path / "bad_stage_attrs")
    assert injected and path.read_bytes() == before


def test_planned_metadata_changes_are_sealed_as_well_as_price_values(tmp_path):
    _, path, before, plan = prepared(tmp_path)
    plan["result"].attrs["cts_timestamp_schema"] = 999
    with pytest.raises(repair.RepairConflict):
        repair.apply_plan(plan, tmp_path / "bad_planned_attrs")
    assert path.read_bytes() == before


def test_failure_after_one_group_swap_can_be_rolled_back_from_durable_journal(tmp_path, monkeypatch):
    _, first, first_before, first_plan = prepared(tmp_path)
    second_ref = candles("2025-12-03", periods=24)
    second = save(tmp_path, local_labels(second_ref), "historical/binance/ETH_USDT/1h.parquet")
    second_before = second.read_bytes()
    second_plan = repair.plan_repair(second, second_ref)
    real_replace = repair.os.replace
    failed = []

    def fail_second_commit(source, destination):
        if Path(destination).resolve() == second.resolve() and not failed:
            failed.append(True)
            raise OSError("injected partial group commit failure")
        return real_replace(source, destination)

    monkeypatch.setattr(repair.os, "replace", fail_second_commit)
    run = tmp_path / "partial_commit"
    with pytest.raises(OSError):
        repair.apply_plan([first_plan, second_plan], run)
    assert first.read_bytes() != first_before and second.read_bytes() == second_before
    repair.rollback_run(run)
    assert first.read_bytes() == first_before and second.read_bytes() == second_before


def partial_candle(frame):
    result = frame.copy()
    result.loc[:, "high"] = result["open"] + 0.5
    result.loc[:, "low"] = result["open"] - 0.5
    result.loc[:, "close"] = result["open"] + 0.1
    result.loc[:, "volume"] = result["volume"] * 0.5
    return result


def test_driver_partial_repair_preserves_unknown_and_is_idempotent(tmp_path):
    reference = candles(periods=10)
    unknown = candles("2026-01-01", periods=1) * 9
    source = pd.concat([local_labels(reference.iloc[:3]),
                        local_labels(partial_candle(reference.iloc[3:4])), unknown])
    source = source[["volume", "close", "open", "low", "high"]]
    path = save(tmp_path, source)
    before = path.read_bytes()
    plan = driver.build_monolith_plan(path, reference, {"source": "fixture_official"})
    assert path.read_bytes() == before
    assert plan["status"] == "ready" and plan["utc_certified"] is False
    assert plan["result"].attrs["cts_timestamp_status"] == "unverified_segments_remain"
    assert "cts_timestamp_timezone" not in plan["result"].attrs
    assert "cts_timestamp_schema" not in plan["result"].attrs
    assert plan["evidence"]["unknown_positions"] == [4]
    assert len(plan["evidence"]["rows"]) == len(source)
    assert [x["position"] for x in plan["evidence"]["rows"]] == list(range(len(source)))
    resolution = plan["evidence"]["official_resolutions"]
    assert len(resolution) == 1 and resolution[0]["position"] == 3
    assert_candles_equal(plan["result"].loc[reference.index[:4]], reference.iloc[:4])
    pd.testing.assert_series_equal(plan["result"].loc[unknown.index[0]], unknown.iloc[0], check_names=False)
    repair.apply_plan(plan, tmp_path / "partial_apply")
    repaired = pd.read_parquet(path)
    second = driver.build_monolith_plan(path, reference, {"source": "fixture_official"})
    assert second["status"] == "already_partial"
    assert second["evidence"]["shifted_rows"] == 0
    pd.testing.assert_frame_equal(second["result"], repaired)


def test_driver_partial_replacement_requires_three_exact_original_witnesses(tmp_path):
    reference = candles(periods=5)
    source = local_labels(pd.concat([reference.iloc[:2], partial_candle(reference.iloc[2:3])]))
    path = save(tmp_path, source)
    before = path.read_bytes()
    with pytest.raises(repair.RepairConflict, match="corroboration"):
        driver.build_monolith_plan(path, reference, {})
    assert path.read_bytes() == before


def test_driver_defers_unknown_collision_chain_without_losing_or_moving_rows(tmp_path):
    reference = candles(periods=48)
    unknown = reference.iloc[:1].copy() * 9
    source = pd.concat([unknown, local_labels(reference.iloc[[0, 8, 24, 25, 26]])])
    path = save(tmp_path, source)
    plan = driver.build_monolith_plan(path, reference, {})
    evidence = plan["evidence"]
    assert evidence["unknown_positions"] == [0, 1, 2]
    assert evidence["final_row_status_counts"]["verified_shift_deferred_unknown_collision"] == 2
    assert evidence["shifted_rows"] == 3
    assert plan["utc_certified"] is False
    assert len(plan["result"]) == len(source) and plan["coalesced"] == []
    for pos in (0, 1, 2):
        pd.testing.assert_series_equal(source.iloc[pos], plan["result"].loc[source.index[pos]], check_names=False)
    assert_candles_equal(plan["result"].loc[reference.index[[24, 25, 26]]], reference.iloc[[24, 25, 26]])
    repair.apply_plan(plan, tmp_path / "defer_apply")
    after = pd.read_parquet(path)
    second = driver.build_monolith_plan(path, reference, {})
    assert second["status"] == "already_partial" and second["evidence"]["shifted_rows"] == 0
    pd.testing.assert_frame_equal(after, second["result"])


def test_driver_daily_conflict_requires_exact_original_duplicate_not_partial_inference(tmp_path):
    reference = candles(periods=6, freq="1D")
    partial = partial_candle(reference.iloc[:1])
    bad = local_labels(reference.iloc[:1].copy() * 9)
    source = pd.concat([partial, bad, local_labels(reference.iloc[1:4])])
    path = save(tmp_path, source, "historical/binance/BTC_USDT/1d.parquet")
    plan = driver.build_monolith_plan(path, reference, {})
    assert plan["evidence"]["unknown_positions"] == [1]
    pd.testing.assert_series_equal(plan["result"].loc[bad.index[0]], bad.iloc[0], check_names=False)
    assert not any(row["position"] == 1 for row in plan["evidence"]["official_resolutions"])


def test_driver_daily_conflict_with_exact_duplicate_has_explicit_row_accounting(tmp_path):
    reference = candles(periods=6, freq="1D")
    bad = local_labels(reference.iloc[:1].copy() * 9)
    source = pd.concat([reference.iloc[:1], bad, local_labels(reference.iloc[1:4])])
    path = save(tmp_path, source, "historical/binance/BTC_USDT/1d.parquet")
    plan = driver.build_monolith_plan(path, reference, {})
    assert plan["utc_certified"] is True
    assert_candles_equal(plan["result"], reference.iloc[:4])
    assert len(source) - len(plan["result"]) == 1
    assert plan["coalesced"][0]["positions"] == [0, 1]
    resolved = plan["evidence"]["official_resolutions"]
    assert len(resolved) == 1 and resolved[0]["position"] == 1
    assert resolved[0]["original_ohlcv"] == bad.iloc[0].tolist()


def partition_pair(tmp_path, reference, *, duplicate=False, extra=False):
    first = reference.iloc[:8].copy()
    second = reference.iloc[8:].copy()
    if duplicate:
        second = pd.concat([reference.iloc[7:8], second])
    if extra:
        first["opaque"] = 1
        second["opaque"] = 2
    previous = save(tmp_path, first, "historical/binance/BTC_USDT/1h_parts/2025-12-01.parquet")
    current = save(tmp_path, second, "historical/binance/BTC_USDT/1h_parts/2025-12-02.parquet")
    return previous, current


def test_driver_partition_rebucket_accounts_overlap_and_correct_utc_day(tmp_path):
    reference = candles("2025-12-01 16:00", periods=16)
    previous, current = partition_pair(tmp_path, reference, duplicate=True)
    plans = driver.build_partition_plans(current, reference, {})
    assert sum(p["rows_before"] for p in plans) - sum(p["rows_after"] for p in plans) == 1
    assert plans[0]["evidence"]["row_accounting"] == 1
    assert len(plans[0]["evidence"]["identical_overlaps"]) == 1
    assert len(plans[0]["evidence"]["identical_overlaps"][0]["origins"]) == 2
    for plan in plans:
        assert set(plan["result"].index.strftime("%Y-%m-%d")) == {Path(plan["path"]).stem}
    repair.apply_plan(plans, tmp_path / "driver_rebucket")
    assert_candles_equal(pd.concat([pd.read_parquet(previous), pd.read_parquet(current)]), reference)
    second = driver.build_partition_plans(current, reference, {})
    assert sum(p["evidence"]["row_accounting"] for p in second) == 0
    for plan in second:
        pd.testing.assert_frame_equal(plan["result"], pd.read_parquet(plan["path"]))


def test_driver_partition_rejects_extra_fields_instead_of_silently_coalescing(tmp_path):
    reference = candles("2025-12-01 16:00", periods=16)
    previous, current = partition_pair(tmp_path, reference, duplicate=True, extra=True)
    before = {p: p.read_bytes() for p in (previous, current)}
    with pytest.raises(repair.RepairConflict):
        driver.build_partition_plans(current, reference, {})
    assert all(p.read_bytes() == value for p, value in before.items())


def test_driver_partition_incomplete_subhour_window_cannot_authorize_rebucket(tmp_path):
    source = candles("2025-12-01 23:00", periods=8, freq="15min")
    ref = source.resample("h").agg(open=("open", "first"), high=("high", "max"),
        low=("low", "min"), close=("close", "last"), volume=("volume", "sum"))
    save(tmp_path, source.iloc[:3], "historical/binance/BTC_USDT/15m_parts/2025-12-01.parquet")
    current = save(tmp_path, source.iloc[4:], "historical/binance/BTC_USDT/15m_parts/2025-12-02.parquet")
    with pytest.raises(repair.RepairConflict, match="Incomplete hourly"):
        driver.build_partition_plans(current, ref, {})


def acceptance_fixture(tmp_path):
    reference = candles(periods=12)
    reference_path = save(tmp_path, reference, "official_reference/BTCUSDT/1h.parquet")
    metadata_path = reference_path.with_suffix(".meta.json")
    metadata_path.write_text(json.dumps({"complete_fetch": True, "market": "spot", "timeZone": "0",
        "interval": "1h", "parquet_sha256": repair.sha256(reference_path)}), encoding="utf-8")
    provenance = {"reference": str(reference_path), "reference_metadata": str(metadata_path),
        "reference_sha256": repair.sha256(reference_path)}
    unknown = candles("2026-01-01", periods=1) * 9
    source = pd.concat([local_labels(reference.iloc[:3]),
        local_labels(partial_candle(reference.iloc[3:4])), unknown])
    path = save(tmp_path, source)
    plan = driver.build_monolith_plan(path, reference, provenance)
    run = tmp_path / "applied_native_core_test/run"
    repair.apply_plan(plan, run)
    return path, run


def test_read_only_acceptance_reconstructs_partial_output_and_fresh_idempotence(tmp_path):
    path, run = acceptance_fixture(tmp_path)
    snapshots = {p: p.read_bytes() for p in (path, run / "manifest.json", run / "evidence_0000.json")}
    report = acceptance.validate(tmp_path)
    assert report["summary"]["verdict"] == "passed"
    assert report["summary"]["unknown_preserved_monolith"] == 1
    assert report["summary"]["official_replacements_monolith"] == 1
    assert report["files"][0]["fresh_plan"]["status"] == "already_partial"
    assert all(p.read_bytes() == value for p, value in snapshots.items())


def test_acceptance_rejects_corrupt_backup_even_when_current_post_hash_matches(tmp_path):
    _, run = acceptance_fixture(tmp_path)
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    Path(manifest["files"][0]["backup"]).write_bytes(b"not the original parquet")
    report = acceptance.validate(tmp_path)
    assert report["summary"]["verdict"] == "failed"
    assert "Backup SHA256" in report["files"][0]["errors"][0]


def test_acceptance_rejects_incomplete_original_position_evidence(tmp_path):
    _, run = acceptance_fixture(tmp_path)
    evidence_path = run / "evidence_0000.json"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    evidence["rows"].pop()
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    report = acceptance.validate(tmp_path)
    assert report["summary"]["verdict"] == "failed"
    assert "Evidence row count" in report["files"][0]["errors"][0]


def test_acceptance_does_not_assume_later_writer_change_is_legitimate(tmp_path):
    path, _ = acceptance_fixture(tmp_path)
    value = pd.read_parquet(path)
    value.iloc[0, 0] += 0.001
    value.to_parquet(path)
    report = acceptance.validate(tmp_path)
    assert report["summary"]["verdict"] == "review_required"
    assert report["files"][0]["current_state"] == "modified_after_commit_unverified"
    assert report["files"][0]["warnings"]


def test_acceptance_pair_reconstructs_all_positions_and_explicit_overlap(tmp_path):
    reference = candles("2025-12-01 16:00", periods=16)
    reference_path = save(tmp_path, reference, "official_reference/BTCUSDT/1h.parquet")
    metadata_path = reference_path.with_suffix(".meta.json")
    metadata_path.write_text(json.dumps({"complete_fetch": True, "market": "spot", "timeZone": "0",
        "interval": "1h", "parquet_sha256": repair.sha256(reference_path)}), encoding="utf-8")
    provenance = {"reference": str(reference_path), "reference_metadata": str(metadata_path),
        "reference_sha256": repair.sha256(reference_path)}
    _, current = partition_pair(tmp_path, reference, duplicate=True)
    plans = driver.build_partition_plans(current, reference, provenance)
    run = tmp_path / "applied_parts_pairs_test/run"
    repair.apply_plan(plans, run)
    report = acceptance.validate(tmp_path)
    assert report["summary"]["verdict"] == "passed"
    lineage = report["runs"][0]["pair_row_validation"]
    assert lineage["input_positions"] == 17 and lineage["output_rows"] == 16
    assert lineage["coalesced_rows"] == 1


def test_acceptance_series_checks_cross_file_origins_and_preserves_unknown(tmp_path):
    reference = candles("2025-12-01 16:00", periods=16)
    reference_path = save(tmp_path, reference, "official_reference/BTCUSDT/1h.parquet")
    metadata_path = reference_path.with_suffix(".meta.json")
    metadata_path.write_text(json.dumps({"complete_fetch": True, "market": "spot", "timeZone": "0",
        "interval": "1h", "parquet_sha256": repair.sha256(reference_path)}), encoding="utf-8")
    provenance = {"reference": str(reference_path), "reference_metadata": str(metadata_path),
        "reference_sha256": repair.sha256(reference_path)}
    unknown = candles("2025-12-02 06:00", periods=1) * 9
    source = pd.concat([local_labels(reference.iloc[:3]), unknown])
    path = save(tmp_path, source, "historical/binance/BTC_USDT/1h_parts/2025-12-02.parquet")
    plans = series.build_series_plans(path.parent, reference, provenance, through="2025-12-02")
    run = tmp_path / "applied_partition_series_test/run"
    repair.apply_plan(plans, run)
    report = acceptance.validate(tmp_path)
    assert report["summary"]["verdict"] == "passed"
    lineage = report["runs"][0]["series_row_validation"]
    assert lineage["input_positions"] == 4 and lineage["output_rows"] == 4
    assert lineage["unknown_preserved"] == 1 and lineage["certified_input_positions"] == 3
