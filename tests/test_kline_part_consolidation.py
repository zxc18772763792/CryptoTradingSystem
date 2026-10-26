"""Folding legacy daily kline parts into period files and legacy packs (core/data/kline_part_consolidation.py)."""
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.data import kline_part_consolidation as kpc
from core.data.data_storage import prepare_utc_kline_frame

OHLCV = ["open", "high", "low", "close", "volume"]
LEGACY_BEFORE = date(2026, 10, 1)


def _frame(start, periods, freq="15min", base=100.0):
    index = pd.date_range(start, periods=periods, freq=freq, name="timestamp")
    values = base + np.arange(periods, dtype=float)
    return pd.DataFrame({"open": values, "high": values + 1, "low": values - 1, "close": values + 0.5,
                         "volume": values * 10}, index=index)


def _day(parts: Path, day: str, *, marked: bool, base: float = 100.0, frame=None) -> pd.DataFrame:
    frame = _frame(day, 96, base=base) if frame is None else frame
    if marked:
        frame = prepare_utc_kline_frame(frame, timestamp_timezone="UTC")
    parts.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(parts / f"{day}.parquet")
    (parts / f"{day}.parquet.lock").write_bytes(b"\0")
    return frame


def _run(parts: Path, *, apply: bool, before=date(2026, 10, 26), legacy_before=LEGACY_BEFORE):
    groups = kpc.daily_files_by_period(parts, parts.name[: -len("_parts")], before)
    every = kpc.daily_files_by_period(parts, parts.name[: -len("_parts")], date.max)
    collisions = kpc.colliding_daily_files([p for g in every.values() for p in g])
    return {stem: kpc.consolidate_period(parts, stem, paths, apply=apply, legacy_before=legacy_before,
                                         collisions=collisions)
            for stem, paths in sorted(groups.items())}


def _union(parts: Path) -> pd.DataFrame:
    frames = [pd.read_parquet(p) for p in sorted(parts.glob("*.parquet"))]
    frame = pd.concat(frames)
    frame.index = frame.index.as_unit("ns")
    return frame.sort_index()


def test_dry_run_changes_nothing(tmp_path):
    parts = tmp_path / "15m_parts"
    _day(parts, "2026-09-01", marked=True)
    _day(parts, "2026-09-02", marked=False)
    before = sorted((p.name, p.read_bytes()) for p in parts.iterdir())
    records = _run(parts, apply=False)
    assert records["2026-09"]["status"] == "would_consolidate"
    assert records["2026-09"]["verified_files"] == 1 and records["2026-09"]["legacy_files"] == 1
    assert sorted((p.name, p.read_bytes()) for p in parts.iterdir()) == before


def test_apply_moves_verified_days_to_period_file_and_unmarked_days_to_pack(tmp_path):
    parts = tmp_path / "15m_parts"
    a = _day(parts, "2026-09-01", marked=True)
    b = _day(parts, "2026-09-02", marked=False, base=300.0)
    c = _day(parts, "2026-09-03", marked=True, base=500.0)
    d = _day(parts, "2026-08-31", marked=False, base=700.0)
    union_before = _union(parts)
    records = _run(parts, apply=True)
    assert {k: v["status"] for k, v in records.items()} == {"2026-08": "consolidated", "2026-09": "consolidated"}
    assert sorted(p.name for p in parts.glob("*.parquet")) == ["2026-08.legacy.parquet", "2026-09.legacy.parquet",
                                                               "2026-09.parquet"]
    month = pd.read_parquet(parts / "2026-09.parquet")
    pd.testing.assert_frame_equal(month, pd.concat([a, c]), check_freq=False)
    assert month.attrs["cts_timestamp_timezone"] == "UTC"
    assert month.attrs["cts_timestamp_origin"] == kpc.CONSOLIDATED_ORIGIN
    pack = pd.read_parquet(parts / "2026-09.legacy.parquet")
    pd.testing.assert_frame_equal(pack, b, check_freq=False)
    assert not pack.attrs
    pd.testing.assert_frame_equal(pd.read_parquet(parts / "2026-08.legacy.parquet"), d, check_freq=False)
    pd.testing.assert_frame_equal(_union(parts), union_before, check_freq=False)
    record = records["2026-09"]
    assert record["deleted_files"] == 3
    assert {o["file"] for o in record["outputs"]} == {"2026-09.parquet", "2026-09.legacy.parquet"}
    assert all(len(o["sha256"]) == 64 for o in record["outputs"])
    json.dumps(record, default=str)
    if __import__("os").name == "nt":
        assert not list(parts.glob("2026-09-0*.parquet.lock"))
    # Idempotent.
    assert _run(parts, apply=True) == {}


def test_what_stays_daily(tmp_path):
    parts = tmp_path / "15m_parts"
    _day(parts, "2026-09-01", marked=True)
    # A day file holding eight hours of the previous day: it and that day stay daily.
    _day(parts, "2026-09-05", marked=False)
    _day(parts, "2026-09-06", marked=False, frame=pd.concat([_frame("2026-09-05 16:00", 32, base=900.0),
                                                              _frame("2026-09-06", 96)]))
    # Partial-repair provenance stays with its own tooling.
    partial = _frame("2026-09-07", 96)
    partial.attrs = {"cts_timestamp_status": "unverified_segments_remain"}
    partial.to_parquet(parts / "2026-09-07.parquet")
    # A non-float column layout and a recent unmarked day.
    _day(parts, "2026-09-08", marked=True, frame=_frame("2026-09-08", 96).astype({"volume": "int64"}))
    _day(parts, "2026-10-20", marked=False)
    records = _run(parts, apply=True)
    held = {h["file"]: h["reason"] for r in records.values() for h in r["held"]}
    assert held == {"2026-09-05.parquet": "shares_timestamps_with_other_daily_file",
                    "2026-09-06.parquet": "shares_timestamps_with_other_daily_file",
                    "2026-09-07.parquet": "partial_timestamp_provenance",
                    "2026-09-08.parquet": "dtypes",
                    "2026-10-20.parquet": "recent_unmarked"}
    assert records["2026-10"]["status"] == "nothing_to_move"
    assert sorted(p.name for p in parts.glob("*.parquet")) == sorted(list(held) + ["2026-09.parquet"])


def test_file_with_rows_outside_its_day_stays(tmp_path):
    parts = tmp_path / "15m_parts"
    _day(parts, "2026-09-02", marked=False, frame=_frame("2026-09-01 20:00", 16))
    records = _run(parts, apply=True)
    assert records["2026-09"]["held"][0]["reason"] == "rows_outside_file_period"
    assert (parts / "2026-09-02.parquet").exists()


def test_daily_file_newer_than_period_rows_wins_and_pack_clash_is_refused(tmp_path):
    parts = tmp_path / "15m_parts"
    existing = prepare_utc_kline_frame(_frame("2026-09-01", 96 * 2), timestamp_timezone="UTC")
    parts.mkdir(parents=True)
    existing.to_parquet(parts / "2026-09.parquet")
    recreated = _day(parts, "2026-09-02", marked=True, base=5000.0)
    records = _run(parts, apply=True)
    assert records["2026-09"]["status"] == "consolidated" and records["2026-09"]["overlap_rows"] == 96
    month = pd.read_parquet(parts / "2026-09.parquet")
    assert len(month) == 192
    pd.testing.assert_frame_equal(month.loc["2026-09-02"], recreated, check_freq=False)
    # An unmarked day already present in the period file is never merged.
    _day(parts, "2026-09-01", marked=False, base=1.0)
    records = _run(parts, apply=True)
    assert records["2026-09"]["status"] == "refused"
    assert records["2026-09"]["reason"] == "day_already_in_other_file"
    assert (parts / "2026-09-01.parquet").exists()


def test_failed_verification_swaps_nothing_and_deletes_nothing(tmp_path, monkeypatch):
    parts = tmp_path / "15m_parts"
    _day(parts, "2026-09-01", marked=True)
    _day(parts, "2026-09-02", marked=False)
    before = sorted((p.name, p.read_bytes()) for p in parts.iterdir())
    real_read = kpc.pd.read_parquet

    def corrupting_read(path, *args, **kwargs):
        frame = real_read(path, *args, **kwargs)
        if str(path).endswith(".tmp") and "legacy" in str(path):
            frame.iloc[0, 0] += 1.0
        return frame

    monkeypatch.setattr(kpc.pd, "read_parquet", corrupting_read)
    with pytest.raises(kpc.ConsolidationError):
        kpc.consolidate_period(parts, "2026-09", sorted(parts.glob("2026-09-0*.parquet")), apply=True,
                               legacy_before=LEGACY_BEFORE)
    assert sorted((p.name, p.read_bytes()) for p in parts.iterdir() if p.name != "_series.lock") == before


def test_microsecond_and_nanosecond_days_merge_row_for_row(tmp_path):
    parts = tmp_path / "1h_parts"
    a = _frame("2026-09-01", 24, "h")
    b = _frame("2026-09-02", 24, "h", base=200.0)
    _day(parts, "2026-09-01", marked=False, frame=a.set_axis(a.index.as_unit("us")))
    _day(parts, "2026-09-02", marked=False, frame=b)
    records = _run(parts, apply=True)
    assert records["2026"]["status"] == "consolidated"
    pack = pd.read_parquet(parts / "2026.legacy.parquet")
    assert pack.index.unit == "ns"
    pd.testing.assert_frame_equal(pack, pd.concat([a, b]), check_freq=False)


def test_orphan_lock_cleanup_only_touches_closed_days_without_parquet(tmp_path):
    parts = tmp_path / "15m_parts"
    _day(parts, "2026-09-01", marked=True)
    (parts / "2026-08-01.parquet.lock").write_bytes(b"\0")
    (parts / "2026-10-26.parquet.lock").write_bytes(b"\0")
    (parts / "_series.lock").write_bytes(b"\0")
    removed = kpc.discard_orphan_day_locks(parts, date(2026, 10, 26))
    names = sorted(p.name for p in parts.iterdir())
    if __import__("os").name == "nt":
        assert removed == 1
        assert names == ["2026-09-01.parquet", "2026-09-01.parquet.lock", "2026-10-26.parquet.lock", "_series.lock"]
    else:
        assert removed == 0
