"""Fold legacy daily kline parts into the per-period files of ``core.data.kline_partitions``.

For each month (minute bars) or year (hour and longer bars) of a series:

* days whose daily file carries the verified UTC marker move into the period
  file ``<period>.parquet``;
* days whose daily file carries no timestamp attributes at all move into the
  period's legacy pack ``<period>.legacy.parquet``, which stays unmarked. A
  writer touching one of those days must pass the UTC guard against that day's
  rows, as it had to with the daily file. Recent unmarked days stay daily (a
  writer process that predates this layout would not see them in a pack);
* everything else stays daily and is reported: rows outside the file's day,
  duplicate or invalid timestamps, a column layout other than float64 OHLCV,
  partial-repair provenance.

Each period is one transaction under the series lock and every daily file's
lock: each output file is written to a temp file, read back and compared with
the expected frame exactly, swapped in, read again, and checked row for row
against every source before any daily file is deleted.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from core.data.data_storage import (
    CTS_TIMESTAMP_ORIGIN,
    CTS_TIMESTAMP_SCHEMA,
    CTS_TIMESTAMP_TIMEZONE,
    _has_utc_timestamp_metadata,
    discard_partition_lock_file,
)
from core.data.kline_partitions import (
    DAY,
    legacy_pack_path,
    list_part_files,
    partition_granularity,
    partition_stems,
    rows_match_partition,
    series_lock_target,
    stem_period,
)
from core.data.parquet_lock import parquet_partition_lock

OHLCV = ["open", "high", "low", "close", "volume"]
CONSOLIDATED_ORIGIN = "consolidated_verified_utc_daily_parts"
CLUSTER_BYTES = 1024 * 1024
UNMARKED = "unmarked"


class ConsolidationError(RuntimeError):
    """A written file did not reproduce its sources; no daily file was deleted."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _clusters(size: int) -> int:
    return max(1, -(-int(size) // CLUSTER_BYTES))


def frame_issue(frame: pd.DataFrame, path: Path) -> Optional[str]:
    """None for a clean verified file, ``UNMARKED`` for a clean file without timestamp attributes, else why it stays."""

    if not isinstance(frame.index, pd.DatetimeIndex):
        return "index_not_datetime"
    if frame.index.tz is not None:
        return "index_timezone_aware"
    if frame.index.hasnans:
        return "nat_timestamps"
    if frame.index.has_duplicates:
        return "duplicate_timestamps"
    if frame.index.name != "timestamp":
        return "index_name"
    if list(frame.columns) != OHLCV:
        return "columns"
    if any(str(dtype) != "float64" for dtype in frame.dtypes):
        return "dtypes"
    if not rows_match_partition(frame.index, path):
        return "rows_outside_file_period"
    if _has_utc_timestamp_metadata(frame):
        return None
    if any(str(key).startswith("cts_") for key in frame.attrs):
        return "partial_timestamp_provenance"  # the timezone-repair cohort: left to its own tooling
    return UNMARKED


def _assert_same(expected: pd.DataFrame, actual: pd.DataFrame, what: str, *, verified: bool) -> None:
    try:
        pd.testing.assert_frame_equal(expected, actual, check_exact=True, check_freq=False)
    except AssertionError as exc:
        raise ConsolidationError(f"{what}: {str(exc)[:300]}") from exc
    if _has_utc_timestamp_metadata(actual) != verified:
        raise ConsolidationError(f"{what}: UTC marker is {'missing' if verified else 'unexpectedly present'}")


def _write_exclusive(frame: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(frame)
    with path.open("xb") as stream:
        pq.write_table(table, stream, compression="zstd", compression_level=9)
        stream.flush()
        os.fsync(stream.fileno())


def _replace(source: Path, target: Path) -> None:
    for attempt in range(8):  # Windows refuses the swap while a reader has the target open
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt == 7:
                raise
            time.sleep(0.25 * (attempt + 1))


def _check_file(path: Path, target_name: str, expected: pd.DataFrame, sources: List[tuple], *, verified: bool) -> None:
    """Read ``path`` back: it must equal ``expected`` and hold every ``(name, frame)`` source unchanged."""

    written = pd.read_parquet(path)
    _assert_same(expected, written, target_name, verified=verified)
    for name, frame in sources:
        # Old daily files mix microsecond and nanosecond indexes; concatenation
        # keeps the finer unit, exactly as the reader's concatenation does.
        source = frame.set_axis(frame.index.as_unit(written.index.unit))
        got = written.loc[source.index]
        got.attrs = dict(written.attrs)
        _assert_same(source, got, f"rows of {name} in {target_name}", verified=verified)


def _write_all_verified(plans: List[tuple]) -> List[dict]:
    """Stage and fully check every output before swapping any in; check again after the swap."""

    staged: List[Path] = []
    try:
        for target, expected, sources, verified in plans:
            stage = target.with_name(f"{target.name}.{uuid4().hex}.tmp")
            staged.append(stage)
            _write_exclusive(expected, stage)
            _check_file(stage, f"staged {target.name}", expected, sources, verified=verified)
        for stage, (target, _, _, _) in zip(staged, plans):
            _replace(stage, target)
    finally:
        for stage in staged:
            stage.unlink(missing_ok=True)
    outputs = []
    for target, expected, sources, verified in plans:
        _check_file(target, target.name, expected, sources, verified=verified)
        outputs.append({"file": target.name, "rows": int(len(expected)), "bytes": target.stat().st_size,
                        "sha256": _sha256(target)})
    return outputs


def daily_files_by_period(parts_dir: Path, timeframe: str, before: date) -> Dict[str, List[Path]]:
    """Daily files older than ``before`` grouped by the month/year file they belong in."""

    granularity = partition_granularity(timeframe)
    groups: Dict[str, List[Path]] = {}
    if granularity == DAY:
        return groups
    for path in list_part_files(parts_dir):
        period = stem_period(path.stem)
        if period is None or period[2] != DAY or period[0] >= before:
            continue
        stem = partition_stems(pd.DatetimeIndex([pd.Timestamp(period[0])]), granularity)[0]
        groups.setdefault(stem, []).append(path)
    return groups


def colliding_daily_files(day_paths: List[Path]) -> Dict[Path, List[str]]:
    """Daily files that share a timestamp with another daily file, each with the names of those files.

    Some old files hold rows of a neighbouring day (an eight-hour local-clock
    copy). Both files stay daily: scripts that concatenate part files in
    directory order and keep the last duplicate then still resolve it as before.
    """

    stamps: List[np.ndarray] = []
    owners: List[np.ndarray] = []
    for number, path in enumerate(day_paths):
        try:
            index = pd.DatetimeIndex(pd.read_parquet(path, columns=[]).index)
        except Exception:  # noqa: BLE001 - the period pass reports unreadable files
            continue
        unique = index.unique()  # repeats inside one file are that file's own issue
        stamps.append(unique.asi8)
        owners.append(np.full(len(unique), number))
    if not stamps:
        return {}
    shared = pd.DataFrame({"stamp": np.concatenate(stamps), "owner": np.concatenate(owners)})
    shared = shared[shared["stamp"].duplicated(keep=False)]
    held: Dict[Path, List[str]] = {}
    for _, group in shared.groupby("stamp")["owner"]:
        names = [day_paths[n] for n in sorted(set(group))]
        for path in names:
            partners = held.setdefault(path, [])
            partners.extend(p.name for p in names if p != path and p.name not in partners)
    return held


def _read_existing(path: Path, want_verified: bool) -> tuple:
    """(frame or None, refusal reason or None) for an existing period file or pack."""

    if not path.exists():
        return None, None
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:  # noqa: BLE001 - reported; the period is left alone
        return None, f"{path.name}_unreadable: {type(exc).__name__}"
    issue = frame_issue(frame, path)
    if issue != (None if want_verified else UNMARKED):
        return None, f"{path.name}_{issue or 'verified'}"
    return frame, None


def consolidate_period(parts_dir: Path, stem: str, day_paths: List[Path], *, apply: bool,
                       legacy_before: date, collisions: Optional[Dict[Path, List[str]]] = None,
                       lock_timeout: float = 120.0) -> dict:
    """Plan (and with ``apply`` perform) folding ``day_paths`` into the period file and legacy pack of ``stem``.

    ``collisions`` (from ``colliding_daily_files`` over the whole series) are kept daily.
    """
    collisions = collisions or {}

    parts_dir = Path(parts_dir)
    target = parts_dir / f"{stem}.parquet"
    pack_path = legacy_pack_path(parts_dir, stem)
    record: dict = {"parts_dir": str(parts_dir), "period": stem, "daily_files": len(day_paths)}
    deleted: List[Path] = []
    with ExitStack() as locks:
        if apply:
            locks.enter_context(parquet_partition_lock(series_lock_target(parts_dir), timeout_seconds=lock_timeout))
            for path in day_paths:
                locks.enter_context(parquet_partition_lock(path, timeout_seconds=lock_timeout))
        verified: List[tuple] = []
        legacy: List[tuple] = []
        held: List[dict] = []
        for path in day_paths:
            if not path.exists():
                continue
            if path in collisions:
                held.append({"file": path.name, "reason": "shares_timestamps_with_other_daily_file",
                             "partners": collisions[path]})
                continue
            try:
                frame = pd.read_parquet(path)
            except Exception as exc:  # noqa: BLE001 - the file stays as it is
                held.append({"file": path.name, "reason": "unreadable", "error": f"{type(exc).__name__}: {str(exc)[:120]}"})
                continue
            issue = frame_issue(frame, path)
            if issue is None:
                verified.append((path, frame))
            elif issue == UNMARKED and date.fromisoformat(path.stem) < legacy_before:
                legacy.append((path, frame))
            else:
                held.append({"file": path.name, "reason": "recent_unmarked" if issue == UNMARKED else issue,
                             "rows": int(len(frame))})
        record.update(held=held, held_files=len(held), verified_files=len(verified), legacy_files=len(legacy),
                      verified_rows=int(sum(len(f) for _, f in verified)),
                      legacy_rows=int(sum(len(f) for _, f in legacy)))
        if not verified and not legacy:
            return {**record, "status": "nothing_to_move"}

        existing_target, refusal = _read_existing(target, want_verified=True)
        existing_pack, pack_refusal = _read_existing(pack_path, want_verified=False)
        if refusal or pack_refusal:
            return {**record, "status": "refused", "reason": refusal or pack_refusal}

        def _days(frames) -> set:
            return {day for frame in frames if frame is not None for day in frame.index.normalize()}

        # A verified daily file may repeat a day of the period file (a writer
        # that predates this layout recreated it; the daily file is newer).
        # Any other repeat would make a day live in two kinds of file.
        verified_days, legacy_days = _days(f for _, f in verified), _days(f for _, f in legacy)
        clash = sorted((verified_days & _days([existing_pack]))
                       | (legacy_days & _days([existing_target, existing_pack])))
        if clash:
            return {**record, "status": "refused", "reason": "day_already_in_other_file",
                    "days": [str(d.date()) for d in clash[:10]]}

        plans = []
        if verified:
            sources = ([existing_target] if existing_target is not None else []) + [f for _, f in verified]
            expected = pd.concat(sources)
            record["overlap_rows"] = int(expected.index.duplicated().sum())
            # A daily file can only be newer than the period file's rows for its day.
            expected = expected[~expected.index.duplicated(keep="last")].sort_index(kind="stable")
            origins = sorted({str(f.attrs.get(CTS_TIMESTAMP_ORIGIN, "")) for f in sources} - {""})
            expected.attrs = {CTS_TIMESTAMP_TIMEZONE: "UTC", CTS_TIMESTAMP_SCHEMA: 1,
                              CTS_TIMESTAMP_ORIGIN: CONSOLIDATED_ORIGIN, "cts_timestamp_status": "verified_utc",
                              "cts_source_origins": origins}
            checks = [(p.name, f) for p, f in verified]
            if existing_target is not None:
                moved = pd.DatetimeIndex(pd.concat([f for _, f in verified]).index)
                checks.append((target.name, existing_target[~existing_target.index.isin(moved)]))
            plans.append((target, expected, checks, True))
        if legacy:
            sources = ([existing_pack] if existing_pack is not None else []) + [f for _, f in legacy]
            expected = pd.concat(sources).sort_index(kind="stable")
            expected.attrs = {}
            checks = [(p.name, f) for p, f in legacy] + ([(pack_path.name, existing_pack)] if existing_pack is not None else [])
            plans.append((pack_path, expected, checks, False))
        for path, expected, _, _ in plans:
            if expected.index.has_duplicates or not rows_match_partition(expected.index, path):
                return {**record, "status": "refused", "reason": f"{path.name}_rows_outside_period_or_duplicated"}
        record["outputs"] = [{"file": p.name, "rows_after": int(len(e)), "verified": v} for p, e, _, v in plans]
        if not apply:
            return {**record, "status": "would_consolidate"}

        record["outputs"] = _write_all_verified(plans)
        record["moved"] = ([{"file": p.name, "rows": int(len(f)), "into": target.name} for p, f in verified]
                           + [{"file": p.name, "rows": int(len(f)), "into": pack_path.name} for p, f in legacy])
        for path, _ in verified + legacy:
            path.unlink()
            deleted.append(path)
    for path in deleted:
        discard_partition_lock_file(path)
    record["deleted_files"] = len(deleted)
    record["status"] = "consolidated"
    return record


def discard_orphan_day_locks(parts_dir: Path, before: date) -> int:
    """Delete lock files of closed days whose daily part no longer exists (Windows only; see discard_partition_lock_file)."""

    removed = 0
    for lock in Path(parts_dir).glob("*.parquet.lock"):
        name = lock.name[: -len(".lock")]
        period = stem_period(name[: -len(".parquet")])
        if period is None or period[2] != DAY or period[0] >= before:
            continue
        partition = lock.with_name(name)
        if partition.exists():
            continue
        discard_partition_lock_file(partition)
        if not lock.exists():
            removed += 1
    return removed


def series_targets(storage_root: Path, exchanges: List[str], only: Optional[set] = None,
                   timeframes: Optional[set] = None) -> List[tuple]:
    """``(exchange, symbol_dir, timeframe, parts_dir)`` for every month/year-layout parts directory."""

    found = []
    for exchange in exchanges:
        base = Path(storage_root) / exchange
        if not base.is_dir():
            continue
        for symbol_dir in sorted(p for p in base.iterdir() if p.is_dir()):
            if only and symbol_dir.name.split("_")[0].upper() not in only:
                continue
            for parts_dir in sorted(p for p in symbol_dir.iterdir() if p.is_dir() and p.name.endswith("_parts")):
                timeframe = parts_dir.name[: -len("_parts")]
                if not timeframe or partition_granularity(timeframe) == DAY:
                    continue
                if timeframes and timeframe not in timeframes:
                    continue
                found.append((exchange, symbol_dir.name, timeframe, parts_dir))
    return found


def file_census(parts_dir: Path) -> dict:
    """Files and allocated clusters in a parts directory (1 MiB clusters on the exFAT data volume)."""

    files = clusters = size = 0
    for entry in os.scandir(parts_dir):
        if entry.is_file():
            files += 1
            nbytes = entry.stat().st_size
            size += nbytes
            clusters += _clusters(nbytes)
    return {"files": files, "bytes": size, "clusters": clusters}


def to_json_line(record: dict) -> str:
    return json.dumps(record, ensure_ascii=False, default=str)
