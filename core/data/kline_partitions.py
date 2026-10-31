"""File layout of kline part directories (``<symbol>/<timeframe>_parts``).

Second-level timeframes keep one file per UTC day: a day of 1s bars is already
megabytes. Every other timeframe stores one file per UTC month (minute bars) or
per UTC year (hour and longer bars). The data volume (F:) is exFAT with 1 MiB
clusters, where the old file per series per day cost about 1 MiB for a few KB
of bars (about 121 GB for 0.7 GB of data on 2026-10-26).

A parts directory may hold, per period:

* ``2026-10.parquet`` / ``2026.parquet``: verified UTC bars (always marked);
* ``2026-10.legacy.parquet``: days packed from old daily files that carry no
  UTC marker. The pack is unmarked, and a writer touching one of its days must
  pass the UTC guard against that day's rows, exactly as with the daily file;
* ``YYYY-MM-DD.parquet``: daily files left as they were (rows outside their own
  day, partial-repair provenance, recent days), and the files of writers that
  predate this layout.

Every row of a packed or period file sits on the UTC day it was stored under in
its daily file, so readers can rebuild the exact order the daily layout gave
them (``read_order``): identical concatenations resolve duplicate timestamps
identically.
"""
from __future__ import annotations

import re
from calendar import monthrange
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

DAY, MONTH, YEAR = "day", "month", "year"
_RANK = {YEAR: 0, MONTH: 1, DAY: 2}
_STEM_FORMAT = {DAY: "%Y-%m-%d", MONTH: "%Y-%m", YEAR: "%Y"}
_STEM_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$")
LEGACY_SUFFIX = ".legacy"

# One lock per series directory serializes month/year writers, consolidation
# and absorption of legacy days (a lock file per month or year would itself
# cost a cluster per period).
SERIES_LOCK_NAME = "_series"


def partition_granularity(timeframe: str) -> str:
    """Target file period for a timeframe: day (seconds), month (minutes), year (hours and longer)."""

    unit = str(timeframe or "").strip()[-1:]
    if unit == "m":
        return MONTH
    if unit in ("h", "H", "d", "D", "w", "W", "M"):
        return YEAR
    return DAY


def partition_stems(index: pd.DatetimeIndex, granularity: str) -> pd.Index:
    """File stem each UTC-naive timestamp belongs to."""

    return pd.DatetimeIndex(index).strftime(_STEM_FORMAT[granularity])


def _split_legacy(stem: str) -> Tuple[str, bool]:
    stem = str(stem)
    if stem.endswith(LEGACY_SUFFIX):
        return stem[: -len(LEGACY_SUFFIX)], True
    return stem, False


def stem_period(stem: str) -> Optional[Tuple[date, date, str]]:
    """``(first_day, last_day, granularity)`` covered by a part file stem, or None if it is not a period."""

    base, legacy = _split_legacy(stem)
    match = _STEM_RE.match(base)
    if not match:
        return None
    year, month, day = match.groups()
    try:
        if day:
            if legacy:
                return None
            only = date(int(year), int(month), int(day))
            return only, only, DAY
        if month:
            first = date(int(year), int(month), 1)
            return first, first.replace(day=monthrange(first.year, first.month)[1]), MONTH
        return date(int(year), 1, 1), date(int(year), 12, 31), YEAR
    except ValueError:
        return None


def is_legacy_pack(path: Path) -> bool:
    return _split_legacy(Path(path).stem)[1] and stem_period(Path(path).stem) is not None


def legacy_pack_path(parts_dir: Path, period_stem: str) -> Path:
    return Path(parts_dir) / f"{period_stem}{LEGACY_SUFFIX}.parquet"


def rows_match_partition(index: pd.Index, path: Path) -> bool:
    """True when every UTC timestamp belongs to the period named by the file's stem."""

    stem = Path(path).stem
    period = stem_period(stem)
    if period is None:
        return len(index) == 0
    return bool((partition_stems(pd.DatetimeIndex(index), period[2]) == _split_legacy(stem)[0]).all())


def part_sort_key(path: Path) -> tuple:
    """Coarse to fine, then chronological (a legacy pack before its period file); non-period names last."""

    period = stem_period(Path(path).stem)
    if period is None:
        return (len(_RANK), date.max, 1, Path(path).name)
    return (_RANK[period[2]], period[0], 0 if is_legacy_pack(path) else 1, Path(path).name)


def list_part_files(parts_dir: Path) -> List[Path]:
    """Every ``*.parquet`` in a parts directory, in read-precedence order (later files win ties)."""

    parts_dir = Path(parts_dir)
    if not parts_dir.is_dir():
        return []
    return sorted(parts_dir.glob("*.parquet"), key=part_sort_key)


def overlapping_part_files(files: Iterable[Path], lower: Optional[date], upper: Optional[date]) -> List[Path]:
    """Files whose period intersects ``[lower, upper]``; files without a period are always kept."""

    kept: List[Path] = []
    for path in files:
        period = stem_period(Path(path).stem)
        if period is not None:
            if lower is not None and period[1] < lower:
                continue
            if upper is not None and period[0] > upper:
                continue
        kept.append(path)
    return kept


def newest_first(files: Iterable[Path]) -> List[Tuple[Path, date]]:
    """Files with the last day they can hold, latest first; files without a period (quarantined copies) last."""

    dated = []
    for path in files:
        period = stem_period(Path(path).stem)
        dated.append((path, period[1] if period is not None else date.min))
    return sorted(dated, key=lambda item: (item[1], part_sort_key(item[0])), reverse=True)


def edge_part_files(files: Iterable[Path]) -> Tuple[List[Path], List[Path]]:
    """``(first, last)``: the files that can hold the earliest and the latest bar of a parts directory.

    The latest bar is no earlier than the start of the latest-starting file, so
    only files ending on or after that day can hold it (and symmetrically).
    """

    files = list(files)
    periods = [(path, stem_period(Path(path).stem)) for path in files]
    dated = [(path, period) for path, period in periods if period is not None]
    undated = [path for path, period in periods if period is None]
    if not dated:
        return undated, undated
    latest_start = max(period[0] for _, period in dated)
    earliest_end = min(period[1] for _, period in dated)
    first = [path for path, period in dated if period[0] <= earliest_end]
    last = [path for path, period in dated if period[1] >= latest_start]
    return first, last


def read_order(frames: List[pd.DataFrame], paths: List[Path]) -> np.ndarray:
    """Row positions that reproduce the daily layout's concatenation of ``frames`` (read from ``paths``).

    The daily layout concatenated one file per day, by day, each sorted by
    time. A row of a period or packed file came from the daily file of its own
    day; a daily file keeps its name's day for every row it holds (some old
    files hold another day's rows). Sorting stably by (file day, time) restores
    that order. A tie (same file day and time) cannot occur in the daily layout;
    it means a writer that predates this layout recreated a day, so only the
    row read last, from the finer and newer file, is kept.
    """

    file_days: List[np.ndarray] = []
    stamps: List[np.ndarray] = []
    for frame, path in zip(frames, paths):
        index = pd.DatetimeIndex(frame.index).as_unit("ns")  # old files mix us and ns indexes
        period = stem_period(Path(path).stem)
        if period is None:
            day = np.full(len(index), np.iinfo(np.int64).max, dtype=np.int64)
        elif period[2] == DAY:
            day = np.full(len(index), pd.Timestamp(period[0]).value, dtype=np.int64)
        else:
            day = index.normalize().asi8
        file_days.append(day)
        stamps.append(index.asi8)
    if not frames:
        return np.array([], dtype=np.int64)
    day_key, time_key = np.concatenate(file_days), np.concatenate(stamps)
    order = np.lexsort((time_key, day_key))
    day_key, time_key = day_key[order], time_key[order]
    tied_with_next = (day_key[:-1] == day_key[1:]) & (time_key[:-1] == time_key[1:])
    if tied_with_next.any():
        order = order[np.append(~tied_with_next, True)]
    return order


def series_lock_target(parts_dir: Path) -> Path:
    """Path whose ``.lock`` sibling serializes month/year writes in one parts directory."""

    return Path(parts_dir) / SERIES_LOCK_NAME


@contextmanager
def part_read_lock(parts_dir: Path) -> Iterator[None]:
    """Keep a period reader's file list valid until its reads finish.

    Consolidation and day absorption replace period files and then remove old
    paths. Atomic file replacement alone cannot protect a multi-file reader
    from silently skipping those removed paths. Seconds have no such moves.
    """
    parts_dir = Path(parts_dir)
    timeframe = parts_dir.name.removesuffix("_parts")
    if not parts_dir.is_dir() or partition_granularity(timeframe) == DAY:
        yield
        return
    from core.data.parquet_lock import parquet_partition_lock

    with parquet_partition_lock(series_lock_target(parts_dir)):
        yield


def series_locks_for(paths: Iterable[Path]) -> List[Path]:
    """Series lock targets a writer of these files must hold first (only month/year/pack files need one).

    A daily file is protected by its own lock: month/year writers take it too
    before they touch that day.
    """

    targets = set()
    for path in paths:
        path = Path(path)
        period = stem_period(path.stem)
        if path.parent.name.endswith("_parts") and period is not None and period[2] != DAY:
            targets.add(series_lock_target(path.parent))
    return sorted(targets, key=lambda p: str(p).lower())


def daily_part_path(parts_dir: Path, day: date) -> Path:
    return Path(parts_dir) / f"{day.isoformat()}.parquet"
