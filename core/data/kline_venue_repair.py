"""Replace USD-M perpetual bars stored in the spot kline store with Binance spot archive bars.

Background: docs/KLINE_VENUE_POLLUTION_2026-10-27.md. With BINANCE_DEFAULT_TYPE=future,
in-process writers saved perp bars under ``data/historical/binance/`` (spot).
``scripts/scan_kline_venue_pollution.py --confirm`` lists the series-days. The
UTC guard refuses any ordinary overwrite of those rows (they disagree with
every spot source), so this is the explicit, reviewed repair.

**Plan** (read-only). For each candidate series-day, every stored row on that
UTC day, in every file of the series that holds it (main ``<tf>.parquet``,
period files, legacy packs, daily files), is compared with the official spot
and USD-M daily archives of the same timeframe (``classify``). Rows of the
classes in ``REPLACED`` get the spot archive's OHLCV at the same timestamp.
No row is added, removed or moved; each file keeps its name, columns, dtypes,
index and attributes (an unmarked legacy pack stays unmarked: replacing some
of its rows does not certify the others). A file's day where more than
``MAX_NEITHER_SHARE`` of the rows match neither archive at their stored time is
left alone entirely (``blocked``): its clock or source is not established. The
plan records, per file, the
sha256 before and the exact sha256 the repaired file will have, and per
series-day the fingerprint of what ``DataStorage.load_klines_from_parquet``
returns before and after (computed by reading the planned bytes in place of
the files).

**Apply**. Per series: every planned row must still hold its planned "before"
values (or already the "after" values); otherwise the series is left alone as
stale. Rows the plan does not touch may have changed since (hourly refresh):
they are written back as they are now. The write itself is
``core.data.parquet_timezone_repair.apply_plan``: it takes the series and file
locks, refuses if a file changed after this check, backs every file up, stages,
reads back, swaps and journals; ``rollback_run`` restores the backups.
"""
from __future__ import annotations

import asyncio
import hashlib
import importlib
import io
import json
import os
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.data.data_storage import DataStorage, discard_partition_lock_file
from core.data.kline_partitions import DAY, is_legacy_pack, list_part_files, stem_period
from core.data.parquet_lock import partition_lock_path
from core.data.parquet_timezone_repair import (
    OHLCV,
    RepairConflict,
    StalePlan,
    _frame_hash,
    _validate,
    apply_plan,
    rollback_run,
    sha256,
)
from core.data.path_utils import symbol_from_storage_dirname

# The module, not the shared instance core.data exports under the same name.
_data_storage = importlib.import_module("core.data.data_storage")
PRICES = OHLCV[:4]
SPOT_NAMESPACE = "binance"
# USD-M publishes no second-level archive, so perp bars cannot be told apart there.
ARCHIVE_TIMEFRAMES = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "1d")
CLASSES = ("spot", "perp", "perp_volume", "perp_forming", "spot_forming", "neither",
           "no_spot_bar", "no_perp_bar", "spot_bar_invalid")
# Only exact archive evidence is replaced; "perp_forming" is a resemblance (perp open, range inside the perp bar)
# that also fits partial bars of unclear origin, so it is reported, not replaced.
REPLACED = ("perp", "perp_volume")
# On a file's day where a larger share of rows matches neither archive at its stored time, the stored clock (or
# source) is not established and nothing on that day is replaced. The 2026-01..05 local-clock days hold whole days
# of bars stored 8 hours off, where a few rows equal the perp bar at the stored time by coincidence. On the
# 2026-10-27 plan every day was at most 20% or at least 50% unmatched.
MAX_NEITHER_SHARE = 0.25
METHOD = "venue_repair_spot_archive_v1"

# (market "spot" | "perp", raw symbol e.g. BTCUSDT, timeframe, UTC day "YYYY-MM-DD") -> archive bars, or
# None when the archive has no such file (not published yet, or not listed that day).
ArchiveFetcher = Callable[[str, str, str, str], Optional[pd.DataFrame]]


@dataclass
class SeriesTarget:
    symbol_dir: str          # BTC_USDT
    timeframe: str           # 15m
    days: List[str]          # UTC days to check
    perp_symbol: str         # BTC_USDT, or 1000PEPE_USDT for contracts quoted per 1000 coins

    @property
    def series(self) -> str:
        return f"{self.symbol_dir}/{self.timeframe}"

    @property
    def multiplier(self) -> float:
        prefix = self.perp_symbol[: len(self.perp_symbol) - len(self.symbol_dir)]
        return float(prefix or 1)


def load_targets(days_file: Path, *, only: Optional[Iterable[str]] = None,
                 timeframes: Optional[Iterable[str]] = None) -> List[SeriesTarget]:
    """Series-days to check, from a ``polluted_days_*.jsonl`` written by the scan with ``--confirm``.

    A day qualifies when the confirm step found rows equal to the perp archive and not to the spot archive,
    or could not compare because the perp archive was not published yet (it is retried here).
    """

    only_set = {s.upper() for s in only} if only else None
    tf_set = set(timeframes) if timeframes else None
    grouped: Dict[str, dict] = {}
    with Path(days_file).open(encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            symbol_dir, timeframe = record["series"].split("/")
            if timeframe not in ARCHIVE_TIMEFRAMES:
                continue
            if only_set and symbol_dir.split("_")[0].upper() not in only_set:
                continue
            if tf_set and timeframe not in tf_set:
                continue
            confirmed = int(record.get("rows_perp_not_spot") or 0) > 0
            unconfirmed = "rows_perp_not_spot" not in record and record.get("rows_equal_perp_archive") is None
            if not (confirmed or unconfirmed):
                continue
            item = grouped.setdefault(record["series"], {"days": set(), "perp": record.get("perp_reference") or symbol_dir})
            item["days"].add(record["day"])
    return [SeriesTarget(series.split("/")[0], series.split("/")[1], sorted(item["days"]), item["perp"])
            for series, item in sorted(grouped.items())]


def fetch_archives(target: SeriesTarget, fetch: ArchiveFetcher, *, workers: int = 8) -> Dict[str, Dict[str, Optional[pd.DataFrame]]]:
    """``{"spot": {day: frame|None}, "perp": {...}}``; perp prices divided by the contract multiplier.

    The connector stored mapped contracts (1000PEPE/USDT for PEPE/USDT) with prices divided by the
    multiplier and the contract volume unchanged, so the reference is built the same way.
    """

    from concurrent.futures import ThreadPoolExecutor

    spot_raw = target.symbol_dir.replace("_", "")
    perp_raw = target.perp_symbol.replace("_", "")
    jobs = [(market, raw, day) for day in target.days for market, raw in (("spot", spot_raw), ("perp", perp_raw))]
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        frames = list(pool.map(lambda job: fetch(job[0], job[1], target.timeframe, job[2]), jobs))
    out: Dict[str, Dict[str, Optional[pd.DataFrame]]] = {"spot": {}, "perp": {}}
    for (market, _, day), frame in zip(jobs, frames):
        if frame is not None:
            frame = frame[OHLCV].astype(float)
            frame.index = pd.DatetimeIndex(frame.index).as_unit("ns")
            frame = frame[~frame.index.duplicated(keep="last")].sort_index()
            if market == "perp" and target.multiplier > 1:
                frame = frame.copy()
                frame[PRICES] = frame[PRICES] / target.multiplier
        out[market][day] = frame
    return out


def _valid_bars(values: np.ndarray) -> np.ndarray:
    o, h, l, c, v = (values[:, i] for i in range(5))
    return (np.isfinite(values).all(axis=1) & (np.minimum.reduce([o, h, l, c]) > 0) & (v >= 0)
            & (h >= np.maximum.reduce([o, l, c])) & (l <= np.minimum.reduce([o, h, c])))


def classify(values: np.ndarray, spot: np.ndarray, perp: np.ndarray) -> np.ndarray:
    """Class of each stored OHLCV row against the spot and perp archive rows at the same timestamp (NaN = absent).

    * ``spot``: OHLCV equals the spot archive (left as it is);
    * ``perp``: open/high/low/close equal the perp archive and differ from spot (the scan's definition);
    * ``perp_volume``: prices equal on both venues, volume equals the perp archive's, not the spot's;
    * ``perp_forming``: looks like a perp bar saved while it was still forming: the perp open (not the spot open),
      a range inside the final perp bar and no more than its volume (reported only: partial bars of other origin
      fit it too);
    * ``spot_forming``: the same against the spot bar (spot data, only incomplete: left as it is);
    * ``neither``, ``no_spot_bar``, ``no_perp_bar``: left as they are;
    * ``spot_bar_invalid``: would be replaced, but the spot archive bar fails the bar checks.
    """

    values, spot, perp = (np.asarray(a, dtype=float).reshape(-1, 5) for a in (values, spot, perp))
    has_spot = ~np.isnan(spot).any(axis=1)
    has_perp = ~np.isnan(perp).any(axis=1)
    eq_spot = (values == spot).all(axis=1)
    px_spot = (values[:, :4] == spot[:, :4]).all(axis=1)
    px_perp = (values[:, :4] == perp[:, :4]).all(axis=1)

    def forming(ref: np.ndarray) -> np.ndarray:
        return ((values[:, 0] == ref[:, 0]) & (values[:, 1] <= ref[:, 1]) & (values[:, 2] >= ref[:, 2])
                & (values[:, 4] <= ref[:, 4]))

    out = np.full(len(values), "neither", dtype=object)
    rules = [
        (~has_spot, "no_spot_bar"),
        (eq_spot, "spot"),
        (~has_perp, "no_perp_bar"),
        (px_perp & ~px_spot, "perp"),
        (px_perp & px_spot & (values[:, 4] == perp[:, 4]) & (values[:, 4] != spot[:, 4]), "perp_volume"),
        (forming(perp) & (values[:, 0] != spot[:, 0]), "perp_forming"),
        (forming(spot), "spot_forming"),
    ]
    decided = np.zeros(len(values), dtype=bool)
    for mask, name in rules:
        take = mask & ~decided
        out[take] = name
        decided |= take
    replace = np.isin(out, REPLACED)
    out[replace & ~_valid_bars(np.where(np.isnan(spot), 0.0, spot))] = "spot_bar_invalid"
    return out


def _utc_ns(index: pd.Index) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex(index)
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    return index.as_unit("ns")


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    """What ``apply_plan`` will write for this frame, byte for byte."""

    buffer = io.BytesIO()
    _validate(frame, unique=True).to_parquet(buffer)
    return buffer.getvalue()


def _read_stable(path: Path) -> Tuple[pd.DataFrame, str]:
    before = sha256(path)
    frame = pd.read_parquet(path)
    if sha256(path) != before:
        raise StalePlan(f"File changed while it was read: {path}")
    return frame, before


def series_files(storage_root: Path, symbol_dir: str, timeframe: str) -> List[Path]:
    root = Path(storage_root) / SPOT_NAMESPACE / symbol_dir
    main = root / f"{timeframe}.parquet"
    return ([main] if main.exists() else []) + list_part_files(root / f"{timeframe}_parts")


def file_kind(path: Path) -> str:
    if not path.parent.name.endswith("_parts"):
        return "main"
    if is_legacy_pack(path):
        return "legacy"
    period = stem_period(path.stem)
    if period is None:
        return "other"
    return "daily" if period[2] == DAY else "period"


def _may_hold(path: Path, days: List[pd.Timestamp]) -> bool:
    """A part file named for a day/month/year holds only that period's rows (the writers enforce it)."""

    period = stem_period(path.stem) if path.parent.name.endswith("_parts") else None
    if period is None:
        return True
    return any(period[0] <= day.date() <= period[1] for day in days)


def day_digests(frame: pd.DataFrame, days: Iterable[pd.Timestamp]) -> Dict[pd.Timestamp, Tuple[int, str]]:
    """Row count and SHA-1 over timestamps and OHLCV per UTC day (as scripts/snapshot_kline_reader_digests.py)."""

    out: Dict[pd.Timestamp, Tuple[int, str]] = {}
    if frame is None or frame.empty:
        return {day: (0, "") for day in days}
    index = _utc_ns(frame.index)
    normalized = index.normalize()
    for day in days:
        mask = normalized == day
        digest = hashlib.sha1(np.ascontiguousarray(index[mask].asi8).tobytes())
        digest.update(np.ascontiguousarray(frame.loc[mask].reindex(columns=OHLCV).to_numpy(dtype="float64")).tobytes())
        out[day] = (int(mask.sum()), digest.hexdigest())
    return out


class _PandasReadingOverrides:
    """``pandas`` as seen by ``core.data.data_storage``, with chosen files read from in-memory bytes."""

    def __init__(self, overrides: Dict[Path, bytes]):
        self._overrides = {os.path.normcase(str(Path(p).resolve())): payload for p, payload in overrides.items()}

    def read_parquet(self, path, *args, **kwargs):
        payload = self._overrides.get(os.path.normcase(str(Path(path).resolve())))
        if payload is not None:
            return pd.read_parquet(io.BytesIO(payload), *args, **kwargs)
        return pd.read_parquet(path, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(pd, name)


@contextmanager
def _reading_overrides(overrides: Dict[Path, bytes]) -> Iterator[None]:
    """Make the reader see planned file contents. Not thread-safe: reader calls must not overlap."""

    if not overrides:
        yield
        return
    original = _data_storage.pd
    _data_storage.pd = _PandasReadingOverrides(overrides)
    try:
        yield
    finally:
        _data_storage.pd = original


def reader_frame(storage_root: Path, symbol_dir: str, timeframe: str,
                 overrides: Optional[Dict[Path, bytes]] = None) -> pd.DataFrame:
    """What ``DataStorage.load_klines_from_parquet`` returns for the whole series (main file plus parts)."""

    storage = DataStorage()
    storage.storage_path = Path(storage_root)
    with _reading_overrides(overrides or {}):
        return asyncio.run(storage.load_klines_from_parquet(
            SPOT_NAMESPACE, symbol_from_storage_dirname(symbol_dir), timeframe))


def _references(frames: Dict[str, Optional[pd.DataFrame]], days: List[str]) -> pd.DataFrame:
    parts = [frames[day] for day in days if frames.get(day) is not None]
    if not parts:
        return pd.DataFrame(columns=OHLCV, index=pd.DatetimeIndex([], name="timestamp", dtype="datetime64[ns]"))
    return pd.concat(parts)


def _class_counts(frame: pd.DataFrame, spot_ref: pd.DataFrame, perp_ref: pd.DataFrame,
                  days: List[pd.Timestamp]) -> Dict[pd.Timestamp, Counter]:
    counts: Dict[pd.Timestamp, Counter] = {day: Counter() for day in days}
    if frame is None or frame.empty:
        return counts
    index = _utc_ns(frame.index)
    mask = index.normalize().isin(days)
    if not mask.any():
        return counts
    stamps = index[mask]
    classes = classify(frame.loc[mask, OHLCV].to_numpy(float),
                       spot_ref.reindex(stamps)[OHLCV].to_numpy(float),
                       perp_ref.reindex(stamps)[OHLCV].to_numpy(float))
    for day, cls in zip(stamps.normalize(), classes):
        counts[day][cls] += 1
    return counts


@dataclass
class SeriesPlan:
    target: SeriesTarget
    status: str = "nothing_to_do"            # ready | nothing_to_do | refused
    reason: str = ""
    files: List[dict] = field(default_factory=list)
    days: List[dict] = field(default_factory=list)
    changes: Optional[pd.DataFrame] = None
    unresolved: Optional[pd.DataFrame] = None
    archives: Optional[pd.DataFrame] = None


def _row_records(series: str, file: str, stamps: pd.DatetimeIndex, classes: np.ndarray,
                 before: np.ndarray, after: np.ndarray) -> pd.DataFrame:
    data = {"series": series, "file": file, "timestamp": stamps, "class": classes}
    for i, name in enumerate(OHLCV):
        data[f"before_{name}"] = before[:, i]
    for i, name in enumerate(OHLCV):
        data[f"after_{name}"] = after[:, i]
    return pd.DataFrame(data)


def plan_series(storage_root: Path, target: SeriesTarget,
                archives: Dict[str, Dict[str, Optional[pd.DataFrame]]]) -> SeriesPlan:
    """Plan the repair of one series (no file is written). See the module docstring."""

    plan = SeriesPlan(target)
    storage_root = Path(storage_root)
    series_root = storage_root / SPOT_NAMESPACE / target.symbol_dir
    usable = [day for day in target.days if archives["spot"].get(day) is not None and archives["perp"].get(day) is not None]
    for day in target.days:
        if day not in usable:
            missing = [m for m in ("spot", "perp") if archives[m].get(day) is None]
            plan.days.append({"series": target.series, "day": day, "status": "no_archive_" + "_".join(missing)})
    evidence = []
    for market in ("spot", "perp"):
        for day in target.days:
            frame = archives[market].get(day)
            if frame is not None and len(frame):
                evidence.append(frame.assign(market=market, series=target.series, day=day))
    if evidence:
        plan.archives = pd.concat(evidence).rename_axis("timestamp").reset_index()
    if not usable:
        plan.status, plan.reason = "nothing_to_do", "no day has both archives"
        return plan
    day_stamps = [pd.Timestamp(day) for day in usable]
    spot_ref = _references(archives["spot"], usable)
    perp_ref = _references(archives["perp"], usable)

    changes: List[pd.DataFrame] = []
    unresolved: List[pd.DataFrame] = []
    overrides: Dict[Path, bytes] = {}
    blocked_files: Counter = Counter()
    try:
        for path in series_files(storage_root, target.symbol_dir, target.timeframe):
            if not _may_hold(path, day_stamps):
                continue
            raw, pre_hash = _read_stable(path)
            index = _utc_ns(raw.index)
            in_days = index.normalize().isin(day_stamps)
            if not in_days.any():
                continue
            rel = path.relative_to(series_root).as_posix()
            if not set(OHLCV).issubset(raw.columns):
                raise RepairConflict(f"{rel}: columns {list(raw.columns)} lack OHLCV")
            if index.hasnans or index[in_days].has_duplicates:
                raise RepairConflict(f"{rel}: invalid or duplicate timestamps on the days to repair")
            positions = np.flatnonzero(in_days)
            stamps = index[positions]
            values = raw.iloc[positions][OHLCV].to_numpy(float)
            spot = spot_ref.reindex(stamps)[OHLCV].to_numpy(float)
            classes = classify(values, spot, perp_ref.reindex(stamps)[OHLCV].to_numpy(float))
            counts = Counter(classes)
            row_days = stamps.normalize()
            unmatched = pd.Series(classes == "neither").groupby(row_days).mean()
            blocked_days = unmatched.index[unmatched > MAX_NEITHER_SHARE]
            blocked = np.asarray(row_days.isin(blocked_days))
            replace = np.isin(classes, REPLACED) & ~blocked
            record = {"series": target.series, "file": rel, "kind": file_kind(path), "rows": int(len(raw)),
                      "rows_on_days": int(len(positions)), "pre_sha256": pre_hash,
                      "frame_hash_before": _frame_hash(raw),
                      **{f"n_{name}": int(counts.get(name, 0)) for name in CLASSES},
                      "blocked_days": ";".join(day.date().isoformat() for day in blocked_days),
                      "rows_blocked": int((np.isin(classes, REPLACED) & blocked).sum())}
            for day in blocked_days:
                blocked_files[day] += 1
            keep = blocked | ~(np.isin(classes, ("spot",)) | replace)
            if keep.any():
                rows = _row_records(target.series, rel, stamps[keep], classes[keep], values[keep], spot[keep])
                unresolved.append(rows.assign(blocked=blocked[keep]))
            record["rows_changed"] = int(replace.sum())
            if replace.any():
                planned = raw.copy()
                columns = [planned.columns.get_loc(name) for name in OHLCV]
                for column, source in zip(columns, spot[replace].T):
                    planned.iloc[positions[replace], column] = source
                payload = _parquet_bytes(planned)
                check = pd.read_parquet(io.BytesIO(payload))
                pd.testing.assert_frame_equal(_validate(planned, unique=True), check, check_exact=True, check_freq=False)
                if check.attrs != raw.attrs:
                    raise RepairConflict(f"{rel}: attributes would not survive the rewrite")
                untouched = np.ones(len(raw), dtype=bool)
                untouched[positions[replace]] = False
                if not np.array_equal(check.iloc[untouched][OHLCV].to_numpy(float), raw.iloc[untouched][OHLCV].to_numpy(float),
                                      equal_nan=True):
                    raise RepairConflict(f"{rel}: rows outside the plan would change")
                overrides[path] = payload
                record.update(planned_post_sha256=hashlib.sha256(payload).hexdigest(),
                              frame_hash_after=_frame_hash(check))
                changes.append(_row_records(target.series, rel, stamps[replace], classes[replace],
                                            values[replace], spot[replace]))
            plan.files.append(record)
    except RepairConflict as exc:  # StalePlan included
        plan.status, plan.reason = "refused", str(exc)
        return plan

    before = reader_frame(storage_root, target.symbol_dir, target.timeframe)
    after = reader_frame(storage_root, target.symbol_dir, target.timeframe, overrides) if overrides else before
    digest_before, digest_after = day_digests(before, day_stamps), day_digests(after, day_stamps)
    classes_before = _class_counts(before, spot_ref, perp_ref, day_stamps)
    classes_after = _class_counts(after, spot_ref, perp_ref, day_stamps)
    archive_digest = {}
    if len(after):
        after_index = _utc_ns(after.index)
        for day in day_stamps:
            stamps = after_index[after_index.normalize() == day]
            ref = spot_ref.reindex(stamps)
            archive_digest[day] = day_digests(ref, [day])[day] if len(stamps) and not ref.isna().any().any() else None
    changed_per_day = Counter()
    for frame in changes:
        changed_per_day.update(pd.DatetimeIndex(frame["timestamp"]).normalize())
    for day in day_stamps:
        row = {"series": target.series, "day": day.date().isoformat(), "status": "checked",
               "rows_changed_in_files": int(changed_per_day.get(day, 0)),
               "files_blocked": int(blocked_files.get(day, 0)),
               "reader_rows_before": digest_before[day][0], "reader_digest_before": digest_before[day][1],
               "reader_rows_after": digest_after[day][0], "reader_digest_after": digest_after[day][1],
               "reader_after_equals_spot_archive": archive_digest.get(day) == digest_after[day]}
        for name in CLASSES:
            row[f"before_{name}"] = int(classes_before[day].get(name, 0))
            row[f"after_{name}"] = int(classes_after[day].get(name, 0))
        plan.days.append(row)
    plan.days.sort(key=lambda r: r["day"])
    plan.changes = pd.concat(changes, ignore_index=True) if changes else None
    plan.unresolved = pd.concat(unresolved, ignore_index=True) if unresolved else None
    if any(sum(r.get(f"after_{name}", 0) for name in REPLACED) for r in plan.days if not r.get("files_blocked")):
        plan.status, plan.reason = "refused", "the reader would still return replaceable rows after the repair"
        return plan
    plan.status = "ready" if changes else "nothing_to_do"
    return plan


def diagnose_blocked_day(target: SeriesTarget, day: str, rows_by_file: Dict[str, pd.DataFrame],
                         fetch: ArchiveFetcher) -> List[dict]:
    """What a blocked file-day holds: the share of its rows equal (OHLCV) to spot or perp bars 0 or 8 hours away.

    Reporting only. Bars stored on the local clock belong to the timezone repair
    (core/data/parquet_timezone_repair.py), not to this one.
    """

    center = pd.Timestamp(day)
    around = [(center + pd.Timedelta(days=k)).date().isoformat() for k in (-1, 0, 1)]
    archives = fetch_archives(SeriesTarget(target.symbol_dir, target.timeframe, around, target.perp_symbol), fetch,
                              workers=4)
    references = {market: _references(archives[market], around) for market in ("spot", "perp")}
    out = []
    for rel, rows in sorted(rows_by_file.items()):
        stamps = pd.DatetimeIndex(rows["timestamp"]).as_unit("ns")
        values = rows[[f"before_{name}" for name in OHLCV]].to_numpy(float)
        shares = {}
        for market, reference in references.items():
            for hours in (-8, 0, 8):
                found = reference.reindex(stamps + pd.Timedelta(hours=hours))[OHLCV].to_numpy(float)
                shares[f"{market}{hours:+d}h"] = float((values == found).all(axis=1).mean()) if len(values) else 0.0
        best = max(shares, key=shares.get)
        out.append({"series": target.series, "day": day, "file": rel, "rows": int(len(rows)),
                    "rows_blocked": int(np.isin(rows["class"], REPLACED).sum()),
                    "explained_by": best if shares[best] >= 0.95 else "unexplained",
                    **{f"share_{name}": round(value, 4) for name, value in shares.items()}})
    return out


# --------------------------------------------------------------------------------------------- apply


@contextmanager
def _no_new_period_locks(paths: Iterable[Path]) -> Iterator[None]:
    """Delete the lock files the engine creates beside period files and packs (one 1 MiB cluster each).

    Month/year writers serialize on the series lock only (core.data.kline_partitions); the engine also
    locks each file. Daily and main files keep their lock files, as their writers expect.
    """

    fresh = [path for path in paths if file_kind(path) in ("period", "legacy") and not partition_lock_path(path).exists()]
    try:
        yield
    finally:
        for path in fresh:
            discard_partition_lock_file(path)


def _changed_frame(raw: pd.DataFrame, rows: pd.DataFrame, rel: str) -> Tuple[Optional[pd.DataFrame], Counter]:
    """``raw`` with the planned rows replaced, or None when every planned row already holds its "after" values."""

    index = _utc_ns(raw.index)
    lookup = pd.Series(np.arange(len(index)), index=index)
    if lookup.index.has_duplicates:
        dup = lookup.index[lookup.index.duplicated()]
        if pd.DatetimeIndex(rows["timestamp"]).isin(dup).any():
            raise StalePlan(f"{rel}: duplicate timestamps on planned rows")
        lookup = lookup[~lookup.index.duplicated(keep="last")]
    stamps = pd.DatetimeIndex(rows["timestamp"]).as_unit("ns")
    missing = stamps[~stamps.isin(lookup.index)]
    if len(missing):
        raise StalePlan(f"{rel}: {len(missing)} planned rows are gone (first {missing.min()})")
    positions = lookup.loc[stamps].to_numpy()
    current = raw.iloc[positions][OHLCV].to_numpy(float)
    before = rows[[f"before_{n}" for n in OHLCV]].to_numpy(float)
    after = rows[[f"after_{n}" for n in OHLCV]].to_numpy(float)
    is_before = (current == before).all(axis=1)
    is_after = (current == after).all(axis=1)
    if not (is_before | is_after).all():
        bad = stamps[~(is_before | is_after)]
        raise StalePlan(f"{rel}: {len(bad)} planned rows changed since the plan (first {bad.min()})")
    counts = Counter(apply=int(is_before.sum()), already=int((is_after & ~is_before).sum()))
    if not is_before.any():
        return None, counts
    output = raw.copy()
    for i, name in enumerate(OHLCV):
        output.iloc[positions[is_before], output.columns.get_loc(name)] = after[is_before, i]
    return output, counts


def prepare_series(storage_root: Path, symbol_dir: str, timeframe: str, changes: pd.DataFrame) -> List[dict]:
    """Engine plans (``parquet_timezone_repair.apply_plan``) for one series' reviewed change set, read now.

    Raises ``StalePlan`` when a planned row no longer holds its planned "before" or "after" values.
    """

    series_root = Path(storage_root) / SPOT_NAMESPACE / symbol_dir
    plans: List[dict] = []
    for rel, rows in changes.groupby("file", sort=True):
        path = series_root / rel
        if not path.exists():
            raise StalePlan(f"{rel}: file is gone")
        raw, pre_hash = _read_stable(path)
        output, counts = _changed_frame(raw, rows, rel)
        if output is None:
            continue
        plans.append({
            "path": str(path.resolve()), "pre_hash": pre_hash, "status": "ready",
            "result": output, "result_hash": _frame_hash(output),
            # apply_plan leaves attributes alone when utc_certified is False: a marked period file stays
            # marked, an unmarked legacy pack stays unmarked.
            "utc_certified": False,
            "rows_before": int(len(raw)), "rows_after": int(len(output)),
            "rows_replaced": counts["apply"], "rows_already_repaired": counts["already"],
            "evidence": {"method": METHOD, "series": f"{symbol_dir}/{timeframe}", "file": rel,
                         "replaced_classes": sorted(set(rows["class"])),
                         "rows": counts["apply"], "first": str(rows["timestamp"].min()), "last": str(rows["timestamp"].max())},
        })
    return plans


def _discard_unapplied_stages(run_dir: Path) -> int:
    """Remove staged outputs the engine did not swap in after a failure; return how many files it did swap."""

    manifest = Path(run_dir) / "manifest.json"
    if not manifest.exists():
        return 0
    journal = json.loads(manifest.read_text(encoding="utf-8"))
    for item in journal.get("files", []):
        if not item.get("applied") and item.get("stage"):
            Path(item["stage"]).unlink(missing_ok=True)
    return sum(1 for item in journal.get("files", []) if item.get("applied"))


def apply_series(storage_root: Path, symbol_dir: str, timeframe: str, changes: pd.DataFrame, run_dir: Path,
                 *, attempts: int = 3) -> dict:
    """Write one series' reviewed change set. ``run_dir`` must not exist yet (backups and manifest go there)."""

    result = {"series": f"{symbol_dir}/{timeframe}", "run_dir": str(run_dir)}
    for attempt in range(1, attempts + 1):
        try:
            plans = prepare_series(storage_root, symbol_dir, timeframe, changes)
        except StalePlan as exc:
            return {**result, "status": "stale", "reason": str(exc)}
        if not plans:
            return {**result, "status": "already_applied"}
        target_dir = Path(run_dir) if attempt == 1 else Path(f"{run_dir}_retry{attempt}")
        try:
            with _no_new_period_locks(Path(p["path"]) for p in plans):
                journal = apply_plan(plans, target_dir)
        except StalePlan as exc:  # a writer changed a file between the check above and the locked re-check
            result.setdefault("retries", []).append(str(exc))
            continue
        except Exception as exc:  # noqa: BLE001 - e.g. Windows refused a swap while a reader had the file open
            applied = _discard_unapplied_stages(target_dir)
            return {**result, "status": "failed" if applied else "refused", "files_swapped": applied,
                    "reason": f"{type(exc).__name__}: {exc}", "run_dir": str(target_dir),
                    # Swapped files hold the repaired rows; the next apply finds them "already repaired".
                    **({"next": "apply again to finish this series"} if applied else {})}
        files = [{"file": Path(item["path"]).name, "pre_sha256": item["pre_hash"], "post_sha256": item["post_hash"],
                  "rows_replaced": item["rows_replaced"], "rows_already_repaired": item["rows_already_repaired"],
                  "backup": item["backup"]} for item in journal["files"]]
        return {**result, "status": journal["status"], "run_dir": str(target_dir), "files": files}
    return {**result, "status": "stale", "reason": "files kept changing while being written"}


def rollback_series(series_run_dir: Path) -> dict:
    """Restore one applied series from its backups (refused for a file changed after the repair)."""

    journal = json.loads((Path(series_run_dir) / "manifest.json").read_text(encoding="utf-8"))
    with _no_new_period_locks(Path(item["path"]) for item in journal["files"]):
        return rollback_run(series_run_dir)


def verify_series(storage_root: Path, symbol_dir: str, timeframe: str, days: pd.DataFrame,
                  archives: pd.DataFrame) -> List[dict]:
    """Reader view of each planned day now, against the plan's "after" fingerprint and the archives."""

    day_stamps = [pd.Timestamp(day) for day in days["day"]]
    spot = archives[archives["market"] == "spot"].set_index("timestamp")[OHLCV]
    perp = archives[archives["market"] == "perp"].set_index("timestamp")[OHLCV]
    spot.index, perp.index = _utc_ns(spot.index), _utc_ns(perp.index)
    frame = reader_frame(storage_root, symbol_dir, timeframe)
    digests = day_digests(frame, day_stamps)
    counts = _class_counts(frame, spot, perp, day_stamps)
    out = []
    for (_, planned), day in zip(days.iterrows(), day_stamps):
        rows, digest = digests[day]
        out.append({"series": planned["series"], "day": planned["day"], "reader_rows": rows,
                    "matches_planned_after": digest == planned["reader_digest_after"],
                    "replaceable_rows_left": int(sum(counts[day].get(name, 0) for name in REPLACED)),
                    **{f"now_{name}": int(counts[day].get(name, 0)) for name in CLASSES}})
    return out


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
