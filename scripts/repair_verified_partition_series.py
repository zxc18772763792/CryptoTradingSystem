"""Plan, but never apply, hourly-witnessed UTC rebucketing of a partition series.

Each complete source-file hour is independently compared with official UTC 1h
OHLCV at stored time and stored time minus eight hours. Unknown rows retain
their timestamps and values. Conflicting or unknown canonical overlaps reject
the conflicting shifted hours (with propagation). Unresolvable original
collisions reject the series. Only literally identical, independently proven
overlaps may be explicitly coalesced. The engine owns backups and application.
"""
from __future__ import annotations

import argparse
import heapq
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.data.parquet_timezone_repair import (  # noqa: E402
    OHLCV, RepairConflict, StalePlan, plan_explicit, sha256, verify_candidates,
)

INTERVAL_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}


def _frame(frame: pd.DataFrame) -> pd.DataFrame:
    if (not isinstance(frame.index, pd.DatetimeIndex) or frame.index.hasnans
            or set(frame.columns) != set(OHLCV)):
        raise RepairConflict("Exact OHLCV columns and valid DatetimeIndex required")
    out = frame[OHLCV].copy()
    if out.index.tz is not None:
        out.index = out.index.tz_convert("UTC").tz_localize(None)
    # Arrow-backed historic files use both us and ns units. Pandas .equals
    # treats those representations as unequal even for identical instants.
    out.index = out.index.as_unit("ns")
    values = out.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values[:, 4] < 0).any():
        raise RepairConflict("Invalid OHLCV; preserve the source without filtering")
    return out


def _read(path: Path, minutes: int, reference: pd.DataFrame) -> dict:
    before = sha256(path)
    original = pd.read_parquet(path)
    source = _frame(original)
    if sha256(path) != before:
        raise StalePlan(f"Partition changed during read: {path}")
    rows = [{"position": pos, "stored": ts.isoformat(), "stored_hour": ts.floor("h").isoformat(), "status": "incomplete_hour",
             "shift_hours": None, "planned": ts.isoformat(), "certified": False}
            for pos, ts in enumerate(source.index)]
    hours, aggregates = [], []
    groups = defaultdict(list)
    for pos, hour in enumerate(source.index.floor("h")):
        groups[hour].append(pos)
    for hour, positions in sorted(groups.items()):
        group = source.iloc[positions].sort_index()
        expected = pd.date_range(hour, periods=60 // minutes, freq=f"{minutes}min")
        evidence = {"stored_hour": hour.isoformat(), "positions": positions,
                    "status": "incomplete_hour", "expected_bars": 60 // minutes,
                    "actual_bars": len(group)}
        hours.append(evidence)
        if not group.index.equals(expected.rename(group.index.name)):
            continue
        aggregates.append((len(hours) - 1, hour, [group.open.iloc[0], group.high.max(),
            group.low.min(), group.close.iloc[-1], group.volume.sum()]))
    if aggregates:
        probe = pd.DataFrame([x[2] for x in aggregates],
                             index=pd.DatetimeIndex([x[1] for x in aggregates]), columns=OHLCV)
        if (original.attrs.get("cts_timestamp_timezone") == "UTC"
                and original.attrs.get("cts_timestamp_schema") == 1):
            probe.attrs = dict(original.attrs)
        witnesses = verify_candidates(probe, reference)
        for (hour_pos, _, values), witness in zip(aggregates, witnesses["rows"]):
            hour = hours[hour_pos]
            hour.update(witness, aggregate_ohlcv=[float(x) for x in values])
            for pos in hour["positions"]:
                rows[pos].update(status=witness["status"], shift_hours=witness["shift_hours"],
                                 certified=witness["status"] == "unique")
                if witness["status"] == "unique":
                    rows[pos]["planned"] = (source.index[pos]
                        + pd.Timedelta(hours=witness["shift_hours"])).isoformat()
    if original.attrs.get("cts_timestamp_timezone") == "UTC" and any(
            row["shift_hours"] == -8 for row in rows):
        raise RepairConflict(f"UTC marker contradicts official hourly witnesses: {path}")
    return {"path": path, "pre_hash": before, "original": original,
            "source": source, "rows": rows, "hours": hours}


def build_series_plans(directory, reference, provenance, through="2026-05-19") -> list[dict]:
    """Return engine-ready plans, including all existing destination neighbors.

    Source files dated <=through are read first. Destination-file closure then
    includes any existing neighbor touched by their planned rows, recursively.
    Every input position has a source mapping, every output has origin pointers,
    and each original file is hash-guarded even when emptied by rebucketing.
    """
    directory = Path(directory).resolve()
    interval = directory.name.removesuffix("_parts")
    if not directory.name.endswith("_parts") or interval not in INTERVAL_MINUTES:
        raise RepairConflict("Only 1m/5m/15m/1h date-partition series are supported")
    cutoff = pd.Timestamp(through)
    if cutoff.tzinfo is not None or cutoff != cutoff.normalize():
        raise RepairConflict("through must be a timezone-naive calendar date")
    ref = _frame(reference).sort_index()
    if not ref.index.is_unique:
        raise RepairConflict("Official reference has duplicate hours")
    paths = []
    for path in sorted(directory.glob("*.parquet")):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.stem):
            raise RepairConflict(f"Unexpected partition filename: {path.name}")
        if pd.Timestamp(path.stem) <= cutoff:
            paths.append(path)
    if not paths:
        raise RepairConflict("No source partitions in the requested date range")
    sources = {}
    pending = set(paths)
    while pending:
        for path in sorted(pending):
            sources[path] = _read(path, INTERVAL_MINUTES[interval], ref)
        pending = set()
        for item in sources.values():
            for row in item["rows"]:
                # A collision can defer a proven shift back to stored time;
                # include neighbors at both possible output dates up front.
                for date in {row["planned"][:10], row["stored"][:10]}:
                    target = directory / f"{date}.parquet"
                    if target not in sources and target.exists():
                        pending.add(target)
    canonical, hour_rows = defaultdict(list), defaultdict(list)
    for path, item in sorted(sources.items()):
        for row in item["rows"]:
            canonical[pd.Timestamp(row["planned"])].append((path, row["position"]))
            hour_rows[(path, row["stored_hour"])].append(row)
    deferred_hours = []
    collision_keys = {key for key, origins in canonical.items() if len(origins) > 1}
    collision_queue = list(collision_keys)
    heapq.heapify(collision_queue)
    while collision_queue:
        key = heapq.heappop(collision_queue)
        collision_keys.remove(key)
        origins = canonical.get(key, [])
        if len(origins) < 2:
            continue
        first_path, first_pos = origins[0]
        value = sources[first_path]["source"].iloc[first_pos]
        proven = all(sources[path]["rows"][pos]["certified"] for path, pos in origins)
        identical = all(value.equals(sources[path]["source"].iloc[pos]) for path, pos in origins[1:])
        if proven and identical:
            continue
        defer = {(path, sources[path]["rows"][pos]["stored_hour"]) for path, pos in origins
                 if sources[path]["rows"][pos]["shift_hours"] == -8}
        if not defer:
            conflict = RepairConflict(f"Unresolvable canonical collision at {key}; entire series deferred")
            conflict.conflict_rows = [{"source_path": str(path), "source_pre_hash": sources[path]["pre_hash"],
                "source_position": pos, "row_mapping": dict(sources[path]["rows"][pos]),
                "original_ohlcv": sources[path]["source"].iloc[pos].tolist(),
                "hour_witness": next(hour for hour in sources[path]["hours"]
                    if hour["stored_hour"] == sources[path]["rows"][pos]["stored_hour"])}
                for path, pos in origins]
            raise conflict
        # Each hour moves at most once. Updating only its old/new keys avoids a
        # full-series scan for every link of a multi-month collision chain.
        for path, hour in sorted(defer):
            affected = hour_rows[(path, hour)]
            deferred_hours.append({"path": str(path), "stored_hour": hour,
                                   "positions": [row["position"] for row in affected]})
            for row in affected:
                old_key, new_key = pd.Timestamp(row["planned"]), pd.Timestamp(row["stored"])
                origin = (path, row["position"])
                canonical[old_key].remove(origin)
                if not canonical[old_key]:
                    del canonical[old_key]
                canonical[new_key].append(origin)
                for changed in (old_key, new_key):
                    if changed not in collision_keys:
                        collision_keys.add(changed)
                        heapq.heappush(collision_queue, changed)
                row.update(proven_utc_deferred=row["planned"], status="verified_shift_deferred_collision",
                           planned=row["stored"], shift_hours=None, certified=False)
    coalesced = []
    values, timestamps, output_origins = [], [], defaultdict(list)
    for key, origins in sorted(canonical.items()):
        first_path, first_pos = origins[0]
        value = sources[first_path]["source"].iloc[first_pos].to_numpy()
        if len(origins) > 1:
            if not all(sources[path]["rows"][pos]["certified"] for path, pos in origins):
                raise RepairConflict(f"Unknown-row canonical collision at {key}; entire series deferred")
            if not all(np.array_equal(value, sources[path]["source"].iloc[pos].to_numpy())
                       for path, pos in origins[1:]):
                raise RepairConflict(f"Literal OHLCV conflict at {key}; entire series deferred")
            coalesced.append({"utc": key.isoformat(), "kind": "literal_identical_proven_overlap",
                              "origins": [{"path": str(path), "position": pos} for path, pos in origins],
                              "removed_rows": len(origins) - 1})
        target = directory / f"{key:%Y-%m-%d}.parquet"
        pointers = [{"path": str(path), "position": pos} for path, pos in origins]
        output_origins[target].append({"timestamp": key.isoformat(), "origins": pointers})
        for path, pos in origins:
            sources[path]["rows"][pos].update(target_path=str(target),
                canonical_origin={"path": str(first_path), "position": first_pos},
                coalesced=(path, pos) != origins[0])
        timestamps.append(key)
        values.append(value)
    combined = pd.DataFrame(values, index=pd.DatetimeIndex(timestamps, name="timestamp"), columns=OHLCV)
    targets = set(sources) | set(output_origins)
    output_groups = {date: group for date, group in combined.groupby(combined.index.strftime("%Y-%m-%d"))}
    total_before = sum(len(item["source"]) for item in sources.values())
    removed = sum(item["removed_rows"] for item in coalesced)
    if total_before != len(combined) + removed:
        raise RepairConflict("Series row accounting failed")
    counts = Counter(row["status"] for item in sources.values() for row in item["rows"])
    summary = {"source_files": len(sources), "target_files": len(targets),
        "rows_before": total_before, "rows_after": len(combined), "coalesced_rows": removed,
        "shifted_rows": sum(row["shift_hours"] == -8 for item in sources.values() for row in item["rows"]),
        "unknown_rows": sum(not row["certified"] for item in sources.values() for row in item["rows"]),
        "deferred_hours": len(deferred_hours),
        "source_row_status_counts": dict(counts),
        "complete_hours": sum(hour["status"] != "incomplete_hour" for item in sources.values() for hour in item["hours"]),
        "hour_status_counts": dict(Counter(hour["status"] for item in sources.values() for hour in item["hours"]))}
    plans = []
    for path in sorted(targets):
        source = sources.get(path)
        output = output_groups.get(path.stem, combined.iloc[:0]).copy()
        if source:
            output.attrs = dict(source["original"].attrs)
        origins = output_origins[path]
        certified = all(sources[Path(pointer["path"])]["rows"][pointer["position"]]["certified"]
                        for row in origins for pointer in row["origins"])
        evidence = {"method": "per_source_complete_hour_two_clock_official_ohlcv_then_series_rebucket",
            "provenance": provenance, "through": cutoff.date().isoformat(), "interval": interval,
            "series_summary": summary, "row_accounting": {"input": total_before,
                "output": len(combined), "explicitly_coalesced": removed},
            "source_path": str(path) if source else None,
            "source_rows": source["rows"] if source else [],
            "source_hour_witnesses": source["hours"] if source else [],
            "source_deferred_hours": [hour for hour in deferred_hours if hour["path"] == str(path)],
            "output_row_origins": origins,
            "unknown_output_rows": sum(not all(sources[Path(p["path"])]["rows"][p["position"]]["certified"]
                for p in row["origins"]) for row in origins)}
        overlap = [item for item in coalesced if item["utc"][:10] == path.stem]
        plan = plan_explicit(path, output, evidence=evidence,
            expected_hash=source["pre_hash"] if source else None,
            rows_before=len(source["source"]) if source else 0,
            coalesced=overlap, utc_certified=certified)
        plans.append(plan)
    # A long series read must not mix versions when an unattended writer ran.
    for path, item in sources.items():
        if sha256(path) != item["pre_hash"]:
            raise StalePlan(f"Partition changed during series planning: {path}")
    return plans


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--through", default="2026-05-19")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    reference_path = args.reference.resolve()
    before = sha256(reference_path)
    metadata_path = reference_path.with_suffix(".meta.json")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if (not metadata.get("complete_fetch") or metadata.get("market") != "spot"
            or metadata.get("interval") != "1h" or metadata.get("timeZone") != "0"
            or metadata.get("parquet_sha256") != before):
        raise RepairConflict("Reference provenance or hash is not valid official UTC spot 1h")
    reference = pd.read_parquet(reference_path)
    if sha256(reference_path) != before:
        raise StalePlan("Official reference changed during read")
    provenance = {"reference": str(reference_path), "reference_sha256": before,
                  "reference_metadata": str(metadata_path)}
    try:
        plans = build_series_plans(args.directory, reference, provenance, args.through)
    except RepairConflict as exc:
        report = {"dry_run": True, "status": "unresolved_unchanged", "directory": str(args.directory.resolve()),
            "through": args.through, "provenance": provenance, "error": str(exc),
            "conflict_rows": getattr(exc, "conflict_rows", [])}
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({"out": str(args.out), "status": report["status"], "error": str(exc)}))
        return 1
    report = {"dry_run": True, "series_summary": plans[0]["evidence"]["series_summary"],
              "plans": [{k: v for k, v in plan.items() if k != "result"} for plan in plans]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"out": str(args.out), **report["series_summary"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
