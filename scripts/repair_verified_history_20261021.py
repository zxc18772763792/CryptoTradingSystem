"""Bounded repair of the audited cohort; unknown logical rows stay unchanged.

Official reference collection is separate and read-only. This command journals
each file/group independently and never edits research snapshots or ledgers.
"""
from __future__ import annotations
import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.data.parquet_timezone_repair import (OHLCV, RepairConflict, apply_plan,
    plan_explicit, sha256, verify_candidates)

AUDIT = ROOT / "data/research/timezone_repair_20261020"
REFS = AUDIT / "official_reference"


def utc_frame(frame):
    out = frame.copy()
    if not isinstance(out.index, pd.DatetimeIndex) or out.index.hasnans:
        raise RepairConflict("Invalid datetime index")
    if out.index.tz is not None:
        out.index = out.index.tz_convert("UTC").tz_localize(None)
    return out


def load_reference(symbol, interval):
    path = REFS / symbol.replace("_", "") / f"{interval}.parquet"
    meta = json.loads(path.with_suffix(".meta.json").read_text(encoding="utf-8"))
    if (not meta.get("complete_fetch") or meta.get("market") != "spot"
            or meta.get("interval") != interval or meta.get("timeZone") != "0"
            or sha256(path) != meta["parquet_sha256"]):
        raise RepairConflict("Incomplete, wrong-market, or changed official reference")
    return utc_frame(pd.read_parquet(path)), {"reference": str(path),
        "reference_sha256": meta["parquet_sha256"], "reference_metadata": str(path.with_suffix(".meta.json"))}


def build_monolith_plan(path, reference, provenance):
    """Resolve proven rows only; preserve unknown positions and mark partial.

    A partial candle can be closed from the official bar only when its exact
    opening price, contained high/low/close and smaller volume identify one
    candidate uniquely. This is recorded separately from exact OHLCV evidence.
    """
    path = Path(path).resolve()
    pre = sha256(path)
    original = pd.read_parquet(path)
    source = utc_frame(original)
    if sha256(path) != pre:
        raise RepairConflict("Source changed during read")
    if set(source.columns) != set(OHLCV):
        raise RepairConflict("Unknown extra columns; do not reconstruct them")
    source = source[OHLCV]
    ref = utc_frame(reference)
    evidence = verify_candidates(source, ref)
    rows = evidence["rows"]
    resolved = []
    # Partial live snapshots are a data-value repair, never silently deduped.
    for item in rows:
        if item["status"] != "unmatched":
            continue
        value = source.iloc[item["position"]][OHLCV].to_numpy(dtype=float)
        options = []
        for offset in (0, -8):
            key = source.index[item["position"]] + pd.Timedelta(hours=offset)
            if key not in ref.index:
                continue
            target = ref.loc[key, OHLCV].to_numpy(dtype=float)
            tol = 1e-8
            valid = (np.isclose(value[0], target[0], rtol=1e-10, atol=1e-10)
                and value[1] <= target[1] + tol and value[2] >= target[2] - tol
                and value[2] <= min(value[0], value[3]) <= max(value[0], value[3]) <= value[1]
                and value[4] <= target[4] + tol and value[4] >= 0)
            if valid:
                options.append((offset, key, target))
        if len(options) == 1:
            offset, key, target = options[0]
            resolved.append({"position": item["position"], "utc": key.isoformat(),
                             "original_ohlcv": value.tolist(), "official_ohlcv": target.tolist(),
                             "reason": "unique_open_and_contained_partial_candle"})
            item.update(status="official_partial_candle_resolution", utc=key.isoformat(), shift_hours=offset)
    # A few partial rows cannot certify an otherwise wholly unknown file.
    if resolved and evidence["counts"].get("unique", 0) < 3:
        raise RepairConflict("Insufficient exact OHLCV corroboration for partial candle resolution")
    # For native daily bars the official opening grid contains only one of
    # 00:00 / 08:00. A conflicting duplicate is resolved only when another
    # original row independently matches that same official key in full.
    if path.stem == "1d":
        proven_keys = {pd.Timestamp(x["utc"]) for x in rows if x["status"] == "unique"}
        for item in rows:
            if item["status"] != "unmatched":
                continue
            ts = source.index[item["position"]]
            keys = [(off, ts + pd.Timedelta(hours=off)) for off in (0, -8)
                    if ts + pd.Timedelta(hours=off) in ref.index]
            if len(keys) == 1 and keys[0][1] in proven_keys:
                offset, key = keys[0]
                resolved.append({"position": item["position"], "utc": key.isoformat(),
                    "original_ohlcv": source.iloc[item["position"]][OHLCV].tolist(),
                    "official_ohlcv": ref.loc[key, OHLCV].tolist(),
                    "reason": "unique_official_daily_open_key_with_independent_exact_duplicate"})
                item.update(status="official_duplicate_conflict_resolution", utc=key.isoformat(), shift_hours=offset)
    # If a verified shift would collide with an unknown original row, defer
    # that shift (and any resulting chain) instead of deleting the unknown row.
    # Every deferred row retains its original timestamp AND original values.
    preserved = {x["position"] for x in rows if x["utc"] is None}
    while True:
        groups = defaultdict(list)
        for item in rows:
            key = item["stored"] if item["position"] in preserved else item["utc"]
            groups[key].append(item["position"])
        defer = {pos for positions in groups.values() if len(positions) > 1
                 and any(i in preserved for i in positions) for pos in positions if pos not in preserved}
        if not defer:
            break
        for pos in defer:
            item = rows[pos]
            item["proven_utc_deferred"] = item["utc"]
            item.update(status="verified_shift_deferred_unknown_collision", utc=None, shift_hours=None)
        preserved.update(defer)
    resolved = [x for x in resolved if x["position"] not in preserved]
    output = source.copy()
    resolved_by_position = {x["position"]: x for x in resolved}
    unknown = []
    for item in rows:
        if item["utc"] is None:
            unknown.append(item["position"])
            item["preserved_unchanged"] = True
            item["utc"] = item["stored"]  # stored wall clock retained, not certified UTC
    output.index = pd.DatetimeIndex([x["utc"] for x in rows], name="timestamp")
    for pos, item in resolved_by_position.items():
        output.iloc[pos] = item["official_ohlcv"]
    groups = defaultdict(list)
    for pos, key in enumerate(output.index):
        groups[key].append(pos)
    remove, coalesced = [], []
    unknown_set = set(unknown)
    for key, positions in groups.items():
        if len(positions) < 2:
            continue
        if any(pos in unknown_set for pos in positions):
            raise RepairConflict(f"Unknown row collision at {key}; entire file preserved")
        values = output.iloc[positions][OHLCV].to_numpy(dtype=float)
        if not np.all(values == values[0]):
            # Only already uniquely identified reference keys can get here.
            # Record every conflicting source value before official resolution.
            if key not in ref.index:
                raise RepairConflict("No official value for canonical conflict")
            coalesced.append({"utc": key.isoformat(), "positions": positions,
                              "kind": "official_value_conflict_resolution",
                              "original_ohlcv": values.tolist(), "official_ohlcv": ref.loc[key, OHLCV].tolist()})
            output.iloc[positions[0]] = ref.loc[key, OHLCV].to_numpy()
        else:
            coalesced.append({"utc": key.isoformat(), "positions": positions, "kind": "identical_overlap"})
        remove.extend(positions[1:])
    keep = np.ones(len(output), dtype=bool)
    keep[remove] = False
    output = output.iloc[keep].sort_index()
    # Prove preservation before any file transaction. Unknown rows cannot collide.
    if unknown:
        unchanged = source.iloc[unknown]
        actual = output.loc[unchanged.index]
        if not np.array_equal(unchanged.index.as_unit("ns").asi8, actual.index.as_unit("ns").asi8):
            raise RepairConflict("Unknown timestamps changed")
        pd.testing.assert_frame_equal(unchanged.reset_index(drop=True), actual.reset_index(drop=True))
    evidence.update(method="all_rows_exact_or_recorded_partial_candle_with_unknown_preservation",
        provenance=provenance, unknown_positions=unknown, official_resolutions=resolved,
        final_row_status_counts=dict(Counter(x["status"] for x in rows)),
        shifted_rows=sum(x["shift_hours"] == -8 for x in rows),
        unknown_rows=len(unknown), utc_certified=not unknown)
    if source.attrs.get("cts_timestamp_timezone") == "UTC" and evidence["shifted_rows"]:
        raise RepairConflict("UTC metadata contradicts official witnesses")
    plan = plan_explicit(path, output, evidence=evidence, expected_hash=pre,
        rows_before=len(source), coalesced=coalesced, utc_certified=not unknown)
    if original.equals(plan["result"]) and original.attrs == plan["result"].attrs:
        plan["status"] = "already_utc" if not unknown else "already_partial"
    return plan


def build_partition_plans(path, reference, provenance):
    """Rebucket the audited UTC day plus previous day, with explicit overlaps."""
    path = Path(path).resolve()
    previous = path.with_name((pd.Timestamp(path.stem) - pd.Timedelta(days=1)).strftime("%Y-%m-%d") + ".parquet")
    paths = [previous, path]
    hashes = {p: sha256(p) for p in paths}
    frames = [utc_frame(pd.read_parquet(p)) for p in paths]
    if any(set(frame.columns) != set(OHLCV) for frame in frames):
        raise RepairConflict("Unknown partition columns require independent preservation review")
    if any(sha256(p) != hashes[p] for p in paths):
        raise RepairConflict("Partition changed during read")
    combined = pd.concat(frames)
    combined.attrs = {}
    origins = [{"path": str(p), "position": i, "timestamp": ts.isoformat()}
               for p, f in zip(paths, frames) for i, ts in enumerate(f.index)]
    overlaps = []
    duplicates = combined.index[combined.index.duplicated(keep=False)].unique()
    for key in duplicates:
        positions = np.flatnonzero(combined.index == key).tolist()
        values = combined.iloc[positions][OHLCV].to_numpy(dtype=float)
        if not np.all(values == values[0]):
            raise RepairConflict(f"Cross-partition price conflict at {key}")
        overlaps.append({"utc": key.isoformat(), "origins": [origins[i] for i in positions],
                         "kind": "identical_overlap"})
    canonical = combined.loc[~combined.index.duplicated(keep="first")].sort_index()
    interval = path.parent.name.removesuffix("_parts")
    if interval == "1h":
        probe = canonical
    else:
        minutes = {"1m": 1, "5m": 5, "15m": 15}[interval]
        for key, group in canonical.groupby(canonical.index.floor("h")):
            expected = pd.date_range(key, periods=60 // minutes, freq=f"{minutes}min")
            if not group.index.equals(expected.rename(group.index.name)):
                raise RepairConflict("Incomplete hourly window; no whole-partition inference")
        probe = canonical.resample("h").agg(open=("open", "first"), high=("high", "max"),
            low=("low", "min"), close=("close", "last"), volume=("volume", "sum"))
    witness = verify_candidates(probe, reference)
    if witness["classification"] != "UTC":
        raise RepairConflict(f"Expected audited UTC partitions; got {witness['classification']}")
    allowed = {p.stem for p in paths}
    if not set(canonical.index.strftime("%Y-%m-%d")).issubset(allowed):
        raise RepairConflict("Additional neighbor needed; no rows may escape this plan")
    evidence = {"method": "complete_hourly_aggregation_vs_official_UTC_then_day_rebucketing",
        "provenance": provenance, "witness": witness, "original_rows": len(combined),
        "canonical_rows": len(canonical), "identical_overlaps": overlaps,
        "row_accounting": len(combined) - len(canonical)}
    plans = []
    for p, frame in zip(paths, frames):
        output = canonical.loc[canonical.index.strftime("%Y-%m-%d") == p.stem].copy()
        plans.append(plan_explicit(p, output, evidence=evidence, expected_hash=hashes[p],
                                  rows_before=len(frame), coalesced=overlaps))
    return plans


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--partitions", action="store_true")
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    audit = json.loads((AUDIT / "source_audit.json").read_text(encoding="utf-8"))
    targets = (audit["mispartitioned_files"] if args.partitions else
               [x for x in audit["cohort_monolith_candidates"] if x["kind"] in ("1h", "4h", "1d")])
    # Include the separately audited unnamed-index 1h cohort via finished refs.
    if not args.partitions:
        existing = {x["path"].replace("\\", "/") for x in targets}
        for meta_path in REFS.glob("*/1h.meta.json"):
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            source = meta["source_path"].replace("\\", "/")
            if source not in existing:
                targets.append({"path": source, "symbol": Path(source).parent.name, "kind": "1h"})
                existing.add(source)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "apply": args.apply, "files": []}
    for i, item in enumerate(targets):
        if args.symbols and item["symbol"] not in args.symbols:
            continue
        path = ROOT / item["path"]
        entry = {"path": str(path), "symbol": item["symbol"], "interval": item["kind"]}
        run = None
        try:
            reference, provenance = load_reference(item["symbol"], "1h" if args.partitions else item["kind"])
            plans = (build_partition_plans(path, reference, provenance) if args.partitions else
                     [build_monolith_plan(path, reference, provenance)])
            entry.update(status=plans[0]["status"], rows_before=sum(p["rows_before"] for p in plans),
                rows_after=sum(p["rows_after"] for p in plans),
                shifted_rows=sum(p["evidence"].get("shifted_rows", 0) for p in plans),
                unknown_rows=sum(p["evidence"].get("unknown_rows", 0) for p in plans),
                utc_certified=all(p.get("utc_certified", True) for p in plans))
            if args.apply and all(p["status"] == "ready" for p in plans):
                run = args.out / f"{i:04d}_{item['symbol']}_{item['kind']}"
                apply_plan(plans, run)
                entry.update(status="applied", run_dir=str(run))
        except Exception as exc:
            entry.update(status="unresolved_unchanged", error=f"{type(exc).__name__}: {exc}")
            if run is not None and (run / "manifest.json").exists():
                journal = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
                entry.update(status=journal["status"], run_dir=str(run))
        report["files"].append(entry)
        report["counts"] = dict(Counter(x["status"] for x in report["files"]))
        (args.out / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(json.dumps(entry, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
