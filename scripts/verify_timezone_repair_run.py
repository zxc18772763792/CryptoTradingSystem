"""Read-only, row-level acceptance checks for journaled historical repairs.

Only the JSON/Markdown reports are written. Source market data, references,
backups, transaction manifests, services and research ledgers are never changed.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from core.data import parquet_timezone_repair as repair
from scripts import repair_verified_history_20261021 as driver

AUDIT = ROOT / "data/research/timezone_repair_20261020"
OHLCV = repair.OHLCV
DEFAULT_GROUPS = ("applied_native_core_*", "applied_native_cohort_*", "applied_native_remaining_*",
    "applied_native_final_*", "applied_parts_pairs_*", "applied_partition_series_*")


def frame(path):
    value = pd.read_parquet(path)
    if not isinstance(value.index, pd.DatetimeIndex) or value.index.hasnans:
        raise AssertionError("DatetimeIndex required without NaT")
    if value.index.tz is not None:
        value.index = value.index.tz_convert("UTC").tz_localize(None)
    return value


def equal_values(actual, expected):
    return np.array_equal(np.asarray(actual, dtype=float), np.asarray(expected, dtype=float))


def reference_equal(actual, expected):
    a, b = np.asarray(actual, dtype=float), np.asarray(expected, dtype=float)
    tolerance = np.isclose(a, b, rtol=1e-8, atol=1e-10)
    tolerance[..., 4] = np.isclose(a[..., 4], b[..., 4], rtol=1e-7, atol=1e-10)
    return bool(tolerance.all())


def gap_stats(value, interval):
    index = value.index
    unique = index.unique().sort_values()
    step = pd.Timedelta(interval)
    differences = unique.to_series().diff().dropna()
    gaps = differences[differences > step]
    return {"rows": len(index), "duplicate_rows": int(index.duplicated().sum()),
        "sorted": bool(index.is_monotonic_increasing), "min": str(unique.min()), "max": str(unique.max()),
        "gap_spans": len(gaps), "missing_grid_bars": int(((gaps // step) - 1).sum()),
        "off_grid_delta_spans": int((differences % step != pd.Timedelta(0)).sum()),
        "max_gap": str(gaps.max()) if len(gaps) else None}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def load_reference(evidence, cache):
    provenance = evidence["provenance"]
    path = Path(provenance["reference"])
    if path not in cache:
        meta_path = Path(provenance["reference_metadata"])
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        current_hash = repair.sha256(path)
        require(metadata.get("complete_fetch") and metadata.get("market") == "spot"
            and metadata.get("timeZone") == "0", "Reference is not complete official UTC spot")
        require(metadata.get("parquet_sha256") == current_hash, "Current reference metadata/hash mismatch")
        reference = frame(path)
        require(reference.index.is_unique, "Reference timestamp duplicate")
        require(repair.sha256(path) == current_hash, "Reference changed while validating")
        cache[path] = (reference, current_hash, metadata)
    reference, current_hash, metadata = cache[path]
    return reference, {"path": str(path), "current_sha256": current_hash,
        "recorded_sha256": provenance["reference_sha256"],
        "changed_since_commit": current_hash != provenance["reference_sha256"],
        "market": metadata["market"], "interval": metadata["interval"]}


def verify_metadata(value, item):
    require(value.index.is_unique and not value.index.hasnans, "Output index is not unique/contains NaT")
    require(value.index.is_monotonic_increasing, "Output index is not sorted")
    if item.get("utc_certified", True):
        require(all(value.attrs.get(key) == expected for key, expected in repair.UTC_ATTRS.items()),
                "Certified output missing exact UTC schema/provenance markers")
    else:
        require("cts_timestamp_timezone" not in value.attrs and "cts_timestamp_schema" not in value.attrs,
                "Partial output incorrectly advertises certified UTC")
        require(value.attrs.get("cts_timestamp_status") == "unverified_segments_remain"
            and value.attrs.get("cts_timestamp_origin") == "verified_partial_timezone_repair",
            "Partial output missing unverified marker")
    path = Path(item["path"])
    if path.parent.name.endswith("_parts"):
        require(all(value.index.strftime("%Y-%m-%d") == path.stem), "Output row outside its UTC date partition")


def verify_monolith(before, after, evidence, item, cache, *, idempotence=True):
    rows = evidence["rows"]
    require(len(rows) == len(before), "Evidence row count does not equal original rows")
    require([row["position"] for row in rows] == list(range(len(before))), "Incomplete/duplicate position mapping")
    unknown = set(evidence.get("unknown_positions", []))
    require(len(unknown) == len(evidence.get("unknown_positions", [])), "Duplicate unknown position entries")
    require(len(unknown) == evidence.get("unknown_rows", len(unknown)), "Unknown position count mismatch")
    require(unknown == {row["position"] for row in rows if row["shift_hours"] is None},
            "Unknown position list disagrees with unresolved shifts")
    require(dict(Counter(row["status"] for row in rows)) == evidence["final_row_status_counts"],
            "Final row status counts disagree with original position map")
    require(item.get("utc_certified", True) == (not unknown), "Certification disagrees with unknown positions")
    reference, reference_info = load_reference(evidence, cache)
    expected = before[OHLCV].copy()
    expected.index = pd.DatetimeIndex([row["utc"] for row in rows], name="timestamp")
    before_values = before[OHLCV].to_numpy()
    require(pd.DatetimeIndex([row["stored"] for row in rows]).equals(before.index), "Stored time position mapping mismatch")
    known_positions = [pos for pos in range(len(rows)) if pos not in unknown]
    unknown_positions = sorted(unknown)
    shifts = [rows[pos]["shift_hours"] for pos in known_positions]
    require(all(shift in (0, -8) for shift in shifts), "Invalid clock shift")
    mapped = before.index[known_positions] + pd.to_timedelta(shifts, unit="h")
    require(expected.index[known_positions].equals(mapped), "Known timestamp transform mismatch")
    require(expected.index[unknown_positions].equals(before.index[unknown_positions]), "Unknown time changed")
    require(all(rows[pos].get("preserved_unchanged") is True and rows[pos]["shift_hours"] is None
        for pos in unknown_positions), "Unknown row was not explicitly preserved")
    for pos in unknown_positions:
        row = rows[pos]
        if row["status"] == "verified_shift_deferred_unknown_collision":
            delta = pd.Timestamp(row["proven_utc_deferred"]) - before.index[pos]
            require(delta in (pd.Timedelta(0), -pd.Timedelta(hours=8)), "Invalid deferred witness shift")
    exact_positions = [row["position"] for row in rows if row["status"] == "unique"]
    require(reference_equal(before_values[exact_positions],
        reference.reindex(expected.index[exact_positions])[OHLCV].to_numpy()),
        "Exact original witness no longer matches official reference")
    shifted = sum(shift == -8 for shift in shifts)
    require(shifted == evidence.get("shifted_rows", shifted), "Shift count mismatch")
    resolutions = evidence.get("official_resolutions", [])
    replaced = set()
    exact_keys = {pd.Timestamp(row["utc"]) for row in rows if row["status"] == "unique"}
    for change in resolutions:
        pos = change["position"]
        require(pos not in unknown and pos not in replaced, "Unknown or duplicate official replacement")
        replaced.add(pos)
        key = pd.Timestamp(change["utc"])
        require(key == expected.index[pos], "Replacement key does not match row mapping")
        require(equal_values(change["original_ohlcv"], before.iloc[pos][OHLCV]), "Original replacement values not recorded")
        require(key in reference.index and equal_values(change["official_ohlcv"], reference.loc[key, OHLCV]),
                "Replacement does not match full current official OHLCV")
        original, official = np.asarray(change["original_ohlcv"]), np.asarray(change["official_ohlcv"])
        if change["reason"] == "unique_open_and_contained_partial_candle":
            require(np.isclose(original[0], official[0], rtol=1e-10, atol=1e-10)
                and original[1] <= official[1] + 1e-8 and original[2] >= official[2] - 1e-8
                and original[2] <= min(original[0], original[3]) <= max(original[0], original[3]) <= original[1]
                and 0 <= original[4] <= official[4] + 1e-8, "Partial candle bounds not satisfied")
            require(evidence["counts"].get("unique", 0) >= 3, "Partial resolution lacks three exact originals")
        elif change["reason"] == "unique_official_daily_open_key_with_independent_exact_duplicate":
            require(Path(item["path"]).stem == "1d" and key in exact_keys,
                    "Daily conflict lacks an independent exact original witness")
        else:
            raise AssertionError("Unknown official replacement reason")
        expected.iloc[pos] = change["official_ohlcv"]
    special = {row["position"] for row in rows if row["status"].startswith("official_")}
    require(special == replaced, "Official status is missing its original/value replacement record")
    remove = set()
    for overlap in item.get("coalesced", []):
        positions = overlap["positions"]
        require(len(positions) > 1 and not (set(positions) & unknown), "Invalid/unknown coalescing")
        key = pd.Timestamp(overlap["utc"])
        require(all(expected.index[pos] == key for pos in positions), "Coalesced rows map to different keys")
        values = expected.iloc[positions][OHLCV].to_numpy()
        if overlap.get("kind", "identical_overlap") == "official_value_conflict_resolution":
            require(equal_values(values, overlap["original_ohlcv"]), "Conflict original values missing")
            require(equal_values(overlap["official_ohlcv"], reference.loc[key, OHLCV]), "Conflict official value mismatch")
            expected.iloc[positions[0]] = overlap["official_ohlcv"]
        else:
            require(all(equal_values(value, values[0]) for value in values), "Nonidentical overlap silently coalesced")
        require(not (set(positions[1:]) & remove), "Duplicate removed-position accounting")
        remove.update(positions[1:])
    keep = [pos for pos in range(len(expected)) if pos not in remove]
    expected = expected.iloc[keep].sort_index()
    require(len(before) - len(after) == len(remove), "Explicit coalesce accounting does not balance")
    pd.testing.assert_frame_equal(after[OHLCV], expected, check_freq=False, check_names=False)
    require(equal_values(before_values[unknown_positions],
        after.reindex(before.index[unknown_positions])[OHLCV].to_numpy()),
        "Unknown/deferred logical row changed or disappeared")
    interval = Path(item["path"]).stem
    known_keys = pd.DatetimeIndex([row["utc"] for row in rows if row["position"] not in unknown])
    if interval in ("1d", "4h"):
        step = pd.Timedelta(interval)
        require(all(known_keys.asi8 % step.value == 0), "Certified native candle is off its UTC opening grid")
    result = {"position_mapping_rows": len(rows), "unknown_preserved": len(unknown),
        "deferred_preserved": sum(row["status"] == "verified_shift_deferred_unknown_collision" for row in rows),
        "shifted_rows": int(shifted), "coalesced_rows": len(remove), "official_replacements": len(resolutions),
        "reference": reference_info, "gaps_before": gap_stats(before, interval), "gaps_after": gap_stats(after, interval)}
    if idempotence:
        fresh = driver.build_monolith_plan(item["path"], reference, evidence["provenance"])
        result["fresh_plan"] = {"status": fresh["status"],
            "shifted_rows": fresh["evidence"].get("shifted_rows", 0),
            "unknown_rows": fresh["evidence"].get("unknown_rows", 0),
            "reference_changed_since_commit": reference_info["changed_since_commit"]}
        if fresh["status"] == "ready":
            result["fresh_plan"]["needs_review"] = True
        else:
            require(fresh["status"] in ("already_utc", "already_partial"), "Unexpected fresh plan status")
            require(fresh["evidence"].get("shifted_rows", 0) == 0, "Already repaired file would shift again")
    return result


def verify_series(records, cache):
    sources = {record["item"]["path"]: record for record in records if record.get("before") is not None}
    pointers, output_count, certified_input, unknown_input = [], 0, 0, 0
    summary = records[0]["evidence"]["series_summary"]
    for record in records:
        evidence, before, after = record["evidence"], record.get("before"), record.get("after")
        if before is not None:
            rows = evidence["source_rows"]
            require([row["position"] for row in rows] == list(range(len(before))), "Series input position mapping incomplete")
            for row in rows:
                pos = row["position"]
                require(pd.Timestamp(row["stored"]) == before.index[pos], "Series stored time mismatch")
                shift = row["shift_hours"] if row["certified"] else 0
                require(shift in (0, -8) and pd.Timestamp(row["planned"]) == before.index[pos] + pd.Timedelta(hours=shift),
                        "Series clock transform or unknown preservation mismatch")
                require(Path(row["target_path"]).stem == row["planned"][:10], "Series target day mismatch")
                certified_input += row["certified"]
                unknown_input += not row["certified"]
            reference, _ = load_reference(evidence, cache)
            minutes = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}[evidence["interval"]]
            for hour in evidence["source_hour_witnesses"]:
                if hour["status"] != "unique":
                    continue
                bars = before.iloc[hour["positions"]][OHLCV].sort_index()
                stored_hour = pd.Timestamp(hour["stored_hour"])
                require(bars.index.equals(pd.date_range(stored_hour, periods=60 // minutes,
                    freq=f"{minutes}min").rename(bars.index.name)), "Certified hour is incomplete")
                values = [bars.open.iloc[0], bars.high.max(), bars.low.min(), bars.close.iloc[-1], bars.volume.sum()]
                key = stored_hour + pd.Timedelta(hours=hour["shift_hours"])
                require(key in reference.index and reference_equal(values, reference.loc[key, OHLCV]),
                        "Series hourly aggregation does not match official UTC reference")
        require(after is not None, "Cannot check series exact row lineage after unjournaled writer change")
        origins = evidence["output_row_origins"]
        require(len(origins) == len(after), "Output origin map does not cover every output row")
        output_count += len(after)
        unknown_output = 0
        for output in origins:
            key = pd.Timestamp(output["timestamp"])
            require(key in after.index and output["origins"], "Output pointer timestamp absent")
            all_certified = all(sources[origin["path"]]["evidence"]["source_rows"][origin["position"]]["certified"]
                for origin in output["origins"])
            unknown_output += not all_certified
            require(len(output["origins"]) == 1 or all_certified, "Unknown origins were coalesced")
            for origin in output["origins"]:
                source = sources[origin["path"]]
                pos = origin["position"]
                row = source["evidence"]["source_rows"][pos]
                require(row["planned"] == output["timestamp"], "Source/output canonical pointer mismatch")
                require(equal_values(after.loc[key, OHLCV], source["before"].iloc[pos][OHLCV]),
                        "Series original OHLCV changed or unknown row lost")
                pointers.append((origin["path"], pos))
        require(record["item"].get("utc_certified", True) == (unknown_output == 0),
                "Series output UTC certification disagrees with input origin witnesses")
        require(evidence["unknown_output_rows"] == unknown_output, "Series unknown output count mismatch")
    expected = {(path, pos) for path, record in sources.items() for pos in range(len(record["before"]))}
    require(len(pointers) == len(set(pointers)) and set(pointers) == expected, "Series position lineage omitted or duplicated")
    removed = sum(item.get("removed_rows", len(item.get("origins", [])) - 1)
        for record in records for item in record["item"].get("coalesced", []))
    require(len(expected) == output_count + removed, "Series explicit coalescing row accounting does not balance")
    require(summary["rows_before"] == len(expected) and summary["rows_after"] == output_count
        and summary["coalesced_rows"] == removed and summary["unknown_rows"] == unknown_input,
        "Series summary does not match independently counted rows")
    return {"input_positions": len(expected), "output_rows": output_count, "coalesced_rows": removed,
        "unknown_preserved": unknown_input, "certified_input_positions": certified_input}


def verify_pairs(records, cache):
    """Independently reconstruct legacy two-file UTC rebucketing from backups."""
    require(all(record.get("before") is not None and record.get("after") is not None for record in records),
            "Pair reconstruction requires every original and committed target")
    before = pd.concat([record["before"][OHLCV] for record in records])
    after = pd.concat([record["after"][OHLCV] for record in records]).sort_index()
    require(after.index.is_unique, "Duplicate across committed pair outputs")
    overlaps = records[0]["evidence"]["identical_overlaps"]
    documented = {pd.Timestamp(row["utc"]): row for row in overlaps}
    actual = set(before.index[before.index.duplicated(keep=False)])
    require(set(documented) == actual, "Pair overlap evidence omits/adds duplicate keys")
    for key, overlap in documented.items():
        group = before.loc[[key], OHLCV].to_numpy()
        require(all(equal_values(value, group[0]) for value in group), "Conflicting pair overlap")
        expected_origins = {(record["item"]["path"], int(pos)) for record in records
            for pos in np.flatnonzero(record["before"].index == key)}
        require({(origin["path"], origin["position"]) for origin in overlap["origins"]} == expected_origins,
                "Pair overlap original position mapping mismatch")
    canonical = before.loc[~before.index.duplicated(keep="first")].sort_index()
    pd.testing.assert_frame_equal(after, canonical, check_freq=False, check_names=False)
    # Vectorized reindex proves every original position, including coalesced origins.
    require(equal_values(after.reindex(before.index)[OHLCV].to_numpy(), before[OHLCV].to_numpy()),
            "Pair original position values or timestamps were lost")
    removed = len(before) - len(after)
    require(sum(len(row["origins"]) - 1 for row in overlaps) == removed, "Pair coalesce count mismatch")
    for record in records:
        evidence = record["evidence"]
        require(evidence["original_rows"] == len(before) and evidence["canonical_rows"] == len(after)
            and evidence["row_accounting"] == removed, "Pair summary row counts mismatch")
        require(record["item"].get("utc_certified", True), "UTC pair should be certified only after full reference proof")
    interval = Path(records[0]["item"]["path"]).parent.name.removesuffix("_parts")
    reference, _ = load_reference(records[0]["evidence"], cache)
    if interval == "1h":
        probe = canonical
    else:
        minutes = {"1m": 1, "5m": 5, "15m": 15}[interval]
        for key, bars in canonical.groupby(canonical.index.floor("h")):
            require(bars.index.equals(pd.date_range(key, periods=60 // minutes, freq=f"{minutes}min").rename(bars.index.name)),
                    "Pair source has incomplete hourly window")
        probe = canonical.resample("h").agg(open=("open", "first"), high=("high", "max"),
            low=("low", "min"), close=("close", "last"), volume=("volume", "sum"))
    require(reference_equal(probe[OHLCV].to_numpy(), reference.reindex(probe.index)[OHLCV].to_numpy()),
            "Pair complete hourly bars do not match official UTC")
    return {"input_positions": len(before), "output_rows": len(after), "coalesced_rows": removed,
            "complete_hourly_witnesses": len(probe)}


def discover(audit, patterns):
    return sorted({path for pattern in patterns for root in audit.glob(pattern) if root.is_dir()
                   for path in root.rglob("manifest.json")})


def audit_saved_anchors(audit):
    """Reuse samples fixed before the repair; compare each market to itself."""
    validation = Path(audit) / "validation"
    saved = validation / "official_fixed_ohlcv.json"
    if not saved.exists():
        return {"status": "not_available", "reason": "No previously fixed official anchor snapshot in this audit"}
    snapshot = json.loads(saved.read_text(encoding="utf-8"))
    retry = json.loads((validation / "official_futures_daily_retry.json").read_text(encoding="utf-8"))
    baseline = json.loads((validation / "fixed_multi_interval_results.json").read_text(encoding="utf-8"))
    baseline_hashes = {(row["market"], row["interval"]): row.get("source_sha256") for row in baseline["results"]}
    cases = [retry if row["market"] == "usds_perpetual" and row["params"]["interval"] == "1d" else row
        for row in snapshot["responses"] if row["params"]["interval"] in ("4h", "1d")]
    results = []
    for case in cases:
        market, interval = case["market"], case["params"]["interval"]
        family = "binance" if market == "spot" else "binance_futures"
        path = ROOT / "data/historical" / family / "BTC_USDT" / f"{interval}.parquet"
        value = frame(path)
        current_hash = repair.sha256(path)
        probes = []
        for raw in case["result"]["structuredContent"]["result"]:
            key = pd.Timestamp(int(raw[0]), unit="ms")
            matched = key in value.index and reference_equal(value.loc[key, OHLCV], np.asarray(raw[1:6], dtype=float))
            probes.append({"official_utc_open": key.isoformat(), "same_market_full_ohlcv_match_at_utc_key": matched})
        results.append({"market": market, "interval": interval, "path": str(path), "current_sha256": current_hash,
            "pre_repair_audit_sha256": baseline_hashes.get((market, interval)),
            "unchanged_since_pre_repair_audit": current_hash == baseline_hashes.get((market, interval)), "probes": probes})
        require(repair.sha256(path) == current_hash, "Anchor source changed during read")
    aligned = {}
    for interval in ("4h", "1d"):
        per_market = [set(probe["official_utc_open"] for probe in row["probes"]
            if probe["same_market_full_ohlcv_match_at_utc_key"]) for row in results if row["interval"] == interval]
        aligned[interval] = sorted(set.intersection(*per_market)) if len(per_market) == 2 else []
    futures_unchanged = all(row["unchanged_since_pre_repair_audit"] for row in results if row["market"] == "usds_perpetual")
    all_match = all(probe["same_market_full_ohlcv_match_at_utc_key"] for row in results for probe in row["probes"])
    cross_plan = json.loads((validation / "cross_market_probe_plan.json").read_text(encoding="utf-8"))
    cross_verdict = json.loads((validation / "cross_market_verdict.json").read_text(encoding="utf-8"))
    contaminated_path = Path(cross_plan["source"])
    spot = frame(contaminated_path)
    preserved = []
    for probe in cross_plan["probes"]:
        key = pd.Timestamp(probe["stored_time"])
        preserved.append({"stored_time": probe["stored_time"], "source_position_in_flagged_era": probe["position_in_flagged_era"],
            "timestamp_and_original_ohlcv_preserved": key in spot.index and equal_values(
                spot.loc[key, OHLCV], [probe["ohlcv"][column] for column in OHLCV]),
            "official_perpetual_exact_candidates_utc_ms": [row["candidate_utc_ms"] for row in cross_verdict["results"]
                if row["stored_time"] == probe["stored_time"] and row["exact_ohlcv_match"]]})
    return {"status": "passed" if all_match and futures_unchanged
        and all(row["timestamp_and_original_ohlcv_preserved"] for row in preserved)
        and spot.attrs.get("cts_timestamp_timezone") != "UTC" else "review_required",
        "selection": snapshot["selection"], "cases": results, "shared_verified_utc_open_keys": aligned,
        "futures_files_unchanged": futures_unchanged,
        "btc_spot_4h_unverified_market_segment": {"flagged_rows": cross_plan["flagged_row_count"],
            "samples": preserved, "has_utc_marker": spot.attrs.get("cts_timestamp_timezone") == "UTC",
            "interpretation": "First and middle fixed samples match USD-M perpetual at as-is UTC only. "
                "This proves sampled cross-market contamination in the spot path, not spot alignment. "
                "The final sample matches neither tested full perpetual bar; all 500 flagged rows remain unverified. "
                "These rows must be excluded from certified spot/perpetual alignment."}}


def validate(audit=AUDIT, patterns=DEFAULT_GROUPS, *, idempotence=True, progress=None):
    manifests = discover(Path(audit), patterns)
    loaded = [(path, json.loads(path.read_text(encoding="utf-8"))) for path in manifests]
    successor_backups = {}
    for _, journal in loaded:
        for item in journal.get("files", []):
            if item.get("pre_hash") and item.get("backup"):
                successor_backups[(item["path"], item["pre_hash"])] = Path(item["backup"])
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "read_only": True,
        "groups": list(patterns), "runs": [], "files": [], "limitations": [
        "No market-data API requests; current hash-verified official spot reference caches are used.",
        "Unjournaled post-commit modifications are reported as changed, not assumed legitimate.",
        "Unknown market/provenance rows remain unverified even when logical preservation passes."]}
    cache = {}
    for manifest_path, journal in loaded:
        run = {"manifest": str(manifest_path), "status": journal["status"], "errors": []}
        report["runs"].append(run)
        if journal["status"] != "complete":
            run["errors"].append("Transaction has not completed")
        records = []
        for item in journal.get("files", []):
            result = {"path": item["path"], "manifest": str(manifest_path), "errors": [], "warnings": [],
                "utc_certified": item.get("utc_certified", True), "rows_before": item["rows_before"],
                "rows_after": item["rows_after"]}
            report["files"].append(result)
            try:
                require(item.get("applied") is True, "File swap is not journaled as applied")
                require(not Path(item["stage"]).exists(), "Committed staging file still exists")
                before = None
                if item["pre_hash"] is not None:
                    backup = Path(item["backup"])
                    require(repair.sha256(backup) == item["pre_hash"], "Backup SHA256 does not match original prehash")
                    before = frame(backup)
                    require(len(before) == item["rows_before"], "Backup rows_before mismatch")
                else:
                    require(item["backup"] is None and item["rows_before"] == 0, "New target incorrectly claims original rows")
                current_path = Path(item["path"])
                current_hash = repair.sha256(current_path) if current_path.exists() else None
                result.update(pre_hash_verified=True, expected_post_hash=item["post_hash"], current_hash=current_hash)
                post_path = current_path
                if current_hash == item["post_hash"]:
                    result["current_state"] = "committed_post_hash"
                elif (item["path"], item["post_hash"]) in successor_backups:
                    post_path = successor_backups[(item["path"], item["post_hash"])]
                    require(repair.sha256(post_path) == item["post_hash"], "Successor backup does not preserve prior committed output")
                    result["current_state"] = "superseded_by_later_journal"
                    result["validated_committed_copy"] = str(post_path)
                else:
                    result["current_state"] = "modified_after_commit_unverified"
                    result["warnings"].append("Current hash differs; a later writer requires independent validation")
                    post_path = None
                after = frame(post_path) if post_path is not None else None
                evidence_path = manifest_path.parent / item["evidence"]
                evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
                record = {"item": item, "before": before, "after": after, "evidence": evidence, "result": result}
                records.append(record)
                if after is not None:
                    require(len(after) == item["rows_after"], "Committed rows_after mismatch")
                    require(repair._frame_hash(after) == item["result_hash"], "Committed frame differs from sealed result hash")
                    verify_metadata(after, item)
                    result["metadata_and_unique_index_verified"] = True
                    interval = (current_path.parent.name.removesuffix("_parts")
                        if current_path.parent.name.endswith("_parts") else current_path.stem)
                    result["gaps_before"] = gap_stats(before if before is not None else after.iloc[:0], interval)
                    result["gaps_after"] = gap_stats(after, interval)
                if "rows" in evidence and after is not None:
                    details = verify_monolith(before, after, evidence, item, cache,
                        idempotence=idempotence and current_hash == item["post_hash"])
                    result.update(details)
                    if details.get("fresh_plan", {}).get("needs_review"):
                        result["warnings"].append("Fresh reference now yields another ready plan; review new evidence before any mutation")
                elif "source_rows" not in evidence and evidence.get("method") != "complete_hourly_aggregation_vs_official_UTC_then_day_rebucketing":
                    result["warnings"].append("Evidence method is not row-lineage-monolith or series; specialized review required")
                if current_hash is not None:
                    require(repair.sha256(current_path) == current_hash, "Current file changed during validation")
            except Exception as exc:
                result["errors"].append(f"{type(exc).__name__}: {exc}")
        if records and all("source_rows" in record["evidence"] for record in records):
            try:
                run["series_row_validation"] = verify_series(records, cache)
            except Exception as exc:
                run["errors"].append(f"{type(exc).__name__}: {exc}")
        elif records and all(record["evidence"].get("method") == "complete_hourly_aggregation_vs_official_UTC_then_day_rebucketing" for record in records):
            try:
                run["pair_row_validation"] = verify_pairs(records, cache)
            except Exception as exc:
                run["errors"].append(f"{type(exc).__name__}: {exc}")
        if progress is not None:
            progress({"validated_run": str(manifest_path.parent), "files": len(records),
                "run_errors": len(run["errors"]),
                "file_errors": sum(bool(record["result"]["errors"]) for record in records)})
    report["summary"] = {"runs": len(report["runs"]), "files": len(report["files"]),
        "unique_paths": len({item["path"] for item in report["files"]}),
        "committed_outputs_certified_utc": sum(item["utc_certified"] for item in report["files"]),
        "committed_outputs_partial": sum(not item["utc_certified"] for item in report["files"]),
        "operation_rows_before": sum(item["rows_before"] for item in report["files"]),
        "operation_rows_after": sum(item["rows_after"] for item in report["files"]),
        "run_errors": sum(bool(run["errors"]) for run in report["runs"]),
        "file_errors": sum(bool(item["errors"]) for item in report["files"]),
        "files_with_warnings": sum(bool(item["warnings"]) for item in report["files"]),
        "current_states": dict(Counter(item.get("current_state", "validation_failed") for item in report["files"])),
        "unknown_preserved_monolith": sum(item.get("unknown_preserved", 0) for item in report["files"]),
        "deferred_preserved_monolith": sum(item.get("deferred_preserved", 0) for item in report["files"]),
        "shifted_rows_monolith": sum(item.get("shifted_rows", 0) for item in report["files"]),
        "official_replacements_monolith": sum(item.get("official_replacements", 0) for item in report["files"]),
        "coalesced_rows_monolith": sum(item.get("coalesced_rows", 0) for item in report["files"])}
    report["summary"]["verdict"] = ("failed" if report["summary"]["run_errors"] or report["summary"]["file_errors"]
        else "review_required" if report["summary"]["files_with_warnings"] else "passed")
    try:
        report["same_market_saved_anchor_alignment"] = audit_saved_anchors(audit)
        if report["same_market_saved_anchor_alignment"]["status"] == "review_required" and report["summary"]["verdict"] == "passed":
            report["summary"]["verdict"] = "review_required"
    except Exception as exc:
        report["same_market_saved_anchor_alignment"] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        report["summary"]["verdict"] = "failed"
    return report


def render_markdown(report):
    summary = report["summary"]
    lines = ["# Historical timezone repair independent acceptance", "", f"Generated: {report['generated_at']}", "",
        f"Verdict: **{summary['verdict']}**. {summary['runs']} runs, {summary['files']} files; "
        f"{summary['run_errors']} run errors, {summary['file_errors']} file errors, "
        f"{summary['files_with_warnings']} files requiring review.", "",
        f"Monolith preserved unknown positions: {summary['unknown_preserved_monolith']}; "
        f"including deferred: {summary['deferred_preserved_monolith']}. "
        f"Recorded official replacements: {summary['official_replacements_monolith']}; "
        f"explicit coalesced positions: {summary['coalesced_rows_monolith']}.", "",
        "| File | Current state | Unknown | Fresh plan | Issues |", "|---|---|---:|---|---|"]
    for item in report["files"]:
        issues = "; ".join(item["errors"] + item["warnings"]).replace("|", "/").replace("\n", " ")
        lines.append(f"| {item['path']} | {item.get('current_state', '')} | {item.get('unknown_preserved', '')} | "
            f"{item.get('fresh_plan', {}).get('status', '')} | {issues} |")
    lines += ["", "## Run errors", ""]
    lines += [f"- {run['manifest']}: {error}" for run in report["runs"] for error in run["errors"]] or ["None."]
    lines += ["", "## Scope and limits", ""] + [f"- {item}" for item in report["limitations"]]
    anchors = report.get("same_market_saved_anchor_alignment", {})
    lines += ["", "## Spot/perpetual alignment from preselected saved official samples", "",
        f"Status: {anchors.get('status')}. Futures files unchanged: {anchors.get('futures_files_unchanged', 'unavailable')}.", ""]
    for interval, keys in anchors.get("shared_verified_utc_open_keys", {}).items():
        lines.append(f"- BTC {interval}: {len(keys)} shared UTC opening keys, with full OHLCV matched to each market's own official source: {', '.join(keys)}.")
    contamination = anchors.get("btc_spot_4h_unverified_market_segment", {})
    if contamination:
        lines += ["", contamination["interpretation"]]
    if anchors.get("error"):
        lines += ["", anchors["error"]]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", type=Path, default=AUDIT)
    parser.add_argument("--group", action="append", help="Applied run directory glob; repeat as needed")
    parser.add_argument("--out", type=Path, default=AUDIT / "validation/post_repair_validation.json")
    parser.add_argument("--skip-idempotence", action="store_true")
    args = parser.parse_args()
    report = validate(args.audit, args.group or DEFAULT_GROUPS, idempotence=not args.skip_idempotence,
        progress=lambda item: print(json.dumps(item, ensure_ascii=False), flush=True))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    args.out.with_suffix(".md").write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps(report["summary"], ensure_ascii=False))
    return 1 if report["summary"]["verdict"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
