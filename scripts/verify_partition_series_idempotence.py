"""Read-only replanning check for applied, bounded partition-series repairs."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.data.parquet_timezone_repair import OHLCV, RepairConflict, sha256  # noqa: E402
from scripts.repair_verified_partition_series import build_series_plans  # noqa: E402

AUDIT = ROOT / "data/research/timezone_repair_20261020"


def _normalized_unit(frame):
    out = frame.copy()
    if not isinstance(out.index, pd.DatetimeIndex) or out.index.hasnans:
        raise ValueError("A valid DatetimeIndex is required")
    out.index = out.index.as_unit("ns")
    return out


def verify_series(directory, reference, provenance, through="2026-05-18") -> dict:
    """Verify exact current-file equivalence without applying any plan.

    Timestamp units alone may differ; timezone, timestamps, order, OHLCV,
    column dtypes, index name, and attrs must agree. Hash every parquet in the
    directory before/after, including files outside the transaction cutoff.
    """
    directory = Path(directory).resolve()
    paths = sorted(directory.glob("*.parquet"))
    before = {path: sha256(path) for path in paths}
    report = {"directory": str(directory), "through": through, "passed": False,
              "read_only": True, "provenance": provenance, "files": [], "errors": []}
    try:
        plans = build_series_plans(directory, reference, provenance, through)
        summary = plans[0]["evidence"]["series_summary"]
        report["series_summary"] = summary
        if summary["shifted_rows"] != 0:
            report["errors"].append("Replanning still shifts rows")
        if summary["coalesced_rows"] != 0:
            report["errors"].append("Replanning still coalesces rows")
        for plan in plans:
            path = Path(plan["path"]).resolve()
            item = {"path": str(path), "plan_pre_hash": plan["pre_hash"],
                    "pre_hash": before.get(path), "rows_before": plan["rows_before"],
                    "rows_after": plan["rows_after"], "errors": []}
            if path not in before or plan["pre_hash"] is None:
                item["errors"].append("Replanning introduces an absent/new target")
            elif plan["pre_hash"] != before[path]:
                item["errors"].append("Plan prehash differs from validation snapshot")
            if not path.exists():
                item["errors"].append("Planned target does not exist")
            else:
                try:
                    current = pd.read_parquet(path)
                    proposed = plan["result"]
                    item.update(current_index_unit=str(current.index.dtype),
                                proposed_index_unit=str(proposed.index.dtype))
                    if set(current.columns) != set(OHLCV) or set(proposed.columns) != set(OHLCV):
                        item["errors"].append("Expected exactly OHLCV columns")
                    try:
                        pd.testing.assert_frame_equal(_normalized_unit(current), _normalized_unit(proposed),
                            check_exact=True, check_freq=False, check_dtype=True)
                        item["index_and_ohlcv_equal"] = True
                    except (AssertionError, ValueError) as exc:
                        item["index_and_ohlcv_equal"] = False
                        item["errors"].append(f"Index/OHLCV differ: {exc}")
                    item["attrs_equal"] = current.attrs == proposed.attrs
                    if not item["attrs_equal"]:
                        item["errors"].append("Attrs differ from current file")
                    item["post_hash"] = sha256(path)
                    if item["post_hash"] != before.get(path):
                        item["errors"].append("File hash changed during validation")
                except Exception as exc:
                    item["errors"].append(f"Read/compare failed: {type(exc).__name__}: {exc}")
            item["passed"] = not item["errors"]
            report["files"].append(item)
    except Exception as exc:
        report["errors"].append(f"Replanning failed: {type(exc).__name__}: {exc}")
    after_paths = set(directory.glob("*.parquet"))
    changed = [str(path) for path, digest in before.items()
               if not path.exists() or sha256(path) != digest]
    created = sorted(str(path) for path in after_paths - set(before))
    report.update(snapshot_files=len(before), files_changed_during_validation=changed,
                  files_created_during_validation=created,
                  checked_plan_files=len(report["files"]))
    if changed or created:
        report["errors"].append("Parquet snapshot changed during read-only validation")
    report["passed"] = (not report["errors"] and bool(report["files"])
                        and all(item["passed"] for item in report["files"]))
    return report


def _targets(report_paths, symbols, interval, through):
    entries, reports = {}, []
    for path in report_paths:
        path = Path(path).resolve()
        before = sha256(path)
        report = json.loads(path.read_text(encoding="utf-8"))
        if sha256(path) != before:
            raise RepairConflict(f"Apply report changed during read: {path}")
        if report.get("apply") is not True or report.get("through") != through:
            raise RepairConflict("Require applied-series reports with the same explicit cutoff")
        reports.append({"path": str(path), "sha256": before})
        for item in report["series"]:
            if item["status"] not in ("complete", "applied") or item["interval"] != interval:
                continue
            if symbols and item["symbol"] not in symbols:
                continue
            entries[(item["symbol"], item["interval"])] = item
    if not entries:
        raise RepairConflict("No completed applied series selected")
    if symbols and set(symbols) - {key[0] for key in entries}:
        raise RepairConflict("Requested symbols are missing from completed apply reports")
    return sorted(entries), reports


def _reference(symbol, reference_root):
    path = reference_root / symbol.replace("_", "") / "1h.parquet"
    metadata_path = path.with_suffix(".meta.json")
    before = sha256(path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if (not metadata.get("complete_fetch") or metadata.get("market") != "spot"
            or metadata.get("interval") != "1h" or metadata.get("timeZone") != "0"
            or metadata.get("symbol") != symbol.replace("_", "")
            or metadata.get("parquet_sha256") != before):
        raise RepairConflict(f"Invalid official UTC reference provenance: {symbol}")
    reference = pd.read_parquet(path)
    if sha256(path) != before:
        raise RepairConflict("Official reference changed during read")
    return reference, {"reference": str(path), "reference_sha256": before,
                       "reference_metadata": str(metadata_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply-report", type=Path, action="append", required=True)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--through", default="2026-05-18")
    parser.add_argument("--historical-root", type=Path, default=ROOT / "data/historical/binance")
    parser.add_argument("--reference-root", type=Path, default=AUDIT / "official_reference")
    parser.add_argument("--out", type=Path, default=AUDIT / "validation/partition_series_idempotence.json")
    args = parser.parse_args()
    targets, apply_reports = _targets(args.apply_report, args.symbols, args.interval, args.through)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "read_only": True,
              "through": args.through, "apply_reports": apply_reports, "series": []}
    for symbol, interval in targets:
        try:
            reference, provenance = _reference(symbol, args.reference_root)
            item = verify_series(args.historical_root / symbol / f"{interval}_parts",
                                 reference, provenance, args.through)
        except Exception as exc:
            item = {"passed": False, "errors": [f"{type(exc).__name__}: {exc}"]}
        item.update(symbol=symbol, interval=interval)
        report["series"].append(item)
        report.update(passed=all(row["passed"] for row in report["series"]),
            checked_series=len(report["series"]), expected_series=len(targets),
            checked_plan_files=sum(row.get("checked_plan_files", 0) for row in report["series"]),
            finished_at=datetime.now(timezone.utc).isoformat())
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({"symbol": symbol, "interval": interval, "passed": item["passed"],
            "files": item.get("checked_plan_files"), "errors": item.get("errors")} ), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
