"""Repair one kline parquet using an independently recorded UTC reference.

Dry-run by default. No date heuristic, blanket shifts, or unbacked writes.
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from core.data.parquet_timezone_repair import apply_plan, plan_repair, rollback_run, sha256


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--path", type=Path, help="Explicit source kline parquet")
    ap.add_argument("--reference", type=Path, help="Official UTC OHLCV parquet, same market/interval")
    ap.add_argument("--run-dir", type=Path, help="NEW directory for backups and evidence")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--rollback", type=Path, help="Hash-guarded rollback of one run")
    args = ap.parse_args()
    if args.rollback:
        print(json.dumps(rollback_run(args.rollback), ensure_ascii=False, default=str))
        return 0
    if not args.path or not args.reference:
        ap.error("--path and --reference required; blanket/age-based migration is disabled")
    if args.path.stem in ("funding_rate", "premium_index_1d"):
        ap.error("Funding and premium data are not klines")
    if args.path.parent.name.endswith("_parts"):
        ap.error("Partitions require group rebucketing, not a single-file shift")
    metadata_path = args.reference.with_suffix(".meta.json")
    if not metadata_path.exists():
        ap.error("Reference requires its official-source .meta.json provenance and hash")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if (args.path.parent.parent.name != "binance" or metadata.get("market") != "spot"
            or metadata.get("symbol") != args.path.parent.name.replace("_", "")
            or metadata.get("interval") != args.path.stem or metadata.get("timeZone") != "0"
            or not metadata.get("complete_fetch") or sha256(args.reference) != metadata.get("parquet_sha256")):
        ap.error("Reference market/symbol/interval/hash must match; this CLI supports verified Binance spot only")
    plan = plan_repair(args.path, pd.read_parquet(args.reference))
    plan["evidence"]["provenance"] = {"reference": str(args.reference.resolve()),
        "reference_metadata": str(metadata_path.resolve()), "reference_sha256": metadata["parquet_sha256"]}
    print(json.dumps({k: v for k, v in plan.items() if k not in ("result", "evidence")}, default=str))
    print(json.dumps({k: v for k, v in plan["evidence"].items() if k != "rows"}, default=str))
    if args.apply:
        if not args.run_dir:
            ap.error("--apply requires --run-dir; backups cannot be disabled")
        if plan["status"] == "already_utc":
            return 0
        result = apply_plan(plan, args.run_dir)
        print(f"{result['status']}: {args.run_dir}")
    return 0 if plan["status"] != "unresolved" else 2


if __name__ == "__main__":
    raise SystemExit(main())
