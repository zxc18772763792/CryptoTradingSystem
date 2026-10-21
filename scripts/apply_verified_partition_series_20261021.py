"""Apply the bounded audited partition cohort through verified transactions."""
from __future__ import annotations
import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.data.parquet_timezone_repair import apply_plan
from scripts.repair_verified_history_20261021 import AUDIT, load_reference
from scripts.repair_verified_partition_series import build_series_plans


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--intervals", nargs="*", default=["1h", "1m", "5m", "15m"])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--through", default="2026-05-19")
    args = ap.parse_args()
    if args.out.exists():
        ap.error("Use a NEW output directory to preserve earlier reports and backups")
    args.out.mkdir(parents=True)
    audit = json.loads((AUDIT / "source_audit.json").read_text(encoding="utf-8"))
    targets = sorted({(x["symbol"], x["kind"]) for x in audit["mispartitioned_files"]},
                     key=lambda x: (x[0], args.intervals.index(x[1]) if x[1] in args.intervals else 100))
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "apply": args.apply,
              "through": args.through, "series": []}
    for i, (symbol, interval) in enumerate(targets):
        if interval not in args.intervals or (args.symbols and symbol not in args.symbols):
            continue
        entry = {"symbol": symbol, "interval": interval}
        run = args.out / f"{i:04d}_{symbol}_{interval}"
        try:
            reference, provenance = load_reference(symbol, "1h")
            plans = build_series_plans(ROOT / "data/historical/binance" / symbol / f"{interval}_parts",
                                       reference, provenance, through=args.through)
            entry.update(status="ready", **plans[0]["evidence"]["series_summary"])
            if not entry["shifted_rows"] and not entry["coalesced_rows"] and entry["unknown_rows"]:
                entry["status"] = "unchanged_no_proven_clock_change"
            elif args.apply:
                journal = apply_plan(plans, run)
                entry.update(status=journal["status"], run_dir=str(run), files=len(plans))
        except Exception as exc:
            entry.update(status="unresolved_unchanged", error=f"{type(exc).__name__}: {exc}")
            if (run / "manifest.json").exists():
                journal = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
                entry.update(status=journal["status"], run_dir=str(run))
        report["series"].append(entry)
        report["counts"] = dict(Counter(x["status"] for x in report["series"]))
        (args.out / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(json.dumps(entry, default=str), flush=True)
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    (args.out / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
