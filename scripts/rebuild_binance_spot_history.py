"""Reconstruct an explicitly listed damaged Binance spot cohort with complete backups."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pandas as pd
from core.data.spot_history_rebuild import OfficialSpotSource, reconstruction_plans, series_files
from core.data.parquet_timezone_repair import apply_plan, sha256
from config.settings import settings
from core.utils.proxy_env import ensure_proxy_env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issues", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    ensure_proxy_env(settings)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    source = OfficialSpotSource(out / "source_cache")
    issues = json.loads(args.issues.read_text("utf-8-sig"))["issues"]
    targets = sorted({(r["symbol"], r["timeframe"]) for r in issues
                      if r.get("namespace") == "binance"
                      and (not args.only or r["symbol"].split("/")[0] in args.only)})
    storage = ROOT / "data/historical"

    def collect(target):
        symbol, tf = target
        files = series_files(storage, symbol, tf)
        folder = out / (symbol.replace("/", "_") + "_" + tf)
        folder.mkdir(exist_ok=True)
        range_path = folder / "original_range.json"
        starts = []
        for p in files:
            frame = pd.read_parquet(p)
            if len(frame):
                stamp = pd.Timestamp(frame.index.min())
                if stamp.tz is not None:
                    stamp = stamp.tz_convert("UTC").tz_localize(None)
                starts.append(stamp)
        if range_path.exists():
            start = pd.Timestamp(json.loads(range_path.read_text("utf-8"))["start_utc"])
        elif (folder / "source_evidence.json").exists():
            start = pd.Timestamp(json.loads((folder / "source_evidence.json").read_text("utf-8"))["requested_start_utc"])
        else:
            start = (min(starts) - pd.Timedelta(hours=8)).normalize()
        if not range_path.exists():
            range_path.write_text(json.dumps({"start_utc": start.isoformat()}), encoding="utf-8")
        reference, evidence = source.reference(symbol.replace("/", ""), tf, start)
        reference.to_parquet(folder / "reference.parquet")
        evidence["reference_sha256"] = sha256(folder / "reference.parquet")
        (folder / "source_evidence.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
        return files, reference, evidence, folder

    results = []
    with (out / "journal.jsonl").open("a", encoding="utf-8") as journal:
        with ThreadPoolExecutor(max_workers=max(1, min(args.workers, 6))) as pool:
            pending = {pool.submit(collect, target): target for target in targets}
            for future in as_completed(pending):
                symbol, tf = pending[future]
                result = {"symbol": symbol, "timeframe": tf, "at": datetime.now(timezone.utc).isoformat()}
                try:
                    files, reference, evidence, folder = future.result()
                    plans = reconstruction_plans(files, reference, tf, evidence)
                    result.update(files=len(plans), rows=len(reference), first=str(reference.index.min()), last=str(reference.index.max()))
                    if args.apply and plans:
                        run = folder / ("transaction_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f"))
                        transaction = apply_plan(plans, run)
                        result.update(status=transaction["status"], transaction=str(run / "manifest.json"))
                        # Verify every staged result, including UTC attributes, after swap.
                        for plan in plans:
                            check = pd.read_parquet(plan["path"])
                            pd.testing.assert_frame_equal(check, plan["result"], check_freq=False)
                            if check.attrs != plan["result"].attrs:
                                raise RuntimeError("Post-apply UTC provenance differs")
                    elif args.apply:
                        result["status"] = "complete"
                        result["already_matches_official_source"] = True
                    else:
                        result["status"] = "planned"
                except Exception as exc:
                    result.update(status="error", error_type=type(exc).__name__, reason=str(exc)[:240])
                results.append(result)
                line = json.dumps(result, ensure_ascii=False)
                journal.write(line + "\n")
                journal.flush()
                print(line, flush=True)
                summary = {"apply": args.apply, "targets": len(targets), "finished": len(results),
                           "completed": sum(r["status"] == "complete" for r in results),
                           "errors": sum(r["status"] == "error" for r in results), "results": results}
                (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return 1 if any(r["status"] == "error" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
