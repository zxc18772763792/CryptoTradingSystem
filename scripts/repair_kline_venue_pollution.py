"""Replace the perp bars stored in the spot kline store with Binance spot archive bars, day by day.

Background and method: docs/KLINE_VENUE_POLLUTION_2026-10-27.md and core/data/kline_venue_repair.py. Candidate
series-days come from scripts/scan_kline_venue_pollution.py --confirm. Archives are downloaded directly from
data.binance.vision (public, no proxy traffic).

    python scripts/repair_kline_venue_pollution.py plan                      # dry run: writes a plan directory only
    python scripts/repair_kline_venue_pollution.py plan --only BTC --timeframes 15m
    python scripts/repair_kline_venue_pollution.py apply --plan <plan dir>   # preflight: what would be written now
    python scripts/repair_kline_venue_pollution.py apply --plan <plan dir> --apply
    python scripts/repair_kline_venue_pollution.py verify --plan <plan dir>  # reader view now vs the plan's "after"
    python scripts/repair_kline_venue_pollution.py rollback --run <run dir>

A plan directory holds:

* ``changes.parquet``: every row to replace (series, file, timestamp, class, before OHLCV, after OHLCV);
* ``files.csv``: per file, sha256 now and the exact sha256 after the repair, frame hashes, class counts;
* ``days.csv``: per series-day, the reader's row count and SHA-1 before and after, class counts before and after,
  and whether the reader's bars after equal the spot archive;
* ``unresolved.parquet``: rows left as they are (class neither / perp_forming / spot_forming / no_spot_bar / ...),
  with the spot archive values beside them, and every row of a blocked file-day (``blocked``);
* ``blocked_days.csv``: file-days left alone because most rows match neither archive at their stored time, with
  the share explained by spot or perp bars 0 or 8 hours away (the local-clock era);
* ``archives.parquet``: the spot and perp archive bars used (perp prices divided by the contract multiplier);
* ``series.json``, ``summary.json`` (with the sha256 of changes.parquet, which apply checks).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from core.data.kline_venue_repair import (  # noqa: E402
    CLASSES,
    OHLCV,
    REPLACED,
    SeriesTarget,
    apply_series,
    diagnose_blocked_day,
    fetch_archives,
    load_targets,
    plan_series,
    prepare_series,
    rollback_series,
    utc_stamp,
    verify_series,
)
from core.data.parquet_timezone_repair import StalePlan  # noqa: E402
import certify_binance_store_from_archive as certify  # noqa: E402 - shared archive client and parser

STORE = PROJECT_ROOT / "data" / "historical"
SCAN_DIR = PROJECT_ROOT / "data" / "research" / "kline_venue_pollution_20261027"
OUT_DIR = PROJECT_ROOT / "data" / "research" / "kline_venue_repair_20261027"


def fetch_archive_day(market: str, raw_symbol: str, timeframe: str, day: str) -> Optional[pd.DataFrame]:
    root = "spot" if market == "spot" else "futures/um"
    payload = certify.fetch_zip(f"{root}/daily/klines/{raw_symbol}/{timeframe}/{raw_symbol}-{timeframe}-{day}.zip")
    return None if payload is None else certify.parse_archive_csv(payload)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _latest_days_file() -> Path:
    files = sorted(SCAN_DIR.glob("polluted_days_*.jsonl"))
    if not files:
        raise SystemExit(f"no polluted_days_*.jsonl in {SCAN_DIR}; run scripts/scan_kline_venue_pollution.py --confirm")
    return files[-1]


def cmd_plan(args: argparse.Namespace) -> int:
    days_file = args.days_file or _latest_days_file()
    targets = load_targets(days_file, only=args.only, timeframes=args.timeframes)
    out = args.out_dir / f"plan_{utc_stamp()}"
    out.mkdir(parents=True, exist_ok=False)
    print(f"{len(targets)} series, {sum(len(t.days) for t in targets)} series-days from {days_file.name}; "
          f"plan -> {out}", flush=True)
    changes, unresolved, archives, files, days, series = [], [], [], [], [], []
    began = time.time()
    # Downloads overlap; planning (which reads the store through the reader) runs one series at a time.
    with ThreadPoolExecutor(max_workers=max(1, args.series_workers)) as pool:
        futures = [pool.submit(fetch_archives, target, fetch_archive_day, workers=args.workers) for target in targets]
        for n, (target, future) in enumerate(zip(targets, futures), 1):
            try:
                plan = plan_series(args.root, target, future.result())
            except Exception as exc:  # noqa: BLE001 - one series' failure is reported, the rest continue
                series.append({"series": target.series, "status": "error", "reason": f"{type(exc).__name__}: {exc}"})
                print(f"  {n}/{len(targets)} {target.series}: error {exc}", flush=True)
                continue
            rows = 0 if plan.changes is None else len(plan.changes)
            series.append({"series": target.series, "status": plan.status, "reason": plan.reason,
                           "days": len(target.days), "perp_symbol": target.perp_symbol, "rows_to_replace": rows,
                           "files_to_rewrite": sum(1 for f in plan.files if f.get("rows_changed"))})
            files.extend(plan.files)
            days.extend(plan.days)
            for frame, sink in ((plan.changes, changes), (plan.unresolved, unresolved), (plan.archives, archives)):
                if frame is not None:
                    sink.append(frame)
            print(f"  {n}/{len(targets)} {target.series}: {plan.status} {rows} rows {plan.reason} "
                  f"({time.time() - began:.0f}s)", flush=True)

    def write(frames: List[pd.DataFrame], name: str) -> Optional[str]:
        if not frames:
            return None
        path = out / name
        pd.concat(frames, ignore_index=True).to_parquet(path, index=False)
        return _sha256(path)

    all_changes = pd.concat(changes, ignore_index=True) if changes else pd.DataFrame(columns=["class", "file"])
    all_unresolved = (pd.concat(unresolved, ignore_index=True) if unresolved
                      else pd.DataFrame(columns=["series", "file", "timestamp", "class", "blocked"]))
    by_series = {target.series: target for target in targets}
    blocked_rows = all_unresolved[all_unresolved["blocked"].astype(bool)]
    diagnosis = []
    for (name, day), rows in blocked_rows.groupby(["series", blocked_rows["timestamp"].dt.date.astype(str)]):
        diagnosis.extend(diagnose_blocked_day(by_series[name], day, dict(tuple(rows.groupby("file"))), fetch_archive_day))
    pd.DataFrame(diagnosis).to_csv(out / "blocked_days.csv", index=False)
    changes_sha = write(changes, "changes.parquet")
    write(unresolved, "unresolved.parquet")
    write(archives, "archives.parquet")
    pd.DataFrame(files).to_csv(out / "files.csv", index=False)
    pd.DataFrame(days).to_csv(out / "days.csv", index=False)
    (out / "series.json").write_text(json.dumps(series, ensure_ascii=False, indent=1), encoding="utf-8")
    day_frame = pd.DataFrame(days)
    checked = day_frame[day_frame["status"] == "checked"] if len(day_frame) else day_frame
    summary = {
        "generated_at": out.name.removeprefix("plan_"), "days_file": str(days_file), "storage_root": str(args.root),
        "series": len(targets), "series_status": dict(Counter(s["status"] for s in series)),
        "series_days": int(len(day_frame)), "series_days_checked": int(len(checked)),
        "series_days_skipped": dict(Counter(day_frame.loc[day_frame["status"] != "checked", "status"])) if len(day_frame) else {},
        "rows_to_replace": int(len(all_changes)), "rows_by_class": dict(Counter(all_changes["class"])),
        "files_to_rewrite": int(sum(1 for f in files if f.get("rows_changed"))),
        "files_by_kind": dict(Counter(f["kind"] for f in files if f.get("rows_changed"))),
        "rows_left_unresolved": dict(Counter(all_unresolved.loc[~all_unresolved["blocked"].astype(bool), "class"])),
        "blocked_file_days": len(diagnosis),
        "rows_on_blocked_file_days": dict(Counter(blocked_rows["class"])),
        "blocked_explained_by": dict(Counter(item["explained_by"] for item in diagnosis)),
        "reader_days_equal_spot_archive_after": int(checked["reader_after_equals_spot_archive"].sum()) if len(checked) else 0,
        "reader_replaceable_rows_before": int(sum(checked[f"before_{c}"].sum() for c in REPLACED)) if len(checked) else 0,
        "reader_replaceable_rows_after": int(sum(checked[f"after_{c}"].sum() for c in REPLACED)) if len(checked) else 0,
        "replaced_classes": list(REPLACED), "changes_sha256": changes_sha,
        "seconds": round(time.time() - began),
    }
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1, default=str), flush=True)
    return 1 if any(s["status"] in ("error", "refused") for s in series) else 0


def _load_plan(plan_dir: Path, args: argparse.Namespace) -> pd.DataFrame:
    summary = json.loads((plan_dir / "summary.json").read_text(encoding="utf-8"))
    path = plan_dir / "changes.parquet"
    if summary.get("changes_sha256") != _sha256(path):
        raise SystemExit(f"{path} differs from the reviewed plan (sha256 in summary.json); re-plan")
    changes = pd.read_parquet(path)
    archives = pd.read_parquet(plan_dir / "archives.parquet")
    spot = archives[archives["market"] == "spot"].set_index(["series", "timestamp"])[OHLCV]
    after = changes.set_index(["series", "timestamp"])[[f"after_{n}" for n in OHLCV]]
    if not (spot.reindex(after.index).to_numpy() == after.to_numpy()).all():
        raise SystemExit("changes.parquet: some 'after' values are not the spot archive bars; re-plan")
    keep = pd.Series(True, index=changes.index)
    if args.only:
        keep &= changes["series"].str.split("_").str[0].str.upper().isin({s.upper() for s in args.only})
    if args.timeframes:
        keep &= changes["series"].str.split("/").str[1].isin(set(args.timeframes))
    if args.series:
        keep &= changes["series"].isin(set(args.series))
    return changes[keep]


def cmd_apply(args: argparse.Namespace) -> int:
    changes = _load_plan(args.plan, args)
    groups = list(changes.groupby("series", sort=True))
    print(f"{len(groups)} series, {len(changes)} planned rows; {'WRITING' if args.apply else 'preflight (no writes)'}",
          flush=True)
    run = args.plan / f"run_{utc_stamp()}" if args.apply else None
    results, counts = [], Counter()
    for n, (series, rows) in enumerate(groups, 1):
        symbol_dir, timeframe = series.split("/")
        if args.apply:
            result = apply_series(args.root, symbol_dir, timeframe, rows, run / f"{symbol_dir}__{timeframe}")
        else:
            try:
                plans = prepare_series(args.root, symbol_dir, timeframe, rows)
                result = {"series": series, "status": "would_write" if plans else "already_applied",
                          "files": [{"file": Path(p["path"]).name, "pre_sha256": p["pre_hash"],
                                     "rows_replaced": p["rows_replaced"],
                                     "rows_already_repaired": p["rows_already_repaired"]} for p in plans]}
            except StalePlan as exc:
                result = {"series": series, "status": "stale", "reason": str(exc)}
        counts[result["status"]] += 1
        results.append(result)
        print(f"  {n}/{len(groups)} {series}: {result['status']} {result.get('reason', '')}", flush=True)
    target = run if run is not None else args.plan
    target.mkdir(parents=True, exist_ok=True)
    name = "results.jsonl" if run is not None else f"preflight_{utc_stamp()}.jsonl"
    with (target / name).open("w", encoding="utf-8") as sink:
        for result in results:
            sink.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")
    print(json.dumps({"counts": dict(counts), "written_to": str(target / name)}, ensure_ascii=False), flush=True)
    return 0 if set(counts) <= {"complete", "already_applied", "would_write"} else 1


def cmd_verify(args: argparse.Namespace) -> int:
    days = pd.read_csv(args.plan / "days.csv")
    days = days[days["status"] == "checked"]
    archives = pd.read_parquet(args.plan / "archives.parquet")
    rows = []
    for series, planned in days.groupby("series", sort=True):
        symbol_dir, timeframe = series.split("/")
        rows.extend(verify_series(args.root, symbol_dir, timeframe, planned, archives[archives["series"] == series]))
    frame = pd.DataFrame(rows).merge(days[["series", "day", "files_blocked"]], on=["series", "day"])
    path = args.plan / f"verify_{utc_stamp()}.csv"
    frame.to_csv(path, index=False)
    repaired = frame[frame["files_blocked"] == 0]  # blocked file-days keep their rows by design
    report = {"days": int(len(frame)), "match_planned_after": int(frame["matches_planned_after"].sum()),
              "days_with_replaceable_rows_left": int((repaired["replaceable_rows_left"] > 0).sum()),
              "replaceable_rows_left": int(repaired["replaceable_rows_left"].sum()),
              "replaceable_rows_on_blocked_days": int(frame.loc[frame["files_blocked"] > 0, "replaceable_rows_left"].sum()),
              "written_to": str(path)}
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return 0 if report["days_with_replaceable_rows_left"] == 0 else 1


def cmd_rollback(args: argparse.Namespace) -> int:
    dirs = sorted(p for p in args.run.iterdir() if (p / "manifest.json").exists())
    if args.series:
        wanted = {s.replace("/", "__") for s in args.series}
        dirs = [p for p in dirs if p.name.split("_retry")[0] in wanted]
    counts = Counter()
    for path in dirs:
        try:
            journal = rollback_series(path)
            counts[journal["status"]] += 1
            print(f"  {path.name}: {journal['status']}", flush=True)
        except Exception as exc:  # noqa: BLE001 - reported per series; the others still roll back
            counts["refused"] += 1
            print(f"  {path.name}: refused {exc}", flush=True)
    print(json.dumps(dict(counts)), flush=True)
    return 0 if set(counts) <= {"rolled_back"} else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", type=Path, default=STORE, help="storage root (default data/historical)")
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan", help="dry run: write a plan directory, change nothing")
    plan.add_argument("--days-file", type=Path, help="polluted_days_*.jsonl (default: the newest scan)")
    plan.add_argument("--only", nargs="*", help="base assets, e.g. BTC ETH")
    plan.add_argument("--timeframes", nargs="*")
    plan.add_argument("--workers", type=int, default=4, help="archive downloads per series")
    plan.add_argument("--series-workers", type=int, default=3, help="series downloading at once")
    plan.add_argument("--out-dir", type=Path, default=OUT_DIR)
    apply = sub.add_parser("apply", help="preflight a reviewed plan; with --apply, write it")
    apply.add_argument("--plan", type=Path, required=True)
    apply.add_argument("--apply", action="store_true", help="write (default: preflight only)")
    apply.add_argument("--only", nargs="*")
    apply.add_argument("--timeframes", nargs="*")
    apply.add_argument("--series", nargs="*", help="e.g. BTC_USDT/15m")
    verify = sub.add_parser("verify", help="compare the reader view now with the plan's fingerprints")
    verify.add_argument("--plan", type=Path, required=True)
    rollback = sub.add_parser("rollback", help="restore an applied run from its backups")
    rollback.add_argument("--run", type=Path, required=True)
    rollback.add_argument("--series", nargs="*")
    args = parser.parse_args(argv)
    return {"plan": cmd_plan, "apply": cmd_apply, "verify": cmd_verify, "rollback": cmd_rollback}[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
