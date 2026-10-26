"""Fold the kline store's legacy daily part files into month/year files (one file per series per period).

The data volume is exFAT with 1 MiB clusters: on 2026-10-26 about 123,000 daily part and lock files held 0.7 GB
of bars in about 121 GB. Writers now append to ``<YYYY-MM>.parquet`` (minute bars) or ``<YYYY>.parquet`` (hour and
longer bars); see ``core/data/kline_partitions.py``. This command moves the existing closed days there.

Days with the verified UTC marker move into the period file. Days without any timestamp attributes move into the
period's unmarked legacy pack (the writer still guards each of those days separately); recent unmarked days stay
daily (50 bars, at least 14 days), because a writer process started before this layout cannot see a pack. Everything else
stays daily and is listed with the reason (core/data/kline_part_consolidation.py). Each output is written to a temp
file, read back and compared row for row with its sources before any daily file is deleted. The default is a dry run.

    python scripts/consolidate_kline_parts.py                  # dry run: what would move, what stays and why
    python scripts/consolidate_kline_parts.py --apply          # verified apply
    python scripts/consolidate_kline_parts.py --apply --only BTC --timeframes 1h
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.data.kline_part_consolidation import (  # noqa: E402
    colliding_daily_files,
    consolidate_period,
    daily_files_by_period,
    discard_orphan_day_locks,
    file_census,
    series_targets,
    to_json_line,
)

LIVE_REFRESH_REACH_BARS = 50


def default_legacy_before(timeframe: str, today: date) -> date:
    """Unmarked days stay daily within the reach of a live writer's recent-bar refresh (about 42 bars)."""
    bar = pd.Timedelta(timeframe.replace("m", "min")) if timeframe.endswith("m") else pd.Timedelta(timeframe)
    return today - max(timedelta(days=14), (bar * LIVE_REFRESH_REACH_BARS).to_pytimedelta())


def run_series(item: tuple, before: date, legacy_before: Optional[date], apply: bool) -> dict:
    exchange, symbol_dir, timeframe, parts_dir = item
    if legacy_before is None:
        legacy_before = default_legacy_before(timeframe, datetime.now(timezone.utc).date())
    census_before = file_census(parts_dir)
    periods = []
    groups = daily_files_by_period(parts_dir, timeframe, before)
    every_day = daily_files_by_period(parts_dir, timeframe, date.max)
    collisions = colliding_daily_files([path for paths in every_day.values() for path in paths])
    for stem, paths in sorted(groups.items()):
        try:
            record = consolidate_period(parts_dir, stem, paths, apply=apply, legacy_before=legacy_before,
                                        collisions=collisions)
        except Exception as exc:  # noqa: BLE001 - reported; sources are only deleted after verification
            record = {"parts_dir": str(parts_dir), "period": stem, "daily_files": len(paths),
                      "status": "error", "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
        periods.append(record)
    orphan_locks = discard_orphan_day_locks(parts_dir, before) if apply else 0
    return {"exchange": exchange, "symbol": symbol_dir, "timeframe": timeframe, "periods": periods,
            "census_before": census_before, "census_after": file_census(parts_dir) if apply else None,
            "orphan_locks_removed": orphan_locks}


def summarize(results: List[dict], apply: bool, before: date, legacy_before: Optional[date], seconds: float) -> dict:
    statuses: Counter = Counter()
    held: Counter = Counter()
    by_tf: dict = {}
    totals = Counter()
    for item in results:
        tf = by_tf.setdefault(item["timeframe"], Counter())
        tf["series"] += 1
        for key in ("files", "bytes", "clusters"):
            tf[f"{key}_before"] += item["census_before"][key]
            if item["census_after"]:
                tf[f"{key}_after"] += item["census_after"][key]
        tf["orphan_locks_removed"] += item["orphan_locks_removed"]
        for period in item["periods"]:
            statuses[period["status"]] += 1
            for entry in period.get("held", []):
                held[entry["reason"]] += 1
            if period["status"] in ("consolidated", "would_consolidate"):
                tf["verified_files_moved"] += period.get("verified_files", 0)
                tf["legacy_files_packed"] += period.get("legacy_files", 0)
                tf["rows_moved"] += period.get("verified_rows", 0) + period.get("legacy_rows", 0)
                tf["output_files"] += len(period.get("outputs", []))
            tf["daily_files_held"] += period.get("held_files", 0)
            totals["overlap_rows"] += period.get("overlap_rows", 0)
    for tf in by_tf.values():
        for key in tf:
            totals[key] += tf[key] if key != "series" else 0
        totals["series"] += tf["series"]
    return {"apply": apply, "before_day": before.isoformat(), "legacy_before_day": legacy_before.isoformat() if legacy_before else "per timeframe",
            "seconds": round(seconds),
            "period_statuses": dict(statuses), "held_daily_files_by_reason": dict(held),
            "by_timeframe": {k: dict(v) for k, v in sorted(by_tf.items())}, "totals": dict(totals)}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="write period files and delete verified daily files")
    parser.add_argument("--exchanges", nargs="*", default=["binance", "gate"])
    parser.add_argument("--only", nargs="*", help="base assets only, e.g. BTC ETH")
    parser.add_argument("--timeframes", nargs="*", help="e.g. 1m 1h (default: every minute/hour/day timeframe)")
    parser.add_argument("--before", help="move days before this UTC date (default: today; today's file is the "
                                         "writer's and moves on its next write)")
    parser.add_argument("--legacy-before", help="pack unmarked days before this UTC date (default per timeframe: "
                                                "50 bars and at least 14 days back)")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--root", type=Path, help="storage root (default: settings.DATA_STORAGE_PATH)")
    parser.add_argument("--out-dir", type=Path, help="journal and summary directory")
    args = parser.parse_args(argv)
    from config.settings import settings

    root = Path(args.root or settings.DATA_STORAGE_PATH)
    before = date.fromisoformat(args.before) if args.before else datetime.now(timezone.utc).date()
    legacy_before = date.fromisoformat(args.legacy_before) if args.legacy_before else None
    only = {s.upper() for s in args.only} if args.only else None
    targets = series_targets(root, args.exchanges, only, set(args.timeframes) if args.timeframes else None)
    out_dir = args.out_dir or ROOT / "data" / "research" / f"kline_parts_consolidation_{datetime.now():%Y%m%d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S") + ("" if args.apply else "_dry")
    journal = out_dir / f"run_{stamp}.jsonl"
    print(f"{len(targets)} series under {root}; days before {before} (unmarked: before "
          f"{legacy_before or 'max(14 days, 50 bars) ago'}); "
          f"{'APPLY' if args.apply else 'DRY RUN'}", flush=True)
    began = time.time()
    results: List[dict] = []
    with journal.open("w", encoding="utf-8") as sink, ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(run_series, item, before, legacy_before, args.apply) for item in targets]
        for done, future in enumerate(as_completed(futures), 1):
            item = future.result()
            results.append(item)
            sink.write(to_json_line(item) + "\n")
            sink.flush()
            if done % 25 == 0 or done == len(futures):
                print(f"  {done}/{len(futures)} series, {time.time() - began:.0f}s", flush=True)
    summary = summarize(results, args.apply, before, legacy_before, time.time() - began)
    summary["journal"] = str(journal)
    (out_dir / f"summary_{stamp}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1), flush=True)
    return 1 if summary["period_statuses"].get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
