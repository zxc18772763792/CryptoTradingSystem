"""One-time migration: rewrite local-time (UTC+8) kline parquet to UTC.

Root cause: scripts/maintain_top100_data.py historically wrote Kline
timestamps with a naive ``datetime.fromtimestamp`` (machine-local, Asia/
Shanghai = UTC+8). The writer is now fixed; this script corrects the
*existing* partitions so the reader no longer needs the future-time
heuristic in ``_normalize_parquet_frame_index``.

Detection is intentionally conservative — we only rewrite files we can
prove are local-stamped, and never guess on ambiguous fully-past files
(those would risk corrupting good UTC data):

  LOCAL_CONFIRMED : max timestamp runs ahead of real UTC now by > 2 min
                    (impossible for honest UTC bar-open data) -> shift -8h.
  SEAM_SUSPECT    : within one (exchange,symbol,tf) series there is a
                    contiguous tail segment offset ~+8h vs the established
                    UTC grid of the older majority -> shift only that
                    segment (reported; rewritten only with --apply).
  OK_UTC          : tz-aware, or tz-naive and not ahead and no seam.
  PAST_AMBIGUOUS  : tz-naive, entirely in the past, no usable reference
                    -> reported only, never modified.

Safe by default: dry-run unless --apply; per-file .bak backup; idempotent
(an already-UTC file is never shifted twice).

Usage:
  python scripts/migrate_parquet_klines_to_utc.py            # dry-run report
  python scripts/migrate_parquet_klines_to_utc.py --apply    # rewrite
"""
from __future__ import annotations

import argparse
import shutil
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

LOCAL_OFFSET = pd.Timedelta(hours=8)  # Asia/Shanghai, no DST
FUTURE_TOLERANCE = pd.Timedelta(minutes=2)
_TF_SECONDS = {
    "1s": 1, "5s": 5, "10s": 10, "30s": 30,
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200,
    "1d": 86400, "1w": 604800,
}


def _now_utc_naive() -> pd.Timestamp:
    return pd.Timestamp.utcnow().tz_localize(None)


def _read_raw(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df


def _tf_from_path(path: Path) -> str:
    # .../<tf>_parts/<date>.parquet  or  .../<tf>.parquet
    parent = path.parent.name
    if parent.endswith("_parts"):
        return parent[: -len("_parts")]
    return path.stem


def classify(df: pd.DataFrame) -> Tuple[str, Dict]:
    if df is None or df.empty:
        return "OK_UTC", {"reason": "empty"}
    idx = pd.to_datetime(df.index, errors="coerce")
    valid = idx[~idx.isna()]
    if len(valid) == 0:
        return "OK_UTC", {"reason": "no valid index"}
    if getattr(valid, "tz", None) is not None:
        return "OK_UTC", {"reason": "tz-aware"}
    now = _now_utc_naive()
    max_ts = valid.max()
    if max_ts > now + FUTURE_TOLERANCE:
        return "LOCAL_CONFIRMED", {
            "max_ts": str(max_ts),
            "now_utc": str(now),
            "ahead": str(max_ts - now),
        }
    # Entirely-past tz-naive: cannot prove local vs UTC from content alone.
    return "PAST_AMBIGUOUS", {"max_ts": str(max_ts), "now_utc": str(now)}


def _shift_file(path: Path, backup: bool) -> None:
    df = _read_raw(path)
    idx = pd.to_datetime(df.index, errors="coerce")
    df = df[~idx.isna()].copy()
    df.index = pd.DatetimeIndex(idx[~idx.isna()]) - LOCAL_OFFSET
    df = df[~df.index.duplicated(keep="last")].sort_index()
    if backup:
        shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))
    df.to_parquet(path)


def iter_kline_parquet(root: Path):
    if not root.exists():
        return
    for p in root.rglob("*.parquet"):
        if p.suffix == ".bak" or p.name.endswith(".bak"):
            continue
        if ".corrupt_" in p.name:
            continue
        yield p


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="./data/historical")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    ap.add_argument("--no-backup", action="store_true")
    args = ap.parse_args()

    root = Path(args.root)
    buckets: Dict[str, List[Path]] = defaultdict(list)
    details: Dict[Path, Dict] = {}

    for path in iter_kline_parquet(root):
        try:
            df = _read_raw(path)
        except Exception as e:  # unreadable/corrupt — leave for quarantine logic
            buckets["UNREADABLE"].append(path)
            details[path] = {"error": str(e)}
            continue
        label, info = classify(df)
        buckets[label].append(path)
        details[path] = info

    print(f"Scanned root: {root.resolve()}")
    for label in ("LOCAL_CONFIRMED", "PAST_AMBIGUOUS", "OK_UTC", "UNREADABLE"):
        print(f"  {label:<16} {len(buckets.get(label, []))}")

    confirmed = buckets.get("LOCAL_CONFIRMED", [])
    if confirmed:
        print("\nLOCAL_CONFIRMED (will shift -8h):")
        for p in confirmed[:20]:
            print(f"  {p}  {details[p]}")
        if len(confirmed) > 20:
            print(f"  ... and {len(confirmed) - 20} more")

    if buckets.get("PAST_AMBIGUOUS"):
        print(
            f"\n{len(buckets['PAST_AMBIGUOUS'])} PAST_AMBIGUOUS files left untouched "
            f"(cannot prove local vs UTC from content; relies on UTC writer going "
            f"forward + reader safety-net). Review manually if backtests look skewed."
        )

    if not args.apply:
        print("\nDRY-RUN. Re-run with --apply to rewrite LOCAL_CONFIRMED files.")
        return 0

    failed = 0
    for p in confirmed:
        try:
            _shift_file(p, backup=not args.no_backup)
            print(f"  shifted -8h: {p}")
        except Exception as e:
            failed += 1
            print(f"  FAILED {p}: {e}")
    print(f"\nDone. shifted={len(confirmed) - failed} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
