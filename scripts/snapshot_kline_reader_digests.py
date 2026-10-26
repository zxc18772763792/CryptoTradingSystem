"""Fingerprint what kline readers see, day by day, so a storage-layout change can be compared before and after.

For every series that has a ``{tf}_parts`` directory (second-level timeframes excluded) it records, per UTC day:

* ``reader``: the bars returned by ``DataStorage.load_klines_from_parquet`` (main file plus parts), and
* ``parts``: the bars a script sees when it concatenates ``{tf}_parts/*.parquet`` itself (keep last, sorted),

as a row count and a SHA-1 over the index and OHLCV values. It also records each series' reader columns, dtypes
and UTC status. Comparing two snapshots day by day separates layout effects (any difference on a day no writer
touched) from ordinary refreshes (the last days, which the hourly and daily jobs rewrite).

    python scripts/snapshot_kline_reader_digests.py --out before.parquet
    python scripts/snapshot_kline_reader_digests.py --out after.parquet --compare before.parquet
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.data.data_storage import DataStorage  # noqa: E402
from core.data.path_utils import symbol_from_storage_dirname  # noqa: E402

COLS = ["open", "high", "low", "close", "volume"]


def series_in_scope(storage_root: Path, exchanges: List[str], only: set[str] | None) -> List[tuple[str, str, str]]:
    found = []
    for exchange in exchanges:
        base = storage_root / exchange
        if not base.exists():
            continue
        for symbol_dir in sorted(p for p in base.iterdir() if p.is_dir()):
            if only and symbol_dir.name.split("_")[0].upper() not in only:
                continue
            for parts in sorted(p for p in symbol_dir.iterdir() if p.is_dir() and p.name.endswith("_parts")):
                timeframe = parts.name[: -len("_parts")]
                if timeframe and not timeframe.endswith("s"):
                    found.append((exchange, symbol_dir.name, timeframe))
    return found


def day_digests(frame: pd.DataFrame) -> Dict[str, tuple[int, str]]:
    if frame is None or frame.empty:
        return {}
    index = pd.DatetimeIndex(frame.index)
    values = np.ascontiguousarray(frame.reindex(columns=COLS).to_numpy(dtype="float64"))
    stamps = np.ascontiguousarray(index.asi8)
    days = index.normalize().asi8
    out: Dict[str, tuple[int, str]] = {}
    bounds = np.flatnonzero(np.diff(days)) + 1
    starts = np.concatenate([[0], bounds])
    ends = np.concatenate([bounds, [len(days)]])
    for start, end in zip(starts, ends):
        digest = hashlib.sha1(stamps[start:end].tobytes())
        digest.update(values[start:end].tobytes())
        out[str(pd.Timestamp(days[start]).date())] = (int(end - start), digest.hexdigest())
    return out


def parts_union(parts_dir: Path) -> pd.DataFrame:
    frames = [pd.read_parquet(path) for path in sorted(parts_dir.glob("*.parquet"))]
    if not frames:
        return pd.DataFrame(columns=COLS)
    frame = pd.concat(frames)
    frame.index = pd.to_datetime(frame.index, utc=True).tz_localize(None)
    return frame[~frame.index.duplicated(keep="last")].sort_index()


def snapshot(storage_root: Path, exchanges: List[str], only: set[str] | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    storage = DataStorage()
    storage.storage_path = storage_root
    day_rows: List[dict] = []
    meta_rows: List[dict] = []
    series = series_in_scope(storage_root, exchanges, only)
    began = time.time()
    for n, (exchange, dirname, timeframe) in enumerate(series, 1):
        symbol = symbol_from_storage_dirname(dirname)
        loaded = asyncio.run(storage.load_klines_from_parquet(exchange, symbol, timeframe))
        loaded = loaded[~loaded.index.duplicated(keep="last")] if len(loaded) else loaded
        union = parts_union(storage_root / exchange / dirname / f"{timeframe}_parts")
        meta_rows.append({
            "exchange": exchange, "symbol": dirname, "timeframe": timeframe,
            "reader_rows": int(len(loaded)), "parts_rows": int(len(union)),
            "reader_columns": json.dumps([str(c) for c in loaded.columns]),
            "reader_dtypes": json.dumps([str(t) for t in loaded.dtypes]),
            "reader_index_name": str(loaded.index.name),
            "reader_status": str(loaded.attrs.get("cts_timestamp_status")),
        })
        for view, frame in (("reader", loaded), ("parts", union)):
            for day, (rows, digest) in day_digests(frame).items():
                day_rows.append({"exchange": exchange, "symbol": dirname, "timeframe": timeframe,
                                 "view": view, "day": day, "rows": rows, "digest": digest})
        if n % 25 == 0 or n == len(series):
            print(f"  {n}/{len(series)} series, {time.time() - began:.0f}s", flush=True)
    return pd.DataFrame(day_rows), pd.DataFrame(meta_rows)


def compare(before_days: pd.DataFrame, after_days: pd.DataFrame, before_meta: pd.DataFrame,
            after_meta: pd.DataFrame) -> dict:
    key = ["exchange", "symbol", "timeframe", "view", "day"]
    merged = before_days.merge(after_days, on=key, how="outer", suffixes=("_before", "_after"), indicator=True)
    same = (merged["_merge"] == "both") & (merged["rows_before"] == merged["rows_after"]) \
        & (merged["digest_before"] == merged["digest_after"])
    changed = merged[~same].copy()
    by_day = changed.groupby("day").size().sort_index()
    meta_key = ["exchange", "symbol", "timeframe"]
    m = before_meta.merge(after_meta, on=meta_key, how="outer", suffixes=("_before", "_after"), indicator=True)
    meta_changes = {}
    for column in ("reader_columns", "reader_dtypes", "reader_index_name", "reader_status"):
        diff = m[(m["_merge"] == "both") & (m[f"{column}_before"] != m[f"{column}_after"])]
        meta_changes[column] = diff[meta_key + [f"{column}_before", f"{column}_after"]].to_dict("records")
    return {
        "series_before": int(len(before_meta)), "series_after": int(len(after_meta)),
        "series_missing_after": m[m["_merge"] == "left_only"][meta_key].to_dict("records"),
        "series_new_after": m[m["_merge"] == "right_only"][meta_key].to_dict("records"),
        "day_digests_compared": int(len(merged)), "day_digests_identical": int(same.sum()),
        "day_digests_changed": int(len(changed)),
        "changed_by_day": {str(k): int(v) for k, v in by_day.items()},
        "changed_by_view": {str(k): int(v) for k, v in changed.groupby("view").size().items()},
        "meta_changes": meta_changes,
        "changed_rows": changed.drop(columns=["_merge"]).to_dict("records"),
    }


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, required=True, help="snapshot file (.parquet); a _meta sibling is written")
    parser.add_argument("--compare", type=Path, help="earlier snapshot to compare against; writes <out>.compare.json")
    parser.add_argument("--exchanges", nargs="*", default=["binance", "gate"])
    parser.add_argument("--only", nargs="*", help="base assets only, e.g. BTC ETH")
    parser.add_argument("--root", type=Path, help="storage root (default: settings.DATA_STORAGE_PATH)")
    args = parser.parse_args(argv)
    from config.settings import settings

    root = Path(args.root or settings.DATA_STORAGE_PATH)
    only = {s.upper() for s in args.only} if args.only else None
    days, meta = snapshot(root, args.exchanges, only)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    days.to_parquet(args.out)
    meta_path = args.out.with_name(args.out.stem + "_meta.parquet")
    meta.to_parquet(meta_path)
    print(f"wrote {len(days)} day digests for {len(meta)} series to {args.out}", flush=True)
    if args.compare:
        before_meta = pd.read_parquet(args.compare.with_name(args.compare.stem + "_meta.parquet"))
        result = compare(pd.read_parquet(args.compare), days, before_meta, meta)
        report = args.out.with_suffix(".compare.json")
        report.write_text(json.dumps(result, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
        summary = {k: v for k, v in result.items() if k not in ("changed_rows", "meta_changes")}
        summary["meta_changes"] = {k: len(v) for k, v in result["meta_changes"].items()}
        print(json.dumps(summary, ensure_ascii=False, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
