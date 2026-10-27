"""Find USD-M perpetual bars stored in the spot kline store (read-only).

With BINANCE_DEFAULT_TYPE=future, in-process writers (web chart refresh, the
autonomous agent, the strategy manager, downloads) saved perp bars under
data/historical/binance/ (spot). This scan checks every spot ``<tf>_parts`` row
against two hourly references:

- spot 1h: ``binance/<SYM>/1h.parquet`` plus the official spot archive
  (data.binance.vision, direct, cached under --out-dir) for later months;
- perp 1h: ``binance_futures/<SYM>/1h.parquet`` (or ``1000<SYM>`` / ``1000000<SYM>``
  divided by the multiplier, as the connector reports them).

Every bar must agree with the 1h bars at hour boundaries: a bar starting at HH:00
carries that hour's open, a bar ending at HH+1:00 that hour's close (1h/4h/1d
bars carry both). A check "matches perp" when it equals the perp value and
differs from spot. ``--confirm`` re-checks each flagged day row for row against
the spot and USD-M daily archives of the same timeframe.

Nothing is written outside --out-dir.

    python scripts/scan_kline_venue_pollution.py                  # whole store
    python scripts/scan_kline_venue_pollution.py --confirm
    python scripts/scan_kline_venue_pollution.py --only BTC --timeframes 1m 15m
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

ROOT = PROJECT_ROOT / "data" / "historical"
OUT_DIR = PROJECT_ROOT / "data" / "research" / "kline_venue_pollution_20261027"
TIMEFRAMES = ("1s", "10s", "1m", "5m", "15m", "1h", "4h", "1d")
STEP = {"1s": "1s", "10s": "10s", "1m": "1min", "5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1D"}
ARCHIVE_TIMEFRAMES = {"1s", "1m", "5m", "15m", "1h", "4h", "1d"}  # USD-M has no 1s; 10s exists nowhere
PRICES = ["open", "high", "low", "close"]
COUNTS = ("checked", "perp", "spot", "both", "neither")
HOUR = pd.Timedelta("1h")


def _archive():
    spec = importlib.util.spec_from_file_location(
        "certify_archive", PROJECT_ROOT / "scripts" / "certify_binance_store_from_archive.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ARCHIVE = _archive()


def _read(path: Path, columns: List[str] = PRICES) -> pd.DataFrame:
    frame = pd.read_parquet(path, columns=[c for c in columns if c in pq.read_schema(path).names])
    index = pd.DatetimeIndex(frame.index)
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    frame.index = index.as_unit("ns")
    return frame[~frame.index.duplicated(keep="last")].sort_index()


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=PRICES, index=pd.DatetimeIndex([], name="timestamp")).astype(float)


def perp_reference(symbol_dir: str) -> Tuple[Optional[pd.DataFrame], str]:
    base, _, quote = symbol_dir.partition("_")
    for multiplier in (1, 1000, 1_000_000):
        name = symbol_dir if multiplier == 1 else f"{multiplier}{base}_{quote}"
        path = ROOT / "binance_futures" / name / "1h.parquet"
        if path.exists():
            frame = _read(path)
            if multiplier > 1:
                frame[PRICES] = frame[PRICES] / float(multiplier)
            return frame, name
    return None, ""


def spot_reference(symbol_dir: str, months: List[str], cache_dir: Path, today: datetime) -> pd.DataFrame:
    """Local spot 1h, extended with archive months it does not reach (cached)."""
    raw = symbol_dir.replace("_", "")
    path = ROOT / "binance" / symbol_dir / "1h.parquet"
    local = _read(path) if path.exists() else _empty()
    local_end = local.index.max() if len(local) else pd.Timestamp("1970-01-01")
    cache = cache_dir / f"{symbol_dir}.parquet"
    meta_path = cache_dir / f"{symbol_dir}.json"
    cached = _read(cache) if cache.exists() else _empty()
    fetched = set(json.loads(meta_path.read_text())["labels"]) if meta_path.exists() else set()
    frames = [cached]
    this_month = today.strftime("%Y-%m")

    def fetch_day(day: pd.Timestamp) -> None:
        label = f"D{day:%Y-%m-%d}"
        if label in fetched or day.date() >= today.date():
            return
        payload = ARCHIVE.fetch_zip(f"spot/daily/klines/{raw}/1h/{raw}-1h-{day:%Y-%m-%d}.zip")
        if payload is not None:
            frames.append(ARCHIVE.parse_archive_csv(payload)[PRICES])
            fetched.add(label)

    for month in months:
        month_start = pd.Timestamp(month + "-01")
        month_last_hour = month_start + pd.offsets.MonthEnd(1) + pd.Timedelta("23h")
        if month_last_hour <= local_end or month > this_month:
            continue
        days = [d for d in pd.date_range(month_start, month_start + pd.offsets.MonthEnd(1), freq="D")]
        if month == this_month:
            for day in days:
                fetch_day(day)
            continue
        if f"M{month}" in fetched:
            continue
        payload = ARCHIVE.fetch_zip(f"spot/monthly/klines/{raw}/1h/{raw}-1h-{month}.zip")
        if payload is not None:
            frames.append(ARCHIVE.parse_archive_csv(payload)[PRICES])
            fetched.add(f"M{month}")
        else:  # month not published yet (or coin not listed then): take its days
            for day in days:
                fetch_day(day)
    archive = pd.concat(frames)
    archive = archive[~archive.index.duplicated(keep="last")].sort_index()
    cache_dir.mkdir(parents=True, exist_ok=True)
    archive.to_parquet(cache)
    meta_path.write_text(json.dumps({"labels": sorted(fetched)}))
    merged = pd.concat([local, archive])
    return merged[~merged.index.duplicated(keep="last")].sort_index()


def classify(values: np.ndarray, spot: np.ndarray, perp: np.ndarray) -> Dict[str, np.ndarray]:
    checked = ~np.isnan(spot) & ~np.isnan(perp)
    eq_spot = checked & (values == spot)
    eq_perp = checked & (values == perp)
    return {
        "checked": checked,
        "perp": eq_perp & ~eq_spot,
        "spot": eq_spot & ~eq_perp,
        "both": eq_spot & eq_perp,
        "neither": checked & ~eq_spot & ~eq_perp,
    }


def scan_frame(frame: pd.DataFrame, timeframe: str, spot: pd.DataFrame, perp: pd.DataFrame) -> pd.DataFrame:
    """Hour-boundary checks of one parts file, counted per UTC day."""
    idx = frame.index
    ends = idx + pd.Timedelta(STEP[timeframe])
    stamps, results = [], []
    for mask, column, hours in (
        (idx == idx.floor("1h"), "open", idx),
        (ends == ends.floor("1h"), "close", ends - HOUR),
    ):
        if not mask.any():
            continue
        hours = pd.DatetimeIndex(hours[mask])
        stamps.append(idx[mask])
        results.append(classify(
            frame.loc[mask, column].to_numpy(float),
            spot[column].reindex(hours).to_numpy(float),
            perp[column].reindex(hours).to_numpy(float),
        ))
    if not results:
        return pd.DataFrame()
    stamp_index = pd.DatetimeIndex(np.concatenate([s.to_numpy() for s in stamps]))
    result = {key: np.concatenate([r[key] for r in results]) for key in COUNTS}
    grouped = pd.DataFrame({k: v.astype(int) for k, v in result.items()}, index=stamp_index.normalize()).groupby(level=0).sum()
    grouped["rows"] = frame.groupby(idx.normalize()).size().reindex(grouped.index).fillna(0).astype(int)
    hits = stamp_index[result["perp"]]
    if len(hits):
        span = pd.Series(hits, index=hits.normalize()).groupby(level=0).agg(["min", "max"])
        grouped["perp_first"] = span["min"].reindex(grouped.index)
        grouped["perp_last"] = span["max"].reindex(grouped.index)
    return grouped


def scan_symbol(symbol_dir: str, timeframes: List[str], cache_dir: Path, today: datetime) -> Tuple[List[dict], List[dict]]:
    # Index-only first pass: 1s parts hold ~86k rows a day, so files are read
    # one at a time below instead of all at once.
    files: List[Tuple[str, Path]] = []
    months: set = set()
    for timeframe in timeframes:
        for path in sorted((ROOT / "binance" / symbol_dir / f"{timeframe}_parts").glob("*.parquet")):
            index = pd.DatetimeIndex(pd.read_parquet(path, columns=[]).index)
            if len(index):
                files.append((timeframe, path))
                months.update(index.strftime("%Y-%m").unique())
    if not files:
        return [], []
    perp, perp_name = perp_reference(symbol_dir)
    summaries: Dict[str, dict] = {}
    for timeframe, _ in files:
        summaries.setdefault(timeframe, {"series": f"{symbol_dir}/{timeframe}", "perp_reference": perp_name,
                                         "files": 0, "rows": 0, **{k: 0 for k in COUNTS}})
    if perp is None:
        for s in summaries.values():
            s["skipped"] = "no perp reference (spot-only coin)"
        return [], list(summaries.values())
    try:
        spot = spot_reference(symbol_dir, sorted(months), cache_dir, today)
    except Exception as exc:  # noqa: BLE001 - reported per symbol
        for s in summaries.values():
            s["skipped"] = f"spot archive: {type(exc).__name__}: {str(exc)[:120]}"
        return [], list(summaries.values())
    records: List[dict] = []
    for timeframe, path in files:
        summary = summaries[timeframe]
        frame = _read(path)
        summary["files"] += 1
        summary["rows"] += len(frame)
        grouped = scan_frame(frame, timeframe, spot, perp)
        del frame
        for day, row in grouped.iterrows():
            for key in COUNTS:
                summary[key] += int(row[key])
            if int(row["perp"]):
                records.append({
                    "series": summary["series"], "file": path.name, "day": day.date().isoformat(),
                    "perp_reference": perp_name, "rows": int(row["rows"]), **{k: int(row[k]) for k in COUNTS},
                    "perp_first": str(row["perp_first"]), "perp_last": str(row["perp_last"]),
                })
    return records, list(summaries.values())


def confirm_day(record: dict) -> dict:
    """Row-for-row comparison of one flagged day with the spot and USD-M daily archives."""
    symbol_dir, timeframe = record["series"].split("/")
    out = dict(record)
    if timeframe not in ARCHIVE_TIMEFRAMES:
        out["confirm"] = "no archive for this timeframe"
        return out
    frame = _read(ROOT / "binance" / symbol_dir / f"{timeframe}_parts" / record["file"])
    day = pd.Timestamp(record["day"])
    rows = frame[(frame.index >= day) & (frame.index < day + pd.Timedelta("1D"))]
    values = rows[PRICES].to_numpy(float)
    out["rows_in_day"] = int(len(rows))
    perp_symbol = record.get("perp_reference") or symbol_dir
    multiplier = float(perp_symbol[: len(perp_symbol) - len(symbol_dir)] or 1)
    equal: Dict[str, Optional[np.ndarray]] = {}
    for market, root, raw in (("spot", "spot", symbol_dir.replace("_", "")),
                              ("perp", "futures/um", perp_symbol.replace("_", ""))):
        equal[market] = None
        try:
            payload = ARCHIVE.fetch_zip(f"{root}/daily/klines/{raw}/{timeframe}/{raw}-{timeframe}-{record['day']}.zip")
        except Exception as exc:  # noqa: BLE001 - reported per day
            out[f"rows_equal_{market}_archive"] = f"error: {exc}"
            continue
        if payload is None:
            out[f"rows_equal_{market}_archive"] = None
            continue
        ref = ARCHIVE.parse_archive_csv(payload)[PRICES]
        if market == "perp" and multiplier > 1:
            ref = ref / multiplier
        equal[market] = (values == ref.reindex(rows.index).to_numpy(float)).all(axis=1)
        out[f"rows_equal_{market}_archive"] = int(equal[market].sum())
    if equal["spot"] is not None and equal["perp"] is not None:
        # Flat bars of illiquid coins can be identical on both venues.
        out["rows_perp_not_spot"] = int((equal["perp"] & ~equal["spot"]).sum())
        out["perp_not_spot_first"] = str(rows.index[equal["perp"] & ~equal["spot"]].min())
        out["perp_not_spot_last"] = str(rows.index[equal["perp"] & ~equal["spot"]].max())
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="*", help="base assets, e.g. BTC ETH")
    parser.add_argument("--timeframes", nargs="*", default=list(TIMEFRAMES))
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--confirm", action="store_true", help="re-check flagged days against daily archives")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args(argv)

    only = {s.upper() for s in args.only or []}
    symbols = [
        d.name for d in sorted((ROOT / "binance").iterdir())
        if d.is_dir() and (not only or d.name.split("_")[0] in only)
        and any((d / f"{tf}_parts").is_dir() for tf in args.timeframes)
    ]
    today = datetime.now(timezone.utc).replace(tzinfo=None)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = args.out_dir / "spot_1h_archive_cache"

    records: List[dict] = []
    summaries: List[dict] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        for found, series in pool.map(lambda s: scan_symbol(s, list(args.timeframes), cache_dir, today), symbols):
            records.extend(found)
            summaries.extend(series)
            for s in series:
                if s["perp"]:
                    print(f"{s['series']}: perp {s['perp']} / checked {s['checked']}", flush=True)

    if args.confirm and records:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            records = list(pool.map(confirm_day, records))

    days_path = args.out_dir / f"polluted_days_{stamp}.jsonl"
    with days_path.open("w", encoding="utf-8") as out:
        for record in sorted(records, key=lambda r: (r["series"], r["day"], r["file"])):
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
    totals = defaultdict(int)
    for s in summaries:
        for key in ("files", "rows", *COUNTS):
            totals[key] += s.get(key, 0)
    report = {
        "generated_at": stamp,
        "series_scanned": len(summaries),
        "series_with_perp_rows": sum(1 for s in summaries if s["perp"]),
        "skipped": sum(1 for s in summaries if s.get("skipped")),
        "polluted_series_days": len(records),
        "totals": dict(totals),
        "series": sorted((s for s in summaries if s["perp"] or s.get("skipped")), key=lambda s: -s["perp"]),
        "days_file": days_path.name,
    }
    summary_path = args.out_dir / f"summary_{stamp}.json"
    summary_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("series_scanned", "series_with_perp_rows", "skipped",
                                             "polluted_series_days", "totals")}, indent=2))
    print(f"wrote {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
