"""Find USD-M perpetual bars stored in the spot kline store (read-only).

With BINANCE_DEFAULT_TYPE=future, in-process writers (web chart refresh, the
autonomous agent, the strategy manager, downloads) saved perp bars under
data/historical/binance/ (spot). This scan checks every row of the spot
``<tf>_parts`` files and main ``<tf>.parquet`` files against two hourly
references:

- spot 1h: the official spot archive (data.binance.vision, direct, cached under
  --out-dir). The local ``binance/<SYM>/1h.parquet`` is not used: it can hold
  the same perp writes as the file being checked and would vouch for them;
- perp 1h: ``binance_futures/<SYM>/1h.parquet`` (or ``1000<SYM>`` / ``1000000<SYM>``
  divided by the multiplier, as the connector reports them).

Every bar must agree with the 1h bars at hour boundaries: a bar starting at HH:00
carries that hour's open, a bar ending at HH+1:00 that hour's close (1h/4h/1d
bars carry both). A check "matches perp" when it equals the perp value and
differs from spot, "perp_only" when it equals the perp value and the spot
archive has no such hour (the contract traded before the spot pair, or after it
was delisted), "perp_8h" when it equals the perp bar 8 hours earlier and not the
spot one (perp bars stored on the local clock: a timezone problem as well, which
a check at the stored time cannot see). ``--confirm`` re-checks each flagged day
row for row against the spot and USD-M daily archives of the same timeframe.

Main files whose UTC marker proves spot provenance are skipped:
``writer:binance_compact`` (the spot collector marks a file only when it created
it from a full spot download or every stored row passed the UTC guard against
its spot bars) and ``binance_public_archive`` (certified row for row against
the spot archive).

Nothing is written outside --out-dir.

    python scripts/scan_kline_venue_pollution.py                  # whole store, parts and main files
    python scripts/scan_kline_venue_pollution.py --confirm
    python scripts/scan_kline_venue_pollution.py --only BTC --timeframes 1m 15m --sources main
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from core.data.data_storage import _parquet_index_column  # noqa: E402

ROOT = PROJECT_ROOT / "data" / "historical"
OUT_DIR = PROJECT_ROOT / "data" / "research" / "kline_venue_pollution_20261027"
TIMEFRAMES = ("1s", "10s", "1m", "5m", "15m", "1h", "4h", "1d")
SOURCES = ("parts", "main")
STEP = {"1s": "1s", "10s": "10s", "1m": "1min", "5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1D"}
ARCHIVE_TIMEFRAMES = {"1s", "1m", "5m", "15m", "1h", "4h", "1d"}  # USD-M has no 1s; 10s exists nowhere
PRICES = ["open", "high", "low", "close"]
COUNTS = ("checked", "perp", "spot", "both", "neither", "perp_only", "perp_8h")
LOCAL_CLOCK = pd.Timedelta(hours=8)
MIN_PERP_8H = 3  # single hits at an 8-hour offset happen by chance on flat bars
SPOT_PROVEN_ORIGINS = {"writer:binance_compact", "binance_public_archive"}
CHUNK_ROWS = 1_000_000  # main 1s files hold tens of millions of rows
HOUR = pd.Timedelta("1h")
DAY = pd.Timedelta("1D")


def _archive():
    spec = importlib.util.spec_from_file_location(
        "certify_archive", PROJECT_ROOT / "scripts" / "certify_binance_store_from_archive.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ARCHIVE = _archive()


def _normalized(frame: pd.DataFrame) -> pd.DataFrame:
    index = pd.DatetimeIndex(frame.index)
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    frame.index = index.as_unit("ns")
    return frame[~frame.index.duplicated(keep="last")].sort_index()


def _read(path: Path, columns: List[str] = PRICES, day: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """Columns of one file (only one UTC day of it when ``day`` is given)."""
    schema = pq.read_schema(path)
    filters = None
    if day is not None:
        column = _parquet_index_column(path)
        lower, upper = day, day + DAY
        if getattr(schema.field(column).type, "tz", None):  # some files store UTC-aware stamps
            lower, upper = lower.tz_localize("UTC"), upper.tz_localize("UTC")
        filters = [(column, ">=", lower.to_pydatetime()), (column, "<", upper.to_pydatetime())]
    frame = pd.read_parquet(path, columns=[c for c in columns if c in schema.names], filters=filters)
    return _normalized(frame)


def _chunks(path: Path) -> Iterator[pd.DataFrame]:
    """The file's price rows, at most CHUNK_ROWS at a time."""
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows <= CHUNK_ROWS:
        yield _read(path)
        return
    column = _parquet_index_column(path)
    columns = [column] + [c for c in PRICES if c in parquet.schema_arrow.names]
    for batch in parquet.iter_batches(batch_size=CHUNK_ROWS, columns=columns):
        frame = pa.Table.from_batches([batch]).replace_schema_metadata(None).to_pandas().set_index(column)
        yield _normalized(frame)


def _months(path: Path) -> List[str]:
    index = pd.DatetimeIndex(pd.read_parquet(path, columns=[]).index)
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    return [str(m) for m in np.unique(index.values.astype("datetime64[M]"))]


def _origin(path: Path) -> str:
    """The file's UTC marker origin, or its timestamp status when it carries no verified marker."""
    meta = pq.read_schema(path).metadata or {}
    attrs: dict = {}
    if meta.get(b"PANDAS_ATTRS"):
        attrs.update(json.loads(meta[b"PANDAS_ATTRS"]))
    if meta.get(b"pandas"):
        attrs.update(json.loads(meta[b"pandas"]).get("attributes") or {})
    if attrs.get("cts_timestamp_timezone") == "UTC" and attrs.get("cts_timestamp_schema") == 1:
        return str(attrs.get("cts_timestamp_origin") or "verified_utc")
    return str(attrs.get("cts_timestamp_status") or "unmarked")


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
    """Official spot 1h bars for the given months (cached; labels of what was fetched live in the cache's attrs).

    Monthly zips for closed months; daily zips for the current month and for last month until its monthly zip is
    published. An older month without a monthly zip had no spot trading; it is remembered and not asked again.
    """
    raw = symbol_dir.replace("_", "")
    cache = cache_dir / f"{symbol_dir}.parquet"
    legacy_meta = cache_dir / f"{symbol_dir}.json"
    cached = pd.read_parquet(cache) if cache.exists() else _empty()
    fetched = set(cached.attrs.get("labels") or [])
    if legacy_meta.exists():
        fetched |= set(json.loads(legacy_meta.read_text())["labels"])
    cached = _normalized(cached[PRICES]) if len(cached) else _empty()
    frames = [cached]
    before = set(fetched)
    this_month = today.strftime("%Y-%m")
    last_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    def fetch_day(day: pd.Timestamp) -> None:
        label = f"D{day:%Y-%m-%d}"
        if label in fetched or day.date() >= today.date():
            return
        payload = ARCHIVE.fetch_zip(f"spot/daily/klines/{raw}/1h/{raw}-1h-{day:%Y-%m-%d}.zip")
        if payload is not None:
            frames.append(ARCHIVE.parse_archive_csv(payload)[PRICES])
            fetched.add(label)

    for month in months:
        if month > this_month:
            continue
        month_start = pd.Timestamp(month + "-01")
        days = list(pd.date_range(month_start, month_start + pd.offsets.MonthEnd(1), freq="D"))
        if month == this_month:
            for day in days:
                fetch_day(day)
            continue
        if f"M{month}" in fetched or f"X{month}" in fetched:
            continue
        payload = ARCHIVE.fetch_zip(f"spot/monthly/klines/{raw}/1h/{raw}-1h-{month}.zip")
        if payload is not None:
            frames.append(ARCHIVE.parse_archive_csv(payload)[PRICES])
            fetched.add(f"M{month}")
        elif month >= last_month:  # monthly zips appear a few days into the next month
            for day in days:
                fetch_day(day)
        else:
            fetched.add(f"X{month}")
    archive = pd.concat(frames)
    archive = archive[~archive.index.duplicated(keep="last")].sort_index()
    if fetched != before or legacy_meta.exists():
        cache_dir.mkdir(parents=True, exist_ok=True)
        stored = archive.copy()
        stored.attrs = {"labels": sorted(fetched)}
        stored.to_parquet(cache)
        legacy_meta.unlink(missing_ok=True)  # one file per symbol (exFAT: 1 MiB per file)
    return archive


def classify(values: np.ndarray, spot: np.ndarray, perp: np.ndarray,
             spot_8h: Optional[np.ndarray] = None, perp_8h: Optional[np.ndarray] = None) -> Dict[str, np.ndarray]:
    """Hour-boundary values against the spot and perp bars at the stored hour (and 8 hours earlier)."""
    has_spot, has_perp = ~np.isnan(spot), ~np.isnan(perp)
    if spot_8h is None or perp_8h is None:
        shifted = np.zeros(len(values), dtype=bool)
    else:
        shifted = ~np.isnan(perp_8h) & (values == perp_8h) & (values != spot_8h)
    checked = has_spot & has_perp
    eq_spot = checked & (values == spot)
    eq_perp = checked & (values == perp)
    return {
        "checked": checked,
        "perp": eq_perp & ~eq_spot,
        "spot": eq_spot & ~eq_perp,
        "both": eq_spot & eq_perp,
        "neither": checked & ~eq_spot & ~eq_perp,
        "perp_only": has_perp & ~has_spot & (values == perp),
        "perp_8h": shifted & ~(checked & (values == spot)) & ~(values == perp),
    }


def scan_frame(frame: pd.DataFrame, timeframe: str, spot: pd.DataFrame, perp: pd.DataFrame) -> pd.DataFrame:
    """Hour-boundary checks of one parts file (or chunk of a main file), counted per UTC day."""
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
            spot[column].reindex(hours - LOCAL_CLOCK).to_numpy(float),
            perp[column].reindex(hours - LOCAL_CLOCK).to_numpy(float),
        ))
    if not results:
        return pd.DataFrame()
    stamp_index = pd.DatetimeIndex(np.concatenate([s.to_numpy() for s in stamps]))
    result = {key: np.concatenate([r[key] for r in results]) for key in COUNTS}
    grouped = pd.DataFrame({k: v.astype(int) for k, v in result.items()}, index=stamp_index.normalize()).groupby(level=0).sum()
    grouped["rows"] = frame.groupby(idx.normalize()).size().reindex(grouped.index).fillna(0).astype(int)
    hits = stamp_index[result["perp"] | result["perp_only"] | result["perp_8h"]]
    if len(hits):
        span = pd.Series(hits, index=hits.normalize()).groupby(level=0).agg(["min", "max"])
        grouped["perp_first"] = span["min"].reindex(grouped.index)
        grouped["perp_last"] = span["max"].reindex(grouped.index)
    return grouped


def _combine(parts: List[pd.DataFrame]) -> pd.DataFrame:
    """Per-day results of consecutive chunks of one file (a day may span two chunks)."""
    parts = [part for part in parts if len(part)]
    if len(parts) <= 1:
        return parts[0] if parts else pd.DataFrame()
    frame = pd.concat(parts)
    how = {key: "sum" for key in COUNTS + ("rows",)}
    if "perp_first" in frame.columns:
        how.update(perp_first="min", perp_last="max")
    return frame.groupby(level=0).agg(how)


def scan_symbol(symbol_dir: str, timeframes: List[str], cache_dir: Path, today: datetime,
                sources: Tuple[str, ...] = SOURCES) -> Tuple[List[dict], List[dict]]:
    # Index-only first pass: files are read one at a time (main files chunk by chunk) below.
    files: List[Tuple[str, str, Path]] = []
    skipped_main: List[dict] = []
    months: set = set()
    symbol_root = ROOT / "binance" / symbol_dir
    for timeframe in timeframes:
        candidates: List[Tuple[str, Path]] = []
        if "parts" in sources:
            candidates += [("parts", p) for p in sorted((symbol_root / f"{timeframe}_parts").glob("*.parquet"))]
        main = symbol_root / f"{timeframe}.parquet"
        if "main" in sources and main.exists():
            origin = _origin(main)
            if origin in SPOT_PROVEN_ORIGINS:
                skipped_main.append({"series": f"{symbol_dir}/{timeframe}", "source": "main",
                                     "skipped": f"spot provenance marker ({origin})"})
            else:
                candidates.append(("main", main))
        for kind, path in candidates:
            found = _months(path)
            if found:
                files.append((timeframe, kind, path))
                months.update(found)
    if not files:
        return [], skipped_main
    perp, perp_name = perp_reference(symbol_dir)
    summaries: Dict[Tuple[str, str], dict] = {}
    for timeframe, kind, _ in files:
        summaries.setdefault((timeframe, kind), {"series": f"{symbol_dir}/{timeframe}", "source": kind,
                                                 "perp_reference": perp_name, "files": 0, "rows": 0,
                                                 **{k: 0 for k in COUNTS}})
    if perp is None:
        for s in summaries.values():
            s["skipped"] = "no perp reference (spot-only coin)"
        return [], list(summaries.values()) + skipped_main
    try:
        spot = spot_reference(symbol_dir, sorted(months), cache_dir, today)
    except Exception as exc:  # noqa: BLE001 - reported per symbol
        for s in summaries.values():
            s["skipped"] = f"spot archive: {type(exc).__name__}: {str(exc)[:120]}"
        return [], list(summaries.values()) + skipped_main
    records: List[dict] = []
    for timeframe, kind, path in files:
        summary = summaries[(timeframe, kind)]
        summary["files"] += 1
        parts = []
        for chunk in _chunks(path):
            summary["rows"] += len(chunk)
            parts.append(scan_frame(chunk, timeframe, spot, perp))
        grouped = _combine(parts)
        for day, row in grouped.iterrows():
            for key in COUNTS:
                summary[key] += int(row[key])
            if int(row["perp"]) or int(row["perp_only"]) or int(row["perp_8h"]) >= MIN_PERP_8H:
                records.append({
                    "series": summary["series"], "source": kind, "file": path.name, "day": day.date().isoformat(),
                    "perp_reference": perp_name, "rows": int(row["rows"]), **{k: int(row[k]) for k in COUNTS},
                    "perp_first": str(row.get("perp_first")), "perp_last": str(row.get("perp_last")),
                })
    return records, list(summaries.values()) + skipped_main


def record_path(record: dict) -> Path:
    symbol_dir, timeframe = record["series"].split("/")
    if record.get("source") == "main":
        return ROOT / "binance" / symbol_dir / record["file"]
    return ROOT / "binance" / symbol_dir / f"{timeframe}_parts" / record["file"]


def confirm_day(record: dict) -> dict:
    """Row-for-row comparison of one flagged day with the spot and USD-M daily archives."""
    symbol_dir, timeframe = record["series"].split("/")
    out = dict(record)
    if timeframe not in ARCHIVE_TIMEFRAMES:
        out["confirm"] = "no archive for this timeframe"
        return out
    day = pd.Timestamp(record["day"])
    rows = _read(record_path(record), day=day)
    rows = rows[(rows.index >= day) & (rows.index < day + DAY)]
    values = rows[PRICES].to_numpy(float)
    out["rows_in_day"] = int(len(rows))
    perp_symbol = record.get("perp_reference") or symbol_dir
    multiplier = float(perp_symbol[: len(perp_symbol) - len(symbol_dir)] or 1)
    equal: Dict[str, Optional[np.ndarray]] = {}
    shifted: Dict[str, Optional[np.ndarray]] = {}
    days = [record["day"]] + ([(day - DAY).date().isoformat()] if record.get("perp_8h") else [])
    for market, root, raw in (("spot", "spot", symbol_dir.replace("_", "")),
                              ("perp", "futures/um", perp_symbol.replace("_", ""))):
        equal[market] = shifted[market] = None
        frames = []
        try:
            for item in days:
                payload = ARCHIVE.fetch_zip(f"{root}/daily/klines/{raw}/{timeframe}/{raw}-{timeframe}-{item}.zip")
                frames.append(None if payload is None else ARCHIVE.parse_archive_csv(payload)[PRICES])
        except Exception as exc:  # noqa: BLE001 - reported per day
            out[f"rows_equal_{market}_archive"] = f"error: {exc}"
            continue
        if frames[0] is None:
            out[f"rows_equal_{market}_archive"] = None
            continue
        ref = pd.concat([f for f in frames if f is not None])
        if market == "perp" and multiplier > 1:
            ref = ref / multiplier
        equal[market] = (values == ref.reindex(rows.index).to_numpy(float)).all(axis=1)
        out[f"rows_equal_{market}_archive"] = int(equal[market].sum())
        if len(days) > 1:
            shifted[market] = (values == ref.reindex(rows.index - LOCAL_CLOCK).to_numpy(float)).all(axis=1)
    if shifted["perp"] is not None:
        spot_8h = shifted["spot"] if shifted["spot"] is not None else np.zeros(len(rows), dtype=bool)
        out["rows_perp_8h"] = int((shifted["perp"] & ~spot_8h).sum())  # perp bars on the local clock
    if equal["spot"] is not None and equal["perp"] is not None:
        # Flat bars of illiquid coins can be identical on both venues.
        hits = equal["perp"] & ~equal["spot"]
        out["rows_perp_not_spot"] = int(hits.sum())
        out["perp_not_spot_first"] = str(rows.index[hits].min())
        out["perp_not_spot_last"] = str(rows.index[hits].max())
    elif equal["perp"] is not None and "rows_equal_spot_archive" in out and out["rows_equal_spot_archive"] is None:
        out["rows_perp_no_spot_archive"] = int(equal["perp"].sum())  # no spot bars that day at all
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="*", help="base assets, e.g. BTC ETH")
    parser.add_argument("--timeframes", nargs="*", default=list(TIMEFRAMES))
    parser.add_argument("--sources", nargs="*", default=list(SOURCES), choices=SOURCES)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--confirm", action="store_true", help="re-check flagged days against daily archives")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args(argv)

    only = {s.upper() for s in args.only or []}
    sources = tuple(args.sources)

    def in_scope(d: Path) -> bool:
        return any(("parts" in sources and (d / f"{tf}_parts").is_dir())
                   or ("main" in sources and (d / f"{tf}.parquet").exists()) for tf in args.timeframes)

    symbols = [d.name for d in sorted((ROOT / "binance").iterdir())
               if d.is_dir() and (not only or d.name.split("_")[0] in only) and in_scope(d)]
    today = datetime.now(timezone.utc).replace(tzinfo=None)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = args.out_dir / "spot_1h_archive_cache"
    print(f"{len(symbols)} symbols, sources {list(sources)}, timeframes {args.timeframes}", flush=True)

    records: List[dict] = []
    summaries: List[dict] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        for found, series in pool.map(lambda s: scan_symbol(s, list(args.timeframes), cache_dir, today, sources), symbols):
            records.extend(found)
            summaries.extend(series)
            for s in series:
                if s.get("perp") or s.get("perp_only"):
                    print(f"{s['series']} [{s['source']}]: perp {s['perp']} perp_only {s['perp_only']} "
                          f"/ checked {s['checked']}", flush=True)

    def confirm_or_report(record: dict) -> dict:
        try:
            return confirm_day(record)
        except Exception as exc:  # noqa: BLE001 - one unreadable day must not lose the whole scan
            return {**record, "confirm": f"error: {type(exc).__name__}: {str(exc)[:160]}"}

    if args.confirm and records:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            records = list(pool.map(confirm_or_report, records))

    days_path = args.out_dir / f"polluted_days_{stamp}.jsonl"
    with days_path.open("w", encoding="utf-8") as out:
        for record in sorted(records, key=lambda r: (r["series"], r["day"], r.get("source", ""), r["file"])):
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
    totals: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for s in summaries:
        for key in ("files", "rows", *COUNTS):
            totals[s["source"]][key] += s.get(key, 0)
    report = {
        "generated_at": stamp,
        "sources": list(sources),
        "spot_reference": "official spot 1h archive",
        "series_scanned": sum(1 for s in summaries if not s.get("skipped")),
        "series_with_perp_rows": sum(1 for s in summaries if s.get("perp") or s.get("perp_only")),
        "skipped": dict(Counter(s["skipped"].split(" (")[0] for s in summaries if s.get("skipped"))),
        "main_files_skipped_by_marker": dict(Counter(s["skipped"] for s in summaries
                                                     if str(s.get("skipped", "")).startswith("spot provenance"))),
        "polluted_series_days": len(records),
        "totals": {source: dict(values) for source, values in totals.items()},
        "series": sorted((s for s in summaries if s.get("perp") or s.get("perp_only")),
                         key=lambda s: -(s["perp"] + s["perp_only"])),
        "days_file": days_path.name,
    }
    summary_path = args.out_dir / f"summary_{stamp}.json"
    summary_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("series_scanned", "series_with_perp_rows", "skipped",
                                             "main_files_skipped_by_marker", "polluted_series_days", "totals")},
                     indent=2), flush=True)
    print(f"wrote {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
