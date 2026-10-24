"""Certify the wide Binance kline store as UTC against Binance's own public archive, then fill from it.

Why (2026-10-24): since the 2026-10-21 timezone repair the shared writer refuses to append to a kline parquet
that has no UTC marker unless the incoming UTC bars cover every stored row and agree with it in full
(core.data.data_storage._heal_local_existing_against_utc).  The 10/12-10/13 wide collection (every trading USDT
perpetual and spot pair, 1h/4h/1d) predates the marker, so the daily top-up failed for ~3,000 series and the
store stopped at 2026-10-20.

What this does: for each unmarked series it downloads the *complete* official history (monthly zips plus the
daily zips of the current month) from data.binance.vision - public, UTC, no proxy traffic - and merges it through
the same guard.  Nothing is guessed or relaxed:
  * the guard compares every stored row against the official bar at the same open time (OHLCV equal in full) and
    refuses the file otherwise; a refused file is left byte-for-byte untouched and is listed with a diagnosis;
  * an accepted file keeps every stored row, gains the UTC marker (origin "binance_public_archive") and any bar
    the store was missing, written with the writer's lock, zstd and atomic replace.

    python scripts/certify_binance_store_from_archive.py --dry-run --only BTC ETH   # judge, write nothing
    python scripts/certify_binance_store_from_archive.py                            # all trading series
    python scripts/certify_binance_store_from_archive.py --market futures --workers 12

Afterwards the ordinary daily top-up (collect_binance_history.py --tail-only) fills what the archive lacks.
Delisted series are left alone: nothing appends to them.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from core.data.data_storage import (  # noqa: E402
    _has_utc_timestamp_metadata,
    _heal_local_existing_against_utc,
    prepare_utc_kline_frame,
)
from core.data.parquet_lock import parquet_partition_lock  # noqa: E402
from core.data.path_utils import canonical_symbol_dir  # noqa: E402

ARCHIVE = "https://data.binance.vision/data"
TIMEFRAMES = ("1h", "4h", "1d")
OHLCV = ["open", "high", "low", "close", "volume"]
STEP_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
           "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
ORIGIN = "binance_public_archive"
OUT_DIR = PROJECT_ROOT / "data" / "research" / "binance_store_certification_20261024"
_LOCAL = threading.local()


def session() -> requests.Session:
    """One keep-alive session per worker thread; the archive is public, so it never goes through the proxy."""
    sess = getattr(_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.trust_env = False
        sess.mount("https://", requests.adapters.HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=0))
        _LOCAL.session = sess
    return sess


def fetch_zip(path: str, attempts: int = 4) -> Optional[bytes]:
    """The zip's bytes, or None when the archive has no such file (404)."""
    for attempt in range(attempts):
        try:
            response = session().get(f"{ARCHIVE}/{path}", timeout=(8, 40))
            if response.status_code == 404:
                return None
            if response.status_code == 200:
                return response.content
            raise RuntimeError(f"HTTP {response.status_code}")
        except Exception as exc:  # noqa: BLE001 - retried, then reported
            if attempt == attempts - 1:
                raise RuntimeError(f"{path}: {type(exc).__name__}: {str(exc)[:100]}") from exc
            time.sleep(1.5 * (attempt + 1))
    return None


def parse_archive_csv(payload: bytes) -> pd.DataFrame:
    """One archive zip -> frame indexed by naive UTC open time (headers and ms/us stamps both occur)."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        name = archive.namelist()[0]
        raw = pd.read_csv(io.BytesIO(archive.read(name)), header=None, usecols=range(6), dtype=str)
    values = raw.apply(pd.to_numeric, errors="coerce").dropna()  # drops the header line of newer files
    if values.empty:
        return pd.DataFrame(columns=OHLCV, index=pd.DatetimeIndex([], name="timestamp"))
    stamp = values.iloc[:, 0].astype("int64").to_numpy()
    stamp = np.where(stamp > 10**14, stamp // 1000, stamp)  # spot switched to microseconds on 2025-01-01
    out = values.iloc[:, 1:6].astype(float)
    out.columns = OHLCV
    out.index = pd.DatetimeIndex(pd.to_datetime(stamp, unit="ms"), name="timestamp")
    return out


def archive_paths(market: str, raw_symbol: str, timeframe: str, first: datetime, now: datetime) -> List[Tuple[str, str]]:
    """(label, path) of every archive file that can hold bars from `first` to yesterday.

    Complete months come as monthly zips (with a per-day fallback if one is not published yet); the current
    month comes as daily zips up to yesterday (the futures archive trails by a day or two; a gap is harmless
    because the guard only needs the stored rows covered).
    """
    root = f"futures/um" if market == "futures" else "spot"
    today = now.date()
    paths: List[Tuple[str, str]] = []
    year, month = first.year, first.month
    while (year, month) <= (today.year, today.month):
        tag = f"{year:04d}-{month:02d}"
        if (year, month) < (today.year, today.month):
            paths.append((f"M{tag}", f"{root}/monthly/klines/{raw_symbol}/{timeframe}/{raw_symbol}-{timeframe}-{tag}.zip"))
        else:
            for day in range(1, today.day):
                stamp = f"{tag}-{day:02d}"
                paths.append((f"D{stamp}", f"{root}/daily/klines/{raw_symbol}/{timeframe}/{raw_symbol}-{timeframe}-{stamp}.zip"))
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return paths


def official_frame(market: str, raw_symbol: str, timeframe: str, first: datetime, now: datetime) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    frames: List[pd.DataFrame] = []
    missing_files: List[str] = []
    for label, path in archive_paths(market, raw_symbol, timeframe, first, now):
        payload = fetch_zip(path)
        if payload is None and label.startswith("M"):  # month not published yet: take its days
            tag = label[1:]
            start = datetime.strptime(tag + "-01", "%Y-%m-%d")
            nxt = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
            root = "futures/um" if market == "futures" else "spot"
            day = start
            while day < min(nxt, datetime(now.year, now.month, now.day)):
                stamp = day.strftime("%Y-%m-%d")
                sub = fetch_zip(f"{root}/daily/klines/{raw_symbol}/{timeframe}/{raw_symbol}-{timeframe}-{stamp}.zip")
                if sub is not None:
                    frames.append(parse_archive_csv(sub))
                day += timedelta(days=1)
            continue
        if payload is None:
            missing_files.append(label)
            continue
        frames.append(parse_archive_csv(payload))
    if not frames:
        return pd.DataFrame(columns=OHLCV, index=pd.DatetimeIndex([], name="timestamp")), {"dropped_invalid": 0, "missing_files": len(missing_files), "missing_sample": missing_files[:3]}
    frame = pd.concat(frames)
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    o, h, l, c, v = (frame[name].to_numpy() for name in OHLCV)
    good = (np.isfinite(frame[OHLCV].to_numpy()).all(axis=1) & (np.minimum.reduce([o, h, l, c]) > 0) & (v >= 0)
            & (h >= np.maximum.reduce([o, l, c])) & (l <= np.minimum.reduce([o, h, c])))
    aligned = (frame.index.astype("int64") // 1_000_000) % STEP_MS[timeframe] == 0
    good &= aligned
    dropped = int((~good).sum())  # same bar rules as maintain_all_binance_market_data.validate_rows
    return frame[good], {"dropped_invalid": dropped, "missing_files": len(missing_files), "missing_sample": missing_files[:3]}


def marker_status(path: Path) -> str:
    """"verified_utc", "unmarked", or the repair project's partial status.

    pandas keeps DataFrame.attrs in PANDAS_ATTRS (to_parquet) or in the "attributes" of the pandas JSON
    (pa.Table.from_pandas); the repair tooling and the writers use different ones.
    """
    meta = pq.read_schema(path).metadata or {}
    attrs: Dict[str, Any] = {}
    if meta.get(b"PANDAS_ATTRS"):
        attrs.update(json.loads(meta[b"PANDAS_ATTRS"]))
    if meta.get(b"pandas"):
        attrs.update(json.loads(meta[b"pandas"]).get("attributes") or {})
    if not attrs:
        return "unmarked"
    if attrs.get("cts_timestamp_timezone") == "UTC" and attrs.get("cts_timestamp_schema") == 1:
        return "verified_utc"
    return str(attrs.get("cts_timestamp_status", "attrs-other"))


MAX_API_CONFIRM_ROWS = 400  # more disagreements than this is not "a few revised bars": leave the file to a human


def disagreements(existing: pd.DataFrame, official: pd.DataFrame) -> pd.DatetimeIndex:
    """Stored rows the archive lacks or holds with other values (Binance restates volumes and keeps outage fillers)."""
    stored = existing[OHLCV]
    missing = stored.index.difference(official.index)
    common = stored.index.intersection(official.index)
    differs = common[(stored.loc[common].to_numpy() != official.loc[common, OHLCV].to_numpy()).any(axis=1)]
    return missing.union(differs)


def confirm_with_api(maint, key, stamps: pd.DatetimeIndex, limiter) -> Dict[pd.Timestamp, List[float]]:
    """The live API's bar at each given open time (one request per cluster of neighbouring bars)."""
    step = STEP_MS[key.timeframe]
    ms = sorted(int(stamp.value // 1_000_000) for stamp in stamps)
    windows: List[List[int]] = []
    for value in ms:
        if windows and value - windows[-1][1] <= 48 * step:
            windows[-1][1] = value
        else:
            windows.append([value, value])
    found: Dict[pd.Timestamp, List[float]] = {}
    for first, last in windows:
        for row in maint.fetch_window(key, maint.FetchWindow(first, last, "confirm"), limiter):
            found[pd.Timestamp(int(row[0]), unit="ms")] = [float(row[i]) for i in range(1, 6)]
    return found


def fingerprint(frame: pd.DataFrame) -> Tuple[int, str, int]:
    return len(frame), str(frame.index.max()), int(pd.util.hash_pandas_object(frame, index=True).sum() % (1 << 63))


def certify_file(maint, result: Dict[str, Any], target: Path, key, load_official, limiter, dry_run: bool,
                 part_day: Optional[str] = None) -> Dict[str, Any]:
    """Judge one parquet against the official bars and, unless dry-run, certify and extend it.

    `load_official(existing)` returns (official frame, info).  `part_day` marks a daily partition file, whose rows
    must all fall on that UTC day.
    """
    if not target.exists():
        return {**result, "status": "no_local_file"}
    marker = marker_status(target)
    if marker == "verified_utc":
        return {**result, "status": "already_verified"}
    if marker != "unmarked":
        return {**result, "status": "skipped_partially_repaired", "marker": marker}  # the timezone-repair cohort
    started = time.time()
    try:
        existing = pd.read_parquet(target)
    except Exception as exc:  # noqa: BLE001
        return {**result, "status": "error", "error": f"read: {type(exc).__name__}: {str(exc)[:100]}"}
    if existing.empty:
        return {**result, "status": "empty_local_file"}
    try:
        seen = fingerprint(existing)
        official, info = load_official(existing)
        result.update(info)
        if official.empty:
            return {**result, "status": "archive_empty"}
        need = disagreements(existing, official)
        last_stamp = existing.index.max()
        if last_stamp in need and last_stamp in official.index:
            stored_bar, final_bar = existing.loc[last_stamp, OHLCV], official.loc[last_stamp, OHLCV]
            # The newest stored candle may have been written while it was still forming: same open, and the final
            # candle contains it.  The ordinary writer overwrites such a bar with the closed one on the next refresh.
            placeholder = (stored_bar["volume"] == 0 and stored_bar["open"] == stored_bar["high"]
                           == stored_bar["low"] == stored_bar["close"])  # opened, no trade yet
            if placeholder or (stored_bar["open"] == final_bar["open"] and final_bar["high"] >= stored_bar["high"]
                               and final_bar["low"] <= stored_bar["low"] and final_bar["volume"] >= stored_bar["volume"]):
                existing = existing.copy()
                existing.loc[last_stamp, OHLCV] = final_bar.to_numpy()
                need = need.drop(last_stamp)
                result["completed_forming_bar"] = str(last_stamp)
        result["disagreeing_rows"] = int(len(need))
        incoming_rows = official
        if len(need):
            if len(need) > MAX_API_CONFIRM_ROWS:
                return {**result, "status": "refused", "reason": "too_many_disagreements", "first": str(need.min()),
                        "last": str(need.max()), "stored_rows": int(len(existing))}
            live = confirm_with_api(maint, key, need, limiter)
            stored = existing[OHLCV]
            unconfirmed = [stamp for stamp in need if stamp not in live or live[stamp] != [float(x) for x in stored.loc[stamp]]]
            if unconfirmed:
                return {**result, "status": "refused", "reason": "live_api_disagrees_with_stored_rows",
                        "unconfirmed": len(unconfirmed), "first": str(min(unconfirmed)), "last": str(max(unconfirmed)),
                        "stored_rows": int(len(existing))}
            incoming_rows = official.copy()
            for stamp in need:  # archive and stored disagree; the live API agrees with stored: it is the evidence there
                incoming_rows.loc[stamp, OHLCV] = live[stamp]
            incoming_rows = incoming_rows.sort_index()
            result["confirmed_by_live_api"] = int(len(need))
        # Never add an outage filler (flat bar, no volume) that the store did not already hold.
        extra = ~incoming_rows.index.isin(existing.index)
        flat = (incoming_rows["volume"] == 0) & (incoming_rows["open"] == incoming_rows["high"]) \
            & (incoming_rows["high"] == incoming_rows["low"]) & (incoming_rows["low"] == incoming_rows["close"])
        incoming_rows = incoming_rows[~(extra & flat.to_numpy())]
        incoming = prepare_utc_kline_frame(incoming_rows, origin=ORIGIN, timestamp_timezone="UTC")
        verified = _heal_local_existing_against_utc(existing, incoming)  # the shared guard, unchanged
    except ValueError as exc:
        return {**result, "status": "refused", "reason": "guard", "guard": str(exc)[:200]}
    except Exception as exc:  # noqa: BLE001 - network, parsing: the file is untouched
        return {**result, "status": "error", "error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    combined = pd.concat([verified, incoming])
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    if not existing.index.isin(combined.index).all() or len(combined) < len(existing):
        return {**result, "status": "error", "error": "merge would lose stored rows; not written"}
    if part_day and any(combined.index.strftime("%Y-%m-%d") != part_day):
        return {**result, "status": "refused", "reason": "rows_outside_partition_day"}
    result.update(rows_before=len(existing), rows_after=len(combined), added=len(combined) - len(existing),
                  last_bar=str(combined.index.max()), seconds=round(time.time() - started, 1))
    if dry_run:
        return {**result, "status": "would_certify"}
    combined.attrs = {**verified.attrs, **incoming.attrs}
    assert _has_utc_timestamp_metadata(combined)
    try:
        with parquet_partition_lock(target, timeout_seconds=90):
            if fingerprint(pd.read_parquet(target)) != seen:  # a writer got in while we were downloading
                return {**result, "status": "changed_during_run"}
            temp = target.with_name(f"{target.name}.{os.getpid()}.{uuid4().hex}.tmp")
            try:
                pq.write_table(pa.Table.from_pandas(combined), str(temp), compression="zstd", compression_level=6)
                if pq.read_table(temp, columns=["timestamp"]).num_rows != len(combined):
                    raise RuntimeError("written row count differs")
                for attempt in range(6):  # Windows refuses the swap while a reader still has the file open
                    try:
                        os.replace(temp, target)
                        break
                    except PermissionError:
                        if attempt == 5:
                            raise
                        time.sleep(1.0 + attempt)
            finally:
                temp.unlink(missing_ok=True)
    except Exception as exc:  # noqa: BLE001 - lock timeout, disk: the file is untouched
        return {**result, "status": "error", "error": f"{type(exc).__name__}: {str(exc)[:160]}"}
    return {**result, "status": "certified"}


def certify_series(maint, namespace: str, market: str, symbol: str, raw_symbol: str, timeframe: str, now: datetime,
                   storage_root: Path, limiter, dry_run: bool) -> Dict[str, Any]:
    """The main <timeframe>.parquet of one series, against the full archive history."""
    result: Dict[str, Any] = {"namespace": namespace, "symbol": symbol, "timeframe": timeframe}
    target = canonical_symbol_dir(storage_root, namespace, symbol) / f"{timeframe}.parquet"
    key = maint.SeriesKey(namespace, market, symbol, timeframe)
    return certify_file(maint, result, target, key,
                        lambda existing: official_frame(market, raw_symbol, timeframe, existing.index.min().to_pydatetime(), now),
                        limiter, dry_run)


def day_frame(raw_symbol: str, timeframe: str, day: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """One UTC day of spot bars from the archive's daily zip (validated like the full-history path)."""
    payload = fetch_zip(f"spot/daily/klines/{raw_symbol}/{timeframe}/{raw_symbol}-{timeframe}-{day}.zip")
    if payload is None:
        return pd.DataFrame(columns=OHLCV, index=pd.DatetimeIndex([], name="timestamp")), {"archive_day": "missing"}
    frame = parse_archive_csv(payload)
    o, h, l, c, v = (frame[name].to_numpy() for name in OHLCV)
    good = (np.isfinite(frame[OHLCV].to_numpy()).all(axis=1) & (np.minimum.reduce([o, h, l, c]) > 0) & (v >= 0)
            & (h >= np.maximum.reduce([o, l, c])) & (l <= np.minimum.reduce([o, h, c])))
    good &= (frame.index.astype("int64") // 1_000_000) % STEP_MS[timeframe] == 0
    return frame[good], {"dropped_invalid": int((~good).sum())}


def certify_partition(maint, symbol_dir: Path, timeframe: str, part: Path, limiter, dry_run: bool) -> Dict[str, Any]:
    base = symbol_dir.name.rsplit("_", 1)[0]
    symbol = f"{base}/USDT"
    result: Dict[str, Any] = {"namespace": "binance", "symbol": symbol, "timeframe": timeframe, "partition": part.stem}
    key = maint.SeriesKey("binance", "spot", symbol, timeframe)
    raw = f"{base}USDT"
    return certify_file(maint, result, part, key, lambda existing: day_frame(raw, timeframe, part.stem), limiter, dry_run,
                        part_day=part.stem)


PARTITION_TIMEFRAMES = ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d")


def partition_targets(storage_root: Path, today: datetime, since: str, only: Optional[set],
                      back_days: int = 3) -> List[Tuple[Path, str, Path]]:
    """The newest partitions (the last `back_days` days on disk) of every spot series whose newest day is in
    [since, yesterday].

    These are the files that block the hourly refresh: it re-fetches up to two days before the newest stored bar
    and merges them into the day files on disk, and one unmarked day it cannot verify makes the writer refuse the
    whole batch (every later day with it).
    """
    yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    found: List[Tuple[Path, str, Path]] = []
    for symbol_dir in sorted((storage_root / "binance").iterdir()):
        if not symbol_dir.is_dir() or not symbol_dir.name.endswith("_USDT"):
            continue
        if only and symbol_dir.name.rsplit("_", 1)[0].upper() not in only:
            continue
        for timeframe in PARTITION_TIMEFRAMES:
            files = sorted((symbol_dir / f"{timeframe}_parts").glob("*.parquet"))
            if not files or not (since <= files[-1].stem <= yesterday):
                continue
            newest = datetime.strptime(files[-1].stem, "%Y-%m-%d")
            for part in files:
                if (newest - datetime.strptime(part.stem, "%Y-%m-%d")).days <= back_days and part.stem <= yesterday:
                    found.append((symbol_dir, timeframe, part))
    return found


def build_targets(limiter, args: argparse.Namespace) -> List[Tuple[str, str, str, str]]:
    import collect_binance_history as collect  # noqa: WPS433 - needs the proxy-aware exchangeInfo helpers

    perps, pairs = collect.universe(limiter)
    only = {s.upper() for s in args.only} if args.only else None
    targets: List[Tuple[str, str, str, str]] = []
    for market, namespace, items in (("futures", "binance_futures", perps), ("spot", "binance", pairs)):
        if args.market not in ("all", market):
            continue
        for item in items:
            if item["status"] != "TRADING":
                continue
            if only and item["symbol"].split("/")[0].upper() not in only:
                continue
            targets.append((namespace, market, item["symbol"], item["raw"]))
    return targets


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="judge every series with the guard, write nothing")
    parser.add_argument("--market", choices=("all", "futures", "spot"), default="all")
    parser.add_argument("--only", nargs="*", help="base assets only, e.g. BTC ETH 1000PEPE")
    parser.add_argument("--timeframes", nargs="*", default=list(TIMEFRAMES))
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--limit", type=int, default=0, help="stop after this many series (pilot runs)")
    parser.add_argument("--partitions", action="store_true",
                        help="certify the newest daily partition of each spot series (unblocks the hourly refresh)")
    parser.add_argument("--since", default="2026-10-10", help="--partitions: oldest newest-day to consider")
    args = parser.parse_args(argv)

    import maintain_all_binance_market_data as maint
    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)  # only the two exchangeInfo calls use it; archive downloads are direct
    limiter = maint.RequestRateLimiter(2.0)  # live API: only the few disagreeing bars and exchangeInfo
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    storage_root = Path(maint.data_storage.storage_path)
    if args.partitions:
        only = {name.upper() for name in args.only} if args.only else None
        parts = partition_targets(storage_root, datetime.now(timezone.utc).replace(tzinfo=None), args.since, only)
        if args.timeframes != list(TIMEFRAMES):
            parts = [item for item in parts if item[1] in args.timeframes]
        if args.limit:
            parts = parts[: args.limit]
        print(f"{len(parts)} newest partitions; {'DRY RUN' if args.dry_run else 'writing'}; {args.workers} workers", flush=True)
        work = [(certify_partition, (maint, symbol_dir, timeframe, part, limiter, args.dry_run)) for symbol_dir, timeframe, part in parts]
    else:
        targets = build_targets(limiter, args)
        jobs = [(ns, mk, sym, raw, tf) for ns, mk, sym, raw in targets for tf in args.timeframes]
        if args.limit:
            jobs = jobs[: args.limit]
        print(f"{len(targets)} trading symbols, {len(jobs)} series; {'DRY RUN' if args.dry_run else 'writing'}; "
              f"{args.workers} workers", flush=True)
        work = [(certify_series, (maint, ns, mk, sym, raw, tf, now, storage_root, limiter, args.dry_run)) for ns, mk, sym, raw, tf in jobs]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S") + ("_parts" if args.partitions else "") + ("_dry" if args.dry_run else "")
    journal = OUT_DIR / f"run_{stamp}.jsonl"
    counts: Dict[str, int] = {}
    began = time.time()
    with journal.open("a", encoding="utf-8") as sink, ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(fn, *call) for fn, call in work]
        for done, future in enumerate(as_completed(futures), 1):
            item = future.result()
            counts[item["status"]] = counts.get(item["status"], 0) + 1
            sink.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
            sink.flush()
            if done % 50 == 0 or done == len(futures):
                print(f"  {done}/{len(futures)} {dict(sorted(counts.items()))} {time.time() - began:.0f}s", flush=True)
    summary = {"run": stamp, "dry_run": args.dry_run, "series": len(work), "counts": counts, "journal": str(journal),
               "seconds": round(time.time() - began)}
    (OUT_DIR / f"summary_{stamp}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 1 if counts.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
