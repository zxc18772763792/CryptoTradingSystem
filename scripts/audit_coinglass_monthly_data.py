"""Audit the monthly composite's inputs; retain exact gaps and stage compatible local history.

No forward trades or frozen universe files are changed. --fill-klines downloads
only missing closed hourly bars for the last 120 days. Coinglass seeding is staged
separately so the managed daily collector remains the sole live writer.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
import gzip
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from core.research import coinglass_daily_store as store
from core.research import coinglass_monthly_universe as monthly
from core.data import data_storage
from core.data.data_storage import _parquet_index_column
from core.data.path_utils import candidate_symbol_dirs
from core.data.kline_partitions import list_part_files, part_read_lock

DAY = 86400000


def valid_row(dataset, row):
    def number(key):
        try:
            value = float(row[key])
            return value if np.isfinite(value) else None
        except (TypeError, KeyError, ValueError):
            return None
    if dataset == "funding":
        return number("close") is not None
    if dataset == "oi":
        value = number("close")
        return value is not None and value > 0
    if dataset in store.SNAPSHOT_AT_STAMP:
        prefix = "global_account" if dataset == "global_ratio" else "top_position"
        long, short = number(prefix + "_long_percent"), number(prefix + "_short_percent")
        ratio = number(prefix + "_long_short_ratio")
        return (long is not None and short is not None and long > 0 and short > 0) or (ratio is not None and ratio > 0)
    return all(number(field) is not None and number(field) >= 0 for field in store.DATASETS[dataset][2])


def cg_profile(coin, day):
    out = {}
    for dataset in store.DATASETS:
        end = store.needed_stamp(dataset, day)
        expected = set(range(end - 59 * DAY, end + 1, DAY))
        rows = coin.get(dataset) or []
        by_stamp = {int(r["time"]): r for r in rows if r.get("time") is not None}
        good = {t for t, r in by_stamp.items() if valid_row(dataset, r)}
        missing = sorted(expected - good)
        out[dataset] = {"valid_days": len(expected & good), "expected_days": 60,
                        "missing_stamps": missing, "required_31_missing": sum(t >= end - 30 * DAY for t in missing),
                        "latest_valid": max(good) if good else None,
                        "invalid_rows": sum(not valid_row(dataset, r) for t, r in by_stamp.items() if t in expected),
                        "duplicate_rows": len(rows) - len(by_stamp)}
    return out


def cached_coin(base, perp, day):
    """Exact same endpoints/parameters; never use other interval or alias data."""
    coin = {"base": base, "perp": perp}
    sources = [ROOT / "data/research/coinglass_positioning/raw" / f"{perp}.json",
               ROOT / "data/research/coinglass_wide/raw" / f"{perp}.json",
               ROOT / "data/research/coinglass_history/raw" / f"{perp}.json.gz"]
    used = []
    for path in sources:
        if not path.exists():
            continue
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as stream:
            raw = json.load(stream)
        name = raw.get("coin") or raw.get("base")
        if name and name != base:
            continue
        datasets = raw.get("datasets", raw)
        for dataset in store.DATASETS:
            key = "top_position" if dataset == "top_ratio" and "top_position" in datasets else dataset
            rows = [r for r in datasets.get(key, []) if valid_row(dataset, r)]
            if rows:
                store.merge(coin, dataset, rows, store.needed_stamp(dataset, day))
        used.append(str(path.relative_to(ROOT)))
    # Prefer observations already stored by the live collector, but do not
    # replace valid cached history with null-valued live rows.
    live = store.load_coin(perp)
    for dataset in store.DATASETS:
        store.merge(coin, dataset, [r for r in live.get(dataset, []) if valid_row(dataset, r)], store.needed_stamp(dataset, day))
    return coin, used


def hourly_profile(namespace, symbol, start, end):
    wanted = pd.date_range(start, end, freq="h", tz="UTC")
    good, all_stamps = set(), set()
    invalid = duplicates = 0
    files, unverified = [], []
    for directory in candidate_symbol_dirs(data_storage.storage_path, namespace, symbol):
        with part_read_lock(directory / "1h_parts"):
            paths = ([directory / "1h.parquet"] if (directory / "1h.parquet").exists() else [])
            paths += list_part_files(directory / "1h_parts")
            for path in paths:
                index = _parquet_index_column(path)
                if index is None:
                    raise ValueError(f"no timestamp index: {path}")
                files.append(str(path))
                with pq.ParquetFile(path) as parquet:
                    metadata = parquet.schema_arrow.metadata or {}
                    pandas_meta = json.loads(metadata.get(b"pandas", b"{}"))
                    attrs = {**pandas_meta.get("attributes", {}),
                             **json.loads(metadata.get(b"PANDAS_ATTRS", b"{}"))}
                    if attrs.get("cts_timestamp_timezone") != "UTC":
                        unverified.append(str(path))
                    for batch in parquet.iter_batches(batch_size=4096, columns=[index, "close"], use_threads=False):
                        stamps = pd.DatetimeIndex(pd.to_datetime(batch.column(index).to_pandas(), utc=True))
                        close = pd.to_numeric(batch.column("close").to_pandas(), errors="coerce")
                        in_range = (stamps >= wanted[0]) & (stamps <= wanted[-1])
                        valid = in_range & (stamps == stamps.floor("h")) & np.isfinite(close) & (close > 0)
                        values = stamps[in_range].asi8.tolist()
                        duplicates += len(values) - len(set(values)) + len(all_stamps & set(values))
                        all_stamps.update(values)
                        good.update(stamps[valid].asi8.tolist())
                        invalid += int((in_range & ~valid).sum())
    missing = [t.isoformat() for t in wanted if t.value not in good]
    return {"namespace": namespace, "symbol": symbol, "timeframe": "1h", "start": wanted[0].isoformat(),
            "end": wanted[-1].isoformat(), "expected": len(wanted), "valid": len(good), "missing": missing,
            "invalid_rows": invalid, "overlapping_rows": duplicates,
            "unverified_timestamp_files": unverified, "files": len(files)}


def summarize(report):
    cg = report["coinglass"]
    return {"coins": report["coins"], "hourly_series": len(report["hourly"]),
            "hourly_complete": sum(not r["missing"] for r in report["hourly"]),
            "hourly_missing_bars": sum(len(r["missing"]) for r in report["hourly"]),
            "cg_live_complete_60d": sum(all(not d["missing_stamps"] for d in c["live"].values()) for c in cg),
            "cg_live_complete_31d": sum(all(not d["required_31_missing"] for d in c["live"].values()) for c in cg),
            "cg_staged_complete_60d": sum(all(not d["missing_stamps"] for d in c["staged"].values()) for c in cg),
            "cg_staged_complete_31d": sum(all(not d["required_31_missing"] for d in c["staged"].values()) for c in cg),
            "cg_missing_by_dataset": {d: sum(len(c["staged"][d]["missing_stamps"]) for c in cg) for d in store.DATASETS}}


async def fill_hourly(report, directory):
    import audit_fill_klines as audit
    import maintain_all_binance_market_data as maint
    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)
    # Append authoritative UTC observations in the normal partition store.
    # The compact-file maintainer refuses partial merges into an old unmarked
    # monolith; do not relabel that historical file just to append missing bars.
    async def save_parts(key, klines):
        return await data_storage.save_klines_to_parquet(
            klines, key.namespace, key.symbol, key.timeframe,
            market_type="spot" if key.market_type == "spot" else "future")
    maint.save_klines_compact = save_parts
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, level="WARNING")
    limiter = maint.RequestRateLimiter(2.0)
    spot_status, futures_status = await asyncio.to_thread(maint.load_market_statuses, limiter)
    work = []
    for row in report["hourly"]:
        if not row["missing"]:
            continue
        market = "spot" if row["namespace"] == "binance" else "futures"
        key = maint.SeriesKey(row["namespace"], market, row["symbol"], "1h")
        status = (spot_status if market == "spot" else futures_status).get(maint.raw_symbol(key.symbol))
        row["source_status"] = status or "NOT_LISTED"
        if status != "TRADING":
            row["repair_status"] = "inactive_source_not_fabricated"
            continue
        missing = pd.DatetimeIndex(pd.to_datetime(row["missing"], utc=True)).tz_localize(None)
        if missing[0] == pd.Timestamp(row["start"]).tz_localize(None):
            first = await asyncio.to_thread(audit.first_source_bar, key, limiter)
            if first:
                row["source_first_bar"] = first.isoformat() + "+00:00"
                row["prelisting_bars"] = int((missing < pd.Timestamp(first)).sum())
                missing = missing[missing >= pd.Timestamp(first)]
        if not len(missing):
            row["repair_status"] = "prelisting_only"
            continue
        # Fetch only absent contiguous runs; a one-hour hole never rewrites a
        # year of local history. Wider gaps paginate through the existing client.
        windows = []
        for stamp in missing:
            if windows and stamp - windows[-1][1] <= pd.Timedelta(hours=1):
                windows[-1][1] = stamp
            else:
                windows.append([stamp, stamp])
        work.append({"series": key, "windows": [(a.to_pydatetime(), b.to_pydatetime()) for a, b in windows]})
    print(f"hourly repair: {len(work)} series with source-available gaps", flush=True)
    filled = await audit.fill(work, limiter, workers=2)
    result = {"downloaded_bars": sum(r.get("downloaded", 0) for r in filled),
              "errors": audit._jsonable([r for r in filled if r.get("fill_error")]),
              "series": audit._jsonable(filled), "source_annotations": [r for r in report["hourly"] if r.get("source_status")]}
    (directory / "hourly_fill.json").write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"downloaded_bars": result["downloaded_bars"], "errors": len(result["errors"])}), flush=True)


async def run(args):
    directory = ROOT / "reports/coinglass_monthly_data_20261031"
    if args.apply_stage:
        import hashlib
        import shutil
        import socket
        try:
            with socket.create_connection(("127.0.0.1", 8000), timeout=2):
                raise RuntimeError("Stop the managed web daily writer before applying staged history")
        except (ConnectionRefusedError, TimeoutError, OSError):
            pass
        before = json.loads((directory / "before.json").read_text(encoding="utf-8"))
        if before["snapshot"] != monthly.load_snapshot()["id"]:
            raise RuntimeError("monthly universe changed since the audit; audit again")
        day = datetime.fromisoformat(before["coinglass_day"])
        backup = directory / "daily_before_seed"
        backup.mkdir(exist_ok=True)
        changes = []
        for path in sorted((directory / "staged_daily").glob("*.json")):
            perp = path.stem
            staged = json.loads(path.read_text(encoding="utf-8"))
            current = store.load_coin(perp)
            original = json.dumps(current, sort_keys=True)
            # Staged historical rows are used only for missing/invalid live
            # observations; never replace a valid current observation.
            merged = dict(current) if current else {"base": staged["base"], "perp": perp}
            added = 0
            for dataset in store.DATASETS:
                gaps = set(store.history_gaps(merged, dataset, day))
                rows = [r for r in staged.get(dataset, []) if int(r["time"]) in gaps and valid_row(dataset, r)]
                if rows:
                    added += len(rows)
                    store.merge(merged, dataset, rows, store.needed_stamp(dataset, day))
            if added:
                live_path = store.coin_path(perp)
                if live_path.exists() and not (backup / path.name).exists():
                    shutil.copy2(live_path, backup / path.name)
                store.save_coin(perp, merged)
                changes.append({"perp": perp, "added_observations": added,
                                "before_sha256": hashlib.sha256(original.encode()).hexdigest()})
        result = {"applied_at": datetime.now(timezone.utc).isoformat(), "changes": changes,
                  "coins_updated": len(changes), "observations_added": sum(r["added_observations"] for r in changes)}
        (directory / "seed_applied.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
        print(json.dumps({k:v for k,v in result.items() if k != "changes"}), flush=True)
        return
    if args.fill_klines:
        report = json.loads((directory / args.report_name).read_text(encoding="utf-8"))
        await fill_hourly(report, directory)
        return
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    start = end - timedelta(hours=120 * 24 - 1)
    day = store.last_closed_day(now)
    snapshot = monthly.load_snapshot(now)
    if not snapshot:
        raise ValueError("current monthly snapshot missing")
    directory.mkdir(parents=True, exist_ok=True)
    stage = directory / "staged_daily"
    stage.mkdir(exist_ok=True)
    report = {"checked_at": now.isoformat(), "hourly_start": start.isoformat(), "hourly_end": end.isoformat(),
              "coinglass_day": day.isoformat(), "snapshot": snapshot["id"], "coins": len(snapshot["members"]),
              "hourly": [], "coinglass": []}
    for i, (base, perp) in enumerate(snapshot["members"].items(), 1):
        for ns, symbol in (("binance", f"{base}/USDT"), ("binance_futures", f"{perp[:-4]}/USDT")):
            row = await asyncio.to_thread(hourly_profile, ns, symbol, start, end)
            report["hourly"].append(row)
        coin, sources = await asyncio.to_thread(cached_coin, base, perp, day)
        (stage / f"{perp}.json").write_text(json.dumps(coin), encoding="utf-8")
        report["coinglass"].append({"base": base, "perp": perp, "sources": sources,
                                   "live": cg_profile(store.load_coin(perp), day), "staged": cg_profile(coin, day)})
        if i % 50 == 0:
            print(f"audited {i}/{len(snapshot['members'])}", flush=True)
    report["summary"] = summarize(report)
    path = directory / args.report_name
    path.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(report["summary"], indent=2), flush=True)
    print(f"report: {path}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-name", default="before.json")
    parser.add_argument("--fill-klines", action="store_true")
    parser.add_argument("--apply-stage", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args))
