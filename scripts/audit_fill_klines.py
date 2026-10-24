"""Audit, and with --fill complete, the local Binance kline store for the wide Coinglass universe.

Why (2026-10-12): the 131-coin research universe was frozen from the coins that happened to have
120 days of local spot 1h bars, so 37 of the 100 most-traded perps (HYPE among them) were left
out, and the futures namespace held only the last ~300 4h bars per perp
(scripts/maintain_all_binance_market_data.py keeps futures that short and only checks a series'
first and last bar). The user asked to check the local database and fill what is missing.

Target per coin of data/research/coinglass_wide/universe.json (234 coins):
  * its USD-M perp in the binance_futures namespace, and
  * its spot USDT market in the binance namespace where Binance lists one,
  at 1h, 4h and 1d, from 2024-01-01 (or the first bar Binance has) to the last closed bar.
"Missing" is every bar of that grid absent locally, interior gaps included. Bars Binance never
published (outages, a halted market) stay missing after --fill and are reported as source gaps.
Reads parquet indexes only; --fill merges new bars through the general maintainer's atomic,
locked writer and never deletes or rewrites older bars.

    python scripts/audit_fill_klines.py              # audit (no downloads): per series coverage and gaps
    python scripts/audit_fill_klines.py --fill       # download the missing windows (~1000 bars per request)
    python scripts/audit_fill_klines.py --tail-only  # daily top-up: only bars after each series' last one
                                                     # (reads edge files' statistics, a few minutes)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

import maintain_all_binance_market_data as maint  # noqa: E402
from core.data.path_utils import candidate_symbol_dirs  # noqa: E402

UNIVERSE_PATH = PROJECT_ROOT / "data" / "research" / "coinglass_wide" / "universe.json"
REPORT_PATH = PROJECT_ROOT / "data" / "research" / "local_klines_audit.json"
START = datetime(2024, 1, 1)  # UTC, naive like the store's index
TIMEFRAMES = ("1h", "4h", "1d")
MERGE_GAP_BARS = 500  # missing runs closer than this are fetched as one window
BARS_PER_REQUEST = 1000
BYTES_PER_BAR = 115  # uncompressed JSON row, for the traffic estimate


# ---------------------------------------------------------------------------------------- local
def _index_column(path: Path) -> Optional[str]:
    schema = pq.read_schema(path)
    meta = (schema.metadata or {}).get(b"pandas")
    if meta:
        try:
            for column in json.loads(meta).get("index_columns") or []:
                if isinstance(column, str) and column in schema.names:
                    return column
        except (TypeError, ValueError, UnicodeDecodeError):
            pass
    for candidate in ("timestamp", "__index_level_0__"):
        if candidate in schema.names:
            return candidate
    return None


def local_stamps(namespace: str, symbol: str, timeframe: str) -> pd.DatetimeIndex:
    """Bar open times held locally (legacy file and daily parts), UTC naive; reads the index only."""
    stamps: List[pd.DatetimeIndex] = []
    storage = Path(maint.data_storage.storage_path)
    for root in candidate_symbol_dirs(storage, namespace, symbol):
        files = [root / f"{timeframe}.parquet"]
        parts = root / f"{timeframe}_parts"
        if parts.exists():
            files += sorted(parts.glob("*.parquet"))
        for path in files:
            if not path.exists():
                continue
            try:
                column = _index_column(path)
                if not column:
                    continue
                values = pq.read_table(path, columns=[column]).column(column).to_pandas()
                index = pd.DatetimeIndex(pd.to_datetime(values, errors="coerce", utc=True)).dropna().tz_localize(None)
                stamps.append(index)
            except Exception as exc:  # noqa: BLE001 - an unreadable file counts as absent bars
                print(f"  unreadable {path}: {type(exc).__name__}: {str(exc)[:80]}", flush=True)
    if not stamps:
        return pd.DatetimeIndex([])
    return stamps[0].append(stamps[1:]).unique().sort_values() if len(stamps) > 1 else stamps[0].unique().sort_values()


# ---------------------------------------------------------------------------------------- plan
def series_keys(universe: Dict[str, Any], spot_status: Dict[str, str], futures_status: Dict[str, str]) -> List[maint.SeriesKey]:
    coins = {**universe["known"], **universe["held_out"]}  # coin -> perp
    keys: List[maint.SeriesKey] = []
    for coin, perp in sorted(coins.items()):
        if futures_status.get(perp) == "TRADING":
            for tf in TIMEFRAMES:
                keys.append(maint.SeriesKey(maint.FUTURES_NAMESPACE, "futures", f"{perp[:-4]}/USDT", tf))
        spot = next((s for s in (f"{coin}USDT", perp) if spot_status.get(s) == "TRADING"), None)
        if spot:
            for tf in TIMEFRAMES:
                keys.append(maint.SeriesKey(maint.SPOT_NAMESPACE, "spot", f"{spot[:-4]}/USDT", tf))
    return keys


def first_source_bar(key: maint.SeriesKey, limiter) -> Optional[datetime]:
    path = "/api/v3/klines" if key.market_type == "spot" else "/fapi/v1/klines"
    hosts = maint.SPOT_HOSTS if key.market_type == "spot" else maint.FUTURES_HOSTS
    payload = maint.request_json(hosts, path, params={"symbol": maint.raw_symbol(key.symbol), "interval": key.timeframe,
                                                     "startTime": 0, "limit": 1}, limiter=limiter)
    return datetime.fromtimestamp(int(payload[0][0]) / 1000, tz=timezone.utc).replace(tzinfo=None) if payload else None


def missing_windows(have: pd.DatetimeIndex, start: datetime, end: datetime, timeframe: str) -> Tuple[int, List[Tuple[datetime, datetime]]]:
    """(missing bars, windows to fetch): runs of absent grid bars, runs closer than MERGE_GAP_BARS joined."""
    step = pd.Timedelta(milliseconds=maint.timeframe_ms(timeframe))
    if start > end:
        return 0, []
    grid = pd.date_range(start, end, freq=step)
    absent = grid.difference(have)
    if not len(absent):
        return 0, []
    windows: List[List[datetime]] = []
    for stamp in absent:
        if windows and stamp - windows[-1][1] <= step * MERGE_GAP_BARS:
            windows[-1][1] = stamp
        else:
            windows.append([stamp, stamp])
    return len(absent), [(a.to_pydatetime(), b.to_pydatetime()) for a, b in windows]


def tail_audit(keys: List[maint.SeriesKey], now: datetime) -> List[Dict[str, Any]]:
    """The cheap daily check: each series from the bar after its last local one to the last closed bar."""
    rows = []
    storage = Path(maint.data_storage.storage_path)
    for key in keys:
        end = maint.last_complete_open(now, key.timeframe)
        last = maint.local_bounds(storage, key).last
        step = timedelta(milliseconds=maint.timeframe_ms(key.timeframe))
        if last is None:  # never filled: the full audit handles it
            rows.append({"series": key, "error": "no local bars: run --fill"})
            continue
        start = last + step
        bars = max(0, int((end - start) / step) + 1) if start <= end else 0
        rows.append({"series": key, "start": start, "end": end, "local_bars": None, "missing": bars,
                     "windows": [(start, end)] if bars else [], "bars_to_fetch": bars,
                     "requests": -(-bars // BARS_PER_REQUEST)})
    return rows


def audit(keys: List[maint.SeriesKey], limiter, now: datetime) -> List[Dict[str, Any]]:
    rows = []
    for i, key in enumerate(keys):
        have = local_stamps(key.namespace, key.symbol, key.timeframe)
        end = maint.last_complete_open(now, key.timeframe)
        start = START
        if not len(have) or have.min() > START:  # maybe listed later: ask Binance where the series begins
            try:
                first = first_source_bar(key, limiter)
            except Exception as exc:  # noqa: BLE001 - reported, the series is skipped
                rows.append({"series": key, "error": f"{type(exc).__name__}: {str(exc)[:80]}"})
                continue
            if first is not None:
                start = max(START, first)
        in_range = have[(have >= start) & (have <= end)]
        missing, windows = missing_windows(in_range, start, end, key.timeframe)
        bars_to_fetch = sum(int((b - a) / pd.Timedelta(milliseconds=maint.timeframe_ms(key.timeframe))) + 1 for a, b in windows)
        rows.append({"series": key, "start": start, "end": end, "local_bars": int(len(in_range)), "missing": missing,
                     "windows": windows, "bars_to_fetch": bars_to_fetch,
                     "requests": sum(-(-(int((b - a) / pd.Timedelta(milliseconds=maint.timeframe_ms(key.timeframe))) + 1) // BARS_PER_REQUEST)
                                     for a, b in windows)})
        if (i + 1) % 100 == 0:
            print(f"  audited {i + 1}/{len(keys)}", flush=True)
    return rows


def summarize(rows: List[Dict[str, Any]], label: str) -> str:
    frame = pd.DataFrame([{"ns": r["series"].namespace, "tf": r["series"].timeframe, "missing": r.get("missing", 0),
                           "complete": r.get("missing", 1) == 0 and "error" not in r, "requests": r.get("requests", 0),
                           "bars": r.get("bars_to_fetch", 0), "error": "error" in r} for r in rows])
    out = [f"{label}: {len(rows)} series"]
    for (ns, tf), g in frame.groupby(["ns", "tf"]):
        out.append(f"  {ns:15s} {tf:3s} series {len(g):3d} complete {int(g['complete'].sum()):3d} "
                   f"missing bars {int(g['missing'].sum()):9,d} | to fetch {int(g['bars'].sum()):9,d} bars, "
                   f"{int(g['requests'].sum()):5,d} requests{f', {int(g.error.sum())} errors' if g.error.any() else ''}")
    total_bars = int(frame["bars"].sum())
    out.append(f"  total: {int(frame['requests'].sum()):,} requests, ~{total_bars * BYTES_PER_BAR / 1e6:,.0f} MB uncompressed "
               f"(gzip on the wire, roughly a quarter)")
    return "\n".join(out)


# ---------------------------------------------------------------------------------------- fill
async def fill(rows: List[Dict[str, Any]], limiter, workers: int = 4) -> List[Dict[str, Any]]:
    gate = asyncio.Semaphore(workers)
    todo = [r for r in rows if r.get("windows")]

    async def one(r: Dict[str, Any]) -> None:
        key = r["series"]
        windows = [maint.FetchWindow(int(a.replace(tzinfo=timezone.utc).timestamp() * 1000),
                                     int(b.replace(tzinfo=timezone.utc).timestamp() * 1000), "audit_gap") for a, b in r["windows"]]
        async with gate:
            try:
                raw = await asyncio.to_thread(maint.fetch_key_windows, key, windows, limiter)
                klines = maint.rows_to_klines(key, raw)
                if klines:
                    await maint.save_klines_compact(key, klines)
                r["downloaded"] = len(klines)
            except Exception as exc:  # noqa: BLE001 - reported; the series keeps what it had
                r["fill_error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        done = sum("downloaded" in x or "fill_error" in x for x in todo)
        if done % 25 == 0:
            print(f"  filled {done}/{len(todo)}", flush=True)

    await asyncio.gather(*(one(r) for r in todo))
    return todo


def fill_error_summary(filled: List[Dict[str, Any]], top: int = 3) -> str:
    """Most common fill errors, so a log line says why a run failed (10/21-10/24 only said "errors 2975")."""
    from collections import Counter

    counts = Counter(str(r["fill_error"])[:140] for r in filled if "fill_error" in r)
    return "; ".join(f"{n}x {message}" for message, n in counts.most_common(top))


def _jsonable(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        key = r["series"]
        item = {"namespace": key.namespace, "symbol": key.symbol, "timeframe": key.timeframe}
        for k, v in r.items():
            if k == "series":
                continue
            if k == "windows":
                v = [[a.isoformat(), b.isoformat()] for a, b in v]
            elif isinstance(v, datetime):
                v = v.isoformat()
            item[k] = v
        out.append(item)
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--fill", action="store_true", help="download the missing windows")
    parser.add_argument("--tail-only", action="store_true", help="only top up each series after its last local bar")
    parser.add_argument("--max-requests-per-second", type=float, default=5.0)
    args = parser.parse_args(argv)

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)  # Binance needs the proxy here
    limiter = maint.RequestRateLimiter(args.max_requests_per_second)
    universe = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    spot_status, futures_status = maint.load_market_statuses(limiter)
    keys = series_keys(universe, spot_status, futures_status)
    now = datetime.now(timezone.utc)
    if args.tail_only:
        rows = tail_audit(keys, now)
        print(summarize(rows, "tail"), flush=True)
        filled = asyncio.run(fill(rows, limiter))
        print(f"topped up {sum('downloaded' in r for r in filled)} series, {sum(r.get('downloaded', 0) for r in filled):,} bars; "
              f"errors {sum('fill_error' in r for r in filled)}", flush=True)
        if any("fill_error" in r for r in filled):
            print(f"  fill errors: {fill_error_summary(filled)}", flush=True)
        return 1 if any("fill_error" in r for r in filled) else 0
    before = audit(keys, limiter, now)
    print(summarize(before, "before"), flush=True)
    report: Dict[str, Any] = {"audited_at": now.isoformat(), "coins": len({**universe["known"], **universe["held_out"]}),
                              "before": _jsonable(before)}
    if args.fill:
        filled = asyncio.run(fill(before, limiter))
        print(f"filled {sum('downloaded' in r for r in filled)} series, {sum(r.get('downloaded', 0) for r in filled):,} bars; "
              f"errors {sum('fill_error' in r for r in filled)}", flush=True)
        after = audit(keys, limiter, now)
        print(summarize(after, "after (what is still missing was never published by Binance, or failed)"), flush=True)
        report.update(filled=_jsonable(filled), after=_jsonable(after))
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    print(f"report: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
