"""Collect Binance's history for every USDT perpetual and spot pair, trading or delisted, into the
local store (research only).

Why (2026-10-13): the user asked to gather, as completely as possible, every kind of historical
data for all coins locally for later research. scripts/audit_fill_klines.py filled the 234-coin
wide universe from 2024-01; this extends the same store to everything Binance lists or listed.

What, from 2020-01-01 (or the first bar Binance has) to the last closed bar:
  * klines 1h / 4h / 1d of every USD-M USDT perpetual (data/historical/binance_futures), TRADING or
    SETTLING. A delisted perp keeps answering with flat zero-volume bars after its deliveryDate, so
    its series end there;
  * klines 1h / 4h / 1d of every spot USDT pair (data/historical/binance), TRADING or BREAK
    (delisted pairs end at their last bar), except stablecoins, fiat and leveraged tokens;
  * every funding settlement of each perp: binance_futures/<SYMBOL>/funding_rate.parquet;
  * the daily premium index (perp vs index basis) of each perp: binance_futures/<SYMBOL>/premium_index_1d.parquet.
Klines are merged through the general maintainer's locked atomic writer (never deleting or
rewriting older bars); every missing bar of the grid is asked for, interior gaps included.
Funding and the premium index are appended the same way. Delisted series are filled once and then
left alone.

    python scripts/collect_binance_history.py --audit       # what is missing, no downloads
    python scripts/collect_binance_history.py               # fill klines, funding and premium index
    python scripts/collect_binance_history.py --tail-only   # daily top-up of trading symbols
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

import audit_fill_klines as afk  # noqa: E402
import maintain_all_binance_market_data as maint  # noqa: E402
from core.data.parquet_lock import parquet_partition_lock  # noqa: E402
from core.data.path_utils import canonical_symbol_dir  # noqa: E402

REPORT_PATH = PROJECT_ROOT / "data" / "research" / "binance_history_audit.json"
START = datetime(2020, 1, 1)  # UTC naive, like the store's index
TIMEFRAMES = ("1h", "4h", "1d")
NOT_COINS = {"USDC", "BUSD", "TUSD", "USDP", "PAX", "FDUSD", "DAI", "USDS", "USDSB", "SUSD", "EUR", "GBP", "AUD",
             "EURI", "AEUR", "XUSD", "USD1", "BFUSD", "PYUSD", "RLUSD", "USDE", "UST", "BKRW", "IDRT", "BIDR", "BVND"}
LEVERAGED = re.compile(r"^[A-Z0-9]{2,}(UP|DOWN|BULL|BEAR)$")
FUNDING_PER_SECOND = 0.5  # /fapi/v1/fundingRate shares 500 requests / 5 minutes per IP with the live service;
# at 1.2/s on 10/13 a rate limit turned into WAF 403s for 546 perps
BAN_WAIT_SEC = 120  # a 403 (WAF), 418 (ban) or 429: wait it out instead of retrying at once


def _utc_naive_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).replace(tzinfo=None)


def _floor(dt: datetime, timeframe: str) -> datetime:
    step = maint.timeframe_ms(timeframe)
    ms = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return _utc_naive_ms(ms // step * step)


# --------------------------------------------------------------------------------------- universe
def universe(limiter) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """(perps, spot pairs) with what each needs: storage symbol, status, and the end of a delisted one."""
    fut = maint.request_json(maint.FUTURES_HOSTS, "/fapi/v1/exchangeInfo", limiter=limiter)
    perps = []
    for s in fut.get("symbols", []):
        if (s.get("contractType") != "PERPETUAL" or s.get("quoteAsset") != "USDT" or s.get("underlyingType") != "COIN"
                or s.get("status") not in {"TRADING", "SETTLING"}):
            continue
        end = _utc_naive_ms(s["deliveryDate"]) if s["status"] == "SETTLING" and s.get("deliveryDate") else None
        perps.append({"raw": s["symbol"], "symbol": f"{s['symbol'][:-4]}/USDT", "status": s["status"], "end": end})
    spot_info = maint.request_json(maint.SPOT_HOSTS, "/api/v3/exchangeInfo", limiter=limiter)
    pairs = []
    for s in spot_info.get("symbols", []):
        base = str(s.get("baseAsset") or "")
        if s.get("quoteAsset") != "USDT" or s.get("status") not in {"TRADING", "BREAK"} or base in NOT_COINS:
            continue
        if s["status"] == "BREAK" and LEVERAGED.fullmatch(base) and len(base) > 4:
            continue  # BTCUP, ETHBULL...: delisted leveraged tokens
        pairs.append({"raw": s["symbol"], "symbol": f"{base}/USDT", "status": s["status"], "end": None})
    return perps, pairs


def last_spot_bar(raw: str, limiter) -> Optional[datetime]:
    """The final 1h bar of a delisted spot pair (its klines simply stop)."""
    payload = maint.request_json(maint.SPOT_HOSTS, "/api/v3/klines", params={"symbol": raw, "interval": "1h", "limit": 1},
                                 limiter=limiter)
    return _utc_naive_ms(payload[-1][0]) if payload else None


# ------------------------------------------------------------------------------------------ klines
def kline_rows(perps: List[Dict[str, Any]], pairs: List[Dict[str, Any]], limiter, now: datetime,
               tail_only: bool = False) -> List[Dict[str, Any]]:
    rows = []
    storage = Path(maint.data_storage.storage_path)
    items = [(maint.FUTURES_NAMESPACE, "futures", p) for p in perps] + [(maint.SPOT_NAMESPACE, "spot", p) for p in pairs]
    for i, (namespace, market, item) in enumerate(items):
        delisted = item["status"] != "TRADING"
        if tail_only and delisted:
            continue
        if delisted and item["end"] is None and market == "spot":
            try:
                item["end"] = last_spot_bar(item["raw"], limiter)
            except Exception:  # noqa: BLE001 - leave it for the next run
                continue
            if item["end"] is None:
                continue
        for tf in TIMEFRAMES:
            key = maint.SeriesKey(namespace, market, item["symbol"], tf)
            end = maint.last_complete_open(now, tf)
            if item["end"] is not None:
                end = min(end, _floor(item["end"], tf))
            if tail_only:
                last = maint.local_bounds(storage, key).last
                if last is None:
                    continue  # never filled: the full run handles it
                start = last + timedelta(milliseconds=maint.timeframe_ms(tf))
                bars = max(0, int((end - start) / timedelta(milliseconds=maint.timeframe_ms(tf))) + 1) if start <= end else 0
                rows.append({"series": key, "start": start, "end": end, "missing": bars,
                             "windows": [(start, end)] if bars else [], "bars_to_fetch": bars,
                             "requests": -(-bars // afk.BARS_PER_REQUEST)})
                continue
            have = afk.local_stamps(namespace, item["symbol"], tf)
            start = START
            if not len(have) or have.min() > START:
                try:
                    first = afk.first_source_bar(key, limiter)
                except Exception as exc:  # noqa: BLE001 - reported; the series is skipped this run
                    rows.append({"series": key, "error": f"{type(exc).__name__}: {str(exc)[:80]}"})
                    continue
                if first is None:
                    continue
                start = max(START, first)
            in_range = have[(have >= start) & (have <= end)]
            missing, windows = afk.missing_windows(in_range, start, end, tf)
            step = pd.Timedelta(milliseconds=maint.timeframe_ms(tf))
            rows.append({"series": key, "start": start, "end": end, "local_bars": int(len(in_range)), "missing": missing,
                         "windows": windows, "bars_to_fetch": sum(int((b - a) / step) + 1 for a, b in windows),
                         "requests": sum(-(-(int((b - a) / step) + 1) // afk.BARS_PER_REQUEST) for a, b in windows)})
        if (i + 1) % 100 == 0:
            print(f"  planned {i + 1}/{len(items)} symbols", flush=True)
    return rows


# --------------------------------------------------------------------------- funding and premium index
def _series_path(symbol: str, name: str) -> Path:
    return canonical_symbol_dir(Path(maint.data_storage.storage_path), maint.FUTURES_NAMESPACE, symbol) / f"{name}.parquet"


def _append_parquet(path: Path, frame: pd.DataFrame) -> int:
    """Merge rows into one parquet (index = UTC-naive time), keeping stored rows as they are."""
    if frame.empty:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with parquet_partition_lock(path, timeout_seconds=60):
        combined = frame
        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, frame[~frame.index.isin(existing.index)]])
        combined = combined[~combined.index.duplicated(keep="first")].sort_index()
        tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        pq.write_table(pa.Table.from_pandas(combined), str(tmp), compression="zstd", compression_level=6)
        os.replace(tmp, path)
    return len(frame)


def _last_index(path: Path) -> Optional[datetime]:
    if not path.exists():
        return None
    try:
        index = pd.read_parquet(path, columns=[]).index
        return pd.Timestamp(index.max()).to_pydatetime() if len(index) else None
    except Exception:  # noqa: BLE001 - an unreadable file is refilled
        return None


def _banned(exc: Exception) -> bool:
    return any(code in str(exc) for code in ("403", "418", "429"))


def fill_funding(perps: List[Dict[str, Any]], tail_only: bool = False) -> Dict[str, Any]:
    limiter = maint.RequestRateLimiter(FUNDING_PER_SECOND)
    added, errors = 0, {}
    for perp in perps:
        if tail_only and perp["status"] != "TRADING":
            continue
        path = _series_path(perp["symbol"], "funding_rate")
        stop = int((perp["end"] or datetime.utcnow()).replace(tzinfo=timezone.utc).timestamp() * 1000)
        for attempt in range(4):
            last = _last_index(path)
            cursor = int(((last + timedelta(milliseconds=1)) if last else START).replace(tzinfo=timezone.utc).timestamp() * 1000)
            rows = []
            failure = None
            try:
                while cursor <= stop:
                    page = maint.request_json(maint.FUTURES_HOSTS, "/fapi/v1/fundingRate",
                                              params={"symbol": perp["raw"], "startTime": cursor, "limit": 1000},
                                              limiter=limiter, attempts=2)
                    if not page:
                        break
                    rows += [r for r in page if int(r["fundingTime"]) <= stop]
                    cursor = int(page[-1]["fundingTime"]) + 1
                    if len(page) < 1000:
                        break
            except Exception as exc:  # noqa: BLE001 - what was fetched is kept below
                failure = exc
            if rows:
                frame = pd.DataFrame({"funding_rate": [float(r["fundingRate"]) for r in rows],
                                      "mark_price": [pd.to_numeric(r.get("markPrice"), errors="coerce") for r in rows]},
                                     index=pd.DatetimeIndex([_utc_naive_ms(r["fundingTime"]) for r in rows], name="funding_time"))
                added += _append_parquet(path, frame)
            if failure is None:
                errors.pop(perp["raw"], None)
                break
            errors[perp["raw"]] = f"{type(failure).__name__}: {str(failure)[:100]}"
            if not _banned(failure):
                break
            time.sleep(BAN_WAIT_SEC * (attempt + 1))
    return {"settlements_added": added, "errors": errors}


def refill_tolerant(rows: List[Dict[str, Any]], limiter) -> Dict[str, Any]:
    """Series the maintainer's validator refused whole (one inconsistent bar from Binance): fetch them
    again, drop only the bars that fail validation, and save the rest."""
    fixed, dropped = {}, 0
    for r in rows:
        if "inconsistent OHLC" not in str(r.get("fill_error", "")) and "invalid OHLCV" not in str(r.get("fill_error", "")):
            continue
        key = r["series"]
        windows = [maint.FetchWindow(int(a.replace(tzinfo=timezone.utc).timestamp() * 1000),
                                     int(b.replace(tzinfo=timezone.utc).timestamp() * 1000), "tolerant") for a, b in r["windows"]]
        raw = [row for w in windows for row in maint.fetch_window(key, w, limiter)]
        good = []
        for row in raw:
            try:
                good += maint.validate_rows([row], key.timeframe)
            except ValueError:
                dropped += 1
        if good:
            asyncio.run(maint.save_klines_compact(key, maint.rows_to_klines(key, sorted(good, key=lambda x: int(x[0])))))
        fixed[f"{key.namespace}:{key.symbol}:{key.timeframe}"] = len(good)
    return {"series": fixed, "bars_dropped": dropped}


def fill_premium(perps: List[Dict[str, Any]], limiter, now: datetime, tail_only: bool = False) -> Dict[str, Any]:
    added, errors = 0, {}
    for perp in perps:
        if tail_only and perp["status"] != "TRADING":
            continue
        path = _series_path(perp["symbol"], "premium_index_1d")
        last = _last_index(path)
        end = maint.last_complete_open(now, "1d")
        if perp["end"] is not None:
            end = min(end, _floor(perp["end"], "1d"))
        start = (last + timedelta(days=1)) if last else START
        cursor, stop = (int(x.replace(tzinfo=timezone.utc).timestamp() * 1000) for x in (start, end))
        rows = []
        try:
            while cursor <= stop:
                page = maint.request_json(maint.FUTURES_HOSTS, "/fapi/v1/premiumIndexKlines",
                                          params={"symbol": perp["raw"], "interval": "1d", "startTime": cursor,
                                                  "endTime": stop + maint.timeframe_ms("1d") - 1, "limit": 1500}, limiter=limiter)
                if not page:
                    break
                rows += [r for r in page if int(r[0]) <= stop]
                cursor = int(page[-1][0]) + maint.timeframe_ms("1d")
                if len(page) < 1500:
                    break
        except Exception as exc:  # noqa: BLE001
            errors[perp["raw"]] = f"{type(exc).__name__}: {str(exc)[:100]}"
        if rows:
            frame = pd.DataFrame({"open": [float(r[1]) for r in rows], "high": [float(r[2]) for r in rows],
                                  "low": [float(r[3]) for r in rows], "close": [float(r[4]) for r in rows]},
                                 index=pd.DatetimeIndex([_utc_naive_ms(r[0]) for r in rows], name="timestamp"))
            added += _append_parquet(path, frame)
    return {"bars_added": added, "errors": errors}


# ------------------------------------------------------------------------------------------- main
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--audit", action="store_true", help="plan only: what is missing, no downloads")
    parser.add_argument("--tail-only", action="store_true", help="daily top-up of trading symbols")
    parser.add_argument("--max-requests-per-second", type=float, default=5.0)
    parser.add_argument("--funding-only", action="store_true", help="only funding settlements (e.g. after a ban)")
    args = parser.parse_args(argv)

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)  # Binance needs the proxy here
    limiter = maint.RequestRateLimiter(args.max_requests_per_second)
    now = datetime.now(timezone.utc)
    perps, pairs = universe(limiter)
    print(f"perps {len(perps)} ({sum(p['status'] == 'TRADING' for p in perps)} trading), "
          f"spot pairs {len(pairs)} ({sum(p['status'] == 'TRADING' for p in pairs)} trading)", flush=True)
    if args.funding_only:
        result = fill_funding(perps, tail_only=args.tail_only)
        print(f"funding: {result['settlements_added']:,} settlements added, errors {len(result['errors'])}: "
              f"{list(result['errors'].items())[:3]}", flush=True)
        return 1 if result["errors"] else 0
    rows = kline_rows(perps, pairs, limiter, now, tail_only=args.tail_only)
    print(afk.summarize(rows, "klines to fill" if not args.tail_only else "tail"), flush=True)
    report: Dict[str, Any] = {"run_at": now.isoformat(), "perps": len(perps), "spot_pairs": len(pairs),
                              "klines_before": afk._jsonable(rows)}
    if not args.audit:
        filled = asyncio.run(afk.fill(rows, limiter))
        print(f"klines: filled {sum('downloaded' in r for r in filled)} series, {sum(r.get('downloaded', 0) for r in filled):,} bars; "
              f"errors {sum('fill_error' in r for r in filled)}", flush=True)
        if any("fill_error" in r for r in filled):
            print(f"  fill errors: {afk.fill_error_summary(filled)}", flush=True)
        report["klines_filled"] = afk._jsonable(filled)
        report["klines_tolerant"] = refill_tolerant(filled, limiter)
        if report["klines_tolerant"]["series"]:
            print(f"klines refilled without bad bars: {report['klines_tolerant']}", flush=True)
        report["funding"] = fill_funding(perps, tail_only=args.tail_only)
        print(f"funding: {report['funding']['settlements_added']:,} settlements added, errors {len(report['funding']['errors'])}", flush=True)
        report["premium_index"] = fill_premium(perps, limiter, now, tail_only=args.tail_only)
        print(f"premium index: {report['premium_index']['bars_added']:,} bars added, errors {len(report['premium_index']['errors'])}",
              flush=True)
    if not args.tail_only:
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    failed = any("fill_error" in r for r in report.get("klines_filled", [])) or (report.get("funding") or {}).get("errors")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
