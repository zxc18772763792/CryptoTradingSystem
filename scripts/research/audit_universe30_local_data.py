"""Audit/fill local Binance historical parquet coverage for research default 30-symbol universe."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import ccxt
import pandas as pd
import requests
from loguru import logger

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.data import data_storage  # noqa: E402
from core.exchanges.base_exchange import Kline  # noqa: E402


UNIVERSE30 = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT", "ADA/USDT", "DOGE/USDT", "TRX/USDT", "LINK/USDT",
    "AVAX/USDT", "DOT/USDT", "POL/USDT", "LTC/USDT", "BCH/USDT", "ETC/USDT", "ATOM/USDT", "NEAR/USDT", "APT/USDT",
    "ARB/USDT", "OP/USDT", "SUI/USDT", "INJ/USDT", "RUNE/USDT", "AAVE/USDT", "MKR/USDT", "UNI/USDT", "FIL/USDT",
    "HBAR/USDT", "ICP/USDT", "TON/USDT",
]

DEFAULT_TFS = ["1m", "5m", "15m", "1h", "4h", "1d"]
MINUTE_TFS = {"1m", "5m", "15m"}
SYMBOL_ALIASES = {
    "MATIC/USDT": "POL/USDT",
    "MATICUSDT": "POL/USDT",
}


@dataclass
class CoverageRow:
    symbol: str
    timeframe: str
    exists: bool
    rows: int = 0
    start: str = ""
    end: str = ""
    age_hours: float = 0.0
    stale_limit_hours: float = 0.0
    is_stale: bool = False
    retired_like: bool = False
    file_path: str = ""
    file_size_mb: float = 0.0


def _mk_binance(market_type: str = "spot") -> ccxt.binance:
    return ccxt.binance(
        {
            "enableRateLimit": True,
            "timeout": 30000,
            "options": {"defaultType": market_type},
        }
    )


def _tf_to_ms(timeframe: str) -> int:
    tf = str(timeframe).strip().lower()
    unit = tf[-1]
    val = int(tf[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
    return val * mult


def _tf_to_hours(timeframe: str) -> float:
    return _tf_to_ms(timeframe) / 3_600_000.0


def _stale_limit_hours_for_tf(timeframe: str, base_hours: float) -> float:
    tf_h = max(_tf_to_hours(timeframe), 1e-6)
    # Prevent false stale on slow bars, especially 1d.
    return max(float(base_hours), tf_h * 2.2 + 0.5)


def _retired_like_age_hours_for_tf(timeframe: str) -> float:
    tf_h = _tf_to_hours(timeframe)
    if tf_h >= 24.0:
        return 24.0 * 45.0
    return max(24.0 * 7.0, tf_h * 200.0)


def _fetch_ohlcv_backfill(exchange: ccxt.binance, symbol: str, timeframe: str, since_ms: int, until_ms: int, batch_limit: int = 1000) -> List[List[float]]:
    tf_ms = int(exchange.parse_timeframe(timeframe) * 1000)
    cursor = int(since_ms)
    out: List[List[float]] = []
    while cursor < until_ms:
        batch = []
        for attempt in range(1, 4):
            try:
                batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=batch_limit)
                break
            except Exception as e:
                if attempt == 3:
                    raise
                wait_s = min(8.0, 1.5 * attempt)
                logger.warning(f"fetch_ohlcv {symbol} {timeframe} since={cursor} attempt={attempt} failed: {e}; retry {wait_s:.1f}s")
                time.sleep(wait_s)
        if not batch:
            break
        out.extend(batch)
        last_ms = int(batch[-1][0])
        if last_ms <= cursor:
            break
        cursor = last_ms + tf_ms
        time.sleep(max(float(getattr(exchange, "rateLimit", 200) or 200) / 1000.0, 0.03))
        if len(batch) < batch_limit and cursor >= until_ms - tf_ms:
            break
    dedup = {int(r[0]): r for r in out if len(r) >= 6}
    return [dedup[k] for k in sorted(dedup.keys())]


def _fetch_ohlcv_backfill_http(symbol: str, timeframe: str, since_ms: int, until_ms: int, market_type: str = "spot", batch_limit: int = 1000) -> List[List[float]]:
    tf_ms = _tf_to_ms(timeframe)
    cursor = int(since_ms)
    out: List[List[float]] = []
    raw_symbol = symbol.replace("/", "")
    hosts = ["https://data-api.binance.vision", "https://api.binance.com"]
    path = "/api/v3/klines" if market_type == "spot" else "/fapi/v1/klines"
    sess = requests.Session()
    sess.headers.update({"User-Agent": "crypto_trading_system/audit_universe30_local_data"})
    while cursor < until_ms:
        batch = None
        last_err: Optional[Exception] = None
        for attempt in range(1, 4):
            for host in hosts:
                try:
                    resp = sess.get(
                        f"{host}{path}",
                        params={"symbol": raw_symbol, "interval": timeframe, "startTime": cursor, "endTime": until_ms, "limit": batch_limit},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    batch = resp.json() or []
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    continue
            if batch is not None:
                break
            if attempt < 3:
                time.sleep(min(8.0, 1.2 * attempt))
        if batch is None:
            raise last_err or RuntimeError("HTTP kline fetch failed")
        if not batch:
            break
        # Binance kline item: [open_time, open, high, low, close, volume, close_time, ...]
        out.extend([[row[0], row[1], row[2], row[3], row[4], row[5]] for row in batch if len(row) >= 6])
        last_ms = int(batch[-1][0])
        if last_ms <= cursor:
            break
        cursor = last_ms + tf_ms
        time.sleep(0.08)
        if len(batch) < batch_limit and cursor >= until_ms - tf_ms:
            break
    dedup = {int(r[0]): r for r in out if len(r) >= 6}
    return [dedup[k] for k in sorted(dedup.keys())]


async def _audit_one(exchange_name: str, symbol: str, timeframe: str, stale_base_hours: float) -> CoverageRow:
    file_path = data_storage.storage_path / exchange_name / symbol.replace("/", "_") / f"{timeframe}.parquet"
    if not file_path.exists():
        return CoverageRow(symbol=symbol, timeframe=timeframe, exists=False, file_path=str(file_path))
    try:
        df = await data_storage.load_klines_from_parquet(exchange=exchange_name, symbol=symbol, timeframe=timeframe)
    except Exception as e:
        logger.warning(f"audit load failed {symbol} {timeframe}: {e}")
        return CoverageRow(symbol=symbol, timeframe=timeframe, exists=False, file_path=str(file_path))
    if df is None or df.empty:
        return CoverageRow(symbol=symbol, timeframe=timeframe, exists=False, file_path=str(file_path))
    idx = pd.to_datetime(df.index)
    now = pd.Timestamp.utcnow().tz_localize(None)
    age_h = max(0.0, float((now - idx.max()).total_seconds() / 3600.0))
    stale_limit_h = _stale_limit_hours_for_tf(timeframe=timeframe, base_hours=stale_base_hours)
    is_stale = bool(age_h > stale_limit_h)
    retired_like = bool(is_stale and age_h > _retired_like_age_hours_for_tf(timeframe))
    return CoverageRow(
        symbol=symbol,
        timeframe=timeframe,
        exists=True,
        rows=int(len(df)),
        start=pd.Timestamp(idx.min()).isoformat(),
        end=pd.Timestamp(idx.max()).isoformat(),
        age_hours=round(age_h, 3),
        stale_limit_hours=round(stale_limit_h, 3),
        is_stale=is_stale,
        retired_like=retired_like,
        file_path=str(file_path),
        file_size_mb=round(file_path.stat().st_size / (1024 * 1024), 3),
    )


async def _fill_one(
    exchange: ccxt.binance,
    exchange_name: str,
    symbol: str,
    timeframe: str,
    days: int,
    overlap_bars: int,
    market_type: str = "spot",
    prefer_http: bool = False,
) -> Dict[str, object]:
    now = datetime.utcnow()
    tf_sec = int(exchange.parse_timeframe(timeframe))
    default_start = now - timedelta(days=max(1, int(days)))
    existing = await data_storage.load_klines_from_parquet(exchange=exchange_name, symbol=symbol, timeframe=timeframe)
    if existing is not None and not existing.empty:
        last_ts = pd.to_datetime(existing.index.max()).to_pydatetime()
        start_dt = max(default_start, last_ts - timedelta(seconds=tf_sec * max(1, overlap_bars)))
    else:
        start_dt = default_start
    try:
        if prefer_http:
            raise RuntimeError("prefer_http enabled")
        rows = _fetch_ohlcv_backfill(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            since_ms=int(start_dt.timestamp() * 1000),
            until_ms=int(now.timestamp() * 1000),
            batch_limit=1000,
        )
    except Exception as e:
        logger.warning(f"ccxt fetch failed {symbol} {timeframe}, fallback to direct HTTP: {e}")
        rows = _fetch_ohlcv_backfill_http(
            symbol=symbol,
            timeframe=timeframe,
            since_ms=int(start_dt.timestamp() * 1000),
            until_ms=int(now.timestamp() * 1000),
            market_type=market_type,
            batch_limit=1000,
        )
    if not rows:
        return {"symbol": symbol, "timeframe": timeframe, "downloaded": 0}
    klines = [
        Kline(
            exchange=exchange_name,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=datetime.utcfromtimestamp(float(r[0]) / 1000.0),
            open=float(r[1]),
            high=float(r[2]),
            low=float(r[3]),
            close=float(r[4]),
            volume=float(r[5]),
        )
        for r in rows
    ]
    await data_storage.save_klines_to_parquet(klines=klines, exchange=exchange_name, symbol=symbol, timeframe=timeframe)
    return {"symbol": symbol, "timeframe": timeframe, "downloaded": len(rows)}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Audit/fill local coverage for 30-symbol research universe.")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--timeframes", default="1m,5m,15m,1h,4h,1d")
    parser.add_argument("--minute-days", type=int, default=30, help="Target lookback days for 1m/5m/15m")
    parser.add_argument("--htf-days", type=int, default=365, help="Target lookback days for 1h/4h/1d")
    parser.add_argument("--overlap-bars", type=int, default=48)
    parser.add_argument("--fill", action="store_true", help="Fill missing files (and refresh stale tails)")
    parser.add_argument("--refresh-stale-hours", type=float, default=12.0, help="When --fill, also refresh files older than this age")
    parser.add_argument("--market-type", default="spot", choices=["spot", "swap", "future"], help="CCXT defaultType for fetch")
    parser.add_argument("--symbols", default="", help="Optional comma-separated subset symbols, e.g. BTC/USDT,ETH/USDT")
    parser.add_argument("--prefer-http", action="store_true", help="Skip CCXT fetch and use Binance public HTTP klines directly")
    parser.add_argument("--out-prefix", default="data/research/universe30_coverage")
    args = parser.parse_args()

    exchange_name = str(args.exchange).strip().lower()
    tfs = [x.strip().lower() for x in str(args.timeframes).split(",") if x.strip()]
    if not tfs:
        tfs = list(DEFAULT_TFS)

    symbols = list(UNIVERSE30)
    if str(args.symbols).strip():
        requested = [x.strip().upper() for x in str(args.symbols).split(",") if x.strip()]
        symbol_map = {s.upper(): s for s in UNIVERSE30}
        alias_map = {k.upper(): v for k, v in SYMBOL_ALIASES.items()}
        resolved: List[str] = []
        missing_req: List[str] = []
        for s in requested:
            s2 = alias_map.get(s, s)
            if s2 in symbol_map:
                resolved.append(symbol_map[s2])
            elif "/" in s2 and s2.endswith("/USDT"):
                resolved.append(s2)
            else:
                missing_req.append(s)
        seen = set()
        symbols = []
        for s in resolved:
            if s not in seen:
                seen.add(s)
                symbols.append(s)
        if missing_req:
            logger.warning(f"Unknown symbols skipped: {missing_req}")
        if not symbols:
            raise SystemExit("No valid symbols after --symbols filter")

    rows: List[CoverageRow] = []
    for s in symbols:
        for tf in tfs:
            rows.append(await _audit_one(exchange_name=exchange_name, symbol=s, timeframe=tf, stale_base_hours=float(args.refresh_stale_hours)))

    df0 = pd.DataFrame([r.__dict__ for r in rows])
    missing_or_stale = df0[(~df0["exists"]) | (df0["is_stale"] == True)]

    fill_results: List[Dict[str, object]] = []
    if args.fill and not missing_or_stale.empty:
        ex = _mk_binance(market_type=args.market_type)
        try:
            try:
                ex.load_markets()
            except Exception as e:
                logger.warning(f"ccxt load_markets failed, will rely on HTTP fallback when needed: {e}")
            for _, r in missing_or_stale.sort_values(["symbol", "timeframe"]).iterrows():
                sym = str(r["symbol"])
                tf = str(r["timeframe"])
                days = int(args.minute_days) if tf in MINUTE_TFS else int(args.htf_days)
                try:
                    logger.info(f"Fill {sym} {tf} days={days} (exists={bool(r['exists'])}, age_h={float(r.get('age_hours') or 0):.2f})")
                    res = await _fill_one(
                        ex,
                        exchange_name,
                        sym,
                        tf,
                        days=days,
                        overlap_bars=int(args.overlap_bars),
                        market_type=args.market_type,
                        prefer_http=bool(args.prefer_http),
                    )
                    fill_results.append(res)
                    logger.info(f"  downloaded={res.get('downloaded', 0)}")
                except Exception as e:
                    logger.warning(f"Fill failed {sym} {tf}: {e}")
                    fill_results.append({"symbol": sym, "timeframe": tf, "error": str(e)})
        finally:
            close_fn = getattr(ex, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception:
                    pass
        # Re-audit after fill
        rows = []
        for s in symbols:
            for tf in tfs:
                rows.append(await _audit_one(exchange_name=exchange_name, symbol=s, timeframe=tf, stale_base_hours=float(args.refresh_stale_hours)))
        df0 = pd.DataFrame([r.__dict__ for r in rows])

    out_base = Path(str(args.out_prefix))
    out_base.parent.mkdir(parents=True, exist_ok=True)
    csv_path = out_base.with_suffix(".csv")
    json_path = out_base.with_suffix(".json")
    df0.sort_values(["symbol", "timeframe"]).to_csv(csv_path, index=False, encoding="utf-8-sig")

    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "exchange": exchange_name,
        "symbols": len(symbols),
        "timeframes": tfs,
        "total_checks": int(len(df0)),
        "covered_count": int((df0["exists"] == True).sum()),
        "missing_count": int((df0["exists"] == False).sum()),
        "missing_items": df0[df0["exists"] == False][["symbol", "timeframe"]].to_dict(orient="records"),
        "stale_base_hours": float(args.refresh_stale_hours),
        "stale_count": int((df0["is_stale"] == True).sum()),
        "retired_like_count": int((df0["retired_like"] == True).sum()),
        "retired_like_items": df0[df0["retired_like"] == True][["symbol", "timeframe", "end", "age_hours"]].to_dict(orient="records"),
        "fill_requested": bool(args.fill),
        "fill_results": fill_results,
        "csv_path": str(csv_path.resolve()),
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({k: v for k, v in summary.items() if k not in {"fill_results", "missing_items"}}, ensure_ascii=False, indent=2))
    if summary["missing_count"]:
        print("MISSING:")
        print(df0[df0["exists"] == False][["symbol", "timeframe"]].sort_values(["symbol", "timeframe"]).to_string(index=False))
    stale_df = df0[df0["is_stale"] == True].sort_values(["symbol", "timeframe"])
    if not stale_df.empty:
        print("STALE:")
        print(stale_df[["symbol", "timeframe", "age_hours", "stale_limit_hours", "retired_like"]].to_string(index=False))


if __name__ == "__main__":
    asyncio.run(main())
