"""Maintain top market-cap crypto universe historical data in local parquet DB."""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import ccxt
import pandas as pd
import requests
from loguru import logger

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings  # noqa: E402
from core.data import data_storage  # noqa: E402
from core.exchanges.base_exchange import Kline  # noqa: E402


_STABLE_SYMBOLS = {
    "USDT",
    "USDC",
    "DAI",
    "TUSD",
    "BUSD",
    "FDUSD",
    "USDP",
    "USDE",
    "PYUSD",
    "FRAX",
    "LUSD",
    "USDD",
}


def _proxy_mapping() -> Dict[str, str]:
    proxies: Dict[str, str] = {}
    http_proxy = str(getattr(settings, "HTTP_PROXY", "") or "").strip()
    https_proxy = str(getattr(settings, "HTTPS_PROXY", "") or "").strip()
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    return proxies


def _coingecko_top_marketcap(limit: int = 100) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page = 1
    per_page = 250
    proxies = _proxy_mapping() or None

    while len(rows) < max(limit * 3, 300) and page <= 6:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false",
            "price_change_percentage": "24h",
        }
        batch: List[Dict[str, Any]] = []
        err: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                resp = requests.get(url, params=params, timeout=25, proxies=proxies)
                resp.raise_for_status()
                batch = resp.json() or []
                err = None
                break
            except Exception as e:
                err = e
                wait_s = min(6.0, 1.2 * attempt)
                logger.warning(f"CoinGecko page={page} attempt={attempt} failed: {e}; retry in {wait_s:.1f}s")
                time.sleep(wait_s)
        if err is not None:
            raise err
        if not batch:
            break
        rows.extend(batch)
        page += 1
        time.sleep(1.2)
    return rows


def _load_binance_spot_usdt_symbols() -> Tuple[ccxt.binance, set[str]]:
    proxies = _proxy_mapping()
    exchange = ccxt.binance(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
            "timeout": 30000,
            "proxies": proxies,
        }
    )
    markets: Dict[str, Any] = {}
    last_err: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            markets = exchange.load_markets()
            last_err = None
            break
        except Exception as e:
            last_err = e
            wait_s = min(10.0, 1.5 * attempt)
            logger.warning(f"Binance load_markets attempt={attempt} failed: {e}; retry in {wait_s:.1f}s")
            time.sleep(wait_s)
    if last_err is not None:
        raise last_err

    out = {
        str(m.get("symbol"))
        for m in markets.values()
        if bool(m.get("spot"))
        and str(m.get("quote", "")).upper() == "USDT"
        and bool(m.get("active", True))
    }
    return exchange, out


def _select_universe(
    top_marketcap: List[Dict[str, Any]],
    spot_symbols: set[str],
    top_n: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    selected: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    seen_pairs: set[str] = set()

    for row in top_marketcap:
        sym = str(row.get("symbol", "")).upper()
        if not sym or sym in _STABLE_SYMBOLS:
            continue
        pair = f"{sym}/USDT"
        if pair in seen_pairs:
            continue
        if pair not in spot_symbols:
            skipped.append(
                {
                    "symbol": sym,
                    "name": row.get("name"),
                    "market_cap_rank": row.get("market_cap_rank"),
                    "reason": "not_on_binance_spot_usdt",
                }
            )
            continue
        seen_pairs.add(pair)
        selected.append(
            {
                "symbol": sym,
                "pair": pair,
                "name": row.get("name"),
                "market_cap_rank": row.get("market_cap_rank"),
                "market_cap": row.get("market_cap"),
            }
        )
        if len(selected) >= top_n:
            break
    return selected, skipped


def _fetch_ohlcv_backfill(
    exchange: ccxt.binance,
    symbol: str,
    timeframe: str,
    since_ms: int,
    until_ms: int,
    batch_limit: int = 1000,
) -> List[List[float]]:
    tf_ms = max(1000, int(exchange.parse_timeframe(timeframe) * 1000))
    cursor = int(since_ms)
    out: List[List[float]] = []

    while cursor < until_ms:
        batch: List[List[float]] = []
        ok = False
        for attempt in range(1, 4):
            try:
                batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=batch_limit)
                ok = True
                break
            except Exception as e:
                wait_s = min(8.0, 1.0 * attempt)
                logger.warning(
                    f"fetch_ohlcv {symbol} {timeframe} since={cursor} attempt={attempt} failed: {e}; retry in {wait_s:.1f}s"
                )
                time.sleep(wait_s)
        if not ok:
            break
        if not batch:
            break
        out.extend(batch)
        last_ms = int(batch[-1][0])
        if last_ms <= cursor:
            break
        cursor = last_ms + tf_ms
        if len(batch) < batch_limit and cursor >= until_ms - tf_ms:
            break
        time.sleep(max(float(getattr(exchange, "rateLimit", 200) or 200) / 1000.0, 0.05))

    # Deduplicate by timestamp.
    dedup: Dict[int, List[float]] = {int(r[0]): r for r in out if len(r) >= 6}
    return [dedup[k] for k in sorted(dedup.keys())]


async def _download_symbol_timeframe(
    exchange: ccxt.binance,
    exchange_name: str,
    symbol: str,
    timeframe: str,
    days: int,
    overlap_bars: int,
) -> Dict[str, Any]:
    now = datetime.now()
    tf_sec = max(1, int(exchange.parse_timeframe(timeframe)))
    default_start = now - timedelta(days=max(1, int(days)))

    existing = await data_storage.load_klines_from_parquet(
        exchange=exchange_name,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not existing.empty:
        last_ts = pd.to_datetime(existing.index.max()).to_pydatetime()
        start_dt = max(default_start, last_ts - timedelta(seconds=tf_sec * max(1, int(overlap_bars))))
    else:
        start_dt = default_start

    rows = _fetch_ohlcv_backfill(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        since_ms=int(start_dt.timestamp() * 1000),
        until_ms=int(now.timestamp() * 1000),
        batch_limit=1000,
    )
    if not rows:
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "downloaded": 0,
            "saved_path": "",
            "start": start_dt.isoformat(),
            "end": now.isoformat(),
        }

    klines = [
        Kline(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=datetime.fromtimestamp(float(r[0]) / 1000.0),
            open=float(r[1]),
            high=float(r[2]),
            low=float(r[3]),
            close=float(r[4]),
            volume=float(r[5]),
            exchange=exchange_name,
        )
        for r in rows
    ]
    saved = await data_storage.save_klines_to_parquet(
        klines=klines,
        exchange=exchange_name,
        symbol=symbol,
        timeframe=timeframe,
    )
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "downloaded": len(rows),
        "saved_path": saved,
        "start": start_dt.isoformat(),
        "end": now.isoformat(),
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Maintain top market-cap universe historical data.")
    parser.add_argument("--top-n", type=int, default=100, help="Target universe size.")
    parser.add_argument("--days", type=int, default=365, help="History lookback days.")
    parser.add_argument("--timeframes", default="1h", help="Comma-separated timeframes.")
    parser.add_argument("--exchange", default="binance", help="Exchange name for local storage path.")
    parser.add_argument("--overlap-bars", type=int, default=48, help="Overlap bars for incremental refresh.")
    parser.add_argument("--universe-out", default="data/research/top100_universe_latest.json")
    args = parser.parse_args()

    top_n = max(20, int(args.top_n))
    days = max(30, int(args.days))
    exchange_name = str(args.exchange or "binance").strip().lower()
    timeframes = [x.strip().lower() for x in str(args.timeframes or "1h").split(",") if x.strip()]
    if not timeframes:
        timeframes = ["1h"]

    logger.info("Loading top market-cap universe from CoinGecko ...")
    marketcap_rows = _coingecko_top_marketcap(limit=top_n)
    exchange, spot_symbols = _load_binance_spot_usdt_symbols()
    selected, skipped = _select_universe(marketcap_rows, spot_symbols, top_n=top_n)

    logger.info(
        f"Universe selected: requested={top_n}, available={len(selected)}, skipped={len(skipped)}, "
        f"timeframes={timeframes}, days={days}"
    )

    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    started_at = datetime.now(timezone.utc).isoformat()

    for idx, item in enumerate(selected, start=1):
        symbol = str(item["pair"]).upper()
        logger.info(f"[{idx}/{len(selected)}] Refresh {symbol}")
        for tf in timeframes:
            try:
                res = await _download_symbol_timeframe(
                    exchange=exchange,
                    exchange_name=exchange_name,
                    symbol=symbol,
                    timeframe=tf,
                    days=days,
                    overlap_bars=max(1, int(args.overlap_bars)),
                )
                results.append(res)
                logger.info(f"  {tf}: downloaded={res['downloaded']}")
            except Exception as e:
                failures.append({"symbol": symbol, "timeframe": tf, "error": str(e)})
                logger.warning(f"  {tf}: failed - {e}")

    close_fn = getattr(exchange, "close", None)
    if callable(close_fn):
        close_fn()

    downloaded_total = int(sum(int(x.get("downloaded", 0) or 0) for x in results))
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at,
        "exchange": exchange_name,
        "top_n_requested": top_n,
        "top_n_selected": len(selected),
        "timeframes": timeframes,
        "days": days,
        "downloaded_rows_total": downloaded_total,
        "results_count": len(results),
        "failures_count": len(failures),
        "selected_universe": selected,
        "skipped": skipped[:200],
        "results": results,
        "failures": failures,
    }

    out_path = Path(args.universe_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "universe_out": str(out_path.resolve()),
                "top_n_selected": len(selected),
                "downloaded_rows_total": downloaded_total,
                "failures_count": len(failures),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
