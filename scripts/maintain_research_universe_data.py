"""Incrementally maintain the default 30-symbol research universe in local parquet storage."""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger
import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

from core.data import data_storage, download_binance_1s_daily_archive, second_level_backfill_manager  # noqa: E402
from core.exchanges import exchange_manager  # noqa: E402
from maintain_top100_data import _download_symbol_timeframe, _load_binance_spot_usdt_symbols  # noqa: E402


DEFAULT_RESEARCH_SYMBOLS: List[str] = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "DOGE/USDT",
    "TRX/USDT",
    "LINK/USDT",
    "AVAX/USDT",
    "DOT/USDT",
    "POL/USDT",
    "LTC/USDT",
    "BCH/USDT",
    "ETC/USDT",
    "ATOM/USDT",
    "NEAR/USDT",
    "APT/USDT",
    "ARB/USDT",
    "OP/USDT",
    "SUI/USDT",
    "INJ/USDT",
    "RUNE/USDT",
    "AAVE/USDT",
    "MKR/USDT",
    "UNI/USDT",
    "FIL/USDT",
    "HBAR/USDT",
    "ICP/USDT",
    "TON/USDT",
]
DEFAULT_IDLE_SECONDS_SYMBOLS: List[str] = ["BTC/USDT", "ETH/USDT"]
DEFAULT_IDLE_SECONDS_DAYS = 1
IDLE_SECONDS_RECENT_MAX_AGE_MINUTES = 12
IDLE_SECONDS_LIVE_WINDOW_MINUTES = 20
IDLE_SECONDS_ARCHIVE_TIMEOUT_SEC = 75
IDLE_SECONDS_LIVE_FETCH_TIMEOUT_SEC = 45
IDLE_SECONDS_TRADES_FETCH_TIMEOUT_SEC = 45
IDLE_SECONDS_SYMBOL_TIMEOUT_SEC = 120
BINANCE_PUBLIC_KLINES_URL = "https://api.binance.com/api/v3/klines"


def _normalize_symbol_list(raw_symbols: List[str]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for raw in raw_symbols:
        text = str(raw or "").strip().upper()
        if not text:
            continue
        symbol = text if "/" in text else f"{text}/USDT"
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _observed_heavy_worker_pids() -> List[int]:
    command = (
        "Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | "
        "Where-Object { "
        "$n=[string]$_.Name; "
        "if ($n -and $n.ToLowerInvariant() -notin @('python.exe','pythonw.exe')) { return $false }; "
        "$cmd=[string]$_.CommandLine; "
        "if (-not $cmd) { return $false }; "
        "$lower=$cmd.ToLowerInvariant(); "
        "return $lower.Contains('core.news.service.worker') -or "
        "$lower.Contains('core.news.service.llm_worker') -or "
        "$lower.Contains('prediction_markets.polymarket.worker') "
        "} | Select-Object -ExpandProperty ProcessId | ConvertTo-Json -Compress"
    )
    try:
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
        )
    except Exception:
        return []
    if proc.returncode != 0:
        return []
    text = str(proc.stdout or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except Exception:
        return []
    if isinstance(payload, list):
        return [int(item) for item in payload if str(item).strip().isdigit()]
    if str(payload).strip().isdigit():
        return [int(payload)]
    return []


def _write_summary(summary_path: Path, payload: Dict[str, Any]) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _fetch_binance_public_1s_window_sync(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    limit: int = 1000,
    timeout: int = 15,
) -> pd.DataFrame:
    market = symbol.replace("/", "").upper()
    since_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)
    rows: List[Dict[str, Any]] = []

    while since_ms <= end_ms:
        response = requests.get(
            BINANCE_PUBLIC_KLINES_URL,
            params={
                "symbol": market,
                "interval": "1s",
                "startTime": since_ms,
                "endTime": end_ms,
                "limit": max(1, min(int(limit), 1000)),
            },
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            break

        max_open_ms = since_ms
        for item in payload:
            open_ms = int(item[0])
            if open_ms > end_ms:
                continue
            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc).replace(tzinfo=None),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                }
            )
            if open_ms > max_open_ms:
                max_open_ms = open_ms

        if len(payload) < limit:
            break
        if max_open_ms <= since_ms:
            since_ms += 1000
        else:
            since_ms = max_open_ms + 1000

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("timestamp").sort_index()
    return df[~df.index.duplicated(keep="last")]


async def _has_recent_kline(
    exchange: str,
    symbol: str,
    timeframe: str,
    max_age_minutes: int = IDLE_SECONDS_RECENT_MAX_AGE_MINUTES,
) -> bool:
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=max(1, int(max_age_minutes)))
    try:
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception:
        return False
    return df is not None and not df.empty


async def _refresh_recent_1s_symbol(exchange_name: str, symbol: str, days: int) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "exchange": exchange_name,
        "symbol": symbol,
        "timeframe": "1s",
        "archive_rows": 0,
        "recent_rows": 0,
        "saved_rows": 0,
        "skipped": False,
    }

    if await _has_recent_kline(exchange=exchange_name, symbol=symbol, timeframe="1s"):
        result["skipped"] = True
        result["reason"] = "recent_1s_data_already_present"
        return result

    end_time = datetime.now(timezone.utc)
    archive_end = end_time.date() - timedelta(days=1)
    archive_start = archive_end - timedelta(days=max(1, int(days)) - 1)
    if archive_end >= archive_start:
        try:
            archive_stats = await asyncio.wait_for(
                asyncio.to_thread(
                    download_binance_1s_daily_archive,
                    symbol,
                    archive_start,
                    archive_end,
                    True,
                ),
                timeout=IDLE_SECONDS_ARCHIVE_TIMEOUT_SEC,
            )
            result["archive_rows"] = int(getattr(archive_stats, "total_rows", 0) or 0)
            result["archive_days"] = int(getattr(archive_stats, "days_processed", 0) or 0)
        except asyncio.TimeoutError:
            result["archive_timeout"] = True
            result["archive_error"] = f"archive_timeout_after_{IDLE_SECONDS_ARCHIVE_TIMEOUT_SEC}s"
        except Exception as exc:
            result["archive_error"] = str(exc)

    today_start = datetime(end_time.year, end_time.month, end_time.day, tzinfo=timezone.utc)
    fetch_start = max(today_start, end_time - timedelta(minutes=IDLE_SECONDS_LIVE_WINDOW_MINUTES))
    try:
        existing_today = await data_storage.load_klines_from_parquet(
            exchange=exchange_name,
            symbol=symbol,
            timeframe="1s",
            start_time=today_start,
            end_time=end_time,
        )
    except Exception:
        existing_today = None
    if existing_today is not None and not existing_today.empty:
        latest_ts = existing_today.index.max().to_pydatetime()
        fetch_start = max(today_start, latest_ts - timedelta(seconds=90))
    result["recent_window_start"] = fetch_start.isoformat()

    try:
        if exchange_name == "binance":
            bars_df = await asyncio.wait_for(
                asyncio.to_thread(
                    _fetch_binance_public_1s_window_sync,
                    symbol,
                    fetch_start,
                    end_time,
                ),
                timeout=IDLE_SECONDS_LIVE_FETCH_TIMEOUT_SEC,
            )
        else:
            bars_df = await asyncio.wait_for(
                second_level_backfill_manager._fetch_1s_klines_window(
                    exchange=exchange_name,
                    symbol=symbol,
                    start_time=fetch_start,
                    end_time=end_time,
                ),
                timeout=IDLE_SECONDS_LIVE_FETCH_TIMEOUT_SEC,
            )
    except asyncio.TimeoutError:
        result["live_fetch_timeout"] = True
        bars_df = pd.DataFrame()
    except Exception as exc:
        result["live_fetch_error"] = str(exc)
        bars_df = pd.DataFrame()
    trades_count = 0
    if bars_df.empty:
        try:
            trades = await asyncio.wait_for(
                second_level_backfill_manager._fetch_trades_window(
                    exchange=exchange_name,
                    symbol=symbol,
                    start_time=fetch_start,
                    end_time=end_time,
                ),
                timeout=IDLE_SECONDS_TRADES_FETCH_TIMEOUT_SEC,
            )
            trades_count = len(trades)
            bars_df = second_level_backfill_manager._trades_to_1s(trades)
        except asyncio.TimeoutError:
            result["trades_fetch_timeout"] = True
            bars_df = pd.DataFrame()
        except Exception as exc:
            result["trades_fetch_error"] = str(exc)
            bars_df = pd.DataFrame()

    inserted = 0
    if bars_df is not None and not bars_df.empty:
        inserted = second_level_backfill_manager._save_parts(exchange_name, symbol, bars_df)
    elif not result.get("archive_rows"):
        result["reason"] = result.get("reason") or "no_recent_1s_rows"
    result["recent_rows"] = int(len(bars_df))
    result["saved_rows"] = int(inserted)
    result["trades_count"] = int(trades_count)
    return result


async def _refresh_idle_seconds_if_allowed(
    exchange_name: str,
    symbols: List[str],
    days: int,
    disable_idle_seconds: bool,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "enabled": False,
        "attempted": False,
        "symbols": symbols,
        "days": int(days),
        "results": [],
    }
    if disable_idle_seconds:
        payload["reason"] = "disabled_by_flag"
        return payload

    heavy_worker_pids = _observed_heavy_worker_pids()
    if heavy_worker_pids:
        payload["reason"] = "heavy_workers_running"
        payload["heavy_worker_pids"] = heavy_worker_pids
        return payload

    payload["enabled"] = True
    payload["attempted"] = True
    await exchange_manager.initialize([exchange_name])
    try:
        for symbol in symbols:
            try:
                item = await asyncio.wait_for(
                    _refresh_recent_1s_symbol(exchange_name=exchange_name, symbol=symbol, days=days),
                    timeout=IDLE_SECONDS_SYMBOL_TIMEOUT_SEC,
                )
                payload["results"].append(item)
                logger.info(
                    "  1s idle refresh {}: archive_rows={} recent_rows={} saved_rows={} skipped={}",
                    symbol,
                    int(item.get("archive_rows", 0) or 0),
                    int(item.get("recent_rows", 0) or 0),
                    int(item.get("saved_rows", 0) or 0),
                    bool(item.get("skipped")),
                )
            except asyncio.TimeoutError:
                payload["results"].append(
                    {
                        "exchange": exchange_name,
                        "symbol": symbol,
                        "timeframe": "1s",
                        "error": f"timeout_after_{IDLE_SECONDS_SYMBOL_TIMEOUT_SEC}s",
                    }
                )
                logger.warning("  1s idle refresh {} failed - timeout after {}s", symbol, IDLE_SECONDS_SYMBOL_TIMEOUT_SEC)
            except Exception as exc:
                payload["results"].append(
                    {
                        "exchange": exchange_name,
                        "symbol": symbol,
                        "timeframe": "1s",
                        "error": str(exc),
                    }
                )
                logger.warning("  1s idle refresh {} failed - {}", symbol, exc)
    finally:
        await exchange_manager.close_all()

    payload["downloaded_rows_total"] = int(
        sum(int(item.get("archive_rows", 0) or 0) + int(item.get("recent_rows", 0) or 0) for item in payload["results"])
    )
    payload["failures_count"] = int(sum(1 for item in payload["results"] if item.get("error")))
    return payload


async def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally maintain the default 30-symbol research universe.")
    parser.add_argument("--exchange", default="binance", help="Exchange name for local parquet storage.")
    parser.add_argument("--timeframes", default="1m,5m,15m", help="Comma-separated timeframes to refresh.")
    parser.add_argument("--days", type=int, default=90, help="Initial lookback days when local data is missing.")
    parser.add_argument("--overlap-bars", type=int, default=48, help="Overlap bars for safe incremental refresh.")
    parser.add_argument(
        "--seconds-symbols",
        default="BTC/USDT,ETH/USDT",
        help="Comma-separated symbols to opportunistically refresh at 1s when the machine is idle.",
    )
    parser.add_argument(
        "--seconds-days",
        type=int,
        default=DEFAULT_IDLE_SECONDS_DAYS,
        help="1s archive lookback days for idle refresh.",
    )
    parser.add_argument("--disable-idle-seconds", action="store_true", help="Disable the opportunistic BTC/ETH 1s refresh.")
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated symbol override. Defaults to the built-in 30-symbol research universe.",
    )
    parser.add_argument(
        "--summary-out",
        default="data/research/research_universe_incremental_latest.json",
        help="Path to write the latest maintenance summary JSON.",
    )
    args = parser.parse_args()

    exchange_name = str(args.exchange or "binance").strip().lower() or "binance"
    timeframes = [item.strip().lower() for item in str(args.timeframes or "1m,5m,15m").split(",") if item.strip()]
    if not timeframes:
        timeframes = ["1m", "5m", "15m"]
    days = max(7, int(args.days or 90))
    overlap_bars = max(8, int(args.overlap_bars or 48))
    seconds_symbols = _normalize_symbol_list(
        str(args.seconds_symbols or "").split(",")
        if str(args.seconds_symbols or "").strip()
        else list(DEFAULT_IDLE_SECONDS_SYMBOLS)
    )
    seconds_days = max(1, min(3, int(args.seconds_days or DEFAULT_IDLE_SECONDS_DAYS)))
    requested_symbols = _normalize_symbol_list(
        str(args.symbols or "").split(",") if str(args.symbols or "").strip() else list(DEFAULT_RESEARCH_SYMBOLS)
    )

    logger.info(
        "Research universe incremental refresh start: exchange={} symbols={} timeframes={} days={} overlap={} idle_seconds={}",
        exchange_name,
        len(requested_symbols),
        timeframes,
        days,
        overlap_bars,
        seconds_symbols,
    )

    exchange, spot_symbols = _load_binance_spot_usdt_symbols()
    selected_symbols = [symbol for symbol in requested_symbols if symbol in spot_symbols]
    skipped_symbols = [symbol for symbol in requested_symbols if symbol not in spot_symbols]

    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    started_at = datetime.now(timezone.utc).isoformat()
    summary_path = Path(args.summary_out)
    if not summary_path.is_absolute():
        summary_path = PROJECT_ROOT / summary_path

    _write_summary(
        summary_path,
        {
            "status": "running",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at,
            "exchange": exchange_name,
            "timeframes": timeframes,
            "days": days,
            "overlap_bars": overlap_bars,
            "seconds_symbols": seconds_symbols,
            "seconds_days": seconds_days,
            "requested_symbols": requested_symbols,
            "selected_symbols": selected_symbols,
            "skipped_symbols": skipped_symbols,
            "downloaded_rows_total": 0,
            "results_count": 0,
            "failures_count": 0,
            "results": [],
            "failures": [],
            "idle_seconds": {
                "enabled": False,
                "attempted": False,
                "symbols": [symbol for symbol in seconds_symbols if symbol in selected_symbols],
                "days": seconds_days,
                "results": [],
            },
        },
    )

    for idx, symbol in enumerate(selected_symbols, start=1):
        logger.info("[{}/{}] Refresh {}", idx, len(selected_symbols), symbol)
        for timeframe in timeframes:
            try:
                result = await _download_symbol_timeframe(
                    exchange=exchange,
                    exchange_name=exchange_name,
                    symbol=symbol,
                    timeframe=timeframe,
                    days=days,
                    overlap_bars=overlap_bars,
                )
                results.append(result)
                logger.info("  {}: downloaded={}", timeframe, int(result.get("downloaded", 0) or 0))
            except Exception as exc:
                failures.append({"symbol": symbol, "timeframe": timeframe, "error": str(exc)})
                logger.warning("  {}: failed - {}", timeframe, exc)

    idle_seconds = await _refresh_idle_seconds_if_allowed(
        exchange_name=exchange_name,
        symbols=[symbol for symbol in seconds_symbols if symbol in selected_symbols],
        days=seconds_days,
        disable_idle_seconds=bool(args.disable_idle_seconds),
    )

    close_fn = getattr(exchange, "close", None)
    if callable(close_fn):
        close_fn()

    downloaded_rows_total = int(sum(int(item.get("downloaded", 0) or 0) for item in results))
    summary = {
        "status": "completed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at,
        "exchange": exchange_name,
        "timeframes": timeframes,
        "days": days,
        "overlap_bars": overlap_bars,
        "seconds_symbols": seconds_symbols,
        "seconds_days": seconds_days,
        "requested_symbols": requested_symbols,
        "selected_symbols": selected_symbols,
        "skipped_symbols": skipped_symbols,
        "downloaded_rows_total": downloaded_rows_total,
        "results_count": len(results),
        "failures_count": len(failures),
        "results": results,
        "failures": failures,
        "idle_seconds": idle_seconds,
    }

    _write_summary(summary_path, summary)

    print(
        json.dumps(
            {
                "summary_out": str(summary_path.resolve()),
                "selected_symbols": len(selected_symbols),
                "downloaded_rows_total": downloaded_rows_total,
                "failures_count": len(failures),
                "idle_seconds_enabled": bool(idle_seconds.get("enabled")),
                "idle_seconds_failures": int(idle_seconds.get("failures_count", 0) or 0),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
