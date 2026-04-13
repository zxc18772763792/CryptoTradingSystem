"""Download Binance second-level klines from public archive."""
from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests
from loguru import logger

from config.settings import settings
from core.data.path_utils import canonical_symbol_dir, normalize_symbol

_BASE_URL = "https://data.binance.vision/data/spot/daily/klines"


@dataclass
class BinanceArchiveDownloadStats:
    symbol: str
    start_date: date
    end_date: date
    total_days: int = 0
    downloaded_days: int = 0
    skipped_days: int = 0
    missing_days: int = 0
    failed_days: int = 0
    total_rows: int = 0

    def to_dict(self) -> Dict[str, int | str]:
        return {
            "symbol": self.symbol,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "total_days": self.total_days,
            "downloaded_days": self.downloaded_days,
            "skipped_days": self.skipped_days,
            "missing_days": self.missing_days,
            "failed_days": self.failed_days,
            "total_rows": self.total_rows,
        }


def _binance_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).replace("/", "")


def _parts_dir(symbol: str) -> Path:
    return canonical_symbol_dir(Path(settings.DATA_STORAGE_PATH), "binance", symbol) / "1s_parts"


def _build_daily_url(symbol: str, day: date) -> str:
    s = _binance_symbol(symbol)
    return f"{_BASE_URL}/{s}/1s/{s}-1s-{day.isoformat()}.zip"


def _download_zip(url: str, timeout: int = 45, retries: int = 2) -> Optional[bytes]:
    last_error = None
    for _ in range(retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                return response.content
            if response.status_code == 404:
                return None
            last_error = RuntimeError(f"HTTP {response.status_code}")
        except Exception as e:
            last_error = e
    if last_error:
        raise last_error
    return None


def _parse_zip_payload(payload: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        csv_files = [name for name in zf.namelist() if name.endswith(".csv")]
        if not csv_files:
            return pd.DataFrame()
        with zf.open(csv_files[0]) as fp:
            raw = pd.read_csv(fp, header=None)

    if raw.empty:
        return pd.DataFrame()

    raw.columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "num_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]

    out = raw[["open_time", "open", "high", "low", "close", "volume"]].copy()
    open_time = pd.to_numeric(out["open_time"], errors="coerce")
    open_time = open_time.fillna(0).astype("int64")
    ts_unit = "us" if int(open_time.iloc[0]) > 10**14 else "ms"
    out["timestamp"] = pd.to_datetime(open_time, unit=ts_unit, utc=True).dt.tz_localize(None)
    out["open"] = out["open"].astype(float)
    out["high"] = out["high"].astype(float)
    out["low"] = out["low"].astype(float)
    out["close"] = out["close"].astype(float)
    out["volume"] = out["volume"].astype(float)
    out = out.drop(columns=["open_time"]).set_index("timestamp").sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out


def download_binance_1s_daily_archive(
    symbol: str,
    start_date: date,
    end_date: date,
    skip_existing: bool = True,
    timeout: int = 45,
) -> BinanceArchiveDownloadStats:
    symbol = normalize_symbol(symbol)
    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")

    stats = BinanceArchiveDownloadStats(symbol=symbol, start_date=start_date, end_date=end_date)
    out_dir = _parts_dir(symbol)
    out_dir.mkdir(parents=True, exist_ok=True)

    day = start_date
    while day <= end_date:
        stats.total_days += 1
        out_file = out_dir / f"{day.isoformat()}.parquet"

        if skip_existing and out_file.exists():
            stats.skipped_days += 1
            day += timedelta(days=1)
            continue

        url = _build_daily_url(symbol, day)
        try:
            payload = _download_zip(url, timeout=timeout)
            if payload is None:
                stats.missing_days += 1
                logger.warning(f"Binance archive missing: {url}")
                day += timedelta(days=1)
                continue

            df = _parse_zip_payload(payload)
            if df.empty:
                stats.failed_days += 1
                logger.warning(f"Binance archive empty/invalid: {url}")
                day += timedelta(days=1)
                continue

            df.to_parquet(out_file)
            stats.downloaded_days += 1
            stats.total_rows += int(len(df))
            logger.info(f"Downloaded 1s archive {symbol} {day.isoformat()} rows={len(df)}")
        except Exception as e:
            stats.failed_days += 1
            logger.warning(f"Binance archive download failed {symbol} {day.isoformat()}: {e}")

        day += timedelta(days=1)

    return stats
