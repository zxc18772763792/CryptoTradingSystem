"""Macro indicator collector — yfinance primary, FRED supplement.

Primary (zero config, pip install yfinance):
    vix      — CBOE VIX         (^VIX)
    dxy      — USD Index        (DX-Y.NYB)
    tnx_10y  — 10Y Treasury     (^TNX)

Supplementary (free FRED API key, auto-loaded from config/fred_api_key.txt
or FRED_API_KEY env var):
    fed_rate — Federal Funds Rate  (FEDFUNDS)
    cpi_yoy  — CPI YoY %          (CPIAUCSL, computed locally)

Cache: data/macro/<name>.parquet, refreshed daily via background worker.

Usage:
    await update_macro_cache()      # fetch + persist (background task)
    snap = load_macro_snapshot()    # {name: float|None} from local cache
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

from loguru import logger

_CACHE_DIR = Path("data/macro")
_KEY_FILE   = Path(__file__).parent.parent.parent / "config" / "fred_api_key.txt"
_FRED_BASE  = "https://api.stlouisfed.org/fred/series/observations"
_TIMEOUT_SEC = 15

# FRED-only series (yfinance doesn't carry these reliably)
_SERIES_FRED: Dict[str, str] = {
    "fed_rate": "FEDFUNDS",
    "cpi_yoy":  "CPIAUCSL",
}

# yfinance tickers → local name
_SERIES_YF: Dict[str, str] = {
    "^VIX":      "vix",
    "DX-Y.NYB":  "dxy",
    "^TNX":      "tnx_10y",
}


# ── helpers ──────────────────────────────────────────────────────────────────

def _api_key() -> str:
    """Read FRED key from env var first, then key file."""
    key = os.environ.get("FRED_API_KEY", "").strip()
    if key:
        return key
    try:
        key = _KEY_FILE.read_text().strip()
    except Exception:
        pass
    return key


def _compute_yoy(series) -> Optional[float]:
    """Compute most-recent YoY % change for a monthly series."""
    try:
        import pandas as pd  # noqa: PLC0415
        idx = pd.to_datetime(series.index)
        s = series.copy()
        s.index = idx
        s = s.sort_index().dropna()
        if len(s) < 13:
            return None
        latest, year_ago = float(s.iloc[-1]), float(s.iloc[-13])
        return round((latest / year_ago - 1) * 100, 4) if year_ago else None
    except Exception:
        return None


# ── yfinance (primary, no key) ────────────────────────────────────────────────

async def _fetch_yfinance_macro() -> Dict[str, Optional[float]]:
    """Fetch VIX, DXY, 10Y yield via yfinance (runs in thread pool)."""

    def _sync() -> Dict[str, Optional[float]]:
        try:
            import yfinance as yf  # noqa: PLC0415
        except ImportError:
            logger.debug("macro_collector: yfinance not installed — run: pip install yfinance")
            return {}
        result: Dict[str, Optional[float]] = {}
        tickers = list(_SERIES_YF.keys())
        try:
            import pandas as pd  # noqa: PLC0415
            raw = yf.download(tickers, period="5d", progress=False, auto_adjust=True)
            # yfinance returns MultiIndex columns when multiple tickers
            close = raw["Close"] if "Close" in raw.columns else raw
            for ticker, name in _SERIES_YF.items():
                try:
                    col = close[ticker] if ticker in close.columns else close.get(ticker)
                    result[name] = float(col.dropna().iloc[-1]) if col is not None else None
                except Exception:
                    result[name] = None
        except Exception as exc:
            logger.debug(f"macro_collector: yfinance batch failed: {exc}")
            # Fallback: fetch one by one
            for ticker, name in _SERIES_YF.items():
                try:
                    t = yf.Ticker(ticker)
                    hist = t.history(period="5d")
                    result[name] = float(hist["Close"].dropna().iloc[-1]) if not hist.empty else None
                except Exception:
                    result[name] = None
        return result

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync)


# ── FRED (supplement, requires free API key) ──────────────────────────────────

async def _fetch_fred_series(series_id: str, api_key: str, days: int = 730):
    """Fetch one FRED series. Returns pd.Series or None."""
    import aiohttp  # noqa: PLC0415
    import pandas as pd  # noqa: PLC0415

    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "series_id": series_id, "api_key": api_key,
        "file_type": "json", "observation_start": start, "sort_order": "asc",
    }
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_SEC)
        ) as session:
            async with session.get(_FRED_BASE, params=params) as resp:
                if resp.status != 200:
                    logger.debug(f"FRED {series_id}: HTTP {resp.status}")
                    return None
                data = await resp.json()
                records: Dict[str, float] = {}
                for o in data.get("observations") or []:
                    try:
                        records[o["date"]] = float(o["value"])
                    except (KeyError, ValueError, TypeError):
                        pass
                if not records:
                    return None
                return pd.Series(records, name=series_id, dtype=float)
    except Exception as exc:
        logger.debug(f"FRED fetch {series_id}: {exc}")
        return None


async def _fetch_fred_macro(api_key: str) -> Dict[str, Optional[float]]:
    """Fetch FRED-only series and return {name: latest_value}."""
    result: Dict[str, Optional[float]] = {}
    for name, series_id in _SERIES_FRED.items():
        s = await _fetch_fred_series(series_id, api_key)
        if s is not None and not s.empty:
            # Persist raw series to parquet
            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
            s.to_frame().to_parquet(_CACHE_DIR / f"{name}.parquet")
            if name == "cpi_yoy":
                result[name] = _compute_yoy(s)
            else:
                result[name] = float(s.iloc[-1])
            logger.debug(f"macro_collector: FRED cached {name} ({series_id}), latest={result[name]}")
        else:
            result[name] = None
    return result


# ── public API ────────────────────────────────────────────────────────────────

async def update_macro_cache() -> Dict[str, int]:
    """Fetch all macro data and write to local parquet cache.

    Always tries yfinance (VIX/DXY/TNX).
    Also tries FRED (fed_rate/cpi_yoy) when API key is available.
    Returns {name: 1} for each successfully cached series.
    """
    import pandas as pd  # noqa: PLC0415

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    updated: Dict[str, int] = {}

    # 1. yfinance (primary, always attempted)
    yf_data = await _fetch_yfinance_macro()
    for name, val in yf_data.items():
        if val is not None:
            path = _CACHE_DIR / f"{name}.parquet"
            pd.Series(
                {datetime.now(timezone.utc).strftime("%Y-%m-%d"): val},
                name=name, dtype=float,
            ).to_frame().to_parquet(path)
            updated[name] = 1
            logger.debug(f"macro_collector: yfinance cached {name}={val:.4f}")

    # 2. FRED (supplement, only if key available)
    key = _api_key()
    if key:
        fred_data = await _fetch_fred_macro(key)
        for name, val in fred_data.items():
            if val is not None:
                updated[name] = 1
    else:
        logger.debug("macro_collector: no FRED key — skipping fed_rate/cpi_yoy")

    return updated


def load_macro_snapshot() -> Dict[str, Optional[float]]:
    """Read latest cached values. Returns None for missing/corrupt series."""
    all_names = list(_SERIES_YF.values()) + list(_SERIES_FRED.keys())
    out: Dict[str, Optional[float]] = {n: None for n in all_names}

    for name in all_names:
        path = _CACHE_DIR / f"{name}.parquet"
        try:
            import pandas as pd  # noqa: PLC0415
            df = pd.read_parquet(path)
            raw = float(df.iloc[-1, 0])
            # cpi_yoy is already stored as YoY % (computed during FRED fetch)
            out[name] = raw
        except Exception:
            pass

    return out
