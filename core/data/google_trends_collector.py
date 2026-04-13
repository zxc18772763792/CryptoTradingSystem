"""Google Trends collector via pytrends (unofficial API).

Installation (optional):
    pip install pytrends

Collects 7-day hourly interest-over-time data for crypto keywords.
Caches to data/google_trends/<keyword>_trends.parquet.

Rate limits: Google returns HTTP 429 below ~60-second inter-request gaps.
_RATE_LIMIT_SEC = 65 is used between consecutive keyword fetches.

Usage (from background task):
    await update_all_keywords()           # update local parquet cache

Usage (from planner/signal):
    val = load_latest("bitcoin")          # 0-100 or None if no cache
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

_CACHE_DIR = Path("data/google_trends")
_KEYWORDS: List[str] = ["bitcoin", "crypto", "ethereum"]
_RATE_LIMIT_SEC: int = 65       # mandatory pause between requests
_TIMEFRAME: str = "now 7-d"     # 7-day hourly granularity


async def fetch_trends_async(keyword: str, timeframe: str = _TIMEFRAME) -> Optional[pd.DataFrame]:
    """Fetch Google Trends interest-over-time for one keyword (runs in thread pool)."""

    def _sync_fetch() -> Optional[pd.DataFrame]:
        try:
            from pytrends.request import TrendReq  # noqa: PLC0415
        except ImportError:
            logger.debug("google_trends: pytrends not installed — skipping")
            return None
        try:
            pt = TrendReq(hl="en-US", tz=0, timeout=(10, 25))
            pt.build_payload([keyword], cat=0, timeframe=timeframe, geo="", gprop="")
            df = pt.interest_over_time()
            if df is None or df.empty:
                return None
            df = df[[keyword]].rename(columns={keyword: "interest"})
            df.index = pd.to_datetime(df.index, utc=True)
            return df
        except Exception as exc:
            logger.debug(f"google_trends: fetch failed for {keyword!r}: {exc}")
            return None

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_fetch)


async def update_all_keywords(keywords: List[str] = _KEYWORDS) -> Dict[str, int]:
    """Fetch and cache all keywords. Returns {keyword: row_count} for succeeded ones."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    results: Dict[str, int] = {}
    for i, kw in enumerate(keywords):
        df = await fetch_trends_async(kw)
        if df is not None and not df.empty:
            path = _CACHE_DIR / f"{kw.replace(' ', '_')}_trends.parquet"
            df.to_parquet(path)
            results[kw] = len(df)
            logger.debug(f"google_trends: cached {len(df)} rows for {kw!r}")
        # Respect rate limit between requests (skip delay after last keyword)
        if i < len(keywords) - 1:
            await asyncio.sleep(_RATE_LIMIT_SEC)
    return results


def load_latest(keyword: str = "bitcoin") -> Optional[float]:
    """Return most recent interest value (0–100) from local cache, or None."""
    path = _CACHE_DIR / f"{keyword.replace(' ', '_')}_trends.parquet"
    try:
        df = pd.read_parquet(path)
        val = float(df["interest"].iloc[-1])
        return val
    except Exception:
        return None


def load_series(keyword: str = "bitcoin", hours: int = 168) -> Optional[pd.Series]:
    """Return recent interest series from local cache, or None."""
    path = _CACHE_DIR / f"{keyword.replace(' ', '_')}_trends.parquet"
    try:
        df = pd.read_parquet(path)
        return df["interest"].iloc[-hours:]
    except Exception:
        return None
