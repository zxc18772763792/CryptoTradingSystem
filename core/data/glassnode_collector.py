"""Glassnode on-chain data collector (paid API, optional).

API key: https://studio.glassnode.com/settings/api
Set GLASSNODE_API_KEY env var or add to config/glassnode_api_key.txt.

Free tier covers daily resolution for most metrics.
Paid tiers unlock hourly/10-min resolution and advanced metrics.

Collected metrics (all BTC by default):
    sopr             — Spent Output Profit Ratio (>1=profit realised, <1=loss)
    mvrv_z           — MVRV Z-Score (>7=top, <0=bottom)
    exchange_netflow — Net exchange flow USD/day (>0=inflow=sell pressure)
    nvt              — Network Value to Transactions ratio

Cache: data/premium/glassnode/<metric>.parquet, refreshed every 4h.

Usage:
    await update_glassnode_cache()
    snap = load_glassnode_snapshot()   # {"sopr": 1.02, "mvrv_z": 2.1, ...}
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger

_CACHE_DIR  = Path("data/premium/glassnode")
_KEY_FILE   = Path(__file__).parent.parent.parent / "config" / "glassnode_api_key.txt"
_BASE_URL   = "https://api.glassnode.com/v1/metrics"
_TIMEOUT    = 20

# (category, metric, local_name)
_METRICS: List[tuple[str, str, str]] = [
    ("indicators",    "sopr",                                "sopr"),
    ("market",        "mvrv_z_score",                        "mvrv_z"),
    ("transactions",  "transfers_volume_exchanges_net",       "exchange_netflow"),
    ("transactions",  "nvt",                                  "nvt"),
]


def _api_key() -> str:
    key = os.environ.get("GLASSNODE_API_KEY", "").strip()
    if key:
        return key
    try:
        return _KEY_FILE.read_text().strip()
    except Exception:
        return ""


async def _fetch_metric(
    category: str,
    metric: str,
    api_key: str,
    asset: str = "BTC",
    interval: str = "24h",
    days: int = 30,
) -> Optional[float]:
    """Fetch latest value for one Glassnode metric. Returns None on any error."""
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    url = f"{_BASE_URL}/{category}/{metric}"
    params = {"a": asset, "i": interval, "s": since, "api_key": api_key}
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT)
        ) as session:
            async with session.get(url, params=params) as resp:
                if resp.status == 401:
                    logger.debug(f"Glassnode: 401 for {metric} — check API key")
                    return None
                if resp.status == 403:
                    logger.debug(f"Glassnode: 403 for {metric} — metric not in plan tier")
                    return None
                if resp.status != 200:
                    logger.debug(f"Glassnode: HTTP {resp.status} for {metric}")
                    return None
                data: List[Dict[str, Any]] = await resp.json()
                if not data:
                    return None
                # Latest entry is last; Glassnode returns [{t, v}, ...]
                return float(data[-1]["v"])
    except Exception as exc:
        logger.debug(f"Glassnode fetch {metric}: {exc}")
        return None


async def update_glassnode_cache(asset: str = "BTC") -> Dict[str, int]:
    """Fetch all configured metrics and persist to parquet. Returns {name: 1} for successes."""
    key = _api_key()
    if not key:
        logger.debug("glassnode: no API key — skipping (set GLASSNODE_API_KEY or config/glassnode_api_key.txt)")
        return {}

    import pandas as pd  # noqa: PLC0415

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    updated: Dict[str, int] = {}

    for category, metric, name in _METRICS:
        val = await _fetch_metric(category, metric, key, asset=asset)
        if val is not None:
            path = _CACHE_DIR / f"{name}.parquet"
            pd.Series(
                {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00"): val},
                name=name, dtype=float,
            ).to_frame().to_parquet(path)
            updated[name] = 1
            logger.debug(f"glassnode: cached {name}={val:.4f}")
        await asyncio.sleep(1)  # be polite to the API

    return updated


def load_glassnode_snapshot() -> Dict[str, Optional[float]]:
    """Load latest cached values. Returns None for missing/unavailable metrics."""
    names = [name for _, _, name in _METRICS]
    out: Dict[str, Optional[float]] = {n: None for n in names}
    for name in names:
        try:
            import pandas as pd  # noqa: PLC0415
            df = pd.read_parquet(_CACHE_DIR / f"{name}.parquet")
            out[name] = float(df.iloc[-1, 0])
        except Exception:
            pass
    return out
