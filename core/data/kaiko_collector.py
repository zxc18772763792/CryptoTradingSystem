"""Kaiko market microstructure collector (paid API, optional).

API key: https://www.kaiko.com/pages/cryptocurrency-api-documentation
Set KAIKO_API_KEY env var or add to config/kaiko_api_key.txt.
Auth: X-Api-Key header.

Kaiko provides institutional-grade L2 tick data and cross-exchange analytics.

Collected metrics:
    cross_exchange_spread_bps — BTC/USD bid-ask spread across top exchanges (bps)
    liquidity_depth_1pct      — Aggregated 1% depth USD (bid + ask, across exchanges)
    vwap_deviation_bps        — VWAP deviation from mid-price (bps)
    trade_count_1h            — Normalised trade count last hour (proxy for activity)

Cache: data/premium/kaiko/<metric>.parquet, refreshed every 1h.

Usage:
    await update_kaiko_cache("BTC/USD")
    snap = load_kaiko_snapshot()
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger

_CACHE_DIR = Path("data/premium/kaiko")
_KEY_FILE  = Path(__file__).parent.parent.parent / "config" / "kaiko_api_key.txt"
_BASE_URL  = "https://us.market-api.kaiko.io/v2/data"
_TIMEOUT   = 20

# Map of local name → Kaiko aggregation endpoint + extraction path
_METRICS: List[tuple[str, str, str, Dict]] = [
    (
        "cross_exchange_spread_bps",
        "/order_book_aggregations.latest/exchanges/aggregated/pairs/btc-usd/snapshot",
        "spread_bps",
        {"depth": "0.01"},
    ),
    (
        "liquidity_depth_1pct",
        "/order_book_aggregations.latest/exchanges/aggregated/pairs/btc-usd/snapshot",
        "depth_1pct_usd",
        {"depth": "0.01"},
    ),
    (
        "trade_count_1h",
        "/trades.v1/exchanges/aggregated/pairs/btc-usd/aggregations/count_ohlcv_vwap",
        "count",
        {"start_time": "1h", "interval": "1h"},
    ),
]


def _api_key() -> str:
    key = os.environ.get("KAIKO_API_KEY", "").strip()
    if key:
        return key
    try:
        return _KEY_FILE.read_text().strip()
    except Exception:
        return ""


async def _get(path: str, api_key: str, params: Optional[Dict] = None) -> Optional[Any]:
    """Authenticated GET to Kaiko API."""
    url = f"{_BASE_URL}{path}"
    headers = {"X-Api-Key": api_key, "Accept": "application/json"}
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT)
        ) as session:
            async with session.get(url, params=params or {}, headers=headers) as resp:
                if resp.status == 401:
                    logger.debug("Kaiko: 401 — check API key")
                    return None
                if resp.status == 403:
                    logger.debug(f"Kaiko: 403 for {path} — not in plan")
                    return None
                if resp.status != 200:
                    logger.debug(f"Kaiko: HTTP {resp.status} for {path}")
                    return None
                return await resp.json()
    except Exception as exc:
        logger.debug(f"Kaiko fetch {path}: {exc}")
        return None


def _extract(data: Any, field: str) -> Optional[float]:
    """Best-effort extraction of a numeric field from Kaiko response."""
    if data is None:
        return None
    try:
        # Kaiko typically wraps in {"data": [...]} or {"result": [...]}
        rows = data.get("data") or data.get("result") or [data]
        if isinstance(rows, list) and rows:
            row = rows[-1]
        else:
            row = rows if isinstance(rows, dict) else data
        val = row.get(field)
        return float(val) if val is not None else None
    except Exception:
        return None


async def update_kaiko_cache(pair: str = "BTC/USD") -> Dict[str, int]:
    """Fetch cross-exchange microstructure metrics and cache to parquet."""
    key = _api_key()
    if not key:
        logger.debug("kaiko: no API key — skipping (set KAIKO_API_KEY)")
        return {}

    import pandas as pd  # noqa: PLC0415

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    updated: Dict[str, int] = {}
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00")

    for name, path, field, params in _METRICS:
        data = await _get(path, key, params)
        val = _extract(data, field)
        if val is not None:
            pd.Series({ts: val}, name=name, dtype=float).to_frame().to_parquet(
                _CACHE_DIR / f"{name}.parquet"
            )
            updated[name] = 1
            logger.debug(f"kaiko: cached {name}={val:.4f}")
        await asyncio.sleep(0.3)

    return updated


def load_kaiko_snapshot() -> Dict[str, Optional[float]]:
    """Load latest cached Kaiko values."""
    names = [name for name, _, _, _ in _METRICS]
    out: Dict[str, Optional[float]] = {n: None for n in names}
    for name in names:
        try:
            import pandas as pd  # noqa: PLC0415
            df = pd.read_parquet(_CACHE_DIR / f"{name}.parquet")
            out[name] = float(df.iloc[-1, 0])
        except Exception:
            pass
    return out
