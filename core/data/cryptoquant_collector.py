"""CryptoQuant on-chain data collector (paid API, optional).

API key: https://cryptoquant.com/docs/getting-started
Set CRYPTOQUANT_API_KEY env var or add to config/cryptoquant_api_key.txt.
Auth: Bearer token in Authorization header.

Collected metrics (BTC):
    miner_reserve    — Total miner BTC holdings (drop = potential sell)
    exchange_inflow  — USD inflow to exchanges (selling pressure)
    exchange_outflow — USD outflow from exchanges (accumulation)
    exchange_netflow — inflow - outflow (positive = sell pressure)
    fund_flow_ratio  — Exchange reserve / total supply (high = sell risk)

Cache: data/premium/cryptoquant/<metric>.parquet, refreshed every 4h.

Usage:
    await update_cryptoquant_cache()
    snap = load_cryptoquant_snapshot()
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger

_CACHE_DIR = Path("data/premium/cryptoquant")
_KEY_FILE  = Path(__file__).parent.parent.parent / "config" / "cryptoquant_api_key.txt"
_BASE_URL  = "https://api.cryptoquant.com/v1"
_TIMEOUT   = 20

# (endpoint_path, local_name, value_field)
_METRICS: List[tuple[str, str, str]] = [
    ("/btc/miner-flows/reserve",        "miner_reserve",    "reserve_usd"),
    ("/btc/exchange-flows/inflow",       "exchange_inflow",  "inflow_usd"),
    ("/btc/exchange-flows/outflow",      "exchange_outflow", "outflow_usd"),
    ("/btc/exchange-flows/netflow",      "exchange_netflow", "netflow_usd"),
    ("/btc/exchange-flows/fund-flow-ratio", "fund_flow_ratio", "fund_flow_ratio"),
]


def _api_key() -> str:
    key = os.environ.get("CRYPTOQUANT_API_KEY", "").strip()
    if key:
        return key
    try:
        return _KEY_FILE.read_text().strip()
    except Exception:
        return ""


async def _fetch_metric(
    path: str,
    api_key: str,
    value_field: str,
    window: str = "day",
    limit: int = 3,
) -> Optional[float]:
    """Fetch latest value for one CryptoQuant metric."""
    url = f"{_BASE_URL}{path}"
    params = {"window": window, "limit": limit}
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT)
        ) as session:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 401:
                    logger.debug(f"CryptoQuant: 401 for {path} — check API key")
                    return None
                if resp.status == 403:
                    logger.debug(f"CryptoQuant: 403 for {path} — metric not in plan")
                    return None
                if resp.status != 200:
                    logger.debug(f"CryptoQuant: HTTP {resp.status} for {path}")
                    return None
                body = await resp.json()
                rows: List[Dict[str, Any]] = (body.get("data") or body.get("result") or [])
                if not rows:
                    return None
                latest = rows[-1]
                raw = latest.get(value_field) or latest.get("value")
                return float(raw) if raw is not None else None
    except Exception as exc:
        logger.debug(f"CryptoQuant fetch {path}: {exc}")
        return None


async def update_cryptoquant_cache() -> Dict[str, int]:
    """Fetch all metrics and cache to parquet. Returns {name: 1} for successes."""
    key = _api_key()
    if not key:
        logger.debug("cryptoquant: no API key — skipping (set CRYPTOQUANT_API_KEY)")
        return {}

    import pandas as pd  # noqa: PLC0415

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    updated: Dict[str, int] = {}

    for path, name, field in _METRICS:
        val = await _fetch_metric(path, key, field)
        if val is not None:
            pd.Series(
                {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00"): val},
                name=name, dtype=float,
            ).to_frame().to_parquet(_CACHE_DIR / f"{name}.parquet")
            updated[name] = 1
            logger.debug(f"cryptoquant: cached {name}={val:.4f}")
        await asyncio.sleep(0.5)

    return updated


def load_cryptoquant_snapshot() -> Dict[str, Optional[float]]:
    """Load latest cached values."""
    names = [name for _, name, _ in _METRICS]
    out: Dict[str, Optional[float]] = {n: None for n in names}
    for name in names:
        try:
            import pandas as pd  # noqa: PLC0415
            df = pd.read_parquet(_CACHE_DIR / f"{name}.parquet")
            out[name] = float(df.iloc[-1, 0])
        except Exception:
            pass
    return out
