"""Nansen smart-money on-chain collector (paid API, optional).

API key: https://app.nansen.ai/api-subscription
Set NANSEN_API_KEY env var or add to config/nansen_api_key.txt.
Auth: apiKey header.

Collected metrics:
    smart_money_inflow   — Smart money net token inflow (USD, positive = accumulation)
    smart_money_outflow  — Smart money net token outflow (USD)
    smart_money_netflow  — inflow - outflow
    dex_lp_tvl_change    — DEX LP TVL 24h change % (proxy for liquidity confidence)

Note: Nansen API v2 is still evolving. Endpoints may require plan-specific access.
      This collector targets the public v2 REST API documented at
      https://docs.nansen.ai/api-reference/

Cache: data/premium/nansen/<metric>.parquet, refreshed every 4h.

Usage:
    await update_nansen_cache("ETH")
    snap = load_nansen_snapshot()
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger

_CACHE_DIR = Path("data/premium/nansen")
_KEY_FILE  = Path(__file__).parent.parent.parent / "config" / "nansen_api_key.txt"
_BASE_URL  = "https://api.nansen.ai/v2"
_TIMEOUT   = 20


def _api_key() -> str:
    key = os.environ.get("NANSEN_API_KEY", "").strip()
    if key:
        return key
    try:
        return _KEY_FILE.read_text().strip()
    except Exception:
        return ""


async def _get(
    path: str,
    api_key: str,
    params: Optional[Dict] = None,
) -> Optional[Dict[str, Any]]:
    """Generic authenticated GET to Nansen v2 API."""
    url = f"{_BASE_URL}{path}"
    headers = {"apiKey": api_key, "Accept": "application/json"}
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT)
        ) as session:
            async with session.get(url, params=params or {}, headers=headers) as resp:
                if resp.status == 401:
                    logger.debug("Nansen: 401 — check API key")
                    return None
                if resp.status == 403:
                    logger.debug(f"Nansen: 403 for {path} — not in plan")
                    return None
                if resp.status != 200:
                    logger.debug(f"Nansen: HTTP {resp.status} for {path}")
                    return None
                return await resp.json()
    except Exception as exc:
        logger.debug(f"Nansen fetch {path}: {exc}")
        return None


async def _fetch_smart_money_flows(api_key: str, token: str = "ETH") -> Dict[str, Optional[float]]:
    """Fetch smart money net flows for a token."""
    data = await _get(
        "/smart-money/flow",
        api_key,
        params={"token": token, "period": "24h"},
    )
    result: Dict[str, Optional[float]] = {
        "smart_money_inflow":  None,
        "smart_money_outflow": None,
        "smart_money_netflow": None,
    }
    if not data:
        return result
    try:
        inflow  = float(data.get("inflow")  or data.get("netInflow")  or 0)
        outflow = float(data.get("outflow") or data.get("netOutflow") or 0)
        result["smart_money_inflow"]  = inflow
        result["smart_money_outflow"] = outflow
        result["smart_money_netflow"] = inflow - outflow
    except Exception:
        pass
    return result


async def _fetch_dex_lp_tvl_change(api_key: str) -> Optional[float]:
    """Fetch DEX LP TVL 24h change % as a liquidity confidence proxy."""
    data = await _get("/dex/lp/tvl-summary", api_key, params={"period": "24h"})
    if not data:
        return None
    try:
        return float(data.get("tvl_change_pct") or data.get("changePct") or 0)
    except Exception:
        return None


async def update_nansen_cache(token: str = "ETH") -> Dict[str, int]:
    """Fetch smart money and DEX LP data, cache to parquet."""
    key = _api_key()
    if not key:
        logger.debug("nansen: no API key — skipping (set NANSEN_API_KEY)")
        return {}

    import pandas as pd  # noqa: PLC0415

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    updated: Dict[str, int] = {}
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00")

    flows, lp_chg = await asyncio.gather(
        _fetch_smart_money_flows(key, token),
        _fetch_dex_lp_tvl_change(key),
        return_exceptions=True,
    )

    metrics: Dict[str, Optional[float]] = {}
    if isinstance(flows, dict):
        metrics.update(flows)
    if isinstance(lp_chg, float):
        metrics["dex_lp_tvl_change"] = lp_chg

    for name, val in metrics.items():
        if val is not None:
            pd.Series({ts: val}, name=name, dtype=float).to_frame().to_parquet(
                _CACHE_DIR / f"{name}.parquet"
            )
            updated[name] = 1
            logger.debug(f"nansen: cached {name}={val:.4f}")

    return updated


def load_nansen_snapshot() -> Dict[str, Optional[float]]:
    """Load latest cached Nansen values."""
    names = ["smart_money_inflow", "smart_money_outflow", "smart_money_netflow", "dex_lp_tvl_change"]
    out: Dict[str, Optional[float]] = {n: None for n in names}
    for name in names:
        try:
            import pandas as pd  # noqa: PLC0415
            df = pd.read_parquet(_CACHE_DIR / f"{name}.parquet")
            out[name] = float(df.iloc[-1, 0])
        except Exception:
            pass
    return out
