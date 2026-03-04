"""Reusable cost model helpers for backtest/research."""
from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


def fee_rate(config: Any, role: str = "taker") -> float:
    model = str(getattr(config, "fee_model", "flat") or "flat").lower()
    if model == "maker_taker":
        fee = getattr(config, "maker_fee", 0.0) if str(role).lower() == "maker" else getattr(config, "taker_fee", 0.0)
        return float(max(0.0, float(fee or 0.0)))
    return float(max(0.0, float(getattr(config, "commission_rate", 0.0) or 0.0)))


def dynamic_slippage_rate(
    atr_pct: float,
    realized_vol: float,
    spread_proxy: float,
    params: Dict[str, float],
) -> float:
    p = dict(params or {})
    min_slip = float(p.get("min_slip", 0.0))
    k_atr = float(p.get("k_atr", 0.0))
    k_rv = float(p.get("k_rv", 0.0))
    k_spread = float(p.get("k_spread", 0.0))
    return float(
        max(
            min_slip,
            k_atr * max(0.0, atr_pct) + k_rv * max(0.0, realized_vol) + k_spread * max(0.0, spread_proxy),
        )
    )


def microstructure_proxies(window: Optional[pd.DataFrame]) -> Dict[str, float]:
    if window is None or window.empty:
        return {"atr_pct": 0.0, "realized_vol": 0.0, "spread_proxy": 0.0}

    w = window.tail(120)
    close = pd.to_numeric(w.get("close"), errors="coerce")
    high = pd.to_numeric(w.get("high"), errors="coerce")
    low = pd.to_numeric(w.get("low"), errors="coerce")
    if close.empty or close.dropna().empty:
        return {"atr_pct": 0.0, "realized_vol": 0.0, "spread_proxy": 0.0}

    ret = np.log(close / close.shift(1))
    realized_vol = float(ret.tail(60).std(ddof=0) or 0.0)

    spread_proxy = 0.0
    if high is not None and low is not None and len(high) and len(low):
        latest_range = ((high - low) / close.replace(0, np.nan)).iloc[-1]
        spread_proxy = float(latest_range) if np.isfinite(latest_range) else 0.0

    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr = float(tr.tail(30).mean() or 0.0)
    last_close = max(float(close.iloc[-1] or 0.0), 1e-12)
    atr_pct = atr / last_close

    return {
        "atr_pct": max(0.0, float(atr_pct if np.isfinite(atr_pct) else 0.0)),
        "realized_vol": max(0.0, float(realized_vol if np.isfinite(realized_vol) else 0.0)),
        "spread_proxy": max(0.0, float(spread_proxy)),
    }


def slippage_rate(config: Any, window: Optional[pd.DataFrame] = None) -> float:
    model = str(getattr(config, "slippage_model", "flat") or "flat").lower()
    if model != "dynamic":
        return float(max(0.0, float(getattr(config, "slippage", 0.0) or 0.0)))

    metrics = microstructure_proxies(window)
    return float(
        max(
            0.0,
            dynamic_slippage_rate(
                atr_pct=float(metrics.get("atr_pct", 0.0)),
                realized_vol=float(metrics.get("realized_vol", 0.0)),
                spread_proxy=float(metrics.get("spread_proxy", 0.0)),
                params=dict(getattr(config, "dynamic_slip", {}) or {}),
            ),
        )
    )
