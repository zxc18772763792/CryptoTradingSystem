"""Reusable cost model helpers for backtest/research (skeleton)."""
from __future__ import annotations

from typing import Any, Dict


def fee_rate(config: Any, role: str = "taker") -> float:
    model = str(getattr(config, "fee_model", "flat") or "flat").lower()
    if model == "maker_taker":
        return float(getattr(config, "maker_fee", 0.0) if str(role).lower() == "maker" else getattr(config, "taker_fee", 0.0))
    return float(getattr(config, "commission_rate", 0.0))


def dynamic_slippage_rate(atr_pct: float, realized_vol: float, spread_proxy: float, params: Dict[str, float]) -> float:
    p = dict(params or {})
    min_slip = float(p.get("min_slip", 0.0))
    k_atr = float(p.get("k_atr", 0.0))
    k_rv = float(p.get("k_rv", 0.0))
    k_spread = float(p.get("k_spread", 0.0))
    return max(min_slip, k_atr * max(0.0, atr_pct) + k_rv * max(0.0, realized_vol) + k_spread * max(0.0, spread_proxy))

