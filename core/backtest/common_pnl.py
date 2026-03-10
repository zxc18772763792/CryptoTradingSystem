from __future__ import annotations

from typing import Any, Dict, Optional


def _round_or_none(value: Any, digits: int = 6) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return None


def build_common_pnl_summary(
    *,
    source: str,
    unit: str,
    gross_pnl: Any,
    fee: Any,
    slippage_cost: Any,
    funding_pnl: Any,
    net_pnl: Any,
    turnover: Any,
    trade_count: Any,
    win_rate: Any,
    cost_model_version: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "source": str(source or ""),
        "unit": str(unit or ""),
        "gross_pnl": _round_or_none(gross_pnl),
        "fee": _round_or_none(fee),
        "slippage_cost": _round_or_none(slippage_cost),
        "funding_pnl": _round_or_none(funding_pnl),
        "net_pnl": _round_or_none(net_pnl),
        "turnover": _round_or_none(turnover),
        "trade_count": int(trade_count or 0),
        "win_rate": _round_or_none(win_rate),
        "cost_model_version": str(cost_model_version or ""),
        "metadata": dict(metadata or {}),
    }
