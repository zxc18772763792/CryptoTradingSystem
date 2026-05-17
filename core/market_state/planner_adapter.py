"""Convert MarketStateSnapshot payloads into planner category hints."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple


_QUOTE_SUFFIXES = ("USDT", "USDC", "FDUSD", "BUSD", "USD")


def _symbol_base(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    main = text.split(":", 1)[0].replace("_", "/").replace("-", "/")
    base = main.split("/", 1)[0] if "/" in main else main
    for suffix in _QUOTE_SUFFIXES:
        if base.endswith(suffix) and len(base) > len(suffix):
            return base[: -len(suffix)]
    return base


def _is_altcoin(symbol: str) -> bool:
    base = _symbol_base(symbol)
    return bool(base and base not in {"BTC", "ETH"})


def market_state_to_planner_hints(
    snapshot: Dict[str, Any],
    *,
    symbol: str = "",
    benchmark_beta: float = 0.0,
) -> Tuple[List[str], List[str], List[str]]:
    boosted: List[str] = []
    suppressed: List[str] = []
    notes: List[str] = []
    if not snapshot:
        return boosted, suppressed, notes

    regime = str(snapshot.get("regime") or "").lower()
    bias = str(snapshot.get("bias") or "").lower()
    risk_posture = str(snapshot.get("risk_posture") or "").lower()
    uncertainty = str(snapshot.get("uncertainty") or "").lower()
    scope = str(snapshot.get("scope") or "symbol").lower()

    if _is_altcoin(symbol) and scope in {"benchmark", "global"} and float(benchmark_beta or 0.0) < 0.6:
        notes.append("benchmark_context_advisory_only")
        if risk_posture in {"defensive", "halt_new_entries"}:
            boosted.append("风险")
        return boosted, suppressed, notes

    if regime in {"trend_bullish", "trend_bearish"} or bias in {"bullish", "bearish"}:
        boosted.extend(["趋势", "动量"])
        suppressed.extend(["均值回归"] if uncertainty == "confirmed" else [])
    elif regime in {"low_info_range"}:
        boosted.extend(["震荡", "均值回归", "统计套利"])
    elif regime in {"event_driven_mixed"}:
        boosted.extend(["宏观", "突破"])

    if risk_posture in {"defensive", "halt_new_entries"}:
        boosted.extend(["风险", "波动率"])
        suppressed.extend(["高杠杆趋势"])
    if uncertainty in {"borderline", "uncertain"}:
        notes.append(f"market_state_uncertain:{uncertainty}")

    return boosted, suppressed, notes
