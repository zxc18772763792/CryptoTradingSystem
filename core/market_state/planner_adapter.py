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


_SECTOR_BENCHMARK_RULES = {
    "BTC": {"bitcoin", "btc", "ordinals", "runes", "pow"},
    "ETH": {"ethereum", "ethereum-ecosystem", "defi", "l2", "layer-2", "restaking"},
    "SOL": {"solana", "solana-ecosystem"},
}


def _sector_linked_to_benchmark(*, sector: str, benchmark_symbol: str) -> bool:
    sector_key = str(sector or "").strip().lower().replace("_", "-")
    if not sector_key:
        return False
    return sector_key in _SECTOR_BENCHMARK_RULES.get(_symbol_base(benchmark_symbol), set())


def _infer_scope(snapshot: Dict[str, Any], *, symbol: str, metadata: Dict[str, Any]) -> str:
    explicit = str(snapshot.get("scope") or metadata.get("scope") or metadata.get("market_state_scope") or "").lower()
    if explicit in {"symbol", "benchmark", "global"}:
        return explicit
    snapshot_symbol = str(snapshot.get("symbol") or metadata.get("benchmark_symbol") or "").strip()
    if _is_altcoin(symbol):
        if not snapshot_symbol:
            return "global"
        if _symbol_base(snapshot_symbol) != _symbol_base(symbol):
            return "benchmark" if _symbol_base(snapshot_symbol) in {"BTC", "ETH", "SOL"} else "global"
    return "symbol"


def market_state_to_planner_hints(
    snapshot: Dict[str, Any],
    *,
    symbol: str = "",
    benchmark_beta: float = 0.0,
    symbol_metadata: Dict[str, Any] | None = None,
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
    metadata = {**dict(snapshot.get("metadata") or {}), **dict(symbol_metadata or {})}
    scope = _infer_scope(snapshot, symbol=symbol, metadata=metadata)
    benchmark_symbol = str(
        metadata.get("benchmark_symbol")
        or snapshot.get("benchmark_symbol")
        or snapshot.get("symbol")
        or ""
    )
    sector = str(metadata.get("sector") or metadata.get("symbol_sector") or "").strip().lower()
    beta_linked = float(benchmark_beta or 0.0) >= 0.6
    sector_linked = _sector_linked_to_benchmark(sector=sector, benchmark_symbol=benchmark_symbol)

    if _is_altcoin(symbol) and scope == "global":
        notes.append("global_context_risk_only")
        if risk_posture in {"defensive", "halt_new_entries"}:
            boosted.append("risk")
        if regime in {"event_driven_mixed"}:
            boosted.append("macro")
        return boosted, suppressed, notes

    if _is_altcoin(symbol) and scope == "benchmark" and not (beta_linked or sector_linked):
        notes.append("benchmark_context_risk_only:no_beta_or_sector_rule")
        if risk_posture in {"defensive", "halt_new_entries"}:
            boosted.append("risk")
        return boosted, suppressed, notes

    if _is_altcoin(symbol) and scope == "benchmark":
        notes.append("benchmark_context_linked_by_beta" if beta_linked else "benchmark_context_linked_by_sector")

    if regime in {"trend_bullish", "trend_bearish"} or bias in {"bullish", "bearish"}:
        boosted.extend(["trend", "momentum"])
        if uncertainty == "confirmed":
            suppressed.append("mean_reversion")
    elif regime in {"low_info_range"}:
        boosted.extend(["range", "mean_reversion", "stat_arb"])
    elif regime in {"event_driven_mixed"}:
        boosted.extend(["macro", "breakout"])

    if risk_posture in {"defensive", "halt_new_entries"}:
        boosted.extend(["risk", "volatility"])
        suppressed.append("high_leverage_trend")
    if uncertainty in {"borderline", "uncertain"}:
        notes.append(f"market_state_uncertain:{uncertainty}")

    return boosted, suppressed, notes
