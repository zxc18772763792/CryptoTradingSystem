"""KOL consensus strategy (macro directional, 5 majors only).

Trades the relay's distilled high-win-rate-KOL long/short consensus, which exists
ONLY for BTC/ETH/SOL/DOGE/BNB. It is a macro/regime call, not an altcoin signal.

Data access mirrors the other macro strategies: reads kol_* columns off the frame
if an enrichment step attached them, else falls back to the in-memory consensus
cache (core.data.coinglass_lsr), which the radar's /kol-consensus endpoint and any
periodic refresher keep warm. When no consensus is available (cold cache, or a
non-major symbol) it emits nothing — never a fabricated decision.

NOT backtestable: the consensus is a live daily snapshot with no history, so the
registry marks backtest unsupported. Paper/live, unvalidated — treat as context.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from core.data.coinglass_lsr import KOL_SYMBOLS, get_cached_kol_symbol
from core.strategies.strategy_base import Signal, SignalType, StrategyBase


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if out == out else float(default)  # NaN guard


class KolConsensusStrategy(StrategyBase):
    mutates_input = False

    def __init__(self, name: str = "KOL_Consensus", params: Optional[Dict[str, Any]] = None):
        default_params = {
            "regime_mode": True,       # emit a HOLD regime signal carrying the read
            "trade_mode": False,       # emit BUY/SELL from long/short consensus
            "min_confidence": 0.35,    # ignore weak consensus
            "allow_long": True,
            "allow_short": True,
            "max_snapshot_age_hours": 30.0,
            "stop_loss_pct": 0.05,
            "take_profit_pct": 0.10,
        }
        if params:
            default_params.update(params)
        super().__init__(name, default_params)

    def _symbol_of(self, data: pd.DataFrame) -> str:
        if "symbol" in getattr(data, "columns", []) and len(data):
            raw = str(data["symbol"].iloc[-1] or "").strip()
            if raw:
                return raw
        return str(self.params.get("symbol") or "").strip()

    def _consensus_for(self, data: pd.DataFrame, symbol: str) -> Optional[Dict[str, Any]]:
        # 1) enrichment columns on the frame, if present
        cols = set(getattr(data, "columns", []))
        if {"kol_decision", "kol_confidence"}.issubset(cols) and len(data):
            row = data.iloc[-1]
            decision = str(row.get("kol_decision") or "").lower()
            if decision:
                return {
                    "decision": decision,
                    "confidence": _f(row.get("kol_confidence")),
                    "trust_adjusted_bias": _f(row.get("kol_bias")),
                    "snapshot_age_hours": _f(row.get("kol_snapshot_age_hours"), 0.0),
                    "source": "frame",
                }
        # 2) shared consensus cache (kept warm by the radar endpoint / refresher)
        cached = get_cached_kol_symbol(symbol)
        if cached:
            return {
                "decision": str(cached.get("decision") or "").lower(),
                "confidence": _f(cached.get("confidence")),
                "trust_adjusted_bias": _f(cached.get("trust_adjusted_bias")),
                "snapshot_age_hours": _f(cached.get("snapshot_age_hours"), 0.0),
                "source": "cache",
            }
        return None

    def generate_signals(self, data: pd.DataFrame) -> List[Signal]:
        if data is None or data.empty or "close" not in data:
            return []
        symbol = self._symbol_of(data)
        base = symbol.upper().replace("/USDT", "").replace("USDT", "")
        if base not in KOL_SYMBOLS:
            return []  # consensus only exists for the 5 majors

        consensus = self._consensus_for(data, symbol)
        if not consensus:
            return []  # cold cache / unavailable -> no fabricated signal
        age = float(consensus.get("snapshot_age_hours") or 0.0)
        if age > float(self.params.get("max_snapshot_age_hours", 30.0)):
            return []

        decision = str(consensus.get("decision") or "neutral")
        confidence = float(consensus.get("confidence") or 0.0)
        bias = float(consensus.get("trust_adjusted_bias") or 0.0)
        price = _f(pd.to_numeric(data["close"], errors="coerce").iloc[-1])
        if price <= 0:
            return []
        timestamp = self._bar_time(data)
        meta_common = {
            "structural_role": "kol_consensus",
            "kol_decision": decision,
            "kol_confidence": round(confidence, 4),
            "kol_bias": round(bias, 4),
            "kol_source": consensus.get("source"),
            "coverage": "major_only",
            "use_atr_stops": False,
        }
        signals: List[Signal] = []

        if bool(self.params.get("regime_mode", True)):
            signals.append(
                Signal(
                    symbol=symbol,
                    signal_type=SignalType.HOLD,
                    price=price,
                    timestamp=timestamp,
                    strategy_name=self.name,
                    strength=max(0.1, min(1.0, confidence)),
                    metadata={**meta_common, "regime": decision},
                )
            )

        if not bool(self.params.get("trade_mode", False)):
            return signals
        if confidence < float(self.params.get("min_confidence", 0.35)):
            return signals

        sl = float(self.params.get("stop_loss_pct", 0.05))
        tp = float(self.params.get("take_profit_pct", 0.10))
        if decision == "long" and bool(self.params.get("allow_long", True)):
            signals.append(
                Signal(
                    symbol=symbol, signal_type=SignalType.BUY, price=price, timestamp=timestamp,
                    strategy_name=self.name, strength=min(1.0, confidence),
                    stop_loss=price * (1.0 - sl), take_profit=price * (1.0 + tp),
                    metadata={**meta_common, "reason_codes": ["kol_consensus_long"]},
                )
            )
        elif decision == "short" and bool(self.params.get("allow_short", True)):
            signals.append(
                Signal(
                    symbol=symbol, signal_type=SignalType.SELL, price=price, timestamp=timestamp,
                    strategy_name=self.name, strength=min(1.0, confidence),
                    stop_loss=price * (1.0 + sl), take_profit=price * (1.0 - tp),
                    metadata={**meta_common, "reason_codes": ["kol_consensus_short"]},
                )
            )
        return signals

    def get_required_data(self) -> Dict[str, Any]:
        return {
            "type": "macro_kol_consensus",
            "columns": ["close"],
            "optional_columns": ["symbol", "kol_decision", "kol_confidence", "kol_bias"],
            "coverage": list(KOL_SYMBOLS),
            "min_length": 2,
        }
