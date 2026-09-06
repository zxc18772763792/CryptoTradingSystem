"""Tests for the LSR/KOL data helpers and the KOL consensus strategy."""
from __future__ import annotations

import numpy as np
import pandas as pd

import core.data.coinglass_lsr as lsr
from config.strategy_registry import get_strategy_registry_entry
from strategies import ALL_STRATEGIES, KolConsensusStrategy


def _frame(symbol: str, n: int = 5) -> pd.DataFrame:
    idx = pd.date_range("2026-09-06", periods=n, freq="1D")
    return pd.DataFrame(
        {"open": [100] * n, "high": [101] * n, "low": [99] * n,
         "close": np.linspace(100, 104, n), "symbol": symbol},
        index=idx,
    )


def test_crowding_label_bands():
    assert lsr.crowding_label(6.0) == "极度拥挤多"
    assert lsr.crowding_label(1.8) == "偏拥挤多"
    assert lsr.crowding_label(1.0) == "均衡"
    assert lsr.crowding_label(0.5) == "偏拥挤空"
    assert lsr.crowding_label(0.2) == "极度拥挤空"
    assert lsr.crowding_label(None) == "unknown"


def test_risk_tone_aggregation():
    rows = [
        {"decision": "long", "confidence": 0.6, "trust_adjusted_bias": 0.3},
        {"decision": "short", "confidence": 0.2, "trust_adjusted_bias": -0.1},
        {"decision": "neutral", "confidence": 0.1, "trust_adjusted_bias": 0.0},
    ]
    tone = lsr._aggregate_risk_tone(rows)
    assert tone["tone"] == "risk_on" and tone["score"] > 0
    assert tone["long"] == 1 and tone["short"] == 1 and tone["neutral"] == 1


def test_registry_marks_kol_unbacktestable():
    assert "KolConsensusStrategy" in ALL_STRATEGIES
    entry = get_strategy_registry_entry("KolConsensusStrategy")
    assert entry["backtest"]["supported"] is False
    assert entry["defaults"]["use_atr_stops"] is False


def test_kol_strategy_emits_regime_from_cache(monkeypatch):
    monkeypatch.setattr(
        "strategies.macro.kol_consensus.get_cached_kol_symbol",
        lambda sym: {"decision": "long", "decision_label": "看多", "confidence": 0.6,
                     "trust_adjusted_bias": 0.3, "snapshot_age_hours": 5.0} if "BTC" in sym.upper() else None,
    )
    s = KolConsensusStrategy("kol")
    sigs = s.generate_signals(_frame("BTC/USDT"))
    assert len(sigs) == 1 and sigs[0].signal_type.value == "hold"
    assert sigs[0].metadata["kol_decision"] == "long"


def test_kol_strategy_trade_mode_and_confidence_gate(monkeypatch):
    monkeypatch.setattr(
        "strategies.macro.kol_consensus.get_cached_kol_symbol",
        lambda sym: {"decision": "short", "confidence": 0.6, "trust_adjusted_bias": -0.3, "snapshot_age_hours": 5.0},
    )
    s = KolConsensusStrategy("kol", {"trade_mode": True, "min_confidence": 0.35})
    types = {sig.signal_type.value for sig in s.generate_signals(_frame("ETH/USDT"))}
    assert "sell" in types  # short decision -> SELL

    weak = KolConsensusStrategy("kol2", {"trade_mode": True, "min_confidence": 0.9})
    weak_types = {sig.signal_type.value for sig in weak.generate_signals(_frame("ETH/USDT"))}
    assert "sell" not in weak_types  # below confidence gate -> regime only


def test_kol_strategy_skips_non_majors_and_cold_cache(monkeypatch):
    monkeypatch.setattr("strategies.macro.kol_consensus.get_cached_kol_symbol", lambda sym: None)
    s = KolConsensusStrategy("kol")
    assert s.generate_signals(_frame("PEPE/USDT")) == []   # not a major
    assert s.generate_signals(_frame("BTC/USDT")) == []     # major but cold cache -> no fabricated signal


def test_kol_strategy_reads_frame_enrichment_columns():
    df = _frame("SOL/USDT")
    df["kol_decision"] = "long"
    df["kol_confidence"] = 0.7
    df["kol_bias"] = 0.4
    df["kol_snapshot_age_hours"] = 3.0
    s = KolConsensusStrategy("kol", {"trade_mode": True})
    metas = [sig.metadata for sig in s.generate_signals(df)]
    assert any(m.get("kol_source") == "frame" for m in metas)
