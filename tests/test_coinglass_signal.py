from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from core.ai import coinglass_signal as signal_module


def test_build_coinglass_signal_prefers_squeeze_building_with_order_flow_confirmation(monkeypatch):
    monkeypatch.setattr(signal_module.settings, "COINGLASS_ENABLED", True, raising=False)
    monkeypatch.setattr(signal_module.settings, "COINGLASS_INCLUDE_AI", True, raising=False)
    monkeypatch.setattr(
        signal_module,
        "load_latest_derivatives_snapshot",
        AsyncMock(
            return_value={
                "timestamp": "2026-04-18T12:00:00Z",
                "crowding_score": 0.58,
                "squeeze_score": 0.74,
                "distribution_score": 0.22,
                "taker_buy_sell_imbalance": 0.19,
                "funding_rate": -0.0004,
                "payload": {
                    "history_ready": True,
                    "funding_zscore": -1.2,
                    "liquidation_burst_score": 0.68,
                    "long_short_ratio_change_24h": -0.08,
                    "crowded_short": True,
                    "squeeze_building": True,
                    "order_flow_confirmed": True,
                    "derivatives_labels": ["crowded_short", "squeeze_building", "order_flow_confirmed"],
                },
            }
        ),
    )

    direction, confidence, payload = asyncio.run(signal_module.build_coinglass_signal("BTC/USDT"))

    assert direction == "LONG"
    assert confidence > 0.60
    assert payload["context"]["squeeze_building"] is True
    assert "squeeze_building" in payload["context_flags"]
    assert "order_flow_confirmed" in payload["context_flags"]
    assert "labels=crowded_short,squeeze_building,order_flow_confirmed" in payload["explain"]


def test_build_coinglass_signal_adds_new_risk_flags_and_reduces_confidence(monkeypatch):
    monkeypatch.setattr(signal_module.settings, "COINGLASS_ENABLED", True, raising=False)
    monkeypatch.setattr(signal_module.settings, "COINGLASS_INCLUDE_AI", True, raising=False)
    monkeypatch.setattr(
        signal_module,
        "load_latest_derivatives_snapshot",
        AsyncMock(
            return_value={
                "timestamp": "2026-04-18T12:00:00Z",
                "crowding_score": 0.84,
                "squeeze_score": 0.31,
                "distribution_score": 0.73,
                "taker_buy_sell_imbalance": -0.06,
                "funding_rate": 0.0018,
                "payload": {
                    "history_ready": False,
                    "funding_zscore": 2.1,
                    "liquidation_burst_score": 0.81,
                    "long_short_ratio_change_24h": 0.22,
                    "crowded_long": True,
                    "basis_dislocation": True,
                    "flow_divergence": True,
                    "flush_risk": True,
                    "heatmap_pressure_score": 0.76,
                    "liquidity_void_score": 0.72,
                    "orderbook_agg_imbalance": -0.24,
                    "derivatives_labels": ["crowded_long", "basis_dislocation", "flow_divergence", "flush_risk"],
                },
            }
        ),
    )

    direction, confidence, payload = asyncio.run(signal_module.build_coinglass_signal("BTC/USDT"))

    assert direction == "SHORT"
    assert 0.0 < confidence < 1.0
    assert "crowded_long" in payload["risk_flags"]
    assert "basis_dislocation" in payload["risk_flags"]
    assert "flow_divergence" in payload["risk_flags"]
    assert "liquidity_heatmap_hot" in payload["risk_flags"]
    assert "liquidity_void" in payload["risk_flags"]
    assert "orderbook_agg_ask_bias" in payload["context_flags"]
    assert "history_incomplete" in payload["risk_flags"]
    assert payload["context"]["history_ready"] is False
