from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pandas as pd
import pytest


def _build_close_df(direction: str = "up", rows: int = 80) -> pd.DataFrame:
    base = 100.0
    step = 0.08 if direction == "up" else -0.08
    close = [base + i * step for i in range(rows)]
    return pd.DataFrame({"close": close})


def test_factor_signal_extreme_fear_boosts_long_confidence(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator
    import core.data.sentiment.fear_greed_collector as fg_module

    agg = SignalAggregator()
    df = _build_close_df("up")

    monkeypatch.setattr(fg_module, "fear_greed_collector", SimpleNamespace(latest=lambda: None))
    direction_base, conf_base = agg._get_factor_signal(df)
    assert direction_base == "LONG"

    fear = SimpleNamespace(is_extreme_fear=True, is_extreme_greed=False)
    monkeypatch.setattr(fg_module, "fear_greed_collector", SimpleNamespace(latest=lambda: fear))
    direction_boost, conf_boost = agg._get_factor_signal(df)

    assert direction_boost == "LONG"
    assert conf_boost == pytest.approx(min(1.0, conf_base + 0.08), rel=1e-9)


def test_factor_signal_extreme_greed_boosts_short_confidence(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator
    import core.data.sentiment.fear_greed_collector as fg_module

    agg = SignalAggregator()
    df = _build_close_df("down")

    monkeypatch.setattr(fg_module, "fear_greed_collector", SimpleNamespace(latest=lambda: None))
    direction_base, conf_base = agg._get_factor_signal(df)
    assert direction_base == "SHORT"

    greed = SimpleNamespace(is_extreme_fear=False, is_extreme_greed=True)
    monkeypatch.setattr(fg_module, "fear_greed_collector", SimpleNamespace(latest=lambda: greed))
    direction_boost, conf_boost = agg._get_factor_signal(df)

    assert direction_boost == "SHORT"
    assert conf_boost == pytest.approx(min(1.0, conf_base + 0.08), rel=1e-9)


def test_signal_aggregator_preview_risk_check_does_not_consume_cooldown():
    from core.ai.signal_aggregator import SignalAggregator

    agg = SignalAggregator()
    df = _build_close_df("up")

    blocked_first, reason_first = agg._apply_risk_gate("BTC/USDT", "LONG", 0.6, df)
    blocked_second, reason_second = agg._apply_risk_gate("BTC/USDT", "LONG", 0.6, df)

    assert blocked_first is False
    assert reason_first == ""
    assert blocked_second is False
    assert reason_second == ""


def test_signal_aggregator_excludes_unavailable_component_weights(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator

    agg = SignalAggregator()
    df = _build_close_df("down")

    async def _fake_llm_signal(symbol, market_data):
        return "FLAT", 0.0

    monkeypatch.setattr(agg, "_get_llm_signal", _fake_llm_signal)
    monkeypatch.setattr(agg, "_get_ml_signal", lambda symbol, market_data: ("FLAT", 0.0))
    monkeypatch.setattr(agg, "_get_factor_signal", lambda market_data: ("SHORT", 0.64))
    monkeypatch.setattr(
        agg,
        "_get_derivatives_signal",
        lambda symbol: asyncio.sleep(0, result=("FLAT", 0.0, {"available": False, "reason": "disabled_in_test"})),
    )
    monkeypatch.setattr(agg, "_apply_risk_gate", lambda symbol, direction, confidence, market_data: (False, ""))
    agg._ml_model = SimpleNamespace(is_loaded=lambda: False)

    result = asyncio.run(agg.aggregate("BTC/USDT", df))

    assert result.direction == "SHORT"
    assert result.confidence == pytest.approx(0.64, rel=1e-9)
    assert result.components["llm"]["available"] is False
    assert result.components["llm"]["effective_weight"] == pytest.approx(0.0, rel=1e-9)
    assert result.components["ml"]["available"] is False
    assert result.components["ml"]["effective_weight"] == pytest.approx(0.0, rel=1e-9)
    assert result.components["factor"]["available"] is True
    assert result.components["factor"]["effective_weight"] == pytest.approx(0.25, rel=1e-9)


def test_signal_aggregator_keeps_neutral_available_component_weight(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator

    agg = SignalAggregator()
    df = _build_close_df("down")

    async def _fake_llm_signal(symbol, market_data):
        return "FLAT", 0.55

    monkeypatch.setattr(agg, "_get_llm_signal", _fake_llm_signal)
    monkeypatch.setattr(agg, "_get_ml_signal", lambda symbol, market_data: ("FLAT", 0.0))
    monkeypatch.setattr(agg, "_get_factor_signal", lambda market_data: ("SHORT", 0.64))
    monkeypatch.setattr(agg, "_apply_risk_gate", lambda symbol, direction, confidence, market_data: (False, ""))
    agg._ml_model = SimpleNamespace(is_loaded=lambda: False)

    result = asyncio.run(agg.aggregate("BTC/USDT", df))

    assert result.direction == "FLAT"
    assert result.components["llm"]["available"] is True
    assert result.components["llm"]["status"] == "neutral"
    assert result.components["llm"]["effective_weight"] == pytest.approx(0.4, rel=1e-9)


def test_signal_aggregator_fast_scan_disables_llm_and_ml(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator

    agg = SignalAggregator()
    df = _build_close_df("up")

    async def _unexpected_llm(symbol, market_data):
        raise AssertionError("fast scan should not call llm signal")

    monkeypatch.setattr(agg, "_get_llm_signal", _unexpected_llm)
    monkeypatch.setattr(agg, "_get_ml_signal", lambda symbol, market_data: (_ for _ in ()).throw(AssertionError("fast scan should not call ml signal")))
    monkeypatch.setattr(agg, "_apply_risk_gate", lambda symbol, direction, confidence, market_data: (False, ""))

    result = asyncio.run(agg.aggregate("BTC/USDT", df, include_llm=False, include_ml=False))

    assert result.components["llm"]["available"] is False
    assert result.components["llm"]["reason"] == "disabled_for_fast_scan"
    assert result.components["ml"]["available"] is False
    assert result.components["ml"]["reason"] == "disabled_for_fast_scan"
    assert result.components["factor"]["available"] is True


def test_signal_aggregator_ml_signal_uses_internal_feature_builder():
    from core.ai.signal_aggregator import SignalAggregator

    agg = SignalAggregator()

    captured = {}

    class _FakeModel:
        def is_loaded(self):
            return True

        def predict(self, features, symbol=""):
            captured["symbol"] = symbol
            captured["columns"] = list(features.columns)
            captured["rows"] = len(features)
            return SimpleNamespace(direction="LONG", confidence=0.73)

    agg._ml_model = _FakeModel()
    df = pd.DataFrame(
        {
            "open": [100.0 + i for i in range(60)],
            "high": [101.0 + i for i in range(60)],
            "low": [99.0 + i for i in range(60)],
            "close": [100.5 + i for i in range(60)],
            "volume": [1000.0 + i for i in range(60)],
        }
    )

    direction, confidence = agg._get_ml_signal("BTC/USDT", df)

    assert direction == "LONG"
    assert confidence == pytest.approx(0.73, rel=1e-9)
    assert captured["symbol"] == "BTC/USDT"
    assert captured["rows"] == len(df)
    from core.ai.ml_signal import FEATURE_COLS

    assert captured["columns"] == FEATURE_COLS


def test_signal_aggregator_handles_missing_market_data(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator

    agg = SignalAggregator()

    async def _unexpected_llm(symbol, market_data):
        raise AssertionError("llm should not be called when disabled")

    monkeypatch.setattr(agg, "_get_llm_signal", _unexpected_llm)
    monkeypatch.setattr(agg, "_apply_risk_gate", lambda symbol, direction, confidence, market_data: (False, ""))
    agg._ml_model = SimpleNamespace(is_loaded=lambda: True)

    result = asyncio.run(agg.aggregate("BTC/USDT", None, include_llm=False))

    assert result.direction == "FLAT"
    assert result.confidence == pytest.approx(0.0, rel=1e-9)
    assert result.components["ml"]["available"] is False
    assert result.components["ml"]["reason"] == "insufficient_market_data"
    assert result.components["factor"]["available"] is False
    assert result.components["factor"]["reason"] == "insufficient_market_data"


def test_signal_aggregator_derivatives_shadow_penalizes_confidence(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator, settings as agg_settings

    agg = SignalAggregator()
    df = _build_close_df("up")

    async def _fake_llm_signal(symbol, market_data):
        return "LONG", 0.80

    async def _fake_derivatives_signal(symbol):
        return (
            "SHORT",
            0.95,
            {
                "available": True,
                "context": {"crowding_warning": True},
                "risk_flags": ["crowding_hot", "distribution_risk"],
                "regime": "mixed",
                "explain": "shadow-only derivatives context",
            },
        )

    monkeypatch.setattr(agg_settings, "COINGLASS_LIVE_GATING_ENABLED", False)
    monkeypatch.setattr(agg, "_get_llm_signal", _fake_llm_signal)
    monkeypatch.setattr(agg, "_get_ml_signal", lambda symbol, market_data: ("FLAT", 0.0))
    monkeypatch.setattr(agg, "_get_factor_signal", lambda market_data: ("LONG", 0.60))
    monkeypatch.setattr(agg, "_get_derivatives_signal", _fake_derivatives_signal)
    monkeypatch.setattr(agg, "_apply_risk_gate", lambda symbol, direction, confidence, market_data: (False, ""))
    agg._ml_model = SimpleNamespace(is_loaded=lambda: False)

    result = asyncio.run(agg.aggregate("BTC/USDT", df))

    baseline_conf = ((0.40 * 0.80) + (0.25 * 0.60)) / (0.40 + 0.25)
    expected_conf = baseline_conf - 0.16

    assert result.direction == "LONG"
    assert result.confidence == pytest.approx(expected_conf, abs=1e-6)
    assert result.market_context["crowding_warning"] is True
    assert result.components["derivatives"]["shadow_only"] is True
    assert result.components["derivatives"]["effective_weight"] == pytest.approx(0.0, rel=1e-9)
    assert result.components["derivatives"]["confidence_adjustment"] == pytest.approx(-0.16, rel=1e-9)
    assert any(g["code"] == "component_derivatives" and g["status"] == "shadow" for g in result.decision_trace["gates"])


def test_signal_aggregator_shadow_adjustment_penalizes_long_for_history_and_basis_context():
    from core.ai.signal_aggregator import SignalAggregator

    penalty = SignalAggregator._apply_derivatives_shadow_adjustment(
        direction="LONG",
        confidence=0.9,
        market_context={
            "history_ready": False,
            "crowded_long": True,
            "basis_dislocation": True,
            "flow_divergence": True,
            "funding_zscore": 2.1,
        },
        risk_flags=["basis_dislocation", "flow_divergence", "history_incomplete"],
    )

    assert penalty == pytest.approx(0.24, rel=1e-9)


def test_signal_aggregator_shadow_adjustment_penalizes_short_for_squeeze_context():
    from core.ai.signal_aggregator import SignalAggregator

    penalty = SignalAggregator._apply_derivatives_shadow_adjustment(
        direction="SHORT",
        confidence=0.9,
        market_context={
            "history_ready": True,
            "crowded_short": True,
            "squeeze_building": True,
            "order_flow_confirmed": True,
            "basis_dislocation": True,
        },
        risk_flags=["squeeze_active", "basis_dislocation"],
    )

    assert penalty == pytest.approx(0.26, rel=1e-9)


def test_signal_aggregator_derivatives_vote_stays_off_until_live_gating(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator, settings as agg_settings

    agg = SignalAggregator()
    df = _build_close_df("up")

    async def _fake_llm_signal(symbol, market_data):
        return "LONG", 0.40

    async def _fake_derivatives_signal(symbol):
        return (
            "SHORT",
            1.0,
            {
                "available": True,
                "context": {},
                "risk_flags": [],
                "regime": "distribution",
                "explain": "live gating vote enabled",
            },
        )

    monkeypatch.setattr(agg_settings, "COINGLASS_LIVE_GATING_ENABLED", True)
    monkeypatch.setattr(agg, "_get_llm_signal", _fake_llm_signal)
    monkeypatch.setattr(agg, "_get_ml_signal", lambda symbol, market_data: ("FLAT", 0.0))
    monkeypatch.setattr(agg, "_get_factor_signal", lambda market_data: ("FLAT", 0.0))
    monkeypatch.setattr(agg, "_get_derivatives_signal", _fake_derivatives_signal)
    monkeypatch.setattr(agg, "_apply_risk_gate", lambda symbol, direction, confidence, market_data: (False, ""))
    agg._ml_model = SimpleNamespace(is_loaded=lambda: False)

    result = asyncio.run(agg.aggregate("BTC/USDT", df))

    assert result.direction == "SHORT"
    assert result.components["derivatives"]["shadow_only"] is False
    assert result.components["derivatives"]["effective_weight"] == pytest.approx(0.20, rel=1e-9)
