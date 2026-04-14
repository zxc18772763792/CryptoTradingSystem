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

    monkeypatch.setattr(fg_module, "fear_greed_collector", SimpleNamespace(_history=[]))
    direction_base, conf_base = agg._get_factor_signal(df)
    assert direction_base == "LONG"

    fear = SimpleNamespace(is_extreme_fear=True, is_extreme_greed=False)
    monkeypatch.setattr(fg_module, "fear_greed_collector", SimpleNamespace(_history=[fear]))
    direction_boost, conf_boost = agg._get_factor_signal(df)

    assert direction_boost == "LONG"
    assert conf_boost == pytest.approx(min(1.0, conf_base + 0.08), rel=1e-9)


def test_factor_signal_extreme_greed_boosts_short_confidence(monkeypatch):
    from core.ai.signal_aggregator import SignalAggregator
    import core.data.sentiment.fear_greed_collector as fg_module

    agg = SignalAggregator()
    df = _build_close_df("down")

    monkeypatch.setattr(fg_module, "fear_greed_collector", SimpleNamespace(_history=[]))
    direction_base, conf_base = agg._get_factor_signal(df)
    assert direction_base == "SHORT"

    greed = SimpleNamespace(is_extreme_fear=False, is_extreme_greed=True)
    monkeypatch.setattr(fg_module, "fear_greed_collector", SimpleNamespace(_history=[greed]))
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
    assert captured["columns"][:5] == ["rsi", "macd", "macd_signal", "macd_hist", "ema_fast"]


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
