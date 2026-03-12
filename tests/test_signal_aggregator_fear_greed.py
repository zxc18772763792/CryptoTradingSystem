from __future__ import annotations

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

