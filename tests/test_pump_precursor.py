"""Tests for the pump-precursor shared feature builder and scorer."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.research.pump_precursor import (
    FEATURES,
    build_daily_features,
    latest_feature_row,
    load_model_weights,
    score_universe,
    top_feature_drivers,
)


def _daily(n: int = 200, seed: int = 5) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2026-01-01", periods=n, freq="1D")
    close = pd.Series(np.exp(np.cumsum(rng.normal(0, 0.01, n))), index=idx)
    return pd.DataFrame(
        {
            "close": close,
            "volume": rng.uniform(1e6, 2e6, n),
            "oi": np.linspace(5e6, 8e6, n),
            "mcap": 50e6,
            "funding": 0.0001,
        },
        index=idx,
    )


def _model() -> dict:
    return {"features": list(FEATURES), "weights": [0.1] * len(FEATURES), "bias": -2.0}


def test_build_daily_features_has_all_columns_and_sane_values():
    frame = build_daily_features(_daily())
    for feat in FEATURES:
        assert feat in frame.columns, feat
    last = frame.iloc[-1]
    assert last["age_days"] == len(frame) - 1
    assert last["oi_mcap"] == pytest.approx(8e6 / 50e6, rel=1e-6)
    assert -1.0 <= last["dd_from_ath"] <= 0.0
    assert 0.0 <= last["vol_pctile_90d"] <= 1.0


def test_pumped_flags_detect_past_doubling():
    daily = _daily()
    quiet = build_daily_features(daily)
    assert quiet.iloc[-1]["pumped_ever"] == 0.0

    pumped = daily.copy()
    pumped.loc[pumped.index[100:110], "close"] = pumped["close"].iloc[99] * 3.0
    flagged = build_daily_features(pumped)
    assert flagged.iloc[-1]["pumped_ever"] == 1.0
    assert flagged.iloc[-1]["pumped_before_120d"] == 1.0


def test_latest_feature_row_fail_closed_on_short_or_nan_history():
    assert latest_feature_row(_daily(20)) is None
    broken = _daily()
    broken["oi"] = np.nan
    assert latest_feature_row(broken) is None


def test_score_universe_ranks_dominating_symbol_first():
    strong = latest_feature_row(_daily())
    weak = dict(strong)
    for key in weak:
        weak[key] = weak[key] - abs(weak[key]) * 0.5 - 0.1
    scored = score_universe({"STRONG": strong, "WEAK": weak}, _model())
    assert list(scored.index)[0] == "STRONG"
    assert 0.0 <= scored["score"].iloc[0] <= 1.0
    drivers = top_feature_drivers(scored.iloc[0], _model())
    assert len(drivers) <= 3


def test_load_model_weights_rejects_feature_mismatch(tmp_path):
    bad = tmp_path / "w.json"
    bad.write_text('{"features": ["nope"], "weights": [1.0], "bias": 0.0}', encoding="utf-8")
    with pytest.raises(ValueError):
        load_model_weights(bad)


def test_exported_weights_artifact_matches_feature_order(tmp_path, monkeypatch):
    import json
    import core.research.pump_precursor as precursor
    target = tmp_path / "weights.json"
    target.write_text(json.dumps(_model()), encoding="utf-8")
    monkeypatch.setattr(precursor, "DEFAULT_WEIGHTS_PATH", target)
    model = load_model_weights()
    assert model["features"] == FEATURES
    assert len(model["weights"]) == len(FEATURES)
