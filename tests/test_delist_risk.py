from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from core.research import delist_risk as dr

MODEL = {"features": [f"{f}_r" for f in dr.FEATURE_SIGNS], "weights": [1.13, 0.12, 0.53, 0.39, 0.38], "bias": -3.8}


def _series(values, start="2025-01-01"):
    return pd.Series(values, index=pd.date_range(start, periods=len(values), freq="D", tz="UTC"))


def test_features_need_history_and_measure_against_btc():
    assert dr.features_from_daily(_series([1.0] * 100), _series([1e6] * 100), _series([1.0] * 100)) is None
    close = _series(list(np.linspace(2.0, 1.0, 200)))           # coin halves
    btc = _series([100.0] * 200)                                 # BTC flat
    qvol = _series([1e6] * 170 + [1e5] * 30)                     # volume collapses lately
    f = dr.features_from_daily(close, qvol, btc)
    assert f["dd365"] == pytest.approx(-0.5)
    assert f["ret90"] < 0 and f["voltrend"] < 1 and f["logvol30"] == pytest.approx(5.0)
    assert f["age"] == 200


def test_low_volume_falling_coin_scores_riskiest_and_top5_flagged():
    rng = np.random.default_rng(0)
    feats = {f"C{i}": {"logvol30": 7 + rng.normal(), "voltrend": 1.0, "ret90": 0.0, "dd365": -0.3, "age": 800} for i in range(99)}
    feats["DEAD"] = {"logvol30": 3.0, "voltrend": 0.2, "ret90": -0.8, "dd365": -0.97, "age": 2000}
    scored = dr.score_universe(feats, MODEL)
    assert scored.index[0] == "DEAD" and bool(scored.loc["DEAD", "flagged"])
    assert int(scored["flagged"].sum()) == 6  # percentile >= 0.95, same boundary as the validated study bucket


def test_scores_roundtrip_and_stale_files_are_ignored(tmp_path):
    scored = dr.score_universe({f"C{i}": {"logvol30": i, "voltrend": 1, "ret90": 0, "dd365": -0.1, "age": 500} for i in range(20)}, MODEL)
    path = tmp_path / "scores.json"
    dr.write_scores(scored, MODEL, path)
    assert set(dr.load_scores(path)) == set(scored.index)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["generated_at"] = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert dr.load_scores(path) == {}
    assert dr.load_scores(tmp_path / "missing.json") == {}


def test_model_feature_order_is_enforced(tmp_path):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({**MODEL, "features": list(reversed(MODEL["features"]))}), encoding="utf-8")
    with pytest.raises(ValueError):
        dr.load_model(path)
