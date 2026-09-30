from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from core.ai.ml_signal import MLSignalModel, build_feature_frame, manifest_gate_passed
from core.ml.pipeline import FEATURE_SET_VERSION, MAX_ONE_SIDE_SHARE


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = REPO_ROOT / "models" / "ml_signal_xgb.json"


def _bars(scale: float, seed: int, freq: str) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0, 0.008, 400))) * scale
    index = pd.date_range("2026-01-01", periods=400, freq=freq, tz="UTC")
    return pd.DataFrame({"open": close, "high": close * 1.004, "low": close * 0.996, "close": close,
                         "volume": rng.uniform(1e3, 2e3, 400)}, index=index)


def test_canonical_ml_model_is_either_a_passing_v2_model_or_refused():
    """The file the aggregator loads must never again be a stale or gate-failed model.

    Until 2026-09-30 it was a BTC-only 1h v1 model (raw price levels as
    features, failed its own gate) that said LONG on every altcoin bar. No v2
    model has passed the gate yet (pooled 15m, 2026-09-30: AUC 0.537, top-minus-
    bottom decile +0.05%/h vs ~0.15-0.2% costs), so the file is refused and the
    ML component contributes no vote. When a passing model is published this
    test checks it scores coins at different price levels without one-sidedness.
    """
    manifest = json.loads(MODEL_PATH.with_suffix(".manifest.json").read_text(encoding="utf-8"))
    model = MLSignalModel.load_from_path(str(MODEL_PATH))
    usable = manifest.get("feature_set_version") == FEATURE_SET_VERSION and manifest_gate_passed(manifest)
    assert model.is_loaded() is usable
    if not usable:
        assert model.load_error
        return
    freq = {"15m": "15min", "1h": "1h"}.get(str(manifest.get("timeframe")), "1h")
    for scale in (0.001, 1.0, 1000.0, 80_000.0):
        probs = []
        for seed in range(5):
            feats = build_feature_frame(_bars(scale, seed, freq)).dropna()
            probs += [model.predict(feats.iloc[: i + 1], symbol="ANY/USDT").long_prob for i in range(40, len(feats), 20)]
        long_share = float(np.mean(np.asarray(probs) >= 0.55))
        assert long_share <= MAX_ONE_SIDE_SHARE, (scale, long_share)
