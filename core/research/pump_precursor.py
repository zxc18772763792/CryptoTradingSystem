"""Pump-precursor scoring: shared feature builder + cross-sectional ranker.

单一实现原则：`build_daily_features` 同时被研究面板
（scripts/pump_precursor_panel.py）与周度名单生成
（scripts/generate_pump_watchlist.py）使用，避免训练/推理特征偏斜。

模型：横截面百分位排名 → 逻辑回归（权重由面板脚本时间切分训练后导出到
data/research/pump_watchlist/model_weights.json）。验证结果与折扣项见
docs/AMBUSH_MODES_BACKTEST_REPORT_2026-07-18.md 第 9 节——绝对收益不可外推，
仅相对提升（命中率 ~2.7×）可信；输出用于观察名单，不是交易信号。
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WEIGHTS_PATH = _PROJECT_ROOT / "data" / "research" / "pump_watchlist" / "model_weights.json"

FEATURES: List[str] = [
    "log_mcap", "oi_mcap", "oi_chg_7d", "oi_chg_30d", "funding_7d",
    "ret_7d", "ret_30d", "dd_from_ath", "vola_30d", "vol_trend",
    "vol_pctile_90d", "range_20d", "age_days", "pumped_before_120d", "pumped_ever",
    "log_vol30",
]

LABEL_HORIZON_DAYS = 30


def build_daily_features(daily: pd.DataFrame) -> pd.DataFrame:
    """Compute the panel feature set on a per-symbol DAILY frame.

    Expects columns: close, volume (quote USD), oi (USD), mcap (USD), funding
    (daily mean rate). Index: daily DatetimeIndex, history up to ~365d.
    Returns the input frame with feature columns added; rows before warmup
    contain NaN and callers should use the last row (or dropna) as needed.
    """
    d = daily.copy()
    c = pd.to_numeric(d["close"], errors="coerce")
    v = pd.to_numeric(d["volume"], errors="coerce")
    oi = pd.to_numeric(d.get("oi"), errors="coerce") if "oi" in d else pd.Series(np.nan, index=d.index)
    mcap = pd.to_numeric(d.get("mcap"), errors="coerce") if "mcap" in d else pd.Series(np.nan, index=d.index)
    funding = pd.to_numeric(d.get("funding"), errors="coerce") if "funding" in d else pd.Series(np.nan, index=d.index)

    d["age_days"] = np.arange(len(d))
    d["oi_mcap"] = oi / mcap
    d["oi_chg_7d"] = oi / oi.shift(7) - 1
    d["oi_chg_30d"] = oi / oi.shift(30) - 1
    d["funding_7d"] = funding.rolling(7, min_periods=3).mean()
    d["ret_7d"] = c / c.shift(7) - 1
    d["ret_30d"] = c / c.shift(30) - 1
    d["dd_from_ath"] = c / c.expanding().max() - 1
    d["vola_30d"] = c.pct_change().rolling(30, min_periods=10).std()
    d["vol_avg_30d"] = v.rolling(30, min_periods=10).mean()
    d["vol_trend"] = v.rolling(7, min_periods=3).mean() / d["vol_avg_30d"]
    d["vol_pctile_90d"] = v.rolling(90, min_periods=20).apply(lambda x: (x[-1] >= x).mean(), raw=True)
    d["range_20d"] = (c.rolling(20).max() - c.rolling(20).min()) / c
    past_pump = (c / c.shift(30) >= 2.0).astype(float)
    d["pumped_before_120d"] = past_pump.rolling(120, min_periods=1).max().shift(1)
    d["pumped_ever"] = past_pump.expanding().max().shift(1)
    d["log_mcap"] = np.log10(mcap.clip(lower=1e5))
    d["log_vol30"] = np.log10(d["vol_avg_30d"].clip(lower=1e2))
    return d


def latest_feature_row(daily: pd.DataFrame) -> Optional[Dict[str, float]]:
    """Feature dict from the last completed daily bar; None if insufficient history."""
    if daily is None or len(daily) < 35:
        return None
    frame = build_daily_features(daily)
    row = frame.iloc[-1]
    out: Dict[str, float] = {}
    for feat in FEATURES:
        value = row.get(feat)
        try:
            value = float(value)
        except Exception:
            return None
        if not math.isfinite(value):
            return None
        out[feat] = value
    return out


def load_model_weights(path: Optional[Path] = None) -> Dict[str, Any]:
    target = Path(path) if path else DEFAULT_WEIGHTS_PATH
    payload = json.loads(target.read_text(encoding="utf-8"))
    if list(payload.get("features") or []) != FEATURES:
        raise ValueError(
            "model_weights.json feature order mismatch — retrain/export via "
            "scripts/pump_precursor_panel.py before scoring"
        )
    return payload


def score_universe(
    feature_rows: Mapping[str, Mapping[str, float]],
    model: Mapping[str, Any],
) -> pd.DataFrame:
    """Cross-sectional rank + logistic score. Index: symbol; sorted best-first.

    BLAS-free on purpose: this box's numpy matmul crashes natively (see
    docs/AMBUSH_MODES_BACKTEST_REPORT_2026-07-18.md) — stick to ufuncs.
    """
    if not feature_rows:
        return pd.DataFrame()
    frame = pd.DataFrame({sym: dict(row) for sym, row in feature_rows.items()}).T
    frame = frame[FEATURES].astype(float)
    ranks = frame.rank(pct=True)
    weights = np.asarray(model["weights"], dtype=float)
    bias = float(model["bias"])
    centered = ranks.to_numpy(dtype=float) - 0.5
    z = (centered * weights).sum(axis=1) + bias
    frame["score"] = 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))
    for feat in FEATURES:
        frame[feat + "_rank"] = ranks[feat]
    return frame.sort_values("score", ascending=False)


def top_feature_drivers(scored_row: Mapping[str, Any], model: Mapping[str, Any], k: int = 3) -> List[str]:
    """The k features contributing most positively for one scored symbol row."""
    weights = dict(zip(model["features"], model["weights"]))
    contribs = []
    for feat in FEATURES:
        rank = scored_row.get(feat + "_rank")
        try:
            rank = float(rank)
        except Exception:
            continue
        if math.isfinite(rank):
            contribs.append((feat, (rank - 0.5) * weights.get(feat, 0.0)))
    contribs.sort(key=lambda kv: kv[1], reverse=True)
    return [name for name, value in contribs[:k] if value > 0]
