"""Binance delisting-risk score: a defensive flag, not a trade.

Why (docs/LLM_TRADING_RESEARCH_ROUND4_2026-09-27.md, scripts/delist_risk_study.py):
on a survivorship-free monthly panel (2023-03..2026-07, delisted pairs
included) a logistic on cross-sectionally ranked features predicted "named in
a Binance delisting announcement within 60 days" with AUC 0.75 both in the
2023-24 fit and on 2025-26, and the top-5% bucket was hit 5x the base rate.
Shorting that bucket LOST money (illiquid coins squeeze), so the score is only
used to warn: 9 of 10 flagged coins are NOT delisted within 60 days.

Features use only the past and must be ranked across the WHOLE spot universe
of the same day (risk is relative), which is why scoring fetches every pair.
One implementation serves both the study and live scoring.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RISK_DIR = PROJECT_ROOT / "data" / "research" / "delist_risk"
MODEL_PATH = RISK_DIR / "model.json"
SCORES_PATH = RISK_DIR / "latest_scores.json"
# +1: higher value = more risk after ranking; -1: lower value = more risk.
FEATURE_SIGNS = {"logvol30": -1, "voltrend": -1, "ret90": -1, "dd365": -1, "age": +1}
FLAG_PERCENTILE = 0.95
MIN_HISTORY_DAYS = 120
STABLE_OR_LEVERAGED = (
    r"(UP|DOWN|BULL|BEAR)$|^(USDC|BUSD|TUSD|FDUSD|USDP|DAI|PAX|EUR|GBP|AUD|UST|USTC|USDS|USDSB|SUSD|"
    r"AEUR|XUSD|USD1|PYUSD|RLUSD|U)$"
)


def features_from_daily(close: pd.Series, quote_volume: pd.Series, btc: pd.Series) -> Optional[Dict[str, float]]:
    """Features from one coin's daily history strictly BEFORE the scoring day."""
    c, v = close.dropna(), quote_volume.dropna()
    if len(c) < MIN_HISTORY_DAYS:
        return None
    vol30, vol180 = float(v.tail(30).mean()), float(v.tail(180).mean())
    ret90 = np.nan
    if len(c) > 91:
        b_now, b_then = btc.asof(c.index[-1]), btc.asof(c.index[-91])
        if b_now and b_then:
            ret90 = (c.iloc[-1] / c.iloc[-91] - 1) - (b_now / b_then - 1)
    return {
        "logvol30": math.log10(max(vol30, 1.0)),
        "voltrend": vol30 / max(vol180, 1.0),
        "ret90": float(ret90),
        "dd365": float(c.iloc[-1] / c.tail(365).max() - 1),
        "age": float(len(c)),
    }


def signed_ranks(frame: pd.DataFrame) -> pd.DataFrame:
    """Cross-sectional percentile ranks, oriented so larger = riskier for every feature."""
    return pd.DataFrame({f"{f}_r": frame[f].rank(pct=True) * sign for f, sign in FEATURE_SIGNS.items()})


def load_model(path: Path = MODEL_PATH) -> Dict[str, Any]:
    model = json.loads(path.read_text(encoding="utf-8"))
    if list(model.get("features") or []) != [f"{f}_r" for f in FEATURE_SIGNS]:
        raise ValueError("delist risk model feature order mismatch; re-export with scripts/delist_risk_study.py")
    return model


def score_universe(features: Dict[str, Dict[str, float]], model: Dict[str, Any]) -> pd.DataFrame:
    """Probability-like score and percentile for every coin (BLAS-free: ufuncs only)."""
    frame = pd.DataFrame.from_dict(features, orient="index").dropna()
    if frame.empty:
        return frame
    ranks = signed_ranks(frame)
    z = (ranks.to_numpy(dtype=float) * np.asarray(model["weights"], dtype=float)).sum(axis=1) + float(model["bias"])
    frame["score"] = 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))
    frame["percentile"] = frame["score"].rank(pct=True)
    frame["flagged"] = frame["percentile"] >= FLAG_PERCENTILE
    return frame.sort_values("score", ascending=False)


def write_scores(scored: pd.DataFrame, model: Dict[str, Any], path: Path = SCORES_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe": int(len(scored)),
        "model": {k: model.get(k) for k in ("fitted_on", "test_auc", "test_top5_lift", "note")},
        "scores": {
            base: {"score": round(float(r["score"]), 4), "percentile": round(float(r["percentile"]), 4), "flagged": bool(r["flagged"])}
            for base, r in scored.iterrows()
        },
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def load_scores(path: Path = SCORES_PATH, max_age_hours: float = 72.0) -> Dict[str, Dict[str, Any]]:
    """Latest per-coin scores; empty if missing or stale (never raises)."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        age = datetime.now(timezone.utc) - datetime.fromisoformat(payload["generated_at"])
        return dict(payload.get("scores") or {}) if age.total_seconds() <= max_age_hours * 3600 else {}
    except Exception:
        return {}


async def compute_live_scores(client, model: Dict[str, Any]) -> pd.DataFrame:
    """Fetch every trading Binance USDT spot pair (~480) and score them."""
    import re

    info = (await client.get("https://api.binance.com/api/v3/exchangeInfo")).json()
    skip = re.compile(STABLE_OR_LEVERAGED)
    bases = [s["baseAsset"] for s in info.get("symbols", [])
             if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING" and not skip.search(s["baseAsset"])]
    today = pd.Timestamp.now(tz="UTC").normalize()

    async def daily(base: str) -> Optional[pd.DataFrame]:
        resp = await client.get("https://api.binance.com/api/v3/klines", params={"symbol": f"{base}USDT", "interval": "1d", "limit": 1000})
        rows = resp.json() if resp.status_code == 200 else []
        if not isinstance(rows, list) or not rows:
            return None
        frame = pd.DataFrame([[r[0], float(r[4]), float(r[7])] for r in rows], columns=["t", "close", "qvol"])
        frame.index = pd.to_datetime(frame.pop("t"), unit="ms", utc=True)
        return frame[frame.index < today]  # completed days only

    btc = await daily("BTC")
    if btc is None:
        raise RuntimeError("BTC daily klines unavailable")
    features: Dict[str, Dict[str, float]] = {}
    for base in bases:
        frame = await daily(base)
        if frame is not None:
            feats = features_from_daily(frame["close"], frame["qvol"], btc["close"])
            if feats is not None:
                features[base] = feats
    return score_universe(features, model)
