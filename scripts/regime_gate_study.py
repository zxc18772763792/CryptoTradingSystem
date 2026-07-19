"""Regime gate study: does universe-level heat predict next-month basket returns?

Motivation: in the OOS window even RANDOM 15-coin baskets made +10.9%/week —
the 妖币潮 regime carried everything. If a past-only universe heat indicator
(weekly ignition count / breadth) predicts forward basket returns, gating the
weekly watchlist basket on regime keeps the upside and skips the bleed.

Basket proxy: top-15 by a fixed rank-sum of the 5 strongest univariate
precursor features (vola_30d, range_20d, oi_mcap, funding_7d, dd_from_ath) —
computable for every week without model-training leakage. Episode return =
30d hold with the standard ladder (40%@+100%, 30%@+300%, rest at horizon).

Output: reports/ambush_modes_2026-07-18/regime_gate_study.json + stdout.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from loguru import logger

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util

spec = importlib.util.spec_from_file_location("bt", SCRIPT_DIR / "backtest_ambush_modes.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

OUT_DIR = PROJECT_ROOT / "reports" / "ambush_modes_2026-07-18"
CACHE = OUT_DIR / "panel_ranked_cache.parquet"
TOP_K = 15
HOLD_DAYS = 30
FEE = 0.001
RANK_FEATURES = ["vola_30d_r", "range_20d_r", "oi_mcap_r", "funding_7d_r", "dd_from_ath_r"]


def episode_return(close_by_base: Dict[str, pd.Series], base: str, entry_date: pd.Timestamp):
    series = close_by_base.get(base)
    if series is None:
        return None
    d = series[series.index >= entry_date]
    if len(d) < 2:
        return None
    entry = float(d.iloc[0])
    hold = d.iloc[1 : HOLD_DAYS + 1]
    if not len(hold) or entry <= 0:
        return None
    path = hold / entry
    remaining, realized = 1.0, 0.0
    if (path >= 2.0).any():
        realized += 0.40 * 1.0
        remaining -= 0.40
    if (path >= 4.0).any():
        realized += 0.30 * 3.0
        remaining -= 0.30
    total = realized + remaining * (float(path.iloc[-1]) - 1.0)
    return total - 2 * FEE


def main() -> None:
    panel = pd.read_parquet(CACHE)
    panel["date"] = pd.to_datetime(panel["date"])

    close_by_base: Dict[str, pd.Series] = {}
    hourly_ret_counts: Dict[str, pd.Series] = {}
    for path in sorted((bt.DATA_DIR / "klines_1h").glob("*.parquet")):
        base = path.stem
        kl = pd.read_parquet(path, columns=["close"])
        closes = pd.to_numeric(kl["close"], errors="coerce")
        close_by_base[base] = closes.resample("1D").last().dropna()
        ret_1h = closes.pct_change()
        hourly_ret_counts[base] = (ret_1h >= 0.06).resample("1D").sum()

    ignition_daily = pd.DataFrame(hourly_ret_counts).fillna(0.0).sum(axis=1)
    breadth_frames = pd.DataFrame({b: s.pct_change(7) for b, s in close_by_base.items()})
    breadth_daily = (breadth_frames > 0).mean(axis=1)

    rows: List[Dict[str, Any]] = []
    for date, grp in panel.groupby("date"):
        grp = grp.dropna(subset=RANK_FEATURES)
        if len(grp) < 30:
            continue
        scored = grp.assign(rs=grp[RANK_FEATURES].mean(axis=1)).nlargest(TOP_K, "rs")
        rets = [episode_return(close_by_base, b, date) for b in scored["base"]]
        rets = [r for r in rets if r is not None]
        if not rets:
            continue
        # past-only heat: trailing 7d ignition count and breadth up to the day before
        heat_window = ignition_daily[(ignition_daily.index < date) & (ignition_daily.index >= date - pd.Timedelta(days=7))]
        breadth_window = breadth_daily[(breadth_daily.index < date) & (breadth_daily.index >= date - pd.Timedelta(days=7))]
        rows.append(
            {
                "date": date,
                "basket_ret": float(np.mean(rets)),
                "ignition_7d": float(heat_window.sum()),
                "breadth_7d": float(breadth_window.mean()) if len(breadth_window) else np.nan,
            }
        )
    df = pd.DataFrame(rows).dropna().sort_values("date").reset_index(drop=True)
    logger.info(f"weekly observations: {len(df)}")

    # information: rank correlation heat -> forward basket ret
    corr_ign = float(df["ignition_7d"].rank().corr(df["basket_ret"].rank()))
    corr_breadth = float(df["breadth_7d"].rank().corr(df["basket_ret"].rank()))

    # past-only gate: trade only when ignition_7d >= expanding median (min 8 weeks)
    df["ign_median"] = df["ignition_7d"].expanding(8).median().shift(1)
    df["gate_on"] = df["ignition_7d"] >= df["ign_median"]
    gated = df[df["gate_on"].fillna(False)]
    ungated = df

    def stat(vals: pd.Series) -> Dict[str, Any]:
        if not len(vals):
            return {"n": 0}
        return {
            "n": int(len(vals)),
            "mean": round(float(vals.mean()), 4),
            "median": round(float(vals.median()), 4),
            "win": round(float((vals > 0).mean()), 3),
            "worst": round(float(vals.min()), 4),
        }

    by_quintile = {}
    df["heat_q"] = pd.qcut(df["ignition_7d"], 4, labels=False, duplicates="drop")
    for q, grp in df.groupby("heat_q"):
        by_quintile[f"q{int(q) + 1}"] = stat(grp["basket_ret"])

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "weeks": len(df),
        "rank_corr_ignition_vs_fwd_basket": round(corr_ign, 3),
        "rank_corr_breadth_vs_fwd_basket": round(corr_breadth, 3),
        "basket_by_heat_quartile": by_quintile,
        "ungated": stat(ungated["basket_ret"]),
        "gated_expanding_median": stat(gated["basket_ret"]),
        "gate_active_share": round(float(df["gate_on"].fillna(False).mean()), 3),
    }
    (OUT_DIR / "regime_gate_study.json").write_text(
        json.dumps({**payload, "weekly": df.assign(date=df["date"].astype(str)).to_dict("records")}, ensure_ascii=False, indent=1),
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
