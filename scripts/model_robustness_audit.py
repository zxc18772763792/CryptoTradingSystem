"""Walk-forward robustness audit of the surviving cross-sectional pump model.

Everything else in this project died under adversarial verification. The weekly
cross-sectional model is the sole survivor, but it was only ever validated on a
SINGLE 2026-02-15 split (26.7% top-15 hit vs 9.7% base = 2.7x). The unlock
false-positive we just killed was driven by pseudo-replication (58 bases,
overlapping 30d windows, one coin = WLD). This harness applies the same
scrutiny to the survivor, deterministically:

  A. WALK-FORWARD: expanding-window retrain, score each future week, top-15 hit
     rate vs that week's base rate, per OOS block. Is the lift stable or was
     one split lucky?
  B. PSEUDO-REPLICATION: dedup row-level pumps to independent episodes; how many
     DISTINCT bases do the model's top-15 hits come from? Is it a few coins?
  C. FEATURE ABLATION: single-feature and drop-one top-15 lift OOS. Which
     features actually carry signal?
  D. LABEL SENSITIVITY: repeat lift at +50/100/200/300% thresholds.

BLAS note: this box's numpy matmul was fixed (openblas) but the trainer stays
ufunc-based for determinism/portability.

Output: reports/ambush_modes_2026-07-18/model_robustness_audit.json + stdout.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.research.pump_precursor import FEATURES  # noqa: E402

PANEL = ROOT / "reports" / "ambush_modes_2026-07-18" / "panel_ranked_cache.parquet"
OUT = ROOT / "reports" / "ambush_modes_2026-07-18" / "model_robustness_audit.json"
TOP_K = 15
MIN_TRAIN_WEEKS = 16
RANK = [f + "_r" for f in FEATURES]


def train_logistic(X: np.ndarray, y: np.ndarray, iters: int = 3000, lr: float = 0.5, lam: float = 1e-2) -> Tuple[np.ndarray, float]:
    w = np.zeros(X.shape[1])
    p_mean = float(y.mean()) if len(y) else 0.05
    b = float(np.log(max(p_mean, 1e-4) / max(1 - p_mean, 1e-4)))
    for _ in range(iters):
        z = (X * w).sum(axis=1) + b
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))
        resid = p - y
        w -= lr * ((X * resid[:, None]).sum(axis=0) / len(y) + lam * w)
        b -= lr * float(resid.mean())
    return w, b


def score(df: pd.DataFrame, w: np.ndarray, b: float) -> np.ndarray:
    X = df[RANK].to_numpy(dtype=float) - 0.5
    z = (X * w).sum(axis=1) + b
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))


def episodes(pump_rows: pd.DataFrame) -> int:
    """Count independent pump episodes: consecutive weekly pumps of one base
    within ~35d collapse to one episode."""
    n = 0
    for base, grp in pump_rows.groupby("base"):
        dates = sorted(grp["date"])
        last = None
        for d in dates:
            if last is None or (d - last).days > 35:
                n += 1
            last = d
    return n


def walk_forward(panel: pd.DataFrame, feats: List[str], label: str = "pump100") -> Dict:
    rank_cols = [f + "_r" for f in feats]
    weeks = sorted(panel["date"].unique())
    picks: List[pd.DataFrame] = []
    per_week = []
    for i, wk in enumerate(weeks):
        if i < MIN_TRAIN_WEEKS:
            continue
        train = panel[panel["date"] < wk].dropna(subset=rank_cols + [label])
        test = panel[panel["date"] == wk].dropna(subset=rank_cols + [label])
        if len(train) < 300 or len(test) < 20:
            continue
        w, b = train_logistic(train[rank_cols].to_numpy(float) - 0.5, train[label].to_numpy(float))
        t = test.copy()
        t["score"] = 1.0 / (1.0 + np.exp(-np.clip((( t[rank_cols].to_numpy(float) - 0.5) * w).sum(axis=1) + b, -30, 30)))
        top = t.nlargest(TOP_K, "score")
        picks.append(top.assign(week=wk))
        per_week.append({
            "week": str(pd.Timestamp(wk).date()),
            "base_rate": round(float(t[label].mean()), 4),
            "top_hit": round(float(top[label].mean()), 4),
            "top_pumps": int(top[label].sum()),
        })
    allpicks = pd.concat(picks) if picks else pd.DataFrame()
    hit = float(allpicks[label].mean()) if len(allpicks) else 0.0
    base = float(pd.concat([panel[panel["date"] == pd.Timestamp(pw["week"])] for pw in per_week])[label].mean()) if per_week else 0.0
    # simpler base: mean of per-week base rates (equal-weight weeks)
    base_ew = float(np.mean([pw["base_rate"] for pw in per_week])) if per_week else 0.0
    top_pump_rows = allpicks[allpicks[label] == 1] if len(allpicks) else pd.DataFrame()
    return {
        "oos_weeks": len(per_week),
        "top_hit_rate": round(hit, 4),
        "base_rate_ew": round(base_ew, 4),
        "lift_x": round(hit / base_ew, 2) if base_ew > 0 else None,
        "top_hits_total": int(allpicks[label].sum()) if len(allpicks) else 0,
        "top_hit_unique_bases": int(top_pump_rows["base"].nunique()) if len(top_pump_rows) else 0,
        "top_hit_episodes": episodes(top_pump_rows) if len(top_pump_rows) else 0,
        "per_week": per_week,
    }


def main() -> None:
    panel = pd.read_parquet(PANEL)
    panel["date"] = pd.to_datetime(panel["date"])
    # derive extra labels from fwd30_maxret
    for thr, name in ((0.5, "pump50"), (1.0, "pump100"), (2.0, "pump200"), (3.0, "pump300")):
        panel[name] = (panel["fwd30_maxret"] >= thr).astype(int)

    result: Dict = {"config": {"top_k": TOP_K, "min_train_weeks": MIN_TRAIN_WEEKS,
                               "total_weeks": int(panel["date"].nunique()),
                               "unique_bases": int(panel["base"].nunique())}}

    # A. walk-forward on the primary label
    result["A_walk_forward_pump100"] = walk_forward(panel, FEATURES, "pump100")

    # B. pseudo-replication summary (row vs episode)
    wf = result["A_walk_forward_pump100"]
    result["B_pseudo_replication"] = {
        "row_level_top_hits": wf["top_hits_total"],
        "unique_bases_in_hits": wf["top_hit_unique_bases"],
        "independent_episodes_in_hits": wf["top_hit_episodes"],
        "note": "if unique_bases and episodes << row_level hits, the 'hit rate' is inflated by weekly-overlapping windows of a few coins",
    }

    # C. feature ablation (single-feature and drop-one), lift on pump100 walk-forward
    single = {}
    for f in FEATURES:
        r = walk_forward(panel, [f], "pump100")
        single[f] = {"lift_x": r["lift_x"], "hit": r["top_hit_rate"], "unique_bases": r["top_hit_unique_bases"]}
    drop_one = {}
    for f in FEATURES:
        r = walk_forward(panel, [x for x in FEATURES if x != f], "pump100")
        drop_one[f] = {"lift_x": r["lift_x"], "hit": r["top_hit_rate"]}
    result["C_feature_ablation"] = {"single_feature": single, "drop_one": drop_one,
                                    "full_model_lift": wf["lift_x"]}

    # D. label sensitivity
    labels = {}
    for name in ("pump50", "pump100", "pump200", "pump300"):
        r = walk_forward(panel, FEATURES, name)
        labels[name] = {"lift_x": r["lift_x"], "hit": r["top_hit_rate"], "base": r["base_rate_ew"],
                        "hits": r["top_hits_total"], "unique_bases": r["top_hit_unique_bases"], "episodes": r["top_hit_episodes"]}
    result["D_label_sensitivity"] = labels

    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    # concise stdout
    print(json.dumps({k: v for k, v in result.items() if k != "A_walk_forward_pump100"}, ensure_ascii=False, indent=1))
    print("\nA_walk_forward_pump100 summary:")
    print(json.dumps({k: v for k, v in wf.items() if k != "per_week"}, ensure_ascii=False, indent=1))
    print("per-week hits:", [(p["week"], p["top_pumps"]) for p in wf["per_week"]])


if __name__ == "__main__":
    main()
