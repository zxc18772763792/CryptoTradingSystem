"""Addendum to the robustness audit: regime split + permutation significance.

E. REGIME DEPENDENCE — the surviving lift may live only in the 2026 meme wave.
   Split the OOS weeks into early (quiet) and late (wave) halves; report lift
   in each. If the edge exists only in the wave, that is a hard limit.
F. PERMUTATION SIGNIFICANCE — with only ~27 independent pump episodes, is the
   2.7x top-15 lift distinguishable from chance? Shuffle the label WITHIN each
   week (preserving weekly base rates) and recompute the walk-forward top-15
   hit rate; report the fraction >= observed.

Pure-numpy (no per-iteration DataFrame copies) for speed and memory safety.
Appends E/F to reports/ambush_modes_2026-07-18/model_robustness_audit.json.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from core.research.pump_precursor import FEATURES  # noqa: E402

PANEL = ROOT / "reports" / "ambush_modes_2026-07-18" / "panel_ranked_cache.parquet"
AUDIT = ROOT / "reports" / "ambush_modes_2026-07-18" / "model_robustness_audit.json"
TOP_K = 15
MIN_TRAIN_WEEKS = 16
RANK = [f + "_r" for f in FEATURES]


def train(X, y, iters=1500, lr=0.5, lam=1e-2):
    w = np.zeros(X.shape[1])
    m = float(y.mean()) if len(y) else 0.05
    b = float(np.log(max(m, 1e-4) / max(1 - m, 1e-4)))
    for _ in range(iters):
        p = 1.0 / (1.0 + np.exp(-np.clip((X * w).sum(1) + b, -30, 30)))
        r = p - y
        w -= lr * ((X * r[:, None]).sum(0) / len(y) + lam * w)
        b -= lr * float(r.mean())
    return w, b


def main() -> None:
    panel = pd.read_parquet(PANEL).dropna(subset=RANK).reset_index(drop=True)
    panel["date"] = pd.to_datetime(panel["date"])
    panel["pump100"] = (panel["fwd30_maxret"] >= 1.0).astype(int)

    X_all = panel[RANK].to_numpy(dtype=float) - 0.5
    y_all = panel["pump100"].to_numpy(dtype=float)
    dates = panel["date"].to_numpy()
    weeks = sorted(panel["date"].unique())

    # Precompute per-test-week train/test row indices (fixed across permutations).
    folds = []
    for i, wk in enumerate(weeks):
        if i < MIN_TRAIN_WEEKS:
            continue
        tr = np.where(dates < wk)[0]
        te = np.where(dates == wk)[0]
        if len(tr) < 300 or len(te) < 20:
            continue
        folds.append((wk, tr, te))

    # Week-group index lists for within-week label shuffling.
    week_groups = [np.where(dates == wk)[0] for wk in weeks]

    def run_walk_forward(y: np.ndarray) -> List[dict]:
        out = []
        for wk, tr, te in folds:
            w, b = train(X_all[tr], y[tr])
            sc = 1.0 / (1.0 + np.exp(-np.clip((X_all[te] * w).sum(1) + b, -30, 30)))
            k = min(TOP_K, len(te))
            top_local = np.argpartition(-sc, k - 1)[:k]
            top_rows = te[top_local]
            out.append({"week": wk, "top_hit": float(y[top_rows].mean()),
                        "base": float(y[te].mean()), "top_pumps": int(y[top_rows].sum())})
        return out

    per = run_walk_forward(y_all)
    n = len(per)
    half = n // 2
    early, late = per[:half], per[half:]

    def agg(block):
        if not block:
            return {"weeks": 0}
        hit = float(np.mean([b["top_hit"] for b in block]))
        base = float(np.mean([b["base"] for b in block]))
        return {"weeks": len(block), "top_hit": round(hit, 4), "base": round(base, 4),
                "lift_x": round(hit / base, 2) if base > 0 else None,
                "total_pumps_in_top": int(sum(b["top_pumps"] for b in block)),
                "span": f"{pd.Timestamp(block[0]['week']).date()}..{pd.Timestamp(block[-1]['week']).date()}"}

    regime = {"early_half": agg(early), "late_half": agg(late), "all": agg(per)}

    rng = np.random.default_rng(20260722)
    observed = float(np.mean([b["top_hit"] for b in per]))
    N_PERM = 400
    ge, perm_hits = 0, []
    y_perm = y_all.copy()
    for _ in range(N_PERM):
        for g in week_groups:
            if len(g) > 1:
                y_perm[g] = y_all[g][rng.permutation(len(g))]
        pblock = run_walk_forward(y_perm)
        mh = float(np.mean([b["top_hit"] for b in pblock])) if pblock else 0.0
        perm_hits.append(mh)
        if mh >= observed - 1e-12:
            ge += 1
    perm = {"observed_top_hit": round(observed, 4), "n_perm": N_PERM,
            "perm_mean": round(float(np.mean(perm_hits)), 4),
            "perm_p95": round(float(np.percentile(perm_hits, 95)), 4),
            "frac_ge_observed": round(ge / N_PERM, 4),
            "note": "label shuffled within each week so weekly base rates preserved; frac_ge is the p-value"}

    audit = json.loads(AUDIT.read_text(encoding="utf-8"))
    audit["E_regime_split"] = regime
    audit["F_permutation_significance"] = perm
    AUDIT.write_text(json.dumps(audit, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"E_regime_split": regime, "F_permutation_significance": perm}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
