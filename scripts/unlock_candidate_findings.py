"""Deterministic candidate findings for the unlock-overhang signal.

Computes the raw statistics that will be handed to adversarial verifiers.
Numbers here are deterministic and inspectable; the workflow's job is to try
to BREAK each finding, not to recompute it differently.

Output: reports/ambush_modes_2026-07-18/unlock_candidate_findings.json
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PANEL = ROOT / "reports" / "ambush_modes_2026-07-18" / "unlock_feature_panel.parquet"
OUT = ROOT / "reports" / "ambush_modes_2026-07-18" / "unlock_candidate_findings.json"
OOS = pd.Timestamp("2026-03-18")


def rate(d: pd.DataFrame, mask: pd.Series, col: str = "pump100") -> dict:
    sub = d[mask]
    return {"n": int(len(sub)), "pump_rate": round(float(sub[col].mean()), 4) if len(sub) else None,
            "pumps": int(sub[col].sum()) if len(sub) else 0,
            "fwd_mean": round(float(sub["fwd30_maxret"].mean()), 4) if len(sub) else None,
            "fwd_median": round(float(sub["fwd30_maxret"].median()), 4) if len(sub) else None}


def main() -> None:
    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])
    a = df[(df["has_schedule"] >= 0) & df["pump100"].notna() & df["unlock_next_30d_pct"].notna()].copy()
    base_rate = float(a["pump100"].mean())

    findings = {}
    findings["_meta"] = {
        "analyzable_rows": int(len(a)),
        "base_pump100_rate": round(base_rate, 4),
        "with_real_schedule": int((a["has_schedule"] == 1).sum()),
        "full_float_rows": int((a["has_schedule"] == 0).sum()),
        "oos_split": str(OOS.date()),
        "unique_bases": int(a["base"].nunique()),
    }

    # F1: big upcoming unlock (>=5% mcap/30d) suppresses pumps
    big = a["unlock_next_30d_pct"] >= 0.05
    findings["F1_big_unlock_suppresses"] = {
        "claim": "coins with >=5% mcap unlocking in next 30d pump less often",
        "with_big_unlock": rate(a, big),
        "without": rate(a, ~big),
        "is": {"big": rate(a[a["date"] < OOS], (a[a["date"] < OOS]["unlock_next_30d_pct"] >= 0.05)),
               "small": rate(a[a["date"] < OOS], (a[a["date"] < OOS]["unlock_next_30d_pct"] < 0.05))},
        "oos": {"big": rate(a[a["date"] >= OOS], (a[a["date"] >= OOS]["unlock_next_30d_pct"] >= 0.05)),
                "small": rate(a[a["date"] >= OOS], (a[a["date"] >= OOS]["unlock_next_30d_pct"] < 0.05))},
    }

    # F2: recently unlocked (>=5% mcap past 30d) forward pump
    post = a["unlock_past_30d_pct"] >= 0.05
    findings["F2_post_unlock"] = {
        "claim": "coins that just unlocked >=5% mcap pump less often forward",
        "recently_unlocked": rate(a, post), "not": rate(a, ~post),
    }

    # F3: quintile monotonicity of unlock_next_30d
    a2 = a.copy()
    a2["uq"] = pd.qcut(a2["unlock_next_30d_pct"].rank(method="first"), 5, labels=False)
    findings["F3_quintile_shape"] = {
        "claim": "pump rate declines monotonically with unlock overhang",
        "by_quintile": {int(q): rate(a2, a2["uq"] == q) for q in range(5)},
    }

    # F4: veto gate value — does excluding big-unlock coins lift a naive top-decile basket?
    # naive score = -unlock_next_30d_pct is not a predictor; test as a FILTER on the
    # existing model's territory: within the model panel, does removing big-unlock rows
    # raise the pump rate of the remaining pool?
    findings["F4_gate_effect"] = {
        "claim": "vetoing big-unlock coins raises residual pool pump rate",
        "full_pool": rate(a, pd.Series(True, index=a.index)),
        "after_veto": rate(a, ~big),
        "removed_rows": int(big.sum()),
        "removed_pumps": int(a[big]["pump100"].sum()),
    }

    # F5: confound exposure — is unlock_next_30d just proxying mcap or age?
    a3 = a.dropna(subset=["mcap"]).copy()
    a3["logmcap"] = np.log10(a3["mcap"].clip(lower=1e5))
    corr_mcap = float(a3["unlock_next_30d_pct"].corr(a3["logmcap"]))
    findings["F5_confounds"] = {
        "claim": "unlock overhang is confounded with market cap",
        "corr_unlock_vs_logmcap": round(corr_mcap, 3),
        "big_unlock_median_logmcap": round(float(a3[a3["unlock_next_30d_pct"] >= 0.05]["logmcap"].median()), 3),
        "small_unlock_median_logmcap": round(float(a3[a3["unlock_next_30d_pct"] < 0.05]["logmcap"].median()), 3),
    }

    OUT.write_text(json.dumps(findings, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(findings, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
