"""Evaluate fixed price-model and exploratory price+OI ranking combinations."""

from __future__ import annotations

import json
import os
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score


HERE = Path(__file__).resolve().parent


def model_score(rows: pd.DataFrame, model: dict) -> np.ndarray:
    factors = model["factors"]
    values = rows[factors].to_numpy(float)
    values = np.where(np.isfinite(values), values, np.nan)
    values = np.where(np.isnan(values), np.array(model["medians"]), values)
    values = np.clip(values, np.array(model["lower_bounds"]), np.array(model["upper_bounds"]))
    standardized = (values - np.array(model["means"])) / np.array(model["scales"])
    coefficients = np.array(model["coefficients_with_intercept"])
    linear = coefficients[0] + np.sum(standardized * coefficients[1:], axis=1)
    return 1.0 / (1.0 + np.exp(-np.clip(linear, -35, 35)))


def evaluate(rows: pd.DataFrame, score: str) -> dict:
    data = rows[["date", "label_200", score]].dropna().copy()
    data["pctile"] = data.groupby("date")[score].rank(pct=True, method="first")
    base = float(data.label_200.mean())
    result = {
        "score": score,
        "n": int(len(data)),
        "positives": int(data.label_200.sum()),
        "base_rate": base,
        "auc": float(roc_auc_score(data.label_200, data[score])),
    }
    for fraction in [0.01, 0.02, 0.05, 0.10]:
        selected = data[data.pctile >= 1 - fraction]
        rate = float(selected.label_200.mean())
        result[f"top_{int(fraction * 100)}pct_n"] = int(len(selected))
        result[f"top_{int(fraction * 100)}pct_rate"] = rate
        result[f"top_{int(fraction * 100)}pct_lift"] = rate / base
    return result


def main() -> None:
    rows = pd.read_csv(HERE / "oi_daily_panel_30d.csv.gz", parse_dates=["date"])
    model = json.loads((HERE / "factor_model.json").read_text(encoding="utf-8"))
    rows["price_model"] = model_score(rows, model)
    for source, target in [
        ("price_model", "price_pct"),
        ("oi_to_cmc_mcap", "oi_level_pct"),
        ("oi_change_1d", "oi_change_pct"),
        ("oi_volatility_3d", "oi_vol_pct"),
    ]:
        rows[target] = rows.groupby("date")[source].rank(pct=True, method="average")
    rows["oi_activity_pct"] = rows[["oi_change_pct", "oi_vol_pct"]].mean(axis=1)
    rows["price80_oi_level20"] = 0.8 * rows["price_pct"] + 0.2 * rows["oi_level_pct"]
    rows["price80_oi_activity20"] = 0.8 * rows["price_pct"] + 0.2 * rows["oi_activity_pct"]
    rows["oi_all_pct"] = rows[["oi_level_pct", "oi_change_pct", "oi_vol_pct"]].mean(axis=1)
    rows["price70_oi_all30"] = 0.7 * rows["price_pct"] + 0.3 * rows["oi_all_pct"]
    score_columns = [
        "price_model",
        "price80_oi_level20",
        "price80_oi_activity20",
        "price70_oi_all30",
    ]
    common = rows.dropna(
        subset=[
            "label_200",
            "price_model",
            "oi_to_cmc_mcap",
            "oi_change_1d",
            "oi_volatility_3d",
        ]
    ).copy()
    metrics = pd.DataFrame([evaluate(common, score) for score in score_columns])
    best = metrics.sort_values("top_2pct_lift", ascending=False).iloc[0].to_dict()
    summary = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "evaluation_window": {
            "first_date": common.date.min().isoformat(),
            "last_date": common.date.max().isoformat(),
            "rows": int(len(common)),
            "positives": int(common.label_200.sum()),
        },
        "scores": metrics.replace({np.nan: None}).to_dict(orient="records"),
        "best_in_same_short_window": best,
        "warning": "OI weights were not trained on a separate sample; comparisons are exploratory and must not be treated as validated strategy selection.",
    }
    metrics.to_csv(HERE / "oi_price_combo_metrics_30d.csv", index=False)
    (HERE / "oi_price_combo_summary_30d.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
