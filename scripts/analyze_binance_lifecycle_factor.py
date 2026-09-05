"""Validate contract listing age as an exchange-lifecycle ranking challenger."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


VALIDATION = _load("binance_runup_validation_for_lifecycle", ROOT / "core" / "research" / "binance_runup_validation.py")
PRICE_FACTORS = VALIDATION.PRICE_FACTORS
LIFECYCLE_FACTOR = "listing_age_log"


def paired_week_bootstrap(rows: pd.DataFrame, *, samples: int = 5000, seed: int = 20260816) -> dict[str, Any]:
    data = rows[
        ["date", "label_primary_int", "price_model_score", "lifecycle_combo_score", "price_model_pctile", "lifecycle_combo_pctile"]
    ].dropna().copy()
    data["week"] = pd.to_datetime(data["date"], utc=True).dt.to_period("W-SUN").astype(str)
    groups = [group.index.to_numpy() for _, group in data.groupby("week")]
    rng = np.random.default_rng(seed)
    ap_delta: list[float] = []
    precision_delta: list[float] = []
    lift_delta: list[float] = []
    for _ in range(samples):
        indices = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        draw = data.loc[indices]
        labels = draw["label_primary_int"].astype(int)
        if labels.nunique() < 2:
            continue
        ap_delta.append(float(average_precision_score(labels, draw["lifecycle_combo_score"]) - average_precision_score(labels, draw["price_model_score"])))
        base_rate = float(labels.mean())
        price_selected = labels[draw["price_model_pctile"] >= 0.98]
        combo_selected = labels[draw["lifecycle_combo_pctile"] >= 0.98]
        if price_selected.empty or combo_selected.empty or base_rate <= 0:
            continue
        price_precision = float(price_selected.mean())
        combo_precision = float(combo_selected.mean())
        precision_delta.append(combo_precision - price_precision)
        lift_delta.append(combo_precision / base_rate - price_precision / base_rate)

    def interval(values: list[float]) -> dict[str, Any]:
        series = pd.Series(values, dtype=float)
        return {
            "samples": int(len(series)),
            "median": float(series.median()) if len(series) else None,
            "lower_95pct": float(series.quantile(0.025)) if len(series) else None,
            "upper_95pct": float(series.quantile(0.975)) if len(series) else None,
        }

    return {"cluster": "7d_week", "average_precision_delta": interval(ap_delta), "precision_delta": interval(precision_delta), "lift_delta": interval(lift_delta)}


def event_capture(scored: pd.DataFrame, pctile_column: str) -> dict[str, Any]:
    events = VALIDATION.independent_event_episodes(scored, score_pctile_column=pctile_column)
    return {
        "events": int(len(events)),
        "captured": int(events["captured_top_2pct"].sum()) if len(events) else 0,
        "capture_rate": float(events["captured_top_2pct"].mean()) if len(events) else None,
    }


def main() -> None:
    daily = pd.read_csv(BASELINE / "daily_feature_panel.csv.gz", compression="gzip")
    daily["date"] = pd.to_datetime(daily["date"], utc=True)
    panel_4h = pd.read_csv(BASELINE / "futures_4h_panel.csv.gz", compression="gzip")
    panel_4h["open_time"] = pd.to_datetime(panel_4h["open_time"], utc=True)
    targeted = VALIDATION.add_forward_targets_from_4h(daily, panel_4h, horizons=(14,))
    modeling = VALIDATION.prepare_modeling_panel(targeted)
    metadata = VALIDATION.load_exchange_metadata(BASELINE / "exchange_info_snapshot.json")
    modeling = modeling.merge(metadata[["symbol", "onboard_date"]], on="symbol", how="left", validate="many_to_one")
    modeling["listing_age_days"] = (modeling["date"] - modeling["onboard_date"]).dt.total_seconds() / 86_400.0
    modeling[LIFECYCLE_FACTOR] = np.log1p(modeling["listing_age_days"].clip(lower=0))
    modeling["listing_age_bucket"] = pd.cut(
        modeling["listing_age_days"],
        bins=[-np.inf, 30, 90, 180, np.inf],
        labels=["0-30d", "31-90d", "91-180d", ">180d"],
    ).astype(str)

    folds = VALIDATION.expanding_walk_forward_splits(modeling)
    scored_frames: list[pd.DataFrame] = []
    fold_records: list[dict[str, Any]] = []
    model_records: list[dict[str, Any]] = []
    for fold in folds:
        train = modeling.loc[fold["train_index"]].copy()
        test = modeling.loc[fold["test_index"]].copy()
        if int(train["label_primary_int"].sum()) < 10 or int(test["label_primary_int"].sum()) < 3:
            continue
        price_model = VALIDATION.fit_frozen_logistic(train, PRICE_FACTORS)
        age_model = VALIDATION.fit_frozen_logistic(train, (LIFECYCLE_FACTOR,))
        combo_model = VALIDATION.fit_frozen_logistic(train, (*PRICE_FACTORS, LIFECYCLE_FACTOR))
        test["price_model_score"] = price_model.predict_score(test)
        test["lifecycle_age_score"] = age_model.predict_score(test)
        test["lifecycle_combo_score"] = combo_model.predict_score(test)
        for score in ["price_model_score", "lifecycle_age_score", "lifecycle_combo_score"]:
            test[score.replace("score", "pctile")] = test[score].groupby(test["date"]).rank(pct=True, method="first")
        test["fold"] = int(fold["fold"])
        scored_frames.append(test)
        price = VALIDATION.score_metrics(test, "price_model_score", "label_primary_int")
        age = VALIDATION.score_metrics(test, "lifecycle_age_score", "label_primary_int")
        combo = VALIDATION.score_metrics(test, "lifecycle_combo_score", "label_primary_int")
        fold_records.append(
            {
                "fold": int(fold["fold"]),
                "train_start": fold["train_start"],
                "train_end": fold["train_end"],
                "test_start": fold["test_start"],
                "test_end": fold["test_end"],
                "rows": int(len(test)),
                "positives": int(test["label_primary_int"].sum()),
                "price_average_precision": price["average_precision"],
                "age_average_precision": age["average_precision"],
                "combo_average_precision": combo["average_precision"],
                "price_top2_precision": price["top_2pct_precision"],
                "age_top2_precision": age["top_2pct_precision"],
                "combo_top2_precision": combo["top_2pct_precision"],
                "price_top2_lift": price["top_2pct_lift"],
                "age_top2_lift": age["top_2pct_lift"],
                "combo_top2_lift": combo["top_2pct_lift"],
                "ap_improved": bool(combo["average_precision"] > price["average_precision"]),
                "lift_improved": bool(combo["top_2pct_lift"] > price["top_2pct_lift"]),
            }
        )
        model_records.append(
            {
                "fold": int(fold["fold"]),
                "standardized_listing_age_coefficient": float(combo_model.coefficients[-1]),
                "train_listing_age_median": float(combo_model.medians[-1]),
                "train_listing_age_lower": float(combo_model.lower_bounds[-1]),
                "train_listing_age_upper": float(combo_model.upper_bounds[-1]),
            }
        )

    scored = pd.concat(scored_frames, ignore_index=True)
    fold_metrics = pd.DataFrame(fold_records)
    models = pd.DataFrame(model_records)
    keep = [
        "symbol", "date", "fold", "listing_age_days", "listing_age_bucket", "label_primary_int",
        "target_complete_14d", "target_max_return_14d", "price_model_score", "price_model_pctile",
        "lifecycle_age_score", "lifecycle_age_pctile", "lifecycle_combo_score", "lifecycle_combo_pctile",
    ]
    scored[keep].to_csv(REPORT / "lifecycle_factor_scored.csv.gz", index=False, compression="gzip")
    fold_metrics.to_csv(REPORT / "lifecycle_factor_fold_metrics.csv", index=False)
    models.to_csv(REPORT / "lifecycle_factor_models.csv", index=False)

    segment = (
        scored.groupby("listing_age_bucket", observed=True)
        .agg(rows=("label_primary_int", "size"), positives=("label_primary_int", "sum"), base_rate=("label_primary_int", "mean"))
        .reset_index()
    )
    segment.to_csv(REPORT / "lifecycle_factor_age_segments.csv", index=False)
    price_pooled = VALIDATION.score_metrics(scored, "price_model_score", "label_primary_int")
    age_pooled = VALIDATION.score_metrics(scored, "lifecycle_age_score", "label_primary_int")
    combo_pooled = VALIDATION.score_metrics(scored, "lifecycle_combo_score", "label_primary_int")
    bootstrap = paired_week_bootstrap(scored)
    price_events = event_capture(scored, "price_model_pctile")
    combo_events = event_capture(scored, "lifecycle_combo_pctile")
    joint_share = float((fold_metrics["ap_improved"] & fold_metrics["lift_improved"]).mean())
    gate = bool(
        joint_share > 0.50
        and combo_pooled["average_precision"] > price_pooled["average_precision"]
        and combo_pooled["top_2pct_lift"] > price_pooled["top_2pct_lift"]
        and bootstrap["average_precision_delta"]["lower_95pct"] > 0
        and bootstrap["lift_delta"]["lower_95pct"] > 0
        and combo_events["capture_rate"] >= price_events["capture_rate"]
    )
    output = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_exchange_lifecycle_challenger",
        "factor": LIFECYCLE_FACTOR,
        "availability": "contract onboard date is known at the signal timestamp; age is recomputed per date",
        "models": ["price_only", "listing_age_only", "price_plus_listing_age"],
        "price_pooled": price_pooled,
        "age_only_pooled": age_pooled,
        "price_plus_age_pooled": combo_pooled,
        "fold_joint_ap_and_lift_improvement_share": joint_share,
        "paired_week_bootstrap": bootstrap,
        "price_event_capture": price_events,
        "price_plus_age_event_capture": combo_events,
        "coefficient_negative_share": float((models["standardized_listing_age_coefficient"] < 0).mean()),
        "historical_incremental_gate_pass": gate,
        "classification": "forward_ranking_challenger" if gate else "annotation_only",
        "frozen_price_ranking_changed": False,
        "automatic_trading_allowed": False,
    }
    (REPORT / "lifecycle_factor_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
