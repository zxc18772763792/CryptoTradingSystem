"""Run event deduplication, ablation and null controls for sequence evidence."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PRIMARY = _load(
    "binance_sequential_strategy_for_robustness",
    ROOT / "scripts" / "analyze_binance_sequential_strategy.py",
)
SEQ = PRIMARY.SEQUENTIAL
VALIDATION = PRIMARY.VALIDATION


RULES: dict[str, tuple[str, Callable[[pd.DataFrame], pd.Series]]] = {
    "all_candidates": ("no 24h filter", lambda x: pd.Series(True, index=x.index)),
    "close_positive": ("24h close return >= 0%", lambda x: x["early_close_return"] >= 0.0),
    "close5": ("24h close return >= 5%", lambda x: x["early_close_return"] >= 0.05),
    "mfe10": ("24h MFE >= 10%", lambda x: x["early_mfe"] >= 0.10),
    "mfe20": ("24h MFE >= 20%", lambda x: x["early_mfe"] >= 0.20),
    "mfe10_close0": (
        "24h MFE >= 10% and close return >= 0%",
        lambda x: (x["early_mfe"] >= 0.10) & (x["early_close_return"] >= 0.0),
    ),
    "mfe20_close5": (
        "24h MFE >= 20% and close return >= 5%",
        lambda x: (x["early_mfe"] >= 0.20) & (x["early_close_return"] >= 0.05),
    ),
    "controlled_launch": (
        "MFE >= 10%, non-negative close, MAE > -15%, pullback > -15%",
        lambda x: (
            (x["early_mfe"] >= 0.10) & (x["early_close_return"] >= 0.0)
            & (x["early_mae"] > -0.15) & (x["early_pullback_from_peak"] > -0.15)
        ),
    ),
    "demand_launch": (
        "close >= 3%, volume >= 1.1x prior, taker share >= 50%",
        lambda x: (
            (x["early_close_return"] >= 0.03) & (x["early_volume_ratio_prior"] >= 1.10)
            & (x["early_taker_buy_share"] >= 0.50)
        ),
    ),
    "clean_breakout": (
        "close >= 5%, efficiency >= 0.15, pullback > -10%",
        lambda x: (
            (x["early_close_return"] >= 0.05) & (x["early_path_efficiency"] >= 0.15)
            & (x["early_pullback_from_peak"] > -0.10)
        ),
    ),
}


def rule_metrics(rows: pd.DataFrame, mask: pd.Series) -> dict[str, Any]:
    labels = rows["original_target200"].astype(bool)
    selected = rows[mask.fillna(False)]
    positives = int(labels.sum())
    selected_positives = int(selected["original_target200"].sum())
    return {
        "rows": int(len(rows)),
        "positives": positives,
        "base_precision": float(labels.mean()) if len(labels) else 0.0,
        "selected_rows": int(len(selected)),
        "selected_positives": selected_positives,
        "selected_precision": float(selected["original_target200"].mean()) if len(selected) else 0.0,
        "positive_retention": float(selected_positives / positives) if positives else 0.0,
        "late_selected_positives": int(selected["late_target200"].sum()),
        "late_selected_precision": float(selected["late_target200"].mean()) if len(selected) else 0.0,
    }


def select_simple_rule(rows: pd.DataFrame, *, cutoff: pd.Timestamp) -> pd.DataFrame:
    calibration = rows[rows["decision_time"] + pd.Timedelta(days=14) < cutoff].copy()
    records: list[dict[str, Any]] = []
    for name, (description, rule) in RULES.items():
        metrics = rule_metrics(calibration, rule(calibration))
        eligible = bool(
            name != "all_candidates" and metrics["rows"] >= 80 and metrics["positives"] >= 5
            and metrics["selected_rows"] >= 20
            and metrics["selected_precision"] > metrics["base_precision"]
            and metrics["positive_retention"] >= 0.40
        )
        score = (
            PRIMARY.ENTRY.wilson_lower_bound(metrics["selected_positives"], metrics["selected_rows"])
            + 0.10 * metrics["positive_retention"]
            + 0.03 * metrics["late_selected_precision"]
        )
        records.append(
            {
                "rule": name,
                "description": description,
                **metrics,
                "selection_score": float(score),
                "selection_eligible": eligible,
            }
        )
    return pd.DataFrame(records).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    ).reset_index(drop=True)


def event_clusters(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for symbol, group in rows.sort_values(["symbol", "date"]).groupby("symbol"):
        cluster: list[pd.Series] = []
        previous: pd.Timestamp | None = None
        for _, row in group.iterrows():
            when = pd.Timestamp(row["date"])
            if previous is not None and when - previous > pd.Timedelta(days=14):
                records.append(summarize_cluster(symbol, cluster))
                cluster = []
            cluster.append(row)
            previous = when
        if cluster:
            records.append(summarize_cluster(symbol, cluster))
    return pd.DataFrame(records)


def summarize_cluster(symbol: str, cluster: list[pd.Series]) -> dict[str, Any]:
    frame = pd.DataFrame(cluster)
    return {
        "symbol": symbol,
        "start_date": frame["date"].min(),
        "end_date": frame["date"].max(),
        "signals": int(len(frame)),
        "positive_event": bool(frame["original_target200"].any()),
        "sequence_selected": bool(frame["sequence_selected"].any()),
        "max_sequence_score": float(frame["sequence_score"].max()),
    }


def permutation_null(rows: pd.DataFrame, *, samples: int = 5000) -> dict[str, Any]:
    labels = rows["original_target200"].astype(int).to_numpy()
    actual_ap_delta = float(
        average_precision_score(labels, rows["sequence_score"])
        - average_precision_score(labels, rows["price_model_score"])
    )
    selected = rows["sequence_selected"].to_numpy(dtype=bool)
    actual_precision_delta = float(labels[selected].mean() - labels.mean())
    fold_indices = [group.index.to_numpy() for _, group in rows.reset_index(drop=True).groupby("fold")]
    rng = np.random.default_rng(20260802)
    ap_null: list[float] = []
    precision_null: list[float] = []
    sequence = rows["sequence_score"].to_numpy(dtype=float)
    price = rows["price_model_score"].to_numpy(dtype=float)
    for _ in range(samples):
        shuffled = labels.copy()
        for indices in fold_indices:
            shuffled[indices] = rng.permutation(shuffled[indices])
        ap_null.append(float(average_precision_score(shuffled, sequence) - average_precision_score(shuffled, price)))
        precision_null.append(float(shuffled[selected].mean() - shuffled.mean()))
    ap_series = pd.Series(ap_null)
    precision_series = pd.Series(precision_null)
    return {
        "samples": samples,
        "actual_ap_delta": actual_ap_delta,
        "ap_one_sided_pvalue": float((1 + (ap_series >= actual_ap_delta).sum()) / (samples + 1)),
        "ap_null_95pct": [float(ap_series.quantile(0.025)), float(ap_series.quantile(0.975))],
        "actual_precision_delta": actual_precision_delta,
        "precision_one_sided_pvalue": float((1 + (precision_series >= actual_precision_delta).sum()) / (samples + 1)),
        "precision_null_95pct": [float(precision_series.quantile(0.025)), float(precision_series.quantile(0.975))],
    }


def leave_one_symbol_out(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for symbol in sorted(rows["symbol"].unique()):
        draw = rows[rows["symbol"] != symbol]
        labels = draw["original_target200"].astype(int)
        selected = draw[draw["sequence_selected"]]
        if labels.nunique() < 2 or selected.empty:
            continue
        records.append(
            {
                "excluded_symbol": symbol,
                "rows": int(len(draw)),
                "positives": int(labels.sum()),
                "ap_delta": float(
                    average_precision_score(labels, draw["sequence_score"])
                    - average_precision_score(labels, draw["price_model_score"])
                ),
                "precision_delta": float(selected["original_target200"].mean() - labels.mean()),
            }
        )
    return pd.DataFrame(records)


def main() -> None:
    decision = json.loads((REPORT / "sequential_strategy_decision.json").read_text(encoding="utf-8"))
    hours = int(decision["selected_checkpoint_hours"])
    family = str(decision["selected_model_family"])
    candidates = pd.read_csv(REPORT / "sequential_checkpoint_candidates.csv.gz", compression="gzip")
    for column in ["date", "baseline_entry_time", "decision_time"]:
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    rows = candidates[candidates["checkpoint_hours"] == hours].copy()
    folds = pd.read_csv(REPORT / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])
    confirmation = pd.read_csv(REPORT / "sequential_model_confirmation_scored.csv.gz", compression="gzip")
    for column in ["date", "baseline_entry_time", "decision_time"]:
        confirmation[column] = pd.to_datetime(confirmation[column], utc=True)

    rule_calibration = select_simple_rule(rows, cutoff=cutoff)
    rule_calibration.to_csv(REPORT / "sequential_simple_rule_calibration.csv", index=False)
    selected_rule = str(rule_calibration.iloc[0]["rule"])
    selected_rule_eligible = bool(rule_calibration.iloc[0]["selection_eligible"])
    confirm_rule_mask = RULES[selected_rule][1](confirmation)
    simple_pooled = rule_metrics(confirmation, confirm_rule_mask)
    simple_folds: list[dict[str, Any]] = []
    for fold, group in confirmation.groupby("fold"):
        metrics = rule_metrics(group, RULES[selected_rule][1](group))
        metrics["fold"] = int(fold)
        metrics["precision_improved"] = bool(metrics["selected_precision"] > metrics["base_precision"])
        simple_folds.append(metrics)
    simple_fold_frame = pd.DataFrame(simple_folds)
    simple_fold_frame.to_csv(REPORT / "sequential_simple_rule_confirmation_folds.csv", index=False)

    path_scored, path_folds = PRIMARY.score_candidate(
        rows,
        folds,
        family=family,
        test_folds=(3, 4, 5, 6),
        features=SEQ.PATH_FEATURES,
    )
    path_ablation = {
        "path_only": PRIMARY.pooled_recognition(path_scored),
        "path_only_fold_metrics": path_folds.to_dict(orient="records"),
        "full_sequence": PRIMARY.pooled_recognition(confirmation),
    }

    clusters = event_clusters(confirmation)
    clusters.to_csv(REPORT / "sequential_event_clusters.csv", index=False)
    positive_events = clusters["positive_event"].astype(bool)
    selected_events = clusters[clusters["sequence_selected"]]
    event_metrics = {
        "clusters": int(len(clusters)),
        "positive_events": int(positive_events.sum()),
        "base_event_precision": float(positive_events.mean()),
        "selected_clusters": int(len(selected_events)),
        "selected_positive_events": int(selected_events["positive_event"].sum()),
        "selected_event_precision": float(selected_events["positive_event"].mean()) if len(selected_events) else 0.0,
        "positive_event_retention": float(selected_events["positive_event"].sum() / positive_events.sum()) if positive_events.sum() else 0.0,
    }
    leave_one = leave_one_symbol_out(confirmation)
    leave_one.to_csv(REPORT / "sequential_leave_one_symbol_out.csv", index=False)
    leave_summary = {
        "symbols": int(len(leave_one)),
        "ap_delta_min": float(leave_one["ap_delta"].min()),
        "ap_delta_positive_share": float((leave_one["ap_delta"] > 0).mean()),
        "precision_delta_min": float(leave_one["precision_delta"].min()),
        "precision_delta_positive_share": float((leave_one["precision_delta"] > 0).mean()),
    }
    null = permutation_null(confirmation)
    simple_majority = float(simple_fold_frame["precision_improved"].mean()) if not simple_fold_frame.empty else 0.0
    output = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "status": "secondary_historical_robustness_not_new_oos",
        "primary_checkpoint_hours": hours,
        "primary_model_family": family,
        "event_deduplication": event_metrics,
        "path_feature_ablation": path_ablation,
        "permutation_null": null,
        "leave_one_symbol_out": leave_summary,
        "selected_simple_rule": selected_rule,
        "selected_simple_rule_description": RULES[selected_rule][0],
        "simple_rule_calibration_eligible": selected_rule_eligible,
        "simple_rule_confirmation": simple_pooled,
        "simple_rule_fold_improvement_share": simple_majority,
        "simple_rule_status": "forward_challenger_only",
        "primary_sequence_status": "frozen_challenger_not_promoted",
    }
    (REPORT / "sequential_robustness_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
