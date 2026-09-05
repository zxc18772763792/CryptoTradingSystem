"""Independently recompute the extreme-intensity and utility-exit frontier."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def profit_factor(values: pd.Series) -> float:
    data = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(data[data > 0].sum())
    losses = float(-data[data < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def close(left: float, right: float, tolerance: float = 1e-10) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=tolerance, atol=tolerance))


def main() -> None:
    intensity = json.loads((REPORT / "extreme_intensity_decision.json").read_text(encoding="utf-8"))
    intensity_cal = pd.read_csv(REPORT / "extreme_intensity_calibration.csv")
    intensity_scored = pd.read_csv(REPORT / "extreme_intensity_confirmation_scored.csv.gz", compression="gzip")
    intensity_events = pd.read_csv(REPORT / "extreme_intensity_event_clusters.csv")
    failure = json.loads((REPORT / "utility_failure_exit_decision.json").read_text(encoding="utf-8"))
    failure_cal = pd.read_csv(REPORT / "utility_failure_exit_calibration.csv")
    tail = json.loads((REPORT / "utility_tail_exit_decision.json").read_text(encoding="utf-8"))
    tail_cal = pd.read_csv(REPORT / "utility_tail_exit_calibration.csv")
    tail_trades = pd.read_csv(REPORT / "utility_tail_exit_trades.csv.gz", compression="gzip")
    tail_paired = pd.read_csv(REPORT / "utility_tail_exit_paired_deltas.csv")

    errors: list[str] = []
    checks: dict[str, bool] = {}
    top_intensity = intensity_cal.iloc[0]
    checks["intensity_selected_from_calibration"] = (
        str(top_intensity["family"]) == intensity["selected_family"]
        and int(top_intensity["checkpoint_hours"]) == int(intensity["selected_checkpoint_hours"])
        and close(top_intensity["selection_quantile"], intensity["selected_quantile"])
    )
    checks["intensity_candidate_grid_frozen"] = len(intensity_cal) == 48 and int(intensity["candidate_count"]) == 48
    labels = intensity_scored["late_target200"].astype(int)
    selected = intensity_scored[intensity_scored["selected"].astype(bool)]
    confirmation = intensity["confirmation"]
    checks["intensity_confirmation_counts"] = (
        len(intensity_scored) == int(confirmation["rows"])
        and int(labels.sum()) == int(confirmation["positives"])
        and len(selected) == int(confirmation["selected_rows"])
        and int(selected["late_target200"].sum()) == int(confirmation["selected_positives"])
    )
    checks["intensity_confirmation_metrics"] = (
        close(selected["late_target200"].mean(), confirmation["selected_precision"])
        and close(average_precision_score(labels, intensity_scored["intensity_score"]), confirmation["intensity_average_precision"])
        and close(average_precision_score(labels, intensity_scored["price_model_score"]), confirmation["price_average_precision"])
    )
    selected_events = intensity_events[intensity_events["selected"].astype(bool)]
    event_metrics = intensity["event_metrics"]
    checks["intensity_event_metrics"] = (
        len(intensity_events) == int(event_metrics["events"])
        and len(selected_events) == int(event_metrics["selected_events"])
        and int(selected_events["positive"].sum()) == int(event_metrics["selected_positive_events"])
    )
    checks["intensity_not_promoted_without_uncertainty"] = (
        intensity["ranking_gate_pass"] is False
        and intensity["classification"] == "watchlist_only"
        and (
            float(intensity["week_bootstrap"]["precision_delta_lower_95pct"]) <= 0
            or float(intensity["week_bootstrap"]["ap_delta_lower_95pct"]) <= 0
            or float(intensity["symbol_bootstrap"]["precision_delta_lower_95pct"]) <= 0
            or float(intensity["symbol_bootstrap"]["ap_delta_lower_95pct"]) <= 0
        )
    )

    checks["failure_exit_selected_from_calibration"] = str(failure_cal.iloc[0]["policy"]) == failure["selected_policy"] == "baseline"
    checks["failure_exit_retains_frozen"] = (
        failure["incremental_exit_gate_pass"] is False
        and failure["classification"] == "retain_frozen_exit"
        and all(float(row) <= 0 for row in failure_cal.loc[failure_cal["policy"] != "baseline", "paired_delta_mean"])
    )

    top_tail = tail_cal.iloc[0]
    checks["tail_exit_selected_from_calibration"] = (
        str(top_tail["policy"]) == tail["selected_policy"] == "half150_trail30"
        and bool(top_tail["selection_eligible"])
    )
    default_tail = tail_trades[np.isclose(tail_trades["slippage_bps_each_side"], 5.0)]
    tail_summary = tail["selected_confirmation_summary"]
    checks["tail_exit_confirmation_metrics"] = (
        len(default_tail) == int(tail_summary["trades"])
        and close(default_tail["net_return"].mean(), tail_summary["expectancy"])
        and close(profit_factor(default_tail["net_return"]), tail_summary["profit_factor"])
        and close(tail_paired["delta_return"].mean(), tail["paired_increment_mean"])
    )
    checks["tail_point_estimate_improves"] = (
        float(tail_summary["expectancy"]) > float(tail["baseline_confirmation_summary"]["expectancy"])
        and float(tail_summary["profit_factor"]) > float(tail["baseline_confirmation_summary"]["profit_factor"])
    )
    checks["tail_not_promoted_without_increment_ci"] = (
        tail["incremental_exit_gate_pass"] is False
        and tail["classification"] == "retain_frozen_exit"
        and float(tail["week_increment_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and float(tail["symbol_increment_bootstrap"]["mean_delta_lower_95pct"]) <= 0
    )
    labeler_text = (ROOT / "scripts" / "binance_forward_strategy_labeler.py").read_text(encoding="utf-8")
    checks["tail_counterfactual_is_forward_recorded_not_primary"] = (
        "hard25_be30_half150_trail30" in labeler_text
        and "prospective_right_tail_counterfactual_not_promoted" in labeler_text
    )
    checks["research_only"] = all(
        item.get("automatic_trading_allowed") is False for item in (intensity, failure, tail)
    )

    for name, passed in checks.items():
        if not passed:
            errors.append(name)
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "intensity_selected_precision": float(selected["late_target200"].mean()),
            "intensity_average_precision": float(average_precision_score(labels, intensity_scored["intensity_score"])),
            "tail_trades": int(len(default_tail)),
            "tail_expectancy": float(default_tail["net_return"].mean()),
            "tail_profit_factor": float(profit_factor(default_tail["net_return"])),
            "tail_paired_increment": float(tail_paired["delta_return"].mean()),
        },
    }
    (REPORT / "intensity_exit_frontier_verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
