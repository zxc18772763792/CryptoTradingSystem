"""Independently verify the 8h-entry / 20h-intensity routing study."""

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


def close(left: float, right: float) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=1e-10, atol=1e-10))


def main() -> None:
    decision = json.loads((REPORT / "utility_intensity_router_decision.json").read_text(encoding="utf-8"))
    recognition_folds = pd.read_csv(REPORT / "utility_intensity_router_recognition_folds.csv")
    trades = pd.read_csv(REPORT / "utility_intensity_router_trades.csv.gz", compression="gzip")
    paired = pd.read_csv(REPORT / "utility_intensity_router_paired_deltas.csv")
    events = pd.read_csv(REPORT / "utility_intensity_router_event_clusters.csv")
    errors: list[str] = []
    checks: dict[str, bool] = {}

    recognition = decision["confirmation_recognition"]
    checks["frozen_entry_and_route_spec"] = (
        decision["entry_model"] == "continuation-utility-8h-v1-frozen-2026-07-19"
        and decision["route_model"] == "ordinal_intensity_20h_q80_secondary"
        and decision["selection_reused_without_retuning"] is True
    )
    checks["recognition_counts"] = (
        int(recognition_folds["rows"].sum()) == int(recognition["rows"])
        and int(recognition_folds["positives"].sum()) == int(recognition["positives_from_entry"])
        and int(recognition_folds["high_rows"].sum()) == int(recognition["high_rows"])
        and int(recognition_folds["high_positives"].sum()) == int(recognition["high_positives_from_entry"])
    )
    labels = []
    intensity_scores = []
    utility_scores = []
    # Fold-level AP values cannot be pooled back to row AP, so verify their
    # joint-improvement share and aggregate precision directly.
    joint = (
        (recognition_folds["high_precision"] > recognition_folds["base_precision"])
        & (recognition_folds["intensity_average_precision"] > recognition_folds["utility_average_precision"])
    )
    checks["recognition_metrics"] = (
        close(recognition_folds["high_positives"].sum() / recognition_folds["high_rows"].sum(), recognition["high_precision_from_entry"])
        and close(recognition_folds["high_positives"].sum() / recognition_folds["positives"].sum(), recognition["positive_retention_from_entry"])
        and close(joint.mean(), recognition["fold_joint_improvement_share"])
    )
    selected_events = events[events["selected"].astype(bool)]
    checks["event_metrics"] = (
        len(events) == int(decision["event_metrics"]["events"])
        and len(selected_events) == int(decision["event_metrics"]["selected_events"])
        and int(selected_events["positive"].sum()) == int(decision["event_metrics"]["selected_positive_events"])
    )
    checks["recognition_gate_failed"] = (
        decision["recognition_gate_pass"] is False
        and float(recognition["fold_joint_improvement_share"]) <= 0.50
        and float(decision["week_recognition_bootstrap"]["precision_delta_lower_95pct"]) <= 0
        and float(decision["symbol_recognition_bootstrap"]["precision_delta_lower_95pct"]) <= 0
    )

    default = trades[np.isclose(trades["slippage_bps_each_side"], 5.0)]
    routed = decision["routed_confirmation_summary"]
    checks["routed_trade_metrics"] = (
        len(default) == int(routed["trades"])
        and close(default["net_return"].mean(), routed["expectancy"])
        and close(profit_factor(default["net_return"]), routed["profit_factor"])
        and close(paired["delta_return"].mean(), decision["paired_increment_mean"])
    )
    checks["calibration_rejected_router"] = decision["calibration"]["trade_selection_eligible"] is False
    checks["router_is_strongly_harmful"] = (
        float(routed["expectancy"]) < 0
        and float(routed["profit_factor"]) < 1
        and float(decision["paired_increment_mean"]) < 0
        and float(decision["week_increment_bootstrap"]["mean_delta_upper_95pct"]) < 0
        and float(decision["symbol_increment_bootstrap"]["mean_delta_upper_95pct"]) < 0
    )
    checks["router_not_promoted"] = (
        decision["trade_gate_pass"] is False
        and decision["classification"] == "retain_8h_entry_and_frozen_exit"
        and decision["automatic_trading_allowed"] is False
    )
    core_text = (ROOT / "core" / "research" / "binance_entry_exit_validation.py").read_text(encoding="utf-8")
    test_text = (ROOT / "tests" / "test_binance_sequential_validation.py").read_text(encoding="utf-8")
    checks["forced_exit_clock_implemented_and_tested"] = (
        "forced_exit_after_hours" in core_text
        and "test_model_routed_exit_fills_at_decision_open_before_same_bar_extremes" in test_text
    )

    for name, passed in checks.items():
        if not passed:
            errors.append(name)
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "high_precision": float(recognition_folds["high_positives"].sum() / recognition_folds["high_rows"].sum()),
            "routed_trades": int(len(default)),
            "routed_expectancy": float(default["net_return"].mean()),
            "routed_profit_factor": float(profit_factor(default["net_return"])),
            "paired_increment": float(paired["delta_return"].mean()),
        },
    }
    (REPORT / "utility_intensity_router_verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
