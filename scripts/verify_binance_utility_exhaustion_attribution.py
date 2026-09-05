"""Independently verify static-tail versus wick-exit factorial attribution."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def close(left: float, right: float) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=1e-10, atol=1e-10))


def profit_factor(values: pd.Series) -> float:
    data = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(data[data > 0].sum())
    losses = float(-data[data < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def main() -> None:
    decision = json.loads((REPORT / "utility_exhaustion_attribution_decision.json").read_text(encoding="utf-8"))
    summaries = pd.read_csv(REPORT / "utility_exhaustion_attribution_summary.csv")
    trades = pd.read_csv(REPORT / "utility_exhaustion_attribution_trades.csv.gz", compression="gzip")
    folds = pd.read_csv(REPORT / "utility_exhaustion_attribution_folds.csv")
    paired = pd.read_csv(REPORT / "utility_exhaustion_attribution_paired_deltas.csv")
    events = pd.read_csv(REPORT / "utility_exhaustion_attribution_event_deltas.csv")
    checks: dict[str, bool] = {}

    checks["fixed_factorial_no_reselection"] = (
        decision["policy_selection_performed"] is False
        and decision["fixed_policies"]
        == [
            "baseline_half100_trail25",
            "half150_trail30",
            "half150_trail35",
            "half150_trail35_wick",
        ]
        and decision["evidence_class"] == "secondary_historical_fixed_policy_factorial_attribution"
    )
    checks["posthoc_policy_explicitly_quarantined"] = (
        decision["posthoc_exploratory_policy"] == "half150_trail30_wick_posthoc"
        and decision["posthoc_hypothesis"]["eligible_for_historical_promotion"] is False
        and decision["posthoc_hypothesis"]["prospective_label_only"] is True
        and decision["primary_exit_changed"] is False
        and decision["automatic_trading_allowed"] is False
    )
    default_summaries = summaries[np.isclose(summaries["slippage_bps_each_side"], 5.0)]
    default_trades = trades[np.isclose(trades["slippage_bps_each_side"], 5.0)]
    metric_checks: list[bool] = []
    for policy, expected in decision["policy_default_cost_summaries"].items():
        policy_trades = default_trades[default_trades["policy"] == policy]
        policy_summary = default_summaries[default_summaries["policy"] == policy]
        metric_checks.append(
            len(policy_summary) == 1
            and len(policy_trades) == int(expected["trades"])
            and close(policy_trades["net_return"].mean(), expected["expectancy"])
            and close(profit_factor(policy_trades["net_return"]), expected["profit_factor"])
        )
    checks["policy_metrics_recomputed"] = bool(metric_checks and all(metric_checks))

    contrast_checks: list[bool] = []
    for contrast, expected in decision["contrasts"].items():
        rows = paired[paired["contrast"] == contrast]
        event_rows = events[events["contrast"] == contrast]
        contrast_checks.append(
            len(rows) == int(expected["paired_rows"])
            and len(event_rows) == int(expected["independent_events"])
            and close(rows["delta_return"].mean(), expected["mean_delta"])
            and close(event_rows["delta_return"].mean(), expected["event_mean_delta"])
        )
    checks["row_and_event_contrasts_recomputed"] = bool(contrast_checks and all(contrast_checks))

    trail_width = decision["contrasts"]["trail35_minus_trail30"]
    checks["wider_35pct_trail_uniformly_harms_confirmation"] = (
        int(trail_width["changed_rows"]) == 12
        and int(trail_width["positive_changed_rows"]) == 0
        and int(trail_width["negative_changed_rows"]) == 12
        and close(trail_width["positive_fold_share"], 0.0)
        and float(trail_width["week_bootstrap"]["mean_delta_upper_95pct"]) < 0
        and float(trail_width["symbol_bootstrap"]["mean_delta_upper_95pct"]) < 0
    )
    pure = decision["contrasts"]["wick_minus_tail35"]
    checks["pure_wick_point_gain_but_not_identified"] = (
        int(pure["changed_rows"]) == 13
        and int(pure["positive_changed_rows"]) == 9
        and int(pure["negative_changed_rows"]) == 4
        and float(pure["mean_delta"]) > 0
        and float(pure["event_mean_delta"]) > 0
        and float(pure["week_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and float(pure["symbol_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and float(pure["event_week_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and float(pure["event_symbol_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and decision["pure_wick_increment_gate_pass"] is False
        and decision["classification"] == "wick_timing_not_independently_identified"
    )
    attribution = decision["attribution"]
    checks["combined_increment_attribution_identity"] = (
        close(
            attribution["combined_wick_policy_increment"],
            float(attribution["static_tail35_increment"]) + float(attribution["pure_wick_timing_increment"]),
        )
        and close(attribution["additive_identity_error"], 0.0)
        and float(attribution["wick_share_of_combined_point_increment"]) > 0.90
    )
    posthoc = decision["posthoc_hypothesis"]
    versus_current = posthoc["increment_vs_current_wick35"]
    versus_baseline = posthoc["increment_vs_baseline"]
    posthoc_folds = folds[folds["policy"] == decision["posthoc_exploratory_policy"]]
    checks["posthoc_tail30_wick_is_forward_hypothesis_only"] = (
        len(posthoc_folds) == 4
        and bool((posthoc_folds["profit_factor"] > 1).all())
        and int(versus_current["changed_rows"]) == 6
        and int(versus_current["positive_changed_rows"]) == 6
        and int(versus_current["negative_changed_rows"]) == 0
        and bool(versus_current["cluster_lower_bounds_positive"])
        and bool(versus_current["event_cluster_lower_bounds_positive"])
        and not bool(versus_baseline["cluster_lower_bounds_positive"])
        and not bool(versus_baseline["event_cluster_lower_bounds_positive"])
    )
    labeler_text = (ROOT / "scripts" / "binance_forward_strategy_labeler.py").read_text(encoding="utf-8")
    checks["posthoc_rule_recorded_prospectively_without_promotion"] = (
        "hard25_be30_half150_trail30_wick_posthoc" in labeler_text
        and "prospective_posthoc_factorial_counterfactual_not_promoted" in labeler_text
        and decision["forward_counterfactual_recommendation"] == "half150_trail30_wick_posthoc"
    )

    errors = [name for name, passed in checks.items() if not passed]
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "policies": int(default_summaries["policy"].nunique()),
            "contrasts": int(paired["contrast"].nunique()),
            "paired_rows_per_contrast": sorted(paired.groupby("contrast").size().unique().tolist()),
            "events_per_contrast": sorted(events.groupby("contrast").size().unique().tolist()),
            "pure_wick_row_increment": float(pure["mean_delta"]),
            "pure_wick_event_increment": float(pure["event_mean_delta"]),
            "posthoc_expectancy": float(posthoc["default_cost_summary"]["expectancy"]),
            "posthoc_profit_factor": float(posthoc["default_cost_summary"]["profit_factor"]),
        },
    }
    (REPORT / "utility_exhaustion_attribution_verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
