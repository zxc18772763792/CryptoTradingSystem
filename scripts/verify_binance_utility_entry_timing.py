"""Independently verify execution timing after the frozen 8h utility score."""

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
    decision = json.loads((REPORT / "utility_entry_timing_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(REPORT / "utility_entry_timing_calibration.csv")
    candidates = pd.read_csv(REPORT / "utility_entry_timing_candidates.csv.gz", compression="gzip")
    trades = pd.read_csv(REPORT / "utility_entry_timing_confirmation_trades.csv.gz", compression="gzip")
    folds = pd.read_csv(REPORT / "utility_entry_timing_confirmation_folds.csv")
    paired_trade = pd.read_csv(REPORT / "utility_entry_timing_trade_paired_deltas.csv")
    paired_recognition = pd.read_csv(REPORT / "utility_entry_timing_recognition_paired.csv")
    checks: dict[str, bool] = {}

    checks["selected_only_from_calibration"] = (
        decision["selected_entry_rule"] == "discount5_reclaim"
        and str(calibration[calibration["selection_eligible"]].iloc[0]["entry_rule"]) == "discount5_reclaim"
        and decision["calibration_eligible"] is True
    )
    checks["candidate_family_and_clock_frozen"] = (
        set(decision["candidate_entry_rules"])
        == {
            "launch_open", "wait4h", "discount5_touch", "discount10_touch",
            "discount5_reclaim", "discount10_reclaim", "controlled_pullback5",
            "breakout6", "prelaunch_high_break",
        }
        and int(decision["trigger_window_hours"]) == 24
        and decision["trigger_clock"] == "completed 4h trigger bar; enter at following 4h open"
        and "test_utility_entry_reclaim_respects_24h_trigger_window"
        in (ROOT / "tests" / "test_binance_sequential_validation.py").read_text(encoding="utf-8")
    )
    baseline = decision["confirmation_recognition_baseline_all"]
    selected = decision["confirmation_recognition_selected"]
    checks["signal_recognition_enrichment_with_retention"] = (
        int(baseline["filled_entries"]) == 88
        and int(baseline["actual_200pct_entries"]) == 10
        and int(selected["filled_entries"]) == 54
        and int(selected["actual_200pct_entries"]) == 8
        and close(selected["fill_rate"], 54 / 88)
        and close(selected["actual_200pct_precision"], 8 / 54)
        and close(selected["positive_retention"], 0.80)
        and float(selected["actual_200pct_precision"]) > float(baseline["actual_200pct_precision"])
    )
    checks["independent_event_enrichment_retains_all_positive_events"] = (
        int(baseline["independent_events"]) == 67
        and int(baseline["positive_independent_events"]) == 5
        and int(selected["independent_events"]) == 45
        and int(selected["positive_independent_events"]) == 5
        and close(baseline["event_precision"], 5 / 67)
        and close(selected["event_precision"], 5 / 45)
        and close(selected["positive_event_retention"], 1.0)
    )
    boot = decision["filter_precision_bootstraps"]
    checks["event_increment_passes_but_row_increment_fails"] = (
        float(boot["row_week"]["precision_delta_lower_95pct"]) <= 0
        and float(boot["row_symbol"]["precision_delta_lower_95pct"]) <= 0
        and float(boot["event_week"]["precision_delta_lower_95pct"]) > 0
        and float(boot["event_symbol"]["precision_delta_lower_95pct"]) > 0
        and decision["recognition_filter_gate_pass"] is False
    )
    checks["entry_price_does_not_manufacture_target_labels"] = (
        len(paired_recognition) == 54
        and int(paired_recognition["delta_return"].abs().sum()) == 0
        and close(decision["recognition_week_increment_bootstrap"]["mean_delta_lower_95pct"], 0.0)
        and close(decision["recognition_symbol_increment_bootstrap"]["mean_delta_lower_95pct"], 0.0)
    )
    default = trades[np.isclose(trades["slippage_bps_each_side"], 5.0)]
    summary = decision["selected_strategy_summary"]
    checks["selected_trade_metrics_recomputed"] = (
        len(default) == int(summary["trades"]) == 44
        and close(default["net_return"].mean(), summary["expectancy"])
        and close(profit_factor(default["net_return"]), summary["profit_factor"])
        and close(paired_trade["delta_return"].mean(), decision["paired_trade_increment_mean"])
    )
    checks["trade_increment_and_absolute_uncertainty_fail"] = (
        len(folds) == 4
        and close(decision["fold_majority_profit_factor_gt_1"], 0.75)
        and float(decision["week_return_bootstrap"]["expectancy_lower_95pct"]) <= 0
        and float(decision["symbol_return_bootstrap"]["expectancy_lower_95pct"]) <= 0
        and float(decision["week_trade_increment_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and float(decision["symbol_trade_increment_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and decision["entry_timing_gate_pass"] is False
        and decision["classification"] == "retain_score_open_entry"
    )
    confirmation = candidates[(candidates["period"] == "confirmation") & (candidates["entry_rule"] == "discount5_reclaim")]
    checks["candidate_rows_match_decision"] = (
        len(confirmation) == int(selected["filled_entries"])
        and close(confirmation["entry_delay_hours_after_launch"].median(), selected["median_delay_hours"])
        and close(confirmation["entry_return_vs_decision_open"].median(), selected["median_entry_return_vs_score_open"])
    )
    labeler_text = (ROOT / "scripts" / "binance_forward_strategy_labeler.py").read_text(encoding="utf-8")
    utility_labeler_text = (ROOT / "scripts" / "binance_continuation_utility_labeler.py").read_text(encoding="utf-8")
    checks["forward_counterfactual_is_read_only_and_not_promoted"] = (
        "discount5_reclaim_24h_next_open" in labeler_text
        and "prospective_utility_entry_filter_not_promoted" in labeler_text
        and "include_utility_entry_counterfactual=True" in utility_labeler_text
        and decision["automatic_trading_allowed"] is False
    )

    errors = [name for name, passed in checks.items() if not passed]
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "calibration_rules": int(len(calibration)),
            "confirmation_filled": int(len(confirmation)),
            "selected_precision": float(selected["actual_200pct_precision"]),
            "selected_event_precision": float(selected["event_precision"]),
            "trades": int(len(default)),
            "expectancy": float(default["net_return"].mean()),
            "profit_factor": float(profit_factor(default["net_return"])),
            "paired_trade_increment": float(paired_trade["delta_return"].mean()),
        },
    }
    (REPORT / "utility_entry_timing_verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
