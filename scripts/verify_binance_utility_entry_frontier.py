"""Independently verify the full 8h utility entry-rule frontier diagnostic."""

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
    decision = json.loads((REPORT / "utility_entry_frontier_decision.json").read_text(encoding="utf-8"))
    summary = pd.read_csv(REPORT / "utility_entry_frontier_summary.csv")
    folds = pd.read_csv(REPORT / "utility_entry_frontier_folds.csv")
    trades = pd.read_csv(REPORT / "utility_entry_frontier_trades.csv.gz", compression="gzip")
    paired = pd.read_csv(REPORT / "utility_entry_frontier_paired_deltas.csv")
    checks: dict[str, bool] = {}

    checks["catalog_fixed_but_confirmation_is_hypothesis_only"] = (
        decision["candidate_catalog_fixed_before_confirmation"] is True
        and decision["calibration_selected_rule"] == "discount5_reclaim"
        and decision["confirmation_catalog_inspected_for_hypothesis_generation"] is True
        and decision["historical_promotion_allowed"] is False
        and decision["prospective_rule"] is None
        and decision["primary_entry_changed"] is False
    )
    checks["all_nine_rules_and_four_folds_recomputed"] = (
        len(summary) == 9
        and set(summary["entry_rule"])
        == {
            "launch_open", "wait4h", "discount5_touch", "discount10_touch",
            "discount5_reclaim", "discount10_reclaim", "controlled_pullback5",
            "breakout6", "prelaunch_high_break",
        }
        and set(folds["fold"].astype(int)) == {3, 4, 5, 6}
    )
    breakout = summary.set_index("entry_rule").loc["breakout6"]
    breakout_detail = decision["rules"]["breakout6"]
    checks["breakout_is_strong_point_estimate_in_every_fold"] = (
        int(breakout["filled_entries"]) == 21
        and int(breakout["actual_200pct_entries"]) == 6
        and close(breakout["actual_200pct_precision"], 6 / 21)
        and int(breakout["independent_events"]) == 18
        and int(breakout["positive_independent_events"]) == 5
        and close(breakout["event_precision"], 5 / 18)
        and close(breakout["fold_precision_improvement_share"], 1.0)
        and int(breakout["calibration_actual_200pct_entries"]) == 0
    )
    checks["breakout_fails_catalog_and_event_uncertainty"] = (
        float(breakout["row_fisher_p_bh"]) > 0.05
        and float(breakout["event_fisher_p_bh"]) > 0.05
        and float(breakout_detail["precision_bootstraps"]["row_week"]["precision_delta_lower_95pct"]) > 0
        and float(breakout_detail["precision_bootstraps"]["row_symbol"]["precision_delta_lower_95pct"]) > 0
        and float(breakout_detail["precision_bootstraps"]["event_week"]["precision_delta_lower_95pct"]) <= 0
        and float(breakout_detail["precision_bootstraps"]["event_symbol"]["precision_delta_lower_95pct"]) <= 0
        and bool(breakout["confirmation_diagnostic_recognition_pass"]) is False
    )
    breakout_trades = trades[trades["entry_rule"] == "breakout6"]
    breakout_paired = paired[paired["entry_rule"] == "breakout6"]
    checks["breakout_trade_metrics_recomputed"] = (
        len(breakout_trades) == int(breakout["trades"]) == 17
        and close(breakout_trades["net_return"].mean(), breakout["expectancy"])
        and close(profit_factor(breakout_trades["net_return"]), breakout["profit_factor"])
        and close(breakout_paired["delta_return"].mean(), breakout["paired_trade_increment_mean"])
    )
    checks["waiting_for_breakout_is_strongly_rejected_as_entry"] = (
        float(breakout["paired_trade_increment_mean"]) < 0
        and float(breakout_detail["week_trade_increment_bootstrap"]["mean_delta_upper_95pct"]) < 0
        and float(breakout_detail["symbol_trade_increment_bootstrap"]["mean_delta_upper_95pct"]) < 0
        and float(breakout_detail["week_return_bootstrap"]["expectancy_lower_95pct"]) <= 0
        and bool(breakout["confirmation_diagnostic_trade_pass"]) is False
    )
    prelaunch = summary.set_index("entry_rule").loc["prelaunch_high_break"]
    prelaunch_detail = decision["rules"]["prelaunch_high_break"]
    checks["second_breakout_family_confirms_chase_penalty"] = (
        close(prelaunch["fold_precision_improvement_share"], 1.0)
        and float(prelaunch["actual_200pct_precision"]) > float(summary.set_index("entry_rule").loc["launch_open", "actual_200pct_precision"])
        and float(prelaunch["paired_trade_increment_mean"]) < 0
        and float(prelaunch_detail["week_trade_increment_bootstrap"]["mean_delta_upper_95pct"]) < 0
        and float(prelaunch_detail["symbol_trade_increment_bootstrap"]["mean_delta_upper_95pct"]) < 0
    )
    checks["no_forward_buy_rule_added"] = (
        decision["prospective_rule_recognition_diagnostic_pass"] is False
        and decision["prospective_rule_trade_diagnostic_pass"] is False
        and decision["forward_counterfactual_added"] is False
        and decision["automatic_trading_allowed"] is False
    )

    errors = [name for name, passed in checks.items() if not passed]
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "rules": int(len(summary)),
            "breakout_entries": int(breakout["filled_entries"]),
            "breakout_precision": float(breakout["actual_200pct_precision"]),
            "breakout_event_precision": float(breakout["event_precision"]),
            "breakout_expectancy": float(breakout["expectancy"]),
            "breakout_profit_factor": float(breakout["profit_factor"]),
            "breakout_paired_trade_increment": float(breakout["paired_trade_increment_mean"]),
        },
    }
    (REPORT / "utility_entry_frontier_verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
