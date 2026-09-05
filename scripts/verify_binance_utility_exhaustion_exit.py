"""Independently verify the profit-exhaustion exit challenger."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


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
    decision = json.loads((REPORT / "utility_exhaustion_exit_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(REPORT / "utility_exhaustion_exit_calibration.csv")
    trades = pd.read_csv(REPORT / "utility_exhaustion_exit_trades.csv.gz", compression="gzip")
    paired = pd.read_csv(REPORT / "utility_exhaustion_exit_paired_deltas.csv")
    folds = pd.read_csv(REPORT / "utility_exhaustion_exit_folds.csv")
    errors: list[str] = []
    checks: dict[str, bool] = {}

    checks["selected_from_calibration"] = (
        str(calibration.iloc[0]["policy"]) == decision["selected_policy"] == "half150_trail35_wick"
        and bool(calibration.iloc[0]["selection_eligible"])
    )
    checks["candidate_family_frozen"] = set(decision["candidate_policies"]) == {
        "baseline_half100_trail25",
        "half150_trail30",
        "half150_trail35",
        "half150_trail35_wick",
        "half150_trail35_demandfade",
        "half150_trail35_either",
    }
    default = trades[np.isclose(trades["slippage_bps_each_side"], 5.0)]
    summary = decision["selected_confirmation_summary"]
    checks["confirmation_metrics"] = (
        len(default) == int(summary["trades"])
        and close(default["net_return"].mean(), summary["expectancy"])
        and close(profit_factor(default["net_return"]), summary["profit_factor"])
        and close(paired["delta_return"].mean(), decision["paired_increment_mean"])
    )
    checks["exhaustion_trigger_count"] = (
        int((default["exit_reason"] == "blowoff_exhaustion").sum()) == 10
        and int(decision["selected_exit_reason_counts"]["blowoff_exhaustion"]) == 10
        and int((default["exit_reason"] == "demand_fade").sum()) == 0
    )
    checks["all_confirmation_folds_profitable"] = (
        len(folds) == 4 and bool((folds["profit_factor"] > 1).all())
        and close(decision["fold_majority_profit_factor_gt_1"], 1.0)
    )
    baseline = decision["baseline_confirmation_summary"]
    checks["point_estimate_improves"] = (
        float(summary["expectancy"]) > float(baseline["expectancy"])
        and float(summary["profit_factor"]) > float(baseline["profit_factor"])
        and float(summary["total_return_on_initial_equity"]) > float(baseline["total_return_on_initial_equity"])
        and float(decision["paired_increment_mean"]) > 0
    )
    checks["absolute_return_uncertainty_passes"] = (
        float(decision["week_return_bootstrap"]["expectancy_lower_95pct"]) > 0
        and float(decision["symbol_return_bootstrap"]["expectancy_lower_95pct"]) > 0
    )
    checks["incremental_uncertainty_fails"] = (
        float(decision["week_increment_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and float(decision["symbol_increment_bootstrap"]["mean_delta_lower_95pct"]) <= 0
        and decision["incremental_exit_gate_pass"] is False
        and decision["classification"] == "retain_frozen_exit"
    )
    checks["conservative_clock_implemented_and_tested"] = (
        decision["signal_clock"] == "completed 4h exhaustion bar; exit at following 4h open"
        and "test_blowoff_exhaustion_waits_for_reversal_bar_close_then_next_open"
        in (ROOT / "tests" / "test_binance_sequential_validation.py").read_text(encoding="utf-8")
    )
    labeler_text = (ROOT / "scripts" / "binance_forward_strategy_labeler.py").read_text(encoding="utf-8")
    checks["forward_counterfactual_recorded_not_promoted"] = (
        "hard25_be30_half150_trail35_wick" in labeler_text
        and "prospective_profit_exhaustion_counterfactual_not_promoted" in labeler_text
        and decision["automatic_trading_allowed"] is False
    )

    for name, passed in checks.items():
        if not passed:
            errors.append(name)
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "trades": int(len(default)),
            "expectancy": float(default["net_return"].mean()),
            "profit_factor": float(profit_factor(default["net_return"])),
            "paired_increment": float(paired["delta_return"].mean()),
            "blowoff_exits": int((default["exit_reason"] == "blowoff_exhaustion").sum()),
        },
    }
    (REPORT / "utility_exhaustion_exit_verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
