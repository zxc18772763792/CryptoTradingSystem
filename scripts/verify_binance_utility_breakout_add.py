"""Independently verify the risk-neutral breakout add-on study."""

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
    decision = json.loads((REPORT / "utility_breakout_add_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(REPORT / "utility_breakout_add_calibration.csv")
    summary = pd.read_csv(REPORT / "utility_breakout_add_summary.csv")
    trades = pd.read_csv(REPORT / "utility_breakout_add_trades.csv.gz", compression="gzip")
    folds = pd.read_csv(REPORT / "utility_breakout_add_folds.csv")
    paired = pd.read_csv(REPORT / "utility_breakout_add_paired_deltas.csv")
    hold = json.loads((REPORT / "utility_breakout_hold_router_decision.json").read_text(encoding="utf-8"))
    checks: dict[str, bool] = {}

    checks["catalog_and_time_isolation_are_fixed"] = (
        decision["allocation_catalog"] == [0.0, 0.1, 0.25, 0.5]
        and int(decision["calibration_horizon_days"]) == 14
        and int(decision["confirmation_horizon_days"]) == 30
        and int(decision["calibration_signals"]) == 42
        and int(decision["calibration_breakout_add_opportunities"]) == 11
        and int(decision["confirmation_signals"]) == 88
        and int(decision["confirmation_breakout_add_opportunities"]) == 21
    )
    checks["risk_is_reserved_not_increased"] = (
        "planned position risk fixed" in decision["risk_constraint"]
        and bool((summary["mean_deployed_fraction"] <= 1.0 + 1e-12).all())
        and bool((calibration["mean_deployed_fraction"] <= 1.0 + 1e-12).all())
        and set(summary["breakout_adds"].astype(int)) == {0, 21}
        and set(calibration["breakout_adds"].astype(int)) == {0, 11}
    )
    calibration_add = calibration[calibration["breakout_add_fraction"] > 0]
    checks["calibration_rejects_every_add_fraction"] = (
        len(calibration_add) == 3
        and bool((calibration_add["paired_increment_mean"] < 0).all())
        and bool(np.isclose(calibration_add["positive_calibration_split_share"], 0.0).all())
        and not bool(calibration_add["selection_eligible"].astype(bool).any())
        and close(decision["selected_breakout_add_fraction"], 0.0)
        and decision["selected_calibration_eligible"] is False
    )

    indexed = summary.set_index("breakout_add_fraction")
    hold_summary = hold["route_details"]["breakout6_half150_trail30_wick"]["strategy_summary"]
    baseline = indexed.loc[0.0]
    checks["full_initial_baseline_matches_hold_router"] = (
        int(baseline["trades"]) == int(hold_summary["trades"]) == 65
        and close(baseline["expectancy"], hold_summary["expectancy"])
        and close(baseline["profit_factor"], hold_summary["profit_factor"])
        and close(baseline["total_return_on_initial_equity"], hold_summary["total_return_on_initial_equity"])
        and close(baseline["max_drawdown"], hold_summary["max_drawdown"])
    )
    confirmation_add = summary[summary["breakout_add_fraction"] > 0]
    checks["confirmation_rejects_reserving_cash_for_breakout"] = (
        bool((confirmation_add["expectancy"] < baseline["expectancy"]).all())
        and bool((confirmation_add["total_return_on_initial_equity"] < baseline["total_return_on_initial_equity"]).all())
        and bool((confirmation_add["paired_increment_mean"] < 0).all())
        and bool((confirmation_add["independent_event_increment_mean"] < 0).all())
        and bool((confirmation_add["fold_increment_positive_share"] <= 0.25).all())
    )

    identity_checks: list[bool] = []
    for fraction in [0.10, 0.25, 0.50]:
        rows = trades[np.isclose(trades["breakout_add_fraction"], fraction)]
        expected = (1.0 - fraction) * rows["initial_leg_return"] + fraction * rows["add_leg_return"].fillna(0.0)
        identity_checks.append(bool(np.allclose(rows["net_return"], expected, rtol=1e-10, atol=1e-10)))
        route_pair = paired[np.isclose(paired["breakout_add_fraction"], fraction)]
        identity_checks.append(close(route_pair["delta_return"].mean(), indexed.loc[fraction, "paired_increment_mean"]))
        route_trades = trades[np.isclose(trades["breakout_add_fraction"], fraction)]
        identity_checks.append(close(route_trades["net_return"].mean(), indexed.loc[fraction, "expectancy"]))
        identity_checks.append(close(profit_factor(route_trades["net_return"]), indexed.loc[fraction, "profit_factor"]))
    checks["weighted_tranche_and_portfolio_metrics_recomputed"] = bool(all(identity_checks))
    checks["all_allocations_cover_all_confirmation_folds"] = all(
        set(folds[np.isclose(folds["breakout_add_fraction"], fraction)]["fold"].astype(int)) == {3, 4, 5, 6}
        for fraction in [0.0, 0.10, 0.25, 0.50]
    )
    checks["no_forward_or_primary_change"] = (
        decision["prospective_diagnostic_gate_pass"] is False
        and decision["classification"] == "retain_full_initial_position"
        and decision["historical_promotion_allowed"] is False
        and decision["forward_counterfactual_added"] is False
        and decision["primary_entry_changed"] is False
        and decision["primary_exit_changed"] is False
        and decision["automatic_trading_allowed"] is False
    )

    errors = [name for name, passed in checks.items() if not passed]
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "selected_breakout_add_fraction": float(decision["selected_breakout_add_fraction"]),
            "baseline_expectancy": float(baseline["expectancy"]),
            "baseline_profit_factor": float(baseline["profit_factor"]),
            "confirmation_add_increments": {
                str(row["breakout_add_fraction"]): float(row["paired_increment_mean"])
                for _, row in confirmation_add.iterrows()
            },
        },
    }
    (REPORT / "utility_breakout_add_verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
