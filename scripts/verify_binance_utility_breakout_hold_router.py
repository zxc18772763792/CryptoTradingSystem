"""Independently verify the prospective breakout-confirmed hold router."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
ROUTE = "breakout6_half150_trail30_wick"


def close(left: float, right: float) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=1e-10, atol=1e-10))


def profit_factor(values: pd.Series) -> float:
    data = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(data[data > 0].sum())
    losses = float(-data[data < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def independent_events(rows: pd.DataFrame, cooldown_days: int = 14) -> pd.DataFrame:
    frame = rows.copy()
    frame["signal_date"] = pd.to_datetime(frame["signal_date"], utc=True)
    kept: list[int] = []
    for _, group in frame.sort_values(["symbol", "signal_date", "signal_key"]).groupby("symbol", sort=False):
        anchor: pd.Timestamp | None = None
        for index, row in group.iterrows():
            when = pd.Timestamp(row["signal_date"])
            if anchor is None or when > anchor + pd.Timedelta(days=cooldown_days):
                kept.append(int(index))
                anchor = when
    return frame.loc[kept]


def main() -> None:
    decision = json.loads((REPORT / "utility_breakout_hold_router_decision.json").read_text(encoding="utf-8"))
    summary = pd.read_csv(REPORT / "utility_breakout_hold_router_summary.csv")
    trades = pd.read_csv(REPORT / "utility_breakout_hold_router_trades.csv.gz", compression="gzip")
    folds = pd.read_csv(REPORT / "utility_breakout_hold_router_folds.csv")
    paired = pd.read_csv(REPORT / "utility_breakout_hold_router_paired_deltas.csv")
    checks: dict[str, bool] = {}

    checks["entry_is_frozen_and_switch_clock_is_past_only"] = (
        decision["entry_changed"] is False
        and decision["primary_entry_changed"] is False
        and decision["trigger_audit"]["switch_clock"]
        == "completed breakout6 4h bar; policy changes at following 4h open"
        and int(decision["trigger_audit"]["breakout_confirmed_signals"]) == 21
        and int(decision["route_details"][ROUTE]["switch_applied_rows"]) == 21
        and int(decision["route_details"][ROUTE]["partial_done_before_switch"]) == 0
        and int(decision["route_details"][ROUTE]["trail_active_before_switch"]) == 0
    )
    checks["breakout_recognition_counts_match_entry_frontier"] = (
        int(decision["trigger_audit"]["baseline_signals"]) == 88
        and int(decision["trigger_audit"]["breakout_positive_rows"]) == 6
        and int(decision["trigger_audit"]["breakout_independent_events"]) == 18
        and int(decision["trigger_audit"]["breakout_positive_independent_events"]) == 5
        and close(decision["trigger_audit"]["median_switch_delay_hours"], 12.0)
    )

    indexed = summary.set_index("route")
    baseline = indexed.loc["baseline_no_router"]
    routed = indexed.loc[ROUTE]
    route_trades = trades[trades["route"] == ROUTE]
    route_folds = folds[folds["route"] == ROUTE]
    route_pair = paired[paired["route"] == ROUTE]
    changed = route_pair[route_pair["delta_return"].abs() > 1e-12]
    detail = decision["route_details"][ROUTE]
    checks["portfolio_metrics_recomputed"] = (
        len(route_trades) == int(routed["trades"]) == 65
        and close(route_trades["net_return"].mean(), routed["expectancy"])
        and close(profit_factor(route_trades["net_return"]), routed["profit_factor"])
        and float(routed["expectancy"]) > float(baseline["expectancy"])
        and float(routed["profit_factor"]) > float(baseline["profit_factor"])
        and float(routed["max_drawdown"]) >= -0.30
    )
    checks["paired_gain_is_small_scope_but_consistent"] = (
        len(route_pair) == 88
        and len(changed) == int(detail["changed_routed_rows"]) == 8
        and int((changed["delta_return"] > 0).sum()) == 7
        and close(route_pair["delta_return"].mean(), detail["paired_increment_mean_all_signals"])
        and close(changed["delta_return"].mean(), detail["changed_routed_increment_mean"])
        and close(detail["fold_increment_positive_share"], 1.0)
    )
    event_pair = independent_events(route_pair)
    checks["independent_event_increment_recomputed"] = (
        len(event_pair) == int(detail["independent_event_rows"]) == 67
        and close(event_pair["delta_return"].mean(), detail["independent_event_increment_mean"])
        and float(detail["week_increment_bootstrap_independent_events"]["mean_delta_lower_95pct"]) > 0
        and float(detail["symbol_increment_bootstrap_independent_events"]["mean_delta_lower_95pct"]) > 0
        and float(detail["week_increment_bootstrap_all_signals"]["mean_delta_lower_95pct"]) > 0
        and float(detail["symbol_increment_bootstrap_all_signals"]["mean_delta_lower_95pct"]) > 0
    )
    checks["fold_state_cost_and_concentration_gates_hold"] = (
        len(route_folds) == 4
        and float((route_folds["profit_factor"] > 1).mean()) >= 0.75
        and float(detail["strategy_summary"]["market_state_majority_profit_factor_gt_1"]) > 0.50
        and len(detail["cost_stress"]) == 3
        and all(
            float(item["expectancy"]) > 0
            and float(item["profit_factor"]) > 1
            and float(item["max_drawdown"]) >= -0.30
            for item in detail["cost_stress"]
        )
        and float(detail["largest_positive_pnl_share"]) <= 0.25
        and float(detail["leave_largest_winner_out_expectancy"]) > 0
    )
    static = decision["route_details"]["breakout6_half150_trail30"]
    checks["wick_variant_is_the_robust_prospective_hypothesis"] = (
        float(static["week_increment_bootstrap_independent_events"]["mean_delta_lower_95pct"]) <= 0
        and float(static["symbol_increment_bootstrap_independent_events"]["mean_delta_lower_95pct"]) <= 0
        and decision["prospective_router"] == ROUTE
        and decision["prospective_diagnostic_gate_pass"] is True
    )
    labeler_text = (ROOT / "scripts" / "binance_forward_strategy_labeler.py").read_text(encoding="utf-8")
    checks["forward_counterfactual_is_rank_neutral_and_not_promoted"] = (
        "breakout6_half150_trail30_wick_24h_next_open" in labeler_text
        and "prospective_breakout_hold_router_not_promoted" in labeler_text
        and decision["historical_promotion_allowed"] is False
        and decision["primary_exit_changed"] is False
        and decision["forward_counterfactual_name"] == "breakout6_half150_trail30_wick_24h_next_open"
        and decision["forward_counterfactual_added"] is True
        and decision["automatic_trading_allowed"] is False
    )

    errors = [name for name, passed in checks.items() if not passed]
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "trades": int(len(route_trades)),
            "expectancy": float(route_trades["net_return"].mean()),
            "profit_factor": float(profit_factor(route_trades["net_return"])),
            "paired_increment": float(route_pair["delta_return"].mean()),
            "independent_event_increment": float(event_pair["delta_return"].mean()),
            "changed_paths": int(len(changed)),
            "improved_changed_paths": int((changed["delta_return"] > 0).sum()),
        },
    }
    (REPORT / "utility_breakout_hold_router_verification.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
