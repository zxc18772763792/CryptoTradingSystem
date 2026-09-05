"""Test breakout confirmation as a hold-management router, not an entry.

The frozen 8h utility signal always enters at its original decision open.  If
the already-defined ``breakout6`` condition completes within the next 24h, the
profit-side exit may switch at the following 4h open.  Stops and state created
before that open are preserved.  The breakout router was proposed after
viewing confirmation outcomes, so this study is hypothesis-generating only and
cannot promote a historical strategy.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


TIMING = _load(
    "binance_utility_entry_timing_for_breakout_hold_router",
    ROOT / "scripts" / "analyze_binance_utility_entry_timing.py",
)
FAILURE = TIMING.FAILURE
CONTINUATION = TIMING.CONTINUATION
ENTRY = TIMING.ENTRY
EXIT = TIMING.EXIT
STUDY = TIMING.STUDY
VALIDATION = TIMING.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def routed_policies(days: int) -> dict[str, dict[str, Any] | None]:
    base = CONTINUATION.frozen_policy(days)
    base["family"] = "frozen_half100_trail25"

    trail30 = dict(base)
    trail30.update({"family": "breakout_hold_trail30", "trail_distance": 0.30})

    tail30 = dict(trail30)
    tail30.update(
        {
            "family": "breakout_hold_half150_trail30",
            "partial_target": 1.50,
            "partial_fraction": 0.50,
            "trail_activation": 1.50,
        }
    )

    tail30_wick = dict(tail30)
    tail30_wick.update(
        {
            "family": "breakout_hold_half150_trail30_wick",
            "exhaustion_activation": 1.00,
            "exhaustion_upper_wick_min": 0.35,
            "exhaustion_close_location_max": 0.35,
            "exhaustion_volume_ratio_min": 1.50,
        }
    )
    return {
        "baseline_no_router": None,
        "breakout6_half100_trail30": trail30,
        "breakout6_half150_trail30": tail30,
        "breakout6_half150_trail30_wick": tail30_wick,
    }


def simulate_router(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    switches: Mapping[int, pd.Timestamp],
    *,
    route_name: str,
    post_policy: Mapping[str, Any] | None,
    slippage_bps: float,
    days: int = 30,
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    baseline_policy = CONTINUATION.frozen_policy(days)
    for _, row in entries.iterrows():
        signal_key = int(row["signal_key"])
        symbol = str(row["symbol"])
        frame = bars.get(symbol)
        switch_time = switches.get(signal_key) if post_policy is not None else None
        outcome = None if frame is None else ENTRY.simulate_stateful_trade(
            frame,
            entry_time=pd.Timestamp(row["entry_time"]),
            policy=baseline_policy,
            slippage_bps=slippage_bps,
            funding=funding.get(symbol),
            fee_bps=5.0,
            include_marks=True,
            policy_switch_time=switch_time,
            post_switch_policy=post_policy if switch_time is not None else None,
        )
        if outcome is None:
            continue
        outcome.update(
            {
                "symbol": symbol,
                "signal_date": pd.Timestamp(row["date"]),
                "fold": int(row["fold"]),
                "validation_split": str(row["validation_split"]),
                "score": float(row["continuation_score"]),
                "score_pctile": float(row["price_model_pctile"]),
                "signal_key": signal_key,
                "entry_rule": "continuation_utility_8h",
                "entry_delay_hours": 8.0,
                "target200_14d_from_entry": bool(row["target200_14d_from_entry"]),
                "future_max_return_14d_from_entry": float(row["future_max_return_14d_from_entry"]),
                "breadth_regime": row.get("breadth_regime"),
                "btc_trend_regime": row.get("btc_trend_regime"),
                "btc_vol_regime": row.get("btc_vol_regime"),
                "market_state": row.get("market_state"),
                "policy": route_name,
                "slippage_bps_each_side": float(slippage_bps),
                "breakout_confirmed": bool(signal_key in switches),
                "breakout_switch_time": switches.get(signal_key),
            }
        )
        outcomes.append(outcome)
    return outcomes


def route_metrics(
    baseline_raw: list[dict[str, Any]],
    outcomes: list[dict[str, Any]],
    *,
    bootstrap_samples: int,
    seed: int,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    trades, equity, summary = TIMING.portfolio(outcomes)
    folds = STUDY.fold_metrics(trades) if len(trades) else pd.DataFrame()
    paired = FAILURE.paired_deltas(baseline_raw, outcomes)
    baseline_frame = pd.DataFrame([EXIT.clean_trade(item) for item in baseline_raw])
    event_keys = set(
        TIMING.first_signal_per_event(baseline_frame)["signal_key"].astype(int)
    ) if len(baseline_frame) else set()
    event_paired = paired[paired["signal_key"].astype(int).isin(event_keys)].copy()
    routed_keys = {
        int(item["signal_key"])
        for item in outcomes
        if bool(item.get("breakout_confirmed"))
    }
    routed_pair = paired[paired["signal_key"].astype(int).isin(routed_keys)].copy()
    changed = routed_pair[routed_pair["delta_return"].abs() > 1e-12].copy()
    week_delta = FAILURE.delta_bootstrap(
        paired, samples=bootstrap_samples, cluster="week", seed=seed
    )
    symbol_delta = FAILURE.delta_bootstrap(
        paired, samples=bootstrap_samples, cluster="symbol", seed=seed + 1
    )
    routed_week_delta = FAILURE.delta_bootstrap(
        routed_pair, samples=bootstrap_samples, cluster="week", seed=seed + 2
    ) if len(routed_pair) else {}
    routed_symbol_delta = FAILURE.delta_bootstrap(
        routed_pair, samples=bootstrap_samples, cluster="symbol", seed=seed + 3
    ) if len(routed_pair) else {}
    event_week_delta = FAILURE.delta_bootstrap(
        event_paired, samples=bootstrap_samples, cluster="week", seed=seed + 6
    ) if len(event_paired) else {}
    event_symbol_delta = FAILURE.delta_bootstrap(
        event_paired, samples=bootstrap_samples, cluster="symbol", seed=seed + 7
    ) if len(event_paired) else {}
    week_return = EXIT.bootstrap_returns(
        trades, samples=bootstrap_samples, cluster="week", seed=seed + 4
    ) if len(trades) else {}
    symbol_return = EXIT.bootstrap_returns(
        trades, samples=bootstrap_samples, cluster="symbol", seed=seed + 5
    ) if len(trades) else {}
    leave_largest, concentration = STUDY.concentration_stats(trades)
    applied = [item for item in outcomes if bool(item.get("policy_switch_applied"))]
    fold_delta = paired.groupby("fold", sort=True)["delta_return"].mean()
    detail = {
        "strategy_summary": summary,
        "fold_metrics": folds.to_dict(orient="records"),
        "fold_profit_factor_gt1_share": float((folds["profit_factor"] > 1).mean()) if len(folds) else 0.0,
        "paired_rows": int(len(paired)),
        "paired_increment_mean_all_signals": float(paired["delta_return"].mean()) if len(paired) else None,
        "independent_event_rows": int(len(event_paired)),
        "independent_event_increment_mean": float(event_paired["delta_return"].mean()) if len(event_paired) else None,
        "fold_increment_positive_share": float((fold_delta > 0).mean()) if len(fold_delta) else 0.0,
        "fold_increment_means": {str(int(key)): float(value) for key, value in fold_delta.items()},
        "routed_rows": int(len(routed_pair)),
        "routed_increment_mean": float(routed_pair["delta_return"].mean()) if len(routed_pair) else None,
        "changed_routed_rows": int(len(changed)),
        "changed_routed_positive_share": float((changed["delta_return"] > 0).mean()) if len(changed) else None,
        "changed_routed_increment_mean": float(changed["delta_return"].mean()) if len(changed) else None,
        "switch_applied_rows": int(len(applied)),
        "partial_done_before_switch": int(sum(bool(item.get("partial_done_before_switch")) for item in applied)),
        "trail_active_before_switch": int(sum(bool(item.get("trail_active_before_switch")) for item in applied)),
        "week_increment_bootstrap_all_signals": week_delta,
        "symbol_increment_bootstrap_all_signals": symbol_delta,
        "week_increment_bootstrap_routed_signals": routed_week_delta,
        "symbol_increment_bootstrap_routed_signals": routed_symbol_delta,
        "week_increment_bootstrap_independent_events": event_week_delta,
        "symbol_increment_bootstrap_independent_events": event_symbol_delta,
        "week_return_bootstrap": week_return,
        "symbol_return_bootstrap": symbol_return,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
    }
    return detail, trades, equity, paired


def main() -> None:
    args = parse_args()
    baseline_dir = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    entries = pd.read_csv(report / "utility_entry_timing_candidates.csv.gz", compression="gzip")
    for column in ["date", "decision_time", "trigger_time", "entry_time"]:
        entries[column] = pd.to_datetime(entries[column], utc=True)
    confirmation = entries[entries["period"] == "confirmation"].copy()
    baseline_entries = confirmation[confirmation["entry_rule"] == "launch_open"].copy()
    breakout = confirmation[confirmation["entry_rule"] == "breakout6"].copy()
    switches = {
        int(row["signal_key"]): pd.Timestamp(row["entry_time"])
        for _, row in breakout.iterrows()
    }

    panel = pd.read_csv(baseline_dir / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)
    policies = routed_policies(30)
    baseline_raw = simulate_router(
        baseline_entries,
        bars,
        funding,
        switches,
        route_name="baseline_no_router",
        post_policy=None,
        slippage_bps=5.0,
    )

    route_details: dict[str, Any] = {}
    summary_records: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    fold_frames: list[pd.DataFrame] = []
    paired_frames: list[pd.DataFrame] = []
    for route_index, (route_name, post_policy) in enumerate(policies.items()):
        default_outcomes: list[dict[str, Any]] = []
        stress: list[dict[str, Any]] = []
        for slippage in (5.0, 15.0, 30.0):
            outcomes = baseline_raw if route_name == "baseline_no_router" and slippage == 5.0 else simulate_router(
                baseline_entries,
                bars,
                funding,
                switches,
                route_name=route_name,
                post_policy=post_policy,
                slippage_bps=slippage,
            )
            trades, equity, strategy = TIMING.portfolio(outcomes)
            if strategy:
                stress.append({"slippage_bps_each_side": slippage, **strategy})
            if slippage == 5.0:
                default_outcomes = outcomes
                if len(trades):
                    trades["route"] = route_name
                    trade_frames.append(trades)
                if len(equity):
                    equity["route"] = route_name
                    equity_frames.append(equity)

        detail, _, _, paired = route_metrics(
            baseline_raw,
            default_outcomes,
            bootstrap_samples=args.bootstrap_samples,
            seed=20260920 + route_index * 10,
        )
        detail["cost_stress"] = stress
        route_details[route_name] = detail
        folds = pd.DataFrame(detail["fold_metrics"])
        if len(folds):
            folds.insert(0, "route", route_name)
            fold_frames.append(folds)
        paired.insert(0, "route", route_name)
        paired_frames.append(paired)
        strategy = detail["strategy_summary"]
        summary_records.append(
            {
                "route": route_name,
                "trades": int(strategy.get("trades", 0)),
                "expectancy": strategy.get("expectancy"),
                "profit_factor": strategy.get("profit_factor"),
                "total_return_on_initial_equity": strategy.get("total_return_on_initial_equity"),
                "max_drawdown": strategy.get("max_drawdown"),
                "fold_profit_factor_gt1_share": detail["fold_profit_factor_gt1_share"],
                "fold_increment_positive_share": detail["fold_increment_positive_share"],
                "paired_increment_mean_all_signals": detail["paired_increment_mean_all_signals"],
                "independent_event_increment_mean": detail["independent_event_increment_mean"],
                "routed_increment_mean": detail["routed_increment_mean"],
                "changed_routed_rows": detail["changed_routed_rows"],
                "changed_routed_positive_share": detail["changed_routed_positive_share"],
                "largest_positive_pnl_share": detail["largest_positive_pnl_share"],
                "leave_largest_winner_out_expectancy": detail["leave_largest_winner_out_expectancy"],
            }
        )

    summary = pd.DataFrame(summary_records)
    baseline_summary = route_details["baseline_no_router"]["strategy_summary"]
    diagnostic_candidates: list[dict[str, Any]] = []
    for route_name in summary.loc[summary["route"] != "baseline_no_router", "route"]:
        detail = route_details[str(route_name)]
        strategy = detail["strategy_summary"]
        stress = detail["cost_stress"]
        prospective_ok = bool(
            strategy.get("expectancy", -np.inf) > baseline_summary.get("expectancy", np.inf)
            and strategy.get("profit_factor", -np.inf) > baseline_summary.get("profit_factor", np.inf)
            and detail["paired_increment_mean_all_signals"] > 0
            and detail["week_increment_bootstrap_all_signals"].get("mean_delta_lower_95pct", -1) > 0
            and detail["symbol_increment_bootstrap_all_signals"].get("mean_delta_lower_95pct", -1) > 0
            and detail["week_increment_bootstrap_independent_events"].get("mean_delta_lower_95pct", -1) > 0
            and detail["symbol_increment_bootstrap_independent_events"].get("mean_delta_lower_95pct", -1) > 0
            and detail["fold_increment_positive_share"] >= 0.75
            and detail["fold_profit_factor_gt1_share"] > 0.50
            and strategy.get("market_state_majority_profit_factor_gt_1", 0) > 0.50
            and len(stress) == 3
            and all(item["expectancy"] > 0 and item["profit_factor"] > 1 and item["max_drawdown"] >= -0.30 for item in stress)
            and detail["largest_positive_pnl_share"] is not None
            and detail["largest_positive_pnl_share"] <= 0.25
            and detail["leave_largest_winner_out_expectancy"] is not None
            and detail["leave_largest_winner_out_expectancy"] > 0
        )
        diagnostic_candidates.append(
            {"route": str(route_name), "prospective_diagnostic_gate_pass": prospective_ok}
        )
    passing = [item["route"] for item in diagnostic_candidates if item["prospective_diagnostic_gate_pass"]]
    prospective_router = None
    if passing:
        ranked = summary[summary["route"].isin(passing)].sort_values(
            ["paired_increment_mean_all_signals", "profit_factor"], ascending=False
        )
        prospective_router = str(ranked.iloc[0]["route"])

    trigger_audit = {
        "baseline_signals": int(len(baseline_entries)),
        "breakout_confirmed_signals": int(len(breakout)),
        "breakout_positive_rows": int(breakout["target200_14d_from_entry"].sum()),
        "breakout_independent_events": int(len(TIMING.first_signal_per_event(breakout))),
        "breakout_positive_independent_events": int(TIMING.first_signal_per_event(breakout)["target200_14d_from_entry"].sum()),
        "median_switch_delay_hours": float(breakout["entry_delay_hours_after_launch"].median()),
        "median_breakout_entry_chase": float(breakout["entry_return_vs_decision_open"].median()),
        "switch_clock": "completed breakout6 4h bar; policy changes at following 4h open",
    }
    forward_name = "breakout6_half150_trail30_wick_24h_next_open"
    labeler_text = (ROOT / "scripts" / "binance_forward_strategy_labeler.py").read_text(encoding="utf-8")
    forward_added = bool(
        prospective_router == "breakout6_half150_trail30_wick"
        and forward_name in labeler_text
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "posthoc_confirmation_only_breakout_hold_router_hypothesis",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "entry_changed": False,
        "trigger_audit": trigger_audit,
        "candidate_routes": list(policies),
        "route_details": route_details,
        "diagnostic_candidates": diagnostic_candidates,
        "prospective_router": prospective_router,
        "prospective_diagnostic_gate_pass": bool(prospective_router is not None),
        "historical_promotion_allowed": False,
        "why_no_historical_promotion": "breakout6 was selected after inspecting confirmation-only entry outcomes",
        "primary_entry_changed": False,
        "primary_exit_changed": False,
        "forward_counterfactual_name": forward_name if forward_added else None,
        "forward_counterfactual_added": forward_added,
        "automatic_trading_allowed": False,
    }
    summary.to_csv(report / "utility_breakout_hold_router_summary.csv", index=False)
    pd.concat(fold_frames, ignore_index=True).to_csv(report / "utility_breakout_hold_router_folds.csv", index=False)
    pd.concat(trade_frames, ignore_index=True).to_csv(
        report / "utility_breakout_hold_router_trades.csv.gz", index=False, compression="gzip"
    )
    pd.concat(equity_frames, ignore_index=True).to_csv(
        report / "utility_breakout_hold_router_equity.csv.gz", index=False, compression="gzip"
    )
    pd.concat(paired_frames, ignore_index=True).to_csv(
        report / "utility_breakout_hold_router_paired_deltas.csv", index=False
    )
    (report / "utility_breakout_hold_router_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready({
        "trigger_audit": trigger_audit,
        "summary": summary.to_dict(orient="records"),
        "diagnostic_candidates": diagnostic_candidates,
        "prospective_router": prospective_router,
        "historical_promotion_allowed": False,
    }), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
