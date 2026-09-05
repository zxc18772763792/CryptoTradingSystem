"""Test risk-neutral breakout add-ons after the frozen 8h utility entry.

The total planned position is fixed.  A fraction enters at the frozen 8h
decision open and the reserved fraction enters only if the already-defined
``breakout6`` trigger completes within 24h, at the following 4h open.  Each
tranche has its own conservative stateful exit, fees, funding and slippage.
Untriggered reserve remains cash with zero return.

Allocation is selected on the earlier mature calibration windows with a
14-day common horizon and then evaluated on confirmation folds with a 30-day
common horizon.  The breakout rule itself was discovered after confirmation
inspection, so even a passing allocation remains prospective-only.
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
ALLOCATION_FRACTIONS = (0.0, 0.10, 0.25, 0.50)
ROUTED_POLICY = "breakout6_half150_trail30_wick"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


HOLD = _load(
    "binance_breakout_hold_router_for_breakout_add",
    ROOT / "scripts" / "analyze_binance_utility_breakout_hold_router.py",
)
TIMING = HOLD.TIMING
FAILURE = HOLD.FAILURE
CONTINUATION = HOLD.CONTINUATION
ENTRY = HOLD.ENTRY
EXIT = HOLD.EXIT
STUDY = HOLD.STUDY
VALIDATION = HOLD.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def _mark_frame(outcome: Mapping[str, Any] | None) -> pd.DataFrame:
    if outcome is None or not outcome.get("mark_path"):
        return pd.DataFrame(columns=["time", "mark_return", "remaining_fraction"])
    frame = pd.DataFrame(outcome["mark_path"])
    frame["time"] = pd.to_datetime(frame["time"], utc=True)
    return frame.sort_values("time").drop_duplicates("time", keep="last").reset_index(drop=True)


def combine_mark_paths(
    initial: Mapping[str, Any],
    add: Mapping[str, Any] | None,
    *,
    initial_fraction: float,
    add_fraction: float,
) -> list[dict[str, Any]]:
    initial_marks = _mark_frame(initial)
    add_marks = _mark_frame(add)
    times = sorted(set(initial_marks.get("time", [])) | set(add_marks.get("time", [])))
    if not times:
        return []

    def value_at(frame: pd.DataFrame, when: pd.Timestamp, field: str) -> float:
        if frame.empty:
            return 0.0
        available = frame[frame["time"] <= when]
        if available.empty:
            return 0.0
        return float(available.iloc[-1][field])

    output: list[dict[str, Any]] = []
    for when in times:
        output.append(
            {
                "time": pd.Timestamp(when),
                "mark_return": float(
                    initial_fraction * value_at(initial_marks, when, "mark_return")
                    + add_fraction * value_at(add_marks, when, "mark_return")
                ),
                "remaining_fraction": float(
                    initial_fraction * value_at(initial_marks, when, "remaining_fraction")
                    + add_fraction * value_at(add_marks, when, "remaining_fraction")
                ),
            }
        )
    return output


def combine_tranches(
    initial: Mapping[str, Any],
    add: Mapping[str, Any] | None,
    *,
    add_fraction: float,
) -> dict[str, Any]:
    initial_fraction = 1.0 - float(add_fraction)
    add_used = add is not None and add_fraction > 0
    combined = dict(initial)
    combined_marks = combine_mark_paths(
        initial,
        add if add_used else None,
        initial_fraction=initial_fraction,
        add_fraction=float(add_fraction) if add_used else 0.0,
    )
    add_return = float(add["net_return"]) if add_used else 0.0
    combined["net_return"] = float(initial_fraction * float(initial["net_return"]) + float(add_fraction) * add_return)
    combined["funding_return"] = float(
        initial_fraction * float(initial.get("funding_return", 0.0))
        + (float(add_fraction) * float(add.get("funding_return", 0.0)) if add_used else 0.0)
    )
    combined["fee_return"] = float(
        initial_fraction * float(initial.get("fee_return", 0.0))
        + (float(add_fraction) * float(add.get("fee_return", 0.0)) if add_used else 0.0)
    )
    if add_used:
        combined["exit_time"] = max(pd.Timestamp(initial["exit_time"]), pd.Timestamp(add["exit_time"]))
        combined["exit_reason"] = f"initial:{initial['exit_reason']}|add:{add['exit_reason']}"
    combined["mark_path"] = combined_marks
    mark_returns = [float(item["mark_return"]) for item in combined_marks]
    combined["mfe"] = max(mark_returns) if mark_returns else float(combined["net_return"])
    combined["mae"] = min(mark_returns) if mark_returns else float(combined["net_return"])
    combined["policy_family"] = "risk_neutral_breakout_add"
    combined["policy"] = f"initial_{initial_fraction:.2f}_breakout_add_{add_fraction:.2f}"
    combined["initial_fraction"] = float(initial_fraction)
    combined["breakout_add_fraction"] = float(add_fraction)
    combined["breakout_add_executed"] = bool(add_used)
    combined["initial_leg_return"] = float(initial["net_return"])
    combined["add_leg_return"] = float(add["net_return"]) if add_used else None
    combined["add_entry_time"] = pd.Timestamp(add["entry_time"]) if add_used else None
    combined["add_entry_price"] = float(add["entry_price"]) if add_used else None
    combined["planned_deployed_fraction"] = float(initial_fraction + (add_fraction if add_used else 0.0))
    combined["partial_take_profit"] = bool(initial.get("partial_take_profit")) or bool(add_used and add.get("partial_take_profit"))
    combined["funding_observed"] = bool(initial.get("funding_observed")) and bool(not add_used or add.get("funding_observed"))
    return combined


def build_leg_cache(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    switches: Mapping[int, pd.Timestamp],
    *,
    days: int,
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
    post_policy = HOLD.routed_policies(days)[ROUTED_POLICY]
    assert post_policy is not None
    initial = HOLD.simulate_router(
        entries,
        bars,
        funding,
        switches,
        route_name=ROUTED_POLICY,
        post_policy=post_policy,
        slippage_bps=slippage_bps,
        days=days,
    )
    add_legs: dict[int, dict[str, Any]] = {}
    row_lookup = entries.set_index(entries["signal_key"].astype(int))
    for signal_key, switch_time in switches.items():
        if signal_key not in row_lookup.index:
            continue
        row = row_lookup.loc[signal_key]
        symbol = str(row["symbol"])
        frame = bars.get(symbol)
        if frame is None:
            continue
        common_horizon = pd.Timestamp(row["entry_time"]) + pd.Timedelta(days=days)
        bounded = frame[pd.to_datetime(frame["open_time"], utc=True) <= common_horizon].copy()
        add = ENTRY.simulate_stateful_trade(
            bounded,
            entry_time=pd.Timestamp(switch_time),
            policy=post_policy,
            slippage_bps=slippage_bps,
            funding=funding.get(symbol),
            fee_bps=5.0,
            include_marks=True,
        )
        if add is not None:
            add_legs[int(signal_key)] = add
    return initial, add_legs


def allocate(
    initial: list[dict[str, Any]],
    add_legs: Mapping[int, dict[str, Any]],
    *,
    add_fraction: float,
) -> list[dict[str, Any]]:
    return [
        combine_tranches(
            item,
            add_legs.get(int(item["signal_key"])),
            add_fraction=add_fraction,
        )
        for item in initial
    ]


def event_paired(paired: pd.DataFrame, baseline_raw: list[dict[str, Any]]) -> pd.DataFrame:
    baseline = pd.DataFrame([EXIT.clean_trade(item) for item in baseline_raw])
    event_keys = set(TIMING.first_signal_per_event(baseline)["signal_key"].astype(int))
    return paired[paired["signal_key"].astype(int).isin(event_keys)].copy()


def calibration_table(
    entries: pd.DataFrame,
    initial: list[dict[str, Any]],
    add_legs: Mapping[int, dict[str, Any]],
) -> tuple[pd.DataFrame, dict[float, list[dict[str, Any]]]]:
    outcomes = {fraction: allocate(initial, add_legs, add_fraction=fraction) for fraction in ALLOCATION_FRACTIONS}
    baseline = outcomes[0.0]
    records: list[dict[str, Any]] = []
    for fraction, raw in outcomes.items():
        trades, _, summary = TIMING.portfolio(raw)
        paired = FAILURE.paired_deltas(baseline, raw)
        by_split = paired.groupby("validation_split")["delta_return"].mean()
        leave_largest, concentration = STUDY.concentration_stats(trades)
        eligible = bool(
            fraction > 0
            and len(trades) >= 20
            and len(by_split) == 3
            and float(paired["delta_return"].mean()) > 0
            and float((by_split > 0).mean()) >= 2 / 3
            and summary.get("expectancy", -1) > 0
            and summary.get("profit_factor", 0) > 1
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.35
        )
        score = float(by_split.mean() - 0.5 * by_split.std(ddof=0)) if len(by_split) else -np.inf
        records.append(
            {
                "breakout_add_fraction": fraction,
                "initial_fraction": 1.0 - fraction,
                "signals": len(raw),
                "breakout_adds": int(sum(bool(item["breakout_add_executed"]) for item in raw)),
                "mean_deployed_fraction": float(np.mean([item["planned_deployed_fraction"] for item in raw])),
                "trades": int(summary.get("trades", 0)),
                "expectancy": summary.get("expectancy"),
                "profit_factor": summary.get("profit_factor"),
                "max_drawdown": summary.get("max_drawdown"),
                "paired_increment_mean": float(paired["delta_return"].mean()),
                "positive_calibration_split_share": float((by_split > 0).mean()),
                "calibration_split_increment_mean": float(by_split.mean()),
                "calibration_split_increment_std": float(by_split.std(ddof=0)),
                "selection_score": score,
                "leave_largest_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_eligible": eligible,
            }
        )
    result = pd.DataFrame(records).sort_values(
        ["selection_eligible", "selection_score", "profit_factor"], ascending=[False, False, False]
    ).reset_index(drop=True)
    return result, outcomes


def confirmation_detail(
    baseline_raw: list[dict[str, Any]],
    raw: list[dict[str, Any]],
    *,
    samples: int,
    seed: int,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    trades, equity, summary = TIMING.portfolio(raw)
    folds = STUDY.fold_metrics(trades) if len(trades) else pd.DataFrame()
    paired = FAILURE.paired_deltas(baseline_raw, raw)
    events = event_paired(paired, baseline_raw)
    week = FAILURE.delta_bootstrap(paired, samples=samples, cluster="week", seed=seed)
    symbol = FAILURE.delta_bootstrap(paired, samples=samples, cluster="symbol", seed=seed + 1)
    event_week = FAILURE.delta_bootstrap(events, samples=samples, cluster="week", seed=seed + 2)
    event_symbol = FAILURE.delta_bootstrap(events, samples=samples, cluster="symbol", seed=seed + 3)
    abs_week = EXIT.bootstrap_returns(trades, samples=samples, cluster="week", seed=seed + 4)
    abs_symbol = EXIT.bootstrap_returns(trades, samples=samples, cluster="symbol", seed=seed + 5)
    by_fold = paired.groupby("fold")["delta_return"].mean()
    flags = pd.DataFrame(
        {
            "signal_key": [int(item["signal_key"]) for item in raw],
            "breakout_add_executed": [bool(item["breakout_add_executed"]) for item in raw],
        }
    )
    segmented = paired.merge(flags, on="signal_key", how="left", validate="one_to_one")
    leave_largest, concentration = STUDY.concentration_stats(trades)
    detail = {
        "strategy_summary": summary,
        "fold_metrics": folds.to_dict(orient="records"),
        "paired_rows": int(len(paired)),
        "independent_event_rows": int(len(events)),
        "paired_increment_mean": float(paired["delta_return"].mean()),
        "independent_event_increment_mean": float(events["delta_return"].mean()),
        "fold_increment_means": {str(int(key)): float(value) for key, value in by_fold.items()},
        "fold_increment_positive_share": float((by_fold > 0).mean()),
        "week_increment_bootstrap": week,
        "symbol_increment_bootstrap": symbol,
        "event_week_increment_bootstrap": event_week,
        "event_symbol_increment_bootstrap": event_symbol,
        "week_return_bootstrap": abs_week,
        "symbol_return_bootstrap": abs_symbol,
        "breakout_segment_increment": float(segmented.loc[segmented["breakout_add_executed"], "delta_return"].mean()),
        "nonbreakout_segment_increment": float(segmented.loc[~segmented["breakout_add_executed"], "delta_return"].mean()),
        "breakout_adds": int(flags["breakout_add_executed"].sum()),
        "mean_deployed_fraction": float(np.mean([item["planned_deployed_fraction"] for item in raw])),
        "mean_add_leg_return": float(np.mean([item["add_leg_return"] for item in raw if item["add_leg_return"] is not None])),
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
    panel = pd.read_csv(baseline_dir / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)

    period_rows: dict[str, pd.DataFrame] = {}
    period_switches: dict[str, dict[int, pd.Timestamp]] = {}
    for period in ["calibration", "confirmation"]:
        period_frame = entries[entries["period"] == period]
        period_rows[period] = period_frame[period_frame["entry_rule"] == "launch_open"].copy()
        breakout = period_frame[period_frame["entry_rule"] == "breakout6"]
        period_switches[period] = {
            int(row["signal_key"]): pd.Timestamp(row["entry_time"])
            for _, row in breakout.iterrows()
        }

    calibration_initial, calibration_add = build_leg_cache(
        period_rows["calibration"], bars, funding, period_switches["calibration"], days=14, slippage_bps=5.0
    )
    calibration, _ = calibration_table(period_rows["calibration"], calibration_initial, calibration_add)
    calibration.to_csv(report / "utility_breakout_add_calibration.csv", index=False)
    eligible = calibration[calibration["selection_eligible"]]
    selected_fraction = float(eligible.iloc[0]["breakout_add_fraction"]) if len(eligible) else 0.0
    selected_calibration_eligible = bool(len(eligible))

    default_initial, default_add = build_leg_cache(
        period_rows["confirmation"], bars, funding, period_switches["confirmation"], days=30, slippage_bps=5.0
    )
    baseline_raw = allocate(default_initial, default_add, add_fraction=0.0)
    details: dict[str, Any] = {}
    summary_records: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    fold_frames: list[pd.DataFrame] = []
    paired_frames: list[pd.DataFrame] = []
    for index, fraction in enumerate(ALLOCATION_FRACTIONS):
        raw = allocate(default_initial, default_add, add_fraction=fraction)
        detail, trades, equity, paired = confirmation_detail(
            baseline_raw, raw, samples=args.bootstrap_samples, seed=20260980 + index * 10
        )
        details[str(fraction)] = detail
        if len(trades):
            trades["breakout_add_fraction"] = fraction
            trade_frames.append(trades)
        if len(equity):
            equity["breakout_add_fraction"] = fraction
            equity_frames.append(equity)
        folds = pd.DataFrame(detail["fold_metrics"])
        if len(folds):
            folds.insert(0, "breakout_add_fraction", fraction)
            fold_frames.append(folds)
        paired.insert(0, "breakout_add_fraction", fraction)
        paired_frames.append(paired)
        strategy = detail["strategy_summary"]
        summary_records.append(
            {
                "breakout_add_fraction": fraction,
                "initial_fraction": 1.0 - fraction,
                "trades": int(strategy.get("trades", 0)),
                "expectancy": strategy.get("expectancy"),
                "profit_factor": strategy.get("profit_factor"),
                "total_return_on_initial_equity": strategy.get("total_return_on_initial_equity"),
                "max_drawdown": strategy.get("max_drawdown"),
                "paired_increment_mean": detail["paired_increment_mean"],
                "independent_event_increment_mean": detail["independent_event_increment_mean"],
                "fold_increment_positive_share": detail["fold_increment_positive_share"],
                "breakout_adds": detail["breakout_adds"],
                "mean_deployed_fraction": detail["mean_deployed_fraction"],
                "mean_add_leg_return": detail["mean_add_leg_return"],
                "largest_positive_pnl_share": detail["largest_positive_pnl_share"],
                "leave_largest_winner_out_expectancy": detail["leave_largest_winner_out_expectancy"],
            }
        )

    summary = pd.DataFrame(summary_records)
    stress_records: list[dict[str, Any]] = []
    selected_default_raw = allocate(default_initial, default_add, add_fraction=selected_fraction)
    for slippage in (5.0, 15.0, 30.0):
        if slippage == 5.0:
            raw = selected_default_raw
        else:
            initial, add_legs = build_leg_cache(
                period_rows["confirmation"], bars, funding, period_switches["confirmation"],
                days=30, slippage_bps=slippage,
            )
            raw = allocate(initial, add_legs, add_fraction=selected_fraction)
        _, _, strategy = TIMING.portfolio(raw)
        stress_records.append({"slippage_bps_each_side": slippage, **strategy})

    selected_detail = details[str(selected_fraction)]
    baseline_detail = details["0.0"]
    selected_strategy = selected_detail["strategy_summary"]
    baseline_strategy = baseline_detail["strategy_summary"]
    diagnostic_gate = bool(
        selected_fraction > 0
        and selected_calibration_eligible
        and selected_strategy.get("expectancy", -1) > baseline_strategy.get("expectancy", np.inf)
        and selected_strategy.get("profit_factor", 0) > baseline_strategy.get("profit_factor", np.inf)
        and selected_detail["paired_increment_mean"] > 0
        and selected_detail["week_increment_bootstrap"].get("mean_delta_lower_95pct", -1) > 0
        and selected_detail["symbol_increment_bootstrap"].get("mean_delta_lower_95pct", -1) > 0
        and selected_detail["event_week_increment_bootstrap"].get("mean_delta_lower_95pct", -1) > 0
        and selected_detail["event_symbol_increment_bootstrap"].get("mean_delta_lower_95pct", -1) > 0
        and selected_detail["fold_increment_positive_share"] >= 0.75
        and selected_strategy.get("fold_majority_profit_factor_gt_1", 0) > 0.50
        and selected_strategy.get("market_state_majority_profit_factor_gt_1", 0) > 0.50
        and len(stress_records) == 3
        and all(item["expectancy"] > 0 and item["profit_factor"] > 1 and item["max_drawdown"] >= -0.30 for item in stress_records)
        and selected_detail["largest_positive_pnl_share"] is not None
        and selected_detail["largest_positive_pnl_share"] <= 0.25
        and selected_detail["leave_largest_winner_out_expectancy"] is not None
        and selected_detail["leave_largest_winner_out_expectancy"] > 0
    )

    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "time_isolated_allocation_on_posthoc_breakout_router_prospective_only",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "allocation_catalog": list(ALLOCATION_FRACTIONS),
        "risk_constraint": "planned position risk fixed; untriggered reserve remains cash; no leverage or extra gross exposure",
        "tranche_exit": "each tranche uses hard25/be30/half150/trail30/wick on its own entry; common signal horizon",
        "calibration_horizon_days": 14,
        "confirmation_horizon_days": 30,
        "calibration_signals": int(len(period_rows["calibration"])),
        "calibration_breakout_add_opportunities": int(len(period_switches["calibration"])),
        "confirmation_signals": int(len(period_rows["confirmation"])),
        "confirmation_breakout_add_opportunities": int(len(period_switches["confirmation"])),
        "calibration_table": calibration.to_dict(orient="records"),
        "selected_breakout_add_fraction": selected_fraction,
        "selected_initial_fraction": 1.0 - selected_fraction,
        "selected_calibration_eligible": selected_calibration_eligible,
        "confirmation_details": details,
        "selected_cost_stress": stress_records,
        "prospective_diagnostic_gate_pass": diagnostic_gate,
        "classification": "prospective_breakout_add_pending_true_oos" if diagnostic_gate else "retain_full_initial_position",
        "historical_promotion_allowed": False,
        "why_no_historical_promotion": "the breakout router was generated after inspecting confirmation outcomes even though allocation was selected on earlier windows",
        "forward_counterfactual_added": False,
        "primary_entry_changed": False,
        "primary_exit_changed": False,
        "automatic_trading_allowed": False,
    }
    summary.to_csv(report / "utility_breakout_add_summary.csv", index=False)
    pd.concat(fold_frames, ignore_index=True).to_csv(report / "utility_breakout_add_folds.csv", index=False)
    pd.concat(trade_frames, ignore_index=True).to_csv(
        report / "utility_breakout_add_trades.csv.gz", index=False, compression="gzip"
    )
    pd.concat(equity_frames, ignore_index=True).to_csv(
        report / "utility_breakout_add_equity.csv.gz", index=False, compression="gzip"
    )
    pd.concat(paired_frames, ignore_index=True).to_csv(report / "utility_breakout_add_paired_deltas.csv", index=False)
    pd.DataFrame(stress_records).to_csv(report / "utility_breakout_add_cost_stress.csv", index=False)
    (report / "utility_breakout_add_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready({
        "selected_breakout_add_fraction": selected_fraction,
        "selected_calibration_eligible": selected_calibration_eligible,
        "calibration": calibration.to_dict(orient="records"),
        "confirmation": summary.to_dict(orient="records"),
        "prospective_diagnostic_gate_pass": diagnostic_gate,
        "classification": decision["classification"],
    }), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
