"""Audit full-position trailing after a breakout-confirmed utility entry.

The current prospective router sells half at +150% and trails the remainder by
30%, while retaining the frozen -20% stop and a conservative volume-wick exit.
This study removes only the partial sale and pre-registers two trail activation
levels and three trail widths.  Selection is performed on the three mature
14-day calibration windows.  Confirmation is reported, never used to rescue a
candidate rejected by calibration.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"
CURRENT_ROUTER = "breakout6_half150_trail30_wick"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


HOLD = _load(
    "binance_utility_breakout_hold_router_for_full_runner",
    ROOT / "scripts" / "analyze_binance_utility_breakout_hold_router.py",
)
CONTINUATION = HOLD.CONTINUATION
ENTRY = HOLD.ENTRY
STUDY = HOLD.STUDY
TIMING = HOLD.TIMING
VALIDATION = HOLD.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def policy_catalog(days: int) -> dict[str, dict[str, Any]]:
    current = HOLD.routed_policies(days)["breakout6_half150_trail30_wick"]
    result = {CURRENT_ROUTER: current}
    for activation in (1.00, 1.50):
        for trail in (0.25, 0.30, 0.35):
            policy = dict(current)
            policy.update(
                {
                    "family": f"breakout6_full_runner_a{int(activation * 100)}_t{int(trail * 100)}_wick",
                    "partial_target": None,
                    "partial_fraction": 0.0,
                    "trail_activation": activation,
                    "trail_distance": trail,
                }
            )
            result[policy["family"]] = policy
    return result


def load_timing_rows(report: Path) -> pd.DataFrame:
    entries = pd.read_csv(report / "utility_entry_timing_candidates.csv.gz", compression="gzip")
    for column in ["date", "decision_time", "trigger_time", "entry_time"]:
        entries[column] = pd.to_datetime(entries[column], utc=True)
    return entries


def period_rows(entries: pd.DataFrame, period: str) -> tuple[pd.DataFrame, dict[int, pd.Timestamp]]:
    subset = entries[entries["period"] == period].copy()
    launch = subset[subset["entry_rule"] == "launch_open"].copy()
    breakout = subset[subset["entry_rule"] == "breakout6"].copy()
    switches = {
        int(row["signal_key"]): pd.Timestamp(row["entry_time"])
        for _, row in breakout.iterrows()
    }
    return launch, switches


def market_data(baseline: Path) -> tuple[dict[str, pd.DataFrame], Mapping[str, pd.Series]]:
    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    return bars, VALIDATION.load_funding_history(AMBUSH_ROOT)


def simulate(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    switches: Mapping[int, pd.Timestamp],
    *,
    policy_name: str,
    policy: Mapping[str, Any],
    days: int,
    slippage: float,
) -> list[dict[str, Any]]:
    return HOLD.simulate_router(
        entries,
        bars,
        funding,
        switches,
        route_name=policy_name,
        post_policy=policy,
        slippage_bps=slippage,
        days=days,
    )


def selection_record(name: str, detail: Mapping[str, Any]) -> dict[str, Any]:
    summary = detail["strategy_summary"]
    folds = pd.DataFrame(detail["fold_metrics"])
    increments = detail["fold_increment_means"]
    eligible = bool(
        summary.get("trades", 0) >= 30
        and summary.get("expectancy", -1) > 0
        and summary.get("profit_factor", 0) > 1
        and detail.get("leave_largest_winner_out_expectancy") is not None
        and detail["leave_largest_winner_out_expectancy"] > 0
        and detail.get("largest_positive_pnl_share") is not None
        and detail["largest_positive_pnl_share"] <= 0.35
        and len(increments) == 3
        and sum(value > 0 for value in increments.values()) >= 2
        and detail.get("paired_increment_mean_all_signals", -1) > 0
    )
    return {
        "policy": name,
        **summary,
        "leave_largest_winner_out_expectancy": detail.get("leave_largest_winner_out_expectancy"),
        "largest_positive_pnl_share": detail.get("largest_positive_pnl_share"),
        "paired_increment_mean": detail.get("paired_increment_mean_all_signals"),
        "event_increment_mean": detail.get("independent_event_increment_mean"),
        "positive_split_share": (
            float(sum(value > 0 for value in increments.values()) / len(increments)) if increments else 0.0
        ),
        "fold_profit_factor_gt1_share": float((folds["profit_factor"] > 1).mean()) if len(folds) else 0.0,
        "selection_eligible": eligible,
    }


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    entries = load_timing_rows(report)
    bars, funding = market_data(baseline)
    calibration_entries, calibration_switches = period_rows(entries, "calibration")
    confirmation_entries, confirmation_switches = period_rows(entries, "confirmation")

    cal_policies = policy_catalog(14)
    cal_current = simulate(
        calibration_entries, bars, funding, calibration_switches,
        policy_name=CURRENT_ROUTER, policy=cal_policies[CURRENT_ROUTER], days=14, slippage=5.0,
    )
    calibration_records: list[dict[str, Any]] = []
    calibration_paired: list[pd.DataFrame] = []
    calibration_details: dict[str, Any] = {}
    for offset, (name, policy) in enumerate(cal_policies.items()):
        outcomes = cal_current if name == CURRENT_ROUTER else simulate(
            calibration_entries, bars, funding, calibration_switches,
            policy_name=name, policy=policy, days=14, slippage=5.0,
        )
        detail, _, _, paired = HOLD.route_metrics(
            cal_current, outcomes, bootstrap_samples=args.bootstrap_samples, seed=20260790 + offset * 20,
        )
        calibration_details[name] = detail
        calibration_records.append(selection_record(name, detail))
        calibration_paired.append(paired.assign(period="calibration", policy=name))
    calibration_frame = pd.DataFrame(calibration_records)
    eligible = calibration_frame[
        (calibration_frame["policy"] != CURRENT_ROUTER) & calibration_frame["selection_eligible"]
    ].sort_values(["paired_increment_mean", "expectancy"], ascending=False)
    selected = str(eligible.iloc[0]["policy"]) if len(eligible) else CURRENT_ROUTER

    # A secondary maturity check: only the first two calibration windows can
    # support a 30-day path before the already-seen confirmation era begins.
    early_entries = calibration_entries[calibration_entries["validation_split"].isin(["cal_1", "cal_2"])].copy()
    early_keys = set(early_entries["signal_key"].astype(int))
    early_switches = {key: value for key, value in calibration_switches.items() if key in early_keys}
    sensitivity_policies = policy_catalog(30)
    sensitivity_current = simulate(
        early_entries, bars, funding, early_switches,
        policy_name=CURRENT_ROUTER, policy=sensitivity_policies[CURRENT_ROUTER], days=30, slippage=5.0,
    )
    sensitivity_records: list[dict[str, Any]] = []
    for offset, (name, policy) in enumerate(sensitivity_policies.items()):
        outcomes = sensitivity_current if name == CURRENT_ROUTER else simulate(
            early_entries, bars, funding, early_switches,
            policy_name=name, policy=policy, days=30, slippage=5.0,
        )
        detail, _, _, _ = HOLD.route_metrics(
            sensitivity_current, outcomes, bootstrap_samples=args.bootstrap_samples,
            seed=20261000 + offset * 20,
        )
        record = selection_record(name, detail)
        record["mature_splits"] = "cal_1|cal_2"
        sensitivity_records.append(record)
    sensitivity_frame = pd.DataFrame(sensitivity_records)

    conf_policies = policy_catalog(30)
    confirmation_summaries: list[dict[str, Any]] = []
    confirmation_paired: list[pd.DataFrame] = []
    confirmation_trades: list[pd.DataFrame] = []
    confirmation_details: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        current = simulate(
            confirmation_entries, bars, funding, confirmation_switches,
            policy_name=CURRENT_ROUTER, policy=conf_policies[CURRENT_ROUTER], days=30, slippage=slippage,
        )
        for offset, (name, policy) in enumerate(conf_policies.items()):
            outcomes = current if name == CURRENT_ROUTER else simulate(
                confirmation_entries, bars, funding, confirmation_switches,
                policy_name=name, policy=policy, days=30, slippage=slippage,
            )
            if slippage == 5.0:
                detail, trades, _, paired = HOLD.route_metrics(
                    current, outcomes, bootstrap_samples=args.bootstrap_samples,
                    seed=20261200 + offset * 20,
                )
                confirmation_details[name] = detail
                confirmation_paired.append(paired.assign(period="confirmation", policy=name))
            else:
                trades, _, summary = TIMING.portfolio(outcomes)
                detail = {"strategy_summary": summary}
            summary = dict(detail["strategy_summary"])
            summary.update({"policy": name, "slippage_bps_each_side": slippage})
            confirmation_summaries.append(summary)
            if len(trades):
                confirmation_trades.append(trades.assign(policy=name, slippage_bps_each_side=slippage))
    confirmation_frame = pd.DataFrame(confirmation_summaries)
    default_confirmation = confirmation_frame[confirmation_frame["slippage_bps_each_side"] == 5.0]
    historical_best = str(default_confirmation.sort_values("expectancy", ascending=False).iloc[0]["policy"])
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "posthoc_family_calibration_selected_confirmation_audited_no_promotion",
        "current_router": CURRENT_ROUTER,
        "pre_registered_candidates": [name for name in cal_policies if name != CURRENT_ROUTER],
        "calibration_selected_policy": selected,
        "calibration_candidate_pass_count": int(len(eligible)),
        "calibration_gate_pass": bool(len(eligible)),
        "early_30d_sensitivity_all_candidate_expectancies_nonpositive": bool(
            (sensitivity_frame[sensitivity_frame["policy"] != CURRENT_ROUTER]["expectancy"] <= 0).all()
        ),
        "early_30d_sensitivity_best_paired_increment": float(
            sensitivity_frame[sensitivity_frame["policy"] != CURRENT_ROUTER]["paired_increment_mean"].max()
        ),
        "confirmation_point_estimate_best_policy": historical_best,
        "confirmation_best_is_posthoc_only": True,
        "historical_promotion_allowed": False,
        "forward_change_allowed": False,
        "classification": "reject_full_runner_retain_current_partial_exit",
        "automatic_trading_allowed": False,
        "calibration_details": calibration_details,
        "confirmation_details": confirmation_details,
    }
    calibration_frame.to_csv(report / "utility_breakout_runner_calibration.csv", index=False)
    sensitivity_frame.to_csv(report / "utility_breakout_runner_30d_calibration_sensitivity.csv", index=False)
    confirmation_frame.to_csv(report / "utility_breakout_runner_confirmation.csv", index=False)
    pd.concat(calibration_paired, ignore_index=True).to_csv(
        report / "utility_breakout_runner_calibration_paired.csv", index=False
    )
    pd.concat(confirmation_paired, ignore_index=True).to_csv(
        report / "utility_breakout_runner_confirmation_paired.csv", index=False
    )
    pd.concat(confirmation_trades, ignore_index=True).to_csv(
        report / "utility_breakout_runner_confirmation_trades.csv.gz", index=False, compression="gzip"
    )
    (report / "utility_breakout_runner_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready({
        "calibration_gate_pass": decision["calibration_gate_pass"],
        "calibration_selected_policy": selected,
        "early_30d_sensitivity_all_candidate_expectancies_nonpositive": decision[
            "early_30d_sensitivity_all_candidate_expectancies_nonpositive"
        ],
        "confirmation_point_estimate_best_policy": historical_best,
        "classification": decision["classification"],
    }), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
