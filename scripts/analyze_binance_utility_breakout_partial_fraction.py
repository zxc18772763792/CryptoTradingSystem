"""Audit 25%, 50%, and 75% partial sales inside the fixed breakout router.

Only the fraction sold at the already-fixed +150% target changes.  The 8h
entry, breakout clock, downside stop, 30% trailing distance, 30-day maximum,
and volume-wick exit remain identical.  Calibration chooses the fraction;
confirmation cannot rescue a rejected choice.  Because the parent router is
posthoc, even a passing result remains prospective-only.
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
BASELINE_FRACTION = 0.50


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


RUNNER = _load(
    "binance_utility_breakout_runner_for_partial_fraction",
    ROOT / "scripts" / "analyze_binance_utility_breakout_runner.py",
)
HOLD = RUNNER.HOLD
TIMING = RUNNER.TIMING
VALIDATION = RUNNER.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def policy_catalog(days: int) -> dict[str, dict[str, Any]]:
    current = HOLD.routed_policies(days)["breakout6_half150_trail30_wick"]
    result: dict[str, dict[str, Any]] = {}
    for fraction in (0.25, 0.50, 0.75):
        policy = dict(current)
        name = f"breakout6_sell{int(fraction * 100)}_at150_trail30_wick"
        policy.update({"family": name, "partial_fraction": fraction})
        result[name] = policy
    return result


def evaluate_period(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    switches: Mapping[int, pd.Timestamp],
    *,
    days: int,
    slippage: float,
    bootstrap_samples: int,
    seed: int,
) -> tuple[pd.DataFrame, dict[str, Any], list[pd.DataFrame], list[pd.DataFrame]]:
    policies = policy_catalog(days)
    baseline_name = "breakout6_sell50_at150_trail30_wick"
    baseline = RUNNER.simulate(
        entries, bars, funding, switches, policy_name=baseline_name,
        policy=policies[baseline_name], days=days, slippage=slippage,
    )
    records: list[dict[str, Any]] = []
    details: dict[str, Any] = {}
    paired_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    for offset, (name, policy) in enumerate(policies.items()):
        outcomes = baseline if name == baseline_name else RUNNER.simulate(
            entries, bars, funding, switches, policy_name=name,
            policy=policy, days=days, slippage=slippage,
        )
        detail, trades, _, paired = HOLD.route_metrics(
            baseline, outcomes, bootstrap_samples=bootstrap_samples, seed=seed + offset * 20,
        )
        record = RUNNER.selection_record(name, detail)
        split_delta = paired.groupby("validation_split", sort=True)["delta_return"].mean()
        record["positive_split_share"] = float((split_delta > 0).mean()) if len(split_delta) else 0.0
        record["split_increment_means"] = "|".join(
            f"{key}:{value:.12g}" for key, value in split_delta.items()
        )
        record["selection_eligible"] = bool(
            record["trades"] >= 30
            and record["expectancy"] > 0
            and record["profit_factor"] > 1
            and record["leave_largest_winner_out_expectancy"] is not None
            and record["leave_largest_winner_out_expectancy"] > 0
            and record["largest_positive_pnl_share"] is not None
            and record["largest_positive_pnl_share"] <= 0.35
            and len(split_delta) >= 3
            and float((split_delta > 0).mean()) >= 2 / 3
            and record["paired_increment_mean"] > 0
        )
        record["partial_fraction"] = float(policy["partial_fraction"])
        record["slippage_bps_each_side"] = slippage
        records.append(record)
        details[name] = detail
        paired_frames.append(paired.assign(policy=name, partial_fraction=policy["partial_fraction"]))
        if len(trades):
            trade_frames.append(trades.assign(policy=name, partial_fraction=policy["partial_fraction"], slippage_bps_each_side=slippage))
    return pd.DataFrame(records), details, paired_frames, trade_frames


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    entries = RUNNER.load_timing_rows(report)
    bars, funding = RUNNER.market_data(args.baseline_dir.resolve())
    calibration_entries, calibration_switches = RUNNER.period_rows(entries, "calibration")
    confirmation_entries, confirmation_switches = RUNNER.period_rows(entries, "confirmation")

    calibration, calibration_details, cal_pairs, _ = evaluate_period(
        calibration_entries, bars, funding, calibration_switches,
        days=14, slippage=5.0, bootstrap_samples=args.bootstrap_samples, seed=20261600,
    )
    eligible = calibration[calibration["selection_eligible"]].sort_values(
        ["paired_increment_mean", "expectancy"], ascending=False
    )
    selected_fraction = float(eligible.iloc[0]["partial_fraction"]) if len(eligible) else BASELINE_FRACTION

    confirmation_frames: list[pd.DataFrame] = []
    confirmation_pairs: list[pd.DataFrame] = []
    confirmation_trades: list[pd.DataFrame] = []
    confirmation_details: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        frame, details, pairs, trades = evaluate_period(
            confirmation_entries, bars, funding, confirmation_switches,
            days=30, slippage=slippage, bootstrap_samples=args.bootstrap_samples,
            seed=20261800 + int(slippage) * 20,
        )
        confirmation_frames.append(frame)
        confirmation_pairs.extend([item.assign(slippage_bps_each_side=slippage) for item in pairs])
        confirmation_trades.extend(trades)
        if slippage == 5.0:
            confirmation_details = details
    confirmation = pd.concat(confirmation_frames, ignore_index=True)
    default = confirmation[confirmation["slippage_bps_each_side"] == 5.0]
    confirmation_best = float(default.sort_values("expectancy", ascending=False).iloc[0]["partial_fraction"])
    selected_confirmation = default[default["partial_fraction"] == selected_fraction].iloc[0]
    confirmation_gate = bool(
        selected_fraction != BASELINE_FRACTION
        and selected_confirmation["paired_increment_mean"] > 0
        and selected_confirmation["positive_split_share"] >= 0.50
    )
    retained_fraction = selected_fraction if confirmation_gate else BASELINE_FRACTION
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "parent_router_posthoc_calibration_fraction_audit_prospective_only",
        "fixed_controls": {
            "entry": "frozen_8h_utility_next_open",
            "breakout_clock": "completed_breakout6_then_following_4h_open",
            "partial_target": 1.50,
            "trail_activation": 1.50,
            "trail_distance": 0.30,
            "wick_exit": "volume_backed_upper_wick_after_100pct",
        },
        "fractions_tested": [0.25, 0.50, 0.75],
        "calibration_selected_fraction": selected_fraction,
        "calibration_gate_pass": bool(len(eligible) and selected_fraction != BASELINE_FRACTION),
        "confirmation_point_estimate_best_fraction": confirmation_best,
        "confirmation_selected_fraction_paired_increment": float(selected_confirmation["paired_increment_mean"]),
        "confirmation_selected_fraction_positive_split_share": float(selected_confirmation["positive_split_share"]),
        "confirmation_gate_pass": confirmation_gate,
        "retained_fraction": retained_fraction,
        "historical_promotion_allowed": False,
        "forward_change_allowed": False,
        "classification": (
            "calibration_fraction_reversed_in_confirmation_retain_sell50"
            if selected_fraction != BASELINE_FRACTION and not confirmation_gate
            else "retain_sell50_partial_fraction"
        ),
        "automatic_trading_allowed": False,
        "calibration_details": calibration_details,
        "confirmation_details_default_cost": confirmation_details,
    }
    calibration.to_csv(report / "utility_breakout_partial_fraction_calibration.csv", index=False)
    confirmation.to_csv(report / "utility_breakout_partial_fraction_confirmation.csv", index=False)
    pd.concat(cal_pairs, ignore_index=True).to_csv(
        report / "utility_breakout_partial_fraction_calibration_paired.csv", index=False
    )
    pd.concat(confirmation_pairs, ignore_index=True).to_csv(
        report / "utility_breakout_partial_fraction_confirmation_paired.csv", index=False
    )
    pd.concat(confirmation_trades, ignore_index=True).to_csv(
        report / "utility_breakout_partial_fraction_trades.csv.gz", index=False, compression="gzip"
    )
    (report / "utility_breakout_partial_fraction_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready({
        "calibration_selected_fraction": selected_fraction,
        "calibration_gate_pass": decision["calibration_gate_pass"],
        "confirmation_point_estimate_best_fraction": confirmation_best,
        "retained_fraction": retained_fraction,
        "classification": decision["classification"],
    }), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
