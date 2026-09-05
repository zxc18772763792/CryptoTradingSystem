"""Test right-tail profit management for the frozen P0+8h utility entry.

The baseline sells half at +100% and trails the remainder by 25%.  This study
pre-registers a small family that sells only one quarter, delays the partial,
or uses a wider/no-partial trailing exit.  Entry selection is unchanged.
Calibration uses fully mature 14-day paths before fold 3; confirmation restores
the 30-day maximum hold.  The module is research-only.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

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


FAILURE = _load(
    "binance_utility_failure_exit_for_tail_exit",
    ROOT / "scripts" / "analyze_binance_utility_failure_exit.py",
)
CONTINUATION = FAILURE.CONTINUATION
ENTRY = FAILURE.ENTRY
EXIT = FAILURE.EXIT
STUDY = FAILURE.STUDY
VALIDATION = FAILURE.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def policies(days: int) -> dict[str, dict[str, Any]]:
    base = CONTINUATION.frozen_policy(days)
    base["family"] = "half100_trail25_baseline"
    result: dict[str, dict[str, Any]] = {"baseline_half100_trail25": base}

    def add(
        name: str,
        *,
        partial_target: float | None,
        partial_fraction: float,
        trail_activation: float,
        trail_distance: float,
    ) -> None:
        policy = dict(base)
        policy.update(
            {
                "family": "right_tail_preservation",
                "partial_target": partial_target,
                "partial_fraction": partial_fraction,
                "trail_activation": trail_activation,
                "trail_distance": trail_distance,
            }
        )
        result[name] = policy

    add("quarter100_trail25", partial_target=1.00, partial_fraction=0.25, trail_activation=1.00, trail_distance=0.25)
    add("quarter100_trail30", partial_target=1.00, partial_fraction=0.25, trail_activation=1.00, trail_distance=0.30)
    add("half150_trail30", partial_target=1.50, partial_fraction=0.50, trail_activation=1.50, trail_distance=0.30)
    add("quarter150_trail30", partial_target=1.50, partial_fraction=0.25, trail_activation=1.50, trail_distance=0.30)
    add("no_partial_trail30_after100", partial_target=None, partial_fraction=0.0, trail_activation=1.00, trail_distance=0.30)
    return result


def calibration_table(
    rows: pd.DataFrame,
    bars: dict[str, pd.DataFrame],
    funding: dict[str, pd.Series],
) -> pd.DataFrame:
    raw = {
        name: FAILURE.simulate_policy(
            rows,
            bars,
            funding,
            policy_name=name,
            policy=policy,
            slippage_bps=5.0,
        )
        for name, policy in policies(14).items()
    }
    baseline = raw["baseline_half100_trail25"]
    records: list[dict[str, Any]] = []
    for name, outcomes in raw.items():
        trades, _, summary = FAILURE.portfolio(outcomes)
        values = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        fold_expectancy = trades.groupby("validation_split")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
        leave_largest, concentration = STUDY.concentration_stats(trades)
        paired = FAILURE.paired_deltas(baseline, outcomes)
        delta_by_split = paired.groupby("validation_split")["delta_return"].mean() if not paired.empty else pd.Series(dtype=float)
        eligible = bool(
            name != "baseline_half100_trail25" and len(values) >= 20 and fold_expectancy.size == 3
            and values.mean() > 0 and CONTINUATION.profit_factor(values) > 1
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.35
            and len(delta_by_split) == 3 and float((delta_by_split > 0).mean()) >= 2 / 3
            and float(delta_by_split.mean()) > 0
        )
        score = float(delta_by_split.mean() - 0.5 * delta_by_split.std(ddof=0)) if len(delta_by_split) else -np.inf
        records.append(
            {
                "policy": name,
                "trades": int(len(values)),
                "expectancy": float(values.mean()) if len(values) else np.nan,
                "profit_factor": float(CONTINUATION.profit_factor(values)),
                "max_drawdown": summary.get("max_drawdown") if summary else np.nan,
                "folds": int(fold_expectancy.size),
                "fold_expectancy_mean": float(fold_expectancy.mean()) if len(fold_expectancy) else np.nan,
                "fold_expectancy_std": float(fold_expectancy.std(ddof=0)) if len(fold_expectancy) else np.nan,
                "paired_rows": int(len(paired)),
                "paired_delta_mean": float(paired["delta_return"].mean()) if len(paired) else np.nan,
                "paired_positive_split_share": float((delta_by_split > 0).mean()) if len(delta_by_split) else 0.0,
                "leave_largest_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_score": score,
                "selection_eligible": eligible,
            }
        )
    return pd.DataFrame(records).sort_values(
        ["selection_eligible", "selection_score", "profit_factor"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def main() -> None:
    args = parse_args()
    baseline_dir = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    candidates = pd.read_csv(report / "continuation_entry_candidates.csv.gz", compression="gzip")
    for column in ("date", "decision_time"):
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    rows = candidates[candidates["checkpoint_hours"] == 8].copy()
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    panel = pd.read_csv(baseline_dir / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)

    calibration_rows, _ = CONTINUATION.calibration_scores(
        rows,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=0.70,
    )
    calibration = calibration_table(calibration_rows, bars, funding)
    calibration.to_csv(report / "utility_tail_exit_calibration.csv", index=False)
    selected = calibration.iloc[0]
    selected_name = str(selected["policy"])
    selected_eligible = bool(selected["selection_eligible"])

    confirmation_rows, _ = CONTINUATION.confirmation_scores(
        rows,
        folds,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=0.70,
    )
    raw_baseline = FAILURE.simulate_policy(
        confirmation_rows,
        bars,
        funding,
        policy_name="baseline_half100_trail25",
        policy=policies(30)["baseline_half100_trail25"],
        slippage_bps=5.0,
    )
    raw_selected_default: list[dict[str, Any]] = []
    summary_records: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = FAILURE.simulate_policy(
            confirmation_rows,
            bars,
            funding,
            policy_name=selected_name,
            policy=policies(30)[selected_name],
            slippage_bps=slippage,
        )
        trades, equity, summary = FAILURE.portfolio(outcomes)
        if not summary:
            continue
        record = dict(summary)
        record.update({"policy": selected_name, "slippage_bps_each_side": slippage})
        summary_records.append(record)
        trades["slippage_bps_each_side"] = slippage
        trade_frames.append(trades)
        if not equity.empty:
            equity["slippage_bps_each_side"] = slippage
            equity_frames.append(equity)
        if slippage == 5.0:
            raw_selected_default = outcomes
            default_trades = trades
            default_summary = summary

    baseline_trades, _, baseline_summary = FAILURE.portfolio(raw_baseline)
    summary_frame = pd.DataFrame(summary_records)
    all_trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    all_equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    trade_folds = STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summary_frame.to_csv(report / "utility_tail_exit_summary.csv", index=False)
    all_trades.to_csv(report / "utility_tail_exit_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(report / "utility_tail_exit_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(report / "utility_tail_exit_folds.csv", index=False)

    paired = FAILURE.paired_deltas(raw_baseline, raw_selected_default)
    paired.to_csv(report / "utility_tail_exit_paired_deltas.csv", index=False)
    week_delta = FAILURE.delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="week", seed=20260826)
    symbol_delta = FAILURE.delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="symbol", seed=20260827)
    week_return = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="week", seed=20260828) if not default_trades.empty else {}
    symbol_return = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260829) if not default_trades.empty else {}
    leave_largest, concentration = STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summary_frame) == 3 and (summary_frame["expectancy"] > 0).all()
        and (summary_frame["profit_factor"] > 1).all()
        and (summary_frame["max_drawdown"] >= -0.30).all()
    )
    incremental_gate = bool(
        selected_name != "baseline_half100_trail25" and selected_eligible
        and default_summary and baseline_summary
        and default_summary.get("expectancy", -1) > baseline_summary.get("expectancy", np.inf)
        and default_summary.get("profit_factor", 0) > baseline_summary.get("profit_factor", np.inf)
        and fold_majority > 0.50 and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week_return.get("expectancy_lower_95pct", -1) > 0
        and symbol_return.get("expectancy_lower_95pct", -1) > 0
        and week_delta.get("mean_delta_lower_95pct", -1) > 0
        and symbol_delta.get("mean_delta_lower_95pct", -1) > 0
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_historical_time_isolated_right_tail_exit_challenger",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "candidate_policies": list(policies(30)),
        "selected_policy": selected_name,
        "calibration_eligible": selected_eligible,
        "baseline_confirmation_summary": baseline_summary,
        "selected_confirmation_summary": default_summary,
        "strategy_stress_costs": summary_frame.to_dict(orient="records"),
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "fold_majority_profit_factor_gt_1": fold_majority,
        "week_return_bootstrap": week_return,
        "symbol_return_bootstrap": symbol_return,
        "week_increment_bootstrap": week_delta,
        "symbol_increment_bootstrap": symbol_delta,
        "paired_increment_mean": float(paired["delta_return"].mean()) if len(paired) else None,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "incremental_exit_gate_pass": incremental_gate,
        "classification": "paper_exit_candidate_pending_true_oos" if incremental_gate else "retain_frozen_exit",
        "automatic_trading_allowed": False,
    }
    (report / "utility_tail_exit_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
