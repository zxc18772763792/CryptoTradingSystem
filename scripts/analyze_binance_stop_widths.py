"""Calibrate risk-equal hard-stop widths for frozen Binance run-up entries."""

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
WIDTHS = (0.15, 0.20, 0.25, 0.30)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


STUDY = _load(
    "binance_entry_timing_for_stop_widths",
    ROOT / "scripts" / "analyze_binance_entry_exit_timing.py",
)
ENTRY = STUDY.ENTRY
VALIDATION = STUDY.VALIDATION
EXIT = STUDY.EXIT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    return parser.parse_args()


def summarize_width(trades: pd.DataFrame) -> dict[str, Any]:
    returns = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
    folds = trades.groupby("fold")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
    leave_largest, concentration = STUDY.concentration_stats(trades)
    return {
        "trades": int(len(returns)),
        "expectancy": float(returns.mean()) if len(returns) else np.nan,
        "profit_factor": float(STUDY.profit_factor(returns)),
        "win_rate": float((returns > 0).mean()) if len(returns) else np.nan,
        "folds": int(folds.size),
        "fold_expectancy_mean": float(folds.mean()) if len(folds) else np.nan,
        "fold_expectancy_std": float(folds.std(ddof=0)) if len(folds) else np.nan,
        "selection_score": float(folds.mean() - 0.5 * folds.std(ddof=0)) if len(folds) else -np.inf,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "hard_stop_rate": float((trades["exit_reason"] == "hard_stop").mean()) if len(trades) else np.nan,
        "median_notional_usd": float(trades["notional_usd"].median()) if len(trades) else np.nan,
    }


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    entries = pd.read_csv(report / "entry_timing_candidates.csv.gz", compression="gzip")
    for column in ["date", "entry_time", "baseline_entry_time", "trigger_time"]:
        entries[column] = pd.to_datetime(entries[column], utc=True)
    entries = entries[entries["entry_rule"] == "next_open"].copy()
    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars_by_symbol = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(ROOT / "data" / "research" / "ambush_modes")
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])
    base_policy = ENTRY.build_exit_policy_catalog()["frozen_half100_trail25"]

    calibration_rows: list[dict[str, Any]] = []
    confirmation_rows: list[dict[str, Any]] = []
    confirmation_trades_by_width: dict[float, pd.DataFrame] = {}
    confirmation_trade_records: list[dict[str, Any]] = []
    for width in WIDTHS:
        policy = dict(base_policy)
        policy["hard_stop"] = width
        all_outcomes = STUDY.simulate_entries(
            entries,
            bars_by_symbol,
            funding,
            policy_name=f"stop{int(width * 100)}_half100_trail25",
            policy=policy,
            slippage_bps=5.0,
            include_marks=False,
        )
        accepted, _, _ = EXIT.summarize_portfolio(all_outcomes, risk_stop_pct=width)
        all_trades = pd.DataFrame([EXIT.clean_trade(row) for row in accepted])
        calibration = all_trades[pd.to_datetime(all_trades["exit_time"], utc=True) < cutoff].copy()
        calibration_summary = {"hard_stop_pct": width, **summarize_width(calibration)}
        calibration_summary["selection_eligible"] = bool(
            calibration_summary["trades"] >= 40 and calibration_summary["folds"] >= 2
            and calibration_summary["expectancy"] > 0 and calibration_summary["profit_factor"] > 1
            and calibration_summary["leave_largest_winner_out_expectancy"] is not None
            and calibration_summary["leave_largest_winner_out_expectancy"] > 0
            and calibration_summary["largest_positive_pnl_share"] is not None
            and calibration_summary["largest_positive_pnl_share"] <= 0.25
        )
        calibration_rows.append(calibration_summary)

        confirm_entries = entries[entries["fold"] >= 3]
        confirm_outcomes = STUDY.simulate_entries(
            confirm_entries,
            bars_by_symbol,
            funding,
            policy_name=f"stop{int(width * 100)}_half100_trail25",
            policy=policy,
            slippage_bps=5.0,
            include_marks=True,
        )
        confirm_accepted, equity, portfolio_summary = EXIT.summarize_portfolio(
            confirm_outcomes, risk_stop_pct=width
        )
        confirm_trades = pd.DataFrame([EXIT.clean_trade(row) for row in confirm_accepted])
        confirmation_trades_by_width[width] = confirm_trades
        if not confirm_trades.empty:
            confirm_trades["hard_stop_pct"] = width
            confirmation_trade_records.extend(confirm_trades.to_dict(orient="records"))
        confirmation_rows.append(
            {
                "hard_stop_pct": width,
                **summarize_width(confirm_trades),
                "max_drawdown": portfolio_summary.get("max_drawdown"),
                "total_return_on_initial_equity": portfolio_summary.get("total_return_on_initial_equity"),
            }
        )

    calibration_frame = pd.DataFrame(calibration_rows).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    )
    eligible = calibration_frame[calibration_frame["selection_eligible"]]
    selected_width = float(eligible.iloc[0]["hard_stop_pct"]) if not eligible.empty else 0.20
    confirmation_frame = pd.DataFrame(confirmation_rows)
    selected_confirmation = confirmation_frame[confirmation_frame["hard_stop_pct"] == selected_width].iloc[0]
    baseline_confirmation = confirmation_frame[confirmation_frame["hard_stop_pct"] == 0.20].iloc[0]
    selected_trades = confirmation_trades_by_width[selected_width]
    selected_folds = STUDY.fold_metrics(selected_trades)
    week = EXIT.bootstrap_returns(selected_trades, samples=args.bootstrap_samples, cluster="week", seed=20260728)
    symbol = EXIT.bootstrap_returns(selected_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260729)
    fold_majority = float((selected_folds["profit_factor"] > 1).mean()) if not selected_folds.empty else 0.0
    improved = bool(
        selected_width != 0.20
        and selected_confirmation["expectancy"] > baseline_confirmation["expectancy"]
        and selected_confirmation["profit_factor"] > baseline_confirmation["profit_factor"]
        and selected_confirmation["leave_largest_winner_out_expectancy"] > 0
        and fold_majority > 0.5
    )
    calibration_frame.to_csv(report / "stop_width_calibration.csv", index=False)
    confirmation_frame.to_csv(report / "stop_width_confirmation.csv", index=False)
    pd.DataFrame(confirmation_trade_records).to_csv(
        report / "stop_width_confirmation_trades.csv.gz", index=False, compression="gzip"
    )
    selected_folds.to_csv(report / "stop_width_selected_folds.csv", index=False)
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "widths_tested": list(WIDTHS),
        "risk_rule": "0.5% equity risk divided by hard-stop width; maximum gross exposure remains 50%",
        "selection_information_cutoff_utc": cutoff,
        "selected_hard_stop_pct": selected_width,
        "selection_eligible": bool(not eligible.empty),
        "confirmation_selected": selected_confirmation.to_dict(),
        "confirmation_baseline_stop20": baseline_confirmation.to_dict(),
        "selected_fold_metrics": selected_folds.to_dict(orient="records"),
        "selected_fold_majority_profit_factor_gt_1": fold_majority,
        "week_block_bootstrap": week,
        "symbol_bootstrap": symbol,
        "confirmation_improved": improved,
        "classification": "directional_candidate" if improved else "no_stop_upgrade",
    }
    (report / "stop_width_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
