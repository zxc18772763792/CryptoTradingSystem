"""Independently verify Binance entry timing, path, threshold, and stop studies."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def profit_factor(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def close(left: Any, right: Any) -> bool:
    return bool(np.isclose(float(left), float(right), atol=1e-12, rtol=1e-10))


def verify_entry_exit(report: Path, baseline: Path) -> dict[str, Any]:
    decision = json.loads((report / "entry_exit_timing_decision.json").read_text(encoding="utf-8"))
    entries = pd.read_csv(report / "entry_timing_candidates.csv.gz", compression="gzip")
    calibration = pd.read_csv(report / "entry_timing_calibration.csv")
    exit_calibration = pd.read_csv(report / "stateful_exit_calibration.csv")
    trades = pd.read_csv(report / "entry_exit_confirmation_trades.csv.gz", compression="gzip")
    eligible_entry = calibration[calibration["selection_eligible"]]
    expected_entry = str(eligible_entry.iloc[0]["entry_rule"]) if not eligible_entry.empty else "next_open"
    eligible_exit = exit_calibration[exit_calibration["selection_eligible"]]
    expected_exit = str(eligible_exit.iloc[0]["exit_policy"]) if not eligible_exit.empty else "frozen_half100_trail25"
    next_open = entries[(entries["entry_rule"] == "next_open") & (entries["fold"] >= 3)]
    selected = entries[(entries["entry_rule"] == expected_entry) & (entries["fold"] >= 3)]
    default = trades[trades["slippage_bps_each_side"] == 5.0]
    returns = pd.to_numeric(default["net_return"], errors="coerce")
    checks = {
        "entry_selection": decision["selected_entry_rule"] == expected_entry,
        "exit_selection": decision["selected_exit_policy"] == expected_exit,
        "next_open_delay_zero": close(next_open["entry_delay_hours"].median(), 0.0),
        "one_bar_wait_delay_four": close(
            entries.loc[(entries["entry_rule"] == "one_bar_wait") & (entries["fold"] >= 3), "entry_delay_hours"].median(),
            4.0,
        ),
        "recognition_signal_count": int(len(selected)) == int(decision["confirmation_recognition"]["selected_entry"]["signals"]),
        "recognition_positive_count": int(selected["target200_14d_from_entry"].sum())
        == int(decision["confirmation_recognition"]["selected_entry"]["actual_200pct_entries"]),
        "trade_count": int(len(default)) == int(decision["default_cost_summary"]["trades"]),
        "trade_expectancy": close(returns.mean(), decision["default_cost_summary"]["expectancy"]),
        "trade_profit_factor": close(profit_factor(returns), decision["default_cost_summary"]["profit_factor"]),
        "failed_gate_not_promoted": bool(
            not decision["paper_trade_gate_pass"] and decision["classification"] == "watchlist_only"
        ),
        "source_score_hash": file_sha256(report / "historical_walk_forward_scored.csv.gz")
        == decision["source_hashes"]["historical_walk_forward_scored.csv.gz"],
        "source_panel_hash": file_sha256(baseline / "futures_4h_panel.csv.gz")
        == decision["source_hashes"]["futures_4h_panel.csv.gz"],
    }
    return {"passed": all(checks.values()), "checks": checks}


def verify_microstructure(report: Path) -> dict[str, Any]:
    decision = json.loads((report / "microstructure_confirmation_decision.json").read_text(encoding="utf-8"))
    scored = pd.read_csv(report / "microstructure_confirmation_scored.csv.gz", compression="gzip")
    labels = scored["target200_14d_from_entry"].astype(int)
    selected = scored[scored["micro_selected"]]
    checks = {
        "row_count": int(len(scored)) == int(decision["pooled_metrics"]["rows"]),
        "positive_count": int(labels.sum()) == int(decision["pooled_metrics"]["positives"]),
        "price_ap": close(average_precision_score(labels, scored["price_only_score"]), decision["pooled_metrics"]["price_average_precision"]),
        "micro_ap": close(average_precision_score(labels, scored["micro_combo_score"]), decision["pooled_metrics"]["micro_average_precision"]),
        "selected_precision": close(selected["target200_14d_from_entry"].mean(), decision["pooled_metrics"]["selected_precision"]),
        "no_fold_precision_improvement": bool(
            not pd.read_csv(report / "microstructure_confirmation_folds.csv")["precision_improved"].astype(bool).any()
        ),
        "failed_gate_not_promoted": bool(
            not decision["ranking_gate_pass"] and not decision["paper_trade_gate_pass"]
            and decision["classification"] == "watchlist_only"
        ),
    }
    return {"passed": all(checks.values()), "checks": checks}


def verify_thresholds(report: Path) -> dict[str, Any]:
    decision = json.loads((report / "rank_threshold_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(report / "rank_threshold_calibration.csv")
    entries = pd.read_csv(report / "rank_threshold_entries.csv.gz", compression="gzip")
    trades = pd.read_csv(report / "rank_threshold_confirmation_trades.csv.gz", compression="gzip")
    eligible = calibration[calibration["selection_eligible"]]
    expected = float(eligible.iloc[0]["rank_threshold"]) if not eligible.empty else 0.98
    selected = entries[(np.isclose(entries["rank_threshold"], expected)) & (entries["fold"] >= 3)]
    baseline = entries[(np.isclose(entries["rank_threshold"], 0.98)) & (entries["fold"] >= 3)]
    selected_trades = trades[np.isclose(trades["rank_threshold"], expected)]
    baseline_trades = trades[np.isclose(trades["rank_threshold"], 0.98)]
    checks = {
        "selection": close(decision["selected_threshold"], expected),
        "selected_signals": int(len(selected)) == int(decision["confirmation_selected"]["signals"]),
        "selected_positives": int(selected["target200_14d_from_entry"].sum())
        == int(decision["confirmation_selected"]["actual_200pct_entries"]),
        "baseline_signals": int(len(baseline)) == int(decision["confirmation_baseline_98pct"]["signals"]),
        "selected_expectancy": close(selected_trades["net_return"].mean(), decision["confirmation_selected"]["expectancy"]),
        "selected_profit_factor": close(profit_factor(selected_trades["net_return"]), decision["confirmation_selected"]["profit_factor"]),
        "baseline_expectancy": close(baseline_trades["net_return"].mean(), decision["confirmation_baseline_98pct"]["expectancy"]),
        "not_promoted": bool(
            not decision["confirmation_improved"] and decision["classification"] == "no_threshold_upgrade"
        ),
    }
    return {"passed": all(checks.values()), "checks": checks}


def verify_stops(report: Path) -> dict[str, Any]:
    decision = json.loads((report / "stop_width_decision.json").read_text(encoding="utf-8"))
    calibration = pd.read_csv(report / "stop_width_calibration.csv")
    trades = pd.read_csv(report / "stop_width_confirmation_trades.csv.gz", compression="gzip")
    eligible = calibration[calibration["selection_eligible"]]
    expected = float(eligible.iloc[0]["hard_stop_pct"]) if not eligible.empty else 0.20
    selected = trades[np.isclose(trades["hard_stop_pct"], expected)]
    width15 = trades[np.isclose(trades["hard_stop_pct"], 0.15)]
    width30 = trades[np.isclose(trades["hard_stop_pct"], 0.30)]
    checks = {
        "selection": close(decision["selected_hard_stop_pct"], expected),
        "selected_trade_count": int(len(selected)) == int(decision["confirmation_selected"]["trades"]),
        "selected_expectancy": close(selected["net_return"].mean(), decision["confirmation_selected"]["expectancy"]),
        "selected_profit_factor": close(profit_factor(selected["net_return"]), decision["confirmation_selected"]["profit_factor"]),
        "risk_equal_sizing": bool(width15["notional_usd"].median() > width30["notional_usd"].median()),
        "not_promoted": bool(
            not decision["confirmation_improved"] and decision["classification"] == "no_stop_upgrade"
        ),
    }
    return {"passed": all(checks.values()), "checks": checks}


def verify_path_timing(report: Path) -> dict[str, Any]:
    decision = json.loads((report / "path_timing_decision.json").read_text(encoding="utf-8"))
    paths = pd.read_csv(report / "path_timing_diagnostics.csv.gz", compression="gzip")
    winners = paths[paths["target200_14d"].astype(bool)]
    losers = paths[~paths["target200_14d"].astype(bool)]
    checks = {
        "signal_count": int(len(paths)) == int(decision["signals"]),
        "winner_count": int(len(winners)) == int(decision["actual_200pct_signals"]),
        "median_mfe_1d": close(winners["mfe_1d"].median(), decision["winner_launch_speed"]["median_mfe_1d"]),
        "median_hours_50": close(pd.to_numeric(winners["hit_50pct_hours"], errors="coerce").median(), decision["winner_launch_speed"]["median_hours_to_50pct"]),
        "winner_stop_before_50": close(winners["stop20_before_50pct"].astype(bool).mean(), decision["stop_path_risk"]["winner_share_stop20_before_50pct"]),
        "loser_stop_share": close(losers["hit_stop20_hours"].notna().mean(), decision["stop_path_risk"]["loser_share_hit_stop20_within_14d"]),
    }
    return {"passed": all(checks.values()), "checks": checks}


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    sections = {
        "entry_exit": verify_entry_exit(report, baseline),
        "microstructure": verify_microstructure(report),
        "rank_thresholds": verify_thresholds(report),
        "stop_widths": verify_stops(report),
        "path_timing": verify_path_timing(report),
    }
    payload = {
        "passed": all(section["passed"] for section in sections.values()),
        "sections": sections,
    }
    (report / "entry_exit_timing_verification.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not payload["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
