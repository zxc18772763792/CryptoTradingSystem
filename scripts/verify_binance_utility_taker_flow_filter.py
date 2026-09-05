"""Independently verify the 8h taker-buy-share prospective diagnostic."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
ANALYSIS_PATH = ROOT / "scripts" / "analyze_binance_utility_taker_flow_filter.py"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ANALYSIS = _load("binance_utility_taker_flow_filter_verifier", ANALYSIS_PATH)


def close(left: Any, right: Any, tolerance: float = 1e-10) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=0.0, atol=tolerance))


def event_rows(rows: pd.DataFrame) -> pd.DataFrame:
    kept: list[int] = []
    for _, group in rows.sort_values(["symbol", "decision_time", "signal_key"]).groupby("symbol", sort=False):
        start: pd.Timestamp | None = None
        for index, row in group.iterrows():
            stamp = pd.Timestamp(row["decision_time"])
            if start is None or stamp > start + pd.Timedelta(days=14):
                kept.append(int(index))
                start = stamp
    return rows.loc[kept].copy()


def profit_factor(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def main() -> None:
    decision = json.loads((REPORT / "utility_taker_flow_decision.json").read_text(encoding="utf-8"))
    confirmation = pd.read_csv(REPORT / "continuation_utility_confirmation_scored.csv.gz", compression="gzip")
    for column in ["date", "decision_time", "baseline_entry_time"]:
        confirmation[column] = pd.to_datetime(confirmation[column], utc=True)
    confirmation = confirmation[confirmation["selected"].astype(bool)].copy()
    confirmation["taker_flow_supportive"] = (
        pd.to_numeric(confirmation["early_taker_buy_share"], errors="coerce") >= 0.50
    )
    chosen = confirmation[confirmation["taker_flow_supportive"]]
    events = event_rows(confirmation)
    chosen_events = events[events["taker_flow_supportive"]]
    reported = decision["confirmation"]

    candidates = pd.read_csv(REPORT / "continuation_entry_candidates.csv.gz", compression="gzip")
    for column in ["date", "decision_time", "baseline_entry_time"]:
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    frame = candidates[candidates["checkpoint_hours"] == 8].copy()
    calibration, _ = ANALYSIS.CONTINUATION.calibration_scores(
        frame,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=0.70,
    )
    calibration = calibration[calibration["selected"]].copy()
    calibration["taker_flow_supportive"] = (
        pd.to_numeric(calibration["early_taker_buy_share"], errors="coerce") >= 0.50
    )
    cal_chosen = calibration[calibration["taker_flow_supportive"]]
    reported_cal = decision["calibration"]

    summaries = pd.read_csv(REPORT / "utility_taker_flow_strategy_summary.csv")
    default = summaries[summaries["slippage_bps_each_side"] == 5.0].set_index("variant")
    trades = pd.read_csv(REPORT / "utility_taker_flow_strategy_trades.csv.gz", compression="gzip")
    folds = pd.read_csv(REPORT / "utility_taker_flow_strategy_folds.csv")
    paired = pd.read_csv(REPORT / "utility_taker_flow_confirmation_router_paired.csv")

    strategy_recomputed = True
    for variant, group in trades.groupby("variant"):
        row = default.loc[variant]
        strategy_recomputed &= int(row["trades"]) == len(group)
        strategy_recomputed &= close(row["expectancy"], group["net_return"].mean())
        strategy_recomputed &= close(row["profit_factor"], profit_factor(group["net_return"]))

    checks = {
        "semantic_threshold_and_clock_are_fixed": bool(
            close(decision["threshold"], 0.50)
            and decision["filter_clock"] == "two completed 4h bars after P0; no additional entry delay"
            and decision["filter_name"] == "taker_buy_share_ge_50pct_at_8h"
        ),
        "calibration_counts_and_precision_recomputed": bool(
            len(calibration) == reported_cal["rows"] == 42
            and int(calibration["late_target200"].sum()) == reported_cal["positives"] == 3
            and len(cal_chosen) == reported_cal["selected_rows"] == 21
            and int(cal_chosen["late_target200"].sum()) == reported_cal["selected_positives"] == 3
            and close(cal_chosen["late_target200"].mean(), reported_cal["selected_precision"])
        ),
        "confirmation_counts_and_precision_recomputed": bool(
            len(confirmation) == reported["rows"] == 88
            and int(confirmation["late_target200"].sum()) == reported["positives"] == 10
            and len(chosen) == reported["selected_rows"] == 48
            and int(chosen["late_target200"].sum()) == reported["selected_positives"] == 9
            and close(chosen["late_target200"].mean(), reported["selected_precision"])
            and close(chosen["utility_return_14d"].mean(), reported["selected_utility_expectancy_14d"])
        ),
        "independent_events_recomputed": bool(
            len(events) == reported["independent_events"] == 67
            and int(events["late_target200"].sum()) == reported["positive_independent_events"] == 5
            and len(chosen_events) == reported["selected_independent_events"] == 33
            and int(chosen_events["late_target200"].sum()) == reported["selected_positive_independent_events"] == 4
        ),
        "row_gate_passes_but_event_gate_fails": bool(
            decision["confirmation_row_gate_pass"] is True
            and reported["week_bootstrap"]["precision_delta_lower_95pct"] > 0
            and reported["symbol_bootstrap"]["precision_delta_lower_95pct"] > 0
            and decision["confirmation_event_gate_pass"] is False
            and reported["event_week_bootstrap"]["precision_delta_lower_95pct"] < 0
            and reported["event_symbol_bootstrap"]["precision_delta_lower_95pct"] < 0
        ),
        "strategy_metrics_recomputed": bool(strategy_recomputed),
        "filtered_strategy_has_all_four_folds": bool(
            set(folds.loc[folds["variant"] == "taker50_frozen", "fold"].astype(int)) == {3, 4, 5, 6}
            and set(folds.loc[folds["variant"] == "taker50_breakout_router", "fold"].astype(int)) == {3, 4, 5, 6}
        ),
        "filtered_router_pair_recomputed": bool(
            len(paired) == 48
            and close(paired["delta_return"].mean(), decision["confirmation_filtered_router_detail"]["paired_increment_mean_all_signals"])
        ),
        "no_historical_promotion": bool(
            decision["historical_promotion_allowed"] is False
            and decision["confirmation_full_gate_pass"] is False
            and decision["classification"] == "prospective_identification_annotation_pending_true_oos"
        ),
        "forward_annotation_is_rank_and_gate_neutral": bool(
            decision["forward_annotation_added"] is True
            and decision["forward_annotation_changes_rank"] is False
            and decision["forward_annotation_changes_stage_veto"] is False
            and decision["forward_annotation_changes_paper_eligibility"] is False
            and decision["primary_entry_changed"] is False
            and decision["primary_exit_changed"] is False
            and decision["automatic_trading_allowed"] is False
        ),
    }
    errors = [name for name, passed in checks.items() if not passed]
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "selected_rows": int(len(chosen)),
            "selected_positives": int(chosen["late_target200"].sum()),
            "selected_precision": float(chosen["late_target200"].mean()),
            "selected_events": int(len(chosen_events)),
            "selected_positive_events": int(chosen_events["late_target200"].sum()),
            "taker50_frozen_expectancy": float(default.loc["taker50_frozen", "expectancy"]),
            "taker50_breakout_router_expectancy": float(default.loc["taker50_breakout_router", "expectancy"]),
        },
    }
    (REPORT / "utility_taker_flow_verification.json").write_text(
        json.dumps(ANALYSIS.VALIDATION.json_ready(result), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(ANALYSIS.VALIDATION.json_ready(result), ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
