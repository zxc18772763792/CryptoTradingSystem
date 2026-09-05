"""Build the bounded native Data Analytics report payload from validated CSV evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    clean = frame.replace({np.nan: None, np.inf: None, -np.inf: None}).copy()
    for column in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[column]):
            clean[column] = clean[column].astype(str)
    return clean.to_dict(orient="records")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = args.output_dir.resolve()

    summary = json.loads((output / "analysis_summary.json").read_text(encoding="utf-8"))
    folds = pd.read_csv(output / "walk_forward_fold_metrics.csv")
    surface = pd.read_csv(output / "threshold_horizon_surface.csv")
    oi_folds = pd.read_csv(output / "oi_walk_forward_fold_metrics.csv")
    paper = pd.read_csv(output / "paper_strategy_summary.csv")
    events = pd.read_csv(output / "current_30d_runups_verified.csv")
    candidates = pd.read_csv(output / "current_research_watchlist.csv")
    continuation = json.loads((output / "continuation_entry_decision.json").read_text(encoding="utf-8"))
    continuation_folds = pd.read_csv(output / "continuation_utility_strategy_folds.csv")
    intensity = json.loads((output / "extreme_intensity_decision.json").read_text(encoding="utf-8"))
    intensity_folds = pd.read_csv(output / "extreme_intensity_confirmation_folds.csv")
    tail_exit = json.loads((output / "utility_tail_exit_decision.json").read_text(encoding="utf-8"))
    tail_exit_folds = pd.read_csv(output / "utility_tail_exit_folds.csv")
    router = json.loads((output / "utility_intensity_router_decision.json").read_text(encoding="utf-8"))
    router_folds = pd.read_csv(output / "utility_intensity_router_recognition_folds.csv")
    exhaustion = json.loads((output / "utility_exhaustion_exit_decision.json").read_text(encoding="utf-8"))
    exhaustion_folds = pd.read_csv(output / "utility_exhaustion_exit_folds.csv")
    attribution = json.loads((output / "utility_exhaustion_attribution_decision.json").read_text(encoding="utf-8"))
    attribution_folds = pd.read_csv(output / "utility_exhaustion_attribution_folds.csv")
    utility_entry_timing = json.loads((output / "utility_entry_timing_decision.json").read_text(encoding="utf-8"))
    utility_entry_timing_folds = pd.read_csv(output / "utility_entry_timing_confirmation_folds.csv")
    utility_entry_frontier = json.loads((output / "utility_entry_frontier_decision.json").read_text(encoding="utf-8"))
    utility_entry_frontier_summary = pd.read_csv(output / "utility_entry_frontier_summary.csv")
    breakout_hold_router = json.loads((output / "utility_breakout_hold_router_decision.json").read_text(encoding="utf-8"))
    breakout_hold_router_folds = pd.read_csv(output / "utility_breakout_hold_router_folds.csv")
    breakout_add = json.loads((output / "utility_breakout_add_decision.json").read_text(encoding="utf-8"))
    breakout_add_summary = pd.read_csv(output / "utility_breakout_add_summary.csv")
    taker_flow = json.loads((output / "utility_taker_flow_decision.json").read_text(encoding="utf-8"))
    taker_flow_strategy = pd.read_csv(output / "utility_taker_flow_strategy_summary.csv")
    taker_robustness = json.loads((output / "utility_taker_flow_robustness_decision.json").read_text(encoding="utf-8"))
    taker_matches = pd.read_csv(output / "utility_taker_flow_price_matched_permutation.csv")
    breakout_runner = json.loads((output / "utility_breakout_runner_decision.json").read_text(encoding="utf-8"))
    breakout_runner_calibration = pd.read_csv(output / "utility_breakout_runner_calibration.csv")
    breakout_runner_confirmation = pd.read_csv(output / "utility_breakout_runner_confirmation.csv")
    partial_fraction = json.loads((output / "utility_breakout_partial_fraction_decision.json").read_text(encoding="utf-8"))
    partial_fraction_calibration = pd.read_csv(output / "utility_breakout_partial_fraction_calibration.csv")
    partial_fraction_confirmation = pd.read_csv(output / "utility_breakout_partial_fraction_confirmation.csv")
    regime_router = json.loads((output / "utility_regime_partial_router_decision.json").read_text(encoding="utf-8"))
    regime_router_calibration = pd.read_csv(output / "utility_regime_partial_router_calibration.csv")
    regime_router_confirmation = pd.read_csv(output / "utility_regime_partial_router_confirmation.csv")
    operational_audit = json.loads(
        (output / "forward_monitor_operational_audit_2026-07-21.json").read_text(encoding="utf-8")
    )

    fold_data = folds[
        [
            "fold", "test_start", "test_end", "test_rows", "test_positives",
            "price_top_2pct_precision", "price_top_2pct_lift", "price_auc",
            "simple_top_2pct_lift",
        ]
    ].copy()
    sensitivity = surface[
        [
            "horizon_days", "threshold_pct", "rows", "positive_rows", "selected_rows",
            "base_rate", "precision", "lift", "p_value_bh",
        ]
    ].copy()
    oi_chart_rows: list[dict[str, Any]] = []
    for _, row in oi_folds.iterrows():
        for model, column in [("price-only", "price_top_2pct_lift"), ("price+OI", "combo_top_2pct_lift")]:
            oi_chart_rows.append(
                {
                    "fold": int(row["fold"]),
                    "model": model,
                    "top_2pct_lift": row.get(column),
                    "test_rows": row.get("test_rows"),
                    "test_positives": row.get("test_positives"),
                    "delta_average_precision": row.get("delta_average_precision"),
                    "delta_top_2pct_lift": row.get("delta_top_2pct_lift"),
                }
            )
    default_paper = paper[paper["slippage_bps_each_side"] == 5.0].copy()
    paper_table = default_paper[
        [
            "policy", "trades", "expectancy", "win_rate", "profit_factor", "cvar_5pct",
            "mean_mfe", "mean_mae", "max_drawdown", "fold_majority_profit_factor_gt_1",
            "market_state_majority_profit_factor_gt_1", "largest_positive_pnl_share",
            "leave_largest_winner_out_expectancy", "gate_pass",
        ]
    ].copy()
    event_table = events[
        ["symbol", "classification", "close_return", "low_return", "entry_time", "peak_time"]
    ].sort_values("close_return", ascending=False)
    candidate_table = candidates[
        [
            "symbol", "ranking_score_pctile", "research_stage", "oi_annotation",
            "return_1d", "return_7d", "oi_usd", "oi_change_3d",
        ]
    ].sort_values("ranking_score_pctile", ascending=False)
    continuation_fold_data = continuation_folds[
        ["fold", "trades", "expectancy", "win_rate", "profit_factor", "mean_mfe", "mean_mae"]
    ].copy()
    intensity_fold_data = intensity_folds[
        ["fold", "test_rows", "test_positives", "selected_rows", "selected_positives", "price_average_precision", "intensity_average_precision"]
    ].copy()
    tail_exit_fold_data = tail_exit_folds[
        ["fold", "trades", "expectancy", "win_rate", "profit_factor", "mean_mfe", "mean_mae"]
    ].copy()
    router_fold_rows: list[dict[str, Any]] = []
    for _, row in router_folds.iterrows():
        router_fold_rows.extend(
            [
                {"fold": row["validation_split"], "group": "all 8h entries", "precision": row["base_precision"]},
                {"fold": row["validation_split"], "group": "20h high intensity", "precision": row["high_precision"]},
            ]
        )
    exhaustion_fold_data = exhaustion_folds[
        ["fold", "trades", "expectancy", "win_rate", "profit_factor", "mean_mfe", "mean_mae"]
    ].copy()
    attribution_fold_data = attribution_folds[
        ["policy", "fold", "trades", "expectancy", "profit_factor"]
    ].copy()
    utility_entry_timing_fold_data = utility_entry_timing_folds[
        ["fold", "trades", "expectancy", "win_rate", "profit_factor", "mean_mfe", "mean_mae"]
    ].copy()
    utility_entry_frontier_data = utility_entry_frontier_summary[
        [
            "entry_rule", "fill_rate", "actual_200pct_precision", "event_precision",
            "paired_trade_increment_mean", "row_fisher_p_bh", "event_fisher_p_bh",
        ]
    ].copy()
    breakout_hold_router_fold_data = breakout_hold_router_folds[
        ["route", "fold", "trades", "expectancy", "profit_factor"]
    ].copy()
    breakout_add_data = breakout_add_summary[
        [
            "breakout_add_fraction", "initial_fraction", "trades", "expectancy",
            "profit_factor", "total_return_on_initial_equity", "max_drawdown",
            "paired_increment_mean", "independent_event_increment_mean",
            "fold_increment_positive_share", "mean_deployed_fraction",
        ]
    ].copy()
    breakout_add_data["reserved_for_breakout"] = breakout_add_data["breakout_add_fraction"].map(
        lambda value: f"{value:.0%}"
    )
    taker_flow_recognition_data = pd.DataFrame(
        [
            {"sample": "Calibration rows", "group": "All 8h signals", "precision": taker_flow["calibration"]["base_precision"]},
            {"sample": "Calibration rows", "group": "Taker buy share >=50%", "precision": taker_flow["calibration"]["selected_precision"]},
            {"sample": "Confirmation rows", "group": "All 8h signals", "precision": taker_flow["confirmation"]["base_precision"]},
            {"sample": "Confirmation rows", "group": "Taker buy share >=50%", "precision": taker_flow["confirmation"]["selected_precision"]},
            {"sample": "Confirmation events", "group": "All 8h signals", "precision": taker_flow["confirmation"]["base_event_precision"]},
            {"sample": "Confirmation events", "group": "Taker buy share >=50%", "precision": taker_flow["confirmation"]["selected_event_precision"]},
        ]
    )
    runner_expectancy_data = pd.concat(
        [
            breakout_runner_calibration[["policy", "expectancy"]].assign(period="Calibration 14d"),
            breakout_runner_confirmation[
                breakout_runner_confirmation["slippage_bps_each_side"] == 5.0
            ][["policy", "expectancy"]].assign(period="Confirmation 30d"),
        ],
        ignore_index=True,
    )
    partial_fraction_data = pd.concat(
        [
            partial_fraction_calibration[["partial_fraction", "expectancy"]].assign(period="Calibration 14d"),
            partial_fraction_confirmation[
                partial_fraction_confirmation["slippage_bps_each_side"] == 5.0
            ][["partial_fraction", "expectancy"]].assign(period="Confirmation 30d"),
        ],
        ignore_index=True,
    )
    partial_fraction_data["sale_fraction"] = partial_fraction_data["partial_fraction"].map(lambda value: f"Sell {value:.0%}")
    regime_router_data = pd.concat(
        [
            regime_router_calibration[["route", "paired_increment_mean"]].assign(period="Calibration"),
            regime_router_confirmation[
                regime_router_confirmation["slippage_bps_each_side"] == 5.0
            ][["route", "paired_increment_mean"]].assign(period="Confirmation"),
        ],
        ignore_index=True,
    )

    sources = [
        {
            "id": "price-validation",
            "label": "Purged historical walk-forward evidence",
            "path": "walk_forward_fold_metrics.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('walk_forward_fold_metrics.csv') ORDER BY fold",
                "description": "Expanding 180-day minimum train, 14-day purge, 30-day test folds using the fixed eight-factor logistic model.",
                "tables_used": ["daily_feature_panel.csv.gz", "walk_forward_fold_metrics.csv"],
                "filters": ["USDT perpetual altcoins", "daily quote volume >= 250,000 USDT", "complete forward horizon"],
                "metric_definitions": [
                    "Top-2% lift = precision among each day's top 2% scores divided by the eligible row base rate.",
                    "Entry is the next UTC day's first 4h open; the primary label is future 14-day high return >= 200%.",
                ],
            },
        },
        {
            "id": "oi-validation",
            "label": "Lagged long-window OI intersection",
            "path": "oi_walk_forward_fold_metrics.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('oi_walk_forward_fold_metrics.csv') ORDER BY fold",
                "description": "Price-only, OI-only, price+OI and factor-group ablations evaluated on identical rows and folds.",
                "tables_used": ["oi_walk_forward_fold_metrics.csv", "oi_cohort_coverage.csv"],
                "filters": ["historical market cap only", "OI and market cap lagged 24 hours", "complete required OI fields"],
                "metric_definitions": ["OI ranking gate requires fold-majority AP and lift improvement plus both bootstrap lower bounds above zero."],
            },
        },
        {
            "id": "paper-validation",
            "label": "4h path-dependent 1x paper simulation",
            "path": "paper_strategy_summary.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('paper_strategy_summary.csv') WHERE slippage_bps_each_side = 5 ORDER BY expectancy DESC",
                "description": "Four pre-registered exits with conservative same-bar ordering, funding, fees, slippage and portfolio constraints.",
                "tables_used": ["futures_4h_panel.csv.gz", "paper_strategy_summary.csv"],
                "filters": ["first daily top-2% crossing", "at most 3 new signals/day", "14-day cooldown"],
                "metric_definitions": ["Default fee is 5 bps each side; displayed default scenario uses 5 bps slippage each side."],
            },
        },
        {
            "id": "current-events",
            "label": "Frozen current 30-day event regression",
            "path": "current_30d_runups_verified.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('current_30d_runups_verified.csv') ORDER BY close_return DESC",
                "description": "Any earlier 4h close to any later 4h high reaches 3x; wick-only uses earlier low when close confirmation fails.",
                "tables_used": ["futures_4h_panel.csv.gz", "current_30d_runups_verified.csv"],
                "metric_definitions": ["+200% return means later high / earlier reference price - 1 >= 2.0."],
            },
        },
        {
            "id": "sensitivity-validation",
            "label": "Threshold and horizon sensitivity matrix",
            "path": "threshold_horizon_surface.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('threshold_horizon_surface.csv') ORDER BY horizon_days, threshold_pct",
                "description": "Same frozen price scores evaluated across seven return thresholds and four horizons.",
                "tables_used": ["threshold_horizon_surface.csv"],
                "metric_definitions": ["BH-adjusted one-sided Fisher exact p-values control the threshold-by-horizon matrix."],
            },
        },
        {
            "id": "candidate-validation",
            "label": "Frozen current research watchlist",
            "path": "current_research_watchlist.csv",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('current_research_watchlist.csv') ORDER BY ranking_score_pctile DESC",
                "description": "Top frozen price ranks with OI annotations that never affect rank.",
                "tables_used": ["current_research_watchlist.csv"],
                "metric_definitions": ["Ranking percentile is computed within the latest eligible altcoin cross-section."],
            },
        },
        {
            "id": "continuation-utility-validation",
            "label": "Frozen 8h continuation utility challenger",
            "path": "continuation_entry_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('continuation_utility_strategy_folds.csv') ORDER BY fold",
                "description": "A time-isolated 8h continuation model trained on positive net 14-day path utility, then simulated with the frozen 30-day exit and portfolio constraints.",
                "tables_used": [
                    "continuation_utility_strategy_folds.csv",
                    "continuation_utility_strategy_trades.csv.gz",
                    "continuation_utility_forward_model.json",
                ],
                "filters": [
                    "features use only completed 4h bars before P0+8h",
                    "calibration windows precede confirmation folds",
                    "daily top three new signals and stage vetoes",
                ],
                "metric_definitions": [
                    "Profit factor is gross positive trade PnL divided by absolute gross negative trade PnL after fees, funding, and slippage.",
                    "This is a secondary paper-entry challenger; it is rank-neutral and is not permission for automatic trading.",
                ],
            },
        },
        {
            "id": "intensity-exit-frontier-validation",
            "label": "Multi-threshold intensity and right-tail exit frontier",
            "path": "extreme_intensity_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('extreme_intensity_confirmation_folds.csv') ORDER BY fold",
                "description": "Calibration-only selection of richer extreme-return targets and right-tail exit variants, followed by confirmation-fold and paired-increment validation.",
                "tables_used": [
                    "extreme_intensity_confirmation_folds.csv",
                    "utility_failure_exit_calibration.csv",
                    "utility_tail_exit_folds.csv",
                    "utility_tail_exit_paired_deltas.csv",
                ],
                "filters": [
                    "same P0 checkpoint clock and fixed early-path features",
                    "confirmation folds 3-6 never used for candidate selection",
                    "right-tail variant recorded as a read-only counterfactual only",
                ],
                "metric_definitions": [
                    "Ordinal intensity is a fixed 10/20/30/40% weighting of +50/+100/+150/+200% probabilities.",
                    "Exit increment is the paired net-return difference versus the frozen exit for the same signal.",
                ],
            },
        },
        {
            "id": "utility-intensity-router-validation",
            "label": "Rejected 8h-entry / 20h-intensity router",
            "path": "utility_intensity_router_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_intensity_router_recognition_folds.csv') ORDER BY validation_split",
                "description": "The frozen 8h utility entry is followed by the already-selected 20h ordinal score; low-intensity trades exit at that decision open.",
                "tables_used": [
                    "utility_intensity_router_recognition_folds.csv",
                    "utility_intensity_router_trade_folds.csv",
                    "utility_intensity_router_paired_deltas.csv",
                ],
                "filters": [
                    "8h utility q70 entry unchanged",
                    "20h ordinal q80 router reused without retuning",
                    "forced exit fills at the decision open before same-bar extremes",
                ],
                "metric_definitions": [
                    "Paired increment is routed net return minus frozen-exit net return for the same signal.",
                    "A negative clustered 95% upper bound is treated as strong rejection, not inconclusive evidence.",
                ],
            },
        },
        {
            "id": "utility-exhaustion-exit-validation",
            "label": "Profit-side volume-backed upper-wick exit challenger",
            "path": "utility_exhaustion_exit_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_exhaustion_exit_folds.csv') ORDER BY fold",
                "description": "After the frozen 8h entry, profit-side exhaustion policies are selected on 14-day calibration paths and restored to 30 days on confirmation folds.",
                "tables_used": [
                    "utility_exhaustion_exit_calibration.csv",
                    "utility_exhaustion_exit_folds.csv",
                    "utility_exhaustion_exit_paired_deltas.csv",
                ],
                "filters": [
                    "position has at least doubled before exhaustion can trigger",
                    "completed 4h reversal bar and next-open execution",
                    "paired increment versus the same signal under the frozen exit",
                ],
                "metric_definitions": [
                    "Blow-off reversal requires upper wick >=35% of range, close location <=35%, and volume ratio >=1.5.",
                    "The challenger remains unpromoted when either clustered paired-increment lower bound is non-positive.",
                ],
            },
        },
        {
            "id": "utility-exhaustion-attribution-validation",
            "label": "Fixed attribution of static trail width versus upper-wick timing",
            "path": "utility_exhaustion_attribution_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_exhaustion_attribution_folds.csv') ORDER BY policy, fold",
                "description": "No-reselection factorial attribution on the same frozen 8h entries, with first-signal 14-day event de-duplication.",
                "tables_used": [
                    "utility_exhaustion_attribution_summary.csv",
                    "utility_exhaustion_attribution_folds.csv",
                    "utility_exhaustion_attribution_paired_deltas.csv",
                    "utility_exhaustion_attribution_event_deltas.csv",
                    "utility_exhaustion_attribution_verification.json",
                ],
                "filters": [
                    "same confirmation signals and default costs for every policy",
                    "one first signal per symbol per 14-day event window",
                    "30pct-trail plus wick combination is explicitly post-hoc and prospective-only",
                ],
                "metric_definitions": [
                    "Pure wick increment compares the wick exit to an otherwise identical static 35pct-trail policy.",
                    "Event increment keeps only the first signal in each symbol's non-overlapping 14-day window.",
                ],
            },
        },
        {
            "id": "utility-entry-timing-validation",
            "label": "Execution timing after the frozen 8h utility score",
            "path": "utility_entry_timing_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_entry_timing_confirmation_folds.csv') ORDER BY fold",
                "description": "Nine time-safe immediate, pullback, reclaim, and breakout entries selected only on mature calibration windows and confirmed on folds 3-6.",
                "tables_used": [
                    "utility_entry_timing_calibration.csv",
                    "utility_entry_timing_confirmation_folds.csv",
                    "utility_entry_timing_recognition_paired.csv",
                    "utility_entry_timing_trade_paired_deltas.csv",
                    "utility_entry_timing_verification.json",
                ],
                "filters": [
                    "frozen 8h utility q70 signals only",
                    "maximum 24h trigger window",
                    "completed 4h trigger and next-open entry",
                    "independent-event de-duplication uses first signal per symbol per 14 days",
                ],
                "metric_definitions": [
                    "Fill rate is the share of frozen 8h signals that trigger the delayed entry rule.",
                    "Recognition precision is recomputed from the actual delayed entry open; missing triggers remain in retention denominators.",
                ],
            },
        },
        {
            "id": "utility-entry-frontier-validation",
            "label": "Full fixed entry-rule frontier with multiple-testing control",
            "path": "utility_entry_frontier_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_entry_frontier_summary.csv') ORDER BY actual_200pct_precision DESC",
                "description": "All nine fixed entry rules are inspected on confirmation with row/event de-duplication, clustered intervals, and catalog-wide BH control.",
                "tables_used": [
                    "utility_entry_frontier_summary.csv",
                    "utility_entry_frontier_folds.csv",
                    "utility_entry_frontier_paired_deltas.csv",
                    "utility_entry_frontier_verification.json",
                ],
                "filters": [
                    "confirmation-only winners are hypothesis-generating and cannot be historically promoted",
                    "completed 4h trigger and next-open entry within 24 hours",
                    "same frozen 8h signals for matched trade-return comparisons",
                ],
                "metric_definitions": [
                    "Breakout recognition and post-breakout execution are evaluated separately.",
                    "BH-adjusted row/event tests and event-cluster intervals must pass before a new entry rule is considered.",
                ],
            },
        },
        {
            "id": "utility-breakout-hold-router-validation",
            "label": "Breakout-confirmed prospective hold-management router",
            "path": "utility_breakout_hold_router_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_breakout_hold_router_folds.csv') ORDER BY route, fold",
                "description": "The frozen 8h entry is retained; a completed six-bar breakout may switch only profit-side management at the next 4h open.",
                "tables_used": [
                    "utility_breakout_hold_router_summary.csv",
                    "utility_breakout_hold_router_folds.csv",
                    "utility_breakout_hold_router_paired_deltas.csv",
                    "utility_breakout_hold_router_verification.json",
                ],
                "filters": [
                    "entry and downside stops remain frozen",
                    "all pre-switch partial-fill and trailing state is preserved",
                    "first signal per symbol per 14 days for independent-event increments",
                    "confirmation-mined result is prospective-only",
                ],
                "metric_definitions": [
                    "Paired increment compares routed and frozen exit returns for the same 8h signal.",
                    "Prospective diagnostic requires positive week/symbol lower bounds for rows and independent events.",
                ],
            },
        },
        {
            "id": "utility-breakout-add-validation",
            "label": "Risk-neutral breakout-add allocation validation",
            "path": "utility_breakout_add_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_breakout_add_summary.csv') ORDER BY breakout_add_fraction",
                "description": "A fixed total planned position is split between the original 8h entry and a possible next-open breakout add; unused reserve remains cash.",
                "tables_used": [
                    "utility_breakout_add_calibration.csv",
                    "utility_breakout_add_summary.csv",
                    "utility_breakout_add_folds.csv",
                    "utility_breakout_add_paired_deltas.csv",
                    "utility_breakout_add_verification.json",
                ],
                "filters": [
                    "no additional leverage or gross exposure",
                    "allocation selected on 14-day calibration windows only",
                    "30-day confirmation is held out from allocation selection",
                    "completed six-bar breakout enters the reserved tranche at the next 4h open",
                ],
                "metric_definitions": [
                    "Mean deployed fraction includes idle cash when no breakout add occurs.",
                    "Paired increment compares the split allocation with the full initial position on the same signal.",
                ],
            },
        },
        {
            "id": "utility-taker-flow-validation",
            "label": "8h taker-buy-share prospective diagnostic",
            "path": "utility_taker_flow_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_taker_flow_confirmation_folds.csv') ORDER BY fold",
                "description": "A semantic 50% taker-buy quote-volume threshold is applied at the frozen 8h decision clock without delaying entry.",
                "tables_used": [
                    "utility_taker_flow_calibration_splits.csv",
                    "utility_taker_flow_confirmation_folds.csv",
                    "utility_taker_flow_strategy_summary.csv",
                    "utility_taker_flow_strategy_folds.csv",
                    "utility_taker_flow_verification.json",
                ],
                "filters": [
                    "frozen 8h utility-selected signals only",
                    "two completed 4h bars after P0",
                    "taker buy quote-volume share >= 50%",
                    "rank, original stage veto and paper eligibility remain unchanged",
                ],
                "metric_definitions": [
                    "Taker buy share is taker-buy quote volume divided by total quote volume across the completed 8h path.",
                    "Independent events retain the first signal in each symbol's non-overlapping 14-day window.",
                ],
            },
        },
        {
            "id": "utility-taker-robustness-validation",
            "label": "8h taker-flow price-path matched audit",
            "path": "utility_taker_flow_robustness_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_taker_flow_price_matched_permutation.csv')",
                "description": "Nearby semantic thresholds and within-fold price-path matched permutations test whether taker flow adds information beyond early momentum.",
                "tables_used": [
                    "utility_taker_flow_robustness_definitions.csv",
                    "utility_taker_flow_price_matched_permutation.csv",
                    "utility_strategy_iteration_verification.json",
                ],
                "filters": ["frozen 8h utility signals", "confirmation folds 3-6", "rank-neutral diagnostic"],
                "metric_definitions": ["One-sided p is the share of within-stratum shuffled flags with precision increment at least as large as observed."],
            },
        },
        {
            "id": "utility-breakout-runner-validation",
            "label": "Breakout full-position runner rejection",
            "path": "utility_breakout_runner_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_breakout_runner_calibration.csv') ORDER BY policy",
                "description": "Six no-partial-sale exit policies are selected only on mature calibration windows and compared with the current half-at-150% breakout router.",
                "tables_used": [
                    "utility_breakout_runner_calibration.csv",
                    "utility_breakout_runner_30d_calibration_sensitivity.csv",
                    "utility_breakout_runner_confirmation.csv",
                    "utility_breakout_runner_calibration_paired.csv",
                    "utility_strategy_iteration_verification.json",
                ],
                "filters": ["same original 8h entry", "same stop and wick exit", "no confirmation-set rescue"],
                "metric_definitions": ["Paired increment compares raw outcomes for the same signal; portfolio expectancy also enforces concurrency and exposure constraints."],
            },
        },
        {
            "id": "utility-breakout-partial-fraction-validation",
            "label": "Breakout partial-sale fraction nonstationarity",
            "path": "utility_breakout_partial_fraction_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_breakout_partial_fraction_confirmation.csv') WHERE slippage_bps_each_side = 5 ORDER BY partial_fraction",
                "description": "The already-fixed breakout router sells 25%, 50%, or 75% at +150%; all other clocks and controls remain identical.",
                "tables_used": [
                    "utility_breakout_partial_fraction_calibration.csv",
                    "utility_breakout_partial_fraction_confirmation.csv",
                    "utility_breakout_partial_fraction_calibration_paired.csv",
                    "utility_strategy_iteration_verification.json",
                ],
                "filters": ["same 8h entry and breakout clock", "same +150% target and 30% trail", "calibration selection followed by confirmation audit"],
                "metric_definitions": ["Temporal reversal means the calibration-selected fraction has a negative paired increment in confirmation."],
            },
        },
        {
            "id": "utility-regime-partial-router-validation",
            "label": "Regime-conditioned partial-sale routing rejection",
            "path": "utility_regime_partial_router_decision.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('utility_regime_partial_router_confirmation.csv') WHERE slippage_bps_each_side = 5 ORDER BY route",
                "description": "Frozen BTC trend, volatility and altcoin breadth states route only the already-fixed partial-sale fraction; calibration defines every rule and confirmation only audits it.",
                "tables_used": [
                    "utility_regime_partial_router_calibration.csv",
                    "utility_regime_partial_router_confirmation.csv",
                    "utility_regime_partial_fraction_segments.csv",
                    "utility_regime_partial_state_support.csv",
                    "utility_strategy_iteration_verification.json",
                ],
                "filters": ["minimum five fraction-sensitive calibration signals", "same entry, target, trail and costs", "no confirmation-set rescue"],
                "metric_definitions": ["Paired increment is routed net return minus the fixed 50% partial-sale return for the same signal."],
            },
        },
        {
            "id": "forward-operational-audit",
            "label": "Immutable forward-monitor operational audit",
            "path": "forward_monitor_operational_audit_2026-07-21.json",
            "query": {
                "engine": "python",
                "language": "json",
                "description": "Snapshot dates, mature-label counts and a late read-only public-data dry-run are reconciled without backfilling missed decision clocks.",
                "tables_used": ["forward_monitor_operational_audit_2026-07-21.json", "utility_strategy_iteration_verification.json"],
                "filters": ["08:20 Asia/Shanghai immutable clock", "dry-run probe excluded from OOS", "no order routing"],
                "metric_definitions": ["A true OOS snapshot must exist at the scheduled decision clock; a later reconstruction is diagnostic only."],
            },
        },
    ]

    price = summary["price_validation"]
    metric = price["historical_walk_forward"]
    bootstrap = price["lift_bootstrap_7d_blocks"]
    event_bootstrap = price["event_capture_bootstrap"]
    oi = summary["oi_validation"]
    strategy = summary["paper_strategy"]
    utility = continuation["entry_utility"]
    utility_summary = utility["strategy_default_summary"]
    intensity_summary = intensity["confirmation"]
    tail_summary = tail_exit["selected_confirmation_summary"]
    router_recognition = router["confirmation_recognition"]
    router_baseline = router["baseline_confirmation_summary"]
    router_summary = router["routed_confirmation_summary"]
    exhaustion_baseline = exhaustion["baseline_confirmation_summary"]
    exhaustion_summary = exhaustion["selected_confirmation_summary"]
    pure_wick = attribution["contrasts"]["wick_minus_tail35"]
    trail_width = attribution["contrasts"]["trail35_minus_trail30"]
    posthoc_wick = attribution["posthoc_hypothesis"]
    utility_timing_baseline = utility_entry_timing["confirmation_recognition_baseline_all"]
    utility_timing_selected = utility_entry_timing["confirmation_recognition_selected"]
    utility_timing_summary = utility_entry_timing["selected_strategy_summary"]
    breakout_rule = utility_entry_frontier["rules"]["breakout6"]
    breakout_hold_detail = breakout_hold_router["route_details"]["breakout6_half150_trail30_wick"]
    breakout_hold_summary = breakout_hold_detail["strategy_summary"]
    breakout_add_baseline = breakout_add["confirmation_details"]["0.0"]
    breakout_add_10pct = breakout_add["confirmation_details"]["0.1"]
    taker_confirmation = taker_flow["confirmation"]
    taker_default = taker_flow_strategy[taker_flow_strategy["slippage_bps_each_side"] == 5.0].set_index("variant")
    title = "Binance 暴涨币因子：严格历史验证与前瞻监控基线"
    manifest = {
        "version": 1,
        "surface": "report",
        "title": title,
        "description": "Fixed-factor historical validation, OI incremental evidence, 4h path paper simulation, and immutable forward-monitor handoff.",
        "generatedAt": summary["generated_at_utc"],
        "sources": sources,
        "charts": [
            {
                "id": "fold-lift",
                "title": "Walk-forward top-2% lift",
                "type": "bar",
                "dataset": "fold_metrics",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "price_top_2pct_lift", "type": "quantitative", "title": "Lift"},
                },
                "sourceId": "price-validation",
            },
            {
                "id": "sensitivity-lift",
                "title": "Threshold and horizon lift",
                "type": "line",
                "dataset": "sensitivity",
                "encodings": {
                    "x": {"field": "threshold_pct", "type": "quantitative", "title": "Threshold (%)"},
                    "y": {"field": "lift", "type": "quantitative", "title": "Lift"},
                    "color": {"field": "horizon_days", "type": "nominal", "title": "Horizon (days)"},
                },
                "sourceId": "sensitivity-validation",
            },
            {
                "id": "oi-fold-lift",
                "title": "Price-only and price+OI lift",
                "type": "bar",
                "dataset": "oi_fold_comparison",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "top_2pct_lift", "type": "quantitative", "title": "Lift"},
                    "color": {"field": "model", "type": "nominal", "title": "Model"},
                },
                "options": {"grouping": "grouped"},
                "sourceId": "oi-validation",
            },
            {
                "id": "continuation-fold-pf",
                "title": "8h utility challenger profit factor by confirmation fold",
                "type": "bar",
                "dataset": "continuation_utility_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "continuation-utility-validation",
            },
            {
                "id": "intensity-fold-ap",
                "title": "Price and ordinal-intensity average precision by confirmation fold",
                "type": "line",
                "dataset": "extreme_intensity_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"fields": ["price_average_precision", "intensity_average_precision"], "type": "quantitative", "title": "Average precision"},
                },
                "sourceId": "intensity-exit-frontier-validation",
            },
            {
                "id": "tail-exit-fold-pf",
                "title": "Right-tail exit profit factor by confirmation fold",
                "type": "bar",
                "dataset": "utility_tail_exit_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "intensity-exit-frontier-validation",
            },
            {
                "id": "utility-intensity-router-precision",
                "title": "8h entries and 20h high-intensity precision by fold",
                "type": "bar",
                "dataset": "utility_intensity_router_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "precision", "type": "quantitative", "title": "Remaining +200% precision"},
                    "color": {"field": "group", "type": "nominal", "title": "Group"},
                },
                "options": {"grouping": "grouped"},
                "sourceId": "utility-intensity-router-validation",
            },
            {
                "id": "utility-exhaustion-exit-pf",
                "title": "Volume-backed upper-wick exit profit factor by fold",
                "type": "bar",
                "dataset": "utility_exhaustion_exit_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "utility-exhaustion-exit-validation",
            },
            {
                "id": "utility-exhaustion-attribution-pf",
                "title": "Static-tail and upper-wick policy profit factor by fold",
                "type": "line",
                "dataset": "utility_exhaustion_attribution_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                    "color": {"field": "policy", "type": "nominal", "title": "Policy"},
                },
                "sourceId": "utility-exhaustion-attribution-validation",
            },
            {
                "id": "utility-entry-timing-pf",
                "title": "5% dip-and-reclaim entry profit factor by fold",
                "type": "bar",
                "dataset": "utility_entry_timing_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                },
                "sourceId": "utility-entry-timing-validation",
            },
            {
                "id": "utility-entry-frontier-precision",
                "title": "Fixed entry-rule +200% precision frontier",
                "type": "bar",
                "dataset": "utility_entry_frontier",
                "encodings": {
                    "x": {"field": "entry_rule", "type": "nominal", "title": "Entry rule"},
                    "y": {"field": "actual_200pct_precision", "type": "quantitative", "title": "+200% precision"},
                },
                "sourceId": "utility-entry-frontier-validation",
            },
            {
                "id": "utility-breakout-hold-router-pf",
                "title": "Breakout hold-router profit factor by fold",
                "type": "line",
                "dataset": "utility_breakout_hold_router_folds",
                "encodings": {
                    "x": {"field": "fold", "type": "nominal", "title": "Fold"},
                    "y": {"field": "profit_factor", "type": "quantitative", "title": "Profit factor"},
                    "color": {"field": "route", "type": "nominal", "title": "Exit route"},
                },
                "sourceId": "utility-breakout-hold-router-validation",
            },
            {
                "id": "utility-breakout-add-expectancy",
                "title": "Breakout reserve allocation and confirmation expectancy",
                "type": "bar",
                "dataset": "utility_breakout_add",
                "encodings": {
                    "x": {"field": "reserved_for_breakout", "type": "nominal", "title": "Position reserved for breakout"},
                    "y": {"field": "expectancy", "type": "quantitative", "title": "Portfolio trade expectancy"},
                },
                "sourceId": "utility-breakout-add-validation",
            },
            {
                "id": "utility-taker-flow-precision",
                "title": "8h taker-buy-share and +200% precision",
                "type": "bar",
                "dataset": "utility_taker_flow_recognition",
                "encodings": {
                    "x": {"field": "sample", "type": "nominal", "title": "Evidence sample"},
                    "y": {"field": "precision", "type": "quantitative", "title": "+200% precision"},
                    "color": {"field": "group", "type": "nominal", "title": "Signal group"},
                },
                "options": {"grouping": "grouped"},
                "sourceId": "utility-taker-flow-validation",
            },
            {
                "id": "utility-taker-matched-p",
                "title": "Taker-flow price-path matched permutation p-values",
                "type": "bar",
                "dataset": "utility_taker_matched_permutation",
                "encodings": {
                    "x": {"field": "schema", "type": "nominal", "title": "Matched price information"},
                    "y": {"field": "one_sided_p", "type": "quantitative", "title": "One-sided p-value"},
                },
                "sourceId": "utility-taker-robustness-validation",
            },
            {
                "id": "utility-breakout-runner-expectancy",
                "title": "Partial exit versus full-position runner expectancy",
                "type": "bar",
                "dataset": "utility_breakout_runner_expectancy",
                "encodings": {
                    "x": {"field": "policy", "type": "nominal", "title": "Exit policy"},
                    "y": {"field": "expectancy", "type": "quantitative", "title": "Trade expectancy"},
                    "color": {"field": "period", "type": "nominal", "title": "Evidence period"},
                },
                "options": {"grouping": "grouped"},
                "sourceId": "utility-breakout-runner-validation",
            },
            {
                "id": "utility-breakout-partial-fraction-expectancy",
                "title": "Partial-sale fraction expectancy by evidence period",
                "type": "bar",
                "dataset": "utility_breakout_partial_fraction_expectancy",
                "encodings": {
                    "x": {"field": "sale_fraction", "type": "nominal", "title": "Fraction sold at +150%"},
                    "y": {"field": "expectancy", "type": "quantitative", "title": "Trade expectancy"},
                    "color": {"field": "period", "type": "nominal", "title": "Evidence period"},
                },
                "options": {"grouping": "grouped"},
                "sourceId": "utility-breakout-partial-fraction-validation",
            },
            {
                "id": "utility-regime-partial-router-increment",
                "title": "Regime-router paired increment by evidence period",
                "type": "bar",
                "dataset": "utility_regime_partial_router_increment",
                "encodings": {
                    "x": {"field": "route", "type": "nominal", "title": "Frozen route"},
                    "y": {"field": "paired_increment_mean", "type": "quantitative", "title": "Paired return increment"},
                    "color": {"field": "period", "type": "nominal", "title": "Evidence period"},
                },
                "options": {"grouping": "grouped"},
                "sourceId": "utility-regime-partial-router-validation",
            },
        ],
        "tables": [
            {
                "id": "paper-table",
                "title": "Paper strategy policies at default costs",
                "dataset": "paper_default",
                "columns": [
                    {"field": "policy", "label": "Policy", "type": "text"},
                    {"field": "trades", "label": "Trades", "type": "number"},
                    {"field": "expectancy", "label": "Expectancy", "type": "percent"},
                    {"field": "profit_factor", "label": "Profit factor", "type": "number"},
                    {"field": "max_drawdown", "label": "Max drawdown", "type": "percent"},
                    {"field": "gate_pass", "label": "Gate pass", "type": "text"},
                ],
                "defaultSort": {"field": "expectancy", "direction": "desc"},
                "sourceId": "paper-validation",
            },
            {
                "id": "event-table",
                "title": "Current 30-day +200% events",
                "dataset": "current_events",
                "columns": [
                    {"field": "symbol", "label": "Symbol", "type": "text"},
                    {"field": "classification", "label": "Class", "type": "text"},
                    {"field": "close_return", "label": "Close reference return", "type": "percent"},
                    {"field": "low_return", "label": "Wick reference return", "type": "percent"},
                    {"field": "peak_time", "label": "Peak time", "type": "date"},
                ],
                "defaultSort": {"field": "close_return", "direction": "desc"},
                "sourceId": "current-events",
            },
            {
                "id": "candidate-table",
                "title": "Current read-only research candidates",
                "dataset": "candidates",
                "columns": [
                    {"field": "symbol", "label": "Symbol", "type": "text"},
                    {"field": "ranking_score_pctile", "label": "Score percentile", "type": "number"},
                    {"field": "research_stage", "label": "Stage", "type": "text"},
                    {"field": "oi_annotation", "label": "OI annotation", "type": "text"},
                    {"field": "return_7d", "label": "7d return", "type": "percent"},
                ],
                "defaultSort": {"field": "ranking_score_pctile", "direction": "desc"},
                "sourceId": "candidate-validation",
            },
        ],
        "blocks": [
            {"id": "title", "type": "markdown", "body": f"# {title}\n\n固定 8 因子、无测试集选模、OI 同行比较、4h 路径组合回测与真正 OOS 监控。"},
            {
                "id": "summary", "type": "markdown", "sourceId": "price-validation",
                "body": (
                    "## Executive Summary\n\n"
                    f"历史 walk-forward top-2% lift 为 **{metric['top_2pct_lift']:.2f}x**（7 日块 95% 区间 "
                    f"{bootstrap['lower_95pct']:.2f}–{bootstrap['upper_95pct']:.2f}x），独立事件捕获率 "
                    f"{price['event_capture_rate']:.1%}（事件 bootstrap 95% 区间 "
                    f"{event_bootstrap['lower_95pct']:.1%}–{event_bootstrap['upper_95pct']:.1%}）。"
                    "这些是已查看过的历史验证，不是真正 OOS。"
                ),
            },
            {"id": "fold-chart-block", "type": "chart", "chartId": "fold-lift"},
            {"id": "sensitivity-heading", "type": "markdown", "body": "## 阈值与周期敏感性\n\n+200% / 14 日为主标签；+900% 只探索十倍币，不参与调参。"},
            {"id": "sensitivity-chart-block", "type": "chart", "chartId": "sensitivity-lift"},
            {
                "id": "oi-heading", "type": "markdown", "sourceId": "oi-validation",
                "body": (
                    "## 小市值与 OI\n\n"
                    f"完整 OI 组合排序资格为 **{oi['ranking_eligible']}**；多数折同时改善 AP 与 lift 的比例为 "
                    f"{oi.get('fold_majority_both_positive', 0):.0%}。未通过时 OI 只作拥挤度或数据质量注释。"
                ),
            },
            {"id": "oi-chart-block", "type": "chart", "chartId": "oi-fold-lift"},
            {
                "id": "paper-heading", "type": "markdown", "sourceId": "paper-validation",
                "body": (
                    "## 1x 永续纸面可行性\n\n"
                    f"最终分类为 **{strategy.get('final_classification', 'watchlist_only')}**。只有成本后期望、利润因子、"
                    "回撤、跨折/市场状态、最大事件剔除和利润集中度同时通过，才称纸面候选；本项目永不自动下单。"
                ),
            },
            {"id": "paper-table-block", "type": "table", "tableId": "paper-table"},
            {
                "id": "continuation-utility-heading", "type": "markdown", "sourceId": "continuation-utility-validation",
                "body": (
                    "## 8h continuation utility challenger\n\n"
                    f"The direct +200% identification gate still fails, but the frozen P0+8h utility challenger is classified as "
                    f"**{continuation['entry_strategy_classification']}**. At default costs its constrained simulation has "
                    f"{utility_summary['trades']} trades, {utility_summary['expectancy']:.1%} expectancy, profit factor "
                    f"{utility_summary['profit_factor']:.2f}, and {utility_summary['max_drawdown']:.1%} maximum drawdown. "
                    f"{utility['fold_majority_profit_factor_gt_1']:.0%} of confirmation folds and "
                    f"{utility_summary['market_state_majority_profit_factor_gt_1']:.0%} of market states have profit factor above one. "
                    f"Week-cluster and symbol-cluster bootstrap profit-factor lower bounds are "
                    f"{utility['week_bootstrap']['profit_factor_lower_95pct']:.2f} and "
                    f"{utility['symbol_bootstrap']['profit_factor_lower_95pct']:.2f}. "
                    "This remains historical evidence pending immutable true-OOS observations; no automatic trading is allowed."
                ),
            },
            {"id": "continuation-utility-chart-block", "type": "chart", "chartId": "continuation-fold-pf"},
            {
                "id": "intensity-exit-frontier-heading", "type": "markdown", "sourceId": "intensity-exit-frontier-validation",
                "body": (
                    "## Multi-threshold recognition and right-tail exit frontier\n\n"
                    f"The calibration-selected ordinal intensity challenger raises confirmation AP to {intensity_summary['intensity_average_precision']:.3f} "
                    f"and retains {intensity_summary['positive_retention']:.1%} of remaining +200% labels, but its clustered incremental bounds cross zero, so classification remains "
                    f"**{intensity['classification']}**. Failure-to-launch exits are rejected. The +150% half / 30% trail counterfactual has "
                    f"{tail_summary['expectancy']:.1%} expectancy and profit factor {tail_summary['profit_factor']:.2f}, but paired incremental bounds also cross zero; "
                    "the frozen exit remains primary and automatic trading remains disabled."
                ),
            },
            {"id": "intensity-fold-ap-block", "type": "chart", "chartId": "intensity-fold-ap"},
            {"id": "tail-exit-fold-pf-block", "type": "chart", "chartId": "tail-exit-fold-pf"},
            {
                "id": "utility-intensity-router-heading", "type": "markdown", "sourceId": "utility-intensity-router-validation",
                "body": (
                    "## Rejected 8h-entry / 20h-intensity router\n\n"
                    f"The pooled remaining +200% precision rises from {router_recognition['base_precision_from_entry']:.1%} to "
                    f"{router_recognition['high_precision_from_entry']:.1%}, but only {router_recognition['positive_retention_from_entry']:.0%} of positives remain and two of four folds select no winner. "
                    f"Forcing low-intensity exits changes expectancy from {router_baseline['expectancy']:.1%} to {router_summary['expectancy']:.1%} and profit factor from "
                    f"{router_baseline['profit_factor']:.2f} to {router_summary['profit_factor']:.2f}. Both clustered paired-increment upper bounds are negative. "
                    "The router is strongly rejected and is not added to forward labels."
                ),
            },
            {"id": "utility-intensity-router-chart-block", "type": "chart", "chartId": "utility-intensity-router-precision"},
            {
                "id": "utility-exhaustion-exit-heading", "type": "markdown", "sourceId": "utility-exhaustion-exit-validation",
                "body": (
                    "## Profit-side volume-backed upper-wick exit\n\n"
                    f"Calibration selects half at +150%, a 35% trail, and next-open exit after a doubled position prints a volume-backed upper-wick reversal. "
                    f"Confirmation expectancy is {exhaustion_summary['expectancy']:.1%} versus {exhaustion_baseline['expectancy']:.1%}, PF is "
                    f"{exhaustion_summary['profit_factor']:.2f} versus {exhaustion_baseline['profit_factor']:.2f}, and all four fold PF values exceed one. "
                    f"Paired mean increment is {exhaustion['paired_increment_mean']:.1%}, but both clustered incremental lower bounds remain negative. "
                    "It is frozen as a forward counterfactual only; the primary exit is unchanged."
                ),
            },
            {"id": "utility-exhaustion-exit-chart-block", "type": "chart", "chartId": "utility-exhaustion-exit-pf"},
            {
                "id": "utility-exhaustion-attribution-heading", "type": "markdown", "sourceId": "utility-exhaustion-attribution-validation",
                "body": (
                    "## Exit-factor attribution: trail width versus upper-wick timing\n\n"
                    f"Widening the trail from 30% to 35% changes 12 paths and harms all 12; mean paired increment is {trail_width['mean_delta']:.1%} and both clustered upper bounds are negative. "
                    f"The pure upper-wick timing effect is +{pure_wick['mean_delta']:.1%} per signal row and +{pure_wick['event_mean_delta']:.1%} per independent event, but all row/event clustered lower bounds cross zero. "
                    f"The derived 30%-trail plus wick policy has {posthoc_wick['default_cost_summary']['expectancy']:.1%} expectancy and PF {posthoc_wick['default_cost_summary']['profit_factor']:.2f}; it was generated after inspecting confirmation and is prospective-label-only. "
                    "The primary exit and automatic-trading prohibition remain unchanged."
                ),
            },
            {"id": "utility-exhaustion-attribution-chart-block", "type": "chart", "chartId": "utility-exhaustion-attribution-pf"},
            {
                "id": "utility-entry-timing-heading", "type": "markdown", "sourceId": "utility-entry-timing-validation",
                "body": (
                    "## Entry execution after the frozen 8h score\n\n"
                    f"Calibration selects a 5% dip followed by a positive higher-close reclaim, entered at the next 4h open within 24 hours. "
                    f"Confirmation filters 88 signals to {utility_timing_selected['filled_entries']}, raises row precision from {utility_timing_baseline['actual_200pct_precision']:.1%} to {utility_timing_selected['actual_200pct_precision']:.1%}, and retains all five positive independent events. "
                    f"Event-cluster precision intervals are positive, but row-cluster intervals cross zero. Trade expectancy is {utility_timing_summary['expectancy']:.1%} and PF {utility_timing_summary['profit_factor']:.2f}; absolute and paired trade intervals also cross zero. "
                    "The 8h score open remains primary; the reclaim rule is recorded prospectively only."
                ),
            },
            {"id": "utility-entry-timing-chart-block", "type": "chart", "chartId": "utility-entry-timing-pf"},
            {
                "id": "utility-entry-frontier-heading", "type": "markdown", "sourceId": "utility-entry-frontier-validation",
                "body": (
                    "## Full fixed entry-rule frontier\n\n"
                    f"A completed six-bar breakout raises confirmation row precision to {breakout_rule['recognition']['actual_200pct_precision']:.1%} "
                    f"and independent-event precision to {breakout_rule['recognition']['event_precision']:.1%}, but it had zero calibration positives, misses catalog-level BH gates, and event intervals cross zero. "
                    f"Waiting for the breakout before buying reduces matched trade return by {abs(breakout_rule['paired_trade_increment_mean']):.1%}; both week and symbol upper bounds are negative. "
                    "Breakout is therefore a hold-strength annotation, not a new buy trigger."
                ),
            },
            {"id": "utility-entry-frontier-chart-block", "type": "chart", "chartId": "utility-entry-frontier-precision"},
            {
                "id": "utility-breakout-hold-router-heading", "type": "markdown", "sourceId": "utility-breakout-hold-router-validation",
                "body": (
                    "## Breakout-confirmed hold management\n\n"
                    "The frozen 8h entry stays unchanged. A completed breakout within 24 hours switches profit-side management only at the next 4h open to +150% half-sale, a 30% trail, and the volume-backed upper-wick next-open exit after a double. "
                    f"Historical expectancy is {breakout_hold_summary['expectancy']:.1%}, PF {breakout_hold_summary['profit_factor']:.2f}, and paired all-row / independent-event increments are "
                    f"{breakout_hold_detail['paired_increment_mean_all_signals']:.1%} / {breakout_hold_detail['independent_event_increment_mean']:.1%}; all four clustered lower bounds are positive. "
                    "Because the router was discovered after confirmation inspection, it is frozen only as a non-ranking forward counterfactual; primary entry, primary exit, and automatic-trading authority remain unchanged."
                ),
            },
            {"id": "utility-breakout-hold-router-chart-block", "type": "chart", "chartId": "utility-breakout-hold-router-pf"},
            {
                "id": "utility-breakout-add-heading", "type": "markdown", "sourceId": "utility-breakout-add-validation",
                "body": (
                    "## Risk-neutral breakout add allocation\n\n"
                    "Total planned position risk is held fixed: reserve that is not triggered remains cash, so no result comes from extra leverage. "
                    f"All 10%/25%/50% reserve choices lose to the full initial position in every calibration split, so the selected reserve is **{breakout_add['selected_breakout_add_fraction']:.0%}**. "
                    f"On confirmation, full initial deployment has {breakout_add_baseline['strategy_summary']['expectancy']:.1%} expectancy; a 10% reserve lowers it to "
                    f"{breakout_add_10pct['strategy_summary']['expectancy']:.1%}, with paired all-row / independent-event increments of "
                    f"{breakout_add_10pct['paired_increment_mean']:.1%} / {breakout_add_10pct['independent_event_increment_mean']:.1%}. "
                    "The higher reported profit factor at larger reserves is explained by lower capital deployment, not incremental alpha. Breakout may manage an existing winner, but should not delay or enlarge the buy."
                ),
            },
            {"id": "utility-breakout-add-chart-block", "type": "chart", "chartId": "utility-breakout-add-expectancy"},
            {
                "id": "utility-taker-flow-heading", "type": "markdown", "sourceId": "utility-taker-flow-validation",
                "body": (
                    "## 8h taker-buy-share prospective annotation\n\n"
                    f"Requiring completed-path taker buy share of at least 50% retains {taker_confirmation['selected_positives']}/{taker_confirmation['positives']} confirmation +200% rows and raises row precision from "
                    f"{taker_confirmation['base_precision']:.1%} to {taker_confirmation['selected_precision']:.1%}. Row week/symbol lower bounds are positive, but independent-event lower bounds cross zero. "
                    f"The filtered frozen-exit portfolio has {taker_default.loc['taker50_frozen', 'expectancy']:.1%} expectancy and PF {taker_default.loc['taker50_frozen', 'profit_factor']:.2f}; "
                    f"the filtered breakout-router combination has {taker_default.loc['taker50_breakout_router', 'expectancy']:.1%} and PF {taker_default.loc['taker50_breakout_router', 'profit_factor']:.2f}, but fold 6 and symbol-cluster gates fail. "
                    "The field is therefore recorded prospectively without changing rank, stage veto, paper eligibility, primary entry/exit, or trading authority."
                ),
            },
            {"id": "utility-taker-flow-chart-block", "type": "chart", "chartId": "utility-taker-flow-precision"},
            {
                "id": "utility-taker-robustness-heading", "type": "markdown", "sourceId": "utility-taker-robustness-validation",
                "body": (
                    "## Taker-flow independence audit\n\n"
                    f"The 48%-51% nearby taker-share thresholds all improve +200% precision in calibration and confirmation. However, matching both early return and path efficiency gives p={taker_robustness['matched_permutation'][1]['one_sided_p']:.3f}; the 50% field is directionally useful but not independently identified. It remains a rank-neutral forward annotation and cannot become a buy gate."
                ),
            },
            {"id": "utility-taker-matched-chart-block", "type": "chart", "chartId": "utility-taker-matched-p"},
            {
                "id": "utility-breakout-runner-heading", "type": "markdown", "sourceId": "utility-breakout-runner-validation",
                "body": (
                    "## Breakout full-position runner rejected\n\n"
                    f"All six no-partial-sale candidates fail calibration against `{breakout_runner['current_router']}`. Confirmation's best point estimate (`{breakout_runner['confirmation_point_estimate_best_policy']}`) is explicitly posthoc-only and cannot rescue the rejected family. The current half-at-150%, 30% trailing router and all safety constraints remain unchanged."
                ),
            },
            {"id": "utility-breakout-runner-chart-block", "type": "chart", "chartId": "utility-breakout-runner-expectancy"},
            {
                "id": "utility-breakout-partial-fraction-heading", "type": "markdown", "sourceId": "utility-breakout-partial-fraction-validation",
                "body": (
                    "## Partial-sale fraction is not stable through time\n\n"
                    f"Calibration selects selling {partial_fraction['calibration_selected_fraction']:.0%} at +150%, but confirmation favors {partial_fraction['confirmation_point_estimate_best_fraction']:.0%}; the calibration-selected fraction reverses to a negative paired increment. The neutral 50% partial sale is retained, with no adaptive switch or forward rule change."
                ),
            },
            {"id": "utility-breakout-partial-fraction-chart-block", "type": "chart", "chartId": "utility-breakout-partial-fraction-expectancy"},
            {
                "id": "utility-regime-partial-router-heading", "type": "markdown", "sourceId": "utility-regime-partial-router-validation",
                "body": (
                    "## Market state does not yet identify the sale fraction\n\n"
                    f"Only {regime_router['affected_calibration_signals']} calibration signals and {regime_router['affected_confirmation_signals']} confirmation signals change return when the partial-sale fraction changes. The semantic volatility rule reverses direction across periods, while the learned state rule falls back to 50% because no state reaches five affected calibration signals. The fixed 50% sale remains the only forward rule."
                ),
            },
            {"id": "utility-regime-partial-router-chart-block", "type": "chart", "chartId": "utility-regime-partial-router-increment"},
            {
                "id": "forward-operational-audit-heading", "type": "markdown", "sourceId": "forward-operational-audit",
                "body": (
                    "## Forward-monitor operations are incomplete\n\n"
                    f"Immutable snapshots exist for {', '.join(operational_audit['immutable_snapshot_dates_present'])}; {', '.join(operational_audit['immutable_snapshot_dates_missing'])} are missing. The late live-data dry-run passed 527/527 price-feature coverage but wrote no snapshot and is excluded from true OOS. Mature 8h utility and launch-microstructure observations remain zero, so the paper candidate cannot be promoted."
                ),
            },
            {"id": "current-heading", "type": "markdown", "body": "## 当前事件与观察名单\n\nAKE、BTW 与 17 个收盘确认、2 个插针事件由独立验证器复算。"},
            {"id": "event-table-block", "type": "table", "tableId": "event-table"},
            {"id": "candidate-table-block", "type": "table", "tableId": "candidate-table"},
            {
                "id": "forward-microstructure", "type": "markdown",
                "body": (
                    "## 真正前瞻的启动结构验证\n\n"
                    "每天 12:05（Asia/Shanghai）在 24 小时启动确认后冻结盘口深度与冲击成本、资金费率、OI 及其 1/4/24 小时变化、"
                    "多空比和主动买卖结构。采集只允许在确认后 15 分钟内完成；迟到或数据不完整时 fail-closed，不能事后回填。"
                    "首轮审计拒绝了已迟到的 AKE 与 ESPORTS，因此当前有效微观结构样本为 0。满 30 天后追加实际资金费率与两套冻结退出结果。"
                    "所有字段仅作前瞻研究，不改变排名、阶段否决或交易权限。"
                ),
            },
            {
                "id": "limits", "type": "markdown",
                "body": (
                    "## 限制与下一步\n\n"
                    "历史下架合约仍有覆盖缺口，长窗 OI 依赖已有缓存，期货/现货成交比不可恢复。"
                    "每日 08:20（Asia/Shanghai）只读监控保存不可覆盖快照；30/60/90 天复盘前不重训、不改阈值。"
                ),
            },
        ],
    }
    artifact = {
        "surface": "report",
        "manifest": manifest,
        "snapshot": {
            "version": 1,
            "status": "ready",
            "generatedAt": summary["generated_at_utc"],
            "datasets": {
                "fold_metrics": records(fold_data),
                "sensitivity": records(sensitivity),
                "oi_fold_comparison": oi_chart_rows,
                "paper_default": records(paper_table),
                "continuation_utility_folds": records(continuation_fold_data),
                "extreme_intensity_folds": records(intensity_fold_data),
                "utility_tail_exit_folds": records(tail_exit_fold_data),
                "utility_intensity_router_folds": router_fold_rows,
                "utility_exhaustion_exit_folds": records(exhaustion_fold_data),
                "utility_exhaustion_attribution_folds": records(attribution_fold_data),
                "utility_entry_timing_folds": records(utility_entry_timing_fold_data),
                "utility_entry_frontier": records(utility_entry_frontier_data),
                "utility_breakout_hold_router_folds": records(breakout_hold_router_fold_data),
                "utility_breakout_add": records(breakout_add_data),
                "utility_taker_flow_recognition": records(taker_flow_recognition_data),
                "utility_taker_matched_permutation": records(taker_matches),
                "utility_breakout_runner_expectancy": records(runner_expectancy_data),
                "utility_breakout_partial_fraction_expectancy": records(partial_fraction_data),
                "utility_regime_partial_router_increment": records(regime_router_data),
                "current_events": records(event_table),
                "candidates": records(candidate_table),
            },
        },
        "sources": sources,
    }
    (output / "artifact.json").write_text(
        json.dumps(artifact, ensure_ascii=True, indent=2), encoding="utf-8"
    )
    print(output / "artifact.json")


if __name__ == "__main__":
    main()
