"""Independent consistency verifier for sequential launch research outputs."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def close(left: float, right: float, tolerance: float = 1e-10) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=tolerance, atol=tolerance))


def profit_factor(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def event_metrics(rows: pd.DataFrame) -> dict[str, float | int]:
    records: list[tuple[bool, bool]] = []
    for _, group in rows.sort_values(["symbol", "date"]).groupby("symbol"):
        cluster: list[pd.Series] = []
        previous: pd.Timestamp | None = None
        for _, row in group.iterrows():
            when = pd.Timestamp(row["date"])
            if previous is not None and when - previous > pd.Timedelta(days=14):
                frame = pd.DataFrame(cluster)
                records.append((bool(frame["original_target200"].any()), bool(frame["simple_selected"].any())))
                cluster = []
            cluster.append(row)
            previous = when
        if cluster:
            frame = pd.DataFrame(cluster)
            records.append((bool(frame["original_target200"].any()), bool(frame["simple_selected"].any())))
    frame = pd.DataFrame(records, columns=["positive", "selected"])
    selected = frame[frame["selected"]]
    return {
        "clusters": int(len(frame)),
        "positive_events": int(frame["positive"].sum()),
        "selected_clusters": int(len(selected)),
        "selected_positive_events": int(selected["positive"].sum()),
        "selected_event_precision": float(selected["positive"].mean()),
        "positive_event_retention": float(selected["positive"].sum() / frame["positive"].sum()),
    }


def ranked_event_capture(rows: pd.DataFrame, pctile_column: str) -> dict[str, float | int]:
    positive = rows[rows["label_primary_int"].astype(bool)].sort_values(["symbol", "date"])
    captured: list[bool] = []
    for _, group in positive.groupby("symbol"):
        cluster: list[pd.Series] = []
        previous: pd.Timestamp | None = None
        for _, row in group.iterrows():
            when = pd.Timestamp(row["date"])
            if previous is not None and when - previous > pd.Timedelta(days=14):
                frame = pd.DataFrame(cluster)
                captured.append(bool((frame[pctile_column] >= 0.98).any()))
                cluster = []
            cluster.append(row)
            previous = when
        if cluster:
            frame = pd.DataFrame(cluster)
            captured.append(bool((frame[pctile_column] >= 0.98).any()))
    return {
        "events": int(len(captured)),
        "captured": int(sum(captured)),
        "capture_rate": float(np.mean(captured)) if captured else np.nan,
    }


def default_trade_metrics(path: Path) -> dict[str, float | int]:
    trades = pd.read_csv(path, compression="gzip")
    trades = trades[np.isclose(trades["slippage_bps_each_side"].astype(float), 5.0)]
    return {
        "trades": int(len(trades)),
        "expectancy": float(trades["net_return"].mean()),
        "profit_factor": float(profit_factor(trades["net_return"])),
    }


def main() -> None:
    model = json.loads((REPORT / "sequential_strategy_decision.json").read_text(encoding="utf-8"))
    robustness = json.loads((REPORT / "sequential_robustness_decision.json").read_text(encoding="utf-8"))
    simple = json.loads((REPORT / "simple_launch_strategy_decision.json").read_text(encoding="utf-8"))
    stop = json.loads((REPORT / "launch_stop_strategy_decision.json").read_text(encoding="utf-8"))
    state = json.loads((REPORT / "launch_exit_state_decision.json").read_text(encoding="utf-8"))
    postlaunch = json.loads((REPORT / "postlaunch_entry_decision.json").read_text(encoding="utf-8"))
    hybrid = json.loads((REPORT / "hybrid_stop_decision.json").read_text(encoding="utf-8"))
    staged = json.loads((REPORT / "staged_entry_decision.json").read_text(encoding="utf-8"))
    lifecycle = json.loads((REPORT / "lifecycle_factor_decision.json").read_text(encoding="utf-8"))

    model_cal = pd.read_csv(REPORT / "sequential_model_calibration.csv")
    model_scored = pd.read_csv(REPORT / "sequential_model_confirmation_scored.csv.gz", compression="gzip")
    labels = model_scored["original_target200"].astype(int)
    selected = model_scored[model_scored["sequence_selected"].astype(bool)]
    model_checks = {
        "selected_from_calibration": (
            int(model_cal.iloc[0]["checkpoint_hours"]) == int(model["selected_checkpoint_hours"])
            and str(model_cal.iloc[0]["model_family"]) == str(model["selected_model_family"])
        ),
        "confirmation_rows": len(model_scored) == int(model["recognition_confirmation"]["rows"]),
        "confirmation_positives": int(labels.sum()) == int(model["recognition_confirmation"]["positives"]),
        "selected_rows": len(selected) == int(model["recognition_confirmation"]["selected_rows"]),
        "selected_positives": int(selected["original_target200"].sum()) == int(model["recognition_confirmation"]["selected_positives"]),
        "sequence_ap": close(
            average_precision_score(labels, model_scored["sequence_score"]),
            model["recognition_confirmation"]["sequence_average_precision"],
        ),
        "price_ap": close(
            average_precision_score(labels, model_scored["price_model_score"]),
            model["recognition_confirmation"]["price_average_precision"],
        ),
        "not_promoted_when_calibration_ineligible": (
            not bool(model["model_calibration_eligible"])
            and not bool(model["ranking_gate_pass"])
            and model["classification"] == "watchlist_only"
        ),
    }

    candidates = pd.read_csv(REPORT / "sequential_checkpoint_candidates.csv.gz", compression="gzip")
    candidates["date"] = pd.to_datetime(candidates["date"], utc=True)
    confirmation = candidates[(candidates["checkpoint_hours"] == 24) & (candidates["fold"] >= 3)].copy()
    confirmation["simple_selected"] = (
        (confirmation["early_mfe"] >= 0.20) & (confirmation["early_close_return"] >= 0.05)
    )
    simple_selected = confirmation[confirmation["simple_selected"]]
    events = event_metrics(confirmation)
    simple_cal = pd.read_csv(REPORT / "sequential_simple_rule_calibration.csv")
    simple_mode_cal = pd.read_csv(REPORT / "simple_launch_trade_mode_calibration.csv")
    reported_events = simple["event_recognition_confirmation"]
    simple_checks = {
        "rule_selected_from_calibration": str(simple_cal.iloc[0]["rule"]) == str(simple["selected_rule"]),
        "mode_selected_from_calibration": str(simple_mode_cal.iloc[0]["trade_mode"]) == str(simple["selected_trade_mode"]),
        "rows": len(confirmation) == int(simple["recognition_confirmation"]["rows"]),
        "selected_rows": len(simple_selected) == int(simple["recognition_confirmation"]["selected_rows"]),
        "selected_positives": int(simple_selected["original_target200"].sum()) == int(simple["recognition_confirmation"]["selected_positives"]),
        "precision": close(simple_selected["original_target200"].mean(), simple["recognition_confirmation"]["selected_precision"]),
        "event_clusters": events["clusters"] == int(reported_events["clusters"]),
        "event_positive_count": events["selected_positive_events"] == int(reported_events["selected_positive_events"]),
        "event_precision": close(events["selected_event_precision"], reported_events["selected_event_precision"]),
        "event_retention": close(events["positive_event_retention"], reported_events["positive_event_retention"]),
        "secondary_evidence_not_promoted": (
            simple["evidence_class"] == "historical_secondary_after_prior_path_exploration"
            and simple["classification"] == "watchlist_only"
            and not bool(simple["automatic_trading_allowed"])
        ),
    }

    stop_cal = pd.read_csv(REPORT / "launch_stop_calibration.csv")
    stop_metrics = default_trade_metrics(REPORT / "launch_stop_confirmation_trades.csv.gz")
    stop_checks = {
        "selected_from_calibration": close(stop_cal.iloc[0]["stop_width"], stop["selected_stop_width"]),
        "risk_equal": bool(stop["risk_equal_sizing"]),
        "trade_count": stop_metrics["trades"] == int(stop["strategy_default_summary"]["trades"]),
        "expectancy": close(stop_metrics["expectancy"], stop["strategy_default_summary"]["expectancy"]),
        "profit_factor": close(stop_metrics["profit_factor"], stop["strategy_default_summary"]["profit_factor"]),
        "failed_gate_not_promoted": not bool(stop["historical_trade_gate_pass"]) and stop["classification"] == "watchlist_only",
    }

    state_cal = pd.read_csv(REPORT / "launch_exit_state_calibration.csv")
    state_metrics = default_trade_metrics(REPORT / "launch_exit_state_trades.csv.gz")
    state_checks = {
        "selected_from_calibration": str(state_cal.iloc[0]["policy"]) == str(state["selected_exit_policy"]),
        "trade_count": state_metrics["trades"] == int(state["strategy_default_summary"]["trades"]),
        "expectancy": close(state_metrics["expectancy"], state["strategy_default_summary"]["expectancy"]),
        "profit_factor": close(state_metrics["profit_factor"], state["strategy_default_summary"]["profit_factor"]),
        "fold_gate_failed": close(state["fold_majority_profit_factor_gt_1"], 0.5),
        "uncertainty_gate_failed": (
            float(state["week_bootstrap"]["expectancy_lower_95pct"]) < 0
            and float(state["symbol_bootstrap"]["expectancy_lower_95pct"]) < 0
        ),
        "failed_gate_not_promoted": not bool(state["historical_trade_gate_pass"]) and state["classification"] == "watchlist_only",
    }

    postlaunch_cal = pd.read_csv(REPORT / "postlaunch_entry_calibration.csv")
    postlaunch_candidates = pd.read_csv(REPORT / "postlaunch_entry_candidates.csv.gz", compression="gzip")
    postlaunch_selected = postlaunch_candidates[
        (postlaunch_candidates["fold"].isin([3, 4, 5, 6]))
        & (postlaunch_candidates["postlaunch_entry_rule"] == postlaunch["selected_entry_rule"])
    ]
    postlaunch_baseline = postlaunch_candidates[
        (postlaunch_candidates["fold"].isin([3, 4, 5, 6]))
        & (postlaunch_candidates["postlaunch_entry_rule"] == "launch_open")
    ]
    postlaunch_metrics = default_trade_metrics(REPORT / "postlaunch_entry_confirmation_trades.csv.gz")
    regimes = pd.read_csv(REPORT / "postlaunch_regime_diagnostics.csv")

    def regime_precision(period: str, dimension: str, regime: str) -> float:
        row = regimes[
            (regimes["period"] == period)
            & (regimes["dimension"] == dimension)
            & (regimes["regime"] == regime)
        ]
        return float(row.iloc[0]["actual_200pct_precision"])

    postlaunch_checks = {
        "selected_from_calibration": str(postlaunch_cal.iloc[0]["entry_rule"]) == str(postlaunch["selected_entry_rule"]),
        "actual_label_consistency": bool(
            (postlaunch_candidates["target200_14d_from_entry"].astype(bool)
             == (postlaunch_candidates["future_max_return_14d_from_entry"] >= 2.0)).all()
        ),
        "baseline_entries": len(postlaunch_baseline) == int(postlaunch["confirmation_recognition_baseline"]["entries"]),
        "selected_entries": len(postlaunch_selected) == int(postlaunch["confirmation_recognition_selected"]["entries"]),
        "selected_positives": int(postlaunch_selected["target200_14d_from_entry"].sum()) == int(postlaunch["confirmation_recognition_selected"]["actual_200pct_entries"]),
        "selected_precision": close(postlaunch_selected["target200_14d_from_entry"].mean(), postlaunch["confirmation_recognition_selected"]["actual_200pct_precision"]),
        "recognition_deteriorated": (
            float(postlaunch["confirmation_recognition_selected"]["actual_200pct_precision"])
            < float(postlaunch["confirmation_recognition_baseline"]["actual_200pct_precision"])
            and float(postlaunch["confirmation_recognition_selected"]["positive_retention_vs_launch_open"]) < 0.40
        ),
        "trade_count": postlaunch_metrics["trades"] == int(postlaunch["strategy_default_summary"]["trades"]),
        "expectancy": close(postlaunch_metrics["expectancy"], postlaunch["strategy_default_summary"]["expectancy"]),
        "profit_factor": close(postlaunch_metrics["profit_factor"], postlaunch["strategy_default_summary"]["profit_factor"]),
        "largest_winner_dependency_failed": float(postlaunch["leave_largest_winner_out_expectancy"]) < 0,
        "regime_direction_reversed": (
            regime_precision("calibration", "btc_trend_regime", "bear")
            > regime_precision("calibration", "btc_trend_regime", "bull")
            and regime_precision("confirmation", "btc_trend_regime", "bear")
            < regime_precision("confirmation", "btc_trend_regime", "bull")
            and regime_precision("calibration", "breadth_regime", "narrow")
            > regime_precision("calibration", "breadth_regime", "broad")
            and regime_precision("confirmation", "breadth_regime", "narrow")
            < regime_precision("confirmation", "breadth_regime", "broad")
        ),
        "regime_not_used_for_selection": not bool(postlaunch["regime_diagnostic"]["selection_allowed"]),
        "failed_gate_not_promoted": (
            not bool(postlaunch["historical_trade_gate_pass"])
            and postlaunch["classification"] == "watchlist_only"
            and not bool(postlaunch["automatic_trading_allowed"])
        ),
    }

    hybrid_cal = pd.read_csv(REPORT / "hybrid_stop_calibration.csv")
    hybrid_path = pd.read_csv(REPORT / "hybrid_stop_path_summary.csv")
    hybrid_metrics = default_trade_metrics(REPORT / "hybrid_stop_confirmation_trades.csv.gz")
    winner50 = hybrid_path[
        (hybrid_path["period"] == "confirmation")
        & np.isclose(hybrid_path["target_return"], 0.50)
        & (hybrid_path["population"] == "eventual_200pct")
    ].iloc[0]
    hybrid_checks = {
        "selected_from_calibration": str(hybrid_cal.iloc[0]["policy"]) == str(hybrid["selected_policy"]),
        "baseline_hard25_retained": hybrid["selected_policy"] == "baseline_hard25",
        "winner_path_count": int(winner50["signals"]) == 8,
        "winner_stop_before_50_share": close(winner50["prior_intrabar_below_minus25_share"], 0.125),
        "winner_close_below_20_share": close(winner50["prior_close_below_minus20_share"], 0.0),
        "trade_count": hybrid_metrics["trades"] == int(hybrid["strategy_default_summary"]["trades"]),
        "expectancy": close(hybrid_metrics["expectancy"], hybrid["strategy_default_summary"]["expectancy"]),
        "profit_factor": close(hybrid_metrics["profit_factor"], hybrid["strategy_default_summary"]["profit_factor"]),
        "no_increment": not bool(hybrid["incremental_vs_hard25"]),
        "failed_gate_not_promoted": not bool(hybrid["historical_trade_gate_pass"]) and hybrid["classification"] == "watchlist_only",
    }

    staged_cal = pd.read_csv(REPORT / "staged_entry_calibration.csv")
    staged_metrics = default_trade_metrics(REPORT / "staged_entry_confirmation_trades.csv.gz")
    staged_checks = {
        "selected_from_calibration": close(staged_cal.iloc[0]["probe_fraction"], staged["selected_probe_fraction"]),
        "confirmation_only_retained": close(staged["selected_probe_fraction"], 0.0) and close(staged["selected_add_fraction_after_launch"], 1.0),
        "trade_count": staged_metrics["trades"] == int(staged["strategy_default_summary"]["trades"]),
        "expectancy": close(staged_metrics["expectancy"], staged["strategy_default_summary"]["expectancy"]),
        "profit_factor": close(staged_metrics["profit_factor"], staged["strategy_default_summary"]["profit_factor"]),
        "no_increment": not bool(staged["incremental_vs_confirmation_only"]),
        "failed_gate_not_promoted": not bool(staged["historical_trade_gate_pass"]) and staged["classification"] == "watchlist_only",
    }

    lifecycle_scored = pd.read_csv(REPORT / "lifecycle_factor_scored.csv.gz", compression="gzip")
    lifecycle_folds = pd.read_csv(REPORT / "lifecycle_factor_fold_metrics.csv")
    lifecycle_models = pd.read_csv(REPORT / "lifecycle_factor_models.csv")
    lifecycle_labels = lifecycle_scored["label_primary_int"].astype(int)
    lifecycle_price_selected = lifecycle_labels[lifecycle_scored["price_model_pctile"] >= 0.98]
    lifecycle_combo_selected = lifecycle_labels[lifecycle_scored["lifecycle_combo_pctile"] >= 0.98]
    lifecycle_price_events = ranked_event_capture(lifecycle_scored, "price_model_pctile")
    lifecycle_combo_events = ranked_event_capture(lifecycle_scored, "lifecycle_combo_pctile")
    lifecycle_checks = {
        "rows": len(lifecycle_scored) == int(lifecycle["price_pooled"]["n"]),
        "positives": int(lifecycle_labels.sum()) == int(lifecycle["price_pooled"]["positives"]),
        "price_ap": close(average_precision_score(lifecycle_labels, lifecycle_scored["price_model_score"]), lifecycle["price_pooled"]["average_precision"]),
        "combo_ap": close(average_precision_score(lifecycle_labels, lifecycle_scored["lifecycle_combo_score"]), lifecycle["price_plus_age_pooled"]["average_precision"]),
        "price_precision": close(lifecycle_price_selected.mean(), lifecycle["price_pooled"]["top_2pct_precision"]),
        "combo_precision": close(lifecycle_combo_selected.mean(), lifecycle["price_plus_age_pooled"]["top_2pct_precision"]),
        "fold_joint_share": close((lifecycle_folds["ap_improved"].astype(bool) & lifecycle_folds["lift_improved"].astype(bool)).mean(), lifecycle["fold_joint_ap_and_lift_improvement_share"]),
        "coefficients_all_negative": bool((lifecycle_models["standardized_listing_age_coefficient"] < 0).all()),
        "price_events": lifecycle_price_events["captured"] == int(lifecycle["price_event_capture"]["captured"]),
        "combo_events": lifecycle_combo_events["captured"] == int(lifecycle["price_plus_age_event_capture"]["captured"]),
        "increment_failed": (
            float(lifecycle["price_plus_age_pooled"]["average_precision"]) < float(lifecycle["price_pooled"]["average_precision"])
            and float(lifecycle["paired_week_bootstrap"]["average_precision_delta"]["upper_95pct"]) < 0
        ),
        "failed_gate_not_promoted": not bool(lifecycle["historical_incremental_gate_pass"]) and lifecycle["classification"] == "annotation_only" and not bool(lifecycle["frozen_price_ranking_changed"]),
    }

    robustness_checks = {
        "permutation_ap_significant": float(robustness["permutation_null"]["ap_one_sided_pvalue"]) < 0.01,
        "permutation_precision_significant": float(robustness["permutation_null"]["precision_one_sided_pvalue"]) < 0.01,
        "leave_one_symbol_ap_positive": float(robustness["leave_one_symbol_out"]["ap_delta_positive_share"]) == 1.0,
        "leave_one_symbol_precision_positive": float(robustness["leave_one_symbol_out"]["precision_delta_positive_share"]) == 1.0,
        "status_is_secondary": robustness["status"] == "secondary_historical_robustness_not_new_oos",
    }
    sections = {
        "sequence_model": model_checks,
        "simple_rule": simple_checks,
        "stop_width": stop_checks,
        "stateful_exit": state_checks,
        "postlaunch_entry": postlaunch_checks,
        "hybrid_stop": hybrid_checks,
        "staged_entry": staged_checks,
        "lifecycle_factor": lifecycle_checks,
        "robustness": robustness_checks,
    }
    passed = all(all(checks.values()) for checks in sections.values())
    output = {
        "passed": passed,
        "sections": {
            name: {"passed": all(checks.values()), "checks": checks}
            for name, checks in sections.items()
        },
    }
    (REPORT / "sequential_strategy_verification.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(output, ensure_ascii=False, indent=2))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
