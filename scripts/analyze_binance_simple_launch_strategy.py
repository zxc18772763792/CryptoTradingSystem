"""Trade-test the calibrated interpretable 24h launch rule.

The rule is selected on fully matured pre-confirmation rows.  Trading mode is
also selected before the fold-3 cutoff.  Because the rule family was created
after earlier path exploration, any passing result remains a forward-only
challenger and is never promoted directly to live or paper-tradable status.
"""

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
BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ROBUST = _load(
    "binance_sequence_robustness_for_simple_strategy",
    ROOT / "scripts" / "analyze_binance_sequence_robustness.py",
)
PRIMARY = ROBUST.PRIMARY
ENTRY = PRIMARY.ENTRY
VALIDATION = PRIMARY.VALIDATION
EXIT = PRIMARY.EXIT


def bootstrap_precision(rows: pd.DataFrame, *, samples: int = 5000) -> dict[str, Any]:
    data = rows.copy()
    data["week"] = data["date"].dt.to_period("W-SUN").astype(str)
    groups = [group.index.to_numpy() for _, group in data.groupby("week", sort=False)]
    rng = np.random.default_rng(20260803)
    deltas: list[float] = []
    for _ in range(samples):
        indices = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        draw = data.loc[indices]
        selected = draw[draw["sequence_selected"]]
        if selected.empty:
            continue
        deltas.append(float(selected["original_target200"].mean() - draw["original_target200"].mean()))
    values = pd.Series(deltas, dtype=float)
    return {
        "samples": int(len(values)),
        "precision_delta_median": float(values.median()),
        "precision_delta_lower_95pct": float(values.quantile(0.025)),
        "precision_delta_upper_95pct": float(values.quantile(0.975)),
    }


def main() -> None:
    calibration = pd.read_csv(REPORT / "sequential_simple_rule_calibration.csv")
    selected_rule = str(calibration.iloc[0]["rule"])
    rule_eligible = bool(calibration.iloc[0]["selection_eligible"])
    if selected_rule not in ROBUST.RULES:
        raise RuntimeError(f"Unknown selected rule: {selected_rule}")
    candidates = pd.read_csv(REPORT / "sequential_checkpoint_candidates.csv.gz", compression="gzip")
    for column in ["date", "baseline_entry_time", "decision_time"]:
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    rows = candidates[candidates["checkpoint_hours"] == 24].copy()
    rows["sequence_selected"] = ROBUST.RULES[selected_rule][1](rows).fillna(False)
    # Keep ordering tied to the already-frozen price score.  The simple rule is
    # a binary gate and does not introduce a post-hoc strength ranking.
    rows["sequence_score"] = rows["price_model_score"]

    folds = pd.read_csv(REPORT / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])
    panel = pd.read_csv(BASELINE / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)

    calibration_rows = rows[rows["decision_time"] + pd.Timedelta(days=14) < cutoff].copy()
    mode_calibration = PRIMARY.trade_mode_calibration(
        calibration_rows,
        bars,
        funding,
        cutoff=cutoff,
    )
    mode_calibration.to_csv(REPORT / "simple_launch_trade_mode_calibration.csv", index=False)
    selected_mode = str(mode_calibration.iloc[0]["trade_mode"])
    mode_eligible = bool(mode_calibration.iloc[0]["selection_eligible"])

    confirmation = rows[rows["fold"].isin([3, 4, 5, 6])].copy()
    recognition = ROBUST.rule_metrics(confirmation, confirmation["sequence_selected"])
    fold_records: list[dict[str, Any]] = []
    for fold, group in confirmation.groupby("fold"):
        metrics = ROBUST.rule_metrics(group, group["sequence_selected"])
        metrics["fold"] = int(fold)
        metrics["precision_improved"] = bool(metrics["selected_precision"] > metrics["base_precision"])
        fold_records.append(metrics)
    recognition_folds = pd.DataFrame(fold_records)
    recognition_folds.to_csv(REPORT / "simple_launch_recognition_folds.csv", index=False)
    recognition_bootstrap = bootstrap_precision(confirmation)
    recognition_fold_majority = float(recognition_folds["precision_improved"].mean())
    event_clusters = ROBUST.event_clusters(confirmation)
    positive_events = event_clusters["positive_event"].astype(bool)
    selected_events = event_clusters[event_clusters["sequence_selected"]]
    event_recognition = {
        "clusters": int(len(event_clusters)),
        "positive_events": int(positive_events.sum()),
        "base_event_precision": float(positive_events.mean()),
        "selected_clusters": int(len(selected_events)),
        "selected_positive_events": int(selected_events["positive_event"].sum()),
        "selected_event_precision": float(selected_events["positive_event"].mean()) if len(selected_events) else 0.0,
        "positive_event_retention": float(
            selected_events["positive_event"].sum() / positive_events.sum()
        ) if positive_events.sum() else 0.0,
    }
    recognition_gate = bool(
        rule_eligible and recognition_fold_majority > 0.50
        and recognition["selected_precision"] > recognition["base_precision"]
        and recognition["positive_retention"] >= 0.25
        and recognition_bootstrap["precision_delta_lower_95pct"] > 0
    )

    # These confirmation-mode results are explicitly diagnostic.  They are
    # not allowed to replace the mode selected above on calibration data.
    diagnostic_modes: list[dict[str, Any]] = []
    for mode in PRIMARY.TRADE_MODES:
        outcomes = PRIMARY.simulate_mode(
            confirmation,
            bars,
            funding,
            mode=mode,
            slippage_bps=5.0,
        )
        diagnostic_trades, _, diagnostic_summary = PRIMARY.summarize_outcomes(outcomes)
        if not diagnostic_summary:
            continue
        diagnostic_folds = PRIMARY.STUDY.fold_metrics(diagnostic_trades)
        diagnostic_modes.append(
            {
                "trade_mode": mode,
                "trades": int(diagnostic_summary["trades"]),
                "expectancy": float(diagnostic_summary["expectancy"]),
                "profit_factor": float(diagnostic_summary["profit_factor"]),
                "max_drawdown": float(diagnostic_summary["max_drawdown"]),
                "fold_majority_profit_factor_gt_1": float(
                    (diagnostic_folds["profit_factor"] > 1).mean()
                ) if not diagnostic_folds.empty else 0.0,
                "selection_allowed": mode == selected_mode,
            }
        )
    pd.DataFrame(diagnostic_modes).to_csv(
        REPORT / "simple_launch_confirmation_mode_diagnostics.csv", index=False
    )

    summary_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = PRIMARY.simulate_mode(
            confirmation,
            bars,
            funding,
            mode=selected_mode,
            slippage_bps=slippage,
        )
        trades, equity, summary = PRIMARY.summarize_outcomes(outcomes)
        if not summary:
            continue
        summary.update(
            {
                "rule": selected_rule,
                "trade_mode": selected_mode,
                "slippage_bps_each_side": slippage,
            }
        )
        summary_records.append(summary)
        trades["slippage_bps_each_side"] = slippage
        trade_records.extend(trades.to_dict(orient="records"))
        if not equity.empty:
            equity["slippage_bps_each_side"] = slippage
            equity_records.extend(equity.to_dict(orient="records"))
        if slippage == 5.0:
            default_trades = trades
            default_summary = summary
    summaries = pd.DataFrame(summary_records)
    all_trades = pd.DataFrame(trade_records)
    all_equity = pd.DataFrame(equity_records)
    trade_folds = PRIMARY.STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summaries.to_csv(REPORT / "simple_launch_strategy_summary.csv", index=False)
    all_trades.to_csv(REPORT / "simple_launch_strategy_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(REPORT / "simple_launch_strategy_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(REPORT / "simple_launch_strategy_folds.csv", index=False)

    week = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="week", seed=20260804) if not default_trades.empty else {}
    symbol = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="symbol", seed=20260805) if not default_trades.empty else {}
    leave_largest, concentration = PRIMARY.STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summaries) == 3 and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all() and (summaries["max_drawdown"] >= -0.30).all()
    )
    historical_trade_gate = bool(
        recognition_gate and mode_eligible and default_summary
        and default_summary.get("expectancy", -1) > 0 and default_summary.get("profit_factor", 0) > 1
        and fold_majority > 0.50 and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week.get("expectancy_lower_95pct", -1) > 0
        and symbol.get("expectancy_lower_95pct", -1) > 0
    )
    output = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "historical_secondary_after_prior_path_exploration",
        "selected_rule": selected_rule,
        "selected_rule_description": ROBUST.RULES[selected_rule][0],
        "rule_calibration_eligible": rule_eligible,
        "selected_trade_mode": selected_mode,
        "trade_mode_calibration_eligible": mode_eligible,
        "recognition_confirmation": recognition,
        "recognition_fold_metrics": recognition_folds.to_dict(orient="records"),
        "recognition_fold_improvement_share": recognition_fold_majority,
        "recognition_bootstrap": recognition_bootstrap,
        "event_recognition_confirmation": event_recognition,
        "recognition_historical_gate_pass": recognition_gate,
        "confirmation_mode_diagnostics_not_for_selection": diagnostic_modes,
        "strategy_default_summary": default_summary,
        "strategy_stress_costs": summaries[["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]].to_dict(orient="records") if not summaries.empty else [],
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "strategy_fold_majority_profit_factor_gt_1": fold_majority,
        "strategy_week_bootstrap": week,
        "strategy_symbol_bootstrap": symbol,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "historical_trade_gate_pass": historical_trade_gate,
        "classification": "paper_challenger_for_forward_oos" if historical_trade_gate else "watchlist_only",
        "automatic_trading_allowed": False,
    }
    (REPORT / "simple_launch_strategy_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
