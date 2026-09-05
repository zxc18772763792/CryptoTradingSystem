"""Diagnose the full fixed entry-rule frontier after the frozen 8h utility score.

The catalog was fixed before confirmation, but only the calibration-selected
5% dip-and-reclaim rule is eligible for historical promotion.  This module
examines all nine confirmation outcomes with catalog-level multiple-testing
control to generate prospective hypotheses.  Any confirmation-only winner is
explicitly quarantined from historical promotion.

All non-immediate triggers use completed 4h bars and enter at the following
4h open within 24 hours.  The module is research-only and has no order-routing
imports.
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
from scipy.stats import fisher_exact


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


TIMING = _load(
    "binance_utility_entry_timing_for_frontier",
    ROOT / "scripts" / "analyze_binance_utility_entry_timing.py",
)
FAILURE = TIMING.FAILURE
CONTINUATION = TIMING.CONTINUATION
ENTRY = TIMING.ENTRY
EXIT = TIMING.EXIT
STUDY = TIMING.STUDY
VALIDATION = TIMING.VALIDATION
POST = TIMING.POST


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def bh_adjust(p_values: pd.Series) -> pd.Series:
    values = pd.to_numeric(p_values, errors="coerce").to_numpy(float)
    order = np.argsort(values)
    adjusted = np.empty(len(values), dtype=float)
    running = 1.0
    for rank_index in range(len(values) - 1, -1, -1):
        original_index = order[rank_index]
        rank = rank_index + 1
        running = min(running, values[original_index] * len(values) / rank)
        adjusted[original_index] = min(1.0, running)
    return pd.Series(adjusted, index=p_values.index)


def precision_bootstrap_fast(
    baseline: pd.DataFrame,
    *,
    filled_keys: set[int],
    cluster: str,
    event_deduplicated: bool,
    samples: int,
    seed: int,
) -> dict[str, Any]:
    rows = TIMING.first_signal_per_event(baseline) if event_deduplicated else baseline.copy()
    rows["filled"] = rows["signal_key"].astype(int).isin(filled_keys).astype(int)
    rows["target"] = rows["target200_14d_from_entry"].astype(int)
    rows["filled_target"] = rows["filled"] * rows["target"]
    if cluster == "week":
        rows["cluster"] = pd.to_datetime(rows["entry_time"], utc=True).dt.to_period("W-SUN").astype(str)
    elif cluster == "symbol":
        rows["cluster"] = rows["symbol"].astype(str)
    else:
        raise ValueError(cluster)
    grouped = rows.groupby("cluster", sort=False).agg(
        total=("target", "size"),
        positives=("target", "sum"),
        filled=("filled", "sum"),
        filled_positives=("filled_target", "sum"),
    )
    stats = grouped[["total", "positives", "filled", "filled_positives"]].to_numpy(float)
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(stats), size=(samples, len(stats)))
    totals = stats[draws].sum(axis=1)
    valid = (totals[:, 0] > 0) & (totals[:, 2] > 0)
    delta = totals[valid, 3] / totals[valid, 2] - totals[valid, 1] / totals[valid, 0]
    values = pd.Series(delta, dtype=float)
    selected = rows[rows["filled"] == 1]
    return {
        "cluster": cluster,
        "event_deduplicated": bool(event_deduplicated),
        "samples": int(len(values)),
        "precision_delta_median": float(values.median()),
        "precision_delta_lower_95pct": float(values.quantile(0.025)),
        "precision_delta_upper_95pct": float(values.quantile(0.975)),
        "base_precision": float(rows["target"].mean()),
        "selected_precision": float(selected["target"].mean()) if len(selected) else 0.0,
    }


def fisher_filter_test(baseline: pd.DataFrame, *, filled_keys: set[int], event_deduplicated: bool) -> float:
    rows = TIMING.first_signal_per_event(baseline) if event_deduplicated else baseline.copy()
    filled = rows[rows["signal_key"].astype(int).isin(filled_keys)]
    unfilled = rows[~rows["signal_key"].astype(int).isin(filled_keys)]
    table = [
        [int(filled["target200_14d_from_entry"].sum()), int(len(filled) - filled["target200_14d_from_entry"].sum())],
        [int(unfilled["target200_14d_from_entry"].sum()), int(len(unfilled) - unfilled["target200_14d_from_entry"].sum())],
    ]
    return float(fisher_exact(table, alternative="greater").pvalue)


def main() -> None:
    args = parse_args()
    baseline_dir = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    entries = pd.read_csv(report / "utility_entry_timing_candidates.csv.gz", compression="gzip")
    for column in ["date", "decision_time", "trigger_time", "entry_time"]:
        entries[column] = pd.to_datetime(entries[column], utc=True)
    confirmation = entries[entries["period"] == "confirmation"].copy()
    baseline_entries = confirmation[confirmation["entry_rule"] == "launch_open"].copy()
    baseline_events = TIMING.first_signal_per_event(baseline_entries)
    panel = pd.read_csv(baseline_dir / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)
    calibration = pd.read_csv(report / "utility_entry_timing_calibration.csv")
    calibration_lookup = calibration.set_index("entry_rule")
    baseline_raw = TIMING.simulate_entries(baseline_entries, bars, funding, days=30, slippage_bps=5.0)

    summary_records: list[dict[str, Any]] = []
    fold_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    paired_frames: list[pd.DataFrame] = []
    market_records: list[dict[str, Any]] = []
    detail: dict[str, dict[str, Any]] = {}
    for rule_index, rule_name in enumerate(POST.POSTLAUNCH_ENTRY_RULES):
        rule_entries = confirmation[confirmation["entry_rule"] == rule_name].copy()
        filled_keys = set(rule_entries["signal_key"].astype(int))
        rec = TIMING.recognition(rule_entries, baseline_entries)
        matched_baseline_entries = baseline_entries[baseline_entries["signal_key"].isin(filled_keys)]
        recognition_pair = TIMING.matched_recognition_delta(matched_baseline_entries, rule_entries)
        matched_label_delta = float(recognition_pair["delta_return"].mean()) if len(recognition_pair) else None
        fold_precision_share = float(np.mean([
            (
                rule_entries[rule_entries["fold"] == fold]["target200_14d_from_entry"].mean()
                > baseline_entries[baseline_entries["fold"] == fold]["target200_14d_from_entry"].mean()
            )
            for fold in (3, 4, 5, 6)
            if len(rule_entries[rule_entries["fold"] == fold])
        ])) if len(rule_entries) else 0.0
        bootstraps = {
            "row_week": precision_bootstrap_fast(
                baseline_entries, filled_keys=filled_keys, cluster="week", event_deduplicated=False,
                samples=args.bootstrap_samples, seed=20260880 + rule_index * 8,
            ),
            "row_symbol": precision_bootstrap_fast(
                baseline_entries, filled_keys=filled_keys, cluster="symbol", event_deduplicated=False,
                samples=args.bootstrap_samples, seed=20260881 + rule_index * 8,
            ),
            "event_week": precision_bootstrap_fast(
                baseline_entries, filled_keys=filled_keys, cluster="week", event_deduplicated=True,
                samples=args.bootstrap_samples, seed=20260882 + rule_index * 8,
            ),
            "event_symbol": precision_bootstrap_fast(
                baseline_entries, filled_keys=filled_keys, cluster="symbol", event_deduplicated=True,
                samples=args.bootstrap_samples, seed=20260883 + rule_index * 8,
            ),
        }
        outcomes = TIMING.simulate_entries(rule_entries, bars, funding, days=30, slippage_bps=5.0)
        trades, _, strategy = TIMING.portfolio(outcomes)
        trades["entry_rule"] = rule_name
        trade_frames.append(trades)
        folds = STUDY.fold_metrics(trades) if len(trades) else pd.DataFrame()
        if len(folds):
            folds.insert(0, "entry_rule", rule_name)
            fold_frames.append(folds)
        matched_baseline_raw = [item for item in baseline_raw if int(item["signal_key"]) in filled_keys]
        paired = FAILURE.paired_deltas(matched_baseline_raw, outcomes)
        paired.insert(0, "entry_rule", rule_name)
        paired_frames.append(paired)
        week_delta = FAILURE.delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="week", seed=20260884 + rule_index * 8) if len(paired) else {}
        symbol_delta = FAILURE.delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="symbol", seed=20260885 + rule_index * 8) if len(paired) else {}
        week_return = EXIT.bootstrap_returns(trades, samples=args.bootstrap_samples, cluster="week", seed=20260886 + rule_index * 8) if len(trades) else {}
        symbol_return = EXIT.bootstrap_returns(trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260887 + rule_index * 8) if len(trades) else {}
        leave_largest, concentration = STUDY.concentration_stats(trades)
        for (fold, state), group in rule_entries.groupby(["fold", "market_state"], dropna=False):
            market_records.append(
                {
                    "entry_rule": rule_name,
                    "fold": int(fold),
                    "market_state": str(state),
                    "entries": int(len(group)),
                    "positives": int(group["target200_14d_from_entry"].sum()),
                    "precision": float(group["target200_14d_from_entry"].mean()),
                }
            )
        record = {
            "entry_rule": rule_name,
            "calibration_selection_eligible": bool(calibration_lookup.loc[rule_name, "selection_eligible"]),
            "calibration_actual_200pct_entries": int(calibration_lookup.loc[rule_name, "actual_200pct_entries"]),
            **rec,
            "matched_label_delta": matched_label_delta,
            "fold_precision_improvement_share": fold_precision_share,
            "row_fisher_p": fisher_filter_test(baseline_entries, filled_keys=filled_keys, event_deduplicated=False),
            "event_fisher_p": fisher_filter_test(baseline_entries, filled_keys=filled_keys, event_deduplicated=True),
            "trades": int(strategy.get("trades", 0)) if strategy else 0,
            "expectancy": strategy.get("expectancy") if strategy else None,
            "profit_factor": strategy.get("profit_factor") if strategy else None,
            "total_return_on_initial_equity": strategy.get("total_return_on_initial_equity") if strategy else None,
            "max_drawdown": strategy.get("max_drawdown") if strategy else None,
            "fold_profit_factor_gt1_share": float((folds["profit_factor"] > 1).mean()) if len(folds) else 0.0,
            "paired_trade_increment_mean": float(paired["delta_return"].mean()) if len(paired) else None,
            "leave_largest_expectancy": leave_largest,
            "largest_positive_pnl_share": concentration,
        }
        summary_records.append(record)
        detail[rule_name] = {
            "recognition": rec,
            "matched_label_delta": matched_label_delta,
            "fold_precision_improvement_share": fold_precision_share,
            "precision_bootstraps": bootstraps,
            "strategy_summary": strategy,
            "fold_metrics": folds.to_dict(orient="records") if len(folds) else [],
            "week_return_bootstrap": week_return,
            "symbol_return_bootstrap": symbol_return,
            "week_trade_increment_bootstrap": week_delta,
            "symbol_trade_increment_bootstrap": symbol_delta,
            "paired_trade_increment_mean": float(paired["delta_return"].mean()) if len(paired) else None,
            "leave_largest_winner_out_expectancy": leave_largest,
            "largest_positive_pnl_share": concentration,
        }

    summary = pd.DataFrame(summary_records)
    nonbaseline = summary["entry_rule"] != "launch_open"
    summary.loc[nonbaseline, "row_fisher_p_bh"] = bh_adjust(summary.loc[nonbaseline, "row_fisher_p"])
    summary.loc[nonbaseline, "event_fisher_p_bh"] = bh_adjust(summary.loc[nonbaseline, "event_fisher_p"])
    summary.loc[~nonbaseline, ["row_fisher_p_bh", "event_fisher_p_bh"]] = np.nan
    recognition_pass: list[bool] = []
    trade_pass: list[bool] = []
    for _, row in summary.iterrows():
        rule_name = str(row["entry_rule"])
        item = detail[rule_name]
        recognition_ok = bool(
            rule_name != "launch_open"
            and row["fill_rate"] >= 0.20
            and row["positive_retention"] >= 0.50
            and row["positive_event_retention"] >= 0.50
            and row["fold_precision_improvement_share"] >= 0.75
            and row["row_fisher_p_bh"] < 0.05
            and row["event_fisher_p_bh"] < 0.05
            and all(
                boot["precision_delta_lower_95pct"] > 0
                for boot in item["precision_bootstraps"].values()
            )
        )
        strategy_ok = bool(
            recognition_ok
            and row["expectancy"] is not None and row["expectancy"] > 0
            and row["profit_factor"] is not None and row["profit_factor"] > 1
            and row["fold_profit_factor_gt1_share"] > 0.50
            and row["leave_largest_expectancy"] is not None and row["leave_largest_expectancy"] > 0
            and row["largest_positive_pnl_share"] is not None and row["largest_positive_pnl_share"] <= 0.25
            and item["week_return_bootstrap"].get("expectancy_lower_95pct", -1) > 0
            and item["symbol_return_bootstrap"].get("expectancy_lower_95pct", -1) > 0
            and item["week_trade_increment_bootstrap"].get("mean_delta_lower_95pct", -1) > 0
            and item["symbol_trade_increment_bootstrap"].get("mean_delta_lower_95pct", -1) > 0
        )
        recognition_pass.append(recognition_ok)
        trade_pass.append(strategy_ok)
    summary["confirmation_diagnostic_recognition_pass"] = recognition_pass
    summary["confirmation_diagnostic_trade_pass"] = trade_pass
    summary = summary.sort_values(
        ["confirmation_diagnostic_trade_pass", "confirmation_diagnostic_recognition_pass", "event_precision", "actual_200pct_precision"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    prospective = summary[summary["confirmation_diagnostic_recognition_pass"]]
    prospective_rule = str(prospective.iloc[0]["entry_rule"]) if len(prospective) else None

    summary.to_csv(report / "utility_entry_frontier_summary.csv", index=False)
    pd.concat(fold_frames, ignore_index=True).to_csv(report / "utility_entry_frontier_folds.csv", index=False)
    pd.concat(trade_frames, ignore_index=True).to_csv(report / "utility_entry_frontier_trades.csv.gz", index=False, compression="gzip")
    pd.concat(paired_frames, ignore_index=True).to_csv(report / "utility_entry_frontier_paired_deltas.csv", index=False)
    pd.DataFrame(market_records).to_csv(report / "utility_entry_frontier_market_states.csv", index=False)
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_historical_confirmation_catalog_diagnostic",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "candidate_catalog_fixed_before_confirmation": True,
        "calibration_selected_rule": "discount5_reclaim",
        "confirmation_catalog_inspected_for_hypothesis_generation": True,
        "catalog_multiple_testing_method": "BH-adjusted one-sided Fisher exact tests over eight nonbaseline rules",
        "baseline_recognition": TIMING.recognition(baseline_entries, baseline_entries),
        "baseline_independent_events": int(len(baseline_events)),
        "rules": detail,
        "prospective_rule": prospective_rule,
        "prospective_rule_recognition_diagnostic_pass": bool(len(prospective)),
        "prospective_rule_trade_diagnostic_pass": bool(
            len(prospective) and bool(prospective.iloc[0]["confirmation_diagnostic_trade_pass"])
        ),
        "historical_promotion_allowed": False,
        "why_no_historical_promotion": "the prospective rule was identified after inspecting confirmation outcomes and had zero calibration positives",
        "primary_entry_changed": False,
        "forward_counterfactual_added": False,
        "automatic_trading_allowed": False,
    }
    (report / "utility_entry_frontier_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
