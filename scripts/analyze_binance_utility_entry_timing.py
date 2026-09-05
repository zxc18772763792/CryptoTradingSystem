"""Calibrate and confirm execution timing after the frozen P0+8h utility score.

The frozen utility model, q70 threshold, daily top-three limit, and stage vetoes
are unchanged.  This study asks whether a selected signal should enter at the
P0+8h decision open or wait up to 24 hours for one of the already-defined
time-safe pullback, reclaim, or breakout triggers.  Any completed-bar trigger
enters at the following 4h open.

Entry-rule selection uses only the three mature calibration windows.  Folds
3-6 are confirmation.  Missing triggers are reported as missed signals rather
than silently removed from recognition-retention metrics.  The module is
research-only and has no order-routing imports.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"
TRIGGER_WINDOW_HOURS = 24
ENTRY_QUANTILE = 0.70
ENTRY_CHECKPOINT_HOURS = 8


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


FAILURE = _load(
    "binance_utility_failure_exit_for_entry_timing",
    ROOT / "scripts" / "analyze_binance_utility_failure_exit.py",
)
POST = _load(
    "binance_postlaunch_validation_for_utility_entry_timing",
    ROOT / "core" / "research" / "binance_postlaunch_validation.py",
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


def build_entries(signals: pd.DataFrame, bars: Mapping[str, pd.DataFrame], *, period: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    keep = [
        "symbol", "date", "fold", "validation_split", "signal_key", "price_model_score",
        "price_model_pctile", "continuation_score", "continuation_threshold",
        "baseline_entry_time", "baseline_entry_open", "decision_time", "decision_open",
        "late_target200", "late_future_max_return_14d", "utility_positive_14d",
        "breadth_regime", "btc_trend_regime", "btc_vol_regime", "market_state",
    ]
    for _, signal in signals[signals["selected"]].iterrows():
        symbol_bars = bars.get(str(signal["symbol"]))
        if symbol_bars is None:
            continue
        base = {column: signal.get(column) for column in keep}
        for rule_name in POST.POSTLAUNCH_ENTRY_RULES:
            located = POST.locate_postlaunch_entry(
                signal,
                symbol_bars,
                rule_name=rule_name,
                trigger_window_hours=TRIGGER_WINDOW_HOURS,
            )
            if located is None:
                continue
            row = dict(base)
            row.update(located)
            row["period"] = period
            row["entry_rule"] = rule_name
            row["entry_id"] = f"{signal['signal_key']}|{rule_name}"
            records.append(row)
    result = pd.DataFrame(records)
    if not result.empty:
        for column in ["date", "baseline_entry_time", "decision_time", "trigger_time", "entry_time"]:
            result[column] = pd.to_datetime(result[column], utc=True)
    return result


def simulate_entries(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    days: int,
    slippage_bps: float,
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    policy = CONTINUATION.frozen_policy(days)
    for _, row in entries.iterrows():
        symbol = str(row["symbol"])
        frame = bars.get(symbol)
        outcome = None if frame is None else ENTRY.simulate_stateful_trade(
            frame,
            entry_time=pd.Timestamp(row["entry_time"]),
            policy=policy,
            slippage_bps=slippage_bps,
            funding=funding.get(symbol),
            fee_bps=5.0,
            include_marks=True,
        )
        if outcome is None:
            continue
        outcome.update(
            {
                "symbol": symbol,
                "signal_date": pd.Timestamp(row["date"]),
                "fold": int(row["fold"]),
                "validation_split": str(row["validation_split"]),
                "score": float(row["continuation_score"]),
                "score_pctile": float(row["price_model_pctile"]),
                "signal_key": int(row["signal_key"]),
                "entry_rule": str(row["entry_rule"]),
                "entry_delay_hours": float(row["entry_delay_hours_after_launch"]),
                "entry_chase_vs_signal_close": float(row["entry_return_vs_decision_open"]),
                "target200_14d_from_entry": bool(row["target200_14d_from_entry"]),
                "future_max_return_14d_from_entry": float(row["future_max_return_14d_from_entry"]),
                "breadth_regime": row.get("breadth_regime"),
                "btc_trend_regime": row.get("btc_trend_regime"),
                "btc_vol_regime": row.get("btc_vol_regime"),
                "market_state": row.get("market_state"),
                "policy": "hard25_be30_half100_trail25",
                "slippage_bps_each_side": float(slippage_bps),
            }
        )
        outcomes.append(outcome)
    return outcomes


def portfolio(outcomes: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    accepted, equity, summary = EXIT.summarize_portfolio(outcomes, risk_stop_pct=0.25)
    trades = pd.DataFrame([EXIT.clean_trade(item) for item in accepted])
    return trades, equity, summary


def first_signal_per_event(rows: pd.DataFrame, *, cooldown_days: int = 14) -> pd.DataFrame:
    kept: list[int] = []
    for _, group in rows.sort_values(["symbol", "entry_time", "signal_key"]).groupby("symbol", sort=False):
        anchor: pd.Timestamp | None = None
        for index, row in group.iterrows():
            when = pd.Timestamp(row["entry_time"])
            if anchor is None or when > anchor + pd.Timedelta(days=cooldown_days):
                kept.append(int(index))
                anchor = when
    return rows.loc[kept].sort_values(["entry_time", "symbol"]).reset_index(drop=True)


def recognition(entries: pd.DataFrame, baseline: pd.DataFrame) -> dict[str, Any]:
    positives = int(entries["target200_14d_from_entry"].sum()) if len(entries) else 0
    baseline_positives = int(baseline["target200_14d_from_entry"].sum()) if len(baseline) else 0
    events = first_signal_per_event(entries) if len(entries) else entries.copy()
    positive_events = int(events["target200_14d_from_entry"].sum()) if len(events) else 0
    baseline_events = first_signal_per_event(baseline) if len(baseline) else baseline.copy()
    baseline_positive_events = int(baseline_events["target200_14d_from_entry"].sum()) if len(baseline_events) else 0
    return {
        "baseline_signals": int(len(baseline)),
        "filled_entries": int(len(entries)),
        "fill_rate": float(len(entries) / len(baseline)) if len(baseline) else 0.0,
        "actual_200pct_entries": positives,
        "actual_200pct_precision": float(entries["target200_14d_from_entry"].mean()) if len(entries) else 0.0,
        "positive_retention": float(positives / baseline_positives) if baseline_positives else 0.0,
        "independent_events": int(len(events)),
        "positive_independent_events": positive_events,
        "event_precision": float(events["target200_14d_from_entry"].mean()) if len(events) else 0.0,
        "positive_event_retention": float(positive_events / baseline_positive_events) if baseline_positive_events else 0.0,
        "median_delay_hours": float(entries["entry_delay_hours_after_launch"].median()) if len(entries) else None,
        "median_entry_return_vs_score_open": float(entries["entry_return_vs_decision_open"].median()) if len(entries) else None,
    }


def matched_recognition_delta(baseline: pd.DataFrame, challenger: pd.DataFrame) -> pd.DataFrame:
    left = baseline[["signal_key", "symbol", "date", "fold", "validation_split", "target200_14d_from_entry"]].rename(
        columns={"target200_14d_from_entry": "baseline_target200", "date": "signal_date"}
    )
    right = challenger[["signal_key", "target200_14d_from_entry"]].rename(
        columns={"target200_14d_from_entry": "challenger_target200"}
    )
    paired = left.merge(right, on="signal_key", how="inner", validate="one_to_one")
    paired["baseline_target200"] = paired["baseline_target200"].astype(int)
    paired["challenger_target200"] = paired["challenger_target200"].astype(int)
    paired["delta_return"] = paired["challenger_target200"] - paired["baseline_target200"]
    return paired


def filter_precision_bootstrap(
    baseline: pd.DataFrame,
    *,
    filled_signal_keys: set[int],
    samples: int,
    cluster: str,
    seed: int,
    event_deduplicated: bool,
) -> dict[str, Any]:
    rows = first_signal_per_event(baseline) if event_deduplicated else baseline.copy()
    rows["filled"] = rows["signal_key"].astype(int).isin(filled_signal_keys)
    rows["target"] = rows["target200_14d_from_entry"].astype(int)
    if cluster == "week":
        rows["cluster"] = pd.to_datetime(rows["entry_time"], utc=True).dt.to_period("W-SUN").astype(str)
    elif cluster == "symbol":
        rows["cluster"] = rows["symbol"].astype(str)
    else:
        raise ValueError(cluster)
    groups = [group.index.to_numpy() for _, group in rows.groupby("cluster", sort=False)]
    rng = np.random.default_rng(seed)
    estimates: list[float] = []
    for _ in range(samples):
        indices = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        draw = rows.loc[indices]
        selected = draw[draw["filled"]]
        if selected.empty:
            continue
        estimates.append(float(selected["target"].mean() - draw["target"].mean()))
    values = pd.Series(estimates, dtype=float)
    selected_rows = rows[rows["filled"]]
    return {
        "cluster": cluster,
        "event_deduplicated": bool(event_deduplicated),
        "samples": int(len(values)),
        "rows": int(len(rows)),
        "filled_rows": int(len(selected_rows)),
        "base_precision": float(rows["target"].mean()) if len(rows) else 0.0,
        "filled_precision": float(selected_rows["target"].mean()) if len(selected_rows) else 0.0,
        "positive_retention": (
            float(selected_rows["target"].sum() / rows["target"].sum())
            if int(rows["target"].sum()) else 0.0
        ),
        "precision_delta_median": float(values.median()) if len(values) else None,
        "precision_delta_lower_95pct": float(values.quantile(0.025)) if len(values) else None,
        "precision_delta_upper_95pct": float(values.quantile(0.975)) if len(values) else None,
    }


def calibration_table(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    baseline = entries[entries["entry_rule"] == "launch_open"].copy()
    baseline_recognition = recognition(baseline, baseline)
    raw_baseline = simulate_entries(baseline, bars, funding, days=14, slippage_bps=5.0)
    records: list[dict[str, Any]] = []
    for rule_name, metadata in POST.POSTLAUNCH_ENTRY_RULES.items():
        rule_entries = entries[entries["entry_rule"] == rule_name].copy()
        rec = recognition(rule_entries, baseline)
        outcomes = simulate_entries(rule_entries, bars, funding, days=14, slippage_bps=5.0)
        outcomes = [item for item in outcomes if pd.Timestamp(item["exit_time"]) < cutoff]
        trades, _, _ = portfolio(outcomes)
        values = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        matched_keys = set(rule_entries["signal_key"].astype(int))
        matched_baseline = [item for item in raw_baseline if int(item["signal_key"]) in matched_keys and pd.Timestamp(item["exit_time"]) < cutoff]
        paired = FAILURE.paired_deltas(matched_baseline, outcomes)
        split_delta = paired.groupby("validation_split")["delta_return"].mean() if len(paired) else pd.Series(dtype=float)
        fold_expectancy = trades.groupby("validation_split")["net_return"].mean() if len(trades) else pd.Series(dtype=float)
        leave_largest, concentration = STUDY.concentration_stats(trades)
        pf = CONTINUATION.profit_factor(values)
        eligible = bool(
            rule_name != "launch_open"
            and rec["fill_rate"] >= 0.40
            and rec["positive_retention"] >= 0.40
            and rec["actual_200pct_precision"] >= baseline_recognition["actual_200pct_precision"]
            and len(values) >= 12
            and fold_expectancy.size == 3
            and values.mean() > 0
            and pf > 1
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.50
            and len(split_delta) == 3
            and float((split_delta > 0).mean()) >= 2 / 3
            and float(split_delta.mean()) > 0
        )
        selection_score = float(
            (split_delta.mean() if len(split_delta) else -0.25)
            - 0.5 * (split_delta.std(ddof=0) if len(split_delta) else 0.25)
            + 0.05 * rec["positive_retention"]
            + 0.02 * rec["event_precision"]
        )
        records.append(
            {
                "entry_rule": rule_name,
                "family": metadata["family"],
                "description": metadata["description"],
                **rec,
                "baseline_actual_200pct_precision": baseline_recognition["actual_200pct_precision"],
                "trades": int(len(values)),
                "expectancy": float(values.mean()) if len(values) else np.nan,
                "profit_factor": float(pf),
                "folds": int(fold_expectancy.size),
                "paired_rows": int(len(paired)),
                "paired_delta_mean": float(paired["delta_return"].mean()) if len(paired) else np.nan,
                "paired_positive_split_share": float((split_delta > 0).mean()) if len(split_delta) else 0.0,
                "leave_largest_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_score": selection_score,
                "selection_eligible": eligible,
            }
        )
    return pd.DataFrame(records).sort_values(
        ["selection_eligible", "selection_score", "positive_retention"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def main() -> None:
    args = parse_args()
    baseline_dir = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    candidates = pd.read_csv(report / "continuation_entry_candidates.csv.gz", compression="gzip")
    for column in ["date", "baseline_entry_time", "decision_time"]:
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    rows = candidates[candidates["checkpoint_hours"] == ENTRY_CHECKPOINT_HOURS].copy()
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])
    panel = pd.read_csv(baseline_dir / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)

    calibration_signals, _ = CONTINUATION.calibration_scores(
        rows,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=ENTRY_QUANTILE,
    )
    confirmation_signals, _ = CONTINUATION.confirmation_scores(
        rows,
        folds,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=ENTRY_QUANTILE,
    )
    calibration_entries = build_entries(calibration_signals, bars, period="calibration")
    confirmation_entries = build_entries(confirmation_signals, bars, period="confirmation")
    all_entries = pd.concat([calibration_entries, confirmation_entries], ignore_index=True)
    all_entries.to_csv(report / "utility_entry_timing_candidates.csv.gz", index=False, compression="gzip")
    calibration = calibration_table(calibration_entries, bars, funding, cutoff=cutoff)
    calibration.to_csv(report / "utility_entry_timing_calibration.csv", index=False)
    eligible = calibration[calibration["selection_eligible"]]
    selected_rule = str(eligible.iloc[0]["entry_rule"]) if len(eligible) else "launch_open"
    calibration_eligible = bool(len(eligible))

    baseline_entries = confirmation_entries[confirmation_entries["entry_rule"] == "launch_open"].copy()
    selected_entries = confirmation_entries[confirmation_entries["entry_rule"] == selected_rule].copy()
    selected_recognition = recognition(selected_entries, baseline_entries)
    baseline_recognition = recognition(baseline_entries, baseline_entries)
    matched_keys = set(selected_entries["signal_key"].astype(int))
    matched_baseline_entries = baseline_entries[baseline_entries["signal_key"].isin(matched_keys)].copy()
    matched_baseline_recognition = recognition(matched_baseline_entries, matched_baseline_entries)
    filter_bootstraps = {
        "row_week": filter_precision_bootstrap(
            baseline_entries,
            filled_signal_keys=matched_keys,
            samples=args.bootstrap_samples,
            cluster="week",
            seed=20260866,
            event_deduplicated=False,
        ),
        "row_symbol": filter_precision_bootstrap(
            baseline_entries,
            filled_signal_keys=matched_keys,
            samples=args.bootstrap_samples,
            cluster="symbol",
            seed=20260867,
            event_deduplicated=False,
        ),
        "event_week": filter_precision_bootstrap(
            baseline_entries,
            filled_signal_keys=matched_keys,
            samples=args.bootstrap_samples,
            cluster="week",
            seed=20260868,
            event_deduplicated=True,
        ),
        "event_symbol": filter_precision_bootstrap(
            baseline_entries,
            filled_signal_keys=matched_keys,
            samples=args.bootstrap_samples,
            cluster="symbol",
            seed=20260869,
            event_deduplicated=True,
        ),
    }
    recognition_paired = matched_recognition_delta(matched_baseline_entries, selected_entries)
    recognition_paired.to_csv(report / "utility_entry_timing_recognition_paired.csv", index=False)
    week_recognition = FAILURE.delta_bootstrap(
        recognition_paired,
        samples=args.bootstrap_samples,
        cluster="week",
        seed=20260870,
    ) if len(recognition_paired) else {}
    symbol_recognition = FAILURE.delta_bootstrap(
        recognition_paired,
        samples=args.bootstrap_samples,
        cluster="symbol",
        seed=20260871,
    ) if len(recognition_paired) else {}

    baseline_raw = simulate_entries(baseline_entries, bars, funding, days=30, slippage_bps=5.0)
    matched_baseline_raw = [item for item in baseline_raw if int(item["signal_key"]) in matched_keys]
    selected_raw_default: list[dict[str, Any]] = []
    summary_records: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate_entries(selected_entries, bars, funding, days=30, slippage_bps=slippage)
        trades, equity, summary = portfolio(outcomes)
        if not summary:
            continue
        summary.update({"entry_rule": selected_rule, "slippage_bps_each_side": slippage})
        summary_records.append(summary)
        trades["slippage_bps_each_side"] = slippage
        trade_frames.append(trades)
        if not equity.empty:
            equity["slippage_bps_each_side"] = slippage
            equity_frames.append(equity)
        if slippage == 5.0:
            selected_raw_default = outcomes
            default_trades = trades
            default_summary = summary

    baseline_trades, _, baseline_summary = portfolio(baseline_raw)
    matched_baseline_trades, _, matched_baseline_summary = portfolio(matched_baseline_raw)
    summaries = pd.DataFrame(summary_records)
    all_trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    all_equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    trade_folds = STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summaries.to_csv(report / "utility_entry_timing_confirmation_summary.csv", index=False)
    all_trades.to_csv(report / "utility_entry_timing_confirmation_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(report / "utility_entry_timing_confirmation_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(report / "utility_entry_timing_confirmation_folds.csv", index=False)
    pd.DataFrame(
        [
            {"entry_rule": "launch_open_all", **baseline_summary},
            {"entry_rule": "launch_open_matched", **matched_baseline_summary},
            {"entry_rule": selected_rule, **default_summary},
        ]
    ).to_csv(report / "utility_entry_timing_confirmation_comparison.csv", index=False)

    paired = FAILURE.paired_deltas(matched_baseline_raw, selected_raw_default)
    paired.to_csv(report / "utility_entry_timing_trade_paired_deltas.csv", index=False)
    week_trade_delta = FAILURE.delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="week", seed=20260872) if len(paired) else {}
    symbol_trade_delta = FAILURE.delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="symbol", seed=20260873) if len(paired) else {}
    week_trade = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="week", seed=20260874) if len(default_trades) else {}
    symbol_trade = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260875) if len(default_trades) else {}
    leave_largest, concentration = STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if len(trade_folds) else 0.0
    stress_ok = bool(
        len(summaries) == 3
        and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all()
        and (summaries["max_drawdown"] >= -0.30).all()
    )
    recognition_gate = bool(
        selected_rule != "launch_open"
        and calibration_eligible
        and selected_recognition["fill_rate"] >= 0.40
        and selected_recognition["positive_retention"] >= 0.40
        and selected_recognition["positive_event_retention"] >= 0.80
        and selected_recognition["actual_200pct_precision"] > baseline_recognition["actual_200pct_precision"]
        and selected_recognition["event_precision"] > baseline_recognition["event_precision"]
        and all(
            item.get("precision_delta_lower_95pct") is not None
            and item["precision_delta_lower_95pct"] > 0
            for item in filter_bootstraps.values()
        )
    )
    entry_gate = bool(
        recognition_gate
        and default_summary.get("expectancy", -1) > 0
        and default_summary.get("profit_factor", 0) > 1
        and fold_majority > 0.50
        and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week_trade.get("expectancy_lower_95pct", -1) > 0
        and symbol_trade.get("expectancy_lower_95pct", -1) > 0
        and week_trade_delta.get("mean_delta_lower_95pct", -1) > 0
        and symbol_trade_delta.get("mean_delta_lower_95pct", -1) > 0
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_historical_time_isolated_utility_entry_execution",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "entry_checkpoint_hours": ENTRY_CHECKPOINT_HOURS,
        "entry_selection_quantile": ENTRY_QUANTILE,
        "trigger_window_hours": TRIGGER_WINDOW_HOURS,
        "candidate_entry_rules": list(POST.POSTLAUNCH_ENTRY_RULES),
        "trigger_clock": "completed 4h trigger bar; enter at following 4h open",
        "selected_entry_rule": selected_rule,
        "calibration_eligible": calibration_eligible,
        "confirmation_recognition_baseline_all": baseline_recognition,
        "confirmation_recognition_baseline_matched": matched_baseline_recognition,
        "confirmation_recognition_selected": selected_recognition,
        "filter_precision_bootstraps": filter_bootstraps,
        "recognition_week_increment_bootstrap": week_recognition,
        "recognition_symbol_increment_bootstrap": symbol_recognition,
        "baseline_strategy_summary_all": baseline_summary,
        "baseline_strategy_summary_matched": matched_baseline_summary,
        "selected_strategy_summary": default_summary,
        "strategy_stress_costs": summaries.to_dict(orient="records"),
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "fold_majority_profit_factor_gt_1": fold_majority,
        "week_return_bootstrap": week_trade,
        "symbol_return_bootstrap": symbol_trade,
        "week_trade_increment_bootstrap": week_trade_delta,
        "symbol_trade_increment_bootstrap": symbol_trade_delta,
        "paired_trade_increment_mean": float(paired["delta_return"].mean()) if len(paired) else None,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "recognition_filter_gate_pass": recognition_gate,
        "entry_timing_gate_pass": entry_gate,
        "classification": (
            "paper_entry_execution_candidate_pending_true_oos"
            if entry_gate
            else "prospective_filter_only_pending_true_oos"
            if recognition_gate
            else "retain_score_open_entry"
        ),
        "forward_counterfactual_added": True,
        "primary_entry_changed": False,
        "automatic_trading_allowed": False,
    }
    (report / "utility_entry_timing_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
