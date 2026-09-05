"""Research when to enter and exit frozen Binance +200% run-up candidates.

Entry and exit choices are calibrated only on folds 0-2 with outcomes that
fully matured before fold 3.  Folds 3-6 are confirmation evidence.  The study
is read-only with respect to trading and never imports an execution module.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

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


ENTRY = _load(
    "binance_entry_exit_validation_standalone",
    ROOT / "core" / "research" / "binance_entry_exit_validation.py",
)
VALIDATION = _load(
    "binance_runup_validation_for_entry_timing",
    ROOT / "core" / "research" / "binance_runup_validation.py",
)
EXIT = _load(
    "binance_exit_helpers_for_entry_timing",
    ROOT / "scripts" / "analyze_binance_exit_strategies.py",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
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


def concentration_stats(trades: pd.DataFrame) -> tuple[float | None, float | None]:
    if trades.empty:
        return None, None
    returns = pd.to_numeric(trades["net_return"], errors="coerce").dropna()
    leave_largest = float(returns.drop(returns.idxmax()).mean()) if len(returns) > 1 else None
    positive = pd.to_numeric(trades.loc[trades["pnl_usd"] > 0, "pnl_usd"], errors="coerce").dropna()
    share = float(positive.max() / positive.sum()) if positive.sum() > 0 else 1.0
    return leave_largest, share


def build_entries(
    signals: pd.DataFrame,
    bars_by_symbol: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    keep = [
        "symbol", "date", "fold", "close", "price_model_score", "price_model_pctile",
        "label_primary_int", "breadth_regime", "btc_trend_regime", "btc_vol_regime",
        "market_state", "listing_age_bucket", "spot_listing", "liquidity_bucket",
    ]
    for signal_key, (_, signal) in enumerate(signals.iterrows()):
        bars = bars_by_symbol.get(str(signal["symbol"]))
        if bars is None:
            continue
        base = {column: signal.get(column) for column in keep}
        base["signal_key"] = int(signal_key)
        for rule_name in ENTRY.ENTRY_RULES:
            located = ENTRY.locate_entry(signal, bars, rule_name=rule_name)
            if located is None:
                continue
            row = dict(base)
            row.update(located)
            row["entry_id"] = f"{signal['symbol']}|{pd.Timestamp(signal['date']).date()}|{rule_name}"
            records.append(row)
    entries = pd.DataFrame(records)
    if not entries.empty:
        for column in ["date", "baseline_entry_time", "trigger_time", "entry_time"]:
            entries[column] = pd.to_datetime(entries[column], utc=True)
    return entries


def simulate_entries(
    entries: pd.DataFrame,
    bars_by_symbol: Mapping[str, pd.DataFrame],
    funding_history: Mapping[str, pd.Series],
    *,
    policy_name: str,
    policy: Mapping[str, Any],
    slippage_bps: float,
    include_marks: bool,
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    for _, entry in entries.iterrows():
        symbol = str(entry["symbol"])
        bars = bars_by_symbol.get(symbol)
        if bars is None:
            continue
        outcome = ENTRY.simulate_stateful_trade(
            bars,
            entry_time=pd.Timestamp(entry["entry_time"]),
            policy=policy,
            slippage_bps=slippage_bps,
            funding=funding_history.get(symbol),
            include_marks=include_marks,
        )
        if outcome is None:
            continue
        outcome.update(
            {
                "symbol": symbol,
                "signal_date": pd.Timestamp(entry["date"]),
                "fold": int(entry["fold"]),
                "score": float(entry["price_model_score"]),
                "score_pctile": float(entry["price_model_pctile"]),
                "breadth_regime": entry.get("breadth_regime"),
                "btc_trend_regime": entry.get("btc_trend_regime"),
                "btc_vol_regime": entry.get("btc_vol_regime"),
                "market_state": entry.get("market_state"),
                "policy": policy_name,
                "slippage_bps_each_side": float(slippage_bps),
                "signal_key": int(entry["signal_key"]),
                "entry_rule": str(entry["entry_rule"]),
                "entry_delay_hours": float(entry["entry_delay_hours"]),
                "entry_chase_vs_signal_close": float(entry["entry_chase_vs_signal_close"]),
                "target200_14d_from_entry": bool(entry["target200_14d_from_entry"]),
                "future_max_return_14d_from_entry": float(entry["future_max_return_14d_from_entry"]),
            }
        )
        outcomes.append(outcome)
    return outcomes


def accepted_frame(outcomes: Sequence[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    accepted, equity, summary = EXIT.summarize_portfolio(outcomes)
    frame = pd.DataFrame([EXIT.clean_trade(row) for row in accepted])
    return frame, equity, summary


def build_entry_calibration(
    entries: pd.DataFrame,
    base_trades: Mapping[str, pd.DataFrame],
    *,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    eligible_labels = entries[
        (entries["entry_time"] + pd.Timedelta(days=14) < cutoff)
    ].copy()
    baseline = eligible_labels[eligible_labels["entry_rule"] == "next_open"]
    baseline_successes = int(baseline["target200_14d_from_entry"].sum())
    baseline_precision = float(baseline["target200_14d_from_entry"].mean()) if len(baseline) else 0.0
    baseline_signals = int(len(baseline))
    rows: list[dict[str, Any]] = []
    for rule_name, meta in ENTRY.ENTRY_RULES.items():
        rule_entries = eligible_labels[eligible_labels["entry_rule"] == rule_name]
        positives = int(rule_entries["target200_14d_from_entry"].sum())
        precision = float(positives / len(rule_entries)) if len(rule_entries) else 0.0
        retention = float(positives / baseline_successes) if baseline_successes else 0.0
        trades = base_trades.get(rule_name, pd.DataFrame()).copy()
        if not trades.empty:
            trades["exit_time"] = pd.to_datetime(trades["exit_time"], utc=True)
            trades = trades[trades["exit_time"] < cutoff]
        returns = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        leave_largest, concentration = concentration_stats(trades)
        fold_exp = trades.groupby("fold")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
        pf = profit_factor(returns)
        selection_score = (
            ENTRY.wilson_lower_bound(positives, len(rule_entries))
            + 0.10 * retention
            + 0.05 * float(np.clip(returns.mean() if len(returns) else -0.20, -0.20, 0.50))
            + 0.02 * min(pf, 3.0) / 3.0
        )
        selection_eligible = bool(
            rule_name != "next_open"
            and len(rule_entries) >= 20 and positives >= 2
            and rule_entries["fold"].nunique() >= 2
            and precision > baseline_precision and retention >= 0.25
            and len(returns) >= 20 and returns.mean() > 0 and pf > 1.0
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.35
        )
        rows.append(
            {
                "entry_rule": rule_name,
                "entry_family": meta["family"],
                "description": meta["description"],
                "matured_signals": baseline_signals,
                "confirmed_entries": int(len(rule_entries)),
                "confirmation_rate": float(len(rule_entries) / baseline_signals) if baseline_signals else 0.0,
                "actual_200pct_entries": positives,
                "actual_200pct_precision": precision,
                "actual_positive_retention": retention,
                "baseline_actual_precision": baseline_precision,
                "wilson_precision_lower": ENTRY.wilson_lower_bound(positives, len(rule_entries)),
                "median_entry_delay_hours": float(rule_entries["entry_delay_hours"].median()) if len(rule_entries) else np.nan,
                "median_entry_chase": float(rule_entries["entry_chase_vs_signal_close"].median()) if len(rule_entries) else np.nan,
                "calibration_trades": int(len(returns)),
                "calibration_expectancy": float(returns.mean()) if len(returns) else np.nan,
                "calibration_profit_factor": float(pf),
                "calibration_fold_expectancy_mean": float(fold_exp.mean()) if len(fold_exp) else np.nan,
                "calibration_fold_expectancy_std": float(fold_exp.std(ddof=0)) if len(fold_exp) else np.nan,
                "leave_largest_winner_out_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_score": float(selection_score),
                "selection_eligible": selection_eligible,
                "information_cutoff_utc": cutoff,
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    ).reset_index(drop=True)


def build_exit_calibration(
    policy_trades: Mapping[str, pd.DataFrame],
    policies: Mapping[str, Mapping[str, Any]],
    *,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for policy_name, policy in policies.items():
        trades = policy_trades.get(policy_name, pd.DataFrame()).copy()
        if not trades.empty:
            trades["exit_time"] = pd.to_datetime(trades["exit_time"], utc=True)
            trades = trades[trades["exit_time"] < cutoff]
        returns = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        pf = profit_factor(returns)
        leave_largest, concentration = concentration_stats(trades)
        fold_exp = trades.groupby("fold")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
        score = float(fold_exp.mean() - 0.5 * fold_exp.std(ddof=0)) if len(fold_exp) else -np.inf
        eligible = bool(
            len(returns) >= 30 and fold_exp.size >= 2
            and returns.mean() > 0 and pf > 1.0
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.35
        )
        rows.append(
            {
                "exit_policy": policy_name,
                "exit_family": policy["family"],
                "calibration_trades": int(len(returns)),
                "calibration_expectancy": float(returns.mean()) if len(returns) else np.nan,
                "calibration_profit_factor": float(pf),
                "calibration_fold_expectancy_mean": float(fold_exp.mean()) if len(fold_exp) else np.nan,
                "calibration_fold_expectancy_std": float(fold_exp.std(ddof=0)) if len(fold_exp) else np.nan,
                "leave_largest_winner_out_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_score": score,
                "selection_eligible": eligible,
                "information_cutoff_utc": cutoff,
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    ).reset_index(drop=True)


def fold_metrics(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for fold, group in trades.groupby("fold"):
        values = pd.to_numeric(group["net_return"], errors="coerce").dropna()
        rows.append(
            {
                "fold": int(fold),
                "trades": int(len(values)),
                "expectancy": float(values.mean()) if len(values) else np.nan,
                "median_return": float(values.median()) if len(values) else np.nan,
                "win_rate": float((values > 0).mean()) if len(values) else np.nan,
                "profit_factor": float(profit_factor(values)),
                "hard_stop_rate": float((group["exit_reason"] == "hard_stop").mean()),
                "failure_to_launch_rate": float((group["exit_reason"] == "failure_to_launch").mean()),
                "mean_mfe": float(group["mfe"].mean()),
                "mean_mae": float(group["mae"].mean()),
            }
        )
    return pd.DataFrame(rows)


def recognition_summary(entries: pd.DataFrame, baseline: pd.DataFrame) -> dict[str, Any]:
    positives = int(entries["target200_14d_from_entry"].sum())
    base_positives = int(baseline["target200_14d_from_entry"].sum())
    return {
        "signals": int(len(entries)),
        "actual_200pct_entries": positives,
        "actual_200pct_precision": float(positives / len(entries)) if len(entries) else 0.0,
        "actual_positive_retention_vs_next_open": float(positives / base_positives) if base_positives else 0.0,
        "unique_success_symbols": int(entries.loc[entries["target200_14d_from_entry"], "symbol"].nunique()),
        "median_entry_delay_hours": float(entries["entry_delay_hours"].median()) if len(entries) else None,
        "median_entry_chase": float(entries["entry_chase_vs_signal_close"].median()) if len(entries) else None,
    }


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    report.mkdir(parents=True, exist_ok=True)
    scored_path = report / "historical_walk_forward_scored.csv.gz"
    panel_path = baseline / "futures_4h_panel.csv.gz"
    folds_path = report / "walk_forward_fold_metrics.csv"

    scored = pd.read_csv(scored_path, compression="gzip")
    scored["date"] = pd.to_datetime(scored["date"], utc=True)
    panel = pd.read_csv(panel_path, compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    folds = pd.read_csv(folds_path)
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])

    signals = VALIDATION.build_first_crossing_signals(scored).reset_index(drop=True)
    bars_by_symbol = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)
    entries = build_entries(signals, bars_by_symbol)
    entries.to_csv(report / "entry_timing_candidates.csv.gz", index=False, compression="gzip")

    policies = ENTRY.build_exit_policy_catalog()
    frozen_name = "frozen_half100_trail25"
    base_trade_frames: dict[str, pd.DataFrame] = {}
    entry_grid_records: list[dict[str, Any]] = []
    for rule_name in ENTRY.ENTRY_RULES:
        rule_entries = entries[entries["entry_rule"] == rule_name]
        outcomes = simulate_entries(
            rule_entries,
            bars_by_symbol,
            funding,
            policy_name=frozen_name,
            policy=policies[frozen_name],
            slippage_bps=5.0,
            include_marks=False,
        )
        trades, _, _ = accepted_frame(outcomes)
        base_trade_frames[rule_name] = trades
        if not trades.empty:
            entry_grid_records.extend(trades.to_dict(orient="records"))
    pd.DataFrame(entry_grid_records).to_csv(
        report / "entry_timing_grid_trades.csv.gz", index=False, compression="gzip"
    )

    entry_calibration = build_entry_calibration(entries, base_trade_frames, cutoff=cutoff)
    eligible_entry = entry_calibration[entry_calibration["selection_eligible"]]
    selected_entry = str(eligible_entry.iloc[0]["entry_rule"]) if not eligible_entry.empty else "next_open"
    entry_calibration.to_csv(report / "entry_timing_calibration.csv", index=False)

    selected_entries = entries[entries["entry_rule"] == selected_entry].copy()
    exit_trade_frames: dict[str, pd.DataFrame] = {}
    exit_grid_records: list[dict[str, Any]] = []
    for policy_name, policy in policies.items():
        outcomes = simulate_entries(
            selected_entries,
            bars_by_symbol,
            funding,
            policy_name=policy_name,
            policy=policy,
            slippage_bps=5.0,
            include_marks=False,
        )
        trades, _, _ = accepted_frame(outcomes)
        exit_trade_frames[policy_name] = trades
        if not trades.empty:
            exit_grid_records.extend(trades.to_dict(orient="records"))
    pd.DataFrame(exit_grid_records).to_csv(
        report / "stateful_exit_grid_trades.csv.gz", index=False, compression="gzip"
    )
    exit_calibration = build_exit_calibration(exit_trade_frames, policies, cutoff=cutoff)
    eligible_exit = exit_calibration[exit_calibration["selection_eligible"]]
    selected_exit = str(eligible_exit.iloc[0]["exit_policy"]) if not eligible_exit.empty else frozen_name
    exit_calibration.to_csv(report / "stateful_exit_calibration.csv", index=False)

    confirmation_selected = selected_entries[selected_entries["fold"] >= 3].copy()
    confirmation_baseline = entries[
        (entries["entry_rule"] == "next_open") & (entries["fold"] >= 3)
    ].copy()
    recognition = {
        "next_open": recognition_summary(confirmation_baseline, confirmation_baseline),
        "selected_entry": recognition_summary(confirmation_selected, confirmation_baseline),
    }

    summary_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate_entries(
            confirmation_selected,
            bars_by_symbol,
            funding,
            policy_name=selected_exit,
            policy=policies[selected_exit],
            slippage_bps=slippage,
            include_marks=True,
        )
        trades, equity, summary = accepted_frame(outcomes)
        if not summary:
            continue
        summary.update(
            {
                "entry_rule": selected_entry,
                "exit_policy": selected_exit,
                "slippage_bps_each_side": slippage,
                "sample_scope": "historical_confirmation_folds_3_to_6",
            }
        )
        summary_records.append(summary)
        trades["sample_scope"] = "historical_confirmation_folds_3_to_6"
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
    folds_out = fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summaries.to_csv(report / "entry_exit_confirmation_summary.csv", index=False)
    all_trades.to_csv(report / "entry_exit_confirmation_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(report / "entry_exit_confirmation_equity.csv.gz", index=False, compression="gzip")
    folds_out.to_csv(report / "entry_exit_confirmation_folds.csv", index=False)

    comparison_rows: list[dict[str, Any]] = []
    comparison_specs = [
        ("next_open_frozen_exit", confirmation_baseline, frozen_name),
        ("selected_entry_frozen_exit", confirmation_selected, frozen_name),
        ("selected_entry_selected_exit", confirmation_selected, selected_exit),
    ]
    for name, comparison_entries, policy_name in comparison_specs:
        outcomes = simulate_entries(
            comparison_entries,
            bars_by_symbol,
            funding,
            policy_name=policy_name,
            policy=policies[policy_name],
            slippage_bps=5.0,
            include_marks=True,
        )
        trades, _, summary = accepted_frame(outcomes)
        if not summary:
            continue
        summary["strategy"] = name
        summary["entry_rule"] = str(comparison_entries["entry_rule"].iloc[0]) if len(comparison_entries) else None
        summary["exit_policy"] = policy_name
        comparison_rows.append(summary)
    comparison = pd.DataFrame(comparison_rows)
    comparison.to_csv(report / "entry_exit_confirmation_comparison.csv", index=False)

    week_bootstrap = (
        EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="week", seed=20260723)
        if not default_trades.empty else {}
    )
    symbol_bootstrap = (
        EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260724)
        if not default_trades.empty else {}
    )
    leave_largest, concentration = concentration_stats(default_trades)
    fold_majority = float((folds_out["profit_factor"] > 1).mean()) if not folds_out.empty else 0.0
    stress_ok = bool(
        len(summaries) == 3
        and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all()
        and (summaries["max_drawdown"] >= -0.30).all()
    )
    week_lower = week_bootstrap.get("expectancy_lower_95pct")
    symbol_lower = symbol_bootstrap.get("expectancy_lower_95pct")
    recognition_improved = bool(
        recognition["selected_entry"]["actual_200pct_precision"]
        > recognition["next_open"]["actual_200pct_precision"]
        and recognition["selected_entry"]["actual_positive_retention_vs_next_open"] >= 0.25
    )
    trade_gate = bool(
        not eligible_entry.empty and not eligible_exit.empty
        and recognition_improved and default_summary
        and default_summary.get("expectancy", -1) > 0
        and default_summary.get("profit_factor", 0) > 1
        and fold_majority > 0.5 and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week_lower is not None and week_lower > 0
        and symbol_lower is not None and symbol_lower > 0
    )

    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "scope": "historical entry/exit timing study on frozen top-2% price candidates",
        "selection_information_cutoff_utc": cutoff,
        "calibration_scope": "folds 0-2; labels need 14 days and trades must exit before cutoff",
        "confirmation_scope": "folds 3-6; no rule reselection",
        "entry_rules_tested": int(len(ENTRY.ENTRY_RULES)),
        "exit_policies_tested": int(len(policies)),
        "selected_entry_rule": selected_entry,
        "entry_selection_eligible": bool(not eligible_entry.empty),
        "selected_exit_policy": selected_exit,
        "exit_selection_eligible": bool(not eligible_exit.empty),
        "fallback_policy": "next_open and frozen_half100_trail25 when no eligible calibration rule exists",
        "confirmation_recognition": recognition,
        "recognition_improved": recognition_improved,
        "default_cost_summary": default_summary,
        "stress_costs": summaries[
            ["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]
        ].to_dict(orient="records") if not summaries.empty else [],
        "fold_metrics": folds_out.to_dict(orient="records"),
        "fold_majority_profit_factor_gt_1": fold_majority,
        "week_block_bootstrap": week_bootstrap,
        "symbol_bootstrap": symbol_bootstrap,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "paper_trade_gate_pass": trade_gate,
        "classification": "paper_candidate" if trade_gate else "watchlist_only",
        "source_hashes": {
            scored_path.name: file_sha256(scored_path),
            panel_path.name: file_sha256(panel_path),
            folds_path.name: file_sha256(folds_path),
        },
    }
    (report / "entry_exit_timing_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
