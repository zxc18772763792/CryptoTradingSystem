"""Test a one-4h-bar microstructure confirmation model on frozen run-up candidates."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"
FEATURES = [
    "price_model_score",
    "entry_chase_vs_signal_close",
    "trigger_bar_return",
    "trigger_volume_ratio",
    "trigger_taker_buy_share_3",
    "trigger_range_pct",
    "trigger_close_vs_ema18",
    "trigger_close_vs_prior_high6",
]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


STUDY = _load(
    "binance_entry_timing_study_for_microstructure",
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


def fit_score(train: pd.DataFrame, test: pd.DataFrame, features: Sequence[str]) -> tuple[np.ndarray, np.ndarray]:
    train_x = train[list(features)].apply(pd.to_numeric, errors="coerce")
    test_x = test[list(features)].apply(pd.to_numeric, errors="coerce")
    lower = train_x.quantile(0.01)
    upper = train_x.quantile(0.99)
    train_x = train_x.clip(lower=lower, upper=upper, axis=1)
    test_x = test_x.clip(lower=lower, upper=upper, axis=1)
    medians = train_x.median().fillna(0.0)
    train_x = train_x.fillna(medians)
    test_x = test_x.fillna(medians)
    means = train_x.mean()
    scales = train_x.std(ddof=0).replace(0, 1.0).fillna(1.0)
    train_x = (train_x - means) / scales
    test_x = (test_x - means) / scales
    model = LogisticRegression(C=0.5, class_weight="balanced", max_iter=2000, solver="lbfgs")
    model.fit(train_x, train["target200_14d_from_entry"].astype(int))
    return model.predict_proba(train_x)[:, 1], model.predict_proba(test_x)[:, 1]


def walk_forward(rows: pd.DataFrame, folds: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored: list[pd.DataFrame] = []
    fold_rows: list[dict[str, Any]] = []
    for fold in (3, 4, 5, 6):
        test_start = pd.Timestamp(folds.loc[folds["fold"] == fold, "test_start"].iloc[0])
        train = rows[rows["entry_time"] + pd.Timedelta(days=14) < test_start].copy()
        test = rows[rows["fold"] == fold].copy()
        if len(train) < 150 or train["target200_14d_from_entry"].sum() < 8 or test.empty:
            continue
        price_train, price_test = fit_score(train, test, ["price_model_score"])
        micro_train, micro_test = fit_score(train, test, FEATURES)
        threshold = float(np.quantile(micro_train, 0.70))
        test["price_only_score"] = price_test
        test["micro_combo_score"] = micro_test
        test["micro_selected"] = test["micro_combo_score"] >= threshold
        labels = test["target200_14d_from_entry"].astype(int)
        selected = test[test["micro_selected"]]
        base_precision = float(labels.mean())
        selected_precision = float(selected["target200_14d_from_entry"].mean()) if len(selected) else 0.0
        positives = int(labels.sum())
        selected_positives = int(selected["target200_14d_from_entry"].sum())
        fold_rows.append(
            {
                "fold": fold,
                "train_rows": int(len(train)),
                "train_positives": int(train["target200_14d_from_entry"].sum()),
                "test_rows": int(len(test)),
                "test_positives": positives,
                "price_average_precision": float(average_precision_score(labels, price_test)),
                "micro_average_precision": float(average_precision_score(labels, micro_test)),
                "price_auc": float(roc_auc_score(labels, price_test)) if labels.nunique() > 1 else np.nan,
                "micro_auc": float(roc_auc_score(labels, micro_test)) if labels.nunique() > 1 else np.nan,
                "selection_threshold": threshold,
                "selected_rows": int(len(selected)),
                "selected_positives": selected_positives,
                "base_precision": base_precision,
                "selected_precision": selected_precision,
                "positive_retention": float(selected_positives / positives) if positives else 0.0,
                "precision_improved": bool(selected_precision > base_precision),
            }
        )
        scored.append(test)
    return pd.concat(scored, ignore_index=True), pd.DataFrame(fold_rows)


def bootstrap_ap_increment(scored: pd.DataFrame, *, samples: int, seed: int) -> dict[str, Any]:
    rows = scored.copy()
    rows["cluster"] = rows["date"].dt.to_period("W-SUN").astype(str)
    groups = [group.index.to_numpy() for _, group in rows.groupby("cluster", sort=False)]
    rng = np.random.default_rng(seed)
    deltas: list[float] = []
    for _ in range(samples):
        sampled = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        draw = rows.loc[sampled]
        labels = draw["target200_14d_from_entry"].astype(int)
        if labels.nunique() < 2:
            continue
        deltas.append(
            float(
                average_precision_score(labels, draw["micro_combo_score"])
                - average_precision_score(labels, draw["price_only_score"])
            )
        )
    values = pd.Series(deltas, dtype=float)
    return {
        "samples": int(len(values)),
        "delta_ap_median": float(values.median()),
        "delta_ap_lower_95pct": float(values.quantile(0.025)),
        "delta_ap_upper_95pct": float(values.quantile(0.975)),
    }


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    entries = pd.read_csv(report / "entry_timing_candidates.csv.gz", compression="gzip")
    for column in ["date", "entry_time", "baseline_entry_time", "trigger_time"]:
        entries[column] = pd.to_datetime(entries[column], utc=True)
    rows = entries[entries["entry_rule"] == "one_bar_wait"].copy()
    if rows.empty:
        raise RuntimeError("one_bar_wait entries are missing; rerun analyze_binance_entry_exit_timing.py")
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    scored, fold_frame = walk_forward(rows, folds)
    scored.to_csv(report / "microstructure_confirmation_scored.csv.gz", index=False, compression="gzip")
    fold_frame.to_csv(report / "microstructure_confirmation_folds.csv", index=False)

    labels = scored["target200_14d_from_entry"].astype(int)
    selected = scored[scored["micro_selected"]].copy()
    pooled = {
        "rows": int(len(scored)),
        "positives": int(labels.sum()),
        "base_precision": float(labels.mean()),
        "price_average_precision": float(average_precision_score(labels, scored["price_only_score"])),
        "micro_average_precision": float(average_precision_score(labels, scored["micro_combo_score"])),
        "selected_rows": int(len(selected)),
        "selected_positives": int(selected["target200_14d_from_entry"].sum()),
        "selected_precision": float(selected["target200_14d_from_entry"].mean()) if len(selected) else 0.0,
        "selected_positive_retention": float(selected["target200_14d_from_entry"].sum() / labels.sum()) if labels.sum() else 0.0,
        "selected_unique_success_symbols": int(selected.loc[selected["target200_14d_from_entry"], "symbol"].nunique()),
        "median_entry_delay_hours": float(selected["entry_delay_hours"].median()) if len(selected) else None,
    }
    bootstrap = bootstrap_ap_increment(scored, samples=args.bootstrap_samples, seed=20260725)
    fold_majority_increment = float(
        ((fold_frame["micro_average_precision"] > fold_frame["price_average_precision"]) & fold_frame["precision_improved"]).mean()
    ) if not fold_frame.empty else 0.0
    ranking_gate = bool(
        len(fold_frame) >= 3 and fold_majority_increment > 0.5
        and pooled["micro_average_precision"] > pooled["price_average_precision"]
        and pooled["selected_precision"] > pooled["base_precision"]
        and pooled["selected_positive_retention"] >= 0.25
        and bootstrap["delta_ap_lower_95pct"] > 0
    )

    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars_by_symbol = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)
    policy_name = "frozen_half100_trail25"
    policy = ENTRY.build_exit_policy_catalog()[policy_name]
    summary_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = STUDY.simulate_entries(
            selected,
            bars_by_symbol,
            funding,
            policy_name=policy_name,
            policy=policy,
            slippage_bps=slippage,
            include_marks=True,
        )
        trades, equity, summary = STUDY.accepted_frame(outcomes)
        if not summary:
            continue
        summary.update({"slippage_bps_each_side": slippage, "entry_rule": "one_bar_micro_top30", "exit_policy": policy_name})
        summary_rows.append(summary)
        trades["slippage_bps_each_side"] = slippage
        trade_rows.extend(trades.to_dict(orient="records"))
        if not equity.empty:
            equity["slippage_bps_each_side"] = slippage
            equity_rows.extend(equity.to_dict(orient="records"))
        if slippage == 5.0:
            default_trades = trades
            default_summary = summary

    summaries = pd.DataFrame(summary_rows)
    trades_out = pd.DataFrame(trade_rows)
    equity_out = pd.DataFrame(equity_rows)
    trade_folds = STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summaries.to_csv(report / "microstructure_confirmation_strategy_summary.csv", index=False)
    trades_out.to_csv(report / "microstructure_confirmation_trades.csv.gz", index=False, compression="gzip")
    equity_out.to_csv(report / "microstructure_confirmation_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(report / "microstructure_confirmation_trade_folds.csv", index=False)
    week = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="week", seed=20260726) if not default_trades.empty else {}
    symbol = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260727) if not default_trades.empty else {}
    leave_largest, concentration = STUDY.concentration_stats(default_trades)
    trade_fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summaries) == 3 and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all() and (summaries["max_drawdown"] >= -0.30).all()
    )
    trade_gate = bool(
        ranking_gate and default_summary and default_summary.get("expectancy", -1) > 0
        and default_summary.get("profit_factor", 0) > 1 and trade_fold_majority > 0.5
        and stress_ok and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week.get("expectancy_lower_95pct", -1) > 0
        and symbol.get("expectancy_lower_95pct", -1) > 0
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "features_frozen": FEATURES,
        "prediction_clock": "observe first completed 4h bar after the daily signal; enter at next 4h open",
        "training_rule": "expanding history; every label must mature 14 days before each test fold",
        "selection_rule": "fixed 70th percentile training-score threshold; no test-fold threshold tuning",
        "fold_metrics": fold_frame.to_dict(orient="records"),
        "pooled_metrics": pooled,
        "bootstrap_7d_blocks": bootstrap,
        "fold_majority_ap_and_precision_positive": fold_majority_increment,
        "ranking_gate_pass": ranking_gate,
        "strategy_default_summary": default_summary,
        "strategy_stress_costs": summaries[["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]].to_dict(orient="records") if not summaries.empty else [],
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "strategy_fold_majority_profit_factor_gt_1": trade_fold_majority,
        "week_block_bootstrap": week,
        "symbol_bootstrap": symbol,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "paper_trade_gate_pass": trade_gate,
        "classification": "paper_candidate" if trade_gate else "watchlist_only",
    }
    (report / "microstructure_confirmation_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
