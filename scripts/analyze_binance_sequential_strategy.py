"""Validate 24-72h launch classification and sequential entry/exit choices.

Only folds 1-2 are used to select the checkpoint, model family and trading
mode.  The selected specification is then frozen and evaluated on folds 3-6.
The script is research-only and has no order-routing imports.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"
CHECKPOINTS = (24, 48, 72)
MODEL_FAMILIES = ("logistic", "shallow_hgb")
SELECTION_QUANTILE = 0.60
TRADE_MODES = ("delayed_selected", "enter_then_exit_reject", "half_probe_then_add")


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SEQUENTIAL = _load(
    "binance_sequential_validation_standalone",
    ROOT / "core" / "research" / "binance_sequential_validation.py",
)
ENTRY = _load(
    "binance_entry_exit_validation_for_sequence",
    ROOT / "core" / "research" / "binance_entry_exit_validation.py",
)
VALIDATION = _load(
    "binance_runup_validation_for_sequence",
    ROOT / "core" / "research" / "binance_runup_validation.py",
)
STUDY = _load(
    "binance_entry_timing_study_for_sequence",
    ROOT / "scripts" / "analyze_binance_entry_exit_timing.py",
)
EXIT = STUDY.EXIT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    return parser.parse_args()


def profit_factor(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def score_candidate(
    rows: pd.DataFrame,
    folds: pd.DataFrame,
    *,
    family: str,
    test_folds: Sequence[int],
    features: Sequence[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    for fold in test_folds:
        fold_row = folds[folds["fold"] == fold]
        if fold_row.empty:
            continue
        test_start = pd.Timestamp(fold_row.iloc[0]["test_start"])
        train = rows[rows["decision_time"] + pd.Timedelta(days=14) < test_start].copy()
        test = rows[rows["fold"] == fold].copy()
        if len(train) < 40 or int(train["original_target200"].sum()) < 3 or test.empty:
            continue
        train_score, test_score = SEQUENTIAL.fit_sequence_score(
            train,
            test,
            family=family,
            features=SEQUENTIAL.SEQUENCE_FEATURES if features is None else features,
            label_column="original_target200",
        )
        threshold = float(np.quantile(train_score, SELECTION_QUANTILE))
        test["sequence_score"] = test_score
        test["sequence_threshold"] = threshold
        test["sequence_selected"] = test["sequence_score"] >= threshold
        labels = test["original_target200"].astype(int)
        late_labels = test["late_target200"].astype(int)
        selected = test[test["sequence_selected"]]
        selected_positive = int(selected["original_target200"].sum())
        metrics.append(
            {
                "fold": int(fold),
                "train_rows": int(len(train)),
                "train_positives": int(train["original_target200"].sum()),
                "test_rows": int(len(test)),
                "test_positives": int(labels.sum()),
                "price_average_precision": float(average_precision_score(labels, test["price_model_score"])),
                "sequence_average_precision": float(average_precision_score(labels, test["sequence_score"])),
                "price_auc": float(roc_auc_score(labels, test["price_model_score"])) if labels.nunique() > 1 else np.nan,
                "sequence_auc": float(roc_auc_score(labels, test["sequence_score"])) if labels.nunique() > 1 else np.nan,
                "selection_threshold": threshold,
                "selected_rows": int(len(selected)),
                "selected_positives": selected_positive,
                "base_precision": float(labels.mean()),
                "selected_precision": float(selected["original_target200"].mean()) if len(selected) else 0.0,
                "positive_retention": float(selected_positive / labels.sum()) if labels.sum() else 0.0,
                "late_base_precision": float(late_labels.mean()),
                "late_selected_precision": float(selected["late_target200"].mean()) if len(selected) else 0.0,
                "ap_improved": bool(
                    average_precision_score(labels, test["sequence_score"])
                    > average_precision_score(labels, test["price_model_score"])
                ),
                "precision_improved": bool(
                    len(selected) and selected["original_target200"].mean() > labels.mean()
                ),
            }
        )
        scored.append(test)
    return (
        pd.concat(scored, ignore_index=True) if scored else pd.DataFrame(),
        pd.DataFrame(metrics),
    )


def pooled_recognition(scored: pd.DataFrame) -> dict[str, Any]:
    if scored.empty:
        return {}
    labels = scored["original_target200"].astype(int)
    late = scored["late_target200"].astype(int)
    selected = scored[scored["sequence_selected"]]
    positives = int(labels.sum())
    late_positives = int(late.sum())
    return {
        "rows": int(len(scored)),
        "positives": positives,
        "base_precision": float(labels.mean()),
        "price_average_precision": float(average_precision_score(labels, scored["price_model_score"])),
        "sequence_average_precision": float(average_precision_score(labels, scored["sequence_score"])),
        "selected_rows": int(len(selected)),
        "selected_positives": int(selected["original_target200"].sum()),
        "selected_precision": float(selected["original_target200"].mean()) if len(selected) else 0.0,
        "positive_retention": float(selected["original_target200"].sum() / positives) if positives else 0.0,
        "late_positives": late_positives,
        "late_base_precision": float(late.mean()),
        "late_selected_positives": int(selected["late_target200"].sum()),
        "late_selected_precision": float(selected["late_target200"].mean()) if len(selected) else 0.0,
        "late_positive_retention": float(selected["late_target200"].sum() / late_positives) if late_positives else 0.0,
        "selected_unique_original_success_symbols": int(
            selected.loc[selected["original_target200"], "symbol"].nunique()
        ),
    }


def bootstrap_recognition(scored: pd.DataFrame, *, samples: int, seed: int) -> dict[str, Any]:
    rows = scored.copy()
    rows["week"] = rows["date"].dt.to_period("W-SUN").astype(str)
    groups = [group.index.to_numpy() for _, group in rows.groupby("week", sort=False)]
    rng = np.random.default_rng(seed)
    ap_delta: list[float] = []
    precision_delta: list[float] = []
    for _ in range(samples):
        indices = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        draw = rows.loc[indices]
        labels = draw["original_target200"].astype(int)
        if labels.nunique() < 2:
            continue
        selected = draw[draw["sequence_selected"]]
        if selected.empty:
            continue
        ap_delta.append(
            float(
                average_precision_score(labels, draw["sequence_score"])
                - average_precision_score(labels, draw["price_model_score"])
            )
        )
        precision_delta.append(float(selected["original_target200"].mean() - labels.mean()))
    ap = pd.Series(ap_delta, dtype=float)
    precision = pd.Series(precision_delta, dtype=float)
    return {
        "samples": int(len(ap)),
        "delta_ap_median": float(ap.median()),
        "delta_ap_lower_95pct": float(ap.quantile(0.025)),
        "delta_ap_upper_95pct": float(ap.quantile(0.975)),
        "delta_precision_median": float(precision.median()),
        "delta_precision_lower_95pct": float(precision.quantile(0.025)),
        "delta_precision_upper_95pct": float(precision.quantile(0.975)),
    }


def _metadata(row: pd.Series, *, score: float, mode: str) -> dict[str, Any]:
    return {
        "symbol": str(row["symbol"]),
        "signal_date": pd.Timestamp(row["date"]),
        "fold": int(row["fold"]),
        "score": float(score),
        "score_pctile": float(row["price_model_pctile"]),
        "signal_key": int(row["signal_key"]),
        "entry_rule": mode,
        "entry_delay_hours": float(row["checkpoint_hours"] if mode == "delayed_selected" else 0.0),
        "entry_chase_vs_signal_close": float(row["entry_chase_vs_signal_close"]),
        "target200_14d_from_entry": bool(row["late_target200"]),
        "future_max_return_14d_from_entry": float(row["late_future_max_return_14d"]),
        "sequence_selected": bool(row["sequence_selected"]),
        "sequence_score": float(row["sequence_score"]),
        "trade_mode": mode,
    }


def simulate_mode(
    rows: pd.DataFrame,
    bars_by_symbol: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    mode: str,
    slippage_bps: float,
) -> list[dict[str, Any]]:
    frozen = ENTRY.build_exit_policy_catalog()["frozen_half100_trail25"]
    outcomes: list[dict[str, Any]] = []
    for _, row in rows.iterrows():
        symbol = str(row["symbol"])
        bars = bars_by_symbol.get(symbol)
        if bars is None:
            continue
        selected = bool(row["sequence_selected"])
        early_policy = dict(frozen)
        if not selected:
            early_policy.update(
                {
                    "family": "sequence_reject",
                    "launch_deadline_days": float(row["checkpoint_hours"]) / 24.0,
                    "launch_mfe_required": 999.0,
                }
            )

        if mode == "delayed_selected":
            if not selected:
                continue
            outcome = ENTRY.simulate_stateful_trade(
                bars,
                entry_time=pd.Timestamp(row["decision_time"]),
                policy=frozen,
                slippage_bps=slippage_bps,
                funding=funding.get(symbol),
                include_marks=True,
            )
            if outcome is None:
                continue
            outcome.update(_metadata(row, score=float(row["sequence_score"]), mode=mode))
            outcomes.append(outcome)
            continue

        first = ENTRY.simulate_stateful_trade(
            bars,
            entry_time=pd.Timestamp(row["baseline_entry_time"]),
            policy=frozen if selected else early_policy,
            slippage_bps=slippage_bps,
            funding=funding.get(symbol),
            include_marks=True,
        )
        if first is None:
            continue
        if mode == "enter_then_exit_reject":
            first.update(_metadata(row, score=float(row["price_model_score"]), mode=mode))
            outcomes.append(first)
            continue
        if mode != "half_probe_then_add":
            raise ValueError(f"unknown trade mode: {mode}")

        second = None
        if selected:
            second = ENTRY.simulate_stateful_trade(
                bars,
                entry_time=pd.Timestamp(row["decision_time"]),
                policy=frozen,
                slippage_bps=slippage_bps,
                funding=funding.get(symbol),
                include_marks=True,
            )
        combined = SEQUENTIAL.combine_weighted_outcomes(first, second, first_weight=0.50)
        combined.update(_metadata(row, score=float(row["price_model_score"]), mode=mode))
        outcomes.append(combined)
    return outcomes


def summarize_outcomes(outcomes: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    accepted, equity, summary = STUDY.accepted_frame(outcomes)
    return accepted, equity, summary


def trade_mode_calibration(
    scored: pd.DataFrame,
    bars_by_symbol: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for mode in TRADE_MODES:
        outcomes = simulate_mode(scored, bars_by_symbol, funding, mode=mode, slippage_bps=5.0)
        outcomes = [row for row in outcomes if pd.Timestamp(row["exit_time"]) < cutoff]
        trades, _, _ = summarize_outcomes(outcomes)
        values = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        fold_expectancy = trades.groupby("fold")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
        leave_largest, concentration = STUDY.concentration_stats(trades)
        pf = profit_factor(values)
        eligible = bool(
            len(values) >= 20 and fold_expectancy.size >= 2 and values.mean() > 0 and pf > 1.0
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.40
        )
        records.append(
            {
                "trade_mode": mode,
                "trades": int(len(values)),
                "expectancy": float(values.mean()) if len(values) else np.nan,
                "profit_factor": float(pf),
                "folds": int(fold_expectancy.size),
                "fold_expectancy_mean": float(fold_expectancy.mean()) if len(fold_expectancy) else np.nan,
                "fold_expectancy_std": float(fold_expectancy.std(ddof=0)) if len(fold_expectancy) else np.nan,
                "leave_largest_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_score": float(fold_expectancy.mean() - 0.5 * fold_expectancy.std(ddof=0)) if len(fold_expectancy) else -np.inf,
                "selection_eligible": eligible,
            }
        )
    return pd.DataFrame(records).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    ).reset_index(drop=True)


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    entries = pd.read_csv(report / "entry_timing_candidates.csv.gz", compression="gzip")
    for column in ["date", "entry_time", "baseline_entry_time"]:
        entries[column] = pd.to_datetime(entries[column], utc=True)
    signals = entries[entries["entry_rule"] == "next_open"].copy().reset_index(drop=True)
    if signals.empty:
        raise RuntimeError("next_open candidate rows are missing")
    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    raw_bars = {symbol: group.sort_values("open_time").reset_index(drop=True) for symbol, group in panel.groupby("symbol", sort=False)}
    feature_bars = {symbol: ENTRY.add_past_only_4h_features(group) for symbol, group in raw_bars.items()}
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)

    checkpoint_frames: dict[int, pd.DataFrame] = {}
    for hours in CHECKPOINTS:
        checkpoint_frames[hours] = SEQUENTIAL.build_checkpoint_rows(signals, raw_bars, horizon_hours=hours)
    all_checkpoints = pd.concat(checkpoint_frames.values(), ignore_index=True)
    all_checkpoints.to_csv(report / "sequential_checkpoint_candidates.csv.gz", index=False, compression="gzip")

    calibration_records: list[dict[str, Any]] = []
    calibration_scored: dict[tuple[int, str], pd.DataFrame] = {}
    for hours in CHECKPOINTS:
        for family in MODEL_FAMILIES:
            scored, fold_metrics = score_candidate(
                checkpoint_frames[hours], folds, family=family, test_folds=(1, 2)
            )
            calibration_scored[(hours, family)] = scored
            pooled = pooled_recognition(scored)
            fold_joint = float((fold_metrics["ap_improved"] & fold_metrics["precision_improved"]).mean()) if not fold_metrics.empty else 0.0
            eligible = bool(
                len(fold_metrics) == 2 and pooled
                and pooled["sequence_average_precision"] > pooled["price_average_precision"]
                and pooled["selected_precision"] > pooled["base_precision"]
                and pooled["positive_retention"] >= 0.30
                and fold_joint >= 0.50
            )
            selection_score = -np.inf
            if pooled:
                selection_score = float(
                    pooled["sequence_average_precision"] - pooled["price_average_precision"]
                    + pooled["selected_precision"] - pooled["base_precision"]
                    + 0.10 * pooled["positive_retention"]
                    + 0.05 * pooled["late_selected_precision"]
                )
            calibration_records.append(
                {
                    "checkpoint_hours": hours,
                    "model_family": family,
                    "folds": int(len(fold_metrics)),
                    "fold_joint_improvement_share": fold_joint,
                    **pooled,
                    "selection_score": selection_score,
                    "selection_eligible": eligible,
                }
            )
    calibration = pd.DataFrame(calibration_records).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    ).reset_index(drop=True)
    calibration.to_csv(report / "sequential_model_calibration.csv", index=False)
    selected_spec = calibration.iloc[0]
    selected_hours = int(selected_spec["checkpoint_hours"])
    selected_family = str(selected_spec["model_family"])
    calibration_eligible = bool(selected_spec["selection_eligible"])
    selected_calibration_scored = calibration_scored[(selected_hours, selected_family)]

    mode_calibration = trade_mode_calibration(
        selected_calibration_scored,
        feature_bars,
        funding,
        cutoff=cutoff,
    )
    mode_calibration.to_csv(report / "sequential_trade_mode_calibration.csv", index=False)
    selected_mode = str(mode_calibration.iloc[0]["trade_mode"])
    mode_eligible = bool(mode_calibration.iloc[0]["selection_eligible"])

    confirmation_scored, confirmation_folds = score_candidate(
        checkpoint_frames[selected_hours],
        folds,
        family=selected_family,
        test_folds=(3, 4, 5, 6),
    )
    confirmation_scored.to_csv(report / "sequential_model_confirmation_scored.csv.gz", index=False, compression="gzip")
    confirmation_folds.to_csv(report / "sequential_model_confirmation_folds.csv", index=False)
    recognition = pooled_recognition(confirmation_scored)
    bootstrap = bootstrap_recognition(
        confirmation_scored,
        samples=args.bootstrap_samples,
        seed=20260730,
    )
    fold_majority = float(
        (confirmation_folds["ap_improved"] & confirmation_folds["precision_improved"]).mean()
    ) if not confirmation_folds.empty else 0.0
    ranking_gate = bool(
        calibration_eligible and len(confirmation_folds) >= 3 and fold_majority > 0.50
        and recognition["sequence_average_precision"] > recognition["price_average_precision"]
        and recognition["selected_precision"] > recognition["base_precision"]
        and recognition["positive_retention"] >= 0.25
        and bootstrap["delta_ap_lower_95pct"] > 0
        and bootstrap["delta_precision_lower_95pct"] > 0
    )

    summary_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate_mode(
            confirmation_scored,
            feature_bars,
            funding,
            mode=selected_mode,
            slippage_bps=slippage,
        )
        trades, equity, summary = summarize_outcomes(outcomes)
        if not summary:
            continue
        summary.update(
            {
                "trade_mode": selected_mode,
                "checkpoint_hours": selected_hours,
                "model_family": selected_family,
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
    trade_folds = STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summaries.to_csv(report / "sequential_strategy_confirmation_summary.csv", index=False)
    all_trades.to_csv(report / "sequential_strategy_confirmation_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(report / "sequential_strategy_confirmation_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(report / "sequential_strategy_confirmation_folds.csv", index=False)

    week = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="week", seed=20260731) if not default_trades.empty else {}
    symbol = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260801) if not default_trades.empty else {}
    leave_largest, concentration = STUDY.concentration_stats(default_trades)
    trade_fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summaries) == 3 and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all() and (summaries["max_drawdown"] >= -0.30).all()
    )
    trade_gate = bool(
        ranking_gate and mode_eligible and default_summary
        and default_summary.get("expectancy", -1) > 0 and default_summary.get("profit_factor", 0) > 1
        and trade_fold_majority > 0.50 and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week.get("expectancy_lower_95pct", -1) > 0
        and symbol.get("expectancy_lower_95pct", -1) > 0
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "prediction_clock": "use completed 4h bars strictly before the checkpoint; act at the checkpoint open",
        "candidate_checkpoints_hours": list(CHECKPOINTS),
        "candidate_model_families": list(MODEL_FAMILIES),
        "selection_quantile": SELECTION_QUANTILE,
        "calibration_folds": [1, 2],
        "confirmation_folds": [3, 4, 5, 6],
        "selected_checkpoint_hours": selected_hours,
        "selected_model_family": selected_family,
        "model_calibration_eligible": calibration_eligible,
        "selected_trade_mode": selected_mode,
        "trade_mode_calibration_eligible": mode_eligible,
        "recognition_confirmation": recognition,
        "recognition_fold_metrics": confirmation_folds.to_dict(orient="records"),
        "recognition_fold_joint_improvement_share": fold_majority,
        "recognition_bootstrap": bootstrap,
        "ranking_gate_pass": ranking_gate,
        "strategy_default_summary": default_summary,
        "strategy_stress_costs": summaries[["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]].to_dict(orient="records") if not summaries.empty else [],
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "strategy_fold_majority_profit_factor_gt_1": trade_fold_majority,
        "strategy_week_bootstrap": week,
        "strategy_symbol_bootstrap": symbol,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "paper_trade_gate_pass": trade_gate,
        "classification": "paper_candidate" if trade_gate else "watchlist_only",
    }
    (report / "sequential_strategy_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
