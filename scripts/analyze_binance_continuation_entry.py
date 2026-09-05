"""Time-isolated continuation and entry-utility challengers for Binance pumps.

The direct challenger predicts whether +200% remains after the actual decision
open.  The utility challenger predicts whether the frozen 14-day trade path is
positive, then evaluates the unchanged 30-day exit on historical confirmation
folds.  Model/checkpoint/threshold selection uses only pre-fold-3 calibration
windows.  This module is research-only and has no order-routing imports.
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
from sklearn.metrics import average_precision_score


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"
CHECKPOINTS = (4, 8, 12, 16, 20, 24)
QUANTILES = (0.60, 0.70, 0.80, 0.90)
CALIBRATION_WINDOWS = (
    ("cal_1", "2026-01-23", "2026-02-07"),
    ("cal_2", "2026-02-07", "2026-02-22"),
    ("cal_3", "2026-02-22", "2026-03-09"),
)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SEQUENTIAL = _load(
    "binance_sequential_validation_for_continuation_entry",
    ROOT / "core" / "research" / "binance_sequential_validation.py",
)
ENTRY = _load(
    "binance_entry_exit_validation_for_continuation_entry",
    ROOT / "core" / "research" / "binance_entry_exit_validation.py",
)
VALIDATION = _load(
    "binance_runup_validation_for_continuation_entry",
    ROOT / "core" / "research" / "binance_runup_validation.py",
)
STUDY = _load(
    "binance_entry_timing_for_continuation_entry",
    ROOT / "scripts" / "analyze_binance_entry_exit_timing.py",
)
EXIT = STUDY.EXIT
FEATURES = [*SEQUENTIAL.SEQUENCE_FEATURES, "entry_chase_vs_signal_close"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def profit_factor(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def frozen_policy(days: int) -> dict[str, Any]:
    policy = dict(ENTRY.build_exit_policy_catalog()["frozen_half100_trail25"])
    policy.update(
        {
            "family": f"continuation_utility_{days}d",
            "hard_stop": 0.25,
            "time_days": int(days),
            "breakeven_activation": 0.30,
        }
    )
    return policy


def simulate_label_returns(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
) -> pd.DataFrame:
    output = rows.copy()
    returns: list[float] = []
    funding_observed: list[bool] = []
    policy = frozen_policy(14)
    for _, row in output.iterrows():
        symbol = str(row["symbol"])
        frame = bars.get(symbol)
        outcome = None if frame is None else ENTRY.simulate_stateful_trade(
            frame,
            entry_time=pd.Timestamp(row["decision_time"]),
            policy=policy,
            slippage_bps=5.0,
            funding=funding.get(symbol),
            fee_bps=5.0,
        )
        returns.append(np.nan if outcome is None else float(outcome["net_return"]))
        funding_observed.append(False if outcome is None else bool(outcome["funding_observed"]))
    output["utility_return_14d"] = returns
    output["utility_positive_14d"] = output["utility_return_14d"] > 0
    output["utility_funding_observed"] = funding_observed
    return output


def apply_daily_limit(rows: pd.DataFrame, selected_column: str, score_column: str) -> pd.Series:
    selected = pd.Series(False, index=rows.index, dtype=bool)
    candidates = rows[rows[selected_column]].copy()
    if candidates.empty:
        return selected
    candidates["decision_day"] = pd.to_datetime(candidates["decision_time"], utc=True).dt.floor("D")
    keep = (
        candidates.sort_values(["decision_day", score_column], ascending=[True, False])
        .groupby("decision_day", sort=False)
        .head(3)
        .index
    )
    selected.loc[keep] = True
    return selected


def score_split(
    rows: pd.DataFrame,
    *,
    train_before: pd.Timestamp,
    test: pd.DataFrame,
    label_column: str,
    maturity_days: int,
    quantile: float,
) -> pd.DataFrame | None:
    train = rows[
        pd.to_datetime(rows["decision_time"], utc=True) + pd.Timedelta(days=maturity_days)
        < pd.Timestamp(train_before)
    ].copy()
    test = test.copy()
    train = train[train[label_column].notna()]
    test = test[test[label_column].notna()]
    if len(train) < 30 or int(train[label_column].astype(int).sum()) < 3 or test.empty:
        return None
    train_score, test_score = SEQUENTIAL.fit_sequence_score(
        train,
        test,
        family="logistic",
        features=FEATURES,
        label_column=label_column,
    )
    threshold = float(np.quantile(train_score, quantile))
    test["continuation_score"] = test_score
    test["continuation_threshold"] = threshold
    test["selected_raw"] = test["continuation_score"] >= threshold
    test["selected"] = apply_daily_limit(test, "selected_raw", "continuation_score")
    return test


def calibration_scores(
    rows: pd.DataFrame,
    *,
    label_column: str,
    maturity_days: int,
    quantile: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    for name, start_raw, end_raw in CALIBRATION_WINDOWS:
        start = pd.Timestamp(start_raw, tz="UTC")
        end = pd.Timestamp(end_raw, tz="UTC")
        test = rows[
            (pd.to_datetime(rows["decision_time"], utc=True) >= start)
            & (pd.to_datetime(rows["decision_time"], utc=True) < end)
        ].copy()
        result = score_split(
            rows,
            train_before=start,
            test=test,
            label_column=label_column,
            maturity_days=maturity_days,
            quantile=quantile,
        )
        if result is None:
            continue
        result["validation_split"] = name
        labels = result[label_column].astype(int)
        selected = result[result["selected"]]
        metrics.append(
            {
                "validation_split": name,
                "test_rows": int(len(result)),
                "test_positives": int(labels.sum()),
                "selected_rows": int(len(selected)),
                "selected_positives": int(selected[label_column].astype(int).sum()),
                "base_precision": float(labels.mean()),
                "selected_precision": float(selected[label_column].astype(int).mean()) if len(selected) else 0.0,
                "price_average_precision": float(average_precision_score(labels, result["price_model_score"])) if labels.nunique() > 1 else np.nan,
                "continuation_average_precision": float(average_precision_score(labels, result["continuation_score"])) if labels.nunique() > 1 else np.nan,
            }
        )
        scored.append(result)
    return (
        pd.concat(scored, ignore_index=True) if scored else pd.DataFrame(),
        pd.DataFrame(metrics),
    )


def continuation_summary(scored: pd.DataFrame, folds: pd.DataFrame) -> dict[str, Any]:
    if scored.empty:
        return {}
    labels = scored["late_target200"].astype(int)
    selected = scored[scored["selected"]]
    valid_ap = labels.nunique() > 1
    joint = (
        (folds["selected_precision"] > folds["base_precision"])
        & (folds["continuation_average_precision"] > folds["price_average_precision"])
    ) if not folds.empty else pd.Series(dtype=bool)
    return {
        "rows": int(len(scored)),
        "positives": int(labels.sum()),
        "base_precision": float(labels.mean()),
        "price_average_precision": float(average_precision_score(labels, scored["price_model_score"])) if valid_ap else np.nan,
        "continuation_average_precision": float(average_precision_score(labels, scored["continuation_score"])) if valid_ap else np.nan,
        "selected_rows": int(len(selected)),
        "selected_positives": int(selected["late_target200"].sum()),
        "selected_precision": float(selected["late_target200"].mean()) if len(selected) else 0.0,
        "positive_retention": float(selected["late_target200"].sum() / labels.sum()) if labels.sum() else 0.0,
        "unique_selected_symbols": int(selected["symbol"].nunique()),
        "fold_joint_improvement_share": float(joint.mean()) if len(joint) else 0.0,
    }


def utility_summary(scored: pd.DataFrame, folds: pd.DataFrame) -> dict[str, Any]:
    if scored.empty:
        return {}
    selected = scored[scored["selected"]]
    returns = pd.to_numeric(selected["utility_return_14d"], errors="coerce").dropna()
    all_returns = pd.to_numeric(scored["utility_return_14d"], errors="coerce").dropna()
    fold_returns = selected.groupby("validation_split")["utility_return_14d"].mean() if "validation_split" in selected else pd.Series(dtype=float)
    leave_largest = float(returns.drop(returns.idxmax()).mean()) if len(returns) > 1 else np.nan
    positive = returns[returns > 0]
    concentration = float(positive.max() / positive.sum()) if positive.sum() > 0 else 1.0
    return {
        "rows": int(len(scored)),
        "base_expectancy_14d": float(all_returns.mean()),
        "base_profit_factor_14d": float(profit_factor(all_returns)),
        "selected_rows": int(len(selected)),
        "selected_symbols": int(selected["symbol"].nunique()),
        "selected_expectancy_14d": float(returns.mean()) if len(returns) else np.nan,
        "selected_profit_factor_14d": float(profit_factor(returns)) if len(returns) else 0.0,
        "selected_win_rate_14d": float((returns > 0).mean()) if len(returns) else 0.0,
        "fold_expectancy_mean": float(fold_returns.mean()) if len(fold_returns) else np.nan,
        "fold_expectancy_std": float(fold_returns.std(ddof=0)) if len(fold_returns) else np.nan,
        "fold_positive_expectancy_share": float((fold_returns > 0).mean()) if len(fold_returns) else 0.0,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_return_share": concentration,
        "late_target200_selected": int(selected["late_target200"].sum()),
        "late_target200_precision": float(selected["late_target200"].mean()) if len(selected) else 0.0,
    }


def confirmation_scores(
    rows: pd.DataFrame,
    folds: pd.DataFrame,
    *,
    label_column: str,
    maturity_days: int,
    quantile: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored: list[pd.DataFrame] = []
    metrics: list[dict[str, Any]] = []
    for fold in (3, 4, 5, 6):
        start = pd.Timestamp(folds.loc[folds["fold"] == fold, "test_start"].iloc[0])
        test = rows[rows["fold"] == fold].copy()
        result = score_split(
            rows,
            train_before=start,
            test=test,
            label_column=label_column,
            maturity_days=maturity_days,
            quantile=quantile,
        )
        if result is None:
            continue
        result["validation_split"] = f"fold_{fold}"
        result["fold"] = fold
        labels = result[label_column].astype(int)
        selected = result[result["selected"]]
        metrics.append(
            {
                "fold": fold,
                "test_rows": int(len(result)),
                "test_positives": int(labels.sum()),
                "selected_rows": int(len(selected)),
                "selected_positives": int(selected[label_column].astype(int).sum()),
                "base_precision": float(labels.mean()),
                "selected_precision": float(selected[label_column].astype(int).mean()) if len(selected) else 0.0,
                "price_average_precision": float(average_precision_score(labels, result["price_model_score"])) if labels.nunique() > 1 else np.nan,
                "continuation_average_precision": float(average_precision_score(labels, result["continuation_score"])) if labels.nunique() > 1 else np.nan,
            }
        )
        scored.append(result)
    return (
        pd.concat(scored, ignore_index=True) if scored else pd.DataFrame(),
        pd.DataFrame(metrics),
    )


def recognition_bootstrap(scored: pd.DataFrame, *, samples: int, cluster: str, seed: int) -> dict[str, Any]:
    rows = scored.copy()
    if cluster == "week":
        rows["cluster"] = pd.to_datetime(rows["decision_time"], utc=True).dt.to_period("W-SUN").astype(str)
    elif cluster == "symbol":
        rows["cluster"] = rows["symbol"].astype(str)
    else:
        raise ValueError(cluster)
    groups = [group.index.to_numpy() for _, group in rows.groupby("cluster", sort=False)]
    rng = np.random.default_rng(seed)
    precision_delta: list[float] = []
    ap_delta: list[float] = []
    for _ in range(samples):
        indices = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        draw = rows.loc[indices]
        labels = draw["late_target200"].astype(int)
        selected = draw[draw["selected"]]
        if labels.nunique() < 2 or selected.empty:
            continue
        precision_delta.append(float(selected["late_target200"].mean() - labels.mean()))
        ap_delta.append(float(average_precision_score(labels, draw["continuation_score"]) - average_precision_score(labels, draw["price_model_score"])))
    precision = pd.Series(precision_delta, dtype=float)
    ap = pd.Series(ap_delta, dtype=float)
    return {
        "cluster": cluster,
        "samples": int(min(len(precision), len(ap))),
        "precision_delta_median": float(precision.median()),
        "precision_delta_lower_95pct": float(precision.quantile(0.025)),
        "precision_delta_upper_95pct": float(precision.quantile(0.975)),
        "ap_delta_median": float(ap.median()),
        "ap_delta_lower_95pct": float(ap.quantile(0.025)),
        "ap_delta_upper_95pct": float(ap.quantile(0.975)),
    }


def event_metrics(scored: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for symbol, group in scored.sort_values(["symbol", "decision_time"]).groupby("symbol"):
        cluster: list[pd.Series] = []
        previous: pd.Timestamp | None = None
        for _, row in group.iterrows():
            when = pd.Timestamp(row["decision_time"])
            if previous is not None and when - previous > pd.Timedelta(days=14):
                frame = pd.DataFrame(cluster)
                records.append({"symbol": symbol, "start": frame["decision_time"].min(), "positive": bool(frame["late_target200"].any()), "selected": bool(frame["selected"].any())})
                cluster = []
            cluster.append(row)
            previous = when
        if cluster:
            frame = pd.DataFrame(cluster)
            records.append({"symbol": symbol, "start": frame["decision_time"].min(), "positive": bool(frame["late_target200"].any()), "selected": bool(frame["selected"].any())})
    events = pd.DataFrame(records)
    selected = events[events["selected"]] if not events.empty else pd.DataFrame()
    positives = int(events["positive"].sum()) if not events.empty else 0
    return events, {
        "events": int(len(events)),
        "positive_events": positives,
        "selected_events": int(len(selected)),
        "selected_positive_events": int(selected["positive"].sum()) if len(selected) else 0,
        "selected_event_precision": float(selected["positive"].mean()) if len(selected) else 0.0,
        "positive_event_retention": float(selected["positive"].sum() / positives) if len(selected) and positives else 0.0,
    }


def simulate_confirmation(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    slippage_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    policy = frozen_policy(30)
    for _, row in rows[rows["selected"]].iterrows():
        symbol = str(row["symbol"])
        frame = bars.get(symbol)
        outcome = None if frame is None else ENTRY.simulate_stateful_trade(
            frame,
            entry_time=pd.Timestamp(row["decision_time"]),
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
                "score": float(row["continuation_score"]),
                "score_pctile": float(row["price_model_pctile"]),
                "signal_key": int(row["signal_key"]),
                "entry_rule": "continuation_utility",
                "entry_delay_hours": float(row["checkpoint_hours"]),
                "target200_14d_from_entry": bool(row["late_target200"]),
                "future_max_return_14d_from_entry": float(row["late_future_max_return_14d"]),
                "continuation_score": float(row["continuation_score"]),
                "policy": "hard25_be30_half100_trail25_time30",
                "breadth_regime": row.get("breadth_regime"),
                "btc_trend_regime": row.get("btc_trend_regime"),
                "btc_vol_regime": row.get("btc_vol_regime"),
                "market_state": row.get("market_state"),
            }
        )
        outcomes.append(outcome)
    accepted, equity, summary = EXIT.summarize_portfolio(outcomes, risk_stop_pct=0.25)
    trades = pd.DataFrame([EXIT.clean_trade(item) for item in accepted])
    return trades, equity, summary


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    entries = pd.read_csv(report / "entry_timing_candidates.csv.gz", compression="gzip")
    for column in ["date", "entry_time", "baseline_entry_time"]:
        entries[column] = pd.to_datetime(entries[column], utc=True)
    signals = entries[entries["entry_rule"] == "next_open"].copy().reset_index(drop=True)
    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    raw_bars = {symbol: group.sort_values("open_time").reset_index(drop=True) for symbol, group in panel.groupby("symbol", sort=False)}
    feature_bars = {symbol: ENTRY.add_past_only_4h_features(group) for symbol, group in raw_bars.items()}
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)

    frames: dict[int, pd.DataFrame] = {}
    context_columns = [
        "signal_key", "breadth_regime", "btc_trend_regime", "btc_vol_regime",
        "market_state", "listing_age_bucket", "spot_listing", "liquidity_bucket",
    ]
    context = signals[[column for column in context_columns if column in signals.columns]].drop_duplicates("signal_key")
    for hours in CHECKPOINTS:
        built = SEQUENTIAL.build_checkpoint_rows(signals, raw_bars, horizon_hours=hours)
        built = built.merge(context, on="signal_key", how="left", validate="many_to_one")
        frames[hours] = simulate_label_returns(built, feature_bars, funding)
    candidates = pd.concat(frames.values(), ignore_index=True)
    candidates.to_csv(report / "continuation_entry_candidates.csv.gz", index=False, compression="gzip")

    direct_records: list[dict[str, Any]] = []
    direct_cache: dict[tuple[int, float], tuple[pd.DataFrame, pd.DataFrame]] = {}
    utility_records: list[dict[str, Any]] = []
    utility_cache: dict[tuple[int, float], tuple[pd.DataFrame, pd.DataFrame]] = {}
    for hours, frame in frames.items():
        for quantile in QUANTILES:
            direct_scored, direct_folds = calibration_scores(frame, label_column="late_target200", maturity_days=14, quantile=quantile)
            direct_cache[(hours, quantile)] = (direct_scored, direct_folds)
            direct = continuation_summary(direct_scored, direct_folds)
            direct_eligible = bool(
                direct and len(direct_folds) == 3 and direct["selected_rows"] >= 20
                and direct["continuation_average_precision"] > direct["price_average_precision"]
                and direct["selected_precision"] > direct["base_precision"]
                and direct["positive_retention"] >= 0.25
                and direct["fold_joint_improvement_share"] >= 2 / 3
            )
            direct_score = float(
                direct.get("selected_precision", 0) - direct.get("base_precision", 0)
                + 0.5 * (direct.get("continuation_average_precision", 0) - direct.get("price_average_precision", 0))
                + 0.10 * direct.get("positive_retention", 0) - 0.0005 * hours
            ) if direct else -np.inf
            direct_records.append({"checkpoint_hours": hours, "selection_quantile": quantile, **direct, "selection_score": direct_score, "selection_eligible": direct_eligible})

            utility_scored, utility_folds = calibration_scores(frame, label_column="utility_positive_14d", maturity_days=14, quantile=quantile)
            utility_cache[(hours, quantile)] = (utility_scored, utility_folds)
            utility = utility_summary(utility_scored, utility_folds)
            utility_eligible = bool(
                utility and len(utility_folds) == 3 and utility["selected_rows"] >= 20
                and utility["selected_symbols"] >= 10
                and utility["selected_expectancy_14d"] > 0
                and utility["selected_profit_factor_14d"] > 1
                and utility["fold_positive_expectancy_share"] >= 2 / 3
                and utility["leave_largest_winner_out_expectancy"] > 0
                and utility["largest_positive_return_share"] <= 0.35
            )
            utility_score = float(
                utility.get("fold_expectancy_mean", -1)
                - 0.5 * utility.get("fold_expectancy_std", 1)
                - 0.0005 * hours
            ) if utility else -np.inf
            utility_records.append({"checkpoint_hours": hours, "selection_quantile": quantile, **utility, "selection_score": utility_score, "selection_eligible": utility_eligible})

    direct_grid = pd.DataFrame(direct_records).sort_values(["selection_eligible", "selection_score"], ascending=[False, False]).reset_index(drop=True)
    utility_grid = pd.DataFrame(utility_records).sort_values(["selection_eligible", "selection_score"], ascending=[False, False]).reset_index(drop=True)
    direct_grid.to_csv(report / "continuation_target200_calibration.csv", index=False)
    utility_grid.to_csv(report / "continuation_utility_calibration.csv", index=False)

    direct_pick = direct_grid.iloc[0]
    direct_hours = int(direct_pick["checkpoint_hours"])
    direct_quantile = float(direct_pick["selection_quantile"])
    direct_confirmation, direct_confirmation_folds = confirmation_scores(
        frames[direct_hours], folds, label_column="late_target200", maturity_days=14, quantile=direct_quantile
    )
    direct_confirmation.to_csv(report / "continuation_target200_confirmation_scored.csv.gz", index=False, compression="gzip")
    direct_confirmation_folds.to_csv(report / "continuation_target200_confirmation_folds.csv", index=False)
    direct_result = continuation_summary(direct_confirmation, direct_confirmation_folds)
    week_direct = recognition_bootstrap(direct_confirmation, samples=args.bootstrap_samples, cluster="week", seed=20260810)
    symbol_direct = recognition_bootstrap(direct_confirmation, samples=args.bootstrap_samples, cluster="symbol", seed=20260811)
    events, event_result = event_metrics(direct_confirmation)
    events.to_csv(report / "continuation_target200_event_clusters.csv", index=False)
    direct_gate = bool(
        direct_pick["selection_eligible"] and direct_result
        and direct_result["fold_joint_improvement_share"] > 0.50
        and direct_result["continuation_average_precision"] > direct_result["price_average_precision"]
        and direct_result["selected_precision"] > direct_result["base_precision"]
        and direct_result["positive_retention"] >= 0.25
        and week_direct["precision_delta_lower_95pct"] > 0 and week_direct["ap_delta_lower_95pct"] > 0
        and symbol_direct["precision_delta_lower_95pct"] > 0 and symbol_direct["ap_delta_lower_95pct"] > 0
    )

    utility_pick = utility_grid.iloc[0]
    utility_hours = int(utility_pick["checkpoint_hours"])
    utility_quantile = float(utility_pick["selection_quantile"])
    utility_confirmation, utility_confirmation_folds = confirmation_scores(
        frames[utility_hours], folds, label_column="utility_positive_14d", maturity_days=14, quantile=utility_quantile
    )
    utility_confirmation.to_csv(report / "continuation_utility_confirmation_scored.csv.gz", index=False, compression="gzip")
    utility_confirmation_folds.to_csv(report / "continuation_utility_confirmation_folds.csv", index=False)
    utility_recognition = utility_summary(utility_confirmation, utility_confirmation_folds)

    summaries: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        trades, equity, summary = simulate_confirmation(utility_confirmation, feature_bars, funding, slippage_bps=slippage)
        if not summary:
            continue
        summary.update({"slippage_bps_each_side": slippage, "checkpoint_hours": utility_hours, "selection_quantile": utility_quantile})
        summaries.append(summary)
        trades["slippage_bps_each_side"] = slippage
        trade_frames.append(trades)
        if not equity.empty:
            equity["slippage_bps_each_side"] = slippage
            equity_frames.append(equity)
        if slippage == 5.0:
            default_trades = trades
            default_summary = summary
    summary_frame = pd.DataFrame(summaries)
    all_trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    all_equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    trade_folds = STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summary_frame.to_csv(report / "continuation_utility_strategy_summary.csv", index=False)
    all_trades.to_csv(report / "continuation_utility_strategy_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(report / "continuation_utility_strategy_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(report / "continuation_utility_strategy_folds.csv", index=False)
    week_trade = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="week", seed=20260812) if not default_trades.empty else {}
    symbol_trade = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260813) if not default_trades.empty else {}
    leave_largest, concentration = STUDY.concentration_stats(default_trades)
    largest_symbol = None
    largest_symbol_positive_pnl_share = None
    leave_largest_symbol_out_expectancy = None
    if not default_trades.empty:
        positive_by_symbol = (
            default_trades[default_trades["pnl_usd"] > 0]
            .groupby("symbol")["pnl_usd"].sum().sort_values(ascending=False)
        )
        if not positive_by_symbol.empty:
            largest_symbol = str(positive_by_symbol.index[0])
            largest_symbol_positive_pnl_share = float(positive_by_symbol.iloc[0] / positive_by_symbol.sum())
            leave_largest_symbol_out_expectancy = float(
                default_trades.loc[default_trades["symbol"] != largest_symbol, "net_return"].mean()
            )
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(len(summary_frame) == 3 and (summary_frame["expectancy"] > 0).all() and (summary_frame["profit_factor"] > 1).all() and (summary_frame["max_drawdown"] >= -0.30).all())
    utility_gate = bool(
        utility_pick["selection_eligible"] and default_summary
        and default_summary.get("expectancy", -1) > 0 and default_summary.get("profit_factor", 0) > 1
        and fold_majority > 0.50 and stress_ok
        and default_summary.get("market_states", 0) >= 3
        and default_summary.get("market_state_majority_profit_factor_gt_1", 0) > 0.50
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and leave_largest_symbol_out_expectancy is not None and leave_largest_symbol_out_expectancy > 0
        and largest_symbol_positive_pnl_share is not None and largest_symbol_positive_pnl_share <= 0.25
        and week_trade.get("expectancy_lower_95pct", -1) > 0
        and symbol_trade.get("expectancy_lower_95pct", -1) > 0
    )

    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "historical_time_isolated_continuation_and_utility_challenger",
        "prediction_clock": "features use completed 4h bars strictly before checkpoint; hypothetical entry is checkpoint open",
        "calibration_windows": [{"name": name, "start": start, "end": end} for name, start, end in CALIBRATION_WINDOWS],
        "confirmation_folds": [3, 4, 5, 6],
        "candidate_checkpoints_hours": list(CHECKPOINTS),
        "candidate_quantiles": list(QUANTILES),
        "model_family": "strongly_regularized_logistic",
        "features": FEATURES,
        "direct_continuation": {
            "target": "+200% within 14d from actual checkpoint open",
            "selected_checkpoint_hours": direct_hours,
            "selected_quantile": direct_quantile,
            "calibration_eligible": bool(direct_pick["selection_eligible"]),
            "confirmation": direct_result,
            "event_metrics": event_result,
            "week_bootstrap": week_direct,
            "symbol_bootstrap": symbol_direct,
            "ranking_gate_pass": direct_gate,
        },
        "entry_utility": {
            "training_target": "positive net return under frozen 14d path; 14d maturity purge",
            "confirmation_trade_policy": "hard25_be30_half100_trail25_time30",
            "selected_checkpoint_hours": utility_hours,
            "selected_quantile": utility_quantile,
            "calibration_eligible": bool(utility_pick["selection_eligible"]),
            "confirmation_label_metrics": utility_recognition,
            "strategy_default_summary": default_summary,
            "strategy_stress_costs": summary_frame[["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]].to_dict(orient="records") if not summary_frame.empty else [],
            "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
            "fold_majority_profit_factor_gt_1": fold_majority,
            "week_bootstrap": week_trade,
            "symbol_bootstrap": symbol_trade,
            "leave_largest_winner_out_expectancy": leave_largest,
            "largest_positive_pnl_share": concentration,
            "largest_positive_symbol": largest_symbol,
            "largest_symbol_positive_pnl_share": largest_symbol_positive_pnl_share,
            "leave_largest_symbol_out_expectancy": leave_largest_symbol_out_expectancy,
            "paper_trade_gate_pass": utility_gate,
        },
        "identification_classification": "ranking_candidate" if direct_gate else "watchlist_only",
        "entry_strategy_classification": "paper_candidate_pending_true_oos" if utility_gate else "watchlist_only",
        "classification": "paper_entry_candidate_pending_true_oos" if utility_gate else "watchlist_only",
        "automatic_trading_allowed": False,
    }
    (report / "continuation_entry_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
