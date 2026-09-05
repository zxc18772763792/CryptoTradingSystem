"""Route frozen P0+8h entries with the calibrated P0+20h ordinal score.

Every trade first passes the frozen 8h utility gate.  Twelve hours later, at
P0+20h, the previously calibration-selected ordinal intensity model is scored
using only completed 4h bars.  High-intensity trades keep the frozen exit;
low-intensity trades exit at that decision open.  No checkpoint, model family,
or threshold is re-selected in this study.

The module is research-only and has no order-routing imports.
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
from sklearn.metrics import average_precision_score


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


FAILURE = _load(
    "binance_utility_failure_exit_for_intensity_router",
    ROOT / "scripts" / "analyze_binance_utility_failure_exit.py",
)
INTENSITY = _load(
    "binance_extreme_intensity_for_utility_router",
    ROOT / "scripts" / "analyze_binance_extreme_intensity.py",
)
CONTINUATION = FAILURE.CONTINUATION
ENTRY = FAILURE.ENTRY
EXIT = FAILURE.EXIT
STUDY = FAILURE.STUDY
VALIDATION = FAILURE.VALIDATION
ENTRY_HOURS = 8
ROUTE_HOURS = 20
ROUTE_DELAY_AFTER_ENTRY_HOURS = ROUTE_HOURS - ENTRY_HOURS
UTILITY_QUANTILE = 0.70
INTENSITY_QUANTILE = 0.80
INTENSITY_FAMILY = "ordinal"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def merge_scored(
    candidates: pd.DataFrame,
    folds: pd.DataFrame,
    *,
    period: str,
) -> pd.DataFrame:
    utility_rows = candidates[candidates["checkpoint_hours"] == ENTRY_HOURS].copy()
    intensity_rows = candidates[candidates["checkpoint_hours"] == ROUTE_HOURS].copy()
    if period == "calibration":
        utility, _ = CONTINUATION.calibration_scores(
            utility_rows,
            label_column="utility_positive_14d",
            maturity_days=14,
            quantile=UTILITY_QUANTILE,
        )
        intensity, _ = INTENSITY._calibration_scores(
            intensity_rows,
            family=INTENSITY_FAMILY,
            quantile=INTENSITY_QUANTILE,
        )
    elif period == "confirmation":
        utility, _ = CONTINUATION.confirmation_scores(
            utility_rows,
            folds,
            label_column="utility_positive_14d",
            maturity_days=14,
            quantile=UTILITY_QUANTILE,
        )
        intensity, _ = INTENSITY._confirmation_scores(
            intensity_rows,
            folds,
            family=INTENSITY_FAMILY,
            quantile=INTENSITY_QUANTILE,
        )
    else:
        raise ValueError(period)
    utility = utility[utility["selected"]].copy()
    intensity_keep = intensity[
        [
            "signal_key", "validation_split", "decision_time", "decision_open",
            "intensity_score", "intensity_threshold", "selected_raw",
            "late_target200", "late_future_max_return_14d",
        ]
    ].rename(
        columns={
            "decision_time": "route_decision_time",
            "decision_open": "route_decision_open",
            "selected_raw": "intensity_selected_raw",
            "late_target200": "late_target200_from_route",
            "late_future_max_return_14d": "late_mfe_from_route",
        }
    )
    merged = utility.merge(
        intensity_keep,
        on=["signal_key", "validation_split"],
        how="inner",
        validate="one_to_one",
    )
    merged["route_high_intensity"] = merged["intensity_score"] >= merged["intensity_threshold"]
    delay = (
        pd.to_datetime(merged["route_decision_time"], utc=True)
        - pd.to_datetime(merged["decision_time"], utc=True)
    ).dt.total_seconds() / 3600.0
    if not np.allclose(delay, ROUTE_DELAY_AFTER_ENTRY_HOURS):
        raise RuntimeError("8h entry and 20h route clocks are not exactly 12 hours apart")
    return merged


def recognition_summary(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    split_records: list[dict[str, Any]] = []
    for split, group in rows.groupby("validation_split", sort=False):
        labels = group["late_target200"].astype(int)
        high = group[group["route_high_intensity"]]
        split_records.append(
            {
                "validation_split": split,
                "rows": int(len(group)),
                "positives": int(labels.sum()),
                "high_rows": int(len(high)),
                "high_positives": int(high["late_target200"].sum()),
                "base_precision": float(labels.mean()),
                "high_precision": float(high["late_target200"].mean()) if len(high) else 0.0,
                "utility_average_precision": float(average_precision_score(labels, group["continuation_score"])) if labels.nunique() > 1 else np.nan,
                "intensity_average_precision": float(average_precision_score(labels, group["intensity_score"])) if labels.nunique() > 1 else np.nan,
            }
        )
    split_frame = pd.DataFrame(split_records)
    labels = rows["late_target200"].astype(int)
    high = rows[rows["route_high_intensity"]]
    joint = (
        (split_frame["high_precision"] > split_frame["base_precision"])
        & (split_frame["intensity_average_precision"] > split_frame["utility_average_precision"])
    ) if not split_frame.empty else pd.Series(dtype=bool)
    return split_frame, {
        "rows": int(len(rows)),
        "positives_from_entry": int(labels.sum()),
        "base_precision_from_entry": float(labels.mean()),
        "high_rows": int(len(high)),
        "high_positives_from_entry": int(high["late_target200"].sum()),
        "high_precision_from_entry": float(high["late_target200"].mean()) if len(high) else 0.0,
        "positive_retention_from_entry": float(high["late_target200"].sum() / labels.sum()) if labels.sum() else 0.0,
        "high_positives_from_route": int(high["late_target200_from_route"].sum()),
        "high_precision_from_route": float(high["late_target200_from_route"].mean()) if len(high) else 0.0,
        "utility_average_precision": float(average_precision_score(labels, rows["continuation_score"])) if labels.nunique() > 1 else np.nan,
        "intensity_average_precision": float(average_precision_score(labels, rows["intensity_score"])) if labels.nunique() > 1 else np.nan,
        "fold_joint_improvement_share": float(joint.mean()) if len(joint) else 0.0,
    }


def recognition_bootstrap(
    rows: pd.DataFrame,
    *,
    samples: int,
    cluster: str,
    seed: int,
) -> dict[str, Any]:
    data = rows.copy()
    if cluster == "week":
        data["cluster"] = pd.to_datetime(data["date"], utc=True).dt.to_period("W-SUN").astype(str)
    elif cluster == "symbol":
        data["cluster"] = data["symbol"].astype(str)
    else:
        raise ValueError(cluster)
    groups = [group.index.to_numpy() for _, group in data.groupby("cluster", sort=False)]
    rng = np.random.default_rng(seed)
    precision_delta: list[float] = []
    ap_delta: list[float] = []
    for _ in range(samples):
        indices = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        draw = data.loc[indices]
        labels = draw["late_target200"].astype(int)
        high = draw[draw["route_high_intensity"]]
        if labels.nunique() < 2 or high.empty:
            continue
        precision_delta.append(float(high["late_target200"].mean() - labels.mean()))
        ap_delta.append(float(average_precision_score(labels, draw["intensity_score"]) - average_precision_score(labels, draw["continuation_score"])))
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


def event_metrics(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for symbol, group in rows.sort_values(["symbol", "decision_time"]).groupby("symbol"):
        cluster_rows: list[pd.Series] = []
        previous: pd.Timestamp | None = None
        for _, row in group.iterrows():
            when = pd.Timestamp(row["decision_time"])
            if previous is not None and when - previous > pd.Timedelta(days=14):
                frame = pd.DataFrame(cluster_rows)
                records.append({"symbol": symbol, "start": frame["decision_time"].min(), "positive": bool(frame["late_target200"].any()), "selected": bool(frame["route_high_intensity"].any())})
                cluster_rows = []
            cluster_rows.append(row)
            previous = when
        if cluster_rows:
            frame = pd.DataFrame(cluster_rows)
            records.append({"symbol": symbol, "start": frame["decision_time"].min(), "positive": bool(frame["late_target200"].any()), "selected": bool(frame["route_high_intensity"].any())})
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


def routed_outcomes(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    days: int,
    slippage_bps: float,
) -> list[dict[str, Any]]:
    baseline_policy = CONTINUATION.frozen_policy(days)
    forced_policy = dict(baseline_policy)
    forced_policy.update(
        {
            "family": "intensity_reject_exit",
            "forced_exit_after_hours": ROUTE_DELAY_AFTER_ENTRY_HOURS,
            "forced_exit_reason": "intensity_reject_exit",
        }
    )
    high = rows[rows["route_high_intensity"]].copy()
    low = rows[~rows["route_high_intensity"]].copy()
    for frame in (high, low):
        frame["selected"] = True
    outcomes = FAILURE.simulate_policy(
        high,
        bars,
        funding,
        policy_name="intensity_high_keep_frozen",
        policy=baseline_policy,
        slippage_bps=slippage_bps,
    )
    outcomes.extend(
        FAILURE.simulate_policy(
            low,
            bars,
            funding,
            policy_name="intensity_low_exit_at_20h",
            policy=forced_policy,
            slippage_bps=slippage_bps,
        )
    )
    return outcomes


def main() -> None:
    args = parse_args()
    baseline_dir = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    candidates = pd.read_csv(report / "continuation_entry_candidates.csv.gz", compression="gzip")
    for column in ("date", "decision_time"):
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    panel = pd.read_csv(baseline_dir / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)

    calibration_rows = merge_scored(candidates, folds, period="calibration")
    calibration_recognition_folds, calibration_recognition = recognition_summary(calibration_rows)
    baseline_cal = FAILURE.simulate_policy(
        calibration_rows.assign(selected=True),
        bars,
        funding,
        policy_name="baseline",
        policy=CONTINUATION.frozen_policy(14),
        slippage_bps=5.0,
    )
    routed_cal = routed_outcomes(calibration_rows, bars, funding, days=14, slippage_bps=5.0)
    baseline_cal_trades, _, baseline_cal_summary = FAILURE.portfolio(baseline_cal)
    routed_cal_trades, _, routed_cal_summary = FAILURE.portfolio(routed_cal)
    paired_cal = FAILURE.paired_deltas(baseline_cal, routed_cal)
    delta_cal = paired_cal.groupby("validation_split")["delta_return"].mean()
    leave_cal, concentration_cal = STUDY.concentration_stats(routed_cal_trades)
    calibration_trade_eligible = bool(
        len(routed_cal_trades) >= 20 and len(delta_cal) == 3
        and float((delta_cal > 0).mean()) >= 2 / 3 and float(delta_cal.mean()) > 0
        and routed_cal_summary.get("expectancy", -1) > 0 and routed_cal_summary.get("profit_factor", 0) > 1
        and leave_cal is not None and leave_cal > 0
        and concentration_cal is not None and concentration_cal <= 0.35
    )
    calibration_summary = {
        "recognition": calibration_recognition,
        "recognition_folds": calibration_recognition_folds.to_dict(orient="records"),
        "baseline_trade_summary": baseline_cal_summary,
        "routed_trade_summary": routed_cal_summary,
        "paired_rows": int(len(paired_cal)),
        "paired_delta_mean": float(paired_cal["delta_return"].mean()),
        "paired_positive_split_share": float((delta_cal > 0).mean()),
        "trade_selection_eligible": calibration_trade_eligible,
    }
    (report / "utility_intensity_router_calibration.json").write_text(
        json.dumps(VALIDATION.json_ready(calibration_summary), ensure_ascii=False, indent=2), encoding="utf-8"
    )

    confirmation_rows = merge_scored(candidates, folds, period="confirmation")
    recognition_folds, recognition = recognition_summary(confirmation_rows)
    recognition_folds.to_csv(report / "utility_intensity_router_recognition_folds.csv", index=False)
    events, event_result = event_metrics(confirmation_rows)
    events.to_csv(report / "utility_intensity_router_event_clusters.csv", index=False)
    week_recognition = recognition_bootstrap(confirmation_rows, samples=args.bootstrap_samples, cluster="week", seed=20260830)
    symbol_recognition = recognition_bootstrap(confirmation_rows, samples=args.bootstrap_samples, cluster="symbol", seed=20260831)
    recognition_gate = bool(
        recognition["high_precision_from_entry"] > recognition["base_precision_from_entry"]
        and recognition["intensity_average_precision"] > recognition["utility_average_precision"]
        and recognition["positive_retention_from_entry"] >= 0.25
        and recognition["fold_joint_improvement_share"] > 0.50
        and week_recognition["precision_delta_lower_95pct"] > 0
        and week_recognition["ap_delta_lower_95pct"] > 0
        and symbol_recognition["precision_delta_lower_95pct"] > 0
        and symbol_recognition["ap_delta_lower_95pct"] > 0
    )

    baseline_default = FAILURE.simulate_policy(
        confirmation_rows.assign(selected=True),
        bars,
        funding,
        policy_name="baseline",
        policy=CONTINUATION.frozen_policy(30),
        slippage_bps=5.0,
    )
    baseline_trades, _, baseline_summary = FAILURE.portfolio(baseline_default)
    routed_default_raw: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    summary_records: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    for slippage in (5.0, 15.0, 30.0):
        outcomes = routed_outcomes(confirmation_rows, bars, funding, days=30, slippage_bps=slippage)
        trades, equity, summary = FAILURE.portfolio(outcomes)
        if not summary:
            continue
        record = dict(summary)
        record["slippage_bps_each_side"] = slippage
        summary_records.append(record)
        trades["slippage_bps_each_side"] = slippage
        trade_frames.append(trades)
        if not equity.empty:
            equity["slippage_bps_each_side"] = slippage
            equity_frames.append(equity)
        if slippage == 5.0:
            routed_default_raw = outcomes
            default_trades = trades
            default_summary = summary
    summary_frame = pd.DataFrame(summary_records)
    all_trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    all_equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    trade_folds = STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summary_frame.to_csv(report / "utility_intensity_router_summary.csv", index=False)
    all_trades.to_csv(report / "utility_intensity_router_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(report / "utility_intensity_router_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(report / "utility_intensity_router_trade_folds.csv", index=False)
    paired = FAILURE.paired_deltas(baseline_default, routed_default_raw)
    paired.to_csv(report / "utility_intensity_router_paired_deltas.csv", index=False)
    week_delta = FAILURE.delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="week", seed=20260832)
    symbol_delta = FAILURE.delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="symbol", seed=20260833)
    week_return = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="week", seed=20260834) if not default_trades.empty else {}
    symbol_return = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260835) if not default_trades.empty else {}
    leave_largest, concentration = STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summary_frame) == 3 and (summary_frame["expectancy"] > 0).all()
        and (summary_frame["profit_factor"] > 1).all()
        and (summary_frame["max_drawdown"] >= -0.30).all()
    )
    trade_gate = bool(
        calibration_trade_eligible and default_summary and baseline_summary
        and default_summary.get("expectancy", -1) > baseline_summary.get("expectancy", np.inf)
        and default_summary.get("profit_factor", 0) > baseline_summary.get("profit_factor", np.inf)
        and fold_majority > 0.50 and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week_return.get("expectancy_lower_95pct", -1) > 0
        and symbol_return.get("expectancy_lower_95pct", -1) > 0
        and week_delta.get("mean_delta_lower_95pct", -1) > 0
        and symbol_delta.get("mean_delta_lower_95pct", -1) > 0
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_historical_time_isolated_sequential_router",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "route_model": "ordinal_intensity_20h_q80_secondary",
        "route_clock": "P0+20h using completed bars; low-intensity exit fills at that open",
        "selection_reused_without_retuning": True,
        "calibration": calibration_summary,
        "confirmation_recognition": recognition,
        "confirmation_recognition_folds": recognition_folds.to_dict(orient="records"),
        "event_metrics": event_result,
        "week_recognition_bootstrap": week_recognition,
        "symbol_recognition_bootstrap": symbol_recognition,
        "recognition_gate_pass": recognition_gate,
        "baseline_confirmation_summary": baseline_summary,
        "routed_confirmation_summary": default_summary,
        "strategy_stress_costs": summary_frame.to_dict(orient="records"),
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "fold_majority_profit_factor_gt_1": fold_majority,
        "week_return_bootstrap": week_return,
        "symbol_return_bootstrap": symbol_return,
        "week_increment_bootstrap": week_delta,
        "symbol_increment_bootstrap": symbol_delta,
        "paired_increment_mean": float(paired["delta_return"].mean()) if len(paired) else None,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "trade_gate_pass": trade_gate,
        "classification": "paper_router_candidate_pending_true_oos" if trade_gate else "retain_8h_entry_and_frozen_exit",
        "automatic_trading_allowed": False,
    }
    (report / "utility_intensity_router_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
