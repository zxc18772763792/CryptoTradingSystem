"""Audit an 8h taker-buy filter without delaying the frozen utility entry.

The frozen utility model already observes the first two completed 4h bars after
P0.  This diagnostic asks whether a deliberately simple condition -- quote
volume bought by takers is at least 50% -- enriches the remaining +200% tail.
The threshold is semantic rather than fitted.  Calibration is reconstructed
from the three mature pre-fold-3 windows and confirmation remains folds 3-6.

The condition was proposed after inspecting historical confirmation features,
so even a passing historical diagnostic is prospective-only.  It cannot change
the primary rank, original stage veto, position size, or trading authority.
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
from scipy.stats import fisher_exact


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"
TAKER_BUY_SHARE_THRESHOLD = 0.50
FILTER_NAME = "taker_buy_share_ge_50pct_at_8h"
ROUTER_NAME = "breakout6_half150_trail30_wick"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


HOLD = _load(
    "binance_utility_breakout_hold_router_for_taker_flow",
    ROOT / "scripts" / "analyze_binance_utility_breakout_hold_router.py",
)
CONTINUATION = HOLD.CONTINUATION
ENTRY = HOLD.ENTRY
EXIT = HOLD.EXIT
STUDY = HOLD.STUDY
TIMING = HOLD.TIMING
VALIDATION = HOLD.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def first_signal_per_event(rows: pd.DataFrame, *, cooldown_days: int = 14) -> pd.DataFrame:
    kept: list[int] = []
    ordered = rows.sort_values(["symbol", "decision_time", "signal_key"])
    for _, group in ordered.groupby("symbol", sort=False):
        event_start: pd.Timestamp | None = None
        for index, row in group.iterrows():
            decision = pd.Timestamp(row["decision_time"])
            if event_start is None or decision > event_start + pd.Timedelta(days=cooldown_days):
                kept.append(int(index))
                event_start = decision
    return rows.loc[kept].sort_values(["decision_time", "symbol"]).reset_index(drop=True)


def nested_precision_bootstrap(
    rows: pd.DataFrame,
    *,
    cluster: str,
    samples: int,
    seed: int,
) -> dict[str, Any]:
    """Bootstrap filtered-minus-all precision while preserving cluster rows."""
    data = rows.copy()
    if cluster == "week":
        data["_cluster"] = pd.to_datetime(data["decision_time"], utc=True).dt.to_period("W-SUN").astype(str)
    elif cluster == "symbol":
        data["_cluster"] = data["symbol"].astype(str)
    else:
        raise ValueError(f"Unsupported cluster: {cluster}")
    aggregates: list[tuple[int, int, int, int]] = []
    for _, group in data.groupby("_cluster", sort=False):
        chosen = group[group["taker_flow_supportive"]]
        aggregates.append(
            (
                int(group["late_target200"].sum()),
                int(len(group)),
                int(chosen["late_target200"].sum()),
                int(len(chosen)),
            )
        )
    if not aggregates:
        return {"cluster": cluster, "samples": 0}
    values = np.asarray(aggregates, dtype=float)
    rng = np.random.default_rng(seed)
    deltas: list[float] = []
    for _ in range(samples):
        drawn = values[rng.integers(0, len(values), size=len(values))].sum(axis=0)
        if drawn[1] <= 0 or drawn[3] <= 0:
            continue
        deltas.append(float(drawn[2] / drawn[3] - drawn[0] / drawn[1]))
    series = pd.Series(deltas, dtype=float)
    return {
        "cluster": cluster,
        "samples": int(len(series)),
        "precision_delta_median": float(series.median()),
        "precision_delta_lower_95pct": float(series.quantile(0.025)),
        "precision_delta_upper_95pct": float(series.quantile(0.975)),
    }


def recognition_summary(
    rows: pd.DataFrame,
    *,
    split_column: str,
    samples: int,
    seed: int,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    base = rows.copy()
    chosen = base[base["taker_flow_supportive"]].copy()
    excluded = base[~base["taker_flow_supportive"]].copy()
    base_positive = int(base["late_target200"].sum())
    selected_positive = int(chosen["late_target200"].sum())
    split_records: list[dict[str, Any]] = []
    for split, group in base.groupby(split_column, sort=True):
        selected = group[group["taker_flow_supportive"]]
        split_records.append(
            {
                split_column: split,
                "rows": int(len(group)),
                "positives": int(group["late_target200"].sum()),
                "base_precision": float(group["late_target200"].mean()),
                "selected_rows": int(len(selected)),
                "selected_positives": int(selected["late_target200"].sum()),
                "selected_precision": float(selected["late_target200"].mean()) if len(selected) else 0.0,
                "precision_increment": (
                    float(selected["late_target200"].mean() - group["late_target200"].mean())
                    if len(selected) else np.nan
                ),
                "base_utility_expectancy_14d": float(group["utility_return_14d"].mean()),
                "selected_utility_expectancy_14d": float(selected["utility_return_14d"].mean()) if len(selected) else np.nan,
            }
        )
    splits = pd.DataFrame(split_records)
    events = first_signal_per_event(base)
    selected_events = events[events["taker_flow_supportive"]]
    event_positive = int(events["late_target200"].sum())
    selected_event_positive = int(selected_events["late_target200"].sum())
    contingency = [
        [selected_positive, int(len(chosen) - selected_positive)],
        [int(excluded["late_target200"].sum()), int(len(excluded) - excluded["late_target200"].sum())],
    ]
    result = {
        "rows": int(len(base)),
        "positives": base_positive,
        "base_precision": float(base["late_target200"].mean()),
        "selected_rows": int(len(chosen)),
        "selected_positives": selected_positive,
        "selected_precision": float(chosen["late_target200"].mean()) if len(chosen) else 0.0,
        "precision_increment": float(chosen["late_target200"].mean() - base["late_target200"].mean()) if len(chosen) else None,
        "positive_retention": float(selected_positive / base_positive) if base_positive else 0.0,
        "base_utility_expectancy_14d": float(base["utility_return_14d"].mean()),
        "selected_utility_expectancy_14d": float(chosen["utility_return_14d"].mean()) if len(chosen) else None,
        "positive_split_share": float((splits["precision_increment"] > 0).mean()),
        "fisher_greater_p": float(fisher_exact(contingency, alternative="greater").pvalue),
        "week_bootstrap": nested_precision_bootstrap(base, cluster="week", samples=samples, seed=seed),
        "symbol_bootstrap": nested_precision_bootstrap(base, cluster="symbol", samples=samples, seed=seed + 1),
        "independent_events": int(len(events)),
        "positive_independent_events": event_positive,
        "selected_independent_events": int(len(selected_events)),
        "selected_positive_independent_events": selected_event_positive,
        "base_event_precision": float(events["late_target200"].mean()) if len(events) else 0.0,
        "selected_event_precision": float(selected_events["late_target200"].mean()) if len(selected_events) else 0.0,
        "event_precision_increment": (
            float(selected_events["late_target200"].mean() - events["late_target200"].mean())
            if len(selected_events) else None
        ),
        "event_positive_retention": float(selected_event_positive / event_positive) if event_positive else 0.0,
        "event_week_bootstrap": nested_precision_bootstrap(events, cluster="week", samples=samples, seed=seed + 100),
        "event_symbol_bootstrap": nested_precision_bootstrap(events, cluster="symbol", samples=samples, seed=seed + 101),
    }
    return result, splits, events


def add_filter(rows: pd.DataFrame) -> pd.DataFrame:
    result = rows.copy()
    result["early_taker_buy_share"] = pd.to_numeric(result["early_taker_buy_share"], errors="coerce")
    result["taker_flow_supportive"] = result["early_taker_buy_share"] >= TAKER_BUY_SHARE_THRESHOLD
    return result


def load_research_rows(report: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates = pd.read_csv(report / "continuation_entry_candidates.csv.gz", compression="gzip")
    for column in ["date", "decision_time", "baseline_entry_time"]:
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    frame = candidates[candidates["checkpoint_hours"] == 8].copy()
    calibration, _ = CONTINUATION.calibration_scores(
        frame,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=0.70,
    )
    calibration = add_filter(calibration[calibration["selected"]].copy())
    calibration["period"] = "calibration"
    confirmation = pd.read_csv(report / "continuation_utility_confirmation_scored.csv.gz", compression="gzip")
    for column in ["date", "decision_time", "baseline_entry_time"]:
        confirmation[column] = pd.to_datetime(confirmation[column], utc=True)
    confirmation = add_filter(confirmation[confirmation["selected"]].copy())
    confirmation["period"] = "confirmation"
    return calibration, confirmation


def load_market(baseline: Path) -> tuple[dict[str, pd.DataFrame], Mapping[str, pd.Series]]:
    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    return bars, VALIDATION.load_funding_history(AMBUSH_ROOT)


def timing_period(report: Path, period: str, supportive_keys: set[int]) -> tuple[pd.DataFrame, dict[int, pd.Timestamp]]:
    entries = pd.read_csv(report / "utility_entry_timing_candidates.csv.gz", compression="gzip")
    for column in ["date", "decision_time", "trigger_time", "entry_time"]:
        entries[column] = pd.to_datetime(entries[column], utc=True)
    subset = entries[entries["period"] == period].copy()
    launch = subset[subset["entry_rule"] == "launch_open"].copy()
    launch["taker_flow_supportive"] = launch["signal_key"].astype(int).isin(supportive_keys)
    breakout = subset[subset["entry_rule"] == "breakout6"]
    switches = {
        int(row["signal_key"]): pd.Timestamp(row["entry_time"])
        for _, row in breakout.iterrows()
    }
    return launch, switches


def strategy_variant(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    switches: Mapping[int, pd.Timestamp],
    *,
    filtered: bool,
    routed: bool,
    days: int,
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    selected = entries[entries["taker_flow_supportive"]].copy() if filtered else entries.copy()
    post_policy = HOLD.routed_policies(days)[ROUTER_NAME] if routed else None
    raw = HOLD.simulate_router(
        selected,
        bars,
        funding,
        switches,
        route_name=("taker50_" if filtered else "all_") + (ROUTER_NAME if routed else "frozen"),
        post_policy=post_policy,
        slippage_bps=slippage_bps,
        days=days,
    )
    trades, equity, summary = TIMING.portfolio(raw)
    return raw, trades, equity, summary


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    calibration, confirmation = load_research_rows(report)
    cal_recognition, cal_splits, cal_events = recognition_summary(
        calibration,
        split_column="validation_split",
        samples=args.bootstrap_samples,
        seed=20261010,
    )
    conf_recognition, conf_folds, conf_events = recognition_summary(
        confirmation,
        split_column="fold",
        samples=args.bootstrap_samples,
        seed=20261020,
    )

    bars, funding = load_market(baseline)
    supportive_keys = {
        "calibration": set(calibration.loc[calibration["taker_flow_supportive"], "signal_key"].astype(int)),
        "confirmation": set(confirmation.loc[confirmation["taker_flow_supportive"], "signal_key"].astype(int)),
    }
    cal_entries, cal_switches = timing_period(report, "calibration", supportive_keys["calibration"])
    conf_entries, conf_switches = timing_period(report, "confirmation", supportive_keys["confirmation"])

    cal_base_raw, _, _, _ = strategy_variant(
        cal_entries, bars, funding, cal_switches,
        filtered=True, routed=False, days=14, slippage_bps=5.0,
    )
    cal_router_raw, _, _, _ = strategy_variant(
        cal_entries, bars, funding, cal_switches,
        filtered=True, routed=True, days=14, slippage_bps=5.0,
    )
    cal_router_detail, _, _, cal_router_paired = HOLD.route_metrics(
        cal_base_raw,
        cal_router_raw,
        bootstrap_samples=args.bootstrap_samples,
        seed=20261030,
    )

    variants = [
        ("all_frozen", False, False),
        ("taker50_frozen", True, False),
        ("all_breakout_router", False, True),
        ("taker50_breakout_router", True, True),
    ]
    summaries: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    fold_frames: list[pd.DataFrame] = []
    default_raw: dict[str, list[dict[str, Any]]] = {}
    default_trades: dict[str, pd.DataFrame] = {}
    for variant, filtered, routed in variants:
        for slippage in (5.0, 15.0, 30.0):
            raw, trades, equity, strategy = strategy_variant(
                conf_entries,
                bars,
                funding,
                conf_switches,
                filtered=filtered,
                routed=routed,
                days=30,
                slippage_bps=slippage,
            )
            summaries.append(
                {
                    "variant": variant,
                    "slippage_bps_each_side": slippage,
                    "filtered": filtered,
                    "breakout_router": routed,
                    "input_signals": int(len(conf_entries[conf_entries["taker_flow_supportive"]])) if filtered else int(len(conf_entries)),
                    **strategy,
                }
            )
            if slippage == 5.0:
                default_raw[variant] = raw
                default_trades[variant] = trades
                if len(trades):
                    trades = trades.copy()
                    trades["variant"] = variant
                    trade_frames.append(trades)
                    folds = STUDY.fold_metrics(trades)
                    folds.insert(0, "variant", variant)
                    fold_frames.append(folds)
                if len(equity):
                    equity = equity.copy()
                    equity["variant"] = variant
                    equity_frames.append(equity)

    filtered_router_detail, _, _, filtered_router_paired = HOLD.route_metrics(
        default_raw["taker50_frozen"],
        default_raw["taker50_breakout_router"],
        bootstrap_samples=args.bootstrap_samples,
        seed=20261040,
    )
    absolute_intervals: dict[str, Any] = {}
    for variant in ["taker50_frozen", "taker50_breakout_router"]:
        trades = default_trades[variant]
        absolute_intervals[variant] = {
            "week": EXIT.bootstrap_returns(trades, samples=args.bootstrap_samples, cluster="week", seed=20261050),
            "symbol": EXIT.bootstrap_returns(trades, samples=args.bootstrap_samples, cluster="symbol", seed=20261051),
        }

    calibration_eligible = bool(
        cal_recognition["selected_rows"] >= 20
        and cal_recognition["selected_positives"] == cal_recognition["positives"]
        and cal_recognition["selected_precision"] > cal_recognition["base_precision"]
        and cal_recognition["positive_split_share"] >= 2 / 3
    )
    row_gate = bool(
        conf_recognition["selected_precision"] > conf_recognition["base_precision"]
        and conf_recognition["positive_retention"] >= 0.75
        and conf_recognition["week_bootstrap"]["precision_delta_lower_95pct"] > 0
        and conf_recognition["symbol_bootstrap"]["precision_delta_lower_95pct"] > 0
    )
    event_gate = bool(
        conf_recognition["selected_event_precision"] > conf_recognition["base_event_precision"]
        and conf_recognition["event_positive_retention"] >= 0.75
        and conf_recognition["event_week_bootstrap"]["precision_delta_lower_95pct"] > 0
        and conf_recognition["event_symbol_bootstrap"]["precision_delta_lower_95pct"] > 0
    )
    confirmation_gate = bool(row_gate and event_gate)
    monitor_text = (ROOT / "scripts" / "binance_continuation_utility_forward_monitor.py").read_text(encoding="utf-8")
    labeler_text = (ROOT / "scripts" / "binance_continuation_utility_labeler.py").read_text(encoding="utf-8")
    forward_annotation = bool(FILTER_NAME in monitor_text and "taker_flow_supportive" in labeler_text)
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "posthoc_confirmation_audited_taker_flow_filter_prospective_only",
        "filter_name": FILTER_NAME,
        "filter_clock": "two completed 4h bars after P0; no additional entry delay",
        "threshold": TAKER_BUY_SHARE_THRESHOLD,
        "threshold_rationale": "semantic buy-volume majority threshold; not optimized on confirmation outcomes",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "calibration": cal_recognition,
        "confirmation": conf_recognition,
        "calibration_recognition_eligible": calibration_eligible,
        "confirmation_row_gate_pass": row_gate,
        "confirmation_event_gate_pass": event_gate,
        "confirmation_full_gate_pass": confirmation_gate,
        "calibration_filtered_router_detail": cal_router_detail,
        "confirmation_filtered_router_detail": filtered_router_detail,
        "absolute_return_intervals": absolute_intervals,
        "historical_promotion_allowed": False,
        "why_no_historical_promotion": "the feature family was chosen after auditing already-viewed confirmation rows and the independent-event interval gate fails",
        "classification": "prospective_identification_annotation_pending_true_oos",
        "forward_annotation_added": forward_annotation,
        "forward_annotation_changes_rank": False,
        "forward_annotation_changes_stage_veto": False,
        "forward_annotation_changes_paper_eligibility": False,
        "primary_entry_changed": False,
        "primary_exit_changed": False,
        "automatic_trading_allowed": False,
    }

    cal_splits.to_csv(report / "utility_taker_flow_calibration_splits.csv", index=False)
    conf_folds.to_csv(report / "utility_taker_flow_confirmation_folds.csv", index=False)
    pd.concat(
        [
            cal_events.assign(period="calibration"),
            conf_events.assign(period="confirmation"),
        ],
        ignore_index=True,
    ).to_csv(report / "utility_taker_flow_event_rows.csv.gz", index=False, compression="gzip")
    pd.DataFrame(summaries).to_csv(report / "utility_taker_flow_strategy_summary.csv", index=False)
    pd.concat(trade_frames, ignore_index=True).to_csv(
        report / "utility_taker_flow_strategy_trades.csv.gz", index=False, compression="gzip"
    )
    pd.concat(equity_frames, ignore_index=True).to_csv(
        report / "utility_taker_flow_strategy_equity.csv.gz", index=False, compression="gzip"
    )
    pd.concat(fold_frames, ignore_index=True).to_csv(report / "utility_taker_flow_strategy_folds.csv", index=False)
    cal_router_paired.assign(period="calibration").to_csv(
        report / "utility_taker_flow_calibration_router_paired.csv", index=False
    )
    filtered_router_paired.assign(period="confirmation").to_csv(
        report / "utility_taker_flow_confirmation_router_paired.csv", index=False
    )
    (report / "utility_taker_flow_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready({
        "calibration": cal_recognition,
        "confirmation": conf_recognition,
        "strategy_summary": pd.DataFrame(summaries).query("slippage_bps_each_side == 5").to_dict(orient="records"),
        "confirmation_full_gate_pass": confirmation_gate,
        "classification": decision["classification"],
        "forward_annotation_added": forward_annotation,
    }), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
