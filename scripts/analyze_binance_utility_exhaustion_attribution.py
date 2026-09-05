"""Attribute the profit-side exit challenger to static tail rules versus wick timing.

This is a fixed, secondary confirmation analysis.  It does not select another
policy.  The frozen P0+8h utility entries and confirmation folds 3-6 are reused,
and four already evaluated exits are simulated on exactly the same signals:

* frozen half at +100% / 25% trail;
* half at +150% / 30% trail;
* half at +150% / 35% trail;
* the same 35% trail plus the completed-bar volume-backed upper-wick exit.

The key contrast is wick versus the otherwise identical 35% static-tail policy.
Every dynamic trigger executes at the following 4h open.  The module is
research-only and has no order-routing imports.
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


EXHAUSTION = _load(
    "binance_utility_exhaustion_exit_for_attribution",
    ROOT / "scripts" / "analyze_binance_utility_exhaustion_exit.py",
)
FAILURE = EXHAUSTION.FAILURE
CONTINUATION = EXHAUSTION.CONTINUATION
ENTRY = EXHAUSTION.ENTRY
EXIT = EXHAUSTION.EXIT
STUDY = EXHAUSTION.STUDY
VALIDATION = EXHAUSTION.VALIDATION

FIXED_POLICY_ORDER = [
    "baseline_half100_trail25",
    "half150_trail30",
    "half150_trail35",
    "half150_trail35_wick",
]
EXPLORATORY_POLICY = "half150_trail30_wick_posthoc"
POLICY_ORDER = FIXED_POLICY_ORDER + [EXPLORATORY_POLICY]
CONTRASTS = [
    ("tail30_minus_baseline", "baseline_half100_trail25", "half150_trail30"),
    ("trail35_minus_trail30", "half150_trail30", "half150_trail35"),
    ("tail35_minus_baseline", "baseline_half100_trail25", "half150_trail35"),
    ("wick_minus_tail35", "half150_trail35", "half150_trail35_wick"),
    ("wick_minus_baseline", "baseline_half100_trail25", "half150_trail35_wick"),
    ("posthoc_wick30_minus_tail30", "half150_trail30", EXPLORATORY_POLICY),
    ("posthoc_wick30_minus_baseline", "baseline_half100_trail25", EXPLORATORY_POLICY),
    ("posthoc_wick30_minus_wick35", "half150_trail35_wick", EXPLORATORY_POLICY),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def fixed_policies(days: int) -> dict[str, dict[str, Any]]:
    available = EXHAUSTION.policies(days)
    result = {name: available[name] for name in FIXED_POLICY_ORDER}
    posthoc = dict(available["half150_trail30"])
    posthoc.update(
        {
            "family": "posthoc_tail30_plus_blowoff_exhaustion",
            "exhaustion_activation": 1.00,
            "exhaustion_upper_wick_min": 0.35,
            "exhaustion_close_location_max": 0.35,
            "exhaustion_volume_ratio_min": 1.50,
        }
    )
    result[EXPLORATORY_POLICY] = posthoc
    return result


def _exit_reason_counts(trades: pd.DataFrame) -> dict[str, int]:
    if trades.empty or "exit_reason" not in trades:
        return {}
    return {str(key): int(value) for key, value in trades["exit_reason"].value_counts().items()}


def first_signal_per_event(paired: pd.DataFrame, *, cooldown_days: int = 14) -> pd.DataFrame:
    """Keep the first signal in each symbol's non-overlapping cooldown window."""
    kept_indices: list[int] = []
    for _, group in paired.sort_values(["symbol", "signal_date", "signal_key"]).groupby("symbol", sort=False):
        event_start: pd.Timestamp | None = None
        for index, row in group.iterrows():
            signal_time = pd.Timestamp(row["signal_date"])
            if event_start is None or signal_time > event_start + pd.Timedelta(days=cooldown_days):
                kept_indices.append(int(index))
                event_start = signal_time
    return paired.loc[kept_indices].sort_values(["signal_date", "symbol"]).reset_index(drop=True)


def _contrast_summary(
    reference: list[dict[str, Any]],
    challenger: list[dict[str, Any]],
    *,
    contrast: str,
    reference_policy: str,
    challenger_policy: str,
    samples: int,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    paired = FAILURE.paired_deltas(reference, challenger)
    paired.insert(0, "contrast", contrast)
    paired.insert(1, "reference_policy", reference_policy)
    paired.insert(2, "challenger_policy", challenger_policy)
    fold_delta = paired.groupby("fold", sort=True)["delta_return"].agg(["count", "mean", "median"])
    fold_delta = fold_delta.reset_index().rename(
        columns={"count": "paired_rows", "mean": "mean_delta", "median": "median_delta"}
    )
    by_signal = pd.to_numeric(paired["delta_return"], errors="coerce").dropna()
    changed = by_signal[~np.isclose(by_signal, 0.0, rtol=0.0, atol=1e-12)]
    week = FAILURE.delta_bootstrap(paired, samples=samples, cluster="week", seed=seed)
    symbol = FAILURE.delta_bootstrap(paired, samples=samples, cluster="symbol", seed=seed + 1)
    event_paired = first_signal_per_event(paired)
    event_values = pd.to_numeric(event_paired["delta_return"], errors="coerce").dropna()
    event_changed = event_values[~np.isclose(event_values, 0.0, rtol=0.0, atol=1e-12)]
    event_fold_delta = event_paired.groupby("fold", sort=True)["delta_return"].mean()
    event_week = FAILURE.delta_bootstrap(event_paired, samples=samples, cluster="week", seed=seed + 100)
    event_symbol = FAILURE.delta_bootstrap(event_paired, samples=samples, cluster="symbol", seed=seed + 101)
    summary = {
        "contrast": contrast,
        "reference_policy": reference_policy,
        "challenger_policy": challenger_policy,
        "paired_rows": int(len(by_signal)),
        "changed_rows": int(len(changed)),
        "changed_row_share": float(len(changed) / len(by_signal)) if len(by_signal) else None,
        "positive_changed_rows": int((changed > 0).sum()),
        "negative_changed_rows": int((changed < 0).sum()),
        "mean_delta": float(by_signal.mean()) if len(by_signal) else None,
        "median_delta": float(by_signal.median()) if len(by_signal) else None,
        "mean_delta_when_changed": float(changed.mean()) if len(changed) else 0.0,
        "positive_fold_share": float((fold_delta["mean_delta"] > 0).mean()) if len(fold_delta) else 0.0,
        "fold_deltas": fold_delta.to_dict(orient="records"),
        "week_bootstrap": week,
        "symbol_bootstrap": symbol,
        "cluster_lower_bounds_positive": bool(
            week.get("mean_delta_lower_95pct", -np.inf) > 0
            and symbol.get("mean_delta_lower_95pct", -np.inf) > 0
        ),
        "independent_events": int(len(event_values)),
        "changed_independent_events": int(len(event_changed)),
        "event_mean_delta": float(event_values.mean()) if len(event_values) else None,
        "event_mean_delta_when_changed": float(event_changed.mean()) if len(event_changed) else 0.0,
        "event_positive_fold_share": float((event_fold_delta > 0).mean()) if len(event_fold_delta) else 0.0,
        "event_week_bootstrap": event_week,
        "event_symbol_bootstrap": event_symbol,
        "event_cluster_lower_bounds_positive": bool(
            event_week.get("mean_delta_lower_95pct", -np.inf) > 0
            and event_symbol.get("mean_delta_lower_95pct", -np.inf) > 0
        ),
    }
    return paired, event_paired, summary


def main() -> None:
    args = parse_args()
    baseline_dir = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    candidates = pd.read_csv(report / "continuation_entry_candidates.csv.gz", compression="gzip")
    for column in ("date", "decision_time"):
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    rows = candidates[candidates["checkpoint_hours"] == 8].copy()
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    panel = pd.read_csv(baseline_dir / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)
    confirmation_rows, _ = CONTINUATION.confirmation_scores(
        rows,
        folds,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=0.70,
    )

    raw_default: dict[str, list[dict[str, Any]]] = {}
    summary_records: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    fold_frames: list[pd.DataFrame] = []
    policy_summaries: dict[str, dict[str, Any]] = {}
    for policy_name, policy in fixed_policies(30).items():
        for slippage in (5.0, 15.0, 30.0):
            outcomes = FAILURE.simulate_policy(
                confirmation_rows,
                bars,
                funding,
                policy_name=policy_name,
                policy=policy,
                slippage_bps=slippage,
            )
            trades, equity, summary = FAILURE.portfolio(outcomes)
            if not summary:
                continue
            record = dict(summary)
            record.update(
                {
                    "policy": policy_name,
                    "slippage_bps_each_side": float(slippage),
                    "exit_reason_counts": json.dumps(_exit_reason_counts(trades), sort_keys=True),
                }
            )
            summary_records.append(record)
            trades["policy"] = policy_name
            trades["slippage_bps_each_side"] = float(slippage)
            trade_frames.append(trades)
            if not equity.empty:
                equity["policy"] = policy_name
                equity["slippage_bps_each_side"] = float(slippage)
                equity_frames.append(equity)
            if slippage == 5.0:
                raw_default[policy_name] = outcomes
                policy_summaries[policy_name] = summary
                fold_metrics = STUDY.fold_metrics(trades)
                fold_metrics.insert(0, "policy", policy_name)
                fold_frames.append(fold_metrics)

    summary_frame = pd.DataFrame(summary_records)
    all_trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    all_equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    all_folds = pd.concat(fold_frames, ignore_index=True) if fold_frames else pd.DataFrame()
    summary_frame.to_csv(report / "utility_exhaustion_attribution_summary.csv", index=False)
    all_trades.to_csv(report / "utility_exhaustion_attribution_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(report / "utility_exhaustion_attribution_equity.csv.gz", index=False, compression="gzip")
    all_folds.to_csv(report / "utility_exhaustion_attribution_folds.csv", index=False)

    paired_frames: list[pd.DataFrame] = []
    event_paired_frames: list[pd.DataFrame] = []
    contrast_summaries: dict[str, dict[str, Any]] = {}
    for index, (contrast, reference_name, challenger_name) in enumerate(CONTRASTS):
        paired, event_paired, summary = _contrast_summary(
            raw_default[reference_name],
            raw_default[challenger_name],
            contrast=contrast,
            reference_policy=reference_name,
            challenger_policy=challenger_name,
            samples=args.bootstrap_samples,
            seed=20260850 + index * 2,
        )
        paired_frames.append(paired)
        event_paired_frames.append(event_paired)
        contrast_summaries[contrast] = summary
    paired_all = pd.concat(paired_frames, ignore_index=True)
    paired_all.to_csv(report / "utility_exhaustion_attribution_paired_deltas.csv", index=False)
    event_paired_all = pd.concat(event_paired_frames, ignore_index=True)
    event_paired_all.to_csv(report / "utility_exhaustion_attribution_event_deltas.csv", index=False)

    pure_wick = contrast_summaries["wick_minus_tail35"]
    combined = contrast_summaries["wick_minus_baseline"]
    static = contrast_summaries["tail35_minus_baseline"]
    posthoc_combined = contrast_summaries["posthoc_wick30_minus_baseline"]
    posthoc_vs_current = contrast_summaries["posthoc_wick30_minus_wick35"]
    pure_wick_gate = bool(
        pure_wick["mean_delta"] is not None
        and pure_wick["mean_delta"] > 0
        and pure_wick["positive_fold_share"] > 0.50
        and pure_wick["cluster_lower_bounds_positive"]
        and pure_wick["event_positive_fold_share"] > 0.50
        and pure_wick["event_cluster_lower_bounds_positive"]
    )
    combined_mean = float(combined["mean_delta"] or 0.0)
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_historical_fixed_policy_factorial_attribution",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "entry_selection_quantile": 0.70,
        "confirmation_folds": [3, 4, 5, 6],
        "policy_selection_performed": False,
        "fixed_policies": FIXED_POLICY_ORDER,
        "posthoc_exploratory_policy": EXPLORATORY_POLICY,
        "contrast_order": [item[0] for item in CONTRASTS],
        "signal_clock": "completed 4h exhaustion bar; exit at following 4h open",
        "policy_default_cost_summaries": policy_summaries,
        "contrasts": contrast_summaries,
        "attribution": {
            "combined_wick_policy_increment": combined_mean,
            "static_tail35_increment": static["mean_delta"],
            "pure_wick_timing_increment": pure_wick["mean_delta"],
            "static_share_of_combined_point_increment": (
                float(static["mean_delta"] / combined_mean) if combined_mean > 0 else None
            ),
            "wick_share_of_combined_point_increment": (
                float(pure_wick["mean_delta"] / combined_mean) if combined_mean > 0 else None
            ),
            "additive_identity_error": (
                float(combined_mean - float(static["mean_delta"]) - float(pure_wick["mean_delta"]))
                if static["mean_delta"] is not None and pure_wick["mean_delta"] is not None
                else None
            ),
        },
        "posthoc_hypothesis": {
            "policy": EXPLORATORY_POLICY,
            "why_generated": "35pct trail was uniformly worse than 30pct while pure wick timing had a positive point estimate",
            "default_cost_summary": policy_summaries[EXPLORATORY_POLICY],
            "increment_vs_baseline": posthoc_combined,
            "increment_vs_current_wick35": posthoc_vs_current,
            "eligible_for_historical_promotion": False,
            "prospective_label_only": True,
        },
        "pure_wick_increment_gate_pass": pure_wick_gate,
        "classification": (
            "pure_wick_historical_increment_pending_true_oos"
            if pure_wick_gate
            else "wick_timing_not_independently_identified"
        ),
        "primary_exit_changed": False,
        "forward_counterfactual_recommendation": EXPLORATORY_POLICY,
        "automatic_trading_allowed": False,
    }
    (report / "utility_exhaustion_attribution_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
