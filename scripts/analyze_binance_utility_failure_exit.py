"""Test early failure-to-launch exits after the frozen P0+8h utility entry.

The entry model and threshold are not re-selected here.  Five pre-registered
exit policies are compared on the same time-isolated calibration signals using
a 14-day path, which is fully mature before fold 3 begins.  The chosen policy
is then restored to a 30-day maximum hold on confirmation folds 3-6.

Any failure trigger is evaluated only after a completed 4h bar and executes at
the following 4h open.  This module is research-only and has no order-routing
imports.
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
ENTRY_CHECKPOINT_HOURS = 8
ENTRY_SELECTION_QUANTILE = 0.70


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CONTINUATION = _load(
    "binance_continuation_entry_for_utility_failure_exit",
    ROOT / "scripts" / "analyze_binance_continuation_entry.py",
)
ENTRY = CONTINUATION.ENTRY
EXIT = CONTINUATION.EXIT
STUDY = CONTINUATION.STUDY
VALIDATION = CONTINUATION.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def policies(days: int) -> dict[str, dict[str, Any]]:
    base = CONTINUATION.frozen_policy(days)
    base["family"] = "utility_baseline"
    result = {"baseline": base}

    def add(name: str, *, deadline_hours_after_entry: int, required_mfe: float) -> None:
        policy = dict(base)
        policy.update(
            {
                "family": "utility_failure_to_launch",
                "launch_deadline_days": float(deadline_hours_after_entry) / 24.0,
                "launch_mfe_required": float(required_mfe),
            }
        )
        result[name] = policy

    # Entry occurs at P0+8h; these deadlines correspond to P0+24/48/72h.
    add("p0_24h_mfe10", deadline_hours_after_entry=16, required_mfe=0.10)
    add("p0_24h_mfe20", deadline_hours_after_entry=16, required_mfe=0.20)
    add("p0_48h_mfe20", deadline_hours_after_entry=40, required_mfe=0.20)
    add("p0_72h_mfe30", deadline_hours_after_entry=64, required_mfe=0.30)
    return result


def simulate_policy(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    policy_name: str,
    policy: Mapping[str, Any],
    slippage_bps: float,
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
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
                "validation_split": str(row.get("validation_split", f"fold_{int(row['fold'])}")),
                "score": float(row["continuation_score"]),
                "score_pctile": float(row["price_model_pctile"]),
                "signal_key": int(row["signal_key"]),
                "policy": policy_name,
                "entry_rule": "continuation_utility_8h",
                "entry_delay_hours": float(row["checkpoint_hours"]),
                "target200_14d_from_entry": bool(row["late_target200"]),
                "future_max_return_14d_from_entry": float(row["late_future_max_return_14d"]),
                "breadth_regime": row.get("breadth_regime"),
                "btc_trend_regime": row.get("btc_trend_regime"),
                "btc_vol_regime": row.get("btc_vol_regime"),
                "market_state": row.get("market_state"),
            }
        )
        outcomes.append(outcome)
    return outcomes


def portfolio(outcomes: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    accepted, equity, summary = EXIT.summarize_portfolio(outcomes, risk_stop_pct=0.25)
    trades = pd.DataFrame([EXIT.clean_trade(item) for item in accepted])
    return trades, equity, summary


def paired_deltas(
    baseline: list[dict[str, Any]],
    challenger: list[dict[str, Any]],
) -> pd.DataFrame:
    base = pd.DataFrame([EXIT.clean_trade(item) for item in baseline])
    other = pd.DataFrame([EXIT.clean_trade(item) for item in challenger])
    columns = ["signal_key", "symbol", "signal_date", "validation_split", "fold", "net_return"]
    base = base[columns].rename(columns={"net_return": "baseline_return"})
    other = other[["signal_key", "net_return"]].rename(columns={"net_return": "challenger_return"})
    paired = base.merge(other, on="signal_key", how="inner", validate="one_to_one")
    paired["delta_return"] = paired["challenger_return"] - paired["baseline_return"]
    return paired


def delta_bootstrap(
    paired: pd.DataFrame,
    *,
    samples: int,
    cluster: str,
    seed: int,
) -> dict[str, Any]:
    data = paired.copy()
    if cluster == "week":
        data["cluster"] = pd.to_datetime(data["signal_date"], utc=True).dt.to_period("W-SUN").astype(str)
    elif cluster == "symbol":
        data["cluster"] = data["symbol"].astype(str)
    else:
        raise ValueError(cluster)
    groups = [pd.to_numeric(group["delta_return"], errors="coerce").dropna().to_numpy(float) for _, group in data.groupby("cluster", sort=False)]
    groups = [values for values in groups if len(values)]
    rng = np.random.default_rng(seed)
    estimates: list[float] = []
    for _ in range(samples):
        draw = np.concatenate([groups[index] for index in rng.integers(0, len(groups), len(groups))])
        estimates.append(float(draw.mean()))
    values = pd.Series(estimates, dtype=float)
    return {
        "cluster": cluster,
        "samples": int(len(values)),
        "mean_delta_median": float(values.median()),
        "mean_delta_lower_95pct": float(values.quantile(0.025)),
        "mean_delta_upper_95pct": float(values.quantile(0.975)),
    }


def calibration_table(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
) -> tuple[pd.DataFrame, dict[str, list[dict[str, Any]]]]:
    raw: dict[str, list[dict[str, Any]]] = {}
    for name, policy in policies(14).items():
        raw[name] = simulate_policy(
            rows,
            bars,
            funding,
            policy_name=name,
            policy=policy,
            slippage_bps=5.0,
        )
    baseline = raw["baseline"]
    records: list[dict[str, Any]] = []
    for name, outcomes in raw.items():
        trades, _, summary = portfolio(outcomes)
        values = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        fold_expectancy = trades.groupby("validation_split")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
        leave_largest, concentration = STUDY.concentration_stats(trades)
        paired = paired_deltas(baseline, outcomes)
        delta_by_split = paired.groupby("validation_split")["delta_return"].mean() if not paired.empty else pd.Series(dtype=float)
        eligible = bool(
            name != "baseline" and len(values) >= 20 and fold_expectancy.size == 3
            and values.mean() > 0 and CONTINUATION.profit_factor(values) > 1
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.35
            and len(delta_by_split) == 3 and float((delta_by_split > 0).mean()) >= 2 / 3
            and float(delta_by_split.mean()) > 0
        )
        score = float(
            delta_by_split.mean() - 0.5 * delta_by_split.std(ddof=0)
        ) if len(delta_by_split) else -np.inf
        records.append(
            {
                "policy": name,
                "trades": int(len(values)),
                "expectancy": float(values.mean()) if len(values) else np.nan,
                "profit_factor": float(CONTINUATION.profit_factor(values)),
                "max_drawdown": summary.get("max_drawdown") if summary else np.nan,
                "folds": int(fold_expectancy.size),
                "fold_expectancy_mean": float(fold_expectancy.mean()) if len(fold_expectancy) else np.nan,
                "fold_expectancy_std": float(fold_expectancy.std(ddof=0)) if len(fold_expectancy) else np.nan,
                "paired_rows": int(len(paired)),
                "paired_delta_mean": float(paired["delta_return"].mean()) if len(paired) else np.nan,
                "paired_positive_split_share": float((delta_by_split > 0).mean()) if len(delta_by_split) else 0.0,
                "leave_largest_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_score": score,
                "selection_eligible": eligible,
            }
        )
    result = pd.DataFrame(records).sort_values(
        ["selection_eligible", "selection_score", "profit_factor"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    return result, raw


def _summary_with_name(summary: dict[str, Any], *, policy: str, slippage: float) -> dict[str, Any]:
    output = dict(summary)
    output.update({"policy": policy, "slippage_bps_each_side": float(slippage)})
    return output


def main() -> None:
    args = parse_args()
    baseline_dir = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    candidates = pd.read_csv(report / "continuation_entry_candidates.csv.gz", compression="gzip")
    for column in ("date", "decision_time"):
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    rows = candidates[candidates["checkpoint_hours"] == ENTRY_CHECKPOINT_HOURS].copy()
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    panel = pd.read_csv(baseline_dir / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)

    calibration_rows, _ = CONTINUATION.calibration_scores(
        rows,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=ENTRY_SELECTION_QUANTILE,
    )
    calibration, _ = calibration_table(calibration_rows, bars, funding)
    calibration.to_csv(report / "utility_failure_exit_calibration.csv", index=False)
    selected = calibration.iloc[0]
    selected_name = str(selected["policy"])
    selected_eligible = bool(selected["selection_eligible"])

    confirmation_rows, _ = CONTINUATION.confirmation_scores(
        rows,
        folds,
        label_column="utility_positive_14d",
        maturity_days=14,
        quantile=ENTRY_SELECTION_QUANTILE,
    )
    raw_baseline = simulate_policy(
        confirmation_rows,
        bars,
        funding,
        policy_name="baseline",
        policy=policies(30)["baseline"],
        slippage_bps=5.0,
    )
    raw_selected_default: list[dict[str, Any]] = []
    summary_records: list[dict[str, Any]] = []
    trade_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate_policy(
            confirmation_rows,
            bars,
            funding,
            policy_name=selected_name,
            policy=policies(30)[selected_name],
            slippage_bps=slippage,
        )
        trades, equity, summary = portfolio(outcomes)
        if not summary:
            continue
        summary_records.append(_summary_with_name(summary, policy=selected_name, slippage=slippage))
        trades["slippage_bps_each_side"] = slippage
        trade_frames.append(trades)
        if not equity.empty:
            equity["slippage_bps_each_side"] = slippage
            equity_frames.append(equity)
        if slippage == 5.0:
            raw_selected_default = outcomes
            default_trades = trades
            default_summary = summary

    baseline_trades, _, baseline_summary = portfolio(raw_baseline)
    summary_frame = pd.DataFrame(summary_records)
    all_trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    all_equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    trade_folds = STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summary_frame.to_csv(report / "utility_failure_exit_summary.csv", index=False)
    all_trades.to_csv(report / "utility_failure_exit_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(report / "utility_failure_exit_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(report / "utility_failure_exit_folds.csv", index=False)

    paired = paired_deltas(raw_baseline, raw_selected_default)
    paired.to_csv(report / "utility_failure_exit_paired_deltas.csv", index=False)
    week_delta = delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="week", seed=20260822)
    symbol_delta = delta_bootstrap(paired, samples=args.bootstrap_samples, cluster="symbol", seed=20260823)
    week_return = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="week", seed=20260824) if not default_trades.empty else {}
    symbol_return = EXIT.bootstrap_returns(default_trades, samples=args.bootstrap_samples, cluster="symbol", seed=20260825) if not default_trades.empty else {}
    leave_largest, concentration = STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summary_frame) == 3 and (summary_frame["expectancy"] > 0).all()
        and (summary_frame["profit_factor"] > 1).all()
        and (summary_frame["max_drawdown"] >= -0.30).all()
    )
    incremental_gate = bool(
        selected_name != "baseline" and selected_eligible and default_summary and baseline_summary
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
        "evidence_class": "secondary_historical_time_isolated_utility_exit_challenger",
        "entry_model": "continuation-utility-8h-v1-frozen-2026-07-19",
        "entry_checkpoint_hours": ENTRY_CHECKPOINT_HOURS,
        "entry_selection_quantile": ENTRY_SELECTION_QUANTILE,
        "candidate_policies": list(policies(30)),
        "failure_trigger_clock": "completed 4h bar; exit at next 4h open",
        "selected_policy": selected_name,
        "calibration_eligible": selected_eligible,
        "baseline_confirmation_summary": baseline_summary,
        "selected_confirmation_summary": default_summary,
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
        "incremental_exit_gate_pass": incremental_gate,
        "classification": "paper_exit_candidate_pending_true_oos" if incremental_gate else "retain_frozen_exit",
        "automatic_trading_allowed": False,
    }
    (report / "utility_failure_exit_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
