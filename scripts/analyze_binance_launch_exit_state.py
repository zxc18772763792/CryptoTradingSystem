"""Validate post-launch failure and break-even exits after the 24h rule."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
AMBUSH_ROOT = ROOT / "data" / "research" / "ambush_modes"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


STOP = _load(
    "binance_launch_stop_for_state_exit",
    ROOT / "scripts" / "analyze_binance_launch_stop_strategy.py",
)
PRIMARY = STOP.PRIMARY
ROBUST = STOP.ROBUST
ENTRY = STOP.ENTRY
VALIDATION = STOP.VALIDATION
EXIT = STOP.EXIT


def policies() -> dict[str, dict[str, Any]]:
    base = dict(ENTRY.build_exit_policy_catalog()["frozen_half100_trail25"])
    base.update({"hard_stop": 0.25, "family": "launch_frozen"})
    result = {"launch_frozen_stop25": base}

    def add(name: str, **updates: Any) -> None:
        policy = dict(base)
        policy.update(updates)
        policy["family"] = name
        result[name] = policy

    add("post1d_mfe5", launch_deadline_days=1, launch_mfe_required=0.05)
    add("post1d_mfe10", launch_deadline_days=1, launch_mfe_required=0.10)
    add("post2d_mfe10", launch_deadline_days=2, launch_mfe_required=0.10)
    add("post2d_mfe20", launch_deadline_days=2, launch_mfe_required=0.20)
    add("breakeven20", breakeven_activation=0.20)
    add("breakeven30", breakeven_activation=0.30)
    add(
        "post1d_mfe10_breakeven20",
        launch_deadline_days=1,
        launch_mfe_required=0.10,
        breakeven_activation=0.20,
    )
    return result


def simulate(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    policy: Mapping[str, Any],
    policy_name: str,
    slippage_bps: float,
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    for _, row in rows[rows["sequence_selected"]].iterrows():
        symbol = str(row["symbol"])
        symbol_bars = bars.get(symbol)
        if symbol_bars is None:
            continue
        outcome = ENTRY.simulate_stateful_trade(
            symbol_bars,
            entry_time=pd.Timestamp(row["decision_time"]),
            policy=policy,
            slippage_bps=slippage_bps,
            funding=funding.get(symbol),
            include_marks=True,
        )
        if outcome is None:
            continue
        outcome.update(
            PRIMARY._metadata(
                row,
                score=float(row["price_model_score"]),
                mode="delayed_selected",
            )
        )
        outcome["policy"] = policy_name
        outcomes.append(outcome)
    return outcomes


def summarize(outcomes: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    accepted, equity, summary = EXIT.summarize_portfolio(outcomes, risk_stop_pct=0.25)
    trades = pd.DataFrame([EXIT.clean_trade(row) for row in accepted])
    return trades, equity, summary


def calibration_table(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for name, policy in policies().items():
        outcomes = simulate(rows, bars, funding, policy=policy, policy_name=name, slippage_bps=5.0)
        outcomes = [row for row in outcomes if pd.Timestamp(row["exit_time"]) < cutoff]
        trades, _, _ = summarize(outcomes)
        values = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        fold_expectancy = trades.groupby("fold")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
        leave_largest, concentration = PRIMARY.STUDY.concentration_stats(trades)
        pf = PRIMARY.profit_factor(values)
        eligible = bool(
            len(values) >= 20 and fold_expectancy.size >= 3 and values.mean() > 0 and pf > 1.0
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.40
        )
        records.append(
            {
                "policy": name,
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
    simple = json.loads((REPORT / "simple_launch_strategy_decision.json").read_text(encoding="utf-8"))
    selected_rule = str(simple["selected_rule"])
    candidates = pd.read_csv(REPORT / "sequential_checkpoint_candidates.csv.gz", compression="gzip")
    for column in ["date", "baseline_entry_time", "decision_time"]:
        candidates[column] = pd.to_datetime(candidates[column], utc=True)
    rows = candidates[candidates["checkpoint_hours"] == 24].copy()
    rows["sequence_selected"] = ROBUST.RULES[selected_rule][1](rows).fillna(False)
    rows["sequence_score"] = rows["price_model_score"]
    folds = pd.read_csv(REPORT / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])
    panel = pd.read_csv(BASELINE / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)
    calibration_rows = rows[rows["decision_time"] + pd.Timedelta(days=14) < cutoff].copy()
    calibration = calibration_table(calibration_rows, bars, funding, cutoff=cutoff)
    calibration.to_csv(REPORT / "launch_exit_state_calibration.csv", index=False)
    selected_name = str(calibration.iloc[0]["policy"])
    calibration_eligible = bool(calibration.iloc[0]["selection_eligible"])
    selected_policy = policies()[selected_name]

    confirmation = rows[rows["fold"].isin([3, 4, 5, 6])].copy()
    summary_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate(
            confirmation,
            bars,
            funding,
            policy=selected_policy,
            policy_name=selected_name,
            slippage_bps=slippage,
        )
        trades, equity, summary = summarize(outcomes)
        if not summary:
            continue
        summary.update({"policy": selected_name, "slippage_bps_each_side": slippage})
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
    trade_folds = PRIMARY.STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summaries.to_csv(REPORT / "launch_exit_state_summary.csv", index=False)
    all_trades.to_csv(REPORT / "launch_exit_state_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(REPORT / "launch_exit_state_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(REPORT / "launch_exit_state_folds.csv", index=False)

    week = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="week", seed=20260808) if not default_trades.empty else {}
    symbol = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="symbol", seed=20260809) if not default_trades.empty else {}
    leave_largest, concentration = PRIMARY.STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summaries) == 3 and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all() and (summaries["max_drawdown"] >= -0.30).all()
    )
    trade_gate = bool(
        calibration_eligible and default_summary
        and default_summary.get("expectancy", -1) > 0 and default_summary.get("profit_factor", 0) > 1
        and fold_majority > 0.50 and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week.get("expectancy_lower_95pct", -1) > 0
        and symbol.get("expectancy_lower_95pct", -1) > 0
    )
    output = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_historical_challenger",
        "entry_rule": selected_rule,
        "hard_stop": 0.25,
        "selected_exit_policy": selected_name,
        "calibration_eligible": calibration_eligible,
        "strategy_default_summary": default_summary,
        "strategy_stress_costs": summaries[["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]].to_dict(orient="records") if not summaries.empty else [],
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "fold_majority_profit_factor_gt_1": fold_majority,
        "week_bootstrap": week,
        "symbol_bootstrap": symbol,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "historical_trade_gate_pass": trade_gate,
        "classification": "paper_challenger_for_forward_oos" if trade_gate else "watchlist_only",
        "automatic_trading_allowed": False,
    }
    (REPORT / "launch_exit_state_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
