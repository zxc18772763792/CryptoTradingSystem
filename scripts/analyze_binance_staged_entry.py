"""Calibrate staged probe/add allocations around the frozen 24h launch rule."""

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
PROBE_FRACTIONS = (0.0, 0.10, 0.25, 0.50, 0.75, 1.0)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


STATE = _load("binance_launch_exit_state_for_staged", ROOT / "scripts" / "analyze_binance_launch_exit_state.py")
PRIMARY = STATE.PRIMARY
ROBUST = STATE.ROBUST
ENTRY = STATE.ENTRY
VALIDATION = STATE.VALIDATION
EXIT = STATE.EXIT
SEQUENTIAL = PRIMARY.SEQUENTIAL


def exit_policy() -> dict[str, Any]:
    return dict(STATE.policies()["breakeven30"])


def simulate(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    probe_fraction: float,
    slippage_bps: float,
) -> list[dict[str, Any]]:
    if not 0.0 <= probe_fraction <= 1.0:
        raise ValueError("probe_fraction must be in [0, 1]")
    policy = exit_policy()
    outcomes: list[dict[str, Any]] = []
    for _, row in rows.iterrows():
        selected = bool(row["sequence_selected"])
        if probe_fraction == 0.0 and not selected:
            continue
        symbol = str(row["symbol"])
        symbol_bars = bars.get(symbol)
        if symbol_bars is None:
            continue

        first = None
        if probe_fraction > 0:
            early_policy = dict(policy)
            if not selected:
                early_policy.update(
                    {
                        "family": "staged_probe_reject",
                        "launch_deadline_days": 1.0,
                        "launch_mfe_required": 999.0,
                    }
                )
            first = ENTRY.simulate_stateful_trade(
                symbol_bars,
                entry_time=pd.Timestamp(row["baseline_entry_time"]),
                policy=early_policy,
                slippage_bps=slippage_bps,
                funding=funding.get(symbol),
                include_marks=True,
            )
            if first is None:
                continue

        second = None
        if selected and probe_fraction < 1.0:
            second = ENTRY.simulate_stateful_trade(
                symbol_bars,
                entry_time=pd.Timestamp(row["decision_time"]),
                policy=policy,
                slippage_bps=slippage_bps,
                funding=funding.get(symbol),
                include_marks=True,
            )
            if second is None:
                continue

        if probe_fraction == 0.0:
            combined = dict(second)
            combined["probe_weight"] = 0.0
            combined["add_weight"] = 1.0
        elif probe_fraction == 1.0:
            combined = dict(first)
            combined["probe_weight"] = 1.0
            combined["add_weight"] = 0.0
        else:
            combined = SEQUENTIAL.combine_weighted_outcomes(first, second, first_weight=probe_fraction)

        combined.update(PRIMARY._metadata(row, score=float(row["price_model_score"]), mode="staged_entry"))
        combined.update(
            {
                "original_target200": bool(row["original_target200"]),
                "launch_confirmed": selected,
                "staged_status": "confirmed_hold" if selected else "rejected_probe",
                "probe_fraction": float(probe_fraction),
                "add_fraction": float(1.0 - probe_fraction if selected else 0.0),
                "policy": "stop25_breakeven30_half100_trail25",
                "slippage_bps_each_side": float(slippage_bps),
            }
        )
        outcomes.append(combined)
    return outcomes


def summarize(outcomes: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    accepted, equity, summary = EXIT.summarize_portfolio(outcomes, risk_stop_pct=0.25)
    trades = pd.DataFrame(
        [
            EXIT.clean_trade(row)
            | {
                "original_target200": row["original_target200"],
                "launch_confirmed": row["launch_confirmed"],
                "probe_fraction": row["probe_fraction"],
                "add_fraction": row["add_fraction"],
                "staged_status": row["staged_status"],
            }
            for row in accepted
        ]
    )
    return trades, equity, summary


def calibration_table(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for fraction in PROBE_FRACTIONS:
        outcomes = simulate(rows, bars, funding, probe_fraction=fraction, slippage_bps=5.0)
        outcomes = [row for row in outcomes if pd.Timestamp(row["exit_time"]) < cutoff]
        trades, _, summary = summarize(outcomes)
        values = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        fold_expectancy = trades.groupby("fold")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
        leave_largest, concentration = PRIMARY.STUDY.concentration_stats(trades)
        pf = PRIMARY.profit_factor(values)
        eligible = bool(
            len(values) >= 20 and fold_expectancy.size >= 3 and values.mean() > 0 and pf > 1.0
            and summary.get("max_drawdown", -1.0) >= -0.30
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.40
        )
        score = float(fold_expectancy.mean() - 0.50 * fold_expectancy.std(ddof=0)) if len(fold_expectancy) else -np.inf
        records.append(
            {
                "probe_fraction": float(fraction),
                "add_fraction_after_launch": float(1.0 - fraction),
                "trades": int(len(values)),
                "launch_confirmed_trade_share": float(trades["launch_confirmed"].mean()) if len(trades) else np.nan,
                "expectancy": float(values.mean()) if len(values) else np.nan,
                "profit_factor": float(pf),
                "max_drawdown": summary.get("max_drawdown"),
                "folds": int(fold_expectancy.size),
                "fold_expectancy_mean": float(fold_expectancy.mean()) if len(fold_expectancy) else np.nan,
                "fold_expectancy_std": float(fold_expectancy.std(ddof=0)) if len(fold_expectancy) else np.nan,
                "leave_largest_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_score": score,
                "selection_eligible": eligible,
            }
        )
    return pd.DataFrame(records).sort_values(["selection_eligible", "selection_score"], ascending=[False, False]).reset_index(drop=True)


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
    bars = {symbol: ENTRY.add_past_only_4h_features(group) for symbol, group in panel.groupby("symbol", sort=False)}
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)

    calibration_rows = rows[rows["decision_time"] + pd.Timedelta(days=14) < cutoff].copy()
    calibration = calibration_table(calibration_rows, bars, funding, cutoff=cutoff)
    calibration.to_csv(REPORT / "staged_entry_calibration.csv", index=False)
    selected_fraction = float(calibration.iloc[0]["probe_fraction"])
    calibration_eligible = bool(calibration.iloc[0]["selection_eligible"])

    confirmation = rows[rows["fold"].isin([3, 4, 5, 6])].copy()
    summaries_list: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate(confirmation, bars, funding, probe_fraction=selected_fraction, slippage_bps=slippage)
        trades, equity, summary = summarize(outcomes)
        if not summary:
            continue
        summary.update({"probe_fraction": selected_fraction, "slippage_bps_each_side": slippage})
        summaries_list.append(summary)
        trades["slippage_bps_each_side"] = slippage
        trade_records.extend(trades.to_dict(orient="records"))
        if not equity.empty:
            equity["slippage_bps_each_side"] = slippage
            equity_records.extend(equity.to_dict(orient="records"))
        if slippage == 5.0:
            default_trades = trades
            default_summary = summary
    summaries = pd.DataFrame(summaries_list)
    all_trades = pd.DataFrame(trade_records)
    all_equity = pd.DataFrame(equity_records)
    trade_folds = PRIMARY.STUDY.fold_metrics(default_trades) if not default_trades.empty else pd.DataFrame()
    summaries.to_csv(REPORT / "staged_entry_confirmation_summary.csv", index=False)
    all_trades.to_csv(REPORT / "staged_entry_confirmation_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(REPORT / "staged_entry_confirmation_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(REPORT / "staged_entry_confirmation_folds.csv", index=False)

    diagnostic_records: list[dict[str, Any]] = []
    baseline_summary: dict[str, Any] = {}
    for fraction in PROBE_FRACTIONS:
        outcomes = simulate(confirmation, bars, funding, probe_fraction=fraction, slippage_bps=5.0)
        trades, _, summary = summarize(outcomes)
        diagnostic_records.append(
            {
                "probe_fraction": float(fraction),
                "trades": summary.get("trades"),
                "expectancy": summary.get("expectancy"),
                "profit_factor": summary.get("profit_factor"),
                "max_drawdown": summary.get("max_drawdown"),
                "launch_confirmed_trade_share": float(trades["launch_confirmed"].mean()) if len(trades) else np.nan,
                "staged_reject_share": float((trades["staged_status"] == "rejected_probe").mean()) if len(trades) else np.nan,
            }
        )
        if fraction == 0.0:
            baseline_summary = summary
    diagnostic = pd.DataFrame(diagnostic_records)
    diagnostic.to_csv(REPORT / "staged_entry_confirmation_catalog_diagnostic.csv", index=False)

    week = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="week", seed=20260814) if not default_trades.empty else {}
    symbol_boot = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="symbol", seed=20260815) if not default_trades.empty else {}
    leave_largest, concentration = PRIMARY.STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summaries) == 3 and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all() and (summaries["max_drawdown"] >= -0.30).all()
    )
    incremental = bool(
        selected_fraction > 0 and default_summary
        and default_summary.get("expectancy", -1) > baseline_summary.get("expectancy", np.inf)
        and default_summary.get("profit_factor", 0) > baseline_summary.get("profit_factor", np.inf)
    )
    trade_gate = bool(
        calibration_eligible and incremental and fold_majority > 0.50 and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week.get("expectancy_lower_95pct", -1) > 0
        and symbol_boot.get("expectancy_lower_95pct", -1) > 0
    )
    output = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "tertiary_staged_entry_study_after_half_probe_diagnostic",
        "launch_rule": selected_rule,
        "candidate_probe_fractions": list(PROBE_FRACTIONS),
        "selected_probe_fraction": selected_fraction,
        "selected_add_fraction_after_launch": 1.0 - selected_fraction,
        "calibration_eligible": calibration_eligible,
        "baseline_strategy_summary": baseline_summary,
        "strategy_default_summary": default_summary,
        "strategy_stress_costs": summaries[["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]].to_dict(orient="records") if not summaries.empty else [],
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "fold_majority_profit_factor_gt_1": fold_majority,
        "week_bootstrap": week,
        "symbol_bootstrap": symbol_boot,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "incremental_vs_confirmation_only": incremental,
        "confirmation_catalog_selection_allowed": False,
        "historical_trade_gate_pass": trade_gate,
        "classification": "paper_challenger_for_forward_oos" if trade_gate else "watchlist_only",
        "automatic_trading_allowed": False,
    }
    (REPORT / "staged_entry_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
