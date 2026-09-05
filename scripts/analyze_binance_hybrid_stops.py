"""Calibrate close-confirmed initial stops for the 24h launch challenger."""

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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


STATE = _load("binance_launch_exit_state_for_hybrid", ROOT / "scripts" / "analyze_binance_launch_exit_state.py")
HYBRID = _load("binance_hybrid_stop_validation_standalone", ROOT / "core" / "research" / "binance_hybrid_stop_validation.py")
PRIMARY = STATE.PRIMARY
ROBUST = STATE.ROBUST
ENTRY = STATE.ENTRY
VALIDATION = STATE.VALIDATION
EXIT = STATE.EXIT


def policy_catalog() -> dict[str, dict[str, Any]]:
    base = dict(STATE.policies()["breakeven30"])
    base["family"] = "baseline_hard25"
    result = {"baseline_hard25": {"mode": "hard", "risk_width": 0.25, **base}}

    def add(name: str, catastrophe: float, close_stop: float, close_bars: int) -> None:
        policy = dict(base)
        policy.update(
            {
                "mode": "hybrid",
                "risk_width": catastrophe,
                "catastrophic_stop": catastrophe,
                "close_stop": close_stop,
                "close_stop_bars": close_bars,
                "family": name,
            }
        )
        result[name] = policy

    add("cat35_close15x1", 0.35, 0.15, 1)
    add("cat40_close15x1", 0.40, 0.15, 1)
    add("cat40_close20x1", 0.40, 0.20, 1)
    add("cat40_close25x1", 0.40, 0.25, 1)
    add("cat50_close20x1", 0.50, 0.20, 1)
    add("cat40_close15x2", 0.40, 0.15, 2)
    add("cat50_close20x2", 0.50, 0.20, 2)
    return result


def simulate(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    policy_name: str,
    slippage_bps: float,
) -> list[dict[str, Any]]:
    policy = policy_catalog()[policy_name]
    outcomes: list[dict[str, Any]] = []
    for _, row in rows[rows["sequence_selected"]].iterrows():
        symbol = str(row["symbol"])
        symbol_bars = bars.get(symbol)
        if symbol_bars is None:
            continue
        if policy["mode"] == "hard":
            outcome = ENTRY.simulate_stateful_trade(
                symbol_bars,
                entry_time=pd.Timestamp(row["decision_time"]),
                policy=policy,
                slippage_bps=slippage_bps,
                funding=funding.get(symbol),
                include_marks=True,
            )
        else:
            outcome = HYBRID.simulate_hybrid_stop_trade(
                symbol_bars,
                entry_time=pd.Timestamp(row["decision_time"]),
                policy=policy,
                slippage_bps=slippage_bps,
                funding=funding.get(symbol),
                include_marks=True,
            )
        if outcome is None:
            continue
        outcome.update(PRIMARY._metadata(row, score=float(row["price_model_score"]), mode="delayed_selected"))
        outcome.update(
            {
                "policy": policy_name,
                "stop_mode": policy["mode"],
                "risk_width": float(policy["risk_width"]),
                "catastrophic_stop": float(policy.get("catastrophic_stop", policy["hard_stop"])),
                "close_stop": None if policy["mode"] == "hard" else float(policy["close_stop"]),
                "close_stop_bars": None if policy["mode"] == "hard" else int(policy["close_stop_bars"]),
                "slippage_bps_each_side": float(slippage_bps),
            }
        )
        outcomes.append(outcome)
    return outcomes


def summarize(outcomes: list[dict[str, Any]], risk_width: float) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    accepted, equity, summary = EXIT.summarize_portfolio(outcomes, risk_stop_pct=risk_width)
    trades = pd.DataFrame([EXIT.clean_trade(row) | {
        "stop_mode": row["stop_mode"],
        "risk_width": row["risk_width"],
        "catastrophic_stop": row["catastrophic_stop"],
        "close_stop": row["close_stop"],
        "close_stop_bars": row["close_stop_bars"],
    } for row in accepted])
    return trades, equity, summary


def calibration_table(
    rows: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for name, policy in policy_catalog().items():
        outcomes = simulate(rows, bars, funding, policy_name=name, slippage_bps=5.0)
        outcomes = [row for row in outcomes if pd.Timestamp(row["exit_time"]) < cutoff]
        trades, _, summary = summarize(outcomes, float(policy["risk_width"]))
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
                "policy": name,
                "stop_mode": policy["mode"],
                "risk_width": float(policy["risk_width"]),
                "catastrophic_stop": float(policy.get("catastrophic_stop", policy["hard_stop"])),
                "close_stop": None if policy["mode"] == "hard" else float(policy["close_stop"]),
                "close_stop_bars": None if policy["mode"] == "hard" else int(policy["close_stop_bars"]),
                "trades": int(len(values)),
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


def path_diagnostics(rows: pd.DataFrame, bars: Mapping[str, pd.DataFrame], cutoff: pd.Timestamp) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for _, row in rows[rows["sequence_selected"]].iterrows():
        symbol_bars = bars.get(str(row["symbol"]))
        if symbol_bars is None:
            continue
        period = "calibration" if pd.Timestamp(row["decision_time"]) + pd.Timedelta(days=14) < cutoff else (
            "confirmation" if int(row["fold"]) in [3, 4, 5, 6] else "unused"
        )
        for target in (0.50, 1.00, 2.00):
            diagnostic = HYBRID.excursion_before_target(
                symbol_bars,
                entry_time=pd.Timestamp(row["decision_time"]),
                target_return=target,
            )
            if diagnostic is None:
                continue
            records.append(
                {
                    "symbol": str(row["symbol"]),
                    "date": pd.Timestamp(row["date"]),
                    "fold": int(row["fold"]),
                    "period": period,
                    "actual_200pct": bool(row["late_target200"]),
                    **diagnostic,
                }
            )
    return pd.DataFrame(records)


def diagnostic_summary(diagnostics: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for (period, target), group in diagnostics[
        (diagnostics["period"] != "unused") & diagnostics["target_hit"].astype(bool)
    ].groupby(["period", "target_return"]):
        for population, subset in [("all_hitters", group), ("eventual_200pct", group[group["actual_200pct"]])]:
            if subset.empty:
                continue
            records.append(
                {
                    "period": period,
                    "target_return": float(target),
                    "population": population,
                    "signals": int(len(subset)),
                    "median_hours_to_target": float(subset["hours_to_target"].median()),
                    "median_prior_intrabar_mae": float(subset["prior_bar_intrabar_mae"].median()),
                    "median_prior_close_mae": float(subset["prior_bar_close_mae"].median()),
                    "prior_intrabar_below_minus20_share": float((subset["prior_bar_intrabar_mae"] <= -0.20).mean()),
                    "prior_intrabar_below_minus25_share": float((subset["prior_bar_intrabar_mae"] <= -0.25).mean()),
                    "prior_close_below_minus15_share": float((subset["prior_bar_close_mae"] <= -0.15).mean()),
                    "prior_close_below_minus20_share": float((subset["prior_bar_close_mae"] <= -0.20).mean()),
                    "target_bar_stop_ambiguity_25_share": float((subset["inclusive_intrabar_mae"] <= -0.25).mean()),
                }
            )
    return pd.DataFrame(records).sort_values(["period", "target_return", "population"]).reset_index(drop=True)


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

    diagnostics = path_diagnostics(rows, bars, cutoff)
    diagnostics.to_csv(REPORT / "hybrid_stop_path_diagnostics.csv.gz", index=False, compression="gzip")
    diagnostics_summary = diagnostic_summary(diagnostics)
    diagnostics_summary.to_csv(REPORT / "hybrid_stop_path_summary.csv", index=False)

    calibration_rows = rows[rows["decision_time"] + pd.Timedelta(days=14) < cutoff].copy()
    calibration = calibration_table(calibration_rows, bars, funding, cutoff=cutoff)
    calibration.to_csv(REPORT / "hybrid_stop_calibration.csv", index=False)
    selected_name = str(calibration.iloc[0]["policy"])
    calibration_eligible = bool(calibration.iloc[0]["selection_eligible"])
    selected_policy = policy_catalog()[selected_name]

    confirmation = rows[rows["fold"].isin([3, 4, 5, 6])].copy()
    summary_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate(confirmation, bars, funding, policy_name=selected_name, slippage_bps=slippage)
        trades, equity, summary = summarize(outcomes, float(selected_policy["risk_width"]))
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
    summaries.to_csv(REPORT / "hybrid_stop_confirmation_summary.csv", index=False)
    all_trades.to_csv(REPORT / "hybrid_stop_confirmation_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(REPORT / "hybrid_stop_confirmation_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(REPORT / "hybrid_stop_confirmation_folds.csv", index=False)

    # Confirmation catalog is diagnostic only and never changes the calibrated choice.
    catalog_records: list[dict[str, Any]] = []
    baseline_trades = pd.DataFrame()
    baseline_summary: dict[str, Any] = {}
    for name, policy in policy_catalog().items():
        outcomes = simulate(confirmation, bars, funding, policy_name=name, slippage_bps=5.0)
        trades, _, summary = summarize(outcomes, float(policy["risk_width"]))
        catalog_records.append(
            {
                "policy": name,
                "stop_mode": policy["mode"],
                "trades": summary.get("trades"),
                "expectancy": summary.get("expectancy"),
                "profit_factor": summary.get("profit_factor"),
                "max_drawdown": summary.get("max_drawdown"),
                "hard_or_catastrophic_exit_rate": float(trades["exit_reason"].isin(["hard_stop", "catastrophic_stop"]).mean()) if len(trades) else np.nan,
                "close_confirmed_exit_rate": float((trades["exit_reason"] == "close_confirmed_stop").mean()) if len(trades) else np.nan,
            }
        )
        if name == "baseline_hard25":
            baseline_trades = trades
            baseline_summary = summary
    catalog = pd.DataFrame(catalog_records)
    catalog.to_csv(REPORT / "hybrid_stop_confirmation_catalog_diagnostic.csv", index=False)

    week = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="week", seed=20260812) if not default_trades.empty else {}
    symbol_boot = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="symbol", seed=20260813) if not default_trades.empty else {}
    leave_largest, concentration = PRIMARY.STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summaries) == 3 and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all() and (summaries["max_drawdown"] >= -0.30).all()
    )
    incremental = bool(
        selected_name != "baseline_hard25" and default_summary
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
    confirm_winner_diag = diagnostics_summary[
        (diagnostics_summary["period"] == "confirmation")
        & (diagnostics_summary["population"] == "eventual_200pct")
    ].to_dict(orient="records")
    output = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "tertiary_hybrid_stop_study_after_entry_path_review",
        "launch_rule": selected_rule,
        "candidate_policies": list(policy_catalog()),
        "selected_policy": selected_name,
        "calibration_eligible": calibration_eligible,
        "confirmation_winner_path_diagnostic": confirm_winner_diag,
        "baseline_strategy_summary": baseline_summary,
        "strategy_default_summary": default_summary,
        "strategy_stress_costs": summaries[["slippage_bps_each_side", "trades", "expectancy", "profit_factor", "max_drawdown"]].to_dict(orient="records") if not summaries.empty else [],
        "strategy_fold_metrics": trade_folds.to_dict(orient="records"),
        "fold_majority_profit_factor_gt_1": fold_majority,
        "week_bootstrap": week,
        "symbol_bootstrap": symbol_boot,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "incremental_vs_hard25": incremental,
        "confirmation_catalog_selection_allowed": False,
        "historical_trade_gate_pass": trade_gate,
        "classification": "paper_challenger_for_forward_oos" if trade_gate else "watchlist_only",
        "automatic_trading_allowed": False,
    }
    (REPORT / "hybrid_stop_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
