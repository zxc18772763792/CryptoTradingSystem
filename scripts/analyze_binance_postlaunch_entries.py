"""Calibrate and confirm post-launch entry locations for the 24h rule."""

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


POST = _load(
    "binance_postlaunch_validation_standalone",
    ROOT / "core" / "research" / "binance_postlaunch_validation.py",
)
STATE = _load(
    "binance_launch_exit_state_for_postlaunch",
    ROOT / "scripts" / "analyze_binance_launch_exit_state.py",
)
PRIMARY = STATE.PRIMARY
ROBUST = STATE.ROBUST
ENTRY = STATE.ENTRY
VALIDATION = STATE.VALIDATION
EXIT = STATE.EXIT


def build_entries(
    signals: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    keep = [
        "symbol", "date", "fold", "signal_key", "price_model_score", "price_model_pctile",
        "baseline_entry_time", "baseline_entry_open", "decision_time", "decision_open",
        "original_target200", "late_target200", "early_mfe", "early_close_return",
    ]
    for _, signal in signals.iterrows():
        symbol_bars = bars.get(str(signal["symbol"]))
        if symbol_bars is None:
            continue
        base = {column: signal.get(column) for column in keep}
        for name in POST.POSTLAUNCH_ENTRY_RULES:
            located = POST.locate_postlaunch_entry(signal, symbol_bars, rule_name=name)
            if located is None:
                continue
            row = dict(base)
            row.update(located)
            row["entry_id"] = f"{signal['symbol']}|{pd.Timestamp(signal['date']).date()}|{name}"
            records.append(row)
    result = pd.DataFrame(records)
    if not result.empty:
        for column in ["date", "baseline_entry_time", "decision_time", "trigger_time", "entry_time"]:
            result[column] = pd.to_datetime(result[column], utc=True)
    return result


def policy() -> dict[str, Any]:
    selected = dict(ENTRY.build_exit_policy_catalog()["frozen_half100_trail25"])
    selected.update(
        {
            "hard_stop": 0.25,
            "breakeven_activation": 0.30,
            "family": "launch_breakeven30",
        }
    )
    return selected


def simulate_entries(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    slippage_bps: float,
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    selected_policy = policy()
    for _, row in entries.iterrows():
        symbol = str(row["symbol"])
        symbol_bars = bars.get(symbol)
        if symbol_bars is None:
            continue
        outcome = ENTRY.simulate_stateful_trade(
            symbol_bars,
            entry_time=pd.Timestamp(row["entry_time"]),
            policy=selected_policy,
            slippage_bps=slippage_bps,
            funding=funding.get(symbol),
            include_marks=True,
        )
        if outcome is None:
            continue
        outcome.update(
            {
                "symbol": symbol,
                "signal_date": pd.Timestamp(row["date"]),
                "fold": int(row["fold"]),
                "score": float(row["price_model_score"]),
                "score_pctile": float(row["price_model_pctile"]),
                "signal_key": int(row["signal_key"]),
                "entry_rule": str(row["postlaunch_entry_rule"]),
                "entry_delay_hours": float(row["entry_delay_hours_after_launch"]),
                "entry_chase_vs_signal_close": float(row["entry_return_vs_decision_open"]),
                "target200_14d_from_entry": bool(row["target200_14d_from_entry"]),
                "future_max_return_14d_from_entry": float(row["future_max_return_14d_from_entry"]),
                "policy": "stop25_breakeven30_half100_trail25",
                "slippage_bps_each_side": float(slippage_bps),
            }
        )
        outcomes.append(outcome)
    return outcomes


def summarize(outcomes: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    accepted, equity, summary = EXIT.summarize_portfolio(outcomes, risk_stop_pct=0.25)
    trades = pd.DataFrame([EXIT.clean_trade(row) for row in accepted])
    return trades, equity, summary


def calibration_table(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    *,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    matured = entries[entries["entry_time"] + pd.Timedelta(days=14) < cutoff].copy()
    baseline = matured[matured["postlaunch_entry_rule"] == "launch_open"]
    baseline_positive = int(baseline["target200_14d_from_entry"].sum())
    baseline_precision = float(baseline["target200_14d_from_entry"].mean()) if len(baseline) else 0.0
    records: list[dict[str, Any]] = []
    for name, metadata in POST.POSTLAUNCH_ENTRY_RULES.items():
        rule_entries = matured[matured["postlaunch_entry_rule"] == name]
        positives = int(rule_entries["target200_14d_from_entry"].sum())
        precision = float(rule_entries["target200_14d_from_entry"].mean()) if len(rule_entries) else 0.0
        retention = float(positives / baseline_positive) if baseline_positive else 0.0
        outcomes = simulate_entries(rule_entries, bars, funding, slippage_bps=5.0)
        outcomes = [row for row in outcomes if pd.Timestamp(row["exit_time"]) < cutoff]
        trades, _, _ = summarize(outcomes)
        values = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
        fold_expectancy = trades.groupby("fold")["net_return"].mean() if not trades.empty else pd.Series(dtype=float)
        leave_largest, concentration = PRIMARY.STUDY.concentration_stats(trades)
        pf = PRIMARY.profit_factor(values)
        eligible = bool(
            name != "launch_open" and len(rule_entries) >= 10 and positives >= 1
            and rule_entries["fold"].nunique() >= 2 and precision >= baseline_precision
            and retention >= 0.40 and len(values) >= 10 and fold_expectancy.size >= 2
            and values.mean() > 0 and pf > 1.0
            and leave_largest is not None and leave_largest > 0
            and concentration is not None and concentration <= 0.50
        )
        score = (
            ENTRY.wilson_lower_bound(positives, len(rule_entries))
            + 0.10 * retention
            + 0.05 * float(np.clip(values.mean() if len(values) else -0.25, -0.25, 0.75))
            + 0.02 * min(pf, 4.0) / 4.0
        )
        records.append(
            {
                "entry_rule": name,
                "family": metadata["family"],
                "description": metadata["description"],
                "baseline_matured_signals": int(len(baseline)),
                "confirmed_entries": int(len(rule_entries)),
                "confirmation_rate": float(len(rule_entries) / len(baseline)) if len(baseline) else 0.0,
                "actual_200pct_entries": positives,
                "actual_200pct_precision": precision,
                "positive_retention": retention,
                "baseline_actual_precision": baseline_precision,
                "median_delay_hours": float(rule_entries["entry_delay_hours_after_launch"].median()) if len(rule_entries) else np.nan,
                "median_entry_return_vs_launch": float(rule_entries["entry_return_vs_decision_open"].median()) if len(rule_entries) else np.nan,
                "trades": int(len(values)),
                "expectancy": float(values.mean()) if len(values) else np.nan,
                "profit_factor": float(pf),
                "folds": int(fold_expectancy.size),
                "leave_largest_expectancy": leave_largest,
                "largest_positive_pnl_share": concentration,
                "selection_score": float(score),
                "selection_eligible": eligible,
            }
        )
    return pd.DataFrame(records).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    ).reset_index(drop=True)


def recognition(entries: pd.DataFrame, baseline: pd.DataFrame) -> dict[str, Any]:
    positives = int(entries["target200_14d_from_entry"].sum())
    base_positive = int(baseline["target200_14d_from_entry"].sum())
    return {
        "entries": int(len(entries)),
        "actual_200pct_entries": positives,
        "actual_200pct_precision": float(entries["target200_14d_from_entry"].mean()) if len(entries) else 0.0,
        "positive_retention_vs_launch_open": float(positives / base_positive) if base_positive else 0.0,
        "median_delay_hours": float(entries["entry_delay_hours_after_launch"].median()) if len(entries) else None,
        "median_entry_return_vs_launch": float(entries["entry_return_vs_decision_open"].median()) if len(entries) else None,
    }


def regime_diagnostics(entries: pd.DataFrame, *, cutoff: pd.Timestamp) -> pd.DataFrame:
    """Describe regime direction without using confirmation rows for selection."""

    baseline = entries[entries["postlaunch_entry_rule"] == "launch_open"].copy()
    baseline["period"] = np.where(
        baseline["entry_time"] + pd.Timedelta(days=14) < cutoff,
        "calibration",
        np.where(baseline["fold"].isin([3, 4, 5, 6]), "confirmation", "unused"),
    )
    records: list[dict[str, Any]] = []
    for dimension in ["btc_trend_regime", "btc_vol_regime", "breadth_regime"]:
        for (period, value), group in baseline[baseline["period"] != "unused"].groupby(["period", dimension]):
            records.append(
                {
                    "period": period,
                    "dimension": dimension,
                    "regime": value,
                    "entries": int(len(group)),
                    "actual_200pct_entries": int(group["target200_14d_from_entry"].sum()),
                    "actual_200pct_precision": float(group["target200_14d_from_entry"].mean()),
                }
            )
    return pd.DataFrame(records).sort_values(["dimension", "period", "regime"]).reset_index(drop=True)


def main() -> None:
    simple = json.loads((REPORT / "simple_launch_strategy_decision.json").read_text(encoding="utf-8"))
    selected_launch_rule = str(simple["selected_rule"])
    checkpoints = pd.read_csv(REPORT / "sequential_checkpoint_candidates.csv.gz", compression="gzip")
    for column in ["date", "baseline_entry_time", "decision_time"]:
        checkpoints[column] = pd.to_datetime(checkpoints[column], utc=True)
    signals = checkpoints[checkpoints["checkpoint_hours"] == 24].copy()
    signals = signals[ROBUST.RULES[selected_launch_rule][1](signals).fillna(False)].copy()
    panel = pd.read_csv(BASELINE / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = VALIDATION.load_funding_history(AMBUSH_ROOT)
    folds = pd.read_csv(REPORT / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])

    entries = build_entries(signals, bars)
    context = pd.read_csv(
        REPORT / "historical_walk_forward_scored.csv.gz",
        compression="gzip",
        usecols=[
            "symbol", "date", "fold", "btc_trend_regime", "btc_vol_regime",
            "breadth_regime", "market_state",
        ],
    )
    context["date"] = pd.to_datetime(context["date"], utc=True)
    entries = entries.merge(context, on=["symbol", "date", "fold"], how="left", validate="many_to_one")
    entries.to_csv(REPORT / "postlaunch_entry_candidates.csv.gz", index=False, compression="gzip")
    regimes = regime_diagnostics(entries, cutoff=cutoff)
    regimes.to_csv(REPORT / "postlaunch_regime_diagnostics.csv", index=False)
    calibration = calibration_table(entries, bars, funding, cutoff=cutoff)
    calibration.to_csv(REPORT / "postlaunch_entry_calibration.csv", index=False)
    selected_rule = str(calibration.iloc[0]["entry_rule"])
    calibration_eligible = bool(calibration.iloc[0]["selection_eligible"])

    confirmation = entries[entries["fold"].isin([3, 4, 5, 6])].copy()
    selected_entries = confirmation[confirmation["postlaunch_entry_rule"] == selected_rule].copy()
    baseline_entries = confirmation[confirmation["postlaunch_entry_rule"] == "launch_open"].copy()
    recognition_selected = recognition(selected_entries, baseline_entries)
    recognition_baseline = recognition(baseline_entries, baseline_entries)

    summary_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = simulate_entries(selected_entries, bars, funding, slippage_bps=slippage)
        trades, equity, summary = summarize(outcomes)
        if not summary:
            continue
        summary.update({"entry_rule": selected_rule, "slippage_bps_each_side": slippage})
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
    summaries.to_csv(REPORT / "postlaunch_entry_confirmation_summary.csv", index=False)
    all_trades.to_csv(REPORT / "postlaunch_entry_confirmation_trades.csv.gz", index=False, compression="gzip")
    all_equity.to_csv(REPORT / "postlaunch_entry_confirmation_equity.csv.gz", index=False, compression="gzip")
    trade_folds.to_csv(REPORT / "postlaunch_entry_confirmation_folds.csv", index=False)

    baseline_outcomes = simulate_entries(baseline_entries, bars, funding, slippage_bps=5.0)
    baseline_trades, _, baseline_summary = summarize(baseline_outcomes)
    comparison = pd.DataFrame(
        [
            {"entry_rule": "launch_open", **baseline_summary},
            {"entry_rule": selected_rule, **default_summary},
        ]
    )
    comparison.to_csv(REPORT / "postlaunch_entry_confirmation_comparison.csv", index=False)
    week = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="week", seed=20260810) if not default_trades.empty else {}
    symbol = EXIT.bootstrap_returns(default_trades, samples=5000, cluster="symbol", seed=20260811) if not default_trades.empty else {}
    leave_largest, concentration = PRIMARY.STUDY.concentration_stats(default_trades)
    fold_majority = float((trade_folds["profit_factor"] > 1).mean()) if not trade_folds.empty else 0.0
    stress_ok = bool(
        len(summaries) == 3 and (summaries["expectancy"] > 0).all()
        and (summaries["profit_factor"] > 1).all() and (summaries["max_drawdown"] >= -0.30).all()
    )
    trade_gate = bool(
        calibration_eligible and default_summary and recognition_selected["actual_200pct_precision"] >= recognition_baseline["actual_200pct_precision"]
        and recognition_selected["positive_retention_vs_launch_open"] >= 0.40
        and default_summary.get("expectancy", -1) > 0 and default_summary.get("profit_factor", 0) > 1
        and fold_majority > 0.50 and stress_ok
        and leave_largest is not None and leave_largest > 0
        and concentration is not None and concentration <= 0.25
        and week.get("expectancy_lower_95pct", -1) > 0
        and symbol.get("expectancy_lower_95pct", -1) > 0
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "secondary_postlaunch_entry_study",
        "launch_rule": selected_launch_rule,
        "candidate_entry_rules": list(POST.POSTLAUNCH_ENTRY_RULES),
        "selected_entry_rule": selected_rule,
        "calibration_eligible": calibration_eligible,
        "confirmation_recognition_baseline": recognition_baseline,
        "confirmation_recognition_selected": recognition_selected,
        "regime_diagnostic": {
            "selection_allowed": False,
            "reason": "regime directions are reported as a falsification diagnostic and were not used to select the entry rule",
            "rows": int(len(regimes)),
        },
        "baseline_strategy_summary": baseline_summary,
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
    (REPORT / "postlaunch_entry_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
