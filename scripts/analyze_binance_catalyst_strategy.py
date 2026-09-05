"""Test the exploratory price-setup plus verified-news catalyst entry gate.

The exit is not re-selected: it is the previously frozen -20% stop, +100%
half take-profit, 25% trailing remainder, 30-day maximum hold.  This script is
research-only and has no trading dependencies.
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
FROZEN_POLICY = "stop20_half100_trail25_time30"

EXIT_PATH = ROOT / "scripts" / "analyze_binance_exit_strategies.py"
SPEC = importlib.util.spec_from_file_location("binance_catalyst_exit_helpers", EXIT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load {EXIT_PATH}")
EXIT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = EXIT
SPEC.loader.exec_module(EXIT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    return parser.parse_args()


def build_signals(scored: pd.DataFrame) -> pd.DataFrame:
    rows = scored.sort_values(["symbol", "date"]).copy()
    rows["entry_gate"] = (
        (rows["price_same_score_pctile"] >= 0.90)
        & (rows["news_catalyst_7d"] > 0)
    )
    prior = rows.groupby("symbol")["entry_gate"].shift(1).fillna(False).astype(bool)
    signals = rows[rows["entry_gate"] & ~prior].copy()
    signals = (
        signals.sort_values(["date", "price_same_score"], ascending=[True, False])
        .groupby("date", as_index=False, group_keys=False)
        .head(3)
        .reset_index(drop=True)
    )
    signals["entry_time"] = signals["date"] + pd.Timedelta(days=1, hours=4)
    signals["price_model_score"] = signals["price_same_score"]
    signals["price_model_pctile"] = signals["price_same_score_pctile"]
    signals["signal_key"] = np.arange(len(signals), dtype=int)
    return signals


def summarize_fold(trades: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for fold, group in trades.groupby("fold"):
        returns = pd.to_numeric(group["net_return"], errors="coerce").dropna()
        gains = float(returns[returns > 0].sum())
        losses = float(-returns[returns < 0].sum())
        records.append(
            {
                "fold": int(fold),
                "trades": int(len(returns)),
                "expectancy": float(returns.mean()) if len(returns) else np.nan,
                "median_return": float(returns.median()) if len(returns) else np.nan,
                "win_rate": float((returns > 0).mean()) if len(returns) else np.nan,
                "profit_factor": gains / losses if losses > 0 else np.inf,
                "hard_stop_rate": float((group["exit_reason"] == "hard_stop").mean()),
                "mean_mfe": float(group["mfe"].mean()),
                "mean_mae": float(group["mae"].mean()),
            }
        )
    return pd.DataFrame(records)


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    scored = pd.read_csv(report / "multisource_news_walk_forward_scored.csv.gz", compression="gzip")
    scored["date"] = pd.to_datetime(scored["date"], utc=True)
    signals = build_signals(scored)
    signals.to_csv(report / "multisource_catalyst_signals.csv", index=False)

    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars_by_symbol = {
        symbol: group.sort_values("open_time")
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    funding = EXIT.VALIDATION.load_funding_history(AMBUSH_ROOT)
    cache = EXIT.build_signal_path_cache(signals, bars_by_symbol, funding)
    policy = EXIT.VALIDATION.build_exit_policy_catalog()[FROZEN_POLICY]

    summary_rows: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    all_equity: list[dict[str, Any]] = []
    default_trades = pd.DataFrame()
    default_summary: dict[str, Any] = {}
    for slippage in (5.0, 15.0, 30.0):
        outcomes = EXIT.simulate_policy(
            signals,
            cache,
            policy_name=FROZEN_POLICY,
            policy=policy,
            slippage_bps=slippage,
            include_marks=True,
        )
        accepted, equity, summary = EXIT.summarize_portfolio(outcomes)
        if not summary:
            continue
        summary.update(
            {
                "entry_rule": "first price top-10% day with a strict catalyst mention in the prior 7 days",
                "policy": FROZEN_POLICY,
                "slippage_bps_each_side": slippage,
                "signals_before_portfolio_constraints": int(len(signals)),
                "sample_scope": "historical exploratory folds 4-6",
            }
        )
        summary_rows.append(summary)
        clean = pd.DataFrame([EXIT.clean_trade(row) for row in accepted])
        clean["slippage_bps_each_side"] = slippage
        all_trades.extend(clean.to_dict(orient="records"))
        if not equity.empty:
            curve = equity.copy()
            curve["slippage_bps_each_side"] = slippage
            all_equity.extend(curve.to_dict(orient="records"))
        if slippage == 5.0:
            default_trades = clean
            default_summary = summary

    summaries = pd.DataFrame(summary_rows)
    trades = pd.DataFrame(all_trades)
    equity = pd.DataFrame(all_equity)
    folds = summarize_fold(default_trades) if not default_trades.empty else pd.DataFrame()
    summaries.to_csv(report / "multisource_catalyst_strategy_summary.csv", index=False)
    trades.to_csv(report / "multisource_catalyst_strategy_trades.csv.gz", index=False, compression="gzip")
    equity.to_csv(report / "multisource_catalyst_strategy_equity.csv.gz", index=False, compression="gzip")
    folds.to_csv(report / "multisource_catalyst_strategy_folds.csv", index=False)

    week_bootstrap = (
        EXIT.bootstrap_returns(
            default_trades,
            samples=args.bootstrap_samples,
            cluster="week",
            seed=20260721,
        ) if not default_trades.empty else {}
    )
    symbol_bootstrap = (
        EXIT.bootstrap_returns(
            default_trades,
            samples=args.bootstrap_samples,
            cluster="symbol",
            seed=20260722,
        ) if not default_trades.empty else {}
    )
    fold_majority = float((folds["profit_factor"] > 1).mean()) if not folds.empty else 0.0
    leave_largest = None
    concentration = None
    if not default_trades.empty:
        returns = pd.to_numeric(default_trades["net_return"], errors="coerce").dropna()
        if len(returns) > 1:
            leave_largest = float(returns.drop(returns.idxmax()).mean())
        positive_pnl = pd.to_numeric(
            default_trades.loc[default_trades["pnl_usd"] > 0, "pnl_usd"], errors="coerce"
        )
        concentration = float(positive_pnl.max() / positive_pnl.sum()) if positive_pnl.sum() > 0 else 1.0
    week_lower = week_bootstrap.get("expectancy", {}).get("lower_95pct")
    symbol_lower = symbol_bootstrap.get("expectancy", {}).get("lower_95pct")
    trade_gate = bool(
        default_summary
        and default_summary.get("expectancy", -1) > 0
        and default_summary.get("profit_factor", 0) > 1
        and default_summary.get("max_drawdown", -1) >= -0.30
        and fold_majority > 0.5
        and leave_largest is not None and leave_largest > 0
        and week_lower is not None and week_lower > 0
        and symbol_lower is not None and symbol_lower > 0
    )
    decision = {
        "entry_rule_status": "exploratory_hypothesis; discovered in the multisource historical study",
        "entry_rule": "first daily price rank >= 90th percentile with strict catalyst news in prior 7 days",
        "exit_policy": FROZEN_POLICY,
        "costs": "5 bps fee each side plus displayed slippage stress",
        "default_summary": default_summary,
        "fold_metrics": folds.to_dict(orient="records"),
        "week_block_bootstrap": week_bootstrap,
        "symbol_bootstrap": symbol_bootstrap,
        "fold_majority_profit_factor_gt_1": fold_majority,
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
        "paper_trade_gate_pass": trade_gate,
        "classification": "paper_candidate" if trade_gate else "watchlist_only",
        "reason": (
            "all profitability, stability, concentration and bootstrap gates passed"
            if trade_gate else
            "historical enrichment is insufficient without stable cost-adjusted trade outcomes and positive bootstrap lower bounds"
        ),
    }
    (report / "multisource_catalyst_strategy_decision.json").write_text(
        json.dumps(EXIT.VALIDATION.json_ready(decision), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(EXIT.VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

