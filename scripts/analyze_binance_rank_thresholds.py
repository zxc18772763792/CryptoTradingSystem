"""Time-isolated threshold study for the frozen Binance price ranker."""

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
THRESHOLDS = (0.95, 0.97, 0.98, 0.985, 0.99, 0.995)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


STUDY = _load(
    "binance_entry_timing_for_rank_thresholds",
    ROOT / "scripts" / "analyze_binance_entry_exit_timing.py",
)
ENTRY = STUDY.ENTRY
VALIDATION = STUDY.VALIDATION
EXIT = STUDY.EXIT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def build_signals(scored: pd.DataFrame, threshold: float) -> pd.DataFrame:
    rows = scored.sort_values(["symbol", "date"]).copy()
    prior = rows.groupby("symbol")["price_model_pctile"].shift(1).fillna(0.0)
    rows["crossing"] = (rows["price_model_pctile"] >= threshold) & (prior < threshold)
    signals = rows[rows["crossing"]].copy()
    signals = (
        signals.sort_values(["date", "price_model_score"], ascending=[True, False])
        .groupby("date", as_index=False, group_keys=False)
        .head(3)
        .reset_index(drop=True)
    )
    signals["entry_time"] = signals["date"] + pd.Timedelta(days=1, hours=4)
    signals["rank_threshold"] = float(threshold)
    return signals


def locate_entries(
    signals: pd.DataFrame,
    bars_by_symbol: Mapping[str, pd.DataFrame],
    threshold: float,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    keep = [
        "symbol", "date", "fold", "close", "price_model_score", "price_model_pctile",
        "breadth_regime", "btc_trend_regime", "btc_vol_regime", "market_state",
    ]
    for signal_key, (_, signal) in enumerate(signals.iterrows()):
        bars = bars_by_symbol.get(str(signal["symbol"]))
        if bars is None:
            continue
        located = ENTRY.locate_entry(signal, bars, rule_name="next_open")
        if located is None:
            continue
        row = {column: signal.get(column) for column in keep}
        row.update(located)
        row["rank_threshold"] = float(threshold)
        row["signal_key"] = int(signal_key)
        records.append(row)
    return pd.DataFrame(records)


def summarize_threshold(
    entries: pd.DataFrame,
    trades: pd.DataFrame,
    *,
    cutoff: pd.Timestamp | None,
    baseline_successes: int,
) -> dict[str, Any]:
    labels = entries.copy()
    if cutoff is not None:
        labels = labels[labels["entry_time"] + pd.Timedelta(days=14) < cutoff]
        if not trades.empty:
            trades = trades[pd.to_datetime(trades["exit_time"], utc=True) < cutoff]
    positives = int(labels["target200_14d_from_entry"].sum())
    returns = pd.to_numeric(trades.get("net_return", pd.Series(dtype=float)), errors="coerce").dropna()
    pf = STUDY.profit_factor(returns)
    leave_largest, concentration = STUDY.concentration_stats(trades)
    precision = float(positives / len(labels)) if len(labels) else 0.0
    retention = float(positives / baseline_successes) if baseline_successes else 0.0
    return {
        "signals": int(len(labels)),
        "actual_200pct_entries": positives,
        "actual_200pct_precision": precision,
        "actual_positive_retention_vs_98pct": retention,
        "wilson_precision_lower": ENTRY.wilson_lower_bound(positives, len(labels)),
        "trades": int(len(returns)),
        "expectancy": float(returns.mean()) if len(returns) else np.nan,
        "profit_factor": float(pf),
        "leave_largest_winner_out_expectancy": leave_largest,
        "largest_positive_pnl_share": concentration,
    }


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    scored = pd.read_csv(report / "historical_walk_forward_scored.csv.gz", compression="gzip")
    scored["date"] = pd.to_datetime(scored["date"], utc=True)
    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars_by_symbol = {
        symbol: ENTRY.add_past_only_4h_features(group)
        for symbol, group in panel.groupby("symbol", sort=False)
    }
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    cutoff = pd.Timestamp(folds.loc[folds["fold"] == 3, "test_start"].iloc[0])
    funding = VALIDATION.load_funding_history(ROOT / "data" / "research" / "ambush_modes")
    frozen_name = "frozen_half100_trail25"
    frozen = ENTRY.build_exit_policy_catalog()[frozen_name]

    entries_by_threshold: dict[float, pd.DataFrame] = {}
    trades_by_threshold: dict[float, pd.DataFrame] = {}
    all_entries: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    for threshold in THRESHOLDS:
        signals = build_signals(scored, threshold)
        entries = locate_entries(signals, bars_by_symbol, threshold)
        for column in ["date", "entry_time", "baseline_entry_time", "trigger_time"]:
            if column in entries:
                entries[column] = pd.to_datetime(entries[column], utc=True)
        outcomes = STUDY.simulate_entries(
            entries,
            bars_by_symbol,
            funding,
            policy_name=frozen_name,
            policy=frozen,
            slippage_bps=5.0,
            include_marks=False,
        )
        trades, _, _ = STUDY.accepted_frame(outcomes)
        entries_by_threshold[threshold] = entries
        trades_by_threshold[threshold] = trades
        all_entries.extend(entries.to_dict(orient="records"))
        if not trades.empty:
            trades["rank_threshold"] = threshold
            all_trades.extend(trades.to_dict(orient="records"))

    baseline_cal = entries_by_threshold[0.98]
    baseline_cal = baseline_cal[baseline_cal["entry_time"] + pd.Timedelta(days=14) < cutoff]
    baseline_successes = int(baseline_cal["target200_14d_from_entry"].sum())
    calibration_rows: list[dict[str, Any]] = []
    for threshold in THRESHOLDS:
        row = {"rank_threshold": threshold}
        row.update(
            summarize_threshold(
                entries_by_threshold[threshold], trades_by_threshold[threshold],
                cutoff=cutoff, baseline_successes=baseline_successes,
            )
        )
        row["selection_score"] = (
            row["wilson_precision_lower"]
            + 0.10 * row["actual_positive_retention_vs_98pct"]
            + 0.03 * float(np.clip(row["expectancy"], -0.20, 0.50))
        )
        row["selection_eligible"] = bool(
            row["signals"] >= 80 and row["actual_200pct_entries"] >= 5
            and row["actual_positive_retention_vs_98pct"] >= 0.50
            and row["trades"] >= 40 and row["expectancy"] > 0 and row["profit_factor"] > 1
            and row["leave_largest_winner_out_expectancy"] is not None
            and row["leave_largest_winner_out_expectancy"] > 0
            and row["largest_positive_pnl_share"] is not None
            and row["largest_positive_pnl_share"] <= 0.25
        )
        calibration_rows.append(row)
    calibration = pd.DataFrame(calibration_rows).sort_values(
        ["selection_eligible", "selection_score"], ascending=[False, False]
    )
    eligible = calibration[calibration["selection_eligible"]]
    selected_threshold = float(eligible.iloc[0]["rank_threshold"]) if not eligible.empty else 0.98

    baseline_confirmation = entries_by_threshold[0.98]
    baseline_confirmation = baseline_confirmation[baseline_confirmation["fold"] >= 3]
    baseline_confirmation_successes = int(baseline_confirmation["target200_14d_from_entry"].sum())
    confirmation_rows: list[dict[str, Any]] = []
    confirmation_fold_rows: list[dict[str, Any]] = []
    confirmation_trade_records: list[dict[str, Any]] = []
    for threshold in THRESHOLDS:
        entry_frame = entries_by_threshold[threshold]
        entry_frame = entry_frame[entry_frame["fold"] >= 3]
        confirmation_outcomes = STUDY.simulate_entries(
            entry_frame,
            bars_by_symbol,
            funding,
            policy_name=frozen_name,
            policy=frozen,
            slippage_bps=5.0,
            include_marks=False,
        )
        trade_frame, _, _ = STUDY.accepted_frame(confirmation_outcomes)
        if not trade_frame.empty:
            trade_frame["rank_threshold"] = threshold
            confirmation_trade_records.extend(trade_frame.to_dict(orient="records"))
        row = {"rank_threshold": threshold, "selected_on_calibration": threshold == selected_threshold}
        row.update(
            summarize_threshold(
                entry_frame, trade_frame, cutoff=None,
                baseline_successes=baseline_confirmation_successes,
            )
        )
        confirmation_rows.append(row)
        for fold, group in entry_frame.groupby("fold"):
            positives = int(group["target200_14d_from_entry"].sum())
            confirmation_fold_rows.append(
                {
                    "rank_threshold": threshold,
                    "fold": int(fold),
                    "signals": int(len(group)),
                    "actual_200pct_entries": positives,
                    "actual_200pct_precision": float(positives / len(group)) if len(group) else 0.0,
                }
            )
    confirmation = pd.DataFrame(confirmation_rows)
    confirmation_folds = pd.DataFrame(confirmation_fold_rows)
    selected_confirmation = confirmation[confirmation["rank_threshold"] == selected_threshold].iloc[0]
    baseline_confirmation_row = confirmation[confirmation["rank_threshold"] == 0.98].iloc[0]
    selected_folds = confirmation_folds[confirmation_folds["rank_threshold"] == selected_threshold].set_index("fold")
    baseline_folds = confirmation_folds[confirmation_folds["rank_threshold"] == 0.98].set_index("fold")
    common_folds = selected_folds.index.intersection(baseline_folds.index)
    fold_precision_improvement = float(
        (selected_folds.loc[common_folds, "actual_200pct_precision"]
         > baseline_folds.loc[common_folds, "actual_200pct_precision"]).mean()
    ) if len(common_folds) else 0.0
    improved = bool(
        selected_confirmation["actual_200pct_precision"] > baseline_confirmation_row["actual_200pct_precision"]
        and selected_confirmation["actual_positive_retention_vs_98pct"] >= 0.50
        and selected_confirmation["expectancy"] > 0
        and selected_confirmation["profit_factor"] > 1
        and selected_confirmation["leave_largest_winner_out_expectancy"] > 0
        and fold_precision_improvement > 0.5
    )
    calibration.to_csv(report / "rank_threshold_calibration.csv", index=False)
    confirmation.to_csv(report / "rank_threshold_confirmation.csv", index=False)
    confirmation_folds.to_csv(report / "rank_threshold_confirmation_folds.csv", index=False)
    pd.DataFrame(confirmation_trade_records).to_csv(
        report / "rank_threshold_confirmation_trades.csv.gz", index=False, compression="gzip"
    )
    pd.DataFrame(all_entries).to_csv(report / "rank_threshold_entries.csv.gz", index=False, compression="gzip")
    pd.DataFrame(all_trades).to_csv(report / "rank_threshold_trades.csv.gz", index=False, compression="gzip")
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "thresholds_tested": list(THRESHOLDS),
        "selection_information_cutoff_utc": cutoff,
        "selected_threshold": selected_threshold,
        "selection_eligible": bool(not eligible.empty),
        "confirmation_selected": selected_confirmation.to_dict(),
        "confirmation_baseline_98pct": baseline_confirmation_row.to_dict(),
        "confirmation_fold_precision_improvement_rate": fold_precision_improvement,
        "confirmation_improved": improved,
        "classification": "directional_candidate" if improved else "no_threshold_upgrade",
        "note": "threshold selection changes neither the frozen factors nor the exit policy",
    }
    (report / "rank_threshold_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
