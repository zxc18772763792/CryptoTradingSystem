"""Independently verify the Binance exit-policy research evidence."""

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
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
VALIDATION_PATH = ROOT / "core" / "research" / "binance_runup_validation.py"
SPEC = importlib.util.spec_from_file_location("binance_exit_independent_validation", VALIDATION_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load {VALIDATION_PATH}")
VALIDATION = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = VALIDATION
SPEC.loader.exec_module(VALIDATION)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def independent_rank(
    trades: pd.DataFrame,
    catalog: dict[str, dict[str, Any]],
    cutoff: pd.Timestamp,
    minimum_trades: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    data = trades.copy()
    data["signal_date"] = pd.to_datetime(data["signal_date"], utc=True)
    data["exit_time"] = pd.to_datetime(data["exit_time"], utc=True)
    data = data[
        (data["slippage_bps_each_side"] == 5.0)
        & (data["signal_date"] < cutoff)
        & (data["exit_time"] < cutoff)
    ]
    for policy, group in data.groupby("policy"):
        returns = pd.to_numeric(group["net_return"], errors="coerce").dropna()
        fold_expectancy = group.groupby("fold")["net_return"].mean().astype(float)
        gains = float(returns[returns > 0].sum())
        losses = float(-returns[returns < 0].sum())
        profit_factor = gains / losses if losses > 0 else np.inf
        leave_largest = float(returns.drop(returns.idxmax()).mean()) if len(returns) > 1 else np.nan
        positive = pd.to_numeric(group.loc[group["pnl_usd"] > 0, "pnl_usd"], errors="coerce")
        concentration = float(positive.max() / positive.sum()) if positive.sum() > 0 else 1.0
        score = float(fold_expectancy.mean() - 0.5 * fold_expectancy.std(ddof=0))
        eligible = bool(
            len(returns) >= minimum_trades
            and fold_expectancy.size >= 2
            and returns.mean() > 0
            and profit_factor > 1.0
            and leave_largest > 0
            and concentration <= 0.25
        )
        rows.append(
            {
                "policy": policy,
                "selection_eligible": eligible,
                "selection_score": score,
                "profit_factor": profit_factor,
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["selection_eligible", "selection_score", "profit_factor"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def recompute_trade_metrics(trades: pd.DataFrame, equity: pd.DataFrame) -> dict[str, Any]:
    returns = pd.to_numeric(trades["net_return"], errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    fold_pf: list[float] = []
    for _, group in trades.groupby("fold"):
        values = pd.to_numeric(group["net_return"], errors="coerce").dropna()
        group_gains = float(values[values > 0].sum())
        group_losses = float(-values[values < 0].sum())
        fold_pf.append(group_gains / group_losses if group_losses > 0 else np.inf)
    positive = pd.to_numeric(trades.loc[trades["pnl_usd"] > 0, "pnl_usd"], errors="coerce")
    return {
        "trades": int(len(returns)),
        "expectancy": float(returns.mean()),
        "win_rate": float((returns > 0).mean()),
        "profit_factor": gains / losses if losses > 0 else np.inf,
        "max_drawdown": float(pd.to_numeric(equity["drawdown"], errors="coerce").min()),
        "fold_majority_profit_factor_gt_1": float(np.mean(np.asarray(fold_pf) > 1.0)),
        "largest_positive_pnl_share": float(positive.max() / positive.sum()),
        "leave_largest_winner_out_expectancy": float(returns.drop(returns.idxmax()).mean()),
    }


def close(left: Any, right: Any, tolerance: float = 1e-10) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=0, atol=tolerance, equal_nan=True))


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    decision = json.loads((report / "exit_strategy_decision.json").read_text(encoding="utf-8"))
    catalog_csv = pd.read_csv(report / "exit_policy_catalog.csv")
    grid_trades = pd.read_csv(report / "exit_policy_grid_trades.csv.gz")
    frozen_summary = pd.read_csv(report / "exit_policy_frozen_confirmation_summary.csv")
    frozen_trades = pd.read_csv(report / "exit_policy_frozen_confirmation_trades.csv.gz")
    frozen_equity = pd.read_csv(report / "exit_policy_frozen_confirmation_equity.csv.gz")
    choices = pd.read_csv(report / "exit_policy_time_forward_choices.csv")
    adaptive_summary = pd.read_csv(report / "exit_policy_time_forward_summary.csv")
    adaptive_trades = pd.read_csv(report / "exit_policy_time_forward_trades.csv.gz")
    adaptive_equity = pd.read_csv(report / "exit_policy_time_forward_equity.csv.gz")
    folds = pd.read_csv(report / "walk_forward_fold_metrics.csv")
    folds["test_start"] = pd.to_datetime(folds["test_start"], utc=True)
    catalog = VALIDATION.build_exit_policy_catalog()
    errors: list[str] = []

    if len(catalog) != 52 or len(catalog_csv) != 52:
        errors.append("exit policy catalog does not contain exactly 52 policies")
    if decision.get("catalog_hash") != VALIDATION.stable_hash(catalog):
        errors.append("catalog hash does not match the frozen policy definitions")
    if decision.get("entry_model_changed") is not False:
        errors.append("exit study claims or implies that the entry model changed")

    cutoff = pd.Timestamp(decision["selection_information_cutoff_utc"])
    rank = independent_rank(grid_trades, catalog, cutoff, minimum_trades=40)
    independently_selected = str(rank.iloc[0]["policy"])
    if independently_selected != decision.get("selected_policy"):
        errors.append(
            f"calibration-selected policy mismatch: {independently_selected} != {decision.get('selected_policy')}"
        )

    selected_policy = str(decision["selected_policy"])
    for cost in (5.0, 15.0, 30.0):
        trades = frozen_trades[frozen_trades["slippage_bps_each_side"] == cost]
        equity = frozen_equity[frozen_equity["slippage_bps_each_side"] == cost]
        summary = frozen_summary[frozen_summary["slippage_bps_each_side"] == cost]
        if trades.empty or equity.empty or summary.empty:
            errors.append(f"missing frozen confirmation evidence for {cost:g} bps")
            continue
        if set(trades["policy"]) != {selected_policy}:
            errors.append(f"unexpected policy in frozen confirmation trades at {cost:g} bps")
        recomputed = recompute_trade_metrics(trades, equity)
        observed = summary.iloc[0]
        for field, value in recomputed.items():
            if not close(value, observed[field]):
                errors.append(f"frozen {cost:g} bps {field} mismatch")

    for _, choice in choices.iterrows():
        fold = int(choice["fold"])
        fold_cutoff = pd.Timestamp(folds.loc[folds["fold"] == fold, "test_start"].iloc[0])
        fold_rank = independent_rank(grid_trades, catalog, fold_cutoff, minimum_trades=30)
        expected = str(fold_rank.iloc[0]["policy"])
        if expected != str(choice["selected_policy"]):
            errors.append(f"time-forward selection mismatch for fold {fold}")

    for cost in (5.0, 15.0, 30.0):
        trades = adaptive_trades[adaptive_trades["slippage_bps_each_side"] == cost]
        equity = adaptive_equity[adaptive_equity["slippage_bps_each_side"] == cost]
        summary = adaptive_summary[adaptive_summary["slippage_bps_each_side"] == cost]
        if trades.empty or equity.empty or summary.empty:
            errors.append(f"missing adaptive evidence for {cost:g} bps")
            continue
        recomputed = recompute_trade_metrics(trades, equity)
        observed = summary.iloc[0]
        for field, value in recomputed.items():
            if not close(value, observed[field]):
                errors.append(f"adaptive {cost:g} bps {field} mismatch")

    fixed_point = bool(decision.get("point_gate_pass"))
    fixed_uncertainty = bool(decision.get("uncertainty_gate_pass"))
    expected_fixed_class = (
        "exit_strategy_candidate" if fixed_point and fixed_uncertainty
        else "promising_but_unconfirmed" if fixed_point
        else "watchlist_only"
    )
    if decision.get("final_classification") != expected_fixed_class:
        errors.append("fixed exit classification is inconsistent with its gates")
    adaptive_point = bool(decision.get("adaptive_point_gate_pass"))
    adaptive_uncertainty = bool(decision.get("adaptive_uncertainty_gate_pass"))
    expected_adaptive_class = (
        "adaptive_exit_strategy_candidate" if adaptive_point and adaptive_uncertainty
        else "adaptive_promising_but_unconfirmed" if adaptive_point
        else "adaptive_watchlist_only"
    )
    if decision.get("adaptive_classification") != expected_adaptive_class:
        errors.append("adaptive exit classification is inconsistent with its gates")

    for name in [
        "week_block_bootstrap",
        "symbol_cluster_bootstrap",
        "adaptive_week_block_bootstrap",
        "adaptive_symbol_cluster_bootstrap",
    ]:
        interval = decision[name]
        if int(interval["samples"]) != 2000:
            errors.append(f"{name} does not contain 2000 bootstrap samples")
        if float(interval["expectancy_lower_95pct"]) > float(interval["expectancy_upper_95pct"]):
            errors.append(f"{name} expectancy interval is inverted")

    checks = {
        "catalog_policies": int(len(catalog)),
        "selected_policy": selected_policy,
        "selection_cutoff_utc": cutoff,
        "fixed_confirmation_trades_default_cost": int(
            len(frozen_trades[frozen_trades["slippage_bps_each_side"] == 5.0])
        ),
        "adaptive_trades_default_cost": int(
            len(adaptive_trades[adaptive_trades["slippage_bps_each_side"] == 5.0])
        ),
        "time_forward_choice_folds": int(choices["fold"].nunique()),
        "fixed_classification": decision.get("final_classification"),
        "adaptive_classification": decision.get("adaptive_classification"),
    }
    result = {"passed": not errors, "errors": errors, "checks": VALIDATION.json_ready(checks)}
    (report / "exit_verification_summary.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    status = "PASS" if not errors else "FAIL"
    lines = [
        "# Exit strategy independent validation",
        "",
        f"- Result: **{status}**",
        f"- Catalog: {len(catalog)} frozen policies",
        f"- Calibration-selected policy: `{selected_policy}`",
        f"- Frozen confirmation: {checks['fixed_confirmation_trades_default_cost']} default-cost trades",
        f"- Time-forward selector: {checks['adaptive_trades_default_cost']} default-cost trades across {checks['time_forward_choice_folds']} folds",
        f"- Fixed classification: `{checks['fixed_classification']}`",
        f"- Adaptive classification: `{checks['adaptive_classification']}`",
    ]
    if errors:
        lines.extend(["", "## Errors", ""] + [f"- {error}" for error in errors])
    (report / "EXIT_VALIDATION_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
