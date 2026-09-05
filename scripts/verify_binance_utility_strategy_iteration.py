"""Independently verify taker-flow robustness and breakout full-runner rejection."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ROBUST = _load(
    "binance_utility_taker_flow_robustness_verifier",
    ROOT / "scripts" / "analyze_binance_utility_taker_flow_robustness.py",
)
RUNNER = _load(
    "binance_utility_breakout_runner_verifier",
    ROOT / "scripts" / "analyze_binance_utility_breakout_runner.py",
)
PARTIAL = _load(
    "binance_utility_breakout_partial_fraction_verifier",
    ROOT / "scripts" / "analyze_binance_utility_breakout_partial_fraction.py",
)
REGIME = _load(
    "binance_utility_regime_partial_router_verifier",
    ROOT / "scripts" / "analyze_binance_utility_regime_partial_router.py",
)


def close(left: Any, right: Any, tolerance: float = 1e-10) -> bool:
    return bool(np.isclose(float(left), float(right), rtol=0.0, atol=tolerance))


def profit_factor(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    return gains / losses if losses > 0 else (np.inf if gains > 0 else 0.0)


def main() -> None:
    robust_decision = json.loads(
        (REPORT / "utility_taker_flow_robustness_decision.json").read_text(encoding="utf-8")
    )
    definitions = pd.read_csv(REPORT / "utility_taker_flow_robustness_definitions.csv")
    calibration, confirmation = ROBUST.TAKER.load_research_rows(REPORT)
    confirmation = ROBUST.add_bar_shares(confirmation)
    fixed = definitions[definitions["definition"] == "mean_ge_50pct"].set_index("period")
    selected = confirmation[confirmation["early_taker_buy_share"] >= 0.50]
    recomputed_match = ROBUST.matched_permutation(
        confirmation,
        match_features=("early_close_return", "early_path_efficiency"),
        permutations=5000,
        seed=20260781,
    )

    runner_decision = json.loads(
        (REPORT / "utility_breakout_runner_decision.json").read_text(encoding="utf-8")
    )
    cal = pd.read_csv(REPORT / "utility_breakout_runner_calibration.csv")
    sensitivity = pd.read_csv(REPORT / "utility_breakout_runner_30d_calibration_sensitivity.csv")
    conf = pd.read_csv(REPORT / "utility_breakout_runner_confirmation.csv")
    cal_paired = pd.read_csv(REPORT / "utility_breakout_runner_calibration_paired.csv")
    trades = pd.read_csv(REPORT / "utility_breakout_runner_confirmation_trades.csv.gz", compression="gzip")
    candidates = cal[cal["policy"] != RUNNER.CURRENT_ROUTER]
    default_conf = conf[conf["slippage_bps_each_side"] == 5.0].set_index("policy")
    best = str(default_conf["expectancy"].idxmax())
    runner_policy = RUNNER.policy_catalog(30)["breakout6_full_runner_a150_t30_wick"]
    current_policy = RUNNER.policy_catalog(30)[RUNNER.CURRENT_ROUTER]

    partial_decision = json.loads(
        (REPORT / "utility_breakout_partial_fraction_decision.json").read_text(encoding="utf-8")
    )
    partial_cal = pd.read_csv(REPORT / "utility_breakout_partial_fraction_calibration.csv")
    partial_conf = pd.read_csv(REPORT / "utility_breakout_partial_fraction_confirmation.csv")
    partial_pairs = pd.read_csv(REPORT / "utility_breakout_partial_fraction_calibration_paired.csv")
    partial_policies = PARTIAL.policy_catalog(30)
    partial_default = partial_conf[partial_conf["slippage_bps_each_side"] == 5.0].set_index("partial_fraction")
    selected_fraction = float(partial_decision["calibration_selected_fraction"])
    selected_pairs = partial_pairs[partial_pairs["partial_fraction"] == selected_fraction]
    selected_split_delta = selected_pairs.groupby("validation_split")["delta_return"].mean()

    regime_decision = json.loads(
        (REPORT / "utility_regime_partial_router_decision.json").read_text(encoding="utf-8")
    )
    regime_cal = pd.read_csv(REPORT / "utility_regime_partial_router_calibration.csv").set_index("route")
    regime_conf = pd.read_csv(REPORT / "utility_regime_partial_router_confirmation.csv")
    regime_conf_default = regime_conf[
        regime_conf["slippage_bps_each_side"] == 5.0
    ].set_index("route")
    regime_cal_pairs = pd.read_csv(REPORT / "utility_regime_partial_router_calibration_paired.csv")
    regime_conf_pairs = pd.read_csv(REPORT / "utility_regime_partial_router_confirmation_paired.csv")
    regime_trades = pd.read_csv(
        REPORT / "utility_regime_partial_router_confirmation_trades.csv.gz", compression="gzip"
    )
    state_support = pd.read_csv(REPORT / "utility_regime_partial_state_support.csv")

    trade_metrics_ok = True
    for (policy, slippage), group in trades.groupby(["policy", "slippage_bps_each_side"]):
        row = conf[(conf["policy"] == policy) & (conf["slippage_bps_each_side"] == slippage)].iloc[0]
        trade_metrics_ok &= int(row["trades"]) == len(group)
        trade_metrics_ok &= close(row["expectancy"], group["net_return"].mean())
        trade_metrics_ok &= close(row["profit_factor"], profit_factor(group["net_return"]))

    regime_trade_metrics_ok = True
    for (route, slippage), group in regime_trades.groupby(["route", "slippage_bps_each_side"]):
        row = regime_conf[
            (regime_conf["route"] == route)
            & (regime_conf["slippage_bps_each_side"] == slippage)
        ].iloc[0]
        regime_trade_metrics_ok &= int(row["trades"]) == len(group)
        regime_trade_metrics_ok &= close(row["expectancy"], group["net_return"].mean())
        regime_trade_metrics_ok &= close(row["profit_factor"], profit_factor(group["net_return"]))

    affected_state = state_support[state_support["affected"].astype(bool)]
    cal_route_means = regime_cal_pairs.groupby("route")["delta_return"].mean()
    conf_route_means = regime_conf_pairs[
        regime_conf_pairs["slippage_bps_each_side"] == 5.0
    ].groupby("route")["delta_return"].mean()
    operational_audit = json.loads(
        (REPORT / "forward_monitor_operational_audit_2026-07-21.json").read_text(encoding="utf-8")
    )
    forward_root = ROOT / "data" / "research" / "binance_200pct_forward_monitor"
    snapshot_dates = sorted(path.stem for path in (forward_root / "snapshots").glob("*.json"))
    sequence_count = len(list((forward_root / "sequence_24h").glob("*.json")))
    utility_mature_count = len(list((forward_root / "continuation_utility_8h").glob("*.json")))
    launch_mature_count = len(list((forward_root / "launch_microstructure").glob("*.json")))

    checks = {
        "fixed_taker_counts_recomputed": bool(
            len(calibration) == 42
            and len(confirmation) == 88
            and len(selected) == int(fixed.loc["confirmation", "selected_rows"]) == 48
            and int(selected["late_target200"].sum()) == int(fixed.loc["confirmation", "selected_positives"]) == 9
        ),
        "nearby_thresholds_are_directionally_robust": bool(
            robust_decision["nearby_thresholds_all_positive_both_periods"] is True
            and (definitions[
                definitions["definition"].isin(
                    ["mean_ge_48pct", "mean_ge_49pct", "mean_ge_50pct", "mean_ge_51pct"]
                )
            ]["precision_increment"] > 0).all()
        ),
        "full_path_match_recomputed_and_fails_independence": bool(
            close(recomputed_match["one_sided_p"], robust_decision["matched_permutation"][1]["one_sided_p"])
            and recomputed_match["one_sided_p"] >= 0.05
            and robust_decision["independent_after_all_price_path_matches"] is False
        ),
        "taker_annotation_remains_non_actionable": bool(
            robust_decision["ranking_change_allowed"] is False
            and robust_decision["buy_gate_change_allowed"] is False
            and robust_decision["automatic_trading_allowed"] is False
        ),
        "runner_catalog_changes_only_partial_and_trail_controls": bool(
            current_policy["partial_target"] == 1.50
            and runner_policy["partial_target"] is None
            and runner_policy["partial_fraction"] == 0.0
            and runner_policy["hard_stop"] == current_policy["hard_stop"]
            and runner_policy["exhaustion_activation"] == current_policy["exhaustion_activation"]
        ),
        "all_runner_candidates_fail_calibration": bool(
            len(candidates) == 6
            and not candidates["selection_eligible"].astype(bool).any()
            and (candidates["paired_increment_mean"] < 0).all()
            and runner_decision["calibration_gate_pass"] is False
        ),
        "calibration_paired_means_recomputed": bool(
            all(
                close(
                    row["paired_increment_mean"],
                    cal_paired[cal_paired["policy"] == row["policy"]]["delta_return"].mean(),
                )
                for _, row in cal.iterrows()
            )
        ),
        "early_30d_absolute_expectancy_is_negative": bool(
            (sensitivity[sensitivity["policy"] != RUNNER.CURRENT_ROUTER]["expectancy"] <= 0).all()
            and runner_decision["early_30d_sensitivity_all_candidate_expectancies_nonpositive"] is True
        ),
        "confirmation_point_estimate_is_not_promoted": bool(
            best == runner_decision["confirmation_point_estimate_best_policy"]
            and best == "breakout6_full_runner_a150_t30_wick"
            and runner_decision["confirmation_best_is_posthoc_only"] is True
            and runner_decision["historical_promotion_allowed"] is False
            and runner_decision["forward_change_allowed"] is False
        ),
        "confirmation_trade_metrics_recomputed": bool(trade_metrics_ok),
        "partial_fraction_catalog_isolated": bool(
            sorted(policy["partial_fraction"] for policy in partial_policies.values()) == [0.25, 0.50, 0.75]
            and all(policy["partial_target"] == 1.50 for policy in partial_policies.values())
            and all(policy["trail_distance"] == 0.30 for policy in partial_policies.values())
        ),
        "calibration_selects_sell75_from_three_windows": bool(
            selected_fraction == 0.75
            and set(selected_split_delta.index) == {"cal_1", "cal_2", "cal_3"}
            and close(selected_split_delta.loc["cal_1"], 0.0)
            and selected_split_delta.loc["cal_2"] > 0
            and selected_split_delta.loc["cal_3"] > 0
            and partial_decision["calibration_gate_pass"] is True
            and bool(partial_cal.loc[partial_cal["partial_fraction"] == 0.75, "selection_eligible"].iloc[0])
        ),
        "partial_fraction_reverses_in_confirmation": bool(
            partial_default.loc[0.25, "paired_increment_mean"] > 0
            and partial_default.loc[0.75, "paired_increment_mean"] < 0
            and partial_decision["confirmation_point_estimate_best_fraction"] == 0.25
            and partial_decision["confirmation_gate_pass"] is False
            and partial_decision["retained_fraction"] == 0.50
        ),
        "partial_fraction_nonstationarity_is_not_promoted": bool(
            partial_decision["historical_promotion_allowed"] is False
            and partial_decision["forward_change_allowed"] is False
            and partial_decision["automatic_trading_allowed"] is False
            and partial_decision["classification"] == "calibration_fraction_reversed_in_confirmation_retain_sell50"
        ),
        "regime_state_support_is_too_sparse": bool(
            len(affected_state) == regime_decision["affected_calibration_signals"] == 3
            and set(affected_state["market_state"]) == {"bear_highvol_broad"}
            and regime_decision["minimum_affected_calibration_trades"]
            == REGIME.MIN_AFFECTED_CALIBRATION_TRADES
            == 5
            and all(close(value, 0.50) for value in regime_decision["calibration_market_state_mapping"].values())
        ),
        "regime_route_direction_reverses_out_of_sample": bool(
            cal_route_means["semantic_vol_high25_low75"] < 0
            and conf_route_means["semantic_vol_high25_low75"] > 0
            and cal_route_means["calibration_vol_high75_low50"] > 0
            and conf_route_means["calibration_vol_high75_low50"] < 0
            and close(
                cal_route_means["semantic_vol_high25_low75"],
                regime_cal.loc["semantic_vol_high25_low75", "paired_increment_mean"],
            )
            and close(
                conf_route_means["semantic_vol_high25_low75"],
                regime_conf_default.loc["semantic_vol_high25_low75", "paired_increment_mean"],
            )
        ),
        "regime_router_is_not_promoted": bool(
            regime_decision["calibration_selected_route"] == "baseline_sell50"
            and regime_decision["calibration_candidate_pass_count"] == 0
            and regime_decision["calibration_gate_pass"] is False
            and regime_decision["confirmation_gate_pass"] is False
            and regime_decision["retained_route"] == "baseline_sell50"
            and regime_decision["historical_promotion_allowed"] is False
            and regime_decision["forward_change_allowed"] is False
            and regime_decision["automatic_trading_allowed"] is False
        ),
        "regime_confirmation_trade_metrics_recomputed": bool(regime_trade_metrics_ok),
        "forward_operational_gap_is_evidence_backed": bool(
            snapshot_dates == operational_audit["immutable_snapshot_dates_present"]
            and operational_audit["immutable_snapshot_dates_missing"] == ["2026-07-20", "2026-07-21"]
            and sequence_count == operational_audit["sequence_24h_observations"] == 6
            and utility_mature_count == operational_audit["mature_utility_observations"] == 0
            and launch_mature_count == operational_audit["mature_launch_microstructure_observations"] == 0
        ),
        "late_diagnostic_is_not_backfilled_as_oos": bool(
            operational_audit["live_public_data_diagnostic"]["dry_run"] is True
            and operational_audit["live_public_data_diagnostic"]["snapshot_written"] is False
            and operational_audit["live_public_data_diagnostic"]["audit_passed"] is True
            and operational_audit["historical_backfill_allowed"] is False
            and operational_audit["oos_claim_allowed"] is False
            and "2026-07-21" not in snapshot_dates
        ),
    }
    errors = [name for name, passed in checks.items() if not passed]
    result = {
        "passed": not errors,
        "errors": errors,
        "checks": checks,
        "recomputed": {
            "confirmation_taker_rows": int(len(selected)),
            "confirmation_taker_positives": int(selected["late_target200"].sum()),
            "full_path_match_p": float(recomputed_match["one_sided_p"]),
            "calibration_runner_best_increment": float(candidates["paired_increment_mean"].max()),
            "confirmation_best_policy": best,
            "confirmation_best_expectancy": float(default_conf.loc[best, "expectancy"]),
            "calibration_selected_partial_fraction": selected_fraction,
            "confirmation_sell75_increment": float(partial_default.loc[0.75, "paired_increment_mean"]),
            "confirmation_sell25_increment": float(partial_default.loc[0.25, "paired_increment_mean"]),
            "regime_calibration_affected_signals": int(len(affected_state)),
            "regime_confirmation_affected_signals": int(regime_decision["affected_confirmation_signals"]),
            "regime_semantic_vol_calibration_increment": float(
                cal_route_means["semantic_vol_high25_low75"]
            ),
            "regime_semantic_vol_confirmation_increment": float(
                conf_route_means["semantic_vol_high25_low75"]
            ),
            "forward_snapshot_dates": snapshot_dates,
            "forward_mature_utility_observations": utility_mature_count,
        },
    }
    (REPORT / "utility_strategy_iteration_verification.json").write_text(
        json.dumps(RUNNER.VALIDATION.json_ready(result), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(RUNNER.VALIDATION.json_ready(result), ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
