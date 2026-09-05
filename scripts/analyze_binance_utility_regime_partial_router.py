"""Test whether frozen market regimes explain partial-sale fraction reversal.

The parent breakout hold router and its +150% partial target remain fixed.  This
study compares three deliberately simple fraction routers with the stable 50%
baseline.  Calibration alone defines the learned router; confirmation cannot
rescue a rule that fails calibration or the minimum affected-trade gate.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Callable, Mapping

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"
BASELINE_NAME = "breakout6_sell50_at150_trail30_wick"
MIN_AFFECTED_CALIBRATION_TRADES = 5


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PARTIAL = _load(
    "binance_utility_breakout_partial_fraction_for_regime_router",
    ROOT / "scripts" / "analyze_binance_utility_breakout_partial_fraction.py",
)
RUNNER = PARTIAL.RUNNER
HOLD = PARTIAL.HOLD
TIMING = PARTIAL.TIMING
VALIDATION = PARTIAL.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--bootstrap-samples", type=int, default=3000)
    return parser.parse_args()


def simulate_fraction_catalog(
    entries: pd.DataFrame,
    bars: Mapping[str, pd.DataFrame],
    funding: Mapping[str, pd.Series],
    switches: Mapping[int, pd.Timestamp],
    *,
    days: int,
    slippage: float,
) -> dict[float, list[dict[str, Any]]]:
    policies = PARTIAL.policy_catalog(days)
    result: dict[float, list[dict[str, Any]]] = {}
    for fraction in (0.25, 0.50, 0.75):
        name = f"breakout6_sell{int(fraction * 100)}_at150_trail30_wick"
        result[fraction] = RUNNER.simulate(
            entries,
            bars,
            funding,
            switches,
            policy_name=name,
            policy=policies[name],
            days=days,
            slippage=slippage,
        )
    return result


def outcome_maps(catalog: Mapping[float, list[dict[str, Any]]]) -> dict[float, dict[int, dict[str, Any]]]:
    return {
        fraction: {int(item["signal_key"]): item for item in outcomes}
        for fraction, outcomes in catalog.items()
    }


def route_outcomes(
    catalog: Mapping[float, list[dict[str, Any]]],
    choose_fraction: Callable[[Mapping[str, Any]], float],
    *,
    route_name: str,
) -> list[dict[str, Any]]:
    maps = outcome_maps(catalog)
    routed: list[dict[str, Any]] = []
    for baseline in catalog[0.50]:
        fraction = float(choose_fraction(baseline))
        chosen = dict(maps[fraction][int(baseline["signal_key"])])
        chosen["policy"] = route_name
        chosen["routed_partial_fraction"] = fraction
        routed.append(chosen)
    return routed


def calibration_state_map(catalog: Mapping[float, list[dict[str, Any]]]) -> tuple[dict[str, float], pd.DataFrame]:
    maps = outcome_maps(catalog)
    records: list[dict[str, Any]] = []
    for baseline in catalog[0.50]:
        key = int(baseline["signal_key"])
        row = {
            "signal_key": key,
            "market_state": str(baseline.get("market_state")),
            "validation_split": str(baseline["validation_split"]),
        }
        base_return = float(maps[0.50][key]["net_return"])
        row["affected"] = any(
            abs(float(maps[fraction][key]["net_return"]) - base_return) > 1e-12
            for fraction in (0.25, 0.75)
        )
        for fraction in (0.25, 0.50, 0.75):
            row[f"return_{int(fraction * 100)}"] = float(maps[fraction][key]["net_return"])
        records.append(row)
    frame = pd.DataFrame(records)
    mapping: dict[str, float] = {}
    for state, group in frame.groupby("market_state", sort=True):
        affected = int(group["affected"].sum())
        if affected < MIN_AFFECTED_CALIBRATION_TRADES:
            mapping[str(state)] = 0.50
            continue
        means = {
            fraction: float(group[f"return_{int(fraction * 100)}"].mean())
            for fraction in (0.25, 0.50, 0.75)
        }
        mapping[str(state)] = max(means, key=means.get)
    return mapping, frame


def route_catalog(
    state_map: Mapping[str, float],
) -> dict[str, Callable[[Mapping[str, Any]], float]]:
    return {
        "baseline_sell50": lambda _: 0.50,
        "semantic_vol_high25_low75": (
            lambda row: 0.25 if row.get("btc_vol_regime") == "highvol" else 0.75
        ),
        "calibration_vol_high75_low50": (
            lambda row: 0.75 if row.get("btc_vol_regime") == "highvol" else 0.50
        ),
        "semantic_risk_bull25_bear_narrow75": (
            lambda row: (
                0.25
                if row.get("btc_trend_regime") == "bull" and row.get("breadth_regime") == "broad"
                else 0.75
                if row.get("btc_trend_regime") == "bear" and row.get("breadth_regime") == "narrow"
                else 0.50
            )
        ),
        "calibration_market_state_min5": (
            lambda row: float(state_map.get(str(row.get("market_state")), 0.50))
        ),
    }


def route_record(
    name: str,
    detail: Mapping[str, Any],
    paired: pd.DataFrame,
) -> dict[str, Any]:
    summary = detail["strategy_summary"]
    split_delta = paired.groupby("validation_split", sort=True)["delta_return"].mean()
    affected = paired[paired["delta_return"].abs() > 1e-12]
    eligible = bool(
        name != "baseline_sell50"
        and len(affected) >= MIN_AFFECTED_CALIBRATION_TRADES
        and summary.get("trades", 0) >= 30
        and summary.get("expectancy", -1) > 0
        and summary.get("profit_factor", 0) > 1
        and detail.get("leave_largest_winner_out_expectancy") is not None
        and detail["leave_largest_winner_out_expectancy"] > 0
        and detail.get("largest_positive_pnl_share") is not None
        and detail["largest_positive_pnl_share"] <= 0.35
        and len(split_delta) >= 3
        and float((split_delta > 0).mean()) >= 2 / 3
        and float(paired["delta_return"].mean()) > 0
    )
    return {
        "route": name,
        **summary,
        "affected_rows": int(len(affected)),
        "paired_increment_mean": float(paired["delta_return"].mean()),
        "affected_increment_mean": float(affected["delta_return"].mean()) if len(affected) else 0.0,
        "positive_split_share": float((split_delta > 0).mean()) if len(split_delta) else 0.0,
        "split_increment_means": "|".join(f"{key}:{value:.12g}" for key, value in split_delta.items()),
        "leave_largest_winner_out_expectancy": detail.get("leave_largest_winner_out_expectancy"),
        "largest_positive_pnl_share": detail.get("largest_positive_pnl_share"),
        "selection_eligible": eligible,
    }


def evaluate_routes(
    catalog: Mapping[float, list[dict[str, Any]]],
    routes: Mapping[str, Callable[[Mapping[str, Any]], float]],
    *,
    bootstrap_samples: int,
    seed: int,
) -> tuple[pd.DataFrame, dict[str, Any], list[pd.DataFrame], list[pd.DataFrame]]:
    baseline = route_outcomes(catalog, routes["baseline_sell50"], route_name="baseline_sell50")
    records: list[dict[str, Any]] = []
    details: dict[str, Any] = {}
    pairs: list[pd.DataFrame] = []
    trades: list[pd.DataFrame] = []
    for offset, (name, rule) in enumerate(routes.items()):
        outcomes = baseline if name == "baseline_sell50" else route_outcomes(catalog, rule, route_name=name)
        detail, trade_frame, _, paired = HOLD.route_metrics(
            baseline,
            outcomes,
            bootstrap_samples=bootstrap_samples,
            seed=seed + offset * 20,
        )
        records.append(route_record(name, detail, paired))
        details[name] = detail
        pairs.append(paired.assign(route=name))
        if len(trade_frame):
            trades.append(trade_frame.assign(route=name))
    return pd.DataFrame(records), details, pairs, trades


def fraction_segment_table(
    calibration: Mapping[float, list[dict[str, Any]]],
    confirmation: Mapping[float, list[dict[str, Any]]],
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for period, catalog in (("calibration", calibration), ("confirmation", confirmation)):
        baseline_map = outcome_maps(catalog)[0.50]
        for fraction in (0.25, 0.75):
            for item in catalog[fraction]:
                key = int(item["signal_key"])
                delta = float(item["net_return"] - baseline_map[key]["net_return"])
                records.append(
                    {
                        "period": period,
                        "signal_key": key,
                        "fraction": fraction,
                        "delta_return": delta,
                        "affected": abs(delta) > 1e-12,
                        "btc_trend_regime": item.get("btc_trend_regime"),
                        "btc_vol_regime": item.get("btc_vol_regime"),
                        "breadth_regime": item.get("breadth_regime"),
                        "market_state": item.get("market_state"),
                    }
                )
    frame = pd.DataFrame(records)
    grouped: list[dict[str, Any]] = []
    for dimension in ("btc_trend_regime", "btc_vol_regime", "breadth_regime", "market_state"):
        for keys, group in frame.groupby(["period", dimension, "fraction"], dropna=False, sort=True):
            affected = group[group["affected"]]
            grouped.append(
                {
                    "dimension": dimension,
                    "period": keys[0],
                    "segment": keys[1],
                    "fraction": keys[2],
                    "rows": int(len(group)),
                    "affected_rows": int(len(affected)),
                    "mean_delta": float(group["delta_return"].mean()),
                    "affected_mean_delta": float(affected["delta_return"].mean()) if len(affected) else np.nan,
                }
            )
    return pd.DataFrame(grouped)


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    entries = RUNNER.load_timing_rows(report)
    bars, funding = RUNNER.market_data(args.baseline_dir.resolve())
    cal_entries, cal_switches = RUNNER.period_rows(entries, "calibration")
    conf_entries, conf_switches = RUNNER.period_rows(entries, "confirmation")

    calibration_catalog = simulate_fraction_catalog(
        cal_entries, bars, funding, cal_switches, days=14, slippage=5.0
    )
    state_map, state_support = calibration_state_map(calibration_catalog)
    routes = route_catalog(state_map)
    calibration, calibration_details, cal_pairs, _ = evaluate_routes(
        calibration_catalog,
        routes,
        bootstrap_samples=args.bootstrap_samples,
        seed=20262000,
    )

    confirmation_frames: list[pd.DataFrame] = []
    confirmation_pairs: list[pd.DataFrame] = []
    confirmation_trades: list[pd.DataFrame] = []
    confirmation_details: dict[str, Any] = {}
    confirmation_default_catalog: dict[float, list[dict[str, Any]]] | None = None
    for slippage in (5.0, 15.0, 30.0):
        catalog = simulate_fraction_catalog(
            conf_entries, bars, funding, conf_switches, days=30, slippage=slippage
        )
        frame, details, pairs, trades = evaluate_routes(
            catalog,
            routes,
            bootstrap_samples=args.bootstrap_samples,
            seed=20262200 + int(slippage) * 20,
        )
        frame["slippage_bps_each_side"] = slippage
        confirmation_frames.append(frame)
        confirmation_pairs.extend([item.assign(slippage_bps_each_side=slippage) for item in pairs])
        confirmation_trades.extend([item.assign(slippage_bps_each_side=slippage) for item in trades])
        if slippage == 5.0:
            confirmation_details = details
            confirmation_default_catalog = catalog
    confirmation = pd.concat(confirmation_frames, ignore_index=True)
    assert confirmation_default_catalog is not None
    segments = fraction_segment_table(calibration_catalog, confirmation_default_catalog)

    eligible = calibration[calibration["selection_eligible"]]
    selected = str(eligible.sort_values("paired_increment_mean", ascending=False).iloc[0]["route"]) if len(eligible) else "baseline_sell50"
    default_confirmation = confirmation[confirmation["slippage_bps_each_side"] == 5.0].set_index("route")
    selected_confirmation_increment = float(default_confirmation.loc[selected, "paired_increment_mean"])
    confirmation_gate = bool(selected != "baseline_sell50" and selected_confirmation_increment > 0)
    affected_calibration = int(state_support["affected"].sum())
    affected_confirmation = int(
        sum(
            abs(float(item["net_return"]) - float(outcome_maps(confirmation_default_catalog)[0.50][int(item["signal_key"])]["net_return"])) > 1e-12
            for item in confirmation_default_catalog[0.25]
        )
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "posthoc_regime_explanation_calibration_selected_confirmation_audited",
        "parent_router": "breakout6_half150_trail30_wick",
        "minimum_affected_calibration_trades": MIN_AFFECTED_CALIBRATION_TRADES,
        "affected_calibration_signals": affected_calibration,
        "affected_confirmation_signals": affected_confirmation,
        "calibration_market_state_mapping": state_map,
        "calibration_selected_route": selected,
        "calibration_candidate_pass_count": int(len(eligible)),
        "calibration_gate_pass": bool(len(eligible)),
        "confirmation_selected_route_increment": selected_confirmation_increment,
        "confirmation_gate_pass": confirmation_gate,
        "retained_route": "baseline_sell50",
        "historical_promotion_allowed": False,
        "forward_change_allowed": False,
        "classification": "regime_router_not_identified_retain_static_sell50",
        "automatic_trading_allowed": False,
        "calibration_details": calibration_details,
        "confirmation_details_default_cost": confirmation_details,
    }
    calibration.to_csv(report / "utility_regime_partial_router_calibration.csv", index=False)
    confirmation.to_csv(report / "utility_regime_partial_router_confirmation.csv", index=False)
    segments.to_csv(report / "utility_regime_partial_fraction_segments.csv", index=False)
    state_support.to_csv(report / "utility_regime_partial_state_support.csv", index=False)
    pd.concat(cal_pairs, ignore_index=True).to_csv(
        report / "utility_regime_partial_router_calibration_paired.csv", index=False
    )
    pd.concat(confirmation_pairs, ignore_index=True).to_csv(
        report / "utility_regime_partial_router_confirmation_paired.csv", index=False
    )
    pd.concat(confirmation_trades, ignore_index=True).to_csv(
        report / "utility_regime_partial_router_confirmation_trades.csv.gz",
        index=False,
        compression="gzip",
    )
    (report / "utility_regime_partial_router_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(VALIDATION.json_ready({
        "affected_calibration_signals": affected_calibration,
        "affected_confirmation_signals": affected_confirmation,
        "calibration_selected_route": selected,
        "calibration_gate_pass": decision["calibration_gate_pass"],
        "confirmation_gate_pass": confirmation_gate,
        "classification": decision["classification"],
    }), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
