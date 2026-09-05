"""Stress-test the prospective 8h taker-flow annotation.

This script does not search for a better threshold.  It checks whether the
predeclared 50% semantic split survives nearby thresholds/definitions and
whether its confirmation precision increment remains unusual after taker-flow
flags are shuffled only among signals with similar price paths.  The result is
diagnostic and cannot alter ranking or trading eligibility.
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
DEFAULT_REPORT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


TAKER = _load(
    "binance_utility_taker_flow_filter_for_robustness",
    ROOT / "scripts" / "analyze_binance_utility_taker_flow_filter.py",
)
VALIDATION = TAKER.VALIDATION


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--permutations", type=int, default=5000)
    return parser.parse_args()


def add_bar_shares(rows: pd.DataFrame) -> pd.DataFrame:
    result = rows.copy()
    mean = pd.to_numeric(result["early_taker_buy_share"], errors="coerce")
    acceleration = pd.to_numeric(result["early_taker_acceleration"], errors="coerce")
    result["first_bar_taker_buy_share"] = mean - acceleration / 2.0
    result["last_bar_taker_buy_share"] = mean + acceleration / 2.0
    return result


def flag_catalog(rows: pd.DataFrame) -> dict[str, pd.Series]:
    mean = rows["early_taker_buy_share"]
    first = rows["first_bar_taker_buy_share"]
    last = rows["last_bar_taker_buy_share"]
    flags = {
        f"mean_ge_{int(threshold * 100)}pct": mean >= threshold
        for threshold in (0.47, 0.48, 0.49, 0.50, 0.51, 0.52)
    }
    flags.update(
        {
            "both_bars_ge_50pct": (first >= 0.50) & (last >= 0.50),
            "last_bar_ge_50pct": last >= 0.50,
            "first_bar_ge_50pct": first >= 0.50,
            "mean_ge_50pct_and_nondeclining": (mean >= 0.50) & (last >= first),
        }
    )
    return flags


def definition_metrics(rows: pd.DataFrame, *, period: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    events = TAKER.first_signal_per_event(rows)
    event_flags = flag_catalog(events)
    for name, flag in flag_catalog(rows).items():
        selected = rows[flag.fillna(False)]
        selected_events = events[event_flags[name].fillna(False)]
        records.append(
            {
                "period": period,
                "definition": name,
                "base_rows": int(len(rows)),
                "base_positives": int(rows["late_target200"].sum()),
                "base_precision": float(rows["late_target200"].mean()),
                "selected_rows": int(len(selected)),
                "selected_positives": int(selected["late_target200"].sum()),
                "selected_precision": float(selected["late_target200"].mean()) if len(selected) else np.nan,
                "precision_increment": (
                    float(selected["late_target200"].mean() - rows["late_target200"].mean())
                    if len(selected) else np.nan
                ),
                "positive_retention": (
                    float(selected["late_target200"].sum() / rows["late_target200"].sum())
                    if rows["late_target200"].sum() else 0.0
                ),
                "utility_expectancy_14d": float(selected["utility_return_14d"].mean()) if len(selected) else np.nan,
                "base_events": int(len(events)),
                "base_positive_events": int(events["late_target200"].sum()),
                "selected_events": int(len(selected_events)),
                "selected_positive_events": int(selected_events["late_target200"].sum()),
                "selected_event_precision": (
                    float(selected_events["late_target200"].mean()) if len(selected_events) else np.nan
                ),
            }
        )
    return records


def _tercile(values: pd.Series) -> pd.Series:
    valid = values.notna()
    result = pd.Series("missing", index=values.index, dtype=object)
    if valid.sum() >= 3:
        ranked = values[valid].rank(method="first")
        result.loc[valid] = pd.qcut(ranked, q=3, labels=["low", "mid", "high"]).astype(str)
    return result


def matched_permutation(
    rows: pd.DataFrame,
    *,
    match_features: tuple[str, ...],
    permutations: int,
    seed: int,
) -> dict[str, Any]:
    data = rows.copy().reset_index(drop=True)
    data["flag"] = data["early_taker_buy_share"] >= 0.50
    strata_parts = [data["fold"].astype(str)]
    for feature in match_features:
        strata_parts.append(data.groupby("fold", group_keys=False)[feature].apply(_tercile))
    stratum = strata_parts[0]
    for part in strata_parts[1:]:
        stratum = stratum + "|" + part.astype(str)
    data["stratum"] = stratum
    selected = data[data["flag"]]
    observed = float(selected["late_target200"].mean() - data["late_target200"].mean())
    groups = [group.index.to_numpy() for _, group in data.groupby("stratum", sort=False)]
    source = data["flag"].to_numpy(dtype=bool)
    labels = data["late_target200"].to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    simulated = np.empty(permutations, dtype=float)
    for iteration in range(permutations):
        shuffled = source.copy()
        for indices in groups:
            shuffled[indices] = rng.permutation(shuffled[indices])
        simulated[iteration] = labels[shuffled].mean() - labels.mean() if shuffled.any() else np.nan
    simulated = simulated[np.isfinite(simulated)]
    return {
        "match_features": list(match_features),
        "rows": int(len(data)),
        "strata": int(data["stratum"].nunique()),
        "observed_precision_increment": observed,
        "permutations": int(len(simulated)),
        "one_sided_p": float((1 + np.sum(simulated >= observed)) / (len(simulated) + 1)),
        "null_median": float(np.median(simulated)),
        "null_upper_95pct": float(np.quantile(simulated, 0.95)),
    }


def main() -> None:
    args = parse_args()
    report = args.report_dir.resolve()
    calibration, confirmation = TAKER.load_research_rows(report)
    calibration = add_bar_shares(calibration)
    confirmation = add_bar_shares(confirmation)
    definitions = pd.DataFrame(
        definition_metrics(calibration, period="calibration")
        + definition_metrics(confirmation, period="confirmation")
    )
    schemas = [
        ("return_only", ("early_close_return",)),
        ("return_and_efficiency", ("early_close_return", "early_path_efficiency")),
        ("return_and_model_score", ("early_close_return", "continuation_score")),
    ]
    matches = [
        {
            "schema": name,
            **matched_permutation(
                confirmation,
                match_features=features,
                permutations=args.permutations,
                seed=20260780 + offset,
            ),
        }
        for offset, (name, features) in enumerate(schemas)
    ]
    fixed = definitions[definitions["definition"] == "mean_ge_50pct"].set_index("period")
    nearby = definitions[
        definitions["definition"].isin(["mean_ge_48pct", "mean_ge_49pct", "mean_ge_50pct", "mean_ge_51pct"])
    ]
    nearby_positive = bool((nearby["precision_increment"] > 0).all())
    full_path_independent = bool(
        all(item["one_sided_p"] < 0.05 for item in matches)
    )
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "evidence_class": "posthoc_confirmation_matched_robustness_prospective_only",
        "fixed_definition": "mean_ge_50pct",
        "nearby_thresholds_all_positive_both_periods": nearby_positive,
        "fixed_calibration_precision_increment": float(fixed.loc["calibration", "precision_increment"]),
        "fixed_confirmation_precision_increment": float(fixed.loc["confirmation", "precision_increment"]),
        "fixed_calibration_utility_expectancy_14d": float(fixed.loc["calibration", "utility_expectancy_14d"]),
        "matched_permutation": matches,
        "independent_after_all_price_path_matches": full_path_independent,
        "classification": (
            "independent_taker_flow_evidence" if full_path_independent
            else "robust_directional_annotation_not_independently_identified"
        ),
        "ranking_change_allowed": False,
        "buy_gate_change_allowed": False,
        "automatic_trading_allowed": False,
    }
    definitions.to_csv(report / "utility_taker_flow_robustness_definitions.csv", index=False)
    pd.DataFrame(matches).to_csv(report / "utility_taker_flow_price_matched_permutation.csv", index=False)
    (report / "utility_taker_flow_robustness_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
