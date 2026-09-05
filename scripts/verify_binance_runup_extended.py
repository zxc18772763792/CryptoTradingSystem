"""Independently verify the extended Binance run-up research evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures"
DEFAULT_OUTPUT = ROOT / "reports" / "binance_200pct_factor_mining_2026-07-18_futures_extended"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def independent_current_event_scan(panel: pd.DataFrame) -> pd.DataFrame:
    end = panel["open_time"].max()
    start = end - pd.Timedelta(days=30)
    records: list[dict[str, Any]] = []
    for symbol, group in panel[panel["open_time"].between(start, end)].groupby("symbol"):
        rows = group.sort_values("open_time")
        close_best = -np.inf
        wick_best = -np.inf
        close_floor = float(rows.iloc[0]["close"])
        low_floor = float(rows.iloc[0]["low"])
        for _, row in rows.iloc[1:].iterrows():
            close_best = max(close_best, float(row["high"]) / close_floor - 1)
            wick_best = max(wick_best, float(row["high"]) / low_floor - 1)
            close_floor = min(close_floor, float(row["close"]))
            low_floor = min(low_floor, float(row["low"]))
        classification = "close-confirmed" if close_best >= 2.0 else "wick-only" if wick_best >= 2.0 else None
        if classification:
            records.append(
                {"symbol": symbol, "classification": classification, "close_return": close_best, "wick_return": wick_best}
            )
    return pd.DataFrame(records)


def recompute_metrics(scored: pd.DataFrame) -> dict[str, float | int]:
    target = scored["label_primary_int"].astype(int)
    percentile = scored["price_model_score"].groupby(scored["date"]).rank(pct=True, method="first")
    selected = target[percentile >= 0.98]
    base = float(target.mean())
    precision = float(selected.mean())
    return {
        "n": int(len(scored)),
        "positives": int(target.sum()),
        "auc": float(roc_auc_score(target, scored["price_model_score"])),
        "average_precision": float(average_precision_score(target, scored["price_model_score"])),
        "top_2pct_n": int(len(selected)),
        "top_2pct_precision": precision,
        "top_2pct_lift": precision / base,
    }


def verify_manifest(output: Path, manifest: dict[str, Any]) -> list[str]:
    errors = []
    for relative, expected in manifest.get("analysis_files", {}).items():
        path = output / relative
        if not path.exists():
            errors.append(f"manifest file missing: {relative}")
        elif sha256(path) != expected.get("sha256"):
            errors.append(f"manifest checksum mismatch: {relative}")
    return errors


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    output = args.output_dir.resolve()
    errors: list[str] = []

    summary = json.loads((output / "analysis_summary.json").read_text(encoding="utf-8"))
    manifest = json.loads((output / "evidence_manifest.json").read_text(encoding="utf-8"))
    errors.extend(verify_manifest(output, manifest))

    folds = pd.read_csv(output / "walk_forward_fold_metrics.csv")
    for column in ["train_end", "purge_start", "purge_end", "test_start", "test_end"]:
        folds[column] = pd.to_datetime(folds[column], utc=True)
    bad_purge = folds[
        (folds["train_end"] >= folds["purge_start"])
        | ((folds["test_start"] - folds["purge_start"]).dt.days != 14)
        | (folds["purge_end"] >= folds["test_start"])
    ]
    if not bad_purge.empty:
        errors.append(f"invalid purged folds: {bad_purge['fold'].tolist()}")

    scored = pd.read_csv(output / "historical_walk_forward_scored.csv.gz", compression="gzip")
    scored["date"] = pd.to_datetime(scored["date"], utc=True)
    scored["entry_time_14d"] = pd.to_datetime(scored["entry_time_14d"], utc=True)
    expected_entry = scored["date"] + pd.Timedelta(days=1, hours=4)
    if not scored["entry_time_14d"].equals(expected_entry):
        errors.append("forward labels do not enter at UTC 04:00 after the feature day")
    metrics = recompute_metrics(scored)
    reported = summary["price_validation"]["historical_walk_forward"]
    for key in ["n", "positives", "auc", "average_precision", "top_2pct_n", "top_2pct_precision", "top_2pct_lift"]:
        if not np.isclose(float(metrics[key]), float(reported[key]), rtol=1e-10, atol=1e-12):
            errors.append(f"metric mismatch {key}: independent={metrics[key]} reported={reported[key]}")

    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    events = independent_current_event_scan(panel)
    close_count = int((events["classification"] == "close-confirmed").sum())
    wick_count = int((events["classification"] == "wick-only").sum())
    if close_count != 17 or wick_count != 2:
        errors.append(f"current event regression failed: close={close_count}, wick={wick_count}")
    if not {"AKEUSDT", "BTWUSDT"}.issubset(set(events["symbol"])):
        errors.append("AKEUSDT or BTWUSDT missing from independently recomputed events")

    surface = pd.read_csv(output / "threshold_horizon_surface.csv")
    expected_cells = 4 * 7
    if len(surface) != expected_cells or surface[["horizon_days", "threshold_return"]].duplicated().any():
        errors.append("threshold x horizon matrix is incomplete or duplicated")
    if surface["p_value_bh"].dropna().gt(1).any() or surface["p_value_bh"].dropna().lt(0).any():
        errors.append("BH-adjusted p-values outside [0,1]")

    oi_decision = summary["oi_validation"]
    if oi_decision.get("ranking_eligible"):
        bootstrap = oi_decision.get("bootstrap", {})
        if not (
            oi_decision.get("fold_majority_both_positive", 0) > 0.5
            and bootstrap.get("delta_ap_lower_95pct", -np.inf) > 0
            and bootstrap.get("delta_lift_lower_95pct", -np.inf) > 0
        ):
            errors.append("OI ranking eligibility contradicts the preregistered gate")

    paper = summary["paper_strategy"]
    if paper.get("paper_trade_candidate") and paper.get("final_classification") != "paper_trade_candidate":
        errors.append("paper strategy classification is internally inconsistent")
    exit_verification_path = output / "exit_verification_summary.json"
    if not exit_verification_path.exists():
        errors.append("dedicated exit-policy verification is missing")
        exit_verification = {"passed": False, "checks": {}}
    else:
        exit_verification = json.loads(exit_verification_path.read_text(encoding="utf-8"))
        if not exit_verification.get("passed"):
            errors.append("dedicated exit-policy verification did not pass")
    exit_strategy = summary.get("exit_strategy")
    if not exit_strategy:
        errors.append("analysis summary is missing the time-isolated exit strategy decision")
    else:
        if exit_strategy.get("catalog_policies") != 52:
            errors.append("exit strategy summary does not cover 52 frozen policies")
        if exit_strategy.get("final_classification") != "watchlist_only":
            errors.append("fixed exit strategy classification does not match strict validation")
        if exit_strategy.get("adaptive_classification") != "adaptive_promising_but_unconfirmed":
            errors.append("adaptive exit strategy classification does not match strict validation")
    if summary.get("paper_strategy_superseded_by_exit_validation") is not True:
        errors.append("full-sample four-policy paper result is not marked as superseded")

    report = (output / "REPORT.md").read_text(encoding="utf-8")
    for required_phrase in [
        "已验证", "方向性证据", "尚未验证", "historical walk-forward", "观察名单",
        "watchlist_only", "adaptive_promising_but_unconfirmed", "+100% 时卖出一半",
    ]:
        if required_phrase not in report:
            errors.append(f"REPORT.md missing required disclosure: {required_phrase}")

    result = {
        "passed": not errors,
        "errors": errors,
        "checks": {
            "manifest_files": len(manifest.get("analysis_files", {})),
            "folds": int(len(folds)),
            "purge_days": 14,
            "recomputed_metrics": metrics,
            "current_close_confirmed_events": close_count,
            "current_wick_only_events": wick_count,
            "ake_btw_present": {"AKEUSDT", "BTWUSDT"}.issubset(set(events["symbol"])),
            "threshold_horizon_cells": int(len(surface)),
            "next_4h_open_entry_utc_hour": 4,
            "exit_policy_verification_passed": bool(exit_verification.get("passed")),
            "exit_catalog_policies": int((exit_strategy or {}).get("catalog_policies", 0)),
            "fixed_exit_classification": (exit_strategy or {}).get("final_classification"),
            "adaptive_exit_classification": (exit_strategy or {}).get("adaptive_classification"),
        },
    }
    (output / "verification_summary.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    lines = [
        "# Independent validation",
        "",
        f"- Result: **{'PASS' if result['passed'] else 'FAIL'}**",
        f"- Walk-forward folds: {len(folds)}; purge: 14 calendar days",
        f"- Current 30-day events: {close_count} close-confirmed + {wick_count} wick-only",
        f"- AKE/BTW regression: {'PASS' if result['checks']['ake_btw_present'] else 'FAIL'}",
        f"- Evidence files checked: {result['checks']['manifest_files']}",
        f"- Exit policies: {result['checks']['exit_catalog_policies']}; dedicated verifier: {'PASS' if result['checks']['exit_policy_verification_passed'] else 'FAIL'}",
        f"- Fixed/adaptive exit: `{result['checks']['fixed_exit_classification']}` / `{result['checks']['adaptive_exit_classification']}`",
    ]
    if errors:
        lines.extend(["", "## Errors", "", *[f"- {error}" for error in errors]])
    (output / "VALIDATION_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
