"""Finalize an already-computed Binance run-up evidence bundle after heavy models finish."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ANALYSIS_PATH = ROOT / "scripts" / "analyze_binance_runup_extended.py"
SPEC = importlib.util.spec_from_file_location("binance_runup_analysis_finalize", ANALYSIS_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load {ANALYSIS_PATH}")
analysis = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = analysis
SPEC.loader.exec_module(analysis)


def main() -> None:
    baseline = analysis.DEFAULT_BASELINE.resolve()
    output = analysis.DEFAULT_OUTPUT.resolve()
    daily = pd.read_csv(baseline / "daily_feature_panel.csv.gz", compression="gzip")
    daily["date"] = pd.to_datetime(daily["date"], utc=True)
    panel_4h = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel_4h["open_time"] = pd.to_datetime(panel_4h["open_time"], utc=True)
    targeted = analysis.add_forward_targets_from_4h(daily, panel_4h)
    modeling = analysis.prepare_modeling_panel(targeted)
    scored = pd.read_csv(output / "historical_walk_forward_scored.csv.gz", compression="gzip")
    scored["date"] = pd.to_datetime(scored["date"], utc=True)
    folds = pd.read_csv(output / "walk_forward_fold_metrics.csv")
    threshold_surface = pd.read_csv(output / "threshold_horizon_surface.csv")
    oi_folds = pd.read_csv(output / "oi_walk_forward_fold_metrics.csv")
    trades_path = output / "paper_trades.csv.gz"
    trades = pd.read_csv(trades_path, compression="gzip") if trades_path.exists() else pd.DataFrame()
    equity_path = output / "paper_equity_curves.csv.gz"
    equity = pd.read_csv(equity_path, compression="gzip") if equity_path.exists() else pd.DataFrame()
    current_events = analysis.scan_30d_runups(panel_4h)
    oi_panel, oi_quality = analysis.load_long_oi_panel(analysis.AMBUSH_ROOT)
    data_quality = analysis.profile_inputs_v2(
        daily,
        panel_4h,
        modeling,
        current_events,
        spot_error="external refresh disabled",
        oi_quality=oi_quality,
    )
    primary = analysis.aggregate_primary_metrics(scored, bootstrap_samples=500)
    oi_decision = json.loads((output / "oi_decision.json").read_text(encoding="utf-8"))
    paper_decision = json.loads((output / "paper_decision.json").read_text(encoding="utf-8"))
    watchlist = analysis.create_current_watchlist(baseline, output)
    oi_cohorts = analysis.build_oi_cohort_coverage(
        daily, oi_panel, baseline / "oi_daily_panel_30d.csv.gz"
    )
    oi_cohorts.to_csv(output / "oi_cohort_coverage.csv", index=False)
    current_events.to_csv(output / "current_30d_runups_verified.csv", index=False)

    exchange_metadata = analysis.load_exchange_metadata(baseline / "exchange_info_snapshot.json")
    summary = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "baseline_dir": str(baseline),
        "model_factors_frozen": list(analysis.PRICE_FACTORS),
        "validation_label": "historical_walk_forward_not_untouched_oos",
        "price_validation": primary,
        "oi_validation": oi_decision,
        "paper_strategy": paper_decision,
        "data_quality": data_quality,
        "universe_bias": {
            "current_exchange_info_symbols": int(len(exchange_metadata)),
            "historical_delisted_symbols_recovered_into_primary_panel": 0,
            "status": "unresolved_survivorship_bias",
        },
    }
    (output / "analysis_summary.json").write_text(
        json.dumps(analysis.json_ready(summary), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output / "data_quality.json").write_text(
        json.dumps(analysis.json_ready(data_quality), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    generated_charts = analysis.plot_results(
        output, folds, threshold_surface, oi_folds, trades, equity, paper_decision
    )
    analysis.write_chart_map(output)
    analysis.build_report_markdown_v2(
        output,
        primary=primary,
        oi_decision=oi_decision,
        paper_decision=paper_decision,
        data_quality=data_quality,
        folds=folds,
        watchlist=watchlist,
    )

    required = [
        baseline / "daily_feature_panel.csv.gz",
        baseline / "futures_4h_panel.csv.gz",
        baseline / "factor_model.json",
        baseline / "exchange_info_snapshot.json",
        baseline / "current_watchlist.csv",
    ]
    manifest_files = [path for path in output.rglob("*") if path.is_file()]
    manifest = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "baseline_inputs": {str(path.name): analysis.file_sha256(path) for path in required},
        "analysis_files": {
            str(path.relative_to(output)): {
                "bytes": path.stat().st_size,
                "sha256": analysis.file_sha256(path),
            }
            for path in sorted(manifest_files)
            if path.name not in {"evidence_manifest.json", "verification_summary.json", "VALIDATION_REPORT.md"}
        },
        "generated_charts": generated_charts,
        "analysis_hash": analysis.stable_hash(summary),
    }
    (output / "evidence_manifest.json").write_text(
        json.dumps(analysis.json_ready(manifest), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(analysis.json_ready(summary), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
