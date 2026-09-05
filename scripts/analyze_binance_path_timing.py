"""Diagnose how quickly frozen Binance candidates launch and how often winners stop out first."""

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
HORIZONS = (1, 3, 5, 7, 14)
TARGETS = (0.10, 0.20, 0.50, 1.00, 2.00)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


VALIDATION = _load(
    "binance_runup_validation_for_path_timing",
    ROOT / "core" / "research" / "binance_runup_validation.py",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def first_hit_hours(path: pd.DataFrame, entry_time: pd.Timestamp, entry_open: float, target: float) -> float | None:
    hits = path[pd.to_numeric(path["high"], errors="coerce") >= entry_open * (1.0 + target)]
    if hits.empty:
        return None
    return float((pd.Timestamp(hits.iloc[0]["open_time"]) - entry_time).total_seconds() / 3600.0)


def main() -> None:
    args = parse_args()
    baseline = args.baseline_dir.resolve()
    report = args.report_dir.resolve()
    entries = pd.read_csv(report / "entry_timing_candidates.csv.gz", compression="gzip")
    entries["entry_time"] = pd.to_datetime(entries["entry_time"], utc=True)
    entries = entries[(entries["entry_rule"] == "next_open") & (entries["fold"] >= 3)].copy()
    panel = pd.read_csv(baseline / "futures_4h_panel.csv.gz", compression="gzip")
    panel["open_time"] = pd.to_datetime(panel["open_time"], utc=True)
    bars_by_symbol = {
        symbol: group.sort_values("open_time").reset_index(drop=True)
        for symbol, group in panel.groupby("symbol", sort=False)
    }

    records: list[dict[str, Any]] = []
    for _, entry in entries.iterrows():
        symbol = str(entry["symbol"])
        bars = bars_by_symbol.get(symbol)
        if bars is None:
            continue
        entry_time = pd.Timestamp(entry["entry_time"])
        entry_open = float(entry["entry_open"])
        path = bars[
            (bars["open_time"] >= entry_time)
            & (bars["open_time"] <= entry_time + pd.Timedelta(days=14))
        ].copy()
        if path.empty:
            continue
        row: dict[str, Any] = {
            "symbol": symbol,
            "signal_date": entry["date"],
            "fold": int(entry["fold"]),
            "entry_time": entry_time,
            "entry_open": entry_open,
            "target200_14d": bool(entry["target200_14d_from_entry"]),
        }
        for days in HORIZONS:
            cut = path[path["open_time"] <= entry_time + pd.Timedelta(days=days)]
            row[f"mfe_{days}d"] = float(cut["high"].max() / entry_open - 1.0)
            row[f"mae_{days}d"] = float(cut["low"].min() / entry_open - 1.0)
        for target in TARGETS:
            row[f"hit_{int(target * 100)}pct_hours"] = first_hit_hours(path, entry_time, entry_open, target)
        stop_hits = path[pd.to_numeric(path["low"], errors="coerce") <= entry_open * 0.80]
        row["hit_stop20_hours"] = (
            None if stop_hits.empty else
            float((pd.Timestamp(stop_hits.iloc[0]["open_time"]) - entry_time).total_seconds() / 3600.0)
        )
        for target in (0.50, 1.00, 2.00):
            target_hours = row[f"hit_{int(target * 100)}pct_hours"]
            stop_hours = row["hit_stop20_hours"]
            row[f"stop20_before_{int(target * 100)}pct"] = bool(
                stop_hours is not None and (target_hours is None or stop_hours <= target_hours)
            )
        records.append(row)

    paths = pd.DataFrame(records)
    paths.to_csv(report / "path_timing_diagnostics.csv.gz", index=False, compression="gzip")
    summary_rows: list[dict[str, Any]] = []
    for success, group in paths.groupby("target200_14d"):
        row: dict[str, Any] = {
            "cohort": "actual_200pct" if success else "not_200pct",
            "signals": int(len(group)),
        }
        for days in HORIZONS:
            row[f"median_mfe_{days}d"] = float(group[f"mfe_{days}d"].median())
            row[f"median_mae_{days}d"] = float(group[f"mae_{days}d"].median())
            for target in (0.10, 0.20, 0.50):
                row[f"share_mfe{int(target * 100)}_by_{days}d"] = float(
                    (group[f"mfe_{days}d"] >= target).mean()
                )
        for target in TARGETS:
            hours = pd.to_numeric(group[f"hit_{int(target * 100)}pct_hours"], errors="coerce")
            row[f"median_hours_to_{int(target * 100)}pct"] = float(hours.median()) if hours.notna().any() else None
        for target in (0.50, 1.00, 2.00):
            row[f"share_stop20_before_{int(target * 100)}pct"] = float(
                group[f"stop20_before_{int(target * 100)}pct"].mean()
            )
        summary_rows.append(row)
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(report / "path_timing_summary.csv", index=False)

    winners = paths[paths["target200_14d"]]
    losers = paths[~paths["target200_14d"]]
    decision = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC"),
        "scope": "next-open top-2% candidates in confirmation folds 3-6",
        "signals": int(len(paths)),
        "actual_200pct_signals": int(len(winners)),
        "winner_launch_speed": {
            "median_mfe_1d": float(winners["mfe_1d"].median()),
            "median_mfe_3d": float(winners["mfe_3d"].median()),
            "median_hours_to_50pct": float(pd.to_numeric(winners["hit_50pct_hours"], errors="coerce").median()),
            "median_hours_to_100pct": float(pd.to_numeric(winners["hit_100pct_hours"], errors="coerce").median()),
            "share_below_10pct_mfe_at_3d": float((winners["mfe_3d"] < 0.10).mean()),
            "share_below_20pct_mfe_at_5d": float((winners["mfe_5d"] < 0.20).mean()),
        },
        "stop_path_risk": {
            "winner_share_stop20_before_50pct": float(winners["stop20_before_50pct"].mean()),
            "winner_share_stop20_before_100pct": float(winners["stop20_before_100pct"].mean()),
            "loser_share_hit_stop20_within_14d": float(losers["hit_stop20_hours"].notna().mean()),
        },
        "interpretation": "failure-to-launch and hard-stop rules must trade off fast loser removal against stopping future +200% paths before ignition",
    }
    (report / "path_timing_decision.json").write_text(
        json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(VALIDATION.json_ready(decision), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
