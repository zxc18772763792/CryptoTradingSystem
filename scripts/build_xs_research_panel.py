"""Build the daily research panel that research loop v2 evaluates formulas on.

Reads the ambush dataset (data/research/ambush_modes, ~110 alts, 1h klines +
OI + market cap + funding with availability shifts already applied by
backtest_ambush_modes.load_enriched_frame), resamples to daily exactly like
scripts/pump_precursor_panel.py, adds the 30-day forward label, and writes
data/research/xs_panel/research_daily.parquet (long format, one row per coin
per day). Run once; re-run only if the underlying dataset is rebuilt.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.research.xs_panel import PANEL_COLUMNS, RESEARCH_PANEL_PATH, add_forward_labels  # noqa: E402

_spec = importlib.util.spec_from_file_location("bt", SCRIPT_DIR / "backtest_ambush_modes.py")
bt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bt)


def daily_frame(base: str) -> pd.DataFrame:
    frame = bt.load_enriched_frame(base)
    if frame is None or pd.to_numeric(frame["mcap_usd"], errors="coerce").notna().sum() < 24:
        return pd.DataFrame()
    daily = pd.DataFrame(
        {
            "close": pd.to_numeric(frame["close"], errors="coerce").resample("1D").last(),
            "volume": pd.to_numeric(frame["volume"], errors="coerce").resample("1D").sum(),
            "oi": pd.to_numeric(frame["oi_usd"], errors="coerce").resample("1D").last(),
            "mcap": pd.to_numeric(frame["mcap_usd"], errors="coerce").resample("1D").last(),
            "funding": pd.to_numeric(frame["funding_rate"], errors="coerce").resample("1D").mean(),
        }
    ).dropna(subset=["close"])
    if len(daily) < 45:
        return pd.DataFrame()
    return daily.assign(base=base).rename_axis("date").reset_index()


def main() -> int:
    bases = sorted(p.stem for p in (bt.DATA_DIR / "klines_1h").glob("*.parquet"))
    parts = [daily_frame(base) for base in bases]
    parts = [p for p in parts if len(p)]
    if not parts:
        print("no usable coins in the ambush dataset")
        return 1
    panel = add_forward_labels(pd.concat(parts, ignore_index=True)[PANEL_COLUMNS])
    RESEARCH_PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(RESEARCH_PANEL_PATH, index=False)
    labelled = panel["fwd30_maxret"].notna()
    print(
        f"research panel -> {RESEARCH_PANEL_PATH}: {panel['base'].nunique()} coins, {len(panel)} rows, "
        f"{panel['date'].min():%Y-%m-%d}..{panel['date'].max():%Y-%m-%d}, labelled rows {int(labelled.sum())}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
