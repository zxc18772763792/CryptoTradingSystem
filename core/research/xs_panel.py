"""Daily per-coin panels for cross-sectional research (research loop v2).

Two sources, one long format (columns: base, date, close, volume, oi, mcap,
funding; one row per coin per day):

* research panel  data/research/xs_panel/research_daily.parquet
  built once by scripts/build_xs_research_panel.py from the ambush dataset
  (2025-07 .. 2026-07). Development + holdout live here.
* forward panel   stitched from data/research/xs_panel/weekly_archive/*.parquet,
  which scripts/generate_pump_watchlist.py appends every Monday. Only data
  after a formula was frozen counts as independent evidence.

Labels follow the validated protocol (docs/AMBUSH_MODES_BACKTEST_REPORT_2026-07-18.md):
Monday snapshots, market cap in [2e6, 1.5e9], pump100 = max close over the
next 30 days >= 2x today's close.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PANEL_DIR = PROJECT_ROOT / "data" / "research" / "xs_panel"
RESEARCH_PANEL_PATH = PANEL_DIR / "research_daily.parquet"
WEEKLY_ARCHIVE_DIR = PANEL_DIR / "weekly_archive"

LABEL_HORIZON_DAYS = 30
PUMP_THRESHOLD = 1.0  # +100%
MCAP_MIN, MCAP_MAX = 2e6, 1.5e9
DEV_END = pd.Timestamp("2026-02-15")  # same split as the validated weekly model
PANEL_COLUMNS = ["base", "date", "close", "volume", "oi", "mcap", "funding"]


def add_forward_labels(daily: pd.DataFrame, horizon: int = LABEL_HORIZON_DAYS) -> pd.DataFrame:
    """Add fwd30_maxret per coin; NaN until the full horizon has elapsed."""
    parts = []
    for _, grp in daily.sort_values(["base", "date"]).groupby("base", sort=False):
        grp = grp.copy()
        close = grp["close"].astype(float).to_numpy()
        n = len(close)
        fwd = np.full(n, np.nan)
        for i in range(n - horizon):
            fwd[i] = np.nanmax(close[i + 1 : i + 1 + horizon]) / close[i] - 1.0 if close[i] > 0 else np.nan
        # Check every adjacent day. A duplicate row can conceal a missing day
        # while leaving the overall first-to-last span unchanged.
        dates = pd.to_datetime(grp["date"]).to_numpy()
        consecutive = np.zeros(n, dtype=bool)
        if n > horizon:
            bad_step = np.diff(dates) != np.timedelta64(1, "D")
            bad_count = np.r_[0, np.cumsum(bad_step)]
            consecutive[: n - horizon] = bad_count[horizon:] == bad_count[: n - horizon]
        fwd[~consecutive] = np.nan
        grp["fwd30_maxret"] = fwd
        parts.append(grp)
    return pd.concat(parts, ignore_index=True) if parts else daily.assign(fwd30_maxret=np.nan)


def load_research_panel(path: Path = RESEARCH_PANEL_PATH) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"{path} missing - build it with scripts/build_xs_research_panel.py"
        )
    frame = pd.read_parquet(path)
    frame["date"] = pd.to_datetime(frame["date"])
    return frame


def stitch_weekly_archive(archive_dir: Path = WEEKLY_ARCHIVE_DIR) -> pd.DataFrame:
    """Merge weekly snapshots into one daily panel; later snapshots win.

    Each snapshot carries ~365 days of history per coin but only a CURRENT
    market cap, so mcap is taken on each snapshot's own last day and
    forward-filled between snapshots instead of trusting the flat history.
    """
    files = sorted(archive_dir.glob("*.parquet"))
    if not files:
        return pd.DataFrame(columns=PANEL_COLUMNS)
    frames = []
    for order, path in enumerate(files):
        snap = pd.read_parquet(path)
        snap["date"] = pd.to_datetime(snap["date"])
        snap["_order"] = order
        frames.append(snap)
    raw = pd.concat(frames, ignore_index=True)
    # Anchor points first: each snapshot's own last day, before later
    # snapshots overwrite overlapping days (a later snapshot wins a tie).
    mcap_points = (
        raw.sort_values(["_order", "date"]).groupby(["base", "_order"]).tail(1)
        .drop_duplicates(["base", "date"], keep="last")[["base", "date", "mcap"]]
    )
    raw = raw.sort_values("_order").drop_duplicates(["base", "date"], keep="last")
    raw = raw.drop(columns=["mcap"]).merge(mcap_points, on=["base", "date"], how="left")
    raw = raw.sort_values(["base", "date"])
    raw["mcap"] = raw.groupby("base")["mcap"].ffill()
    return raw[PANEL_COLUMNS].reset_index(drop=True)


def weekly_rows(daily: pd.DataFrame, feature: pd.Series) -> pd.DataFrame:
    """Monday snapshots with the feature value, the label and the universe filter."""
    frame = daily.assign(feature=feature.to_numpy())
    frame = frame[pd.to_datetime(frame["date"]).dt.dayofweek == 0]
    frame = frame[frame["mcap"].between(MCAP_MIN, MCAP_MAX)]
    frame = frame[frame["fwd30_maxret"].notna()]
    frame = frame.assign(pump=(frame["fwd30_maxret"] >= PUMP_THRESHOLD).astype(int))
    return frame[["base", "date", "feature", "pump", "fwd30_maxret"]].reset_index(drop=True)


def anonymized_summary(daily: pd.DataFrame) -> Dict[str, object]:
    """What the LLM may know about the panel: shape only, no names or dates."""
    weekly = daily[pd.to_datetime(daily["date"]).dt.dayofweek == 0]
    weekly = weekly[weekly["mcap"].between(MCAP_MIN, MCAP_MAX) & weekly["fwd30_maxret"].notna()]
    return {
        "coins": int(daily["base"].nunique()),
        "weekly_cross_sections": int(weekly["date"].nunique()),
        "base_rate_pump_2x_in_30d": round(float((weekly["fwd30_maxret"] >= PUMP_THRESHOLD).mean()), 4),
        "market_cap_range_usd": [MCAP_MIN, MCAP_MAX],
    }


def optional_panel(path: Path) -> Optional[pd.DataFrame]:
    try:
        return load_research_panel(path)
    except FileNotFoundError:
        return None
