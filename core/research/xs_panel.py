"""Daily per-coin panels for cross-sectional research (research loop v2).

Two sources, one long format (columns: base, date, close, volume, oi, mcap,
funding; one row per coin per day):

* research panel  data/research/xs_panel/research_daily.parquet
  built once by scripts/build_xs_research_panel.py from the ambush dataset
  (2025-07 .. 2026-07). Development + holdout live here.
* forward archive data/research/xs_panel/weekly_archive/*.parquet, which
  scripts/generate_pump_watchlist.py appends every Monday. Only archives
  captured after a formula was frozen count as independent evidence, and each
  archive is judged on its own capture-day contents (load_archive_vintages):
  a later archive restates ~a year of history, so the stitched panel can
  show values that were not visible on an earlier signal day.

Labels follow the validated protocol (docs/AMBUSH_MODES_BACKTEST_REPORT_2026-07-18.md):
Monday snapshots, market cap in [2e6, 1.5e9], pump100 = max close over the
next 30 days >= 2x today's close.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
# Provenance written with each archive since schema 2. Schema-1 archives lack
# it; their capture day is the file name (the writer's UTC date).
ARCHIVE_SCHEMA_VERSION = 2
ARCHIVE_META_COLUMNS = ["captured_at", "oi_source", "mcap_source", "schema_version"]


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


def stitch_weekly_archive(archive_dir: Optional[Path] = None) -> pd.DataFrame:
    """Merge weekly snapshots into one daily panel; later snapshots win.

    Each snapshot carries ~365 days of history per coin but only a CURRENT
    market cap, so mcap is taken on each snapshot's own last day and
    forward-filled between snapshots instead of trusting the flat history.

    NOT point-in-time: fine for outcome labels (closes of finished bars) and
    live OI history, never for features on a past signal day.
    """
    files = sorted((archive_dir or WEEKLY_ARCHIVE_DIR).glob("*.parquet"))
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


def load_archive_vintages(archive_dir: Optional[Path] = None) -> Tuple[List[Dict[str, object]], List[Dict[str, str]]]:
    """Each weekly archive as it was captured: point-in-time data for its last day.

    An archive is accepted only when its capture day is provable (the
    ``captured_at`` column, or for schema-1 files the file name) and its last
    closed bar is the day before capture. Coins whose own last bar lags that
    day are dropped from the vintage. Everything else fails closed: rejected
    archives are returned with a reason and never enter a verdict.
    """
    accepted: List[Dict[str, object]] = []
    rejected: List[Dict[str, str]] = []
    for path in sorted((archive_dir or WEEKLY_ARCHIVE_DIR).glob("*.parquet")):
        try:
            file_day = pd.to_datetime(path.stem, format="%Y-%m-%d")
        except ValueError:
            rejected.append({"file": path.name, "reason": "unparseable_name"})
            continue
        try:
            snap = pd.read_parquet(path)
        except Exception:  # noqa: BLE001 - a truncated write must not stop the verdict pass
            rejected.append({"file": path.name, "reason": "unreadable"})
            continue
        if snap.empty:
            rejected.append({"file": path.name, "reason": "empty"})
            continue
        snap["date"] = pd.to_datetime(snap["date"])
        if "captured_at" in snap:
            # Earliest fetch of the run: conservative against a freeze during the run.
            captured = pd.to_datetime(snap["captured_at"], utc=True).min().tz_localize(None)
            if pd.isna(captured) or captured.normalize() != file_day:
                rejected.append({"file": path.name, "reason": "capture_time_mismatch"})
                continue
        else:
            captured = file_day  # start of the write day: earliest possible capture
        signal_date = snap["date"].max()
        if captured.normalize() - signal_date != pd.Timedelta(days=1):
            rejected.append({"file": path.name, "reason": "last_bar_not_previous_day"})
            continue
        last_bar = snap.groupby("base")["date"].max()
        current = last_bar.index[last_bar == signal_date]
        frame = snap[snap["base"].isin(current)][PANEL_COLUMNS].reset_index(drop=True)
        accepted.append({
            "file": path.name, "captured_at": captured, "signal_date": signal_date, "frame": frame,
            "coins": int(len(current)), "lagging_coins_dropped": int(len(last_bar) - len(current)),
        })
    return accepted, rejected


def vintage_labels(vintages: List[Dict[str, object]]) -> pd.DataFrame:
    """Outcome labels (base, date, fwd30_maxret) from the accepted archives' closes.

    Later archives win: closes of finished bars are outcomes, not features,
    so a restatement cannot leak into a signal.
    """
    if not vintages:
        return pd.DataFrame(columns=["base", "date", "fwd30_maxret"])
    closes = pd.concat(
        [v["frame"][["base", "date", "close"]].assign(_order=i) for i, v in enumerate(vintages)], ignore_index=True,
    )
    closes = closes.sort_values("_order").drop_duplicates(["base", "date"], keep="last").drop(columns="_order")
    return add_forward_labels(closes)[["base", "date", "fwd30_maxret"]]


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
