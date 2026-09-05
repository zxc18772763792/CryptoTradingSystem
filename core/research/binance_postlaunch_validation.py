"""Time-safe post-launch entry location helpers.

Every non-immediate rule observes a completed 4h trigger bar and enters only at
the next 4h open.  The module is research-only and has no execution imports.
"""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd


POSTLAUNCH_ENTRY_RULES: dict[str, dict[str, str]] = {
    "launch_open": {"family": "baseline", "description": "enter at the 24h checkpoint open"},
    "wait4h": {"family": "wait", "description": "observe one more completed 4h bar"},
    "discount5_touch": {"family": "discount", "description": "5% discount touched; enter next open"},
    "discount10_touch": {"family": "discount", "description": "10% discount touched; enter next open"},
    "discount5_reclaim": {"family": "reclaim", "description": "5% discount then positive two-close reclaim"},
    "discount10_reclaim": {"family": "reclaim", "description": "10% discount then positive two-close reclaim"},
    "controlled_pullback5": {"family": "reclaim", "description": "5% pullback, reclaim discount level, limited chase"},
    "breakout6": {"family": "breakout", "description": "fresh six-bar close breakout with limited chase"},
    "prelaunch_high_break": {"family": "breakout", "description": "close above the preceding 24h path high"},
}


def _utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def _exact_index(rows: pd.DataFrame, when: pd.Timestamp) -> int | None:
    times = pd.to_datetime(rows["open_time"], utc=True)
    values = times.dt.as_unit("ns").astype("int64").to_numpy()
    index = int(np.searchsorted(values, when.value, side="left"))
    if index >= len(rows) or pd.Timestamp(times.iloc[index]) != when:
        return None
    return index


def locate_postlaunch_entry(
    signal: Mapping[str, Any],
    bars: pd.DataFrame,
    *,
    rule_name: str,
    trigger_window_hours: int = 48,
) -> dict[str, Any] | None:
    """Locate one post-launch entry and recompute its actual 14d outcome."""

    if rule_name not in POSTLAUNCH_ENTRY_RULES:
        raise ValueError(f"unknown post-launch entry rule: {rule_name}")
    rows = bars.sort_values("open_time").reset_index(drop=True).copy()
    rows["open_time"] = pd.to_datetime(rows["open_time"], utc=True)
    for column in ["open", "high", "low", "close"]:
        rows[column] = pd.to_numeric(rows[column], errors="coerce")
    decision_time = _utc(signal["decision_time"])
    start = _exact_index(rows, decision_time)
    if start is None:
        return None
    decision_open = float(signal["decision_open"])
    baseline_open = float(signal["baseline_entry_open"])
    prelaunch_high = baseline_open * (1.0 + float(signal["early_mfe"]))
    trigger_index: int | None = None

    if rule_name == "launch_open":
        entry_index = start
    elif rule_name == "wait4h":
        if start + 1 >= len(rows):
            return None
        trigger_index = start
        entry_index = start + 1
    else:
        max_trigger_time = decision_time + pd.Timedelta(hours=trigger_window_hours)
        touched5 = False
        touched10 = False
        entry_index = -1
        for index in range(start, len(rows) - 1):
            row = rows.iloc[index]
            when = pd.Timestamp(row["open_time"])
            if when >= max_trigger_time:
                break
            low_return = float(row["low"] / decision_open - 1.0)
            close_return = float(row["close"] / decision_open - 1.0)
            bar_return = float(row["close"] / row["open"] - 1.0)
            touched5 = bool(touched5 or low_return <= -0.05)
            touched10 = bool(touched10 or low_return <= -0.10)
            prior_close = float(rows.iloc[index - 1]["close"]) if index > 0 else np.nan
            prior_high6 = float(rows.iloc[max(0, index - 6):index]["high"].max()) if index > 0 else np.nan
            fires = False
            if rule_name == "discount5_touch":
                fires = low_return <= -0.05
            elif rule_name == "discount10_touch":
                fires = low_return <= -0.10
            elif rule_name == "discount5_reclaim":
                fires = touched5 and bar_return > 0 and float(row["close"]) > prior_close
            elif rule_name == "discount10_reclaim":
                fires = touched10 and bar_return > 0 and float(row["close"]) > prior_close
            elif rule_name == "controlled_pullback5":
                fires = touched5 and bar_return > 0 and close_return >= -0.05 and close_return <= 0.10
            elif rule_name == "breakout6":
                fires = (
                    np.isfinite(prior_high6) and float(row["close"]) > prior_high6
                    and 0.0 < bar_return <= 0.10 and close_return <= 0.25
                )
            elif rule_name == "prelaunch_high_break":
                fires = float(row["close"]) > prelaunch_high and 0.0 < bar_return <= 0.12
            if fires:
                trigger_index = index
                entry_index = index + 1
                break
        if entry_index < 0:
            return None

    entry = rows.iloc[entry_index]
    entry_time = pd.Timestamp(entry["open_time"])
    entry_open = float(entry["open"])
    horizon_end = entry_time + pd.Timedelta(days=14)
    path = rows[(rows["open_time"] >= entry_time) & (rows["open_time"] <= horizon_end)]
    if path.empty or pd.Timestamp(path.iloc[-1]["open_time"]) < horizon_end - pd.Timedelta(hours=4):
        return None
    max_return = float(path["high"].max() / entry_open - 1.0)
    target = path[path["high"] >= entry_open * 3.0]
    first_target_hours = None
    if not target.empty:
        first_target_hours = float(
            (pd.Timestamp(target.iloc[0]["open_time"]) - entry_time).total_seconds() / 3600.0
        )
    trigger = rows.iloc[trigger_index] if trigger_index is not None else None
    return {
        "postlaunch_entry_rule": rule_name,
        "postlaunch_entry_family": POSTLAUNCH_ENTRY_RULES[rule_name]["family"],
        "decision_time": decision_time,
        "decision_open": decision_open,
        "trigger_time": None if trigger is None else pd.Timestamp(trigger["open_time"]) + pd.Timedelta(hours=4),
        "entry_time": entry_time,
        "entry_open": entry_open,
        "entry_delay_hours_after_launch": float((entry_time - decision_time).total_seconds() / 3600.0),
        "entry_return_vs_decision_open": float(entry_open / decision_open - 1.0),
        "trigger_low_return": None if trigger is None else float(trigger["low"] / decision_open - 1.0),
        "trigger_close_return": None if trigger is None else float(trigger["close"] / decision_open - 1.0),
        "future_max_return_14d_from_entry": max_return,
        "target200_14d_from_entry": bool(max_return >= 2.0),
        "first_target_hours_from_entry": first_target_hours,
    }
