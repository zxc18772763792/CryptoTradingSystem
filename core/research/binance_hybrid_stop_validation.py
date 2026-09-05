"""Time-safe hybrid stop research for post-launch long positions.

The initial risk control combines a catastrophic intrabar stop with one or more
completed 4h closes below an entry-relative threshold.  A close trigger exits
only at the next 4h open.  Once break-even or trailing protection activates,
those protective stops retain conservative intrabar execution.
"""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

from core.research.binance_entry_exit_validation import add_past_only_4h_features


def _utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def excursion_before_target(
    bars: pd.DataFrame,
    *,
    entry_time: pd.Timestamp,
    target_return: float,
    horizon_days: int = 14,
) -> dict[str, Any] | None:
    """Measure adverse path before a target using prior-only and inclusive bars."""

    rows = bars.sort_values("open_time").reset_index(drop=True).copy()
    rows["open_time"] = pd.to_datetime(rows["open_time"], utc=True)
    when = _utc(entry_time)
    path = rows[(rows["open_time"] >= when) & (rows["open_time"] <= when + pd.Timedelta(days=horizon_days))]
    if path.empty or pd.Timestamp(path.iloc[0]["open_time"]) > when + pd.Timedelta(hours=4):
        return None
    entry_open = float(path.iloc[0]["open"])
    target = entry_open * (1.0 + float(target_return))
    hits = path[pd.to_numeric(path["high"], errors="coerce") >= target]
    if hits.empty:
        return {
            "target_return": float(target_return),
            "target_hit": False,
            "hours_to_target": None,
            "prior_bar_intrabar_mae": None,
            "inclusive_intrabar_mae": None,
            "prior_bar_close_mae": None,
            "inclusive_close_mae": None,
        }
    hit_index = int(hits.index[0])
    first_index = int(path.index[0])
    prior = rows.loc[first_index : hit_index - 1]
    inclusive = rows.loc[first_index:hit_index]

    def minimum_return(frame: pd.DataFrame, column: str) -> float | None:
        if frame.empty:
            return 0.0
        values = pd.to_numeric(frame[column], errors="coerce").dropna()
        return None if values.empty else float(values.min() / entry_open - 1.0)

    target_time = pd.Timestamp(rows.loc[hit_index, "open_time"])
    return {
        "target_return": float(target_return),
        "target_hit": True,
        "hours_to_target": float((target_time - when).total_seconds() / 3600.0),
        "prior_bar_intrabar_mae": minimum_return(prior, "low"),
        "inclusive_intrabar_mae": minimum_return(inclusive, "low"),
        "prior_bar_close_mae": minimum_return(prior, "close"),
        "inclusive_close_mae": minimum_return(inclusive, "close"),
    }


def simulate_hybrid_stop_trade(
    bars: pd.DataFrame,
    *,
    entry_time: pd.Timestamp,
    policy: Mapping[str, Any],
    slippage_bps: float,
    funding: pd.Series | None = None,
    fee_bps: float = 5.0,
    include_marks: bool = False,
) -> dict[str, Any] | None:
    """Simulate a close-confirmed initial stop with a catastrophic floor."""

    rows = bars if "ema6" in bars.columns else add_past_only_4h_features(bars)
    entry_time = _utc(entry_time)
    candidates = rows[rows["open_time"] >= entry_time]
    if candidates.empty or pd.Timestamp(candidates.iloc[0]["open_time"]) > entry_time + pd.Timedelta(hours=4):
        return None
    start_index = int(candidates.index[0])
    horizon = entry_time + pd.Timedelta(days=int(policy["time_days"]))
    path = rows.loc[start_index:]
    path = path[path["open_time"] <= horizon]
    if path.empty:
        return None

    slip = float(slippage_bps) / 10_000.0
    fee = float(fee_bps) / 10_000.0
    entry_price = float(path.iloc[0]["open"]) * (1.0 + slip)
    catastrophic_price = entry_price * (1.0 - float(policy["catastrophic_stop"]))
    close_threshold = entry_price * (1.0 - float(policy["close_stop"]))
    close_bars_required = int(policy.get("close_stop_bars", 1))
    remaining = 1.0
    realized_return = -fee
    exit_fees = 0.0
    funding_return = 0.0
    peak_price = entry_price
    trail_active = False
    breakeven_active = False
    partial_done = False
    close_breach_count = 0
    scheduled_exit: str | None = None
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason = "time_stop"
    mfe = -np.inf
    mae = np.inf
    mark_path: list[dict[str, Any]] = []
    funding_series = funding.copy() if funding is not None else pd.Series(dtype=float)
    if not funding_series.empty:
        funding_series.index = pd.to_datetime(funding_series.index, utc=True)
        funding_series = pd.to_numeric(funding_series, errors="coerce").dropna().sort_index()
    previous_time: pd.Timestamp | None = None

    for _, bar in path.iterrows():
        bar_time = pd.Timestamp(bar["open_time"])
        bar_open = float(bar["open"])
        bar_high = float(bar["high"])
        bar_low = float(bar["low"])
        bar_close = float(bar["close"])

        if scheduled_exit is not None:
            fill = bar_open * (1.0 - slip)
            realized_return += remaining * (fill / entry_price - 1.0)
            exit_fees += remaining * fee
            remaining = 0.0
            exit_time = bar_time
            exit_price = fill
            exit_reason = scheduled_exit
            if include_marks:
                mark_path.append({"time": bar_time, "mark_return": realized_return + funding_return - exit_fees, "remaining_fraction": 0.0})
            break

        mfe = max(mfe, bar_high / entry_price - 1.0)
        mae = min(mae, bar_low / entry_price - 1.0)
        if not funding_series.empty:
            lower = previous_time if previous_time is not None else bar_time - pd.Timedelta(microseconds=1)
            charges = funding_series[(funding_series.index > lower) & (funding_series.index <= bar_time)]
            funding_return -= float(charges.sum()) * remaining

        active_stop = catastrophic_price
        stop_reason = "catastrophic_stop"
        if breakeven_active and entry_price > active_stop:
            active_stop = entry_price
            stop_reason = "break_even_stop"
        if trail_active:
            trailing = peak_price * (1.0 - float(policy["trail_distance"]))
            if trailing > active_stop:
                active_stop = trailing
                stop_reason = "trailing_stop"

        # Conservative same-bar ordering: protective loss exit precedes profit.
        if bar_low <= active_stop:
            raw_fill = bar_open if bar_open < active_stop else active_stop
            fill = raw_fill * (1.0 - slip)
            realized_return += remaining * (fill / entry_price - 1.0)
            exit_fees += remaining * fee
            remaining = 0.0
            exit_time = bar_time
            exit_price = fill
            exit_reason = stop_reason
        else:
            target = policy.get("partial_target")
            if target is not None and not partial_done and bar_high >= entry_price * (1.0 + float(target)):
                target_fill = entry_price * (1.0 + float(target)) * (1.0 - slip)
                fraction = min(float(policy.get("partial_fraction", 0.5)), remaining)
                realized_return += fraction * (target_fill / entry_price - 1.0)
                exit_fees += fraction * fee
                remaining -= fraction
                partial_done = True

            peak_price = max(peak_price, bar_high)
            if policy.get("trail_activation") is not None and peak_price >= entry_price * (1.0 + float(policy["trail_activation"])):
                trail_active = True
            if policy.get("breakeven_activation") is not None and peak_price >= entry_price * (1.0 + float(policy["breakeven_activation"])):
                breakeven_active = True

            if not breakeven_active and not trail_active:
                close_breach_count = close_breach_count + 1 if bar_close <= close_threshold else 0
                if close_breach_count >= close_bars_required:
                    scheduled_exit = "close_confirmed_stop"

        if include_marks:
            marked = realized_return + funding_return - exit_fees
            if remaining > 0:
                marked += remaining * (bar_close / entry_price - 1.0)
            mark_path.append({"time": bar_time, "mark_return": float(marked), "remaining_fraction": float(remaining)})
        previous_time = bar_time
        if remaining <= 0:
            break

    if remaining > 0:
        final = path.iloc[-1]
        exit_time = pd.Timestamp(final["open_time"]) + pd.Timedelta(hours=4)
        exit_price = float(final["close"]) * (1.0 - slip)
        realized_return += remaining * (exit_price / entry_price - 1.0)
        exit_fees += remaining * fee
        remaining = 0.0
        if include_marks:
            mark_path.append({"time": exit_time, "mark_return": float(realized_return + funding_return - exit_fees), "remaining_fraction": 0.0})

    return {
        "entry_time": pd.Timestamp(path.iloc[0]["open_time"]),
        "entry_price": float(entry_price),
        "exit_time": exit_time,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "net_return": float(realized_return + funding_return - exit_fees),
        "mfe": float(mfe),
        "mae": float(mae),
        "funding_return": float(funding_return),
        "fee_return": float(fee + exit_fees),
        "partial_take_profit": bool(partial_done),
        "partial_fraction": float(policy.get("partial_fraction", 0.0)) if partial_done else 0.0,
        "full_take_profit": False,
        "policy_family": str(policy.get("family", "hybrid_stop")),
        "funding_observed": bool(not funding_series.empty),
        "mark_path": mark_path,
    }
