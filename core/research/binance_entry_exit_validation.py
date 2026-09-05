"""Time-safe entry confirmation and stateful exit helpers for Binance run-up research.

The module is deliberately research-only.  It has no order-routing imports and
uses only information available at a completed 4h bar.  Confirmation rules
therefore enter at the following 4h open.  Stateful exits also activate stops
or discretionary exits no earlier than the next bar after their trigger.
"""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd


ENTRY_RULES: dict[str, dict[str, Any]] = {
    "next_open": {"family": "baseline", "description": "next eligible 4h open"},
    "one_bar_wait": {"family": "baseline", "description": "observe one completed 4h bar and enter at the following open"},
    "breakout_balanced": {"family": "breakout", "description": "6-bar breakout with moderate demand and chase cap"},
    "breakout_strict": {"family": "breakout", "description": "12-bar breakout with stronger volume and tighter chase cap"},
    "volume_demand": {"family": "demand", "description": "volume, taker demand and EMA alignment"},
    "retest5_reclaim": {"family": "retest", "description": "reclaim after at least a 5% post-signal retracement"},
    "retest10_reclaim": {"family": "retest", "description": "reclaim after at least a 10% post-signal retracement"},
    "discount5_reclaim": {"family": "retest", "description": "reclaim after trading 5% below the signal close"},
    "discount10_reclaim": {"family": "retest", "description": "reclaim after trading 10% below the signal close"},
    "compression_breakout": {"family": "compression", "description": "range compression followed by a volume-backed 6-bar breakout"},
    "trend_reclaim": {"family": "trend", "description": "EMA18 reclaim with short EMA confirmation and chase cap"},
}


def build_exit_policy_catalog() -> dict[str, dict[str, Any]]:
    """Return a small pre-registered stateful exit family.

    Every policy retains the 20% catastrophic stop and 30-day maximum hold.
    The catalog changes only failure-to-launch, break-even, momentum failure,
    and right-tail profit-management behaviour.
    """

    base = {
        "family": "frozen",
        "hard_stop": 0.20,
        "time_days": 30,
        "partial_target": 1.00,
        "partial_fraction": 0.50,
        "trail_activation": 1.00,
        "trail_distance": 0.25,
        "launch_deadline_days": None,
        "launch_mfe_required": None,
        "breakeven_activation": None,
        "momentum_activation": None,
        "momentum_drawdown": None,
    }
    policies: dict[str, dict[str, Any]] = {"frozen_half100_trail25": dict(base)}

    def add(name: str, family: str, **updates: Any) -> None:
        policy = dict(base)
        policy.update(updates)
        policy["family"] = family
        policies[name] = policy

    add("launch3_mfe10", "failure_to_launch", launch_deadline_days=3, launch_mfe_required=0.10)
    add("launch3_mfe20", "failure_to_launch", launch_deadline_days=3, launch_mfe_required=0.20)
    add("launch5_mfe20", "failure_to_launch", launch_deadline_days=5, launch_mfe_required=0.20)
    add("breakeven30", "break_even", breakeven_activation=0.30)
    add("breakeven50", "break_even", breakeven_activation=0.50)
    add(
        "launch3_mfe10_breakeven30",
        "failure_and_break_even",
        launch_deadline_days=3,
        launch_mfe_required=0.10,
        breakeven_activation=0.30,
    )
    add(
        "launch5_mfe20_breakeven30",
        "failure_and_break_even",
        launch_deadline_days=5,
        launch_mfe_required=0.20,
        breakeven_activation=0.30,
    )
    add(
        "momentum30_ema6_dd10",
        "momentum_failure",
        momentum_activation=0.30,
        momentum_drawdown=0.10,
    )
    add(
        "launch3_mfe10_momentum30",
        "failure_and_momentum",
        launch_deadline_days=3,
        launch_mfe_required=0.10,
        momentum_activation=0.30,
        momentum_drawdown=0.10,
    )
    add(
        "half50_trail20",
        "faster_profit_lock",
        partial_target=0.50,
        trail_activation=0.50,
        trail_distance=0.20,
    )
    return policies


def add_past_only_4h_features(bars: pd.DataFrame) -> pd.DataFrame:
    """Add confirmation features whose rolling references exclude the current bar."""

    rows = bars.sort_values("open_time").copy()
    rows["open_time"] = pd.to_datetime(rows["open_time"], utc=True)
    numeric = ["open", "high", "low", "close", "quote_volume", "taker_buy_quote"]
    for column in numeric:
        rows[column] = pd.to_numeric(rows[column], errors="coerce")
    rows["bar_return"] = rows["close"] / rows["open"] - 1.0
    rows["range_pct"] = rows["high"] / rows["low"].replace(0, np.nan) - 1.0
    rows["taker_buy_share"] = rows["taker_buy_quote"] / rows["quote_volume"].replace(0, np.nan)
    rows["taker_buy_share_3"] = rows["taker_buy_share"].rolling(3, min_periods=2).mean()
    prior_volume = rows["quote_volume"].shift(1).rolling(18, min_periods=6).median()
    rows["volume_ratio_18"] = rows["quote_volume"] / prior_volume.replace(0, np.nan)
    rows["ema6"] = rows["close"].ewm(span=6, adjust=False, min_periods=3).mean()
    rows["ema18"] = rows["close"].ewm(span=18, adjust=False, min_periods=8).mean()
    rows["prior_close"] = rows["close"].shift(1)
    rows["prior_ema18"] = rows["ema18"].shift(1)
    rows["prior_high_1"] = rows["high"].shift(1)
    rows["prior_high_6"] = rows["high"].shift(1).rolling(6, min_periods=4).max()
    rows["prior_high_12"] = rows["high"].shift(1).rolling(12, min_periods=8).max()
    rows["range_mean_3_prior"] = rows["range_pct"].shift(1).rolling(3, min_periods=3).mean()
    rows["range_mean_18_prior"] = rows["range_pct"].shift(1).rolling(18, min_periods=8).mean()
    return rows.reset_index(drop=True)


def _finite(value: Any) -> bool:
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def _entry_rule_fires(
    rule_name: str,
    row: pd.Series,
    *,
    signal_close: float,
    seen_peak_drawdown: float,
    seen_signal_discount: float,
) -> bool:
    close = float(row["close"])
    chase = close / signal_close - 1.0
    bar_return = float(row["bar_return"])
    volume = float(row["volume_ratio_18"]) if _finite(row["volume_ratio_18"]) else np.nan
    taker = float(row["taker_buy_share_3"]) if _finite(row["taker_buy_share_3"]) else np.nan
    ema6 = float(row["ema6"]) if _finite(row["ema6"]) else np.nan
    ema18 = float(row["ema18"]) if _finite(row["ema18"]) else np.nan
    prior_high_1 = float(row["prior_high_1"]) if _finite(row["prior_high_1"]) else np.nan
    prior_high_6 = float(row["prior_high_6"]) if _finite(row["prior_high_6"]) else np.nan
    prior_high_12 = float(row["prior_high_12"]) if _finite(row["prior_high_12"]) else np.nan

    if rule_name == "breakout_balanced":
        return bool(
            close > prior_high_6 and 0.01 <= bar_return <= 0.12
            and volume >= 1.25 and taker >= 0.50 and chase <= 0.25
        )
    if rule_name == "breakout_strict":
        return bool(
            close > prior_high_12 and 0.02 <= bar_return <= 0.10
            and volume >= 1.50 and taker >= 0.52 and chase <= 0.20
        )
    if rule_name == "volume_demand":
        return bool(
            close > ema6 > ema18 and volume >= 1.50 and taker >= 0.55
            and bar_return >= 0.0 and chase <= 0.20
        )
    if rule_name in {"retest5_reclaim", "retest10_reclaim"}:
        threshold = -0.05 if rule_name == "retest5_reclaim" else -0.10
        return bool(
            seen_peak_drawdown <= threshold and close > prior_high_1 and close > ema6
            and 0.01 <= bar_return <= 0.10 and chase <= 0.20
        )
    if rule_name in {"discount5_reclaim", "discount10_reclaim"}:
        threshold = -0.05 if rule_name == "discount5_reclaim" else -0.10
        chase_cap = 0.15 if rule_name == "discount5_reclaim" else 0.10
        return bool(
            seen_signal_discount <= threshold and close > prior_high_1 and close > ema6
            and 0.01 <= bar_return <= 0.10 and chase <= chase_cap
        )
    if rule_name == "compression_breakout":
        short_range = row["range_mean_3_prior"]
        long_range = row["range_mean_18_prior"]
        return bool(
            _finite(short_range) and _finite(long_range) and float(long_range) > 0
            and float(short_range) / float(long_range) <= 0.75
            and close > prior_high_6 and volume >= 1.25 and 0.0 <= bar_return <= 0.12
            and chase <= 0.20
        )
    if rule_name == "trend_reclaim":
        prior_close = row["prior_close"]
        prior_ema18 = row["prior_ema18"]
        return bool(
            _finite(prior_close) and _finite(prior_ema18)
            and float(prior_close) <= float(prior_ema18)
            and close > ema18 and ema6 >= ema18 and volume >= 1.0
            and 0.0 <= bar_return <= 0.10 and chase <= 0.15
        )
    raise ValueError(f"Unknown entry rule: {rule_name}")


def locate_entry(
    signal: Mapping[str, Any],
    bars: pd.DataFrame,
    *,
    rule_name: str,
    confirmation_days: int = 5,
) -> dict[str, Any] | None:
    """Locate the first time-safe entry for one daily signal and one rule."""

    if rule_name not in ENTRY_RULES:
        raise ValueError(f"Unknown entry rule: {rule_name}")
    baseline_time = pd.Timestamp(signal["entry_time"])
    if baseline_time.tzinfo is None:
        baseline_time = baseline_time.tz_localize("UTC")
    times = pd.to_datetime(bars["open_time"], utc=True)
    # Pandas may store timezone-aware timestamps at microsecond resolution.
    # Normalize to nanoseconds before comparing with Timestamp.value.
    time_ns = times.dt.as_unit("ns").astype("int64").to_numpy()
    start = int(np.searchsorted(time_ns, baseline_time.value, side="left"))
    if start >= len(bars) or times.iloc[start] > baseline_time + pd.Timedelta(hours=4):
        return None
    signal_close = float(signal["close"])

    if rule_name in {"next_open", "one_bar_wait"}:
        if rule_name == "one_bar_wait" and start + 1 >= len(bars):
            return None
        entry_index = start if rule_name == "next_open" else start + 1
        trigger_index: int | None = None if rule_name == "next_open" else start
    else:
        last_trigger_time = baseline_time + pd.Timedelta(days=confirmation_days)
        seen_peak = signal_close
        seen_peak_drawdown = 0.0
        seen_signal_discount = 0.0
        entry_index = -1
        trigger_index = None
        for index in range(start, len(bars) - 1):
            row = bars.iloc[index]
            bar_time = pd.Timestamp(row["open_time"])
            if bar_time > last_trigger_time:
                break
            if _entry_rule_fires(
                rule_name,
                row,
                signal_close=signal_close,
                seen_peak_drawdown=seen_peak_drawdown,
                seen_signal_discount=seen_signal_discount,
            ):
                trigger_index = index
                entry_index = index + 1
                break
            seen_peak = max(seen_peak, float(row["high"]))
            seen_peak_drawdown = min(seen_peak_drawdown, float(row["low"]) / seen_peak - 1.0)
            seen_signal_discount = min(seen_signal_discount, float(row["low"]) / signal_close - 1.0)
        if entry_index < 0:
            return None

    entry = bars.iloc[entry_index]
    entry_time = pd.Timestamp(entry["open_time"])
    entry_open = float(entry["open"])
    horizon_end = entry_time + pd.Timedelta(days=14)
    path = bars[(times >= entry_time) & (times <= horizon_end)]
    if path.empty:
        return None
    max_return = float(pd.to_numeric(path["high"], errors="coerce").max() / entry_open - 1.0)
    target_rows = path[pd.to_numeric(path["high"], errors="coerce") >= entry_open * 3.0]
    first_target_hours = None
    if not target_rows.empty:
        first_target_hours = float(
            (pd.Timestamp(target_rows.iloc[0]["open_time"]) - entry_time).total_seconds() / 3600.0
        )
    trigger = bars.iloc[trigger_index] if trigger_index is not None else None
    return {
        "entry_rule": rule_name,
        "entry_family": ENTRY_RULES[rule_name]["family"],
        "baseline_entry_time": baseline_time,
        "trigger_time": None if trigger is None else pd.Timestamp(trigger["open_time"]) + pd.Timedelta(hours=4),
        "entry_time": entry_time,
        "entry_open": entry_open,
        "entry_delay_hours": float((entry_time - baseline_time).total_seconds() / 3600.0),
        "entry_chase_vs_signal_close": float(entry_open / signal_close - 1.0),
        "trigger_bar_return": None if trigger is None else float(trigger["bar_return"]),
        "trigger_volume_ratio": None if trigger is None or not _finite(trigger["volume_ratio_18"]) else float(trigger["volume_ratio_18"]),
        "trigger_taker_buy_share_3": None if trigger is None or not _finite(trigger["taker_buy_share_3"]) else float(trigger["taker_buy_share_3"]),
        "trigger_range_pct": None if trigger is None or not _finite(trigger["range_pct"]) else float(trigger["range_pct"]),
        "trigger_close_vs_ema18": None if trigger is None or not _finite(trigger["ema18"]) else float(trigger["close"] / trigger["ema18"] - 1.0),
        "trigger_close_vs_prior_high6": None if trigger is None or not _finite(trigger["prior_high_6"]) else float(trigger["close"] / trigger["prior_high_6"] - 1.0),
        "future_max_return_14d_from_entry": max_return,
        "target200_14d_from_entry": bool(max_return >= 2.0),
        "first_target_hours_from_entry": first_target_hours,
    }


def simulate_stateful_trade(
    bars: pd.DataFrame,
    *,
    entry_time: pd.Timestamp,
    policy: Mapping[str, Any],
    slippage_bps: float,
    funding: pd.Series | None = None,
    fee_bps: float = 5.0,
    include_marks: bool = False,
    policy_switch_time: pd.Timestamp | None = None,
    post_switch_policy: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Simulate a long trade with conservative 4h state transitions.

    An optional policy switch is applied only at or after the first 4h open at
    ``policy_switch_time``.  State already created by earlier bars (partial
    fills, trailing activation, and peak price) is preserved, so a completed
    trigger bar cannot retroactively change its own execution.
    """

    if (policy_switch_time is None) != (post_switch_policy is None):
        raise ValueError("policy_switch_time and post_switch_policy must be provided together")

    rows = bars if "ema6" in bars.columns else add_past_only_4h_features(bars)
    entry_time = pd.Timestamp(entry_time)
    if entry_time.tzinfo is None:
        entry_time = entry_time.tz_localize("UTC")
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
    hard_stop = entry_price * (1.0 - float(policy["hard_stop"]))
    current_policy: Mapping[str, Any] = policy
    switch_time: pd.Timestamp | None = None
    if policy_switch_time is not None:
        switch_time = pd.Timestamp(policy_switch_time)
        switch_time = switch_time.tz_localize("UTC") if switch_time.tzinfo is None else switch_time.tz_convert("UTC")
    policy_switch_applied = False
    partial_done_before_switch = False
    trail_active_before_switch = False
    remaining = 1.0
    realized_return = -fee
    exit_fees = 0.0
    funding_return = 0.0
    peak_price = entry_price
    trail_active = False
    breakeven_active = False
    partial_done = False
    scheduled_exit: str | None = None
    launch_checked = False
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

        if (
            not policy_switch_applied
            and switch_time is not None
            and bar_time >= switch_time
        ):
            partial_done_before_switch = bool(partial_done)
            trail_active_before_switch = bool(trail_active)
            current_policy = post_switch_policy or policy
            policy_switch_applied = True

        forced_exit_after_hours = current_policy.get("forced_exit_after_hours")
        if (
            remaining > 0
            and forced_exit_after_hours is not None
            and bar_time >= entry_time + pd.Timedelta(hours=float(forced_exit_after_hours))
        ):
            fill = bar_open * (1.0 - slip)
            realized_return += remaining * (fill / entry_price - 1.0)
            exit_fees += remaining * fee
            remaining = 0.0
            exit_time = bar_time
            exit_price = fill
            exit_reason = str(current_policy.get("forced_exit_reason") or "forced_time_exit")
            if include_marks:
                mark_path.append({"time": bar_time, "mark_return": realized_return + funding_return - exit_fees, "remaining_fraction": 0.0})
            break

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

        active_stop = hard_stop
        stop_reason = "hard_stop"
        if breakeven_active and entry_price > active_stop:
            active_stop = entry_price
            stop_reason = "break_even_stop"
        if trail_active:
            trailing = peak_price * (1.0 - float(current_policy["trail_distance"]))
            if trailing > active_stop:
                active_stop = trailing
                stop_reason = "trailing_stop"
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
            target = current_policy.get("partial_target")
            if target is not None and not partial_done and bar_high >= entry_price * (1.0 + float(target)):
                target_fill = entry_price * (1.0 + float(target)) * (1.0 - slip)
                fraction = min(float(current_policy.get("partial_fraction", 0.5)), remaining)
                realized_return += fraction * (target_fill / entry_price - 1.0)
                exit_fees += fraction * fee
                remaining -= fraction
                partial_done = True

            peak_price = max(peak_price, bar_high)
            if current_policy.get("trail_activation") is not None and peak_price >= entry_price * (1.0 + float(current_policy["trail_activation"])):
                trail_active = True
            if current_policy.get("breakeven_activation") is not None and peak_price >= entry_price * (1.0 + float(current_policy["breakeven_activation"])):
                breakeven_active = True

            exhaustion_activation = current_policy.get("exhaustion_activation")
            upper_wick_min = current_policy.get("exhaustion_upper_wick_min")
            close_location_max = current_policy.get("exhaustion_close_location_max")
            volume_ratio_min = current_policy.get("exhaustion_volume_ratio_min")
            bar_range = bar_high - bar_low
            upper_wick_share = (
                (bar_high - max(bar_open, bar_close)) / bar_range if bar_range > 0 else 0.0
            )
            close_location = (bar_close - bar_low) / bar_range if bar_range > 0 else 0.5
            volume_ratio = bar.get("volume_ratio_18")
            if (
                remaining > 0 and scheduled_exit is None
                and exhaustion_activation is not None
                and peak_price >= entry_price * (1.0 + float(exhaustion_activation))
                and upper_wick_min is not None and upper_wick_share >= float(upper_wick_min)
                and close_location_max is not None and close_location <= float(close_location_max)
                and volume_ratio_min is not None and _finite(volume_ratio)
                and float(volume_ratio) >= float(volume_ratio_min)
            ):
                scheduled_exit = "blowoff_exhaustion"

            elapsed_days = (bar_time + pd.Timedelta(hours=4) - entry_time).total_seconds() / 86_400.0
            deadline = current_policy.get("launch_deadline_days")
            required_mfe = current_policy.get("launch_mfe_required")
            if (
                remaining > 0 and not launch_checked and deadline is not None
                and elapsed_days >= float(deadline)
            ):
                launch_checked = True
                if peak_price / entry_price - 1.0 < float(required_mfe):
                    scheduled_exit = "failure_to_launch"

            momentum_activation = current_policy.get("momentum_activation")
            momentum_drawdown = current_policy.get("momentum_drawdown")
            momentum_taker_buy_max = current_policy.get("momentum_taker_buy_max")
            taker_buy_share = bar.get("taker_buy_share_3")
            if (
                remaining > 0 and scheduled_exit is None and momentum_activation is not None
                and peak_price >= entry_price * (1.0 + float(momentum_activation))
                and bar_close / peak_price - 1.0 <= -float(momentum_drawdown)
                and _finite(bar.get("ema6")) and bar_close < float(bar["ema6"])
                and (
                    momentum_taker_buy_max is None
                    or (_finite(taker_buy_share) and float(taker_buy_share) <= float(momentum_taker_buy_max))
                )
            ):
                scheduled_exit = "demand_fade" if momentum_taker_buy_max is not None else "momentum_failure"

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
        "partial_fraction": float(current_policy.get("partial_fraction", 0.5)) if partial_done else 0.0,
        "full_take_profit": False,
        "policy_family": str(current_policy.get("family") or "stateful"),
        "policy_switch_time": switch_time,
        "policy_switch_applied": bool(policy_switch_applied),
        "partial_done_before_switch": bool(partial_done_before_switch),
        "trail_active_before_switch": bool(trail_active_before_switch),
        "funding_observed": bool(not funding_series.empty),
        "mark_path": mark_path,
    }


def wilson_lower_bound(positives: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    p = positives / total
    denominator = 1.0 + z * z / total
    centre = p + z * z / (2.0 * total)
    margin = z * np.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
    return float((centre - margin) / denominator)
