"""Phase 3 fast_exit_arrays: NumPy-only execution simulator.

The trusted ``ExitEngine`` in ``core/backtest/exit_engine.py`` walks bars
with a Python loop calling pandas single-cell accessors (``.iloc[i]``)
hundreds of thousands of times per backtest. The Phase 0 profile of
MAStrategy 5m/1mo (8 639 bars) shows ``_simulate_execution_summary`` at
52.7 % of total runtime — second only to the strategy replay itself.

This module implements an array-only fast path that handles the
**simple-signal-following case**:

  * no initial stop (initial_stop_mode == "none")
  * no fixed stop loss / take profit
  * no breakeven
  * no partial take profit
  * no trailing stop
  * no time stop
  * signal_reversal_exit == True

That's the default config of ``_simulate_execution_summary`` when called
with ``use_stop_take=False`` and no exit template — which is the most
common backtest API call path. Any other config falls through to the
trusted ``run_exit_engine`` in the caller.

Inputs and outputs mirror what ``_simulate_execution_summary`` returns,
so the dispatcher can swap implementations transparently.

Locked by ``tests/test_execution_arrays_parity.py``: every supported
case is compared bar-by-bar against ``run_exit_engine`` on synthetic
fixtures (long/short/reversal/no-trade) — exact equality required on
effective_position / gross_returns / trade_stats counts / exit_reason
breakdown.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core.backtest.exit_engine import ExitEngineConfig


def is_supported_config(config: ExitEngineConfig) -> bool:
    """Return True iff the array path can faithfully simulate ``config``.

    Conservative: anything that touches a price level (stops, take
    profits, trailing) or a bar-count timer is delegated back to
    ``run_exit_engine`` to avoid silent parity drift.
    """
    if str(config.initial_stop_mode or "none").lower() != "none":
        return False
    if config.fixed_stop_loss_pct is not None and config.fixed_stop_loss_pct > 0:
        return False
    if config.fixed_take_profit_pct is not None and config.fixed_take_profit_pct > 0:
        return False
    if bool(config.breakeven_enabled):
        return False
    if bool(config.partial_take_profit_enabled):
        return False
    if str(config.trailing_mode or "none").lower() != "none":
        return False
    if bool(config.time_stop_enabled):
        return False
    if not bool(config.signal_reversal_exit):
        # Without signal_reversal_exit and no other exit mechanism, an
        # entry would never close — the trusted engine has its own
        # branches for that pathological case; defer.
        return False
    return True


def _signed_state_np(x: np.ndarray) -> np.ndarray:
    """Vectorized equivalent of ``exit_engine._signed_state``.

    NaN / 0 → 0.0; positive → 1.0; negative → -1.0.
    """
    out = np.zeros_like(x, dtype=float)
    finite = np.isfinite(x)
    pos = finite & (x > 0)
    neg = finite & (x < 0)
    out[pos] = 1.0
    out[neg] = -1.0
    return out


def simulate_execution_arrays(
    df: pd.DataFrame,
    signal_position: pd.Series,
    config: ExitEngineConfig,
) -> Dict[str, Any]:
    """Array fast path for the supported config subset.

    Returns the same dict shape as ``_simulate_execution_summary`` so the
    caller can use it interchangeably.
    """
    if not is_supported_config(config):
        raise ValueError(
            "simulate_execution_arrays called with an unsupported config; "
            "the caller must check is_supported_config and fall back to "
            "run_exit_engine."
        )

    # --- Normalize inputs to match the trusted engine's preprocessing ---
    bars = df.copy()
    bars.index = pd.to_datetime(bars.index)
    bars = bars.sort_index()
    index = bars.index
    n = len(index)

    if n == 0:
        empty = pd.Series([], index=index, dtype=float)
        return {
            "effective_position": empty,
            "gross_returns": empty,
            "turnover": empty,
            "trade_points": {
                "buy_points": [],
                "sell_points": [],
                "open_points": [],
                "close_points": [],
                "entries": 0,
                "exits": 0,
            },
            "trade_stats": {"entries": 0, "exits": 0, "completed": 0, "win_rate": 0.0},
            "protective_stats": {"forced_stop_exits": 0, "forced_take_exits": 0},
            "exit_reason_breakdown": {
                "stop": 0,
                "take_profit": 0,
                "trailing": 0,
                "reversal": 0,
                "partial": 0,
                "time_stop": 0,
            },
            "exit_events": [],
            "completed_trades": [],
            "exit_config": dict(config.to_dict()),
        }

    close_arr = pd.to_numeric(bars.get("close"), errors="coerce").reindex(index).to_numpy(dtype=float)
    high_arr = (
        pd.to_numeric(bars.get("high", bars.get("close")), errors="coerce")
        .reindex(index).fillna(pd.Series(close_arr, index=index)).to_numpy(dtype=float)
    )
    low_arr = (
        pd.to_numeric(bars.get("low", bars.get("close")), errors="coerce")
        .reindex(index).fillna(pd.Series(close_arr, index=index)).to_numpy(dtype=float)
    )

    raw = pd.to_numeric(signal_position.reindex(index), errors="coerce").fillna(0.0).to_numpy(dtype=float)
    raw_signed = _signed_state_np(raw)

    # === Single-pass state machine over NumPy arrays ===
    # The trusted engine's logic for this config reduces to:
    #
    #   for idx in range(n):
    #     if active and raw_signed[idx] crosses against active.direction:
    #         close at close[idx] (reason="reversal"); active = None
    #     elif active:
    #         bar_return = direction * (close[idx]/prev_close - 1)
    #     if not active:
    #         if raw_signed[idx] > prev_raw_signed: open long at close[idx]
    #         elif raw_signed[idx] < prev_raw_signed: open short
    #     effective_position[idx] = direction (1/-1) or 0 if flat
    #
    # The trusted code uses px_prev_close that falls back to px_close when
    # idx == 0 or prev is NaN; we reproduce that.

    effective_position = np.zeros(n, dtype=float)
    gross_returns = np.zeros(n, dtype=float)

    buy_points: List[Dict[str, Any]] = []
    sell_points: List[Dict[str, Any]] = []
    open_points: List[Dict[str, Any]] = []
    close_points: List[Dict[str, Any]] = []
    exit_events: List[Dict[str, Any]] = []
    completed_trades: List[Dict[str, Any]] = []
    exit_reason_counter: Counter[str] = Counter()

    active_direction = 0.0  # 0 = flat; +1 = long; -1 = short
    active_entry_idx = -1
    active_entry_price = 0.0
    active_entry_ts: Optional[pd.Timestamp] = None
    active_trade_id = 0
    active_mfe = 0.0
    active_mae = 0.0
    next_trade_id = 1

    entry_count = 0
    exit_count = 0
    completed_count = 0
    wins = 0

    raw_prev_signed_arr = np.concatenate(([0.0], raw_signed[:-1]))
    long_entry_signal = (raw_signed > 0) & (raw_prev_signed_arr <= 0)
    short_entry_signal = (raw_signed < 0) & (raw_prev_signed_arr >= 0)

    for idx in range(n):
        px_close = float(close_arr[idx])
        px_high = float(high_arr[idx]) if np.isfinite(high_arr[idx]) else px_close
        px_low = float(low_arr[idx]) if np.isfinite(low_arr[idx]) else px_close
        if idx > 0 and np.isfinite(close_arr[idx - 1]):
            px_prev_close = float(close_arr[idx - 1])
        else:
            px_prev_close = px_close

        if not np.isfinite(px_close) or px_close <= 0:
            # Mirror trusted: keep effective_position frozen at last active
            # (1.0 magnitude if in trade, else 0).
            effective_position[idx] = active_direction if active_direction != 0.0 else 0.0
            continue

        bar_return = 0.0

        # --- 1. Handle active trade: update excursions, check reversal exit ---
        if active_direction != 0.0:
            # update MFE/MAE (matches _ActiveTrade.update_excursions)
            if active_entry_price > 0:
                if active_direction > 0:
                    if np.isfinite(px_high):
                        active_mfe = max(active_mfe, (px_high / active_entry_price) - 1.0)
                    if np.isfinite(px_low):
                        active_mae = min(active_mae, (px_low / active_entry_price) - 1.0)
                else:
                    if np.isfinite(px_low):
                        active_mfe = max(active_mfe, 1.0 - (px_low / active_entry_price))
                    if np.isfinite(px_high):
                        active_mae = min(active_mae, 1.0 - (px_high / active_entry_price))

            # Reversal check: _raw_support_lost — long lost when raw <= 0;
            # short lost when raw >= 0.
            cur_signed = raw_signed[idx]
            reversal = (active_direction > 0 and cur_signed <= 0) or (
                active_direction < 0 and cur_signed >= 0
            )
            if reversal:
                # Close trade at px_close, reason="reversal"
                trade_ret = active_direction * ((px_close / px_prev_close) - 1.0) if px_prev_close > 0 else 0.0
                bar_return += trade_ret

                ts = index[idx]
                bars_in_trade = idx - active_entry_idx + 1
                gross_return_pct = active_direction * (
                    (px_close / active_entry_price) - 1.0
                ) * 100.0 if active_entry_price > 0 else 0.0

                close_evt = {
                    "trade_id": int(active_trade_id),
                    "timestamp": pd.Timestamp(ts).isoformat(),
                    "price": float(px_close),
                    "direction": "long" if active_direction > 0 else "short",
                    "reason": "reversal",
                    "exit_size": 1.0,
                    "bars_in_trade": int(bars_in_trade),
                }
                close_points.append(close_evt)
                exit_events.append(close_evt)
                if active_direction > 0:
                    sell_points.append({"timestamp": close_evt["timestamp"], "price": float(px_close)})
                else:
                    buy_points.append({"timestamp": close_evt["timestamp"], "price": float(px_close)})
                exit_reason_counter["reversal"] += 1
                exit_count += 1
                completed_count += 1
                if gross_return_pct > 0:
                    wins += 1

                completed_trades.append(
                    {
                        "trade_id": int(active_trade_id),
                        "direction": "long" if active_direction > 0 else "short",
                        "entry_timestamp": pd.Timestamp(active_entry_ts).isoformat()
                        if active_entry_ts is not None else None,
                        "entry_price": float(active_entry_price),
                        "exit_timestamp": pd.Timestamp(ts).isoformat(),
                        "exit_price": float(px_close),
                        "bars_in_trade": int(bars_in_trade),
                        "reason": "reversal",
                        "gross_return_pct": float(gross_return_pct),
                        "mfe_pct": float(active_mfe * 100.0),
                        "mae_pct": float(active_mae * 100.0),
                        "partial_exit_count": 0,
                    }
                )

                # Clear active state
                active_direction = 0.0
                active_entry_idx = -1
                active_entry_price = 0.0
                active_entry_ts = None
                active_trade_id = 0
                active_mfe = 0.0
                active_mae = 0.0
            else:
                # Still in trade — accrue gross return for the bar
                if px_prev_close > 0:
                    bar_return += active_direction * ((px_close / px_prev_close) - 1.0)

        gross_returns[idx] = bar_return

        # --- 2. If flat, consider opening a new trade ---
        if active_direction == 0.0:
            le = bool(long_entry_signal[idx])
            se = bool(short_entry_signal[idx])
            entry_direction = 0.0
            if le and not se:
                entry_direction = 1.0
            elif se and not le:
                entry_direction = -1.0
            if entry_direction != 0.0:
                active_direction = entry_direction
                active_entry_idx = idx
                active_entry_price = px_close
                active_entry_ts = index[idx]
                active_trade_id = next_trade_id
                next_trade_id += 1
                active_mfe = 0.0
                active_mae = 0.0
                entry_count += 1

                open_evt = {
                    "trade_id": int(active_trade_id),
                    "timestamp": pd.Timestamp(index[idx]).isoformat(),
                    "price": float(px_close),
                    "direction": "long" if entry_direction > 0 else "short",
                }
                open_points.append(open_evt)
                if entry_direction > 0:
                    buy_points.append({"timestamp": open_evt["timestamp"], "price": float(px_close)})
                else:
                    sell_points.append({"timestamp": open_evt["timestamp"], "price": float(px_close)})

        effective_position[idx] = active_direction if active_direction != 0.0 else 0.0

    # --- Outputs ---
    eff_pos = pd.Series(effective_position, index=index, dtype=float)
    gross_ret = pd.Series(gross_returns, index=index, dtype=float)
    turnover = eff_pos.diff().abs().fillna(0.0)
    if len(turnover) > 0:
        turnover.iloc[0] = abs(float(eff_pos.iloc[0] or 0.0))

    win_rate = (wins / completed_count * 100.0) if completed_count else 0.0

    return {
        "effective_position": eff_pos,
        "gross_returns": gross_ret,
        "turnover": turnover,
        "trade_points": {
            "buy_points": buy_points,
            "sell_points": sell_points,
            "open_points": open_points,
            "close_points": close_points,
            "entries": int(entry_count),
            "exits": int(exit_count),
        },
        "trade_stats": {
            "entries": int(entry_count),
            "exits": int(exit_count),
            "completed": int(completed_count),
            "win_rate": round(float(win_rate), 2),
        },
        "protective_stats": {
            "forced_stop_exits": 0,
            "forced_take_exits": 0,
        },
        "exit_reason_breakdown": {
            "stop": 0,
            "take_profit": 0,
            "trailing": 0,
            "reversal": int(exit_reason_counter.get("reversal", 0)),
            "partial": 0,
            "time_stop": 0,
        },
        "exit_events": exit_events,
        "completed_trades": completed_trades,
        "exit_config": dict(config.to_dict()),
    }
