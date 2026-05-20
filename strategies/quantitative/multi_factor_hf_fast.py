"""Phase 2 fast_exact builder for ``MultiFactorHFStrategy``.

Per-bar replay of ``MultiFactorHFStrategy`` calls ``compute_factor`` six
times per bar; on a 30-day 5m frame (8,639 bars) that is ~52,000 redundant
rolling-factor computes and accounts for 77% of wall time in the Phase 0
profile.

This module produces an identical position series with a single pass over
the input frame:

  1. Precompute each factor (and gate metric) ONCE over the whole frame
     using the same ``compute_factor`` function.
  2. Batch-apply the per-bar transform (zscore / rank / none) via a
     rolling reduction.
  3. Walk a single Python loop that runs the strategy's state machine
     (cooldown, virtual side, gate gating) reading precomputed values.

Strategy class semantics that must match the per-bar path exactly:

  * ``min_len = 120`` short-circuits ``generate_signals`` — bars before
    index 119 emit nothing and the position is the carry-over state.
  * The trailing window the strategy sees is at most
    ``max(120, get_required_data().min_length + 20) == 200`` bars; every
    factor's lookback is well below that, so the value at bar ``i`` from
    a full-frame compute equals the value from a per-bar trailing-window
    compute (causal rolling).
  * ``_apply_transform("zscore")`` uses ``s.dropna().tail(120)`` and
    returns the raw value when fewer than 5 non-NaN factor values are
    available — reproduced in :func:`_batch_zscore`.
  * ``_compute_gates`` evaluates ``cooldown_ok`` BEFORE decrementing
    ``cooldown_left``; the order is preserved in the loop.
  * ``CLOSE_X`` always emits on transition; ``BUY``/``SELL`` only emit
    when ``gates.all_ok`` is true. ``cooldown_left`` is bumped only when
    a transition actually happened.

Activation: gated by the ``BACKTEST_FAST_EXACT_STRATEGIES`` setting and
strict parity tests in ``tests/test_multi_factor_hf_parity.py``. The
original per-bar replay path is preserved as the trusted reference.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from core.factors_ts.registry import compute_factor

# Reuse exact config-loading semantics so a fast-path run keys off the same
# YAML/param overrides as the live strategy class.
from strategies.quantitative.multi_factor_hf import (
    _default_config,
    _load_yaml_strategy_config,
)


_MIN_LEN = 120


def _resolve_config(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Mirror ``MultiFactorHFStrategy.__init__``'s nested-merge order so
    the same params dict produces the same final config."""
    params = dict(params or {})
    cfg_path = params.pop("config_path", None)
    cfg = _load_yaml_strategy_config(cfg_path)
    for k, v in params.items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            merged = dict(cfg[k])
            merged.update(v)
            cfg[k] = merged
        else:
            cfg[k] = v
    return cfg


def _factor_key(fname: str, fparams: Dict[str, Any]) -> str:
    """Mirror ``_compute_factor_block``'s key naming so the score terms
    correspond to the same dict keys (kept for parity even though the
    fast path doesn't expose them)."""
    if not fparams:
        return fname
    return f"{fname}({','.join(f'{k}={v}' for k, v in sorted(fparams.items()))})"


def _batch_zscore(s: pd.Series, lookback: int = 120) -> pd.Series:
    """Vectorized equivalent of ``_rolling_z_last(s, lookback)`` applied
    at every bar.

    Per-bar reference (see ``multi_factor_hf._rolling_z_last``):

      tail = s.dropna().tail(max(5, lookback))
      if len(tail) < 5:        return raw last (or 0 if NaN)
      if std(tail, ddof=0) <= 1e-12:   return 0
      else:                     return (tail.iloc[-1] - mean) / std

    Batch construction: drop NaN, rolling mean/std on the dropna'd index,
    forward-fill back to the full index (so a NaN factor reading at bar
    ``i`` keeps the most recent non-NaN z, exactly like per-bar).
    """
    lb = max(5, int(lookback))
    s_num = pd.to_numeric(s, errors="coerce")
    dn = s_num.dropna()
    if dn.empty:
        return pd.Series(0.0, index=s.index, dtype=float)

    mu = dn.rolling(lb, min_periods=5).mean()
    sd = dn.rolling(lb, min_periods=5).std(ddof=0)
    z = (dn - mu) / sd
    # Per-bar returns 0 when std collapses
    z = z.where(sd > 1e-12, 0.0)
    # Per-bar returns the raw value when fewer than 5 non-NaN values exist
    # (mu is NaN there). Use the most recent dn value in that case.
    z_filled = z.where(mu.notna(), dn)
    # Forward-fill across NaN factor readings so the most recent z is used,
    # mirroring per-bar's "tail.iloc[-1]" being the most recent non-NaN.
    return z_filled.reindex(s.index).ffill().fillna(0.0)


def _batch_rank(s: pd.Series, lookback: int = 120) -> pd.Series:
    """Vectorized equivalent of ``_rolling_rank_last``.

    Per-bar: pct = (tail <= last).mean(); return pct * 2 - 1.
    """
    lb = max(5, int(lookback))
    s_num = pd.to_numeric(s, errors="coerce")
    dn = s_num.dropna()
    if dn.empty:
        return pd.Series(0.0, index=s.index, dtype=float)

    def _rank_last(x: np.ndarray) -> float:
        last = x[-1]
        return float((x <= last).mean() * 2.0 - 1.0)

    rank_dn = dn.rolling(lb, min_periods=5).apply(_rank_last, raw=True)
    # When fewer than 5 non-NaN values, per-bar returns 0.0 (len(tail) < 5 branch)
    rank_dn = rank_dn.fillna(0.0)
    return rank_dn.reindex(s.index).ffill().fillna(0.0)


def _batch_transform(series: pd.Series, transform: str, lookback: int = 120) -> pd.Series:
    t = str(transform or "none").lower()
    if t == "none":
        # Per-bar: raw value, 0 if NaN. Batch: just fillna.
        return pd.to_numeric(series, errors="coerce").fillna(0.0)
    if t == "zscore":
        return _batch_zscore(series, lookback=lookback)
    if t == "rank":
        return _batch_rank(series, lookback=lookback)
    raise ValueError(f"Unknown transform: {transform}")


def build_multifactor_hf_position_series(
    df: pd.DataFrame,
    params: Optional[Dict[str, Any]] = None,
    *,
    allow_long: bool = True,
    allow_short: bool = True,
    reverse_on_signal: bool = True,
) -> pd.Series:
    """Fast_exact position builder for ``MultiFactorHFStrategy``.

    Returns a position series indexed identically to ``df`` with the same
    values as ``_replay_signal_strategy_position(MultiFactorHFStrategy, df,
    ...)``. Locked in by ``tests/test_multi_factor_hf_parity.py``.
    """
    if df is None or df.empty:
        return pd.Series([], index=df.index if df is not None else None, dtype=float)

    n = len(df)
    cfg = _resolve_config(params)

    # --- Precompute factor terms (each factor ONE call, full frame) ---
    factor_specs = list(cfg.get("factors") or [])
    score = pd.Series(0.0, index=df.index, dtype=float)
    for spec in factor_specs:
        if not isinstance(spec, dict):
            continue
        fname = str(spec.get("name") or "").strip()
        if not fname:
            continue
        fparams = dict(spec.get("params") or {})
        weight = float(spec.get("weight") or 0.0)
        transform = str(spec.get("transform") or "none")
        raw_series = compute_factor(fname, df, params=fparams)
        transformed = _batch_transform(raw_series, transform)
        score = score + weight * transformed

    score = score.replace([np.inf, -np.inf], 0.0).fillna(0.0)

    # --- Precompute gate metric series ---
    # IMPORTANT: do NOT fillna here. The per-bar path does
    #     latest["x"] = float(pd.to_numeric(series.iloc[-1]) or 0.0)
    # but `float('nan') or 0.0` is NaN (NaN is truthy), and NaN <= max_rv
    # is False — so a NaN gate metric BLOCKS the gate. fillna(0.0) would
    # silently flip a fail into a pass on the low-volume fixture (constant
    # volume → std=0 → volume_z=NaN → trusted blocks, batch wouldn't).
    rv_series = pd.to_numeric(
        compute_factor("realized_vol", df, params={"lookback": 60}), errors="coerce"
    )
    atr_series = pd.to_numeric(
        compute_factor("atr_pct", df, params={"lookback": 30}), errors="coerce"
    )
    spread_series = pd.to_numeric(
        compute_factor("spread_proxy", df, params={}), errors="coerce"
    )
    volz_series = pd.to_numeric(
        compute_factor("volume_z", df, params={"lookback": 60}), errors="coerce"
    )

    # --- Pre-evaluate stateless gate checks (cooldown is stateful, applied in loop) ---
    gates_cfg = dict(cfg.get("gates") or {})
    max_rv = float(gates_cfg.get("max_rv", np.inf))
    max_atr = float(gates_cfg.get("max_atr_pct", np.inf))
    max_spread = float(gates_cfg.get("max_spread_proxy", np.inf))
    min_volume_z = float(gates_cfg.get("min_volume_z", -np.inf))

    # NumPy comparisons with NaN return False — matches per-bar's NaN-blocks-gate.
    rv_ok = (rv_series.to_numpy() <= max_rv)
    atr_ok = (atr_series.to_numpy() <= max_atr)
    spread_ok = (spread_series.to_numpy() <= max_spread)
    volume_ok = (volz_series.to_numpy() >= min_volume_z)
    score_np = score.to_numpy()

    enter_th = float(cfg.get("enter_th", 0.75))
    exit_th = float(cfg.get("exit_th", 0.25))
    if exit_th > enter_th:
        exit_th = enter_th * 0.6
    cooldown_bars = int(cfg.get("cooldown_bars", 0))

    # --- Lazy-import the trade-policy applicator from backtest.py to keep
    #     the position-stream-to-state mapping identical to the per-bar
    #     replay path. The lazy import avoids a circular dependency. ---
    from web.api.backtest import _apply_signal_to_position_state  # noqa: PLC0415
    from core.strategies.strategy_base import SignalType  # noqa: PLC0415

    virtual_side = "flat"
    cooldown_left = 0
    state = 0.0
    out = np.zeros(n, dtype=float)

    for i in range(n):
        # Mirror per-bar's `if len(data) < min_len: return []`
        if i + 1 < _MIN_LEN:
            out[i] = state
            continue

        # Mirror `price = float(pd.to_numeric(work["close"].iloc[-1]))`
        # — if non-finite/<=0, per-bar returns [] before any state update.
        close_i = float(df["close"].iat[i]) if "close" in df.columns else 0.0
        if not np.isfinite(close_i) or close_i <= 0:
            out[i] = state
            continue

        cooldown_ok = cooldown_left <= 0
        all_ok = (
            bool(rv_ok[i])
            and bool(atr_ok[i])
            and bool(spread_ok[i])
            and bool(volume_ok[i])
            and cooldown_ok
        )

        s = float(score_np[i])
        if all_ok:
            if s > enter_th:
                desired = "long"
            elif s < -enter_th:
                desired = "short"
            elif abs(s) < exit_th:
                desired = "flat"
            else:
                desired = virtual_side
        else:
            desired = "flat"

        # Decrement BEFORE transition handling, matching per-bar order.
        if cooldown_left > 0:
            cooldown_left -= 1

        if desired != virtual_side:
            if virtual_side == "long":
                state = _apply_signal_to_position_state(
                    state, SignalType.CLOSE_LONG,
                    allow_long=allow_long, allow_short=allow_short,
                    reverse_on_signal=reverse_on_signal,
                )
            elif virtual_side == "short":
                state = _apply_signal_to_position_state(
                    state, SignalType.CLOSE_SHORT,
                    allow_long=allow_long, allow_short=allow_short,
                    reverse_on_signal=reverse_on_signal,
                )

            if desired == "long" and all_ok:
                state = _apply_signal_to_position_state(
                    state, SignalType.BUY,
                    allow_long=allow_long, allow_short=allow_short,
                    reverse_on_signal=reverse_on_signal,
                )
            elif desired == "short" and all_ok:
                state = _apply_signal_to_position_state(
                    state, SignalType.SELL,
                    allow_long=allow_long, allow_short=allow_short,
                    reverse_on_signal=reverse_on_signal,
                )

            if desired != virtual_side:
                cooldown_left = max(cooldown_left, cooldown_bars)
                virtual_side = desired

        out[i] = state

    return pd.Series(out, index=df.index, dtype=float)
