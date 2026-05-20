"""Derivatives crowding and liquidation-reset decisions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

import numpy as np
import pandas as pd

from core.structural.context import (
    DerivativesContext,
    RiskGateDecision,
    StructuralMarketContext,
    TradeSignalDecision,
    clamp,
    clip_z,
    safe_float,
)


@dataclass
class DerivativesCrowdingConfig:
    crowded_score_enter: float = 0.75
    crowded_score_exit: float = 0.55
    liquidation_burst_enter: float = 0.80
    previous_crowded_enter: float = 0.70
    funding_extreme_z: float = 1.5
    funding_neutral_z: float = 0.75
    oi_reset_threshold: float = -0.015
    price_flush_atr_mult: float = 1.0
    execution_risk_enter: float = 0.65
    execution_risk_block: float = 0.85
    base_position_pct: float = 0.04
    max_position_pct: float = 0.08
    min_signal_strength: float = 0.55


def _linear_reduce(score: float, enter: float, minimum: float = 0.3, maximum: float = 0.7) -> float:
    if score <= enter:
        return maximum
    span = max(1e-9, 1.0 - enter)
    pressure = clamp((score - enter) / span)
    return clamp(maximum - pressure * (maximum - minimum), minimum, maximum)


def calculate_crowding_scores(features: Mapping[str, Any] | DerivativesContext) -> Tuple[float, float]:
    if isinstance(features, DerivativesContext):
        oi_change_z = features.oi_change_z
        funding_z = features.funding_z
        long_short_ratio_z = features.long_short_ratio_z
        basis_z = features.basis_z
        taker_imbalance_z = features.taker_imbalance_z
    else:
        oi_change_z = safe_float(features.get("oi_change_z"))
        funding_z = safe_float(features.get("funding_z"))
        long_short_ratio_z = safe_float(features.get("long_short_ratio_z"))
        basis_z = safe_float(features.get("basis_z"))
        taker_imbalance_z = safe_float(features.get("taker_imbalance_z", features.get("taker_buy_imbalance_z", 0.0)))

    crowded_long = (
        0.30 * clip_z(oi_change_z)
        + 0.25 * clip_z(funding_z)
        + 0.20 * clip_z(long_short_ratio_z)
        + 0.15 * clip_z(basis_z)
        + 0.10 * clip_z(taker_imbalance_z)
    )
    crowded_short = (
        0.30 * clip_z(oi_change_z)
        + 0.25 * clip_z(-funding_z)
        + 0.20 * clip_z(-long_short_ratio_z)
        + 0.15 * clip_z(-basis_z)
        + 0.10 * clip_z(-taker_imbalance_z)
    )
    return clamp(crowded_long), clamp(crowded_short)


def execution_risk_score(ctx: StructuralMarketContext) -> float:
    if ctx.execution.execution_risk_score > 0:
        return clamp(ctx.execution.execution_risk_score)
    return clamp(
        0.50 * ctx.execution.spread_bps_percentile
        + 0.30 * ctx.execution.low_depth_percentile
        + 0.20 * abs(ctx.execution.depth_imbalance)
    )


def update_derivatives_scores(ctx: StructuralMarketContext) -> StructuralMarketContext:
    long_score, short_score = calculate_crowding_scores(ctx.derivatives)
    if ctx.derivatives.crowded_long_score <= 0:
        ctx.derivatives.crowded_long_score = long_score
    if ctx.derivatives.crowded_short_score <= 0:
        ctx.derivatives.crowded_short_score = short_score
    if ctx.derivatives.crowded_long_score > ctx.derivatives.crowded_short_score and ctx.derivatives.crowded_long_score >= 0.5:
        ctx.derivatives.crowding_side = "long"
    elif ctx.derivatives.crowded_short_score > ctx.derivatives.crowded_long_score and ctx.derivatives.crowded_short_score >= 0.5:
        ctx.derivatives.crowding_side = "short"
    else:
        ctx.derivatives.crowding_side = "neutral"
    if ctx.execution.execution_risk_score <= 0:
        ctx.execution.execution_risk_score = execution_risk_score(ctx)
    return ctx


class LiquidationOICrowdingGate:
    def __init__(self, config: Optional[DerivativesCrowdingConfig] = None):
        self.config = config or DerivativesCrowdingConfig()

    def evaluate(self, ctx: StructuralMarketContext) -> RiskGateDecision:
        ctx = update_derivatives_scores(ctx)
        cfg = self.config
        reasons: list[str] = []
        block_longs = False
        block_shorts = False
        long_scalar = 1.0
        short_scalar = 1.0
        severity = 0.0

        d = ctx.derivatives
        if d.degraded:
            reasons.append("derivatives_degraded_neutral")
        if (
            d.crowded_long_score >= cfg.crowded_score_enter
            and d.funding_z > cfg.funding_extreme_z
        ):
            block_longs = True
            long_scalar = min(long_scalar, _linear_reduce(d.crowded_long_score, cfg.crowded_score_enter))
            severity = max(severity, d.crowded_long_score)
            reasons.append("derivatives_crowded_long_block_new_longs")

        if (
            d.crowded_short_score >= cfg.crowded_score_enter
            and d.funding_z < -cfg.funding_extreme_z
        ):
            block_shorts = True
            short_scalar = min(short_scalar, _linear_reduce(d.crowded_short_score, cfg.crowded_score_enter))
            severity = max(severity, d.crowded_short_score)
            reasons.append("derivatives_crowded_short_block_new_shorts")

        exec_risk = execution_risk_score(ctx)
        if exec_risk >= cfg.execution_risk_enter:
            scalar = _linear_reduce(exec_risk, cfg.execution_risk_enter, minimum=0.25, maximum=0.75)
            long_scalar = min(long_scalar, scalar)
            short_scalar = min(short_scalar, scalar)
            severity = max(severity, exec_risk)
            reasons.append("execution_cost_or_depth_risk_reduce")
            if exec_risk >= cfg.execution_risk_block:
                block_longs = True
                block_shorts = True
                reasons.append("execution_cost_or_depth_risk_block_entries")

        if not reasons:
            reasons.append("derivatives_gate_neutral")

        return RiskGateDecision(
            symbol=ctx.symbol,
            timestamp=ctx.timestamp,
            block_new_longs=block_longs,
            block_new_shorts=block_shorts,
            reduce_long_position_scalar=long_scalar,
            reduce_short_position_scalar=short_scalar,
            gate_scalar=min(long_scalar, short_scalar),
            severity=severity,
            reason_codes=reasons,
            metadata={
                "crowded_long_score": round(d.crowded_long_score, 6),
                "crowded_short_score": round(d.crowded_short_score, 6),
                "funding_z": round(d.funding_z, 6),
                "execution_risk_score": round(exec_risk, 6),
                "crowding_side": d.crowding_side,
            },
        )


def _series_zscore(series: pd.Series, lookback: int) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    mean = vals.rolling(lookback, min_periods=max(5, min(lookback, 20))).mean()
    std = vals.rolling(lookback, min_periods=max(5, min(lookback, 20))).std(ddof=0)
    return (vals - mean) / std.replace(0, np.nan)


def _score_from_z(series: pd.Series, lookback: int) -> pd.Series:
    z = _series_zscore(series, lookback)
    return (z.clip(lower=0.0, upper=3.0) / 3.0).fillna(0.0)


# Columns produced by prepare_derivatives_features. If all of these are present
# on the input frame, the call is a no-op — callers (e.g. detect_flush_reversal)
# previously re-ran the whole pipeline on already-prepared data, which made the
# strategy O(n²) over backtest replay; this skip short-circuits that.
_PREPARED_COLUMNS = (
    "oi_change_z",
    "funding_z",
    "long_short_ratio_z",
    "basis_z",
    "taker_imbalance_z",
    "liquidation_burst_score",
    "long_liquidation_burst_score",
    "short_liquidation_burst_score",
    "crowded_long_score",
    "crowded_short_score",
    "execution_risk_score",
    "price_return_1h",
    "atr_pct",
)


def _np_clip_z(values: np.ndarray, z_cap: float = 3.0) -> np.ndarray:
    """Vectorized equivalent of context.clip_z applied to a NumPy array.

    Mirrors ``clamp(max(0, safe_float(x, 0)) / z_cap, 0, 1)`` on every element.
    NaN → 0 (safe_float default) before clipping.
    """
    cap = max(float(z_cap), 1e-9)
    arr = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
    return np.clip(np.maximum(0.0, arr) / cap, 0.0, 1.0)


def _vectorized_crowding_scores(out: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Vectorized form of ``calculate_crowding_scores`` over a whole frame.

    Locked to the per-row implementation by parity tests — see
    ``tests/test_derivatives_crowding_vectorized_parity.py``.
    """
    def _col(name: str) -> np.ndarray:
        return pd.to_numeric(out.get(name, pd.Series(0.0, index=out.index)), errors="coerce").to_numpy()

    oi = _np_clip_z(_col("oi_change_z"))
    funding = _col("funding_z")
    fz_pos = _np_clip_z(funding)
    fz_neg = _np_clip_z(-funding)
    ls = _col("long_short_ratio_z")
    ls_pos = _np_clip_z(ls)
    ls_neg = _np_clip_z(-ls)
    bz = _col("basis_z")
    bz_pos = _np_clip_z(bz)
    bz_neg = _np_clip_z(-bz)
    # Per the original: taker_imbalance_z falls back to taker_buy_imbalance_z
    ti_raw = pd.to_numeric(
        out.get("taker_imbalance_z", out.get("taker_buy_imbalance_z", pd.Series(0.0, index=out.index))),
        errors="coerce",
    ).to_numpy()
    ti_pos = _np_clip_z(ti_raw)
    ti_neg = _np_clip_z(-ti_raw)

    long_raw = 0.30 * oi + 0.25 * fz_pos + 0.20 * ls_pos + 0.15 * bz_pos + 0.10 * ti_pos
    short_raw = 0.30 * oi + 0.25 * fz_neg + 0.20 * ls_neg + 0.15 * bz_neg + 0.10 * ti_neg
    return np.clip(long_raw, 0.0, 1.0), np.clip(short_raw, 0.0, 1.0)


def prepare_derivatives_features(data: pd.DataFrame, *, lookback: int = 120) -> pd.DataFrame:
    if data is None or len(data) == 0:
        # ``data.copy()`` on an empty frame is cheap but keep the early return
        # for the None case (some callers pass None on the cold path).
        return data.copy() if data is not None else pd.DataFrame()

    # Fast path: if every column this function produces already exists, the
    # caller has already prepared the frame and we should NOT copy or recompute.
    # This used to be a hot O(n²) bug — strategy passed prepared df, then
    # detect_flush_reversal recomputed twice more per bar.
    if all(col in data.columns for col in _PREPARED_COLUMNS):
        return data

    out = data.copy()

    if "oi_change_1h" not in out and "oi" in out:
        out["oi_change_1h"] = pd.to_numeric(out["oi"], errors="coerce").pct_change()
    if "oi_change_z" not in out:
        base = out.get("oi_change_1h", pd.Series(0.0, index=out.index))
        out["oi_change_z"] = _series_zscore(base, lookback).fillna(0.0)

    if "funding_z" not in out:
        base = out.get("funding_rate", pd.Series(0.0, index=out.index))
        out["funding_z"] = _series_zscore(base, lookback).fillna(0.0)

    if "long_short_ratio_z" not in out:
        base = out.get("global_long_short_ratio", out.get("top_trader_position_ratio", pd.Series(0.0, index=out.index)))
        out["long_short_ratio_z"] = _series_zscore(base, lookback).fillna(0.0)

    if "basis_z" not in out:
        if "basis_pct" in out:
            base = out["basis_pct"]
        elif {"mark_price", "index_price"}.issubset(out.columns):
            idx = pd.to_numeric(out["index_price"], errors="coerce").replace(0, np.nan)
            base = (pd.to_numeric(out["mark_price"], errors="coerce") - idx) / idx
        elif "mark_index_basis" in out:
            base = out["mark_index_basis"]
        else:
            base = pd.Series(0.0, index=out.index)
        out["basis_z"] = _series_zscore(base, lookback).fillna(0.0)

    if "taker_imbalance" not in out and {"taker_buy_volume", "taker_sell_volume"}.issubset(out.columns):
        buy = pd.to_numeric(out["taker_buy_volume"], errors="coerce")
        sell = pd.to_numeric(out["taker_sell_volume"], errors="coerce")
        out["taker_imbalance"] = (buy - sell) / (buy + sell).replace(0, np.nan)
    if "taker_imbalance_z" not in out:
        base = out.get("taker_imbalance", out.get("taker_buy_imbalance", pd.Series(0.0, index=out.index)))
        out["taker_imbalance_z"] = _series_zscore(base, lookback).fillna(0.0)

    if "liquidation_burst_score" not in out:
        long_liq = pd.to_numeric(out.get("liquidation_long_usd", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
        short_liq = pd.to_numeric(out.get("liquidation_short_usd", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
        total_liq = long_liq + short_liq
        out["liquidation_burst_score"] = _score_from_z(total_liq, lookback)
    if "long_liquidation_burst_score" not in out:
        out["long_liquidation_burst_score"] = _score_from_z(
            pd.to_numeric(out.get("liquidation_long_usd", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0),
            lookback,
        )
    if "short_liquidation_burst_score" not in out:
        out["short_liquidation_burst_score"] = _score_from_z(
            pd.to_numeric(out.get("liquidation_short_usd", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0),
            lookback,
        )

    if "price_return_1h" not in out and "close" in out:
        out["price_return_1h"] = pd.to_numeric(out["close"], errors="coerce").pct_change().fillna(0.0)
    if "atr_pct" not in out and {"high", "low", "close"}.issubset(out.columns):
        high = pd.to_numeric(out["high"], errors="coerce")
        low = pd.to_numeric(out["low"], errors="coerce")
        close = pd.to_numeric(out["close"], errors="coerce")
        tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
        out["atr_pct"] = (tr.rolling(14, min_periods=3).mean() / close.replace(0, np.nan)).fillna(0.0)

    # Crowding scores: vectorized, and ONLY run when the columns are missing.
    # The original code computed these via a Python ``iterrows`` loop on EVERY
    # call (the missing-column guards were on assignment only, after the loop),
    # making this function O(n) even on already-prepared frames.
    needs_long = "crowded_long_score" not in out
    needs_short = "crowded_short_score" not in out
    if needs_long or needs_short:
        long_arr, short_arr = _vectorized_crowding_scores(out)
        if needs_long:
            out["crowded_long_score"] = long_arr
        if needs_short:
            out["crowded_short_score"] = short_arr
    if "execution_risk_score" not in out:
        spread_pct = pd.to_numeric(out.get("spread_bps_percentile", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
        low_depth = pd.to_numeric(out.get("low_depth_percentile", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
        depth_imb = pd.to_numeric(out.get("depth_imbalance", pd.Series(0.0, index=out.index)), errors="coerce").abs().fillna(0.0)
        out["execution_risk_score"] = (0.5 * spread_pct + 0.3 * low_depth + 0.2 * depth_imb).clip(0.0, 1.0)
    return out


def detect_flush_reversal(
    data: pd.DataFrame,
    *,
    side: str,
    config: Optional[DerivativesCrowdingConfig] = None,
) -> TradeSignalDecision:
    cfg = config or DerivativesCrowdingConfig()
    if data.empty or len(data) < 3:
        symbol = str(data["symbol"].iloc[-1]) if "symbol" in data and len(data) else "UNKNOWN"
        ts = data.index[-1].to_pydatetime() if len(data.index) else None
        return TradeSignalDecision.hold(symbol, ts, "insufficient_derivatives_history")

    prepared = prepare_derivatives_features(data)
    row = prepared.iloc[-1]
    prev = prepared.iloc[-2]
    symbol = str(row.get("symbol", "UNKNOWN"))
    # Force tz-aware UTC. A naive bar index would otherwise leak through and
    # mix with the rest of the codebase's timezone-aware timestamps (e.g.
    # ``StrategyBase._bar_time`` always returns tz-aware UTC).
    _last = data.index[-1]
    _ts = pd.Timestamp(_last) if not isinstance(_last, pd.Timestamp) else _last
    if _ts.tzinfo is None:
        _ts = _ts.tz_localize("UTC")
    else:
        _ts = _ts.tz_convert("UTC")
    timestamp = _ts.to_pydatetime()
    side = str(side).lower()

    if side == "long_flush":
        prior_crowded = safe_float(prev.get("crowded_long_score")) >= cfg.previous_crowded_enter
        liq_burst = safe_float(row.get("long_liquidation_burst_score", row.get("liquidation_burst_score"))) >= cfg.liquidation_burst_enter
        oi_reset = safe_float(row.get("oi_change_1h", row.get("oi_change_z"))) <= cfg.oi_reset_threshold or safe_float(row.get("oi_change_z")) < -0.35
        funding_normalizes = abs(safe_float(row.get("funding_z"))) <= max(abs(safe_float(prev.get("funding_z"))), cfg.funding_neutral_z)
        price_flush = safe_float(row.get("price_return_1h")) <= -cfg.price_flush_atr_mult * max(safe_float(row.get("atr_pct")), 1e-6)
        taker_weakens = safe_float(row.get("taker_imbalance_z")) >= safe_float(prev.get("taker_imbalance_z")) - 0.05
        checks = [prior_crowded, liq_burst, oi_reset, funding_normalizes, price_flush, taker_weakens]
        passed = sum(bool(x) for x in checks)
        if passed >= 5:
            strength = clamp(0.45 + 0.10 * passed + 0.20 * safe_float(row.get("long_liquidation_burst_score", row.get("liquidation_burst_score"))))
            return TradeSignalDecision(
                symbol=symbol,
                timestamp=timestamp,
                direction="buy",
                bias=strength,
                strength=strength,
                position_pct=cfg.base_position_pct,
                reason_codes=["liquidation_long_flush_reversal"],
                metadata={
                    "passed_checks": passed,
                    "previous_crowded_long_score": round(safe_float(prev.get("crowded_long_score")), 6),
                    "long_liquidation_burst_score": round(safe_float(row.get("long_liquidation_burst_score")), 6),
                    "oi_change_1h": round(safe_float(row.get("oi_change_1h")), 6),
                    "funding_z": round(safe_float(row.get("funding_z")), 6),
                },
            )
        return TradeSignalDecision.hold(symbol, timestamp, "liquidation_long_flush_reversal_not_confirmed")

    prior_crowded = safe_float(prev.get("crowded_short_score")) >= cfg.previous_crowded_enter
    liq_burst = safe_float(row.get("short_liquidation_burst_score", row.get("liquidation_burst_score"))) >= cfg.liquidation_burst_enter
    oi_reset = safe_float(row.get("oi_change_1h", row.get("oi_change_z"))) <= cfg.oi_reset_threshold or safe_float(row.get("oi_change_z")) < -0.35
    funding_normalizes = abs(safe_float(row.get("funding_z"))) <= max(abs(safe_float(prev.get("funding_z"))), cfg.funding_neutral_z)
    price_squeeze = safe_float(row.get("price_return_1h")) >= cfg.price_flush_atr_mult * max(safe_float(row.get("atr_pct")), 1e-6)
    taker_weakens = safe_float(row.get("taker_imbalance_z")) <= safe_float(prev.get("taker_imbalance_z")) + 0.05
    checks = [prior_crowded, liq_burst, oi_reset, funding_normalizes, price_squeeze, taker_weakens]
    passed = sum(bool(x) for x in checks)
    if passed >= 5:
        strength = clamp(0.45 + 0.10 * passed + 0.20 * safe_float(row.get("short_liquidation_burst_score", row.get("liquidation_burst_score"))))
        return TradeSignalDecision(
            symbol=symbol,
            timestamp=timestamp,
            direction="sell",
            bias=-strength,
            strength=strength,
            position_pct=cfg.base_position_pct,
            reason_codes=["liquidation_short_squeeze_reversal"],
            metadata={
                "passed_checks": passed,
                "previous_crowded_short_score": round(safe_float(prev.get("crowded_short_score")), 6),
                "short_liquidation_burst_score": round(safe_float(row.get("short_liquidation_burst_score")), 6),
                "oi_change_1h": round(safe_float(row.get("oi_change_1h")), 6),
                "funding_z": round(safe_float(row.get("funding_z")), 6),
            },
        )
    return TradeSignalDecision.hold(symbol, timestamp, "liquidation_short_squeeze_reversal_not_confirmed")
