"""Honest scoring of a feature formula against forward 7-day returns (research loop v3).

Question: ranked across the day's ~150 most liquid perps, does the feature order the next 7
days' returns, does it carry information the known factors do not, and does the top-minus-bottom
fifth book still earn after costs and funding?

Protections carried over from research loop v2 (core/research/xs_evaluation.py), where each was
calibrated against a false positive on real data:
* coin-clustered permutation null: every coin gets another coin's whole feature trajectory, which
  keeps per-coin persistence and market-wide moves; IC z-scores are against that null. Where the
  donor has no value on a day the receiver is scored (listed later, delisted earlier), another
  random coin that has one fills those days (never the coin itself), so each draw ranks as many
  coins as the observed day. A plain column permutation kept only 23% of the development-period
  cells (few perps in 2020-21) and made that null about three times too wide (calibrated
  2026-10-18);
* incremental value is measured by orthogonalizing the feature's cross-sectional ranks against
  the known factors' ranks day by day (a copy of a known factor scores ~0), not by blending;
* the book is checked without the three coins that contribute most, so one meme coin cannot
  carry the verdict.
Books follow the forward trackers: one 7-day book per day, 7 non-overlapping phases averaged,
0.12% per round trip on the names replaced versus the phase's previous book, actual funding
(longs pay, shorts earn). Returns are arithmetic, so short legs are (entry - exit) / entry.
All numerics are elementwise / reductions: this environment's BLAS crashes natively.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from core.research.xs_daily_panel import FACTORS, HOLD_DAYS, Panel
from core.research.xs_feature_dsl import evaluate_formula_wide

MIN_COINS = 30
COST_ROUND_TRIP = 0.0012
RESID_FLOOR = 1e-6  # residual norm below this share of the feature's norm = no new information
MAX_DONOR_HOPS = 40  # 2020 had ~50 live perps of 590: 40 random donors leave ~3% of those cells unfilled
DEV_START, DEV_END = pd.Timestamp("2020-07-01"), pd.Timestamp("2023-12-31")
HOLDOUT_START = pd.Timestamp("2024-01-01")


class EvalContext:
    """Arrays shared by every formula evaluated on one panel."""

    def __init__(self, panel: Panel, periods: Optional[Mapping[str, tuple]] = None):
        self.panel = panel
        self.dates = panel.dates
        self.coins = panel.coins
        self.universe = panel.universe.to_numpy(dtype=bool)
        self.fwd = panel.fwd7.to_numpy(dtype=float)
        self.ret = np.expm1(self.fwd)
        self.fund = panel.fund7.to_numpy(dtype=float)
        self.label_ok = self.universe & np.isfinite(self.fwd)
        last_label = self.dates[np.flatnonzero(self.label_ok.sum(axis=1) >= MIN_COINS).max()]
        periods = periods or {"dev": (DEV_START, DEV_END), "holdout": (HOLDOUT_START, last_label)}
        self.periods = {name: (self.dates >= start) & (self.dates <= end) for name, (start, end) in periods.items()}
        self.period_bounds = {name: (str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date()))
                              for name, (start, end) in periods.items()}
        # Compact layout for the IC work: row t lists that day's ranked coins (universe with a label),
        # padded to the widest day (~150 of ~590 columns), so ranks and projections touch only them.
        self.width = max(1, int(self.label_ok.sum(axis=1).max()))
        order = np.argsort(~self.label_ok, axis=1, kind="stable")[:, : self.width]
        self.slot = np.take_along_axis(self.label_ok, order, axis=1)
        self.idx = np.where(self.slot, order, 0)
        self.rowsel = np.arange(len(self.dates))[:, None]
        self.fwd_c = self.compact(self.fwd)
        # known factors, ranked in the universe (sign does not matter for orthogonalizing)
        self.factor_names = list(FACTORS)
        self.factor_values = [panel.factors[name].to_numpy(dtype=float) for name in self.factor_names]
        self.factor_values_c = [self.compact(v) for v in self.factor_values]
        self.factor_ranks = [np.where(self.slot, self.compact(r), 0.0) for r in _factor_ranks(self)]

    def compact(self, full: np.ndarray) -> np.ndarray:
        """(dates x coins) -> (dates x width): each day's ranked coins; padding is missing."""
        return np.where(self.slot, full[self.rowsel, self.idx], np.nan)


def feature_array(formula: Mapping[str, Any], panel: Panel) -> np.ndarray:
    """Feature values (dates x coins), signed so that higher = expected to outperform."""
    used = _columns_used(formula["expr"])
    frames = {k: panel.columns[k] for k in used}
    values = evaluate_formula_wide(formula, frames).to_numpy(dtype=float)
    return values if formula.get("direction", "high") == "high" else -values


def _columns_used(node: Mapping[str, Any]) -> List[str]:
    if "col" in node:
        return [node["col"]]
    return sorted({c for a in node.get("args") or [] for c in _columns_used(a)})


def centered_ranks(values: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """Average ranks within each row's valid cells, centred on zero; 0 elsewhere."""
    ranks = pd.DataFrame(np.where(valid, values, np.nan)).rank(axis=1, method="average").to_numpy()
    counts = valid.sum(axis=1, keepdims=True)
    return np.where(valid, ranks - (counts + 1) / 2.0, 0.0)


def row_corr(a: np.ndarray, b: np.ndarray, valid: np.ndarray) -> np.ndarray:
    """Per-row correlation of zero-centred scores (NaN when a row has fewer than MIN_COINS)."""
    n = valid.sum(axis=1)
    num = (a * b).sum(axis=1)
    den = np.sqrt((a * a).sum(axis=1) * (b * b).sum(axis=1))
    out = np.where(den > 1e-12, num / np.where(den > 1e-12, den, 1.0), 0.0)
    return np.where(n >= MIN_COINS, out, np.nan)


def residualize(scores: np.ndarray, factors: Sequence[np.ndarray], valid: np.ndarray) -> np.ndarray:
    """Remove from each row of ``scores`` its projection on the factors (Gram-Schmidt per row)."""
    n = np.maximum(valid.sum(axis=1, keepdims=True), 1)
    basis: List[np.ndarray] = []
    for factor in factors:
        v = np.where(valid, factor, 0.0)
        v = np.where(valid, v - v.sum(axis=1, keepdims=True) / n, 0.0)
        for u in basis:
            v = v - (v * u).sum(axis=1, keepdims=True) * u
        norm = np.sqrt((v * v).sum(axis=1, keepdims=True))
        basis.append(np.where(norm > 1e-9, v / np.where(norm > 1e-9, norm, 1.0), 0.0))
    out = scores.copy()
    for u in basis:
        out = out - (out * u).sum(axis=1, keepdims=True) * u
    return out


def _ic_rows(ctx: EvalContext, feature_c: np.ndarray, factor_ranks: Optional[List[np.ndarray]] = None) -> Dict[str, np.ndarray]:
    """Daily IC (and residual IC) from compact arrays (EvalContext.compact)."""
    valid = ctx.slot & np.isfinite(feature_c)
    rf = centered_ranks(feature_c, valid)
    ry = centered_ranks(ctx.fwd_c, valid)
    out = {"ic": row_corr(rf, ry, valid), "n": valid.sum(axis=1)}
    if factor_ranks is not None:
        resid = residualize(rf, factor_ranks, valid)
        # a feature inside the factors' span leaves rounding noise; correlating that noise would be random
        kept = np.sqrt((resid * resid).sum(axis=1)) > RESID_FLOOR * np.sqrt((rf * rf).sum(axis=1))
        out["resid_ic"] = row_corr(np.where(kept[:, None], resid, 0.0), ry, valid)
    return out


def _factor_ranks(ctx: EvalContext) -> List[np.ndarray]:
    """Known factors ranked within universe cells that have a value (missing -> 0, the centre)."""
    out = []
    for values in ctx.factor_values:
        ok = ctx.universe & np.isfinite(values)
        out.append(centered_ranks(values, ok))
    return out


def books(ctx: EvalContext, feature: np.ndarray, rows: np.ndarray, exclude: Optional[np.ndarray] = None) -> Dict[str, Any]:
    """Top-minus-bottom fifth, one 7-day book per day, 7 phases; net of costs and funding."""
    days = np.flatnonzero(rows)
    nets, longs_rel, shorts_rel = [], [], []
    contrib = np.zeros(feature.shape[1])
    for offset in range(HOLD_DAYS):
        prev = None
        for t in days[offset::HOLD_DAYS]:
            ok = ctx.label_ok[t] & np.isfinite(feature[t])
            if exclude is not None:
                ok &= ~exclude
            idx = np.flatnonzero(ok)
            if len(idx) < MIN_COINS:
                prev = None
                continue
            ordered = idx[np.argsort(feature[t, idx], kind="mergesort")]
            k = len(ordered) // 5
            short, long_ = ordered[:k], ordered[-k:]
            r, f = ctx.ret[t], ctx.fund[t]
            spread = float(r[long_].mean() - r[short].mean())
            fund = float(f[long_].mean() - f[short].mean())
            turn = 1.0 if prev is None else 0.5 * (len(set(long_) - prev[0]) + len(set(short) - prev[1])) / k
            nets.append(spread - fund - 2 * turn * COST_ROUND_TRIP)
            mean_all = float(r[idx].mean())
            longs_rel.append(float(r[long_].mean()) - mean_all)
            shorts_rel.append(mean_all - float(r[short].mean()))
            contrib[long_] += (r[long_] - f[long_]) / k
            contrib[short] -= (r[short] - f[short]) / k
            prev = (set(long_), set(short))
    if not nets:
        return {"books": 0}
    return {
        "books": len(nets),
        "net_per_book": _round(float(np.mean(nets))),
        "net_per_day": _round(float(np.mean(nets)) / HOLD_DAYS),
        "positive_books": _round(float(np.mean(np.asarray(nets) > 0)), 3),
        "long_leg_vs_universe": _round(float(np.mean(longs_rel))),
        "short_leg_vs_universe": _round(float(np.mean(shorts_rel))),
        "_contrib": contrib,
    }


def permuted_feature(ctx: EvalContext, feature: np.ndarray, needed_c: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """One null draw (compact): each ranked coin carries another coin's trajectory.

    The donors are one random permutation of all coins, so a coin keeps the same donor every day;
    cells that donor cannot fill (no value that day) go to further random donors, up to
    MAX_DONOR_HOPS, never the receiving coin itself. Only ``needed_c`` (the observed feature's
    scored cells) is filled, so each draw ranks exactly the coins the observed day ranks.
    """
    n = feature.shape[1]
    donor = rng.permutation(n)[ctx.idx]
    out = feature[ctx.rowsel, donor]
    out[(donor == ctx.idx) | ~needed_c] = np.nan
    for _ in range(MAX_DONOR_HOPS):
        missing = needed_c & ~np.isfinite(out)
        if not missing.any():
            break
        donor = rng.permutation(n)[ctx.idx]
        candidate = feature[ctx.rowsel, donor]
        candidate[donor == ctx.idx] = np.nan
        fill = missing & np.isfinite(candidate)
        out[fill] = candidate[fill]
    return out


def evaluate(formula: Mapping[str, Any], ctx: EvalContext, *, n_perm: int = 200, seed: int = 0,
             feature: Optional[np.ndarray] = None) -> Dict[str, Dict[str, Any]]:
    """Metrics per period: coverage, IC and z, residual IC and z, nearest known factor, books."""
    feature = feature_array(formula, ctx.panel) if feature is None else feature
    feature_c = ctx.compact(feature)
    factor_ranks = ctx.factor_ranks
    observed = _ic_rows(ctx, feature_c, factor_ranks)
    rng = np.random.default_rng(seed)
    needed_c = ctx.slot & np.isfinite(feature_c)
    null_ic: Dict[str, List[float]] = {name: [] for name in ctx.periods}
    null_resid: Dict[str, List[float]] = {name: [] for name in ctx.periods}
    for _ in range(n_perm):
        shuffled = permuted_feature(ctx, feature, needed_c, rng)
        draw = _ic_rows(ctx, shuffled, factor_ranks)
        for name, rows in ctx.periods.items():
            null_ic[name].append(_mean(draw["ic"][rows]))
            null_resid[name].append(_mean(draw["resid_ic"][rows]))
    universe_rows = ctx.universe.sum(axis=1)
    covered = (ctx.universe & np.isfinite(feature)).sum(axis=1)
    out: Dict[str, Dict[str, Any]] = {}
    for name, rows in ctx.periods.items():
        scored = rows & (observed["n"] >= MIN_COINS)
        result: Dict[str, Any] = {
            "period": ctx.period_bounds[name],
            "coverage": _round(float(covered[rows].sum() / max(1, universe_rows[rows].sum())), 3),
            "days": int(scored.sum()),
        }
        if scored.sum() < 60:
            out[name] = {**result, "status": "insufficient_sample"}
            continue
        ic, resid = _mean(observed["ic"][rows]), _mean(observed["resid_ic"][rows])
        result.update(status="ok", ic=_round(ic), z=_z(ic, null_ic[name]),
                      resid_ic=_round(resid), resid_z=_z(resid, null_resid[name]))
        days = np.flatnonzero(scored)
        half = len(days) // 2
        result["ic_halves"] = [_round(_mean(observed["ic"][days[:half]])), _round(_mean(observed["ic"][days[half:]]))]
        result.update(_nearest_factor(ctx, feature_c, rows))
        book = books(ctx, feature, rows)
        contrib = book.pop("_contrib", None)
        result.update(book)
        if contrib is not None and book.get("books"):
            top3 = np.argsort(-contrib)[:3]
            exclude = np.zeros(feature.shape[1], dtype=bool)
            exclude[top3] = True
            ex = books(ctx, feature, rows, exclude=exclude)
            result["net_per_book_ex_top3"] = ex.get("net_per_book")
        out[name] = result
    return out


def _nearest_factor(ctx: EvalContext, feature_c: np.ndarray, rows: np.ndarray) -> Dict[str, Any]:
    best_name, best = None, 0.0
    for name, values_c in zip(ctx.factor_names, ctx.factor_values_c):
        valid = ctx.slot & np.isfinite(feature_c) & np.isfinite(values_c)
        valid[~rows] = False
        corr = _mean(row_corr(centered_ranks(feature_c, valid), centered_ranks(values_c, valid), valid))
        if math.isfinite(corr) and abs(corr) > abs(best):
            best_name, best = name, corr
    return {"nearest_factor": best_name, "nearest_factor_corr": _round(best, 3)}


def bonferroni_z(n_trials: int, alpha: float = 0.05) -> float:
    from core.research.xs_evaluation import bonferroni_z as _bz

    return _bz(n_trials, alpha)


def _mean(values: np.ndarray) -> float:
    values = values[np.isfinite(values)]
    return float(values.mean()) if len(values) else float("nan")


def _z(value: float, null: List[float]) -> Optional[float]:
    arr = np.asarray([v for v in null if math.isfinite(v)])
    if len(arr) < 10 or not math.isfinite(value):
        return None
    sd = float(arr.std())
    return _round((value - float(arr.mean())) / sd) if sd > 0 else None


def _round(value: Optional[float], digits: int = 4) -> Optional[float]:
    return round(float(value), digits) if value is not None and math.isfinite(float(value)) else None
