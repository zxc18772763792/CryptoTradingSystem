"""Honest scoring of a cross-sectional feature formula (research loop v2).

Question answered: when coins are ranked by this feature each Monday, do the
top 20% pump (2x within 30 days) more often than the universe does?

Protections, each learned from an earlier false positive in this repo:
* non-overlapping sampling: the 30-day label spans ~4 weekly rows, so only
  every 4th Monday is used (all 4 phases averaged). Otherwise one pump counts
  4 times (WLD/SIREN pseudo-replication, see the weekly-model audit).
* coin-clustered permutation null: each coin is given another coin's whole
  feature trajectory (keeps per-coin persistence and market-wide pumps); z
  against that null. A within-date shuffle was calibrated and rejected: random
  persistent features passed z>1.645 25% of the time.
* leave-top-3-coins-out: a result carried by one or two meme coins is not a rule.
* incremental value over the validated weekly model: inside the model's own
  shortlist (its top 40%), do the feature's top-half coins pump more often?
  ("shortlist_lift", with its own coin-permutation z). An equal-weight blend
  test was tried first and rejected: it punished any feature weaker than the
  model (every candidate lost 0.4-0.9 lift) instead of measuring new information.
All numerics are ufunc/reduction only: this env's BLAS can crash natively.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Optional

import numpy as np
import pandas as pd

from core.research.xs_feature_dsl import evaluate_formula
from core.research.xs_panel import MCAP_MAX, MCAP_MIN, PUMP_THRESHOLD, weekly_rows

TOP_QUANTILE = 0.8
PHASES = 4


def feature_panel(formula: Mapping[str, Any], daily: pd.DataFrame) -> pd.Series:
    """Feature value for every daily row, computed coin by coin (no cross-coin leakage)."""
    out = pd.Series(np.nan, index=daily.index, dtype=float)
    for _, grp in daily.groupby("base", sort=False):
        frame = grp.set_index("date").sort_index()
        values = evaluate_formula(formula, frame)
        out.loc[grp.sort_values("date").index] = values.to_numpy()
    return out


def _signed_rank(rows: pd.DataFrame, direction: str, by=("date",)) -> pd.Series:
    # "low" = the smallest values are expected to pump, so they get the top ranks.
    return rows.groupby(list(by))["feature"].rank(pct=True, ascending=(direction == "high"))


def _phase_mask(dates: pd.Series, phase: int) -> np.ndarray:
    weeks = ((pd.to_datetime(dates) - pd.Timestamp("2020-01-06")).dt.days // 7).to_numpy()
    return (weeks % PHASES) == phase


def _lift(pump: np.ndarray, top: np.ndarray) -> float:
    base = pump.mean() if len(pump) else float("nan")
    if not len(pump) or base <= 0 or top.sum() == 0:
        return float("nan")
    return float(pump[top].mean() / base)


def _phase_averaged_lift(rows: pd.DataFrame, rank: np.ndarray, top_quantile: float = TOP_QUANTILE) -> float:
    lifts = []
    pump = rows["pump"].to_numpy()
    for phase in range(PHASES):
        mask = _phase_mask(rows["date"], phase)
        if mask.sum() == 0:
            continue
        value = _lift(pump[mask], rank[mask] >= top_quantile)
        if math.isfinite(value):
            lifts.append(value)
    return float(np.mean(lifts)) if lifts else float("nan")


def _coin_permuted_ranks(rows: pd.DataFrame, direction: str, rng: np.random.Generator, by=("date",)) -> np.ndarray:
    """Null draw: every coin gets ANOTHER coin's whole feature trajectory.

    Features and pumps are both persistent per coin (repeat pumpers), so the
    null must keep that persistence. Shuffling within each date instead broke
    it and let random persistent features "pass" z>1.645 25% of the time
    (calibrated 2026-09-26); coin-level shuffling is the honest null.
    """
    coins = rows["base"].unique()
    mapping = dict(zip(coins, rng.permutation(coins)))
    lookup = rows.set_index(["base", "date"])["feature"]
    donor = pd.MultiIndex.from_arrays([rows["base"].map(mapping), rows["date"]])
    shuffled = rows.assign(feature=lookup.reindex(donor).to_numpy())
    rank = _signed_rank(shuffled, direction, by).to_numpy()
    return np.where(np.isnan(rank), -1.0, rank)  # donor absent that week -> not in top bucket


SHORTLIST_QUANTILE = 0.6  # the validated model's top 40% ...
SHORTLIST_BAND = 0.1      # ... split into 4 score bands of 10% each


def _shortlist_test(shortlist: pd.DataFrame, direction: str, *, n_perm: int, seed: int) -> Dict[str, Any]:
    """Incremental information: inside the model's own shortlist, does the
    feature separate winners the model's ranking cannot?

    The shortlist is cut into bands of the model's score and the feature is
    ranked WITHIN each band, so the model's own gradient cannot masquerade as
    new information (a copy of the model scores ~1.0 here; unbanded, a feature
    already in the model still showed 1.27). Blending was also rejected: it
    punished any feature weaker than the model instead of measuring novelty.
    """
    if len(shortlist) < 40 or shortlist["pump"].sum() < 5:
        return {"shortlist_rows": int(len(shortlist)), "shortlist_lift": None, "shortlist_z": None}
    by = ("date", "band")
    rank = _signed_rank(shortlist, direction, by).to_numpy()
    lift = _phase_averaged_lift(shortlist, rank, top_quantile=0.5 + 1e-9)
    rng = np.random.default_rng(seed + 1)
    null = np.array([
        _phase_averaged_lift(shortlist, _coin_permuted_ranks(shortlist, direction, rng, by), top_quantile=0.5 + 1e-9)
        for _ in range(n_perm)
    ])
    null = null[np.isfinite(null)]
    sd = float(null.std()) if len(null) > 1 else float("nan")
    z = (lift - float(null.mean())) / sd if sd and math.isfinite(sd) and sd > 0 else float("nan")
    return {"shortlist_rows": int(len(shortlist)), "shortlist_lift": _round(lift), "shortlist_z": _round(z)}


def score_period(
    rows: pd.DataFrame,
    direction: str,
    *,
    n_perm: int = 300,
    seed: int = 0,
    baseline_score: Optional[pd.Series] = None,
) -> Dict[str, Any]:
    """Metrics for one period's weekly rows (columns: base, date, feature, pump)."""
    total = len(rows)
    if baseline_score is not None:
        rows = rows.assign(_baseline=baseline_score.to_numpy())  # carried along before any filtering
    rows = rows.dropna(subset=["feature"]).reset_index(drop=True)
    result: Dict[str, Any] = {
        "rows": int(len(rows)),
        "coverage": round(len(rows) / total, 3) if total else 0.0,
        "dates": int(rows["date"].nunique()) if len(rows) else 0,
        "coins": int(rows["base"].nunique()) if len(rows) else 0,
    }
    if len(rows) < 50 or rows["pump"].sum() < 5:
        result["status"] = "insufficient_sample"
        return result
    rank = _signed_rank(rows, direction).to_numpy()
    top = rank >= TOP_QUANTILE
    lift = _phase_averaged_lift(rows, rank)
    rng = np.random.default_rng(seed)
    null = np.array([_phase_averaged_lift(rows, _coin_permuted_ranks(rows, direction, rng)) for _ in range(n_perm)])
    null = null[np.isfinite(null)]
    sd = float(null.std()) if len(null) > 1 else float("nan")
    z = (lift - float(null.mean())) / sd if sd and math.isfinite(sd) and sd > 0 else float("nan")
    p_emp = float((1 + (null >= lift).sum()) / (1 + len(null))) if len(null) else float("nan")

    # Which coins carry the result? Drop the top-3 contributors of top-bucket pumps.
    contrib = rows.loc[top & (rows["pump"].to_numpy() == 1), "base"].value_counts()
    drop = set(contrib.head(3).index)
    kept = rows[~rows["base"].isin(drop)].reset_index(drop=True)
    kept_rank = _signed_rank(kept, direction).to_numpy() if len(kept) else np.array([])
    lift_ex3 = _phase_averaged_lift(kept, kept_rank) if len(kept) else float("nan")

    result.update({
        "status": "ok",
        "base_rate": round(float(rows["pump"].mean()), 4),
        "top_rate": round(float(rows.loc[top, "pump"].mean()), 4),
        "lift": _round(lift),
        "null_mean": _round(float(null.mean()) if len(null) else float("nan")),
        "z": _round(z),
        "p_perm": _round(p_emp),
        "lift_ex_top3_coins": _round(lift_ex3),
        "top_pump_coins": int(contrib.shape[0]),
    })

    if "_baseline" in rows:
        both = rows.rename(columns={"_baseline": "b"}).dropna(subset=["b"]).reset_index(drop=True)
        if len(both) >= 50 and both["pump"].sum() >= 5:
            f_rank = _signed_rank(both, direction)
            b_rank = both.groupby("date")["b"].rank(pct=True)
            combo = both.assign(feature=(f_rank + b_rank) / 2.0)
            base_lift = _phase_averaged_lift(both, b_rank.to_numpy())
            combo_lift = _phase_averaged_lift(combo, _signed_rank(combo, "high").to_numpy())
            result.update({
                "corr_with_baseline": _round(spearman(f_rank, b_rank)),
                "baseline_lift": _round(base_lift),
                "combo_lift": _round(combo_lift),
                "combo_gain": _round(combo_lift - base_lift),  # informational: a 50/50 blend dilutes a strong model
            })
            shortlist = both.assign(band=np.minimum(((b_rank - SHORTLIST_QUANTILE) / SHORTLIST_BAND).astype(float) // 1, 3))
            shortlist = shortlist[b_rank.to_numpy() >= SHORTLIST_QUANTILE].reset_index(drop=True)
            result.update(_shortlist_test(shortlist, direction, n_perm=max(50, n_perm // 2), seed=seed))
    return result


def evaluate_formula_on_panel(
    formula: Mapping[str, Any],
    daily: pd.DataFrame,
    *,
    periods: Mapping[str, tuple],
    n_perm: int = 300,
    baseline: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """Score one formula on each named (start, end] date period of a labelled daily panel."""
    feature = feature_panel(formula, daily)
    weekly = weekly_rows(daily, feature)
    if baseline is not None and len(weekly):
        weekly = weekly.merge(baseline[["base", "date", "baseline_score"]], on=["base", "date"], how="left")
    out: Dict[str, Any] = {}
    for name, (start, end) in periods.items():
        mask = pd.Series(True, index=weekly.index)
        if start is not None:
            mask &= weekly["date"] > pd.Timestamp(start)
        if end is not None:
            mask &= weekly["date"] <= pd.Timestamp(end)
        part = weekly[mask].reset_index(drop=True)
        base_series = part["baseline_score"] if "baseline_score" in part else None
        out[name] = score_period(part.drop(columns=["baseline_score"], errors="ignore"), formula["direction"],
                                 n_perm=n_perm, baseline_score=base_series)
    return out


def point_in_time_rows(formula: Mapping[str, Any], vintages: List[Mapping[str, Any]], labels: pd.DataFrame) -> pd.DataFrame:
    """Signal rows computed only from what each archive saw on its capture day.

    ``labels`` (base, date, fwd30_maxret) may come from later archives:
    outcomes are closes of finished bars, which later archives do not revise.
    """
    parts = []
    for vintage in vintages:
        frame = vintage["frame"]
        rows = frame.assign(feature=feature_panel(formula, frame).to_numpy())
        rows = rows[(rows["date"] == vintage["signal_date"]) & rows["mcap"].between(MCAP_MIN, MCAP_MAX)]
        rows = rows[["base", "date", "feature"]]
        baseline = vintage.get("baseline")
        if baseline is not None and len(baseline):
            rows = rows.merge(baseline[["base", "date", "baseline_score"]], on=["base", "date"], how="left")
        parts.append(rows)
    if not parts:
        return pd.DataFrame(columns=["base", "date", "feature", "pump", "fwd30_maxret"])
    rows = pd.concat(parts, ignore_index=True).merge(labels[["base", "date", "fwd30_maxret"]], on=["base", "date"], how="left")
    rows = rows[rows["fwd30_maxret"].notna()]
    return rows.assign(pump=(rows["fwd30_maxret"] >= PUMP_THRESHOLD).astype(int)).reset_index(drop=True)


def evaluate_formula_forward(
    formula: Mapping[str, Any],
    vintages: List[Mapping[str, Any]],
    labels: pd.DataFrame,
    *,
    after: pd.Timestamp,
    n_perm: int = 300,
) -> Dict[str, Any]:
    """Post-freeze verdict metrics from archives captured strictly after ``after`` (naive UTC)."""
    used = [v for v in vintages if v["captured_at"] > after]
    rows = point_in_time_rows(formula, used, labels)
    base_series = rows["baseline_score"] if "baseline_score" in rows else None
    result = score_period(rows.drop(columns=["baseline_score"], errors="ignore"), formula["direction"],
                          n_perm=n_perm, baseline_score=base_series)
    result.update(method="point_in_time_vintages", vintages=len(used))
    return result


def baseline_scores(daily: pd.DataFrame, model: Mapping[str, Any], *, dates: Optional[Iterable] = None) -> pd.DataFrame:
    """The validated weekly model's score on every Monday row, or on ``dates`` (for redundancy checks)."""
    from core.research.pump_precursor import FEATURES, build_daily_features

    keep = None if dates is None else pd.DatetimeIndex(pd.to_datetime(list(dates)))
    parts = []
    for base, grp in daily.groupby("base", sort=False):
        frame = build_daily_features(grp.set_index("date").sort_index()[["close", "volume", "oi", "mcap", "funding"]])
        frame = frame[frame.index.dayofweek == 0] if keep is None else frame[frame.index.isin(keep)]
        frame = frame.assign(base=base).reset_index().rename(columns={"index": "date"})
        parts.append(frame[["base", "date"] + FEATURES])
    feats = pd.concat(parts, ignore_index=True)
    ranks = feats.groupby("date")[FEATURES].rank(pct=True)
    weights = np.asarray(model["weights"], dtype=float)
    z = ((ranks.to_numpy(dtype=float) - 0.5) * weights).sum(axis=1) + float(model["bias"])
    feats["baseline_score"] = 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))
    feats.loc[ranks.isna().any(axis=1).to_numpy(), "baseline_score"] = np.nan
    return feats[["base", "date", "baseline_score"]]


def spearman(a: pd.Series, b: pd.Series) -> float:
    """Rank correlation with elementwise ops only.

    pandas' method="spearman" goes through scipy -> numpy.corrcoef -> BLAS,
    which dies natively in this env (Windows 0xc06d007f, exit 127).
    """
    frame = pd.DataFrame({"a": a.to_numpy(), "b": b.to_numpy()}).dropna()
    if len(frame) < 3:
        return float("nan")
    x = frame["a"].rank().to_numpy(dtype=float)
    y = frame["b"].rank().to_numpy(dtype=float)
    x -= x.mean()
    y -= y.mean()
    denom = math.sqrt(float((x * x).sum()) * float((y * y).sum()))
    return float((x * y).sum() / denom) if denom > 0 else float("nan")


def bonferroni_z(n_trials: int, alpha: float = 0.05) -> float:
    """One-sided normal threshold for alpha / n_trials (bisection on erf, no scipy)."""
    target = 1.0 - alpha / max(1, int(n_trials))
    lo, hi = 0.0, 10.0
    for _ in range(100):
        mid = (lo + hi) / 2
        if 0.5 * (1 + math.erf(mid / math.sqrt(2))) < target:
            lo = mid
        else:
            hi = mid
    return round((lo + hi) / 2, 3)


def _round(value: float, digits: int = 4) -> Optional[float]:
    return round(float(value), digits) if value is not None and math.isfinite(float(value)) else None
