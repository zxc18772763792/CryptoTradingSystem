"""Leakage-resistant research helpers for extreme Binance USD-M run-ups.

This module is intentionally isolated from the live strategy stack.  It contains
only data preparation, model validation, event de-duplication, and paper
simulation helpers.  Nothing in this file can place or route an order.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score


PRICE_FACTORS: tuple[str, ...] = (
    "realized_vol_7d",
    "intraday_range_1d",
    "atr_7d_pct",
    "upper_wick_pct",
    "breakout_vs_prior_20d",
    "drawdown_from_30d_high",
    "lower_wick_pct",
    "range_compression_7d_30d",
)

FRAGILITY_FACTORS: tuple[str, ...] = (
    "realized_vol_7d",
    "intraday_range_1d",
    "atr_7d_pct",
)

OI_FACTORS: tuple[str, ...] = (
    "log_oi_usd",
    "log_mcap_usd",
    "oi_to_mcap",
    "oi_change_1d",
    "oi_change_3d",
    "oi_change_7d",
    "oi_volatility_3d",
    "funding_rate_daily",
)

TARGET_HORIZONS: tuple[int, ...] = (3, 7, 14, 30)
TARGET_RETURNS: tuple[float, ...] = (0.50, 1.00, 1.50, 2.00, 3.00, 5.00, 9.00)
PRIMARY_HORIZON = 14
PRIMARY_RETURN = 2.0


def json_ready(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, Mapping):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_ready(item) for item in value]
    return value


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(json_ready(payload), ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class FrozenLogisticModel:
    factors: tuple[str, ...]
    medians: np.ndarray
    lower_bounds: np.ndarray
    upper_bounds: np.ndarray
    means: np.ndarray
    scales: np.ndarray
    coefficients: np.ndarray

    def predict_score(self, rows: pd.DataFrame) -> np.ndarray:
        raw = rows.loc[:, list(self.factors)].to_numpy(dtype=float)
        raw = np.where(np.isfinite(raw), raw, np.nan)
        values = np.where(np.isnan(raw), self.medians, raw)
        values = np.clip(values, self.lower_bounds, self.upper_bounds)
        standardized = (values - self.means) / self.scales
        linear = self.coefficients[0] + standardized @ self.coefficients[1:]
        linear = np.clip(linear, -30.0, 30.0)
        return 1.0 / (1.0 + np.exp(-linear))

    def to_json(self) -> dict[str, Any]:
        return {
            "model": "winsorized ridge logistic regression fitted by deterministic scikit-learn liblinear",
            "factors": list(self.factors),
            "medians": self.medians.tolist(),
            "lower_bounds": self.lower_bounds.tolist(),
            "upper_bounds": self.upper_bounds.tolist(),
            "means": self.means.tolist(),
            "scales": self.scales.tolist(),
            "coefficients_with_intercept": self.coefficients.tolist(),
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "FrozenLogisticModel":
        return cls(
            factors=tuple(str(item) for item in payload["factors"]),
            medians=np.asarray(payload["medians"], dtype=float),
            lower_bounds=np.asarray(payload["lower_bounds"], dtype=float),
            upper_bounds=np.asarray(payload["upper_bounds"], dtype=float),
            means=np.asarray(payload["means"], dtype=float),
            scales=np.asarray(payload["scales"], dtype=float),
            coefficients=np.asarray(payload["coefficients_with_intercept"], dtype=float),
        )


def fit_frozen_logistic(
    train: pd.DataFrame,
    factors: Sequence[str],
    *,
    label_column: str = "label_primary_int",
) -> FrozenLogisticModel:
    factor_names = tuple(factors)
    raw = train.loc[:, list(factor_names)].to_numpy(dtype=float)
    raw = np.where(np.isfinite(raw), raw, np.nan)
    medians = np.nanmedian(raw, axis=0)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    values = np.where(np.isnan(raw), medians, raw)
    lower_bounds = np.nanquantile(values, 0.01, axis=0)
    upper_bounds = np.nanquantile(values, 0.99, axis=0)
    values = np.clip(values, lower_bounds, upper_bounds)
    means = values.mean(axis=0)
    scales = values.std(axis=0)
    scales = np.where(scales > 1e-12, scales, 1.0)
    standardized = (values - means) / scales
    target = train[label_column].to_numpy(dtype=float)

    date_counts = train.groupby("date")["symbol"].transform("count").clip(lower=1)
    sample_weight = 1.0 / date_counts
    positive_count = max(float(target.sum()), 1.0)
    negative_count = max(float(len(target) - target.sum()), 1.0)
    class_weight = np.where(
        target > 0,
        len(target) / (2.0 * positive_count),
        len(target) / (2.0 * negative_count),
    )
    weights = sample_weight.to_numpy(dtype=float) * class_weight
    weights = weights / weights.mean()

    estimator = LogisticRegression(
        C=1.0,
        solver="liblinear",
        max_iter=300,
        random_state=20260718,
    )
    estimator.fit(standardized, target.astype(int), sample_weight=weights)
    coefficients = np.concatenate(
        [np.asarray(estimator.intercept_, dtype=float), np.asarray(estimator.coef_[0], dtype=float)]
    )
    return FrozenLogisticModel(
        factors=factor_names,
        medians=medians,
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
        means=means,
        scales=scales,
        coefficients=coefficients,
    )


def add_forward_targets(
    panel: pd.DataFrame,
    *,
    horizons: Sequence[int] = TARGET_HORIZONS,
) -> pd.DataFrame:
    """Add past-observable-entry forward targets for multiple horizons.

    A row for UTC day ``t`` enters at the next available UTC day's open.  A
    target is complete only when every expected daily row through the horizon is
    present and the dates are consecutive; this prevents listing gaps from being
    mistaken for a shorter calendar window.
    """

    output: list[pd.DataFrame] = []
    for _, raw_group in panel.groupby("symbol", sort=False):
        group = raw_group.sort_values("date").copy().reset_index(drop=True)
        dates = pd.to_datetime(group["date"], utc=True)
        highs = pd.to_numeric(group["high"], errors="coerce").to_numpy(float)
        lows = pd.to_numeric(group["low"], errors="coerce").to_numpy(float)
        closes = pd.to_numeric(group["close"], errors="coerce").to_numpy(float)
        opens = pd.to_numeric(group["open"], errors="coerce").to_numpy(float)
        count = len(group)
        for horizon in horizons:
            entry_open = np.full(count, np.nan)
            future_high = np.full(count, np.nan)
            future_low = np.full(count, np.nan)
            future_close = np.full(count, np.nan)
            coverage = np.zeros(count, dtype=int)
            complete = np.zeros(count, dtype=bool)
            for index in range(count):
                start = index + 1
                stop = min(count, index + int(horizon) + 1)
                if start >= stop:
                    continue
                entry_open[index] = opens[start]
                future_high[index] = float(np.nanmax(highs[start:stop]))
                future_low[index] = float(np.nanmin(lows[start:stop]))
                future_close[index] = closes[stop - 1]
                coverage[index] = stop - start
                expected_last = dates.iloc[index] + pd.Timedelta(days=int(horizon))
                complete[index] = coverage[index] == int(horizon) and dates.iloc[stop - 1] == expected_last
            suffix = f"_{int(horizon)}d"
            group[f"entry_open{suffix}"] = entry_open
            group[f"future_high{suffix}"] = future_high
            group[f"future_low{suffix}"] = future_low
            group[f"future_close{suffix}"] = future_close
            group[f"coverage_days{suffix}"] = coverage
            group[f"target_complete{suffix}"] = complete
            group[f"target_max_return{suffix}"] = future_high / entry_open - 1.0
            group[f"target_min_return{suffix}"] = future_low / entry_open - 1.0
            group[f"target_close_return{suffix}"] = future_close / entry_open - 1.0
        output.append(group)
    return pd.concat(output, ignore_index=True) if output else panel.copy()


def add_forward_targets_from_4h(
    daily_panel: pd.DataFrame,
    panel_4h: pd.DataFrame,
    *,
    horizons: Sequence[int] = TARGET_HORIZONS,
    entry_hour_utc: int = 4,
) -> pd.DataFrame:
    """Build targets from the first 4h open strictly after the 08:20 monitor.

    The UTC daily bar for day ``t`` is complete at ``t+1 00:00``.  The monitor
    runs at 00:20 UTC, so the next executable 4h open is ``t+1 04:00``.  The
    target path begins at that bar and excludes all earlier bars.
    """

    output: list[pd.DataFrame] = []
    four_hours_ns = int(pd.Timedelta(hours=4).value)
    bars_by_symbol = {
        str(symbol): group.sort_values("open_time")
        for symbol, group in panel_4h.groupby("symbol", sort=False)
    }
    for symbol, raw_daily in daily_panel.groupby("symbol", sort=False):
        daily = raw_daily.sort_values("date").copy().reset_index(drop=True)
        bars = bars_by_symbol.get(str(symbol), pd.DataFrame())
        if bars.empty:
            output.append(daily)
            continue
        bar_times = pd.DatetimeIndex(pd.to_datetime(bars["open_time"], utc=True)).as_unit("ns").asi8
        opens = pd.to_numeric(bars["open"], errors="coerce").to_numpy(float)
        highs = pd.to_numeric(bars["high"], errors="coerce").to_numpy(float)
        lows = pd.to_numeric(bars["low"], errors="coerce").to_numpy(float)
        closes = pd.to_numeric(bars["close"], errors="coerce").to_numpy(float)
        feature_dates = pd.to_datetime(daily["date"], utc=True)
        entry_times = feature_dates + pd.Timedelta(days=1, hours=entry_hour_utc)
        entry_ns_values = pd.DatetimeIndex(entry_times).as_unit("ns").asi8
        count = len(daily)
        for horizon in horizons:
            entry_open = np.full(count, np.nan)
            future_high = np.full(count, np.nan)
            future_low = np.full(count, np.nan)
            future_close = np.full(count, np.nan)
            coverage = np.zeros(count, dtype=int)
            complete = np.zeros(count, dtype=bool)
            expected_bars = int(horizon) * 6
            horizon_ns = int(pd.Timedelta(days=int(horizon)).value)
            for index, entry_ns in enumerate(entry_ns_values):
                start = int(np.searchsorted(bar_times, entry_ns, side="left"))
                stop_ns = int(entry_ns + horizon_ns)
                stop = int(np.searchsorted(bar_times, stop_ns, side="left"))
                if start >= stop or start >= len(bar_times) or bar_times[start] != entry_ns:
                    continue
                path_highs = highs[start:stop]
                path_lows = lows[start:stop]
                entry_open[index] = opens[start]
                future_high[index] = float(np.nanmax(path_highs))
                future_low[index] = float(np.nanmin(path_lows))
                future_close[index] = closes[stop - 1]
                coverage[index] = stop - start
                complete[index] = (
                    coverage[index] == expected_bars
                    and bar_times[stop - 1] == stop_ns - four_hours_ns
                )
            suffix = f"_{int(horizon)}d"
            daily[f"entry_time{suffix}"] = entry_times
            daily[f"entry_open{suffix}"] = entry_open
            daily[f"future_high{suffix}"] = future_high
            daily[f"future_low{suffix}"] = future_low
            daily[f"future_close{suffix}"] = future_close
            daily[f"coverage_bars{suffix}"] = coverage
            daily[f"target_complete{suffix}"] = complete
            daily[f"target_max_return{suffix}"] = future_high / entry_open - 1.0
            daily[f"target_min_return{suffix}"] = future_low / entry_open - 1.0
            daily[f"target_close_return{suffix}"] = future_close / entry_open - 1.0
        output.append(daily)
    return pd.concat(output, ignore_index=True) if output else daily_panel.copy()


def prepare_modeling_panel(panel: pd.DataFrame) -> pd.DataFrame:
    rows = panel.copy()
    rows["date"] = pd.to_datetime(rows["date"], utc=True)
    if "target_complete_14d" not in rows.columns:
        rows = add_forward_targets(rows)
    primary_complete = rows["target_complete_14d"].fillna(False)
    eligible = (
        rows["is_altcoin"].fillna(False)
        & primary_complete
        & (pd.to_numeric(rows["quote_volume"], errors="coerce") >= 250_000.0)
    )
    rows = rows.loc[eligible].copy()
    rows["label_primary"] = rows["target_max_return_14d"] >= PRIMARY_RETURN
    rows["label_primary_int"] = rows["label_primary"].astype(int)
    fragility_parts = [
        rows[factor].groupby(rows["date"]).rank(pct=True).fillna(0.5)
        for factor in FRAGILITY_FACTORS
    ]
    rows["fragility_score"] = sum(fragility_parts) / len(fragility_parts)
    return rows.sort_values(["date", "symbol"]).reset_index(drop=True)


def expanding_walk_forward_splits(
    rows: pd.DataFrame,
    *,
    minimum_train_days: int = 180,
    purge_days: int = 14,
    test_days: int = 30,
) -> list[dict[str, Any]]:
    dates = pd.Series(pd.to_datetime(rows["date"], utc=True).dropna().unique()).sort_values()
    if dates.empty:
        return []
    minimum_date = pd.Timestamp(dates.iloc[0])
    maximum_date = pd.Timestamp(dates.iloc[-1])
    test_start = minimum_date + pd.Timedelta(days=minimum_train_days + purge_days)
    folds: list[dict[str, Any]] = []
    fold_id = 0
    while test_start <= maximum_date:
        test_end = min(test_start + pd.Timedelta(days=test_days - 1), maximum_date)
        train_cutoff = test_start - pd.Timedelta(days=purge_days)
        train_mask = rows["date"] < train_cutoff
        test_mask = rows["date"].between(test_start, test_end)
        train = rows.loc[train_mask]
        test = rows.loc[test_mask]
        calendar_train_days = int((train_cutoff - minimum_date).days)
        if calendar_train_days >= minimum_train_days and not train.empty and not test.empty:
            folds.append(
                {
                    "fold": fold_id,
                    "train_start": train["date"].min(),
                    "train_end": train["date"].max(),
                    "purge_start": train_cutoff,
                    "purge_end": test_start - pd.Timedelta(days=1),
                    "test_start": test_start,
                    "test_end": test_end,
                    "train_index": train.index.to_numpy(),
                    "test_index": test.index.to_numpy(),
                }
            )
            fold_id += 1
        test_start += pd.Timedelta(days=test_days)
    return folds


def score_metrics(rows: pd.DataFrame, score_column: str, label_column: str) -> dict[str, Any]:
    data = rows[["date", score_column, label_column]].dropna().copy()
    target = data[label_column].astype(int)
    if data.empty or target.nunique() < 2:
        return {
            "n": int(len(data)),
            "positives": int(target.sum()) if not data.empty else 0,
            "base_rate": float(target.mean()) if not data.empty else None,
            "auc": None,
            "average_precision": None,
            "top_2pct_n": 0,
            "top_2pct_precision": None,
            "top_2pct_lift": None,
        }
    rank = data[score_column].groupby(data["date"]).rank(pct=True, method="first")
    selected = target[rank >= 0.98]
    base_rate = float(target.mean())
    precision = float(selected.mean()) if not selected.empty else np.nan
    return {
        "n": int(len(data)),
        "positives": int(target.sum()),
        "base_rate": base_rate,
        "auc": float(roc_auc_score(target, data[score_column])),
        "average_precision": float(average_precision_score(target, data[score_column])),
        "top_2pct_n": int(len(selected)),
        "top_2pct_precision": float(precision) if np.isfinite(precision) else None,
        "top_2pct_lift": float(precision / base_rate) if base_rate > 0 and np.isfinite(precision) else None,
    }


def run_price_walk_forward(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    folds = expanding_walk_forward_splits(rows)
    scored_frames: list[pd.DataFrame] = []
    records: list[dict[str, Any]] = []
    for fold in folds:
        train = rows.loc[fold["train_index"]].copy()
        test = rows.loc[fold["test_index"]].copy()
        if int(train["label_primary_int"].sum()) < 10 or int(test["label_primary_int"].sum()) < 3:
            continue
        model = fit_frozen_logistic(train, PRICE_FACTORS)
        test["price_model_score"] = model.predict_score(test)
        test["price_model_pctile"] = test["price_model_score"].groupby(test["date"]).rank(
            pct=True, method="first"
        )
        test["fragility_pctile"] = test["fragility_score"].groupby(test["date"]).rank(
            pct=True, method="first"
        )
        test["fold"] = int(fold["fold"])
        scored_frames.append(test)
        price_metrics = score_metrics(test, "price_model_score", "label_primary_int")
        simple_metrics = score_metrics(test, "fragility_score", "label_primary_int")
        records.append(
            {
                "fold": int(fold["fold"]),
                "train_start": fold["train_start"],
                "train_end": fold["train_end"],
                "purge_start": fold["purge_start"],
                "purge_end": fold["purge_end"],
                "test_start": fold["test_start"],
                "test_end": fold["test_end"],
                "train_rows": int(len(train)),
                "train_positives": int(train["label_primary_int"].sum()),
                "test_rows": int(len(test)),
                "test_positives": int(test["label_primary_int"].sum()),
                **{f"price_{key}": value for key, value in price_metrics.items()},
                **{f"simple_{key}": value for key, value in simple_metrics.items()},
            }
        )
    scored = pd.concat(scored_frames, ignore_index=True) if scored_frames else pd.DataFrame()
    return scored, pd.DataFrame(records)


def threshold_horizon_surface(scored: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    if scored.empty:
        return pd.DataFrame()
    for horizon in TARGET_HORIZONS:
        return_column = f"target_max_return_{horizon}d"
        complete_column = f"target_complete_{horizon}d"
        eligible = scored[scored[complete_column].fillna(False) & scored[return_column].notna()].copy()
        for threshold in TARGET_RETURNS:
            label = eligible[return_column] >= threshold
            selected_mask = eligible["price_model_pctile"] >= 0.98
            selected = label[selected_mask]
            base_rate = float(label.mean()) if len(label) else np.nan
            precision = float(selected.mean()) if len(selected) else np.nan
            selected_positive = int(selected.sum()) if len(selected) else 0
            selected_negative = int(len(selected) - selected_positive)
            other_positive = int(label.sum() - selected_positive)
            other_negative = int((~label).sum() - selected_negative)
            p_value = np.nan
            if len(selected) and label.sum() > 0:
                _, p_value = fisher_exact(
                    [[selected_positive, selected_negative], [other_positive, other_negative]],
                    alternative="greater",
                )
            records.append(
                {
                    "horizon_days": horizon,
                    "threshold_return": threshold,
                    "threshold_pct": int(threshold * 100),
                    "rows": int(len(label)),
                    "positive_rows": int(label.sum()),
                    "selected_rows": int(len(selected)),
                    "selected_positive_rows": selected_positive,
                    "base_rate": base_rate,
                    "precision": precision,
                    "lift": precision / base_rate if base_rate > 0 and np.isfinite(precision) else np.nan,
                    "p_value_one_sided": p_value,
                }
            )
    surface = pd.DataFrame(records)
    surface["p_value_bh"] = benjamini_hochberg(surface["p_value_one_sided"])
    return surface


def benjamini_hochberg(values: Iterable[float]) -> np.ndarray:
    series = pd.Series(values, dtype=float)
    valid = series.dropna().sort_values()
    adjusted = pd.Series(np.nan, index=series.index, dtype=float)
    if valid.empty:
        return adjusted.to_numpy()
    count = len(valid)
    raw = valid.to_numpy() * count / np.arange(1, count + 1)
    monotone = np.minimum.accumulate(raw[::-1])[::-1]
    adjusted.loc[valid.index] = np.clip(monotone, 0, 1)
    return adjusted.to_numpy()


def independent_event_episodes(
    scored: pd.DataFrame,
    *,
    horizon_days: int = PRIMARY_HORIZON,
    threshold_return: float = PRIMARY_RETURN,
    score_pctile_column: str = "price_model_pctile",
) -> pd.DataFrame:
    if scored.empty:
        return pd.DataFrame()
    return_column = f"target_max_return_{horizon_days}d"
    positive = scored[
        scored[f"target_complete_{horizon_days}d"].fillna(False)
        & (scored[return_column] >= threshold_return)
    ].sort_values(["symbol", "date"]).copy()
    events: list[dict[str, Any]] = []
    for symbol, group in positive.groupby("symbol"):
        cluster: list[pd.Series] = []
        previous_date: pd.Timestamp | None = None
        for _, row in group.iterrows():
            date = pd.Timestamp(row["date"])
            if previous_date is not None and (date - previous_date).days > horizon_days:
                events.append(_summarize_event_cluster(symbol, cluster, return_column, score_pctile_column))
                cluster = []
            cluster.append(row)
            previous_date = date
        if cluster:
            events.append(_summarize_event_cluster(symbol, cluster, return_column, score_pctile_column))
    return pd.DataFrame(events)


def _summarize_event_cluster(
    symbol: str,
    rows: Sequence[pd.Series],
    return_column: str,
    score_pctile_column: str,
) -> dict[str, Any]:
    frame = pd.DataFrame(rows)
    captured = frame[frame[score_pctile_column] >= 0.98]
    return {
        "event_id": f"{symbol}#{pd.Timestamp(frame['date'].min()).date().isoformat()}",
        "symbol": symbol,
        "event_start": frame["date"].min(),
        "event_end": frame["date"].max(),
        "positive_rows": int(len(frame)),
        "max_forward_return": float(frame[return_column].max()),
        "max_score_pctile": float(frame[score_pctile_column].max()),
        "captured_top_2pct": bool(not captured.empty),
        "first_capture_date": captured["date"].min() if not captured.empty else pd.NaT,
    }


def bootstrap_lift_by_week(
    rows: pd.DataFrame,
    *,
    score_pctile_column: str = "price_model_pctile",
    label_column: str = "label_primary_int",
    samples: int = 2000,
    seed: int = 20260718,
) -> dict[str, Any]:
    data = rows[["date", score_pctile_column, label_column]].dropna().copy()
    data["week"] = data["date"].dt.to_period("W-SUN").dt.start_time
    weekly: list[tuple[int, int, int, int]] = []
    for _, group in data.groupby("week"):
        selected = group[group[score_pctile_column] >= 0.98]
        weekly.append(
            (
                int(group[label_column].sum()),
                int(len(group)),
                int(selected[label_column].sum()),
                int(len(selected)),
            )
        )
    if not weekly:
        return {"samples": 0, "median": None, "lower_95pct": None, "upper_95pct": None}
    values = np.asarray(weekly, dtype=float)
    rng = np.random.default_rng(seed)
    lifts: list[float] = []
    for _ in range(samples):
        totals = values[rng.integers(0, len(values), len(values))].sum(axis=0)
        base_rate = totals[0] / totals[1] if totals[1] else np.nan
        top_rate = totals[2] / totals[3] if totals[3] else np.nan
        if base_rate > 0 and np.isfinite(top_rate):
            lifts.append(float(top_rate / base_rate))
    series = pd.Series(lifts, dtype=float)
    return {
        "samples": int(len(series)),
        "median": float(series.median()) if not series.empty else None,
        "lower_95pct": float(series.quantile(0.025)) if not series.empty else None,
        "upper_95pct": float(series.quantile(0.975)) if not series.empty else None,
    }


def bootstrap_independent_event_capture(
    rows: pd.DataFrame,
    *,
    samples: int = 2000,
    seed: int = 20260719,
) -> dict[str, Any]:
    events = independent_event_episodes(rows)
    if events.empty:
        return {"samples": 0, "median": None, "lower_95pct": None, "upper_95pct": None}
    captured = events["captured_top_2pct"].astype(float).to_numpy()
    rng = np.random.default_rng(seed)
    estimates = np.empty(samples, dtype=float)
    for index in range(samples):
        estimates[index] = captured[rng.integers(0, len(captured), len(captured))].mean()
    return {
        "samples": int(samples),
        "median": float(np.median(estimates)),
        "lower_95pct": float(np.quantile(estimates, 0.025)),
        "upper_95pct": float(np.quantile(estimates, 0.975)),
    }


def load_exchange_metadata(snapshot_path: Path) -> pd.DataFrame:
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    records: list[dict[str, Any]] = []
    for item in payload.get("symbols", []):
        if item.get("quoteAsset") != "USDT" or item.get("contractType") != "PERPETUAL":
            continue
        records.append(
            {
                "symbol": str(item.get("symbol")),
                "onboard_date": pd.to_datetime(item.get("onboardDate"), unit="ms", utc=True, errors="coerce"),
                "exchange_status": item.get("status"),
                "delivery_date": pd.to_datetime(item.get("deliveryDate"), unit="ms", utc=True, errors="coerce"),
            }
        )
    return pd.DataFrame(records).drop_duplicates("symbol", keep="last")


def add_segments(
    scored: pd.DataFrame,
    *,
    exchange_metadata: pd.DataFrame,
    spot_symbols: set[str] | None = None,
    market_context: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows = scored.merge(exchange_metadata, on="symbol", how="left", validate="many_to_one")
    rows["listing_age_days"] = (rows["date"] - rows["onboard_date"]).dt.total_seconds() / 86400.0
    rows["listing_age_bucket"] = pd.cut(
        rows["listing_age_days"],
        bins=[-np.inf, 30, 90, 180, np.inf],
        labels=["0-30d", "31-90d", "91-180d", ">180d"],
    ).astype(str)
    rows["spot_listing"] = rows["symbol"].isin(spot_symbols or set())
    volume_rank = rows["quote_volume"].groupby(rows["date"]).rank(pct=True, method="average")
    rows["liquidity_bucket"] = pd.cut(
        volume_rank,
        bins=[0, 0.2, 0.4, 0.6, 0.8, 1.000001],
        labels=["Q1-low", "Q2", "Q3", "Q4", "Q5-high"],
        include_lowest=True,
    ).astype(str)
    ordered = rows.sort_values(["symbol", "date"]).copy()
    rolling_mean = ordered.groupby("symbol")["close"].transform(
        lambda values: values.shift(1).rolling(20, min_periods=10).mean()
    )
    ordered["above_prior_20d_mean"] = ordered["close"] > rolling_mean
    breadth = ordered.groupby("date")["above_prior_20d_mean"].mean().rename("altcoin_breadth")
    ordered = ordered.merge(breadth, on="date", how="left", validate="many_to_one")
    expanding_median = (
        breadth.expanding(min_periods=30).median().shift(1).rename("breadth_expanding_median")
    )
    ordered = ordered.merge(expanding_median, left_on="date", right_index=True, how="left")
    ordered["breadth_regime"] = np.where(
        ordered["altcoin_breadth"] >= ordered["breadth_expanding_median"],
        "broad",
        "narrow",
    )
    if market_context is not None and not market_context.empty:
        ordered = ordered.merge(market_context, on="date", how="left", validate="many_to_one")
        ordered["market_state"] = (
            ordered["btc_trend_regime"].fillna("unknown")
            + "_"
            + ordered["btc_vol_regime"].fillna("unknown")
            + "_"
            + ordered["breadth_regime"].fillna("unknown")
        )
    return ordered.sort_values(["date", "symbol"]).reset_index(drop=True)


def build_market_context(daily_panel: pd.DataFrame) -> pd.DataFrame:
    btc = daily_panel[daily_panel["symbol"] == "BTCUSDT"].sort_values("date").copy()
    if btc.empty:
        return pd.DataFrame()
    btc["date"] = pd.to_datetime(btc["date"], utc=True)
    returns = pd.to_numeric(btc["close"], errors="coerce").pct_change(fill_method=None)
    prior_mean = pd.to_numeric(btc["close"], errors="coerce").shift(1).rolling(20, min_periods=10).mean()
    btc_vol = returns.shift(1).rolling(20, min_periods=10).std() * np.sqrt(365)
    prior_vol_median = btc_vol.expanding(min_periods=30).median().shift(1)
    return pd.DataFrame(
        {
            "date": btc["date"],
            "btc_trend_regime": np.where(btc["close"] >= prior_mean, "bull", "bear"),
            "btc_vol_regime": np.where(btc_vol >= prior_vol_median, "highvol", "lowvol"),
            "btc_realized_vol_20d": btc_vol,
        }
    )


def segment_metrics(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for dimension in [
        "listing_age_bucket",
        "spot_listing",
        "liquidity_bucket",
        "btc_trend_regime",
        "btc_vol_regime",
        "breadth_regime",
        "market_state",
    ]:
        if dimension not in rows.columns:
            continue
        for segment, group in rows.groupby(dimension, dropna=False):
            metrics = score_metrics(group, "price_model_score", "label_primary_int")
            events = independent_event_episodes(group)
            records.append(
                {
                    "dimension": dimension,
                    "segment": str(segment),
                    "independent_events": int(len(events)),
                    "captured_events": int(events["captured_top_2pct"].sum()) if not events.empty else 0,
                    "conclusion_allowed": bool(len(events) >= 5),
                    **metrics,
                }
            )
    return pd.DataFrame(records)


def load_long_oi_panel(ambush_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    manifest = json.loads((ambush_root / "manifest.json").read_text(encoding="utf-8"))
    frames: list[pd.DataFrame] = []
    source_errors: list[str] = []
    supply_approximation_symbols: list[str] = []
    for base_asset, item in manifest.get("symbols", {}).items():
        symbol = str(item.get("symbol") or f"{base_asset}USDT")
        try:
            oi_path = ambush_root / "oi_1d" / f"{base_asset}.parquet"
            mcap_path = ambush_root / "mcap_1d" / f"{base_asset}.parquet"
            funding_path = ambush_root / "funding" / f"{base_asset}.parquet"
            if not oi_path.exists() or not mcap_path.exists():
                continue
            oi = pd.read_parquet(oi_path)[["oi_usd"]].copy()
            oi.index = pd.to_datetime(oi.index, utc=True).normalize() + pd.Timedelta(days=1)
            oi = oi[~oi.index.duplicated(keep="last")]
            mcap = pd.read_parquet(mcap_path)[["mcap_usd"]].copy()
            mcap.index = pd.to_datetime(mcap.index, utc=True).normalize() + pd.Timedelta(days=1)
            mcap = mcap[~mcap.index.duplicated(keep="last")]
            joined = oi.join(mcap, how="inner")
            if funding_path.exists():
                funding = pd.read_parquet(funding_path)[["funding_rate"]].copy()
                funding.index = pd.to_datetime(funding.index, utc=True)
                funding_daily = funding["funding_rate"].resample("1D").mean().rename("funding_rate_daily")
                joined = joined.join(funding_daily, how="left")
            else:
                joined["funding_rate_daily"] = np.nan
            joined = joined.sort_index()
            joined["oi_change_1d"] = joined["oi_usd"].pct_change(1, fill_method=None)
            joined["oi_change_3d"] = joined["oi_usd"].pct_change(3, fill_method=None)
            joined["oi_change_7d"] = joined["oi_usd"].pct_change(7, fill_method=None)
            joined["oi_volatility_3d"] = joined["oi_usd"].pct_change(fill_method=None).rolling(3).std()
            joined["oi_to_mcap"] = joined["oi_usd"] / joined["mcap_usd"]
            joined["log_oi_usd"] = np.log1p(joined["oi_usd"].clip(lower=0))
            joined["log_mcap_usd"] = np.log1p(joined["mcap_usd"].clip(lower=0))
            joined["symbol"] = symbol
            joined["base_asset"] = str(base_asset)
            joined["date"] = joined.index
            joined["mcap_source"] = "historical_coingecko"
            if item.get("cg_id") is None:
                joined["mcap_source"] = "current_supply_price_approximation"
                supply_approximation_symbols.append(symbol)
            frames.append(joined.reset_index(drop=True))
        except Exception as exc:  # noqa: BLE001 - keep per-symbol gaps auditable
            source_errors.append(f"{symbol}: {type(exc).__name__}: {exc}")
    panel = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    quality = {
        "manifest_symbols": int(len(manifest.get("symbols", {}))),
        "loaded_symbols": int(panel["symbol"].nunique()) if not panel.empty else 0,
        "rows": int(len(panel)),
        "min_date": panel["date"].min() if not panel.empty else None,
        "max_date": panel["date"].max() if not panel.empty else None,
        "source_errors": source_errors,
        "supply_approximation_symbols": sorted(set(supply_approximation_symbols)),
    }
    return panel, quality


def run_oi_walk_forward(
    modeling: pd.DataFrame,
    oi_panel: pd.DataFrame,
    *,
    bootstrap_samples: int = 2000,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if oi_panel.empty:
        return pd.DataFrame(), pd.DataFrame(), {"ranking_eligible": False, "reason": "no_oi_panel"}
    join_columns = ["symbol", "date"]
    enriched = modeling.merge(oi_panel, on=join_columns, how="inner", suffixes=("", "_oi"), validate="many_to_one")
    primary_mcap = enriched["mcap_source"].eq("historical_coingecko")
    required = [*PRICE_FACTORS, *OI_FACTORS, "label_primary_int"]
    enriched = enriched.loc[primary_mcap & enriched[required].notna().all(axis=1)].copy()
    folds = expanding_walk_forward_splits(enriched)
    scored_frames: list[pd.DataFrame] = []
    records: list[dict[str, Any]] = []
    for fold in folds:
        train = enriched.loc[fold["train_index"]].copy()
        test = enriched.loc[fold["test_index"]].copy()
        if int(train["label_primary_int"].sum()) < 10 or int(test["label_primary_int"].sum()) < 2:
            continue
        price_model = fit_frozen_logistic(train, PRICE_FACTORS)
        oi_model = fit_frozen_logistic(train, OI_FACTORS)
        combo_model = fit_frozen_logistic(train, (*PRICE_FACTORS, *OI_FACTORS))
        ablation_factor_sets = {
            "small_cap": (*PRICE_FACTORS, "log_mcap_usd"),
            "absolute_oi": (*PRICE_FACTORS, "log_oi_usd"),
            "oi_to_mcap": (*PRICE_FACTORS, "oi_to_mcap"),
            "oi_growth": (*PRICE_FACTORS, "oi_change_1d", "oi_change_3d", "oi_change_7d"),
        }
        ablation_models = {
            name: fit_frozen_logistic(train, factors)
            for name, factors in ablation_factor_sets.items()
        }
        test["price_only_score"] = price_model.predict_score(test)
        test["oi_only_score"] = oi_model.predict_score(test)
        test["price_oi_score"] = combo_model.predict_score(test)
        for name, model in ablation_models.items():
            test[f"{name}_score"] = model.predict_score(test)
        score_columns = [
            "price_only_score", "oi_only_score", "price_oi_score",
            *[f"{name}_score" for name in ablation_models],
        ]
        for column in score_columns:
            test[f"{column}_pctile"] = test[column].groupby(test["date"]).rank(pct=True, method="first")
        test["fold"] = int(fold["fold"])
        scored_frames.append(test)
        price_metrics = score_metrics(test, "price_only_score", "label_primary_int")
        oi_metrics = score_metrics(test, "oi_only_score", "label_primary_int")
        combo_metrics = score_metrics(test, "price_oi_score", "label_primary_int")
        ablation_metrics = {
            name: score_metrics(test, f"{name}_score", "label_primary_int")
            for name in ablation_models
        }
        ablation_record: dict[str, Any] = {}
        for name, metrics in ablation_metrics.items():
            ablation_record[f"{name}_delta_average_precision"] = _safe_difference(
                metrics.get("average_precision"), price_metrics.get("average_precision")
            )
            ablation_record[f"{name}_delta_top_2pct_lift"] = _safe_difference(
                metrics.get("top_2pct_lift"), price_metrics.get("top_2pct_lift")
            )
        records.append(
            {
                "fold": int(fold["fold"]),
                "test_start": fold["test_start"],
                "test_end": fold["test_end"],
                "train_rows": int(len(train)),
                "train_positives": int(train["label_primary_int"].sum()),
                "test_rows": int(len(test)),
                "test_positives": int(test["label_primary_int"].sum()),
                **{f"price_{key}": value for key, value in price_metrics.items()},
                **{f"oi_{key}": value for key, value in oi_metrics.items()},
                **{f"combo_{key}": value for key, value in combo_metrics.items()},
                "delta_average_precision": _safe_difference(
                    combo_metrics.get("average_precision"), price_metrics.get("average_precision")
                ),
                "delta_top_2pct_lift": _safe_difference(
                    combo_metrics.get("top_2pct_lift"), price_metrics.get("top_2pct_lift")
                ),
                **ablation_record,
            }
        )
    scored = pd.concat(scored_frames, ignore_index=True) if scored_frames else pd.DataFrame()
    fold_metrics = pd.DataFrame(records)
    bootstrap = bootstrap_oi_increment(scored, samples=bootstrap_samples)
    majority = 0.0
    if not fold_metrics.empty:
        majority = float(
            (
                (fold_metrics["delta_average_precision"] > 0)
                & (fold_metrics["delta_top_2pct_lift"] > 0)
            ).mean()
        )
    ranking_eligible = bool(
        not fold_metrics.empty
        and majority > 0.5
        and (bootstrap.get("delta_ap_lower_95pct") or -np.inf) > 0
        and (bootstrap.get("delta_lift_lower_95pct") or -np.inf) > 0
    )
    decision = {
        "ranking_eligible": ranking_eligible,
        "folds": int(len(fold_metrics)),
        "fold_majority_both_positive": majority,
        "bootstrap": bootstrap,
        "factor_group_evidence": {
            name: {
                "fold_majority_ap_and_lift_positive": float(
                    (
                        (fold_metrics[f"{name}_delta_average_precision"] > 0)
                        & (fold_metrics[f"{name}_delta_top_2pct_lift"] > 0)
                    ).mean()
                ) if not fold_metrics.empty else 0.0,
                "median_delta_average_precision": float(
                    fold_metrics[f"{name}_delta_average_precision"].median()
                ) if not fold_metrics.empty else None,
                "median_delta_top_2pct_lift": float(
                    fold_metrics[f"{name}_delta_top_2pct_lift"].median()
                ) if not fold_metrics.empty else None,
                "evidence_tier": "directional_only",
            }
            for name in ["small_cap", "absolute_oi", "oi_to_mcap", "oi_growth"]
        },
        "rule": "majority of folds improve AP and top-2% lift, with both 95% lower bounds above zero",
    }
    return scored, fold_metrics, decision


def _safe_difference(left: Any, right: Any) -> float:
    if left is None or right is None:
        return np.nan
    return float(left) - float(right)


def bootstrap_oi_increment(
    scored: pd.DataFrame,
    *,
    samples: int = 2000,
    seed: int = 20260718,
) -> dict[str, Any]:
    if scored.empty:
        return {
            "samples": 0,
            "delta_ap_median": None,
            "delta_ap_lower_95pct": None,
            "delta_lift_median": None,
            "delta_lift_lower_95pct": None,
        }
    data = scored.copy()
    data["week"] = data["date"].dt.to_period("W-SUN").dt.start_time
    weeks = sorted(data["week"].dropna().unique())
    rng = np.random.default_rng(seed)
    delta_ap: list[float] = []
    delta_lift: list[float] = []
    for _ in range(samples):
        selected_weeks = rng.choice(weeks, size=len(weeks), replace=True)
        sampled = pd.concat([data[data["week"] == week] for week in selected_weeks], ignore_index=True)
        if sampled["label_primary_int"].nunique() < 2:
            continue
        price = score_metrics(sampled, "price_only_score", "label_primary_int")
        combo = score_metrics(sampled, "price_oi_score", "label_primary_int")
        ap_delta = _safe_difference(combo.get("average_precision"), price.get("average_precision"))
        lift_delta = _safe_difference(combo.get("top_2pct_lift"), price.get("top_2pct_lift"))
        if np.isfinite(ap_delta) and np.isfinite(lift_delta):
            delta_ap.append(ap_delta)
            delta_lift.append(lift_delta)
    ap = pd.Series(delta_ap, dtype=float)
    lift = pd.Series(delta_lift, dtype=float)
    return {
        "samples": int(min(len(ap), len(lift))),
        "delta_ap_median": float(ap.median()) if not ap.empty else None,
        "delta_ap_lower_95pct": float(ap.quantile(0.025)) if not ap.empty else None,
        "delta_ap_upper_95pct": float(ap.quantile(0.975)) if not ap.empty else None,
        "delta_lift_median": float(lift.median()) if not lift.empty else None,
        "delta_lift_lower_95pct": float(lift.quantile(0.025)) if not lift.empty else None,
        "delta_lift_upper_95pct": float(lift.quantile(0.975)) if not lift.empty else None,
    }


PAPER_POLICIES: dict[str, dict[str, Any]] = {
    "hold_14d": {
        "hard_stop": None,
        "time_days": 14,
        "trail_activation": None,
        "trail_distance": None,
        "partial_target": None,
    },
    "stop20_time14": {
        "hard_stop": 0.20,
        "time_days": 14,
        "trail_activation": None,
        "trail_distance": None,
        "partial_target": None,
    },
    "stop20_trail25_after50_time30": {
        "hard_stop": 0.20,
        "time_days": 30,
        "trail_activation": 0.50,
        "trail_distance": 0.25,
        "partial_target": None,
    },
    "stop20_half_at100_trail25_time30": {
        "hard_stop": 0.20,
        "time_days": 30,
        "trail_activation": 0.50,
        "trail_distance": 0.25,
        "partial_target": 1.00,
    },
}


def build_first_crossing_signals(scored: pd.DataFrame) -> pd.DataFrame:
    if scored.empty:
        return pd.DataFrame()
    rows = scored.sort_values(["symbol", "date"]).copy()
    previous = rows.groupby("symbol")["price_model_pctile"].shift(1).fillna(0.0)
    rows["first_crossing"] = (rows["price_model_pctile"] >= 0.98) & (previous < 0.98)
    signals = rows[rows["first_crossing"]].copy()
    signals = (
        signals.sort_values(["date", "price_model_score"], ascending=[True, False])
        .groupby("date", as_index=False, group_keys=False)
        .head(3)
        .reset_index(drop=True)
    )
    signals["entry_time"] = signals["date"] + pd.Timedelta(days=1, hours=4)
    return signals


def load_funding_history(ambush_root: Path) -> dict[str, pd.Series]:
    manifest_path = ambush_root / "manifest.json"
    if not manifest_path.exists():
        return {}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    output: dict[str, pd.Series] = {}
    for base_asset, item in manifest.get("symbols", {}).items():
        path = ambush_root / "funding" / f"{base_asset}.parquet"
        if not path.exists():
            continue
        frame = pd.read_parquet(path)
        if "funding_rate" not in frame.columns:
            continue
        index = pd.to_datetime(frame.index, utc=True)
        series = pd.Series(pd.to_numeric(frame["funding_rate"], errors="coerce").to_numpy(), index=index)
        output[str(item.get("symbol") or f"{base_asset}USDT")] = series.dropna().sort_index()
    return output


def simulate_trade_path(
    bars: pd.DataFrame,
    *,
    entry_time: pd.Timestamp,
    policy_name: str,
    funding: pd.Series | None = None,
    fee_bps: float = 5.0,
    slippage_bps: float = 5.0,
) -> dict[str, Any] | None:
    if policy_name not in PAPER_POLICIES:
        raise ValueError(f"Unknown policy: {policy_name}")
    return simulate_trade_path_with_policy(
        bars,
        entry_time=entry_time,
        policy=PAPER_POLICIES[policy_name],
        policy_name=policy_name,
        funding=funding,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
    )


def simulate_trade_path_with_policy(
    bars: pd.DataFrame,
    *,
    entry_time: pd.Timestamp,
    policy: Mapping[str, Any],
    policy_name: str = "custom_exit_policy",
    funding: pd.Series | None = None,
    fee_bps: float = 5.0,
    slippage_bps: float = 5.0,
) -> dict[str, Any] | None:
    """Simulate one long trade with an explicit, auditable exit policy.

    Exit ordering is deliberately conservative.  A hard or previously active
    trailing stop is evaluated before any profit target on the same 4h bar.
    Profit targets are resting limit orders and therefore fill at their stated
    level (less slippage), never at an optimistic intrabar high.  A trailing
    stop activated by the current bar can only fire on the next bar because the
    OHLC path inside a bar is unknown.

    ``partial_target`` sells ``partial_fraction`` of the original position.
    ``full_take_profit`` then closes whatever remains.  The two may be combined
    to express staged exits without introducing discretionary intrabar logic.
    """
    required = {"time_days"}
    missing = required.difference(policy)
    if missing:
        raise ValueError(f"Exit policy {policy_name!r} is missing {sorted(missing)}")
    policy = {
        "family": str(policy.get("family") or "custom"),
        "hard_stop": policy.get("hard_stop"),
        "time_days": int(policy["time_days"]),
        "trail_activation": policy.get("trail_activation"),
        "trail_distance": policy.get("trail_distance"),
        "partial_target": policy.get("partial_target"),
        "partial_fraction": float(policy.get("partial_fraction", 0.5)),
        "full_take_profit": policy.get("full_take_profit"),
    }
    if not 0.0 < policy["partial_fraction"] < 1.0:
        raise ValueError("partial_fraction must be strictly between zero and one")
    if policy["time_days"] <= 0:
        raise ValueError("time_days must be positive")
    rows = bars
    if not pd.api.types.is_datetime64_any_dtype(rows["open_time"]):
        rows = rows.copy()
        rows["open_time"] = pd.to_datetime(rows["open_time"], utc=True)
    if not rows["open_time"].is_monotonic_increasing:
        rows = rows.sort_values("open_time")
    entry_time = pd.Timestamp(entry_time)
    if entry_time.tzinfo is None:
        entry_time = entry_time.tz_localize("UTC")
    entry_candidates = rows[rows["open_time"] >= entry_time]
    if entry_candidates.empty:
        return None
    entry_row = entry_candidates.iloc[0]
    if entry_row["open_time"] > entry_time + pd.Timedelta(hours=4):
        return None
    horizon_end = entry_time + pd.Timedelta(days=int(policy["time_days"]))
    path = rows[rows["open_time"].between(entry_row["open_time"], horizon_end)]
    if path.empty:
        return None

    slip = float(slippage_bps) / 10_000.0
    fee = float(fee_bps) / 10_000.0
    entry_open = float(entry_row["open"])
    entry_price = entry_open * (1.0 + slip)
    hard_stop_price = (
        entry_price * (1.0 - float(policy["hard_stop"])) if policy["hard_stop"] is not None else None
    )
    remaining = 1.0
    cumulative_return = -fee
    exit_fee_return = 0.0
    funding_return = 0.0
    peak_price = entry_price
    trail_active = False
    partial_done = False
    full_target_done = False
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason = "time_stop"
    mfe = -np.inf
    mae = np.inf
    mark_path: list[dict[str, Any]] = []

    funding_series = funding if funding is not None else pd.Series(dtype=float)
    if not funding_series.empty:
        funding_series = funding_series.copy()
        funding_series.index = pd.to_datetime(funding_series.index, utc=True)

    previous_bar_time: pd.Timestamp | None = None
    for _, bar in path.iterrows():
        bar_time = pd.Timestamp(bar["open_time"])
        bar_open = float(bar["open"])
        bar_high = float(bar["high"])
        bar_low = float(bar["low"])
        bar_close = float(bar["close"])
        mfe = max(mfe, bar_high / entry_price - 1.0)
        mae = min(mae, bar_low / entry_price - 1.0)

        if not funding_series.empty:
            start = previous_bar_time if previous_bar_time is not None else bar_time - pd.Timedelta(microseconds=1)
            charges = funding_series[(funding_series.index > start) & (funding_series.index <= bar_time)]
            if not charges.empty:
                funding_return -= float(charges.sum()) * remaining

        stop_fill: float | None = None
        stop_reason: str | None = None
        if hard_stop_price is not None and bar_low <= hard_stop_price:
            raw_fill = bar_open if bar_open < hard_stop_price else hard_stop_price
            stop_fill = raw_fill * (1.0 - slip)
            stop_reason = "hard_stop"
        elif trail_active and policy["trail_distance"] is not None:
            trailing_price = peak_price * (1.0 - float(policy["trail_distance"]))
            if bar_low <= trailing_price:
                raw_fill = bar_open if bar_open < trailing_price else trailing_price
                stop_fill = raw_fill * (1.0 - slip)
                stop_reason = "trailing_stop"

        if stop_fill is not None:
            cumulative_return += remaining * (stop_fill / entry_price - 1.0)
            exit_fee_return += remaining * fee
            remaining = 0.0
            exit_time = bar_time
            exit_price = stop_fill
            exit_reason = str(stop_reason)
        else:
            if (
                policy["partial_target"] is not None
                and not partial_done
                and bar_high >= entry_price * (1.0 + float(policy["partial_target"]))
            ):
                target_fill = entry_price * (1.0 + float(policy["partial_target"])) * (1.0 - slip)
                partial_fraction = min(float(policy["partial_fraction"]), remaining)
                cumulative_return += partial_fraction * (target_fill / entry_price - 1.0)
                exit_fee_return += partial_fraction * fee
                remaining -= partial_fraction
                partial_done = True

            if (
                remaining > 0
                and policy["full_take_profit"] is not None
                and bar_high >= entry_price * (1.0 + float(policy["full_take_profit"]))
            ):
                target_fill = (
                    entry_price * (1.0 + float(policy["full_take_profit"])) * (1.0 - slip)
                )
                cumulative_return += remaining * (target_fill / entry_price - 1.0)
                exit_fee_return += remaining * fee
                remaining = 0.0
                full_target_done = True
                exit_time = bar_time
                exit_price = target_fill
                exit_reason = "take_profit"

            # New highs and newly activated trailing stops apply from the next
            # bar, avoiding an optimistic assumption about intrabar ordering.
            peak_price = max(peak_price, bar_high)
            if (
                policy["trail_activation"] is not None
                and peak_price >= entry_price * (1.0 + float(policy["trail_activation"]))
            ):
                trail_active = True

        current_return = cumulative_return + funding_return - exit_fee_return
        if remaining > 0:
            current_return += remaining * (bar_close / entry_price - 1.0)
        mark_path.append(
            {
                "time": bar_time,
                "mark_return": current_return,
                "remaining_fraction": remaining,
            }
        )
        previous_bar_time = bar_time
        if remaining <= 0:
            break

    if remaining > 0:
        final = path.iloc[-1]
        exit_time = pd.Timestamp(final["open_time"]) + pd.Timedelta(hours=4)
        raw_exit = float(final["close"])
        exit_price = raw_exit * (1.0 - slip)
        cumulative_return += remaining * (exit_price / entry_price - 1.0)
        exit_fee_return += remaining * fee
        remaining = 0.0
        mark_path.append(
            {
                "time": exit_time,
                "mark_return": cumulative_return + funding_return - exit_fee_return,
                "remaining_fraction": 0.0,
            }
        )

    net_return = cumulative_return + funding_return - exit_fee_return
    return {
        "entry_time": pd.Timestamp(entry_row["open_time"]),
        "entry_price": entry_price,
        "exit_time": exit_time,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "net_return": float(net_return),
        "mfe": float(mfe),
        "mae": float(mae),
        "funding_return": float(funding_return),
        "fee_return": float(fee + exit_fee_return),
        "partial_take_profit": bool(partial_done),
        "partial_fraction": float(policy["partial_fraction"]) if partial_done else 0.0,
        "full_take_profit": bool(full_target_done),
        "policy_family": str(policy["family"]),
        "policy_name": str(policy_name),
        "funding_observed": bool(funding is not None and not funding_series.empty),
        "mark_path": mark_path,
    }


def build_exit_policy_catalog() -> dict[str, dict[str, Any]]:
    """Return the frozen exit-policy grid used by the dedicated exit study.

    The entry signal and -20% risk budget stay fixed.  Only the way profitable
    positions are closed changes, which isolates the question posed by the
    user and avoids silently re-optimising the price ranker.
    """
    policies: dict[str, dict[str, Any]] = {}
    for time_days in (14, 30):
        policies[f"stop20_time{time_days}"] = {
            "family": "time_only",
            "hard_stop": 0.20,
            "time_days": time_days,
            "trail_activation": None,
            "trail_distance": None,
            "partial_target": None,
            "partial_fraction": 0.5,
            "full_take_profit": None,
        }
        for target in (0.25, 0.50, 0.75, 1.00, 1.50, 2.00, 3.00):
            label = int(round(target * 100))
            policies[f"stop20_tp{label}_time{time_days}"] = {
                "family": "fixed_take_profit",
                "hard_stop": 0.20,
                "time_days": time_days,
                "trail_activation": None,
                "trail_distance": None,
                "partial_target": None,
                "partial_fraction": 0.5,
                "full_take_profit": target,
            }
    for activation in (0.50, 0.75, 1.00, 1.50):
        activation_label = int(round(activation * 100))
        for distance in (0.15, 0.20, 0.25, 0.30):
            distance_label = int(round(distance * 100))
            policies[f"stop20_trail{distance_label}_after{activation_label}_time30"] = {
                "family": "trailing_take_profit",
                "hard_stop": 0.20,
                "time_days": 30,
                "trail_activation": activation,
                "trail_distance": distance,
                "partial_target": None,
                "partial_fraction": 0.5,
                "full_take_profit": None,
            }
            policies[
                f"stop20_half{activation_label}_trail{distance_label}_time30"
            ] = {
                "family": "partial_then_trailing",
                "hard_stop": 0.20,
                "time_days": 30,
                "trail_activation": activation,
                "trail_distance": distance,
                "partial_target": activation,
                "partial_fraction": 0.5,
                "full_take_profit": None,
            }
    for first_target, final_target in ((0.50, 1.00), (0.75, 1.50), (1.00, 2.00), (1.50, 3.00)):
        first_label = int(round(first_target * 100))
        final_label = int(round(final_target * 100))
        policies[f"stop20_half{first_label}_final{final_label}_time30"] = {
            "family": "two_stage_fixed_take_profit",
            "hard_stop": 0.20,
            "time_days": 30,
            "trail_activation": None,
            "trail_distance": None,
            "partial_target": first_target,
            "partial_fraction": 0.5,
            "full_take_profit": final_target,
        }
    return policies


def run_path_backtests(
    scored: pd.DataFrame,
    panel_4h: pd.DataFrame,
    *,
    funding_history: Mapping[str, pd.Series] | None = None,
    initial_equity: float = 100_000.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    signals = build_first_crossing_signals(scored)
    if signals.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {"paper_trade_candidate": False}
    bars_by_symbol = {
        symbol: group.sort_values("open_time").copy()
        for symbol, group in panel_4h.groupby("symbol", sort=False)
    }
    funding_map = dict(funding_history or {})
    all_trades: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    equity_frames: list[pd.DataFrame] = []
    for policy_name in PAPER_POLICIES:
        for slippage_bps in (5.0, 15.0, 30.0):
            outcomes: list[dict[str, Any]] = []
            for _, signal in signals.iterrows():
                symbol = str(signal["symbol"])
                bars = bars_by_symbol.get(symbol)
                if bars is None:
                    continue
                outcome = simulate_trade_path(
                    bars,
                    entry_time=pd.Timestamp(signal["entry_time"]),
                    policy_name=policy_name,
                    funding=funding_map.get(symbol),
                    slippage_bps=slippage_bps,
                )
                if outcome is None:
                    continue
                outcome.update(
                    {
                        "symbol": symbol,
                        "signal_date": signal["date"],
                        "fold": int(signal["fold"]),
                        "score": float(signal["price_model_score"]),
                        "score_pctile": float(signal["price_model_pctile"]),
                        "breadth_regime": signal.get("breadth_regime"),
                        "btc_trend_regime": signal.get("btc_trend_regime"),
                        "btc_vol_regime": signal.get("btc_vol_regime"),
                        "market_state": signal.get("market_state"),
                        "policy": policy_name,
                        "slippage_bps_each_side": slippage_bps,
                    }
                )
                outcomes.append(outcome)
            accepted, equity_curve = _apply_portfolio_constraints(
                outcomes,
                initial_equity=initial_equity,
            )
            if not accepted:
                continue
            trade_frame = pd.DataFrame(
                [{key: value for key, value in trade.items() if key != "mark_path"} for trade in accepted]
            )
            all_trades.extend(
                [{key: value for key, value in trade.items() if key != "mark_path"} for trade in accepted]
            )
            summary = summarize_trade_frame(trade_frame, equity_curve, initial_equity=initial_equity)
            summary.update({"policy": policy_name, "slippage_bps_each_side": slippage_bps})
            summaries.append(summary)
            if not equity_curve.empty:
                curve = equity_curve.copy()
                curve["policy"] = policy_name
                curve["slippage_bps_each_side"] = slippage_bps
                equity_frames.append(curve)
    trades = pd.DataFrame(all_trades)
    summary_frame = pd.DataFrame(summaries)
    equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    default = summary_frame[summary_frame["slippage_bps_each_side"] == 5.0].copy()
    if default.empty:
        decision = {"paper_trade_candidate": False, "reason": "no_default_cost_results"}
    else:
        best = default.sort_values(["gate_pass", "expectancy"], ascending=[False, False]).iloc[0]
        decision = {
            "paper_trade_candidate": bool(best["gate_pass"]),
            "best_policy": str(best["policy"]),
            "best_policy_expectancy": float(best["expectancy"]),
            "best_policy_profit_factor": float(best["profit_factor"]),
            "best_policy_max_drawdown": float(best["max_drawdown"]),
            "final_classification": "paper_trade_candidate" if bool(best["gate_pass"]) else "watchlist_only",
        }
    return trades, summary_frame, equity, decision


def _apply_portfolio_constraints(
    outcomes: Sequence[dict[str, Any]],
    *,
    initial_equity: float,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    ordered = sorted(outcomes, key=lambda row: (pd.Timestamp(row["entry_time"]), -float(row["score"])))
    active: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []
    last_exit: dict[str, pd.Timestamp] = {}
    realized_equity = float(initial_equity)
    for raw in ordered:
        entry_time = pd.Timestamp(raw["entry_time"])
        still_active: list[dict[str, Any]] = []
        for trade in active:
            if pd.Timestamp(trade["exit_time"]) <= entry_time:
                realized_equity += float(trade["pnl_usd"])
                last_exit[str(trade["symbol"])] = pd.Timestamp(trade["exit_time"])
            else:
                still_active.append(trade)
        active = still_active
        symbol = str(raw["symbol"])
        if symbol in {str(item["symbol"]) for item in active}:
            continue
        if symbol in last_exit and entry_time < last_exit[symbol] + pd.Timedelta(days=14):
            continue
        if len(active) >= 10:
            continue
        gross_active = sum(float(item["notional_usd"]) for item in active)
        risk_notional = realized_equity * 0.005 / 0.20
        available = max(0.0, realized_equity * 0.50 - gross_active)
        notional = min(risk_notional, available)
        if notional <= 1.0:
            continue
        trade = dict(raw)
        trade["notional_usd"] = float(notional)
        trade["pnl_usd"] = float(notional * float(trade["net_return"]))
        active.append(trade)
        accepted.append(trade)
    for trade in sorted(active, key=lambda item: pd.Timestamp(item["exit_time"])):
        realized_equity += float(trade["pnl_usd"])

    if not accepted:
        return [], pd.DataFrame()
    start = min(pd.Timestamp(item["entry_time"]).floor("D") for item in accepted)
    end = max(pd.Timestamp(item["exit_time"]).ceil("D") for item in accepted)
    dates = pd.date_range(start, end, freq="1D", tz="UTC")
    curve_records: list[dict[str, Any]] = []
    for date in dates:
        value = float(initial_equity)
        for trade in accepted:
            entry = pd.Timestamp(trade["entry_time"])
            exit_time = pd.Timestamp(trade["exit_time"])
            if date < entry.floor("D"):
                continue
            if date >= exit_time.ceil("D"):
                value += float(trade["pnl_usd"])
                continue
            marks = trade.get("mark_path") or []
            eligible = [mark for mark in marks if pd.Timestamp(mark["time"]) <= date + pd.Timedelta(days=1)]
            if eligible:
                value += float(trade["notional_usd"]) * float(eligible[-1]["mark_return"])
        curve_records.append({"date": date, "equity": value})
    curve = pd.DataFrame(curve_records)
    curve["peak_equity"] = curve["equity"].cummax()
    curve["drawdown"] = curve["equity"] / curve["peak_equity"] - 1.0
    return accepted, curve


def summarize_trade_frame(
    trades: pd.DataFrame,
    equity_curve: pd.DataFrame,
    *,
    initial_equity: float,
) -> dict[str, Any]:
    returns = pd.to_numeric(trades["net_return"], errors="coerce").dropna()
    pnl = pd.to_numeric(trades["pnl_usd"], errors="coerce").dropna()
    gains = float(returns[returns > 0].sum())
    losses = float(-returns[returns < 0].sum())
    positive_pnl = pnl[pnl > 0]
    profit_factor = gains / losses if losses > 0 else np.inf
    max_drawdown = float(equity_curve["drawdown"].min()) if not equity_curve.empty else np.nan
    fold_pf: list[float] = []
    for _, group in trades.groupby("fold"):
        fold_returns = group["net_return"]
        fold_gains = float(fold_returns[fold_returns > 0].sum())
        fold_losses = float(-fold_returns[fold_returns < 0].sum())
        fold_pf.append(fold_gains / fold_losses if fold_losses > 0 else np.inf)
    leave_largest_out_expectancy = float(returns.drop(returns.idxmax()).mean()) if len(returns) > 1 else np.nan
    contribution = float(positive_pnl.max() / positive_pnl.sum()) if positive_pnl.sum() > 0 else 1.0
    fold_majority = float(np.mean(np.asarray(fold_pf) > 1.0)) if fold_pf else 0.0
    state_pf: list[float] = []
    if "market_state" in trades.columns:
        for _, group in trades.dropna(subset=["market_state"]).groupby("market_state"):
            state_returns = group["net_return"]
            state_gains = float(state_returns[state_returns > 0].sum())
            state_losses = float(-state_returns[state_returns < 0].sum())
            state_pf.append(state_gains / state_losses if state_losses > 0 else np.inf)
    state_majority = float(np.mean(np.asarray(state_pf) > 1.0)) if state_pf else 0.0
    gate_pass = bool(
        float(returns.mean()) > 0
        and profit_factor > 1.0
        and max_drawdown >= -0.30
        and fold_majority > 0.5
        and state_majority > 0.5
        and leave_largest_out_expectancy > 0
        and contribution <= 0.25
    )
    return {
        "trades": int(len(trades)),
        "expectancy": float(returns.mean()),
        "median_return": float(returns.median()),
        "win_rate": float((returns > 0).mean()),
        "profit_factor": float(profit_factor),
        "cvar_5pct": float(returns[returns <= returns.quantile(0.05)].mean()),
        "mean_mfe": float(trades["mfe"].mean()),
        "mean_mae": float(trades["mae"].mean()),
        "funding_coverage": float(trades["funding_observed"].mean()),
        "total_return_on_initial_equity": float(pnl.sum() / initial_equity),
        "max_drawdown": max_drawdown,
        "folds": int(trades["fold"].nunique()),
        "fold_majority_profit_factor_gt_1": fold_majority,
        "market_states": int(len(state_pf)),
        "market_state_majority_profit_factor_gt_1": state_majority,
        "largest_positive_pnl_share": contribution,
        "leave_largest_winner_out_expectancy": leave_largest_out_expectancy,
        "median_notional_usd": float(trades["notional_usd"].median()),
        "maximum_concurrent_positions_constraint": 10,
        "maximum_gross_exposure_constraint": 0.50,
        "gate_pass": gate_pass,
    }


def scan_30d_runups(
    panel_4h: pd.DataFrame,
    *,
    end_time: pd.Timestamp | None = None,
    window_days: int = 30,
    threshold_return: float = PRIMARY_RETURN,
) -> pd.DataFrame:
    rows = panel_4h.copy()
    rows["open_time"] = pd.to_datetime(rows["open_time"], utc=True)
    if end_time is None:
        end_time = rows["open_time"].max()
    end_time = pd.Timestamp(end_time)
    if end_time.tzinfo is None:
        end_time = end_time.tz_localize("UTC")
    start_time = end_time - pd.Timedelta(days=window_days)
    rows = rows[rows["open_time"].between(start_time, end_time)].copy()
    events: list[dict[str, Any]] = []
    for symbol, group in rows.groupby("symbol"):
        group = group.sort_values("open_time").reset_index(drop=True)
        if len(group) < 2:
            continue
        highs = group["high"].to_numpy(float)
        closes = group["close"].to_numpy(float)
        lows = group["low"].to_numpy(float)
        best_close = (-np.inf, None, None)
        best_low = (-np.inf, None, None)
        running_close = closes[0]
        running_close_index = 0
        running_low = lows[0]
        running_low_index = 0
        for index in range(1, len(group)):
            close_return = highs[index] / running_close - 1.0 if running_close > 0 else -np.inf
            low_return = highs[index] / running_low - 1.0 if running_low > 0 else -np.inf
            if close_return > best_close[0]:
                best_close = (float(close_return), running_close_index, index)
            if low_return > best_low[0]:
                best_low = (float(low_return), running_low_index, index)
            if closes[index] < running_close:
                running_close = closes[index]
                running_close_index = index
            if lows[index] < running_low:
                running_low = lows[index]
                running_low_index = index
        close_confirmed = best_close[0] >= threshold_return
        wick_only = not close_confirmed and best_low[0] >= threshold_return
        if not close_confirmed and not wick_only:
            continue
        reference = best_close if close_confirmed else best_low
        events.append(
            {
                "symbol": symbol,
                "classification": "close-confirmed" if close_confirmed else "wick-only",
                "close_return": float(best_close[0]),
                "low_return": float(best_low[0]),
                "entry_time": group.iloc[int(reference[1])]["open_time"],
                "peak_time": group.iloc[int(reference[2])]["open_time"],
                "window_start": start_time,
                "window_end": end_time,
            }
        )
    return pd.DataFrame(events).sort_values(["classification", "close_return"], ascending=[True, False])
