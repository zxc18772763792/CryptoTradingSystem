"""Time-safe sequential launch features for Binance run-up candidates.

The helpers in this module are research-only.  A checkpoint feature row uses
completed 4h bars strictly before ``decision_time``; the open at
``decision_time`` is used only as the hypothetical entry or exit price.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression


PATH_FEATURES = [
    "early_close_return",
    "early_mfe",
    "early_mae",
    "early_max_drawdown",
    "early_pullback_from_peak",
    "early_log_return_vol",
    "early_path_efficiency",
    "early_positive_bar_share",
    "early_close_location",
    "early_range_mean",
    "early_range_expansion",
    "early_volume_ratio_prior",
    "early_volume_acceleration",
    "early_taker_buy_share",
    "early_taker_acceleration",
]

SEQUENCE_FEATURES = ["price_model_score", "price_model_pctile", *PATH_FEATURES]


def _utc(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def _safe_ratio(numerator: float, denominator: float, default: float = np.nan) -> float:
    if not np.isfinite(numerator) or not np.isfinite(denominator) or denominator == 0:
        return float(default)
    return float(numerator / denominator)


def checkpoint_row(
    signal: Mapping[str, Any],
    bars: pd.DataFrame,
    *,
    horizon_hours: int,
    require_future_labels: bool = True,
) -> dict[str, Any] | None:
    """Build one sequential observation from completed bars only."""

    if horizon_hours <= 0 or horizon_hours % 4:
        raise ValueError("horizon_hours must be a positive multiple of four")
    rows = bars.sort_values("open_time").reset_index(drop=True).copy()
    rows["open_time"] = pd.to_datetime(rows["open_time"], utc=True)
    for column in ["open", "high", "low", "close", "quote_volume", "taker_buy_quote"]:
        rows[column] = pd.to_numeric(rows[column], errors="coerce")

    baseline_time = _utc(signal["entry_time"])
    decision_time = baseline_time + pd.Timedelta(hours=horizon_hours)
    time_ns = rows["open_time"].dt.as_unit("ns").astype("int64").to_numpy()
    start = int(np.searchsorted(time_ns, baseline_time.value, side="left"))
    decision = int(np.searchsorted(time_ns, decision_time.value, side="left"))
    if start >= len(rows) or decision >= len(rows):
        return None
    if rows.iloc[start]["open_time"] != baseline_time or rows.iloc[decision]["open_time"] != decision_time:
        return None
    observed = rows.iloc[start:decision].copy()
    expected = horizon_hours // 4
    if len(observed) != expected:
        return None
    expected_times = pd.date_range(baseline_time, periods=expected, freq="4h", tz="UTC")
    if not observed["open_time"].reset_index(drop=True).equals(pd.Series(expected_times)):
        return None

    baseline_open = float(signal.get("entry_open", observed.iloc[0]["open"]))
    signal_close = float(signal.get("close", baseline_open))
    decision_open = float(rows.iloc[decision]["open"])
    highs = observed["high"].to_numpy(dtype=float)
    lows = observed["low"].to_numpy(dtype=float)
    closes = observed["close"].to_numpy(dtype=float)
    opens = observed["open"].to_numpy(dtype=float)
    quote = observed["quote_volume"].to_numpy(dtype=float)
    taker = observed["taker_buy_quote"].to_numpy(dtype=float)
    ranges = highs / np.where(lows == 0, np.nan, lows) - 1.0
    shares = taker / np.where(quote == 0, np.nan, quote)
    prior_quote = rows.iloc[max(0, start - 18):start]["quote_volume"].to_numpy(dtype=float)
    prior_median = float(np.nanmedian(prior_quote)) if len(prior_quote) else np.nan

    peaks = np.maximum.accumulate(np.maximum(highs, baseline_open))
    drawdowns = lows / peaks - 1.0
    close_path = np.r_[baseline_open, closes]
    log_returns = np.diff(np.log(np.where(close_path > 0, close_path, np.nan)))
    arithmetic = np.diff(close_path) / close_path[:-1]
    total_path = float(np.nansum(np.abs(arithmetic)))
    split = max(1, min(3, len(observed) // 2))
    first_quote = float(np.nanmedian(quote[:split]))
    last_quote = float(np.nanmedian(quote[-split:]))
    first_taker = float(np.nanmean(shares[:split]))
    last_taker = float(np.nanmean(shares[-split:]))
    first_range = float(np.nanmean(ranges[:split]))
    last_range = float(np.nanmean(ranges[-split:]))
    path_low = float(np.nanmin(lows))
    path_high = float(np.nanmax(highs))

    late_max_return: float | None = None
    first_target_hours = None
    if require_future_labels:
        future_end = decision_time + pd.Timedelta(days=14)
        end = int(np.searchsorted(time_ns, future_end.value, side="right"))
        future = rows.iloc[decision:end]
        if future.empty or future.iloc[-1]["open_time"] < future_end - pd.Timedelta(hours=4):
            return None
        future_high = float(future["high"].max())
        late_max_return = future_high / decision_open - 1.0
        target_rows = future[future["high"] >= decision_open * 3.0]
        if not target_rows.empty:
            first_target_hours = float(
                (pd.Timestamp(target_rows.iloc[0]["open_time"]) - decision_time).total_seconds() / 3600.0
            )

    return {
        "symbol": str(signal["symbol"]),
        "date": _utc(signal["date"]),
        "fold": int(signal["fold"]),
        "signal_key": int(signal.get("signal_key", -1)),
        "baseline_entry_time": baseline_time,
        "baseline_entry_open": baseline_open,
        "decision_time": decision_time,
        "decision_open": decision_open,
        "checkpoint_hours": int(horizon_hours),
        "entry_delay_hours": float(horizon_hours),
        "entry_chase_vs_signal_close": float(decision_open / signal_close - 1.0),
        "price_model_score": float(signal["price_model_score"]),
        "price_model_pctile": float(signal["price_model_pctile"]),
        "original_target200": bool(signal.get("target200_14d_from_entry", False)) if require_future_labels else None,
        "late_future_max_return_14d": None if late_max_return is None else float(late_max_return),
        "late_target200": None if late_max_return is None else bool(late_max_return >= 2.0),
        "late_first_target_hours": first_target_hours,
        "early_close_return": float(closes[-1] / baseline_open - 1.0),
        "early_mfe": float(np.nanmax(highs) / baseline_open - 1.0),
        "early_mae": float(np.nanmin(lows) / baseline_open - 1.0),
        "early_max_drawdown": float(np.nanmin(drawdowns)),
        "early_pullback_from_peak": float(closes[-1] / np.nanmax(highs) - 1.0),
        "early_log_return_vol": float(np.nanstd(log_returns, ddof=0)),
        "early_path_efficiency": float((closes[-1] / baseline_open - 1.0) / total_path) if total_path > 0 else 0.0,
        "early_positive_bar_share": float(np.nanmean(closes > opens)),
        "early_close_location": _safe_ratio(closes[-1] - path_low, path_high - path_low, 0.5),
        "early_range_mean": float(np.nanmean(ranges)),
        "early_range_expansion": _safe_ratio(last_range, first_range, 1.0),
        "early_volume_ratio_prior": _safe_ratio(float(np.nanmedian(quote)), prior_median, 1.0),
        "early_volume_acceleration": _safe_ratio(last_quote, first_quote, 1.0),
        "early_taker_buy_share": float(np.nanmean(shares)),
        "early_taker_acceleration": float(last_taker - first_taker),
    }


def build_checkpoint_rows(
    signals: pd.DataFrame,
    bars_by_symbol: Mapping[str, pd.DataFrame],
    *,
    horizon_hours: int,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for _, signal in signals.iterrows():
        bars = bars_by_symbol.get(str(signal["symbol"]))
        if bars is None:
            continue
        row = checkpoint_row(signal, bars, horizon_hours=horizon_hours)
        if row is not None:
            records.append(row)
    result = pd.DataFrame(records)
    if not result.empty:
        for column in ["date", "baseline_entry_time", "decision_time"]:
            result[column] = pd.to_datetime(result[column], utc=True)
    return result


def _prepare_features(
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: Sequence[str],
) -> tuple[np.ndarray, np.ndarray]:
    train_x = train[list(features)].apply(pd.to_numeric, errors="coerce")
    test_x = test[list(features)].apply(pd.to_numeric, errors="coerce")
    lower = train_x.quantile(0.01)
    upper = train_x.quantile(0.99)
    train_x = train_x.clip(lower=lower, upper=upper, axis=1)
    test_x = test_x.clip(lower=lower, upper=upper, axis=1)
    medians = train_x.median().fillna(0.0)
    train_x = train_x.fillna(medians)
    test_x = test_x.fillna(medians)
    means = train_x.mean()
    scales = train_x.std(ddof=0).replace(0, 1.0).fillna(1.0)
    return ((train_x - means) / scales).to_numpy(), ((test_x - means) / scales).to_numpy()


def fit_sequence_score(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    family: str,
    features: Sequence[str] = SEQUENCE_FEATURES,
    label_column: str = "original_target200",
) -> tuple[np.ndarray, np.ndarray]:
    """Fit one strongly regularised challenger and return train/test scores."""

    train_x, test_x = _prepare_features(train, test, features)
    target = train[label_column].astype(int).to_numpy()
    if np.unique(target).size < 2:
        raise ValueError("sequence model requires both target classes")
    if family == "logistic":
        model = LogisticRegression(
            C=0.20,
            class_weight="balanced",
            max_iter=3000,
            solver="lbfgs",
        )
        model.fit(train_x, target)
    elif family == "shallow_hgb":
        positives = max(1, int(target.sum()))
        negatives = max(1, int(len(target) - positives))
        weights = np.where(target == 1, len(target) / (2.0 * positives), len(target) / (2.0 * negatives))
        model = HistGradientBoostingClassifier(
            learning_rate=0.05,
            max_iter=60,
            max_leaf_nodes=5,
            max_depth=2,
            min_samples_leaf=15,
            l2_regularization=5.0,
            random_state=20260719,
        )
        model.fit(train_x, target, sample_weight=weights)
    else:
        raise ValueError(f"unknown model family: {family}")
    return model.predict_proba(train_x)[:, 1], model.predict_proba(test_x)[:, 1]


def combine_weighted_outcomes(
    first: Mapping[str, Any],
    second: Mapping[str, Any] | None,
    *,
    first_weight: float = 0.50,
) -> dict[str, Any]:
    """Combine a probe leg and an optional add-on leg into one planned trade."""

    if not 0.0 < first_weight <= 1.0:
        raise ValueError("first_weight must be in (0, 1]")
    second_weight = 1.0 - first_weight if second is not None else 0.0
    combined = dict(first)
    combined["net_return"] = first_weight * float(first["net_return"])
    combined["funding_return"] = first_weight * float(first.get("funding_return", 0.0))
    combined["fee_return"] = first_weight * float(first.get("fee_return", 0.0))
    combined["mfe"] = first_weight * float(first.get("mfe", 0.0))
    combined["mae"] = first_weight * float(first.get("mae", 0.0))
    combined["exit_reason"] = "staged_reject"
    components = [(first, first_weight)]
    if second is not None:
        combined["net_return"] += second_weight * float(second["net_return"])
        combined["funding_return"] += second_weight * float(second.get("funding_return", 0.0))
        combined["fee_return"] += second_weight * float(second.get("fee_return", 0.0))
        combined["mfe"] += second_weight * float(second.get("mfe", 0.0))
        combined["mae"] += second_weight * float(second.get("mae", 0.0))
        combined["exit_time"] = max(_utc(first["exit_time"]), _utc(second["exit_time"]))
        combined["exit_reason"] = "staged_hold_and_add"
        components.append((second, second_weight))

    times = sorted({_utc(mark["time"]) for leg, _ in components for mark in leg.get("mark_path", [])})
    marks: list[dict[str, Any]] = []
    for time in times:
        value = 0.0
        for leg, weight in components:
            leg_marks = leg.get("mark_path", [])
            eligible = [mark for mark in leg_marks if _utc(mark["time"]) <= time]
            if eligible:
                value += weight * float(eligible[-1]["mark_return"])
        marks.append({"time": time, "mark_return": float(value), "remaining_fraction": np.nan})
    combined["mark_path"] = marks
    combined["probe_weight"] = float(first_weight)
    combined["add_weight"] = float(second_weight)
    return combined
