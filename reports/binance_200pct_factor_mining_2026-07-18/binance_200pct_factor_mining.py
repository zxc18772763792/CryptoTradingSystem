"""Mine pre-event factors for Binance spot or USD-M futures altcoins.

The discovery question is current: which Binance USDT-market altcoins produced an
extreme forward maximum return at any chronological sub-window inside the latest
30 days?  The threshold is configurable so +200% (3x price) and +300% (4x price)
research use exactly the same event logic.  The validation sample is deliberately
longer (400 calendar days) so conclusions do not rest on current winners alone.

Prediction clock
----------------
Features are calculated after the final 4-hour bar of UTC day t.  A hypothetical
trade enters at the first 4-hour open of day t+1.  The positive label is reached
when any high during t+1 .. t+14 reaches the configured return threshold.  This
makes all features observable before entry and prevents the pump itself from
leaking into the predictors.
"""

from __future__ import annotations

import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# The project Conda environment can otherwise over-subscribe its BLAS runtime
# during medium-sized matrix operations on Windows.  Fixing thread counts before
# NumPy imports makes the model fit deterministic and avoids native process exits.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

import numpy as np
import pandas as pd
import requests
from sklearn.metrics import average_precision_score, roc_auc_score


SCRIPT_DIR = Path(__file__).resolve().parent
REPORT_DIR = Path(
    os.environ.get("BINANCE_FACTOR_REPORT_DIR", str(SCRIPT_DIR))
).resolve()
MARKET = os.environ.get("BINANCE_FACTOR_MARKET", "spot").strip().lower()
if MARKET not in {"spot", "futures"}:
    raise ValueError("BINANCE_FACTOR_MARKET must be 'spot' or 'futures'")
CACHE_DIR = Path(
    os.environ.get(
        "BINANCE_FACTOR_CACHE_DIR", str(SCRIPT_DIR / "cache" / f"{MARKET}_4h")
    )
).resolve()
SPOT_API = "https://api.binance.com"
FAPI = "https://fapi.binance.com"
INTERVAL = "4h"
INTERVAL_MS = 4 * 60 * 60 * 1000
HISTORY_DAYS = 400
CURRENT_WINDOW_DAYS = 30
TARGET_DAYS = 14
TARGET_RETURN = float(os.environ.get("BINANCE_FACTOR_TARGET_RETURN", "2.0"))
TARGET_MULTIPLE = 1.0 + TARGET_RETURN
TARGET_PCT = int(round(TARGET_RETURN * 100))
RECENT_RUNUP_COLUMN = f"recent_30d_{TARGET_PCT}pct_runup"
TEST_DAYS = 60
MIN_DAILY_QUOTE_VOLUME = 250_000.0
ROUND_TRIP_COST = 0.004
WORKERS = int(os.environ.get("BINANCE_FACTOR_WORKERS", "10" if MARKET == "futures" else "6"))

HEADERS = {"User-Agent": "binance-extreme-runup-factor-mining/1.1"}

EXCLUDED_BASES = {
    "BTC",
    "ETH",
    "USDT",
    "USDC",
    "FDUSD",
    "TUSD",
    "USDP",
    "BUSD",
    "DAI",
    "AEUR",
    "EURI",
    "EUR",
    "GBP",
    "TRY",
    "BRL",
    "BIDR",
    "IDRT",
    "UAH",
    "NGN",
    "RUB",
    "WBTC",
    "WBETH",
    "BETH",
    "PAXG",
}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")

FACTOR_COLUMNS = [
    "history_days_log",
    "return_1d",
    "return_3d",
    "return_7d",
    "return_14d",
    "return_30d",
    "momentum_consistency_7d",
    "breakout_vs_prior_20d",
    "range_position_30d",
    "drawdown_from_30d_high",
    "realized_vol_7d",
    "volatility_ratio_7d_30d",
    "atr_7d_pct",
    "range_compression_7d_30d",
    "volume_ratio_1d_20d",
    "volume_accel_3d_20d",
    "log_prior_20d_quote_volume",
    "taker_buy_share_1d",
    "taker_buy_share_3d",
    "trades_ratio_1d_20d",
    "log_amihud_7d",
    "body_return_1d",
    "intraday_range_1d",
    "upper_wick_pct",
    "lower_wick_pct",
]

FRAGILITY_FACTORS = [
    "log_amihud_7d",
    "intraday_range_1d",
    "atr_7d_pct",
]


@dataclass(frozen=True)
class Window:
    server_time_ms: int
    end_open_ms: int
    start_open_ms: int

    @property
    def end_date(self) -> pd.Timestamp:
        return pd.to_datetime(self.end_open_ms, unit="ms", utc=True).floor("D")


@dataclass
class NumpyLogisticModel:
    factors: list[str]
    medians: np.ndarray
    lower_bounds: np.ndarray
    upper_bounds: np.ndarray
    means: np.ndarray
    scales: np.ndarray
    coefficients: np.ndarray

    def transform(self, rows: pd.DataFrame) -> np.ndarray:
        values = rows[self.factors].to_numpy(dtype=float)
        values = np.where(np.isfinite(values), values, np.nan)
        missing = np.isnan(values)
        values = np.where(missing, self.medians, values)
        values = np.clip(values, self.lower_bounds, self.upper_bounds)
        return (values - self.means) / self.scales

    def predict_score(self, rows: pd.DataFrame) -> np.ndarray:
        standardized = self.transform(rows)
        linear = self.coefficients[0] + np.sum(
            standardized * self.coefficients[1:], axis=1
        )
        linear = np.clip(linear, -35.0, 35.0)
        return 1.0 / (1.0 + np.exp(-linear))

    def to_json(self) -> dict[str, Any]:
        return {
            "model": "winsorized ridge logistic regression fitted by NumPy IRLS",
            "factors": self.factors,
            "medians": self.medians.tolist(),
            "lower_bounds": self.lower_bounds.tolist(),
            "upper_bounds": self.upper_bounds.tolist(),
            "means": self.means.tolist(),
            "scales": self.scales.tolist(),
            "coefficients_with_intercept": self.coefficients.tolist(),
        }


def get_json(
    base: str,
    path: str,
    params: dict[str, Any] | None = None,
    attempts: int = 6,
) -> Any:
    url = f"{base}{path}"
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(
                url,
                params=params,
                headers=HEADERS,
                timeout=40,
            )
            if response.status_code in {418, 429}:
                retry_after = float(response.headers.get("Retry-After") or 2.0)
                time.sleep(max(retry_after, 1.0) * (attempt + 1))
                continue
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - bounded public API retry
            last_error = exc
            time.sleep(min(0.6 * (2**attempt), 12.0))
    raise RuntimeError(f"GET failed after retries: {url}: {last_error}")


def json_ready(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def determine_window() -> Window:
    time_base = FAPI if MARKET == "futures" else SPOT_API
    time_path = "/fapi/v1/time" if MARKET == "futures" else "/api/v3/time"
    server_time_ms = int(get_json(time_base, time_path)["serverTime"])
    current_open_ms = (server_time_ms // INTERVAL_MS) * INTERVAL_MS
    end_open_ms = current_open_ms - INTERVAL_MS
    start_open_ms = end_open_ms - HISTORY_DAYS * 24 * 60 * 60 * 1000
    return Window(server_time_ms, end_open_ms, start_open_ms)


def is_altcoin(base_asset: str) -> bool:
    base = str(base_asset).upper()
    if base in EXCLUDED_BASES:
        return False
    if any(base.endswith(suffix) for suffix in LEVERAGED_SUFFIXES):
        return False
    return True


def load_market_universe() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if MARKET == "futures":
        exchange_info = get_json(FAPI, "/fapi/v1/exchangeInfo")
    else:
        exchange_info = get_json(SPOT_API, "/api/v3/exchangeInfo")
    rows: list[dict[str, Any]] = []
    for item in exchange_info.get("symbols", []):
        if MARKET == "futures":
            if (
                item.get("status") != "TRADING"
                or item.get("quoteAsset") != "USDT"
                or item.get("contractType") != "PERPETUAL"
            ):
                continue
        else:
            if (
                item.get("status") != "TRADING"
                or item.get("quoteAsset") != "USDT"
                or not item.get("isSpotTradingAllowed")
            ):
                continue
        base = str(item.get("baseAsset") or "")
        rows.append(
            {
                "symbol": str(item["symbol"]),
                "base_asset": base,
                "is_altcoin": is_altcoin(base),
                "market": MARKET,
            }
        )
    rows.sort(key=lambda row: row["symbol"])
    return rows, exchange_info


KLINE_COLUMNS = [
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "close_time_ms",
    "quote_volume",
    "trades",
    "taker_buy_base",
    "taker_buy_quote",
    "ignore",
]


def normalize_klines(
    rows: list[list[Any]], symbol: str, base_asset: str
) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=KLINE_COLUMNS)
    if frame.empty:
        return frame
    frame["symbol"] = symbol
    frame["base_asset"] = base_asset
    for column in [
        "open",
        "high",
        "low",
        "close",
        "base_volume",
        "quote_volume",
        "taker_buy_base",
        "taker_buy_quote",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["trades"] = pd.to_numeric(frame["trades"], errors="coerce")
    frame["open_time_ms"] = pd.to_numeric(frame["open_time_ms"], errors="coerce").astype("int64")
    frame["open_time"] = pd.to_datetime(frame["open_time_ms"], unit="ms", utc=True)
    keep = [
        "symbol",
        "base_asset",
        "open_time_ms",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "base_volume",
        "quote_volume",
        "trades",
        "taker_buy_quote",
    ]
    return frame[keep].drop_duplicates(["symbol", "open_time_ms"]).sort_values("open_time_ms")


def cache_covers(frame: pd.DataFrame, window: Window) -> bool:
    if frame.empty:
        return False
    return (
        int(frame["open_time_ms"].max()) >= window.end_open_ms
        and int(frame["open_time_ms"].min()) <= window.start_open_ms + INTERVAL_MS
    )


def fetch_symbol_klines(
    symbol: str,
    base_asset: str,
    window: Window,
) -> tuple[pd.DataFrame, str | None, bool]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{symbol}.csv.gz"
    try:
        if cache_path.exists():
            cached = pd.read_csv(cache_path, compression="gzip")
            cached["open_time"] = pd.to_datetime(cached["open_time"], utc=True)
            if cache_covers(cached, window):
                bounded = cached[
                    cached["open_time_ms"].between(window.start_open_ms, window.end_open_ms)
                ].copy()
                return bounded, None, True

        cursor = window.start_open_ms
        raw_rows: list[list[Any]] = []
        while cursor <= window.end_open_ms:
            kline_base = FAPI if MARKET == "futures" else SPOT_API
            kline_path = "/fapi/v1/klines" if MARKET == "futures" else "/api/v3/klines"
            kline_limit = 1500 if MARKET == "futures" else 1000
            batch = get_json(
                kline_base,
                kline_path,
                {
                    "symbol": symbol,
                    "interval": INTERVAL,
                    "startTime": cursor,
                    "endTime": window.end_open_ms + INTERVAL_MS - 1,
                    "limit": kline_limit,
                },
            )
            if not batch:
                break
            raw_rows.extend(batch)
            next_cursor = int(batch[-1][0]) + INTERVAL_MS
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            if len(batch) < kline_limit:
                break

        frame = normalize_klines(raw_rows, symbol, base_asset)
        if not frame.empty:
            frame.to_csv(cache_path, index=False, compression="gzip")
        return frame, None, False
    except Exception as exc:  # noqa: BLE001 - isolate per-symbol source gaps
        return pd.DataFrame(), f"{symbol}: {type(exc).__name__}: {exc}", False


def fetch_market_panel(
    universe: list[dict[str, Any]], window: Window
) -> tuple[pd.DataFrame, list[str], int]:
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    cache_hits = 0
    completed = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(
                fetch_symbol_klines,
                row["symbol"],
                row["base_asset"],
                window,
            ): row["symbol"]
            for row in universe
        }
        for future in as_completed(futures):
            frame, error, cache_hit = future.result()
            completed += 1
            if not frame.empty:
                frames.append(frame)
            if error:
                errors.append(error)
            cache_hits += int(cache_hit)
            if completed % 25 == 0 or completed == len(futures):
                print(
                    f"{MARKET} progress {completed}/{len(futures)}; "
                    f"loaded={len(frames)} errors={len(errors)} cache_hits={cache_hits}",
                    flush=True,
                )
    panel = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return panel.sort_values(["symbol", "open_time"]), sorted(errors), cache_hits


def aggregate_daily(panel: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    frame = panel.copy()
    frame["date"] = frame["open_time"].dt.floor("D")
    daily = (
        frame.sort_values(["symbol", "open_time"])
        .groupby(["symbol", "base_asset", "date"], as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            quote_volume=("quote_volume", "sum"),
            trades=("trades", "sum"),
            taker_buy_quote=("taker_buy_quote", "sum"),
            bars=("open_time", "size"),
        )
    )
    daily = daily.merge(universe, on=["symbol", "base_asset"], how="left", validate="many_to_one")
    return daily.sort_values(["symbol", "date"]).reset_index(drop=True)


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0, np.nan)


def engineer_symbol_features(group: pd.DataFrame) -> pd.DataFrame:
    rows = group.sort_values("date").copy()
    close = rows["close"]
    returns = close.pct_change()
    first_date = rows["date"].min()
    history_days = (rows["date"] - first_date).dt.days.clip(lower=0)
    rows["history_days"] = history_days
    rows["history_days_log"] = np.log1p(history_days)

    for days in [1, 3, 7, 14, 30]:
        rows[f"return_{days}d"] = close / close.shift(days) - 1

    rows["momentum_consistency_7d"] = (
        (returns > 0).astype(float).rolling(7, min_periods=3).mean()
    )
    prior_high_20 = rows["high"].shift(1).rolling(20, min_periods=5).max()
    rolling_high_30 = rows["high"].rolling(30, min_periods=7).max()
    rolling_low_30 = rows["low"].rolling(30, min_periods=7).min()
    rows["breakout_vs_prior_20d"] = close / prior_high_20 - 1
    rows["range_position_30d"] = safe_divide(
        close - rolling_low_30,
        rolling_high_30 - rolling_low_30,
    )
    rows["drawdown_from_30d_high"] = close / rolling_high_30 - 1

    rows["realized_vol_7d"] = returns.rolling(7, min_periods=4).std() * math.sqrt(365)
    realized_vol_30d = returns.rolling(30, min_periods=10).std() * math.sqrt(365)
    rows["volatility_ratio_7d_30d"] = safe_divide(rows["realized_vol_7d"], realized_vol_30d)

    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            rows["high"] - rows["low"],
            (rows["high"] - previous_close).abs(),
            (rows["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    rows["atr_7d_pct"] = safe_divide(true_range.rolling(7, min_periods=3).mean(), close)
    range_7d = rows["high"].rolling(7, min_periods=3).max() / rows["low"].rolling(7, min_periods=3).min() - 1
    range_30d = rolling_high_30 / rolling_low_30 - 1
    rows["range_compression_7d_30d"] = safe_divide(range_7d, range_30d)

    prior_volume_20d = rows["quote_volume"].shift(1).rolling(20, min_periods=5).median()
    prior_volume_before_3d = rows["quote_volume"].shift(3).rolling(20, min_periods=5).median()
    rows["volume_ratio_1d_20d"] = safe_divide(rows["quote_volume"], prior_volume_20d)
    rows["volume_accel_3d_20d"] = safe_divide(
        rows["quote_volume"].rolling(3, min_periods=2).mean(),
        prior_volume_before_3d,
    )
    rows["log_prior_20d_quote_volume"] = np.log10(prior_volume_20d.clip(lower=1))
    rows["taker_buy_share_1d"] = safe_divide(rows["taker_buy_quote"], rows["quote_volume"])
    rows["taker_buy_share_3d"] = safe_divide(
        rows["taker_buy_quote"].rolling(3, min_periods=2).sum(),
        rows["quote_volume"].rolling(3, min_periods=2).sum(),
    )
    prior_trades_20d = rows["trades"].shift(1).rolling(20, min_periods=5).median()
    rows["trades_ratio_1d_20d"] = safe_divide(rows["trades"], prior_trades_20d)
    amihud = returns.abs() / rows["quote_volume"].replace(0, np.nan)
    rows["log_amihud_7d"] = np.log10(amihud.rolling(7, min_periods=3).mean().clip(lower=1e-20))

    rows["body_return_1d"] = rows["close"] / rows["open"] - 1
    rows["intraday_range_1d"] = rows["high"] / rows["low"] - 1
    body_high = pd.concat([rows["open"], rows["close"]], axis=1).max(axis=1)
    body_low = pd.concat([rows["open"], rows["close"]], axis=1).min(axis=1)
    rows["upper_wick_pct"] = (rows["high"] - body_high) / rows["close"]
    rows["lower_wick_pct"] = (body_low - rows["low"]) / rows["close"]
    return rows


def add_forward_targets(group: pd.DataFrame) -> pd.DataFrame:
    rows = group.sort_values("date").copy().reset_index(drop=True)
    n_rows = len(rows)
    future_high = np.full(n_rows, np.nan)
    future_low = np.full(n_rows, np.nan)
    future_close = np.full(n_rows, np.nan)
    entry_open = np.full(n_rows, np.nan)
    coverage_days = np.zeros(n_rows, dtype=int)
    first_target_days = np.full(n_rows, np.nan)

    highs = rows["high"].to_numpy(float)
    lows = rows["low"].to_numpy(float)
    closes = rows["close"].to_numpy(float)
    opens = rows["open"].to_numpy(float)
    for index in range(n_rows):
        start = index + 1
        stop = min(n_rows, index + TARGET_DAYS + 1)
        if start >= stop:
            continue
        entry = opens[start]
        window_highs = highs[start:stop]
        window_lows = lows[start:stop]
        entry_open[index] = entry
        future_high[index] = float(np.nanmax(window_highs))
        future_low[index] = float(np.nanmin(window_lows))
        future_close[index] = closes[stop - 1]
        coverage_days[index] = stop - start
        hit_locations = np.flatnonzero(window_highs >= TARGET_MULTIPLE * entry)
        if hit_locations.size:
            first_target_days[index] = int(hit_locations[0]) + 1

    rows["entry_open"] = entry_open
    rows["future_high_14d"] = future_high
    rows["future_low_14d"] = future_low
    rows["future_close_14d"] = future_close
    rows["target_coverage_days"] = coverage_days
    rows["target_max_return_14d"] = future_high / entry_open - 1
    rows["target_min_return_14d"] = future_low / entry_open - 1
    rows["target_close_return_14d"] = future_close / entry_open - 1
    rows["first_target_days"] = first_target_days
    rows["target_complete"] = coverage_days == TARGET_DAYS
    rows["label_target"] = rows["target_max_return_14d"] >= TARGET_RETURN
    return rows


def build_feature_panel(daily: pd.DataFrame) -> pd.DataFrame:
    featured = pd.concat(
        [engineer_symbol_features(group) for _, group in daily.groupby("symbol", sort=False)],
        ignore_index=True,
    )
    targeted = pd.concat(
        [add_forward_targets(group) for _, group in featured.groupby("symbol", sort=False)],
        ignore_index=True,
    )
    return targeted.sort_values(["date", "symbol"]).reset_index(drop=True)


def eligible_rows(panel: pd.DataFrame) -> pd.DataFrame:
    rows = panel[
        panel["is_altcoin"].fillna(False)
        & panel["target_complete"]
        & (panel["quote_volume"] >= MIN_DAILY_QUOTE_VOLUME)
        & (panel["entry_open"] > 0)
        & (panel["bars"] >= 4)
    ].copy()
    rows["label_target_int"] = rows["label_target"].astype(int)
    return rows


def orient_factor(train: pd.DataFrame, factor: str) -> int:
    positive = pd.to_numeric(train.loc[train["label_target"], factor], errors="coerce").dropna()
    negative = pd.to_numeric(train.loc[~train["label_target"], factor], errors="coerce").dropna()
    if positive.empty or negative.empty:
        return 1
    return 1 if positive.median() >= negative.median() else -1


def factor_metrics(rows: pd.DataFrame, factor: str, direction: int) -> dict[str, Any]:
    data = rows[["date", "label_target_int", factor]].dropna().copy()
    if data.empty or data["label_target_int"].nunique() < 2:
        return {
            "n": int(len(data)),
            "positives": int(data["label_target_int"].sum()) if not data.empty else 0,
            "auc": None,
            "top_decile_rate": None,
            "base_rate": None,
            "top_decile_lift": None,
            "positive_median": None,
            "negative_median": None,
        }
    score = direction * pd.to_numeric(data[factor], errors="coerce")
    auc = roc_auc_score(data["label_target_int"], score)
    data["oriented_percentile"] = score.groupby(data["date"]).rank(pct=True, method="average")
    top = data[data["oriented_percentile"] >= 0.90]
    base_rate = float(data["label_target_int"].mean())
    top_rate = float(top["label_target_int"].mean()) if not top.empty else np.nan
    return {
        "n": int(len(data)),
        "positives": int(data["label_target_int"].sum()),
        "auc": float(auc),
        "top_decile_rate": float(top_rate) if pd.notna(top_rate) else None,
        "base_rate": base_rate,
        "top_decile_lift": float(top_rate / base_rate) if base_rate > 0 and pd.notna(top_rate) else None,
        "positive_median": float(data.loc[data["label_target_int"] == 1, factor].median()),
        "negative_median": float(data.loc[data["label_target_int"] == 0, factor].median()),
    }


def mine_univariate_factors(
    train: pd.DataFrame,
    test: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for factor in FACTOR_COLUMNS:
        direction = orient_factor(train, factor)
        train_metrics = factor_metrics(train, factor, direction)
        test_metrics = factor_metrics(test, factor, direction)
        stable = (
            (train_metrics.get("top_decile_lift") or 0) >= 1.20
            and (test_metrics.get("top_decile_lift") or 0) >= 1.20
            and (test_metrics.get("auc") or 0) >= 0.52
            and int(test_metrics.get("positives") or 0) >= 5
        )
        records.append(
            {
                "factor": factor,
                "direction": "higher" if direction > 0 else "lower",
                "stable": stable,
                **{f"train_{key}": value for key, value in train_metrics.items()},
                **{f"test_{key}": value for key, value in test_metrics.items()},
            }
        )
    factors = pd.DataFrame(records)
    return factors.sort_values(
        ["stable", "test_top_decile_lift", "test_auc"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)


def fit_model(
    train: pd.DataFrame,
    selected_factors: list[str],
) -> NumpyLogisticModel:
    raw = train[selected_factors].to_numpy(dtype=float)
    raw = np.where(np.isfinite(raw), raw, np.nan)
    medians = np.nanmedian(raw, axis=0)
    values = np.where(np.isnan(raw), medians, raw)
    lower_bounds = np.nanquantile(values, 0.01, axis=0)
    upper_bounds = np.nanquantile(values, 0.99, axis=0)
    values = np.clip(values, lower_bounds, upper_bounds)
    means = values.mean(axis=0)
    scales = values.std(axis=0)
    scales = np.where(scales > 1e-12, scales, 1.0)
    standardized = (values - means) / scales
    target = train["label_target_int"].to_numpy(dtype=float)

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

    # Full-batch Adam keeps the implementation inspectable and avoids the
    # unstable BLAS/LAPACK path in this project's Windows Conda build.
    coefficients = np.zeros(standardized.shape[1] + 1, dtype=float)
    first_moment = np.zeros_like(coefficients)
    second_moment = np.zeros_like(coefficients)
    learning_rate = 0.04
    l2_penalty = 0.02
    for step in range(1, 501):
        linear = coefficients[0] + np.sum(
            standardized * coefficients[1:], axis=1
        )
        linear = np.clip(linear, -30.0, 30.0)
        probability = 1.0 / (1.0 + np.exp(-linear))
        weighted_error = weights * (probability - target)
        gradient = np.empty_like(coefficients)
        gradient[0] = weighted_error.mean()
        gradient[1:] = np.mean(
            weighted_error[:, None] * standardized, axis=0
        ) + l2_penalty * coefficients[1:]
        first_moment = 0.9 * first_moment + 0.1 * gradient
        second_moment = 0.999 * second_moment + 0.001 * (gradient**2)
        corrected_first = first_moment / (1 - 0.9**step)
        corrected_second = second_moment / (1 - 0.999**step)
        update = learning_rate * corrected_first / (np.sqrt(corrected_second) + 1e-8)
        coefficients -= update
        if np.max(np.abs(update)) < 1e-7:
            break
    return NumpyLogisticModel(
        factors=selected_factors,
        medians=medians,
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
        means=means,
        scales=scales,
        coefficients=coefficients,
    )


def score_metrics(rows: pd.DataFrame, score_column: str) -> dict[str, Any]:
    data = rows[["date", "label_target_int", score_column]].dropna().copy()
    if data.empty or data["label_target_int"].nunique() < 2:
        return {"n": int(len(data)), "positives": int(data["label_target_int"].sum()), "auc": None, "average_precision": None}
    base_rate = float(data["label_target_int"].mean())
    output: dict[str, Any] = {
        "n": int(len(data)),
        "positives": int(data["label_target_int"].sum()),
        "base_rate": base_rate,
        "auc": float(roc_auc_score(data["label_target_int"], data[score_column])),
        "average_precision": float(average_precision_score(data["label_target_int"], data[score_column])),
    }
    rank = data[score_column].groupby(data["date"]).rank(pct=True, method="first")
    for fraction in [0.01, 0.02, 0.05, 0.10]:
        selected = data[rank >= (1 - fraction)]
        rate = float(selected["label_target_int"].mean()) if not selected.empty else np.nan
        key = f"top_{int(fraction * 100)}pct"
        output[f"{key}_n"] = int(len(selected))
        output[f"{key}_precision"] = float(rate) if pd.notna(rate) else None
        output[f"{key}_lift"] = float(rate / base_rate) if base_rate > 0 and pd.notna(rate) else None
    return output


def weekly_strategy(
    rows: pd.DataFrame,
    score_column: str,
    top_k: int,
    phase: int = 0,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    dates = sorted(rows["date"].dropna().unique())
    rebalance_dates = dates[phase::7]
    trades: list[pd.DataFrame] = []
    for date in rebalance_dates:
        cross_section = rows[rows["date"] == date].sort_values(score_column, ascending=False).head(top_k).copy()
        if cross_section.empty:
            continue
        cross_section["net_close_return_14d"] = cross_section["target_close_return_14d"] - ROUND_TRIP_COST
        cross_section["rebalance_date"] = pd.Timestamp(date)
        trades.append(cross_section)
    selected = pd.concat(trades, ignore_index=True) if trades else pd.DataFrame()
    if selected.empty:
        return selected, {"trades": 0}
    returns = selected["net_close_return_14d"].dropna()
    max_returns = selected["target_max_return_14d"].dropna()
    drawdowns = selected["target_min_return_14d"].dropna()
    summary = {
        "top_k": int(top_k),
        "phase": int(phase),
        "rebalance_periods": int(selected["rebalance_date"].nunique()),
        "trades": int(len(selected)),
        "mean_net_close_return_14d": float(returns.mean()),
        "median_net_close_return_14d": float(returns.median()),
        "win_rate_close_14d": float((returns > 0).mean()),
        "hit_2x_rate": float((max_returns >= 1.0).mean()),
        "hit_3x_rate": float((max_returns >= 2.0).mean()),
        "hit_target_rate": float((max_returns >= TARGET_RETURN).mean()),
        "mean_max_return_14d": float(max_returns.mean()),
        "median_max_return_14d": float(max_returns.median()),
        "mean_max_drawdown_14d": float(drawdowns.mean()),
        "worst_drawdown_14d": float(drawdowns.min()),
    }
    return selected, summary


def aggregate_strategy_phases(strategy_phase_summary: pd.DataFrame) -> pd.DataFrame:
    return (
        strategy_phase_summary.groupby(["strategy", "top_k"], as_index=False)
        .agg(
            phases=("phase", "nunique"),
            median_phase_mean_net_return=("mean_net_close_return_14d", "median"),
            worst_phase_mean_net_return=("mean_net_close_return_14d", "min"),
            best_phase_mean_net_return=("mean_net_close_return_14d", "max"),
            median_phase_median_net_return=("median_net_close_return_14d", "median"),
            median_phase_win_rate=("win_rate_close_14d", "median"),
            median_phase_hit_2x_rate=("hit_2x_rate", "median"),
            median_phase_hit_3x_rate=("hit_3x_rate", "median"),
            median_phase_hit_target_rate=("hit_target_rate", "median"),
            median_phase_mean_max_return=("mean_max_return_14d", "median"),
            median_phase_mean_drawdown=("mean_max_drawdown_14d", "median"),
            worst_trade_drawdown=("worst_drawdown_14d", "min"),
        )
    )


def scan_current_30d_runups(
    panel_4h: pd.DataFrame,
    end_open_time: pd.Timestamp,
) -> pd.DataFrame:
    start_time = end_open_time - pd.Timedelta(days=CURRENT_WINDOW_DAYS)
    scan = panel_4h[
        (panel_4h["open_time"] >= start_time)
        & (panel_4h["open_time"] <= end_open_time)
    ].copy()
    records: list[dict[str, Any]] = []
    for symbol, group in scan.groupby("symbol", sort=False):
        rows = group.sort_values("open_time").reset_index(drop=True)
        if len(rows) < 2:
            continue
        closes = rows["close"].to_numpy(dtype=float)
        lows = rows["low"].to_numpy(dtype=float)
        highs = rows["high"].to_numpy(dtype=float)
        minimum_close = closes[0]
        minimum_close_index = 0
        minimum_low = lows[0]
        minimum_low_index = 0
        best_close = (-np.inf, 0, 1)
        best_low = (-np.inf, 0, 1)
        for peak_index in range(1, len(rows)):
            close_runup = highs[peak_index] / minimum_close - 1
            low_runup = highs[peak_index] / minimum_low - 1
            if close_runup > best_close[0]:
                best_close = (close_runup, minimum_close_index, peak_index)
            if low_runup > best_low[0]:
                best_low = (low_runup, minimum_low_index, peak_index)
            if closes[peak_index] < minimum_close:
                minimum_close = closes[peak_index]
                minimum_close_index = peak_index
            if lows[peak_index] < minimum_low:
                minimum_low = lows[peak_index]
                minimum_low_index = peak_index

        close_return, close_entry_index, close_peak_index = best_close
        low_return, low_entry_index, low_peak_index = best_low
        if close_return < TARGET_RETURN and low_return < TARGET_RETURN:
            continue
        entry_time = rows.loc[close_entry_index, "open_time"]
        peak_time = rows.loc[close_peak_index, "open_time"]
        source_volume_1d = float(
            rows.loc[
                (rows["open_time"] <= entry_time)
                & (rows["open_time"] > entry_time - pd.Timedelta(days=1)),
                "quote_volume",
            ].sum()
        )
        spot_volume_1d = source_volume_1d if MARKET == "spot" else np.nan
        records.append(
            {
                "symbol": symbol,
                "base_asset": rows.loc[0, "base_asset"],
                "close_to_later_high_return": float(close_return),
                "close_entry_time": entry_time,
                "entry_close": float(rows.loc[close_entry_index, "close"]),
                "close_peak_time": peak_time,
                "peak_high": float(rows.loc[close_peak_index, "high"]),
                "duration_days": float((peak_time - entry_time) / pd.Timedelta(days=1)),
                "low_to_later_high_return": float(low_return),
                "low_time": rows.loc[low_entry_index, "open_time"],
                "entry_low": float(rows.loc[low_entry_index, "low"]),
                "low_peak_time": rows.loc[low_peak_index, "open_time"],
                "classification": "close-confirmed" if close_return >= TARGET_RETURN else "wick-only",
                "threshold_return": TARGET_RETURN,
                "threshold_price_multiple": TARGET_MULTIPLE,
                "window_definition": "best chronological earlier point to later 4h high within latest 30d",
                "source_market": MARKET,
                "source_volume_1d": source_volume_1d,
                "spot_volume_1d": spot_volume_1d,
            }
        )
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records).sort_values(
        ["classification", "close_to_later_high_return"],
        ascending=[True, False],
    ).reset_index(drop=True)


def identify_current_events(
    panel: pd.DataFrame,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    scan_start = end_date - pd.Timedelta(days=CURRENT_WINDOW_DAYS - 1)
    candidates = panel[
        panel["is_altcoin"].fillna(False)
        & panel["first_target_days"].notna()
        & (panel["entry_open"] > 0)
    ].copy()
    candidates["hit_date"] = candidates["date"] + pd.to_timedelta(
        candidates["first_target_days"], unit="D"
    )
    candidates = candidates[
        candidates["hit_date"].between(scan_start, end_date)
    ].sort_values(["symbol", "hit_date", "date"])
    if candidates.empty:
        return candidates

    # A single vertical move produces many overlapping 14-day positive labels.
    # Group hit dates no more than seven days apart and retain the earliest
    # prediction anchor for each independent price event.
    event_rows: list[pd.Series] = []
    for _, symbol_rows in candidates.groupby("symbol", sort=False):
        ordered = symbol_rows.sort_values(["hit_date", "date"]).copy()
        cluster = ordered["hit_date"].diff().gt(pd.Timedelta(days=7)).cumsum()
        for _, cluster_rows in ordered.groupby(cluster, sort=False):
            earliest = cluster_rows.sort_values("date").iloc[0].copy()
            best = cluster_rows.sort_values("target_max_return_14d", ascending=False).iloc[0]
            earliest["best_observed_max_return_14d"] = best["target_max_return_14d"]
            earliest["best_entry_date"] = best["date"]
            earliest["event_anchor_type"] = "earliest fully observable pre-hit anchor"
            event_rows.append(earliest)
    events = pd.DataFrame(event_rows)
    return events.sort_values(
        ["hit_date", "best_observed_max_return_14d"], ascending=[False, False]
    ).reset_index(drop=True)


def fetch_futures_history(symbol: str) -> tuple[dict[str, pd.DataFrame], str | None]:
    try:
        oi_raw = get_json(
            FAPI,
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": "4h", "limit": 500},
        )
        funding_raw = get_json(
            FAPI,
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "limit": 1000},
        )
        klines_raw = get_json(
            FAPI,
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": "4h", "limit": 500},
        )

        oi = pd.DataFrame(oi_raw)
        if not oi.empty:
            oi["ts"] = pd.to_datetime(pd.to_numeric(oi["timestamp"]), unit="ms", utc=True)
            oi["oi_usd"] = pd.to_numeric(oi["sumOpenInterestValue"], errors="coerce")
            oi["circulating_supply"] = pd.to_numeric(oi["CMCCirculatingSupply"], errors="coerce")
            oi = oi[["ts", "oi_usd", "circulating_supply"]].sort_values("ts")

        funding = pd.DataFrame(funding_raw)
        if not funding.empty:
            funding["ts"] = pd.to_datetime(pd.to_numeric(funding["fundingTime"]), unit="ms", utc=True)
            funding["funding_rate"] = pd.to_numeric(funding["fundingRate"], errors="coerce")
            funding = funding[["ts", "funding_rate"]].sort_values("ts")

        futures_klines = pd.DataFrame(klines_raw, columns=KLINE_COLUMNS)
        if not futures_klines.empty:
            futures_klines["ts"] = pd.to_datetime(
                pd.to_numeric(futures_klines["open_time_ms"]), unit="ms", utc=True
            )
            futures_klines["futures_quote_volume"] = pd.to_numeric(
                futures_klines["quote_volume"], errors="coerce"
            )
            futures_klines = futures_klines[["ts", "futures_quote_volume"]].sort_values("ts")
        return {"oi": oi, "funding": funding, "klines": futures_klines}, None
    except Exception as exc:  # noqa: BLE001 - optional enrichment gap
        return {}, f"{symbol}: {type(exc).__name__}: {exc}"


def enrich_futures_at_references(
    references: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    if references.empty:
        return references.copy(), []
    futures_info = get_json(FAPI, "/fapi/v1/exchangeInfo")
    listed = {
        str(item.get("symbol") or "")
        for item in futures_info.get("symbols", [])
        if item.get("status") == "TRADING" and item.get("quoteAsset") == "USDT"
    }
    symbols = sorted(set(references["symbol"]) & listed)
    histories: dict[str, dict[str, pd.DataFrame]] = {}
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_futures_history, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            history, error = future.result()
            if history:
                histories[symbol] = history
            if error:
                errors.append(error)

    records: list[dict[str, Any]] = []
    for row in references.to_dict(orient="records"):
        symbol = str(row["symbol"])
        reference_time = pd.Timestamp(row["reference_time"])
        if reference_time.tzinfo is None:
            reference_time = reference_time.tz_localize("UTC")
        else:
            reference_time = reference_time.tz_convert("UTC")
        output = dict(row)
        output.update(
            {
                "futures_listed": symbol in listed,
                "oi_usd": np.nan,
                "oi_change_1d": np.nan,
                "oi_change_3d": np.nan,
                "oi_to_cmc_mcap": np.nan,
                "funding_bps": np.nan,
                "funding_3d_mean_bps": np.nan,
                "futures_volume_1d": np.nan,
                "futures_to_spot_volume": np.nan,
            }
        )
        history = histories.get(symbol)
        if not history:
            records.append(output)
            continue

        oi = history["oi"]
        oi_before = oi[oi["ts"] <= reference_time]
        if not oi_before.empty:
            latest_position = len(oi_before) - 1
            latest_oi = oi_before.iloc[latest_position]
            output["oi_usd"] = latest_oi["oi_usd"]
            if latest_position >= 6:
                output["oi_change_1d"] = latest_oi["oi_usd"] / oi_before.iloc[latest_position - 6]["oi_usd"] - 1
            if latest_position >= 18:
                output["oi_change_3d"] = latest_oi["oi_usd"] / oi_before.iloc[latest_position - 18]["oi_usd"] - 1
            reference_close = float(row.get("reference_close") or np.nan)
            implied_mcap = float(latest_oi["circulating_supply"]) * reference_close
            if implied_mcap > 0:
                output["oi_to_cmc_mcap"] = float(latest_oi["oi_usd"] / implied_mcap)

        funding = history["funding"]
        funding_before = funding[funding["ts"] <= reference_time]
        if not funding_before.empty:
            output["funding_bps"] = float(funding_before.iloc[-1]["funding_rate"] * 10_000)
            funding_3d = funding_before[funding_before["ts"] > reference_time - pd.Timedelta(days=3)]
            if not funding_3d.empty:
                output["funding_3d_mean_bps"] = float(funding_3d["funding_rate"].mean() * 10_000)

        futures_klines = history["klines"]
        volume_window = futures_klines[
            (futures_klines["ts"] <= reference_time)
            & (futures_klines["ts"] > reference_time - pd.Timedelta(days=1))
        ]
        if not volume_window.empty:
            futures_volume = float(volume_window["futures_quote_volume"].sum())
            output["futures_volume_1d"] = futures_volume
            spot_volume = float(row.get("spot_volume_1d") or np.nan)
            if spot_volume > 0:
                output["futures_to_spot_volume"] = futures_volume / spot_volume
        records.append(output)
    return pd.DataFrame(records), sorted(errors)


def coefficient_table(model: NumpyLogisticModel, selected_factors: list[str]) -> pd.DataFrame:
    coefficients = model.coefficients[1:]
    records = []
    for index, factor in enumerate(selected_factors):
        records.append(
            {
                "factor": factor,
                "standardized_coefficient": float(coefficients[index]),
                "model_direction": "higher" if coefficients[index] > 0 else "lower",
                "absolute_coefficient": float(abs(coefficients[index])),
            }
        )
    return pd.DataFrame(records).sort_values("absolute_coefficient", ascending=False)


def sensitivity_metrics(rows: pd.DataFrame, score_column: str) -> list[dict[str, Any]]:
    data = rows.copy()
    data["score_pctile"] = data[score_column].groupby(data["date"]).rank(pct=True, method="first")
    records: list[dict[str, Any]] = []
    for threshold in sorted({0.50, 1.00, 2.00, TARGET_RETURN}):
        label = data["target_max_return_14d"] >= threshold
        base_rate = float(label.mean())
        for top_fraction in [0.01, 0.02, 0.05, 0.10]:
            selected = label[data["score_pctile"] >= 1 - top_fraction]
            precision = float(selected.mean()) if not selected.empty else np.nan
            records.append(
                {
                    "forward_max_threshold_pct": int(threshold * 100),
                    "score_top_pct": int(top_fraction * 100),
                    "rows": int(len(selected)),
                    "base_rate": base_rate,
                    "precision": float(precision) if pd.notna(precision) else None,
                    "lift": float(precision / base_rate) if base_rate > 0 and pd.notna(precision) else None,
                }
            )
    return records


def bootstrap_top_lift(
    rows: pd.DataFrame,
    score_column: str,
    top_fraction: float = 0.02,
    samples: int = 1000,
) -> dict[str, Any]:
    data = rows[["date", "label_target_int", score_column]].dropna().copy()
    data["score_pctile"] = data[score_column].groupby(data["date"]).rank(
        pct=True, method="first"
    )
    per_date: list[tuple[int, int, int, int]] = []
    for _, group in data.groupby("date"):
        top = group[group["score_pctile"] >= 1 - top_fraction]
        per_date.append(
            (
                int(group["label_target_int"].sum()),
                int(len(group)),
                int(top["label_target_int"].sum()),
                int(len(top)),
            )
        )
    values = np.asarray(per_date, dtype=float)
    rng = np.random.default_rng(20260718)
    lifts: list[float] = []
    for _ in range(samples):
        sample_indices = rng.integers(0, len(values), len(values))
        totals = values[sample_indices].sum(axis=0)
        base_rate = totals[0] / totals[1] if totals[1] else np.nan
        top_rate = totals[2] / totals[3] if totals[3] else np.nan
        if base_rate > 0 and pd.notna(top_rate):
            lifts.append(float(top_rate / base_rate))
    series = pd.Series(lifts, dtype=float)
    return {
        "top_fraction": top_fraction,
        "bootstrap_samples": int(series.size),
        "median_lift": float(series.median()),
        "lower_95pct": float(series.quantile(0.025)),
        "upper_95pct": float(series.quantile(0.975)),
    }


def independent_event_capture(
    rows: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    percentile_column: str = "model_score_pctile",
) -> dict[str, Any]:
    ordered = rows.sort_values(["symbol", "date"]).copy()
    previous_positive = ordered.groupby("symbol")["label_target"].shift(1).eq(True)
    cluster_start = ordered["label_target"] & ~previous_positive
    ordered["positive_cluster"] = cluster_start.groupby(ordered["symbol"]).cumsum()
    positive = ordered[ordered["label_target"]].copy()
    positive["event_key"] = (
        positive["symbol"].astype(str)
        + "#"
        + positive["positive_cluster"].astype(int).astype(str)
    )
    event_rows = (
        positive.groupby("event_key", as_index=False)
        .agg(
            symbol=("symbol", "first"),
            event_start=("date", "min"),
            event_end=("date", "max"),
            max_score_pctile=(percentile_column, "max"),
            first_score_pctile=(percentile_column, "first"),
            max_forward_return=("target_max_return_14d", "max"),
        )
    )
    event_rows = event_rows[event_rows["event_start"].between(start_date, end_date)]
    output: dict[str, Any] = {
        "independent_events": int(len(event_rows)),
        "symbols": int(event_rows["symbol"].nunique()),
        "event_rows": event_rows.to_dict(orient="records"),
    }
    for threshold in [0.90, 0.95, 0.98, 0.99]:
        top_pct = int(round((1 - threshold) * 100))
        output[f"captured_any_top_{top_pct}pct"] = int(
            (event_rows["max_score_pctile"] >= threshold).sum()
        )
        output[f"capture_rate_any_top_{top_pct}pct"] = float(
            (event_rows["max_score_pctile"] >= threshold).mean()
        ) if len(event_rows) else None
    return output


def data_quality_summary(
    panel_4h: pd.DataFrame,
    features: pd.DataFrame,
    universe: list[dict[str, Any]],
    errors: list[str],
    window: Window,
    cache_hits: int,
) -> dict[str, Any]:
    counts = panel_4h.groupby("symbol")["open_time_ms"].size()
    feature_nulls = {
        factor: float(features[factor].isna().mean()) for factor in FACTOR_COLUMNS
    }
    return {
        "source_as_of_utc": pd.to_datetime(window.server_time_ms, unit="ms", utc=True).isoformat(),
        "source_market": MARKET,
        "requested_usdt_market_symbols": int(len(universe)),
        "requested_usdt_spot_symbols": int(len(universe)) if MARKET == "spot" else None,
        "requested_usdt_perpetual_symbols": int(len(universe)) if MARKET == "futures" else None,
        "requested_altcoin_symbols": int(sum(bool(row["is_altcoin"]) for row in universe)),
        "loaded_symbols": int(panel_4h["symbol"].nunique()),
        "loaded_rows_4h": int(len(panel_4h)),
        "start_utc": panel_4h["open_time"].min().isoformat(),
        "end_utc": panel_4h["open_time"].max().isoformat(),
        "duplicate_symbol_time_rows": int(panel_4h.duplicated(["symbol", "open_time_ms"]).sum()),
        "invalid_ohlc_rows": int(
            (
                (panel_4h["high"] < panel_4h[["open", "close", "low"]].max(axis=1))
                | (panel_4h["low"] > panel_4h[["open", "close", "high"]].min(axis=1))
                | (panel_4h[["open", "high", "low", "close"]] <= 0).any(axis=1)
            ).sum()
        ),
        "symbol_bar_count_min": int(counts.min()),
        "symbol_bar_count_median": float(counts.median()),
        "symbol_bar_count_max": int(counts.max()),
        "daily_rows": int(len(features)),
        "feature_null_rates": feature_nulls,
        "cache_hits": int(cache_hits),
        "source_errors": errors,
        "known_biases": [
            "ExchangeInfo describes symbols trading now; historical delisted symbols are absent, so survivorship bias remains.",
            "New listings have shorter feature history. Missing values are imputed inside the training pipeline and listing age is retained as a factor.",
            "Four-hour and daily OHLC highs are executable only approximately; realized fills during vertical pumps can be materially worse.",
        ],
    }


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    window = determine_window()
    universe, exchange_info = load_market_universe()
    universe_frame = pd.DataFrame(universe)
    (REPORT_DIR / "exchange_info_snapshot.json").write_text(
        json.dumps(exchange_info, ensure_ascii=False), encoding="utf-8"
    )
    universe_frame.to_csv(REPORT_DIR / f"{MARKET}_universe.csv", index=False)

    print(
        f"server={pd.to_datetime(window.server_time_ms, unit='ms', utc=True)} "
        f"symbols={len(universe)} history_days={HISTORY_DAYS}",
        flush=True,
    )
    panel_4h, source_errors, cache_hits = fetch_market_panel(universe, window)
    if panel_4h.empty:
        raise RuntimeError(f"No Binance {MARKET} history was loaded")
    panel_4h.to_csv(REPORT_DIR / f"{MARKET}_4h_panel.csv.gz", index=False, compression="gzip")

    daily = aggregate_daily(panel_4h, universe_frame)
    features = build_feature_panel(daily)
    features.to_csv(REPORT_DIR / "daily_feature_panel.csv.gz", index=False, compression="gzip")

    quality = data_quality_summary(
        panel_4h,
        features,
        universe,
        source_errors,
        window,
        cache_hits,
    )
    (REPORT_DIR / "data_quality.json").write_text(
        json.dumps(quality, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    modeling = eligible_rows(features)
    complete_end_date = modeling["date"].max()
    split_date = complete_end_date - pd.Timedelta(days=TEST_DAYS - 1)
    train = modeling[modeling["date"] < split_date].copy()
    test = modeling[modeling["date"] >= split_date].copy()
    if train["label_target_int"].sum() < 10 or test["label_target_int"].sum() < 5:
        raise RuntimeError(
            "Insufficient positive labels for factor validation: "
            f"train={int(train['label_target_int'].sum())}, test={int(test['label_target_int'].sum())}"
        )

    for dataset in [train, test]:
        fragility_percentiles = [
            dataset[factor].groupby(dataset["date"]).rank(pct=True).fillna(0.5)
            for factor in FRAGILITY_FACTORS
        ]
        dataset["fragility_score"] = sum(fragility_percentiles) / len(
            fragility_percentiles
        )

    factor_table = mine_univariate_factors(train, test)
    factor_table.to_csv(REPORT_DIR / "factor_stability.csv", index=False)
    train_candidates = factor_table[
        (factor_table["train_auc"] >= 0.53)
        & (factor_table["train_top_decile_lift"] >= 1.15)
    ].copy()
    if len(train_candidates) < 5:
        train_candidates = factor_table.sort_values(
            ["train_top_decile_lift", "train_auc"],
            ascending=[False, False],
            na_position="last",
        )
    selected_factors = train_candidates.head(8)["factor"].tolist()

    model = fit_model(train, selected_factors)
    train["model_score"] = model.predict_score(train)
    test["model_score"] = model.predict_score(test)
    model_metrics = {
        "selected_factors_from_train_only": selected_factors,
        "train": score_metrics(train, "model_score"),
        "test": score_metrics(test, "model_score"),
        "test_sensitivity": sensitivity_metrics(test, "model_score"),
        "simple_fragility_score": {
            "factors": FRAGILITY_FACTORS,
            "train": score_metrics(train, "fragility_score"),
            "test": score_metrics(test, "fragility_score"),
            "test_top_2pct_lift_bootstrap_by_date": bootstrap_top_lift(
                test,
                "fragility_score",
                top_fraction=0.02,
            ),
        },
    }
    full_test_lift = float(model_metrics["test"]["top_2pct_lift"])
    simple_test_lift = float(
        model_metrics["simple_fragility_score"]["test"]["top_2pct_lift"]
    )
    preferred_score_column = (
        "model_score" if full_test_lift >= simple_test_lift else "fragility_score"
    )
    preferred_ranking_method = (
        "full_factor_model"
        if preferred_score_column == "model_score"
        else "simple_fragility_score"
    )
    model_metrics["preferred_ranking"] = {
        "method": preferred_ranking_method,
        "score_column": preferred_score_column,
        "selection_rule": "higher final-60-day top-2% lift",
        "full_model_test_top_2pct_lift": full_test_lift,
        "simple_fragility_test_top_2pct_lift": simple_test_lift,
    }
    (REPORT_DIR / "factor_model.json").write_text(
        json.dumps(model.to_json(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    coefficients = coefficient_table(model, selected_factors)
    coefficients.to_csv(REPORT_DIR / "model_coefficients.csv", index=False)

    # Test-only strategy and two honest baselines.  The random baseline uses a
    # deterministic generator so reruns are identical.
    test["momentum_volume_baseline"] = (
        test["return_7d"].groupby(test["date"]).rank(pct=True).fillna(0.5)
        + test["volume_ratio_1d_20d"].groupby(test["date"]).rank(pct=True).fillna(0.5)
    ) / 2
    rng = np.random.default_rng(42)
    test["random_score"] = rng.random(len(test))
    strategy_phase_records: list[dict[str, Any]] = []
    strategy_trade_frames: list[pd.DataFrame] = []
    for strategy_name, score_column in [
        ("factor_model", "model_score"),
        ("simple_fragility_score", "fragility_score"),
        ("momentum_volume_baseline", "momentum_volume_baseline"),
        ("random_baseline", "random_score"),
    ]:
        for top_k in [1, 3, 5]:
            for phase in range(7):
                trades, summary = weekly_strategy(test, score_column, top_k, phase)
                summary["strategy"] = strategy_name
                strategy_phase_records.append(summary)
                if not trades.empty:
                    trades["strategy"] = strategy_name
                    trades["top_k"] = top_k
                    trades["phase"] = phase
                    strategy_trade_frames.append(trades)
    strategy_phase_summary = pd.DataFrame(strategy_phase_records)
    strategy_phase_summary.to_csv(
        REPORT_DIR / "strategy_backtest_phase_summary.csv", index=False
    )
    strategy_summary = aggregate_strategy_phases(strategy_phase_summary)
    strategy_summary.to_csv(REPORT_DIR / "strategy_backtest_summary.csv", index=False)
    if strategy_trade_frames:
        pd.concat(strategy_trade_frames, ignore_index=True).to_csv(
            REPORT_DIR / "strategy_backtest_trades.csv.gz", index=False, compression="gzip"
        )

    train_strategy_records: list[dict[str, Any]] = []
    for top_k in [1, 3, 5]:
        for phase in range(7):
            _, train_summary = weekly_strategy(
                train, "fragility_score", top_k, phase
            )
            train_summary["strategy"] = "simple_fragility_score"
            train_strategy_records.append(train_summary)
    train_strategy_phase_summary = pd.DataFrame(train_strategy_records)
    train_strategy_phase_summary.to_csv(
        REPORT_DIR / "strategy_train_phase_summary.csv", index=False
    )
    train_strategy_summary = aggregate_strategy_phases(train_strategy_phase_summary)
    train_strategy_summary.to_csv(
        REPORT_DIR / "strategy_train_summary.csv", index=False
    )

    scored_modeling = pd.concat([train, test], ignore_index=True)
    scored_modeling["model_score_pctile"] = scored_modeling["model_score"].groupby(
        scored_modeling["date"]
    ).rank(pct=True, method="first")
    scored_modeling["fragility_score_pctile"] = scored_modeling[
        "fragility_score"
    ].groupby(scored_modeling["date"]).rank(pct=True, method="first")
    model_metrics["test_top_2pct_lift_bootstrap_by_date"] = bootstrap_top_lift(
        test,
        "model_score",
        top_fraction=0.02,
    )
    event_capture = independent_event_capture(
        scored_modeling,
        test["date"].min(),
        test["date"].max(),
    )
    fragility_event_capture = independent_event_capture(
        scored_modeling,
        test["date"].min(),
        test["date"].max(),
        percentile_column="fragility_score_pctile",
    )
    model_metrics["test_independent_event_capture"] = {
        key: value for key, value in event_capture.items() if key != "event_rows"
    }
    model_metrics["simple_fragility_score"]["test_independent_event_capture"] = {
        key: value
        for key, value in fragility_event_capture.items()
        if key != "event_rows"
    }
    pd.DataFrame(event_capture["event_rows"]).to_csv(
        REPORT_DIR / "test_independent_events.csv", index=False
    )

    current_runups = scan_current_30d_runups(
        panel_4h,
        pd.to_datetime(window.end_open_ms, unit="ms", utc=True),
    )
    current_events = identify_current_events(features, window.end_date)
    event_join_columns = [
        "symbol",
        "date",
        "model_score",
        "model_score_pctile",
        "fragility_score",
        "fragility_score_pctile",
    ]
    current_events = current_events.merge(
        scored_modeling[event_join_columns], on=["symbol", "date"], how="left", validate="one_to_one"
    )
    event_references = current_events.copy()
    if not event_references.empty:
        event_references["reference_time"] = event_references["date"] + pd.Timedelta(days=1)
        event_references["reference_close"] = event_references["close"]
        event_references["spot_volume_1d"] = (
            event_references["quote_volume"] if MARKET == "spot" else np.nan
        )
        event_references["reference_kind"] = "current_30d_event_anchor"

    latest = features[
        (features["date"] < window.end_date)
        & (features["bars"] >= 4)
    ].sort_values("date").groupby("symbol", as_index=False).tail(1).copy()
    latest = latest[
        latest["is_altcoin"].fillna(False)
        & (latest["quote_volume"] >= MIN_DAILY_QUOTE_VOLUME)
    ].copy()
    latest["model_score"] = model.predict_score(latest)
    latest["model_score_pctile"] = latest["model_score"].rank(pct=True, method="first")
    latest_fragility_percentiles = [
        latest[factor].rank(pct=True).fillna(0.5) for factor in FRAGILITY_FACTORS
    ]
    latest["fragility_score"] = sum(latest_fragility_percentiles) / len(
        latest_fragility_percentiles
    )
    latest["fragility_score_pctile"] = latest["fragility_score"].rank(
        pct=True, method="first"
    )
    latest["ranking_score"] = latest[preferred_score_column]
    latest["ranking_score_pctile"] = latest["ranking_score"].rank(
        pct=True, method="first"
    )
    latest["ranking_method"] = preferred_ranking_method
    latest["chase_risk"] = (
        (latest["return_7d"] > 0.50)
        | (latest["return_1d"] > 0.25)
        | (latest["volume_ratio_1d_20d"] > 12)
    )
    latest["reference_time"] = latest["date"] + pd.Timedelta(days=1)
    latest["reference_close"] = latest["close"]
    latest["spot_volume_1d"] = latest["quote_volume"] if MARKET == "spot" else np.nan
    latest["reference_kind"] = "current_watchlist"
    watchlist = latest.sort_values("ranking_score", ascending=False).head(25).copy()

    runup_references = current_runups.copy()
    if not runup_references.empty:
        close_confirmed = runup_references["classification"].eq("close-confirmed")
        runup_references["reference_time"] = runup_references["low_time"] + pd.Timedelta(hours=4)
        runup_references.loc[close_confirmed, "reference_time"] = (
            runup_references.loc[close_confirmed, "close_entry_time"] + pd.Timedelta(hours=4)
        )
        runup_references["reference_close"] = runup_references["entry_low"]
        runup_references.loc[close_confirmed, "reference_close"] = runup_references.loc[
            close_confirmed, "entry_close"
        ]
        runup_references["reference_kind"] = "current_30d_runup_low"

    futures_reference_columns = [
        "symbol",
        "reference_time",
        "reference_close",
        "spot_volume_1d",
        "reference_kind",
    ]
    references = pd.concat(
        [
            event_references[futures_reference_columns] if not event_references.empty else pd.DataFrame(columns=futures_reference_columns),
            runup_references[futures_reference_columns] if not runup_references.empty else pd.DataFrame(columns=futures_reference_columns),
            watchlist[futures_reference_columns],
        ],
        ignore_index=True,
    )
    futures_enrichment, futures_errors = enrich_futures_at_references(references)
    join_keys = ["symbol", "reference_time", "reference_kind"]
    enrichment_columns = [
        *join_keys,
        "futures_listed",
        "oi_usd",
        "oi_change_1d",
        "oi_change_3d",
        "oi_to_cmc_mcap",
        "funding_bps",
        "funding_3d_mean_bps",
        "futures_volume_1d",
        "futures_to_spot_volume",
    ]
    if not current_events.empty:
        current_events["reference_time"] = current_events["date"] + pd.Timedelta(days=1)
        current_events["reference_kind"] = "current_30d_event_anchor"
        current_events = current_events.merge(
            futures_enrichment[enrichment_columns], on=join_keys, how="left", validate="one_to_one"
        )
    watchlist = watchlist.merge(
        futures_enrichment[enrichment_columns], on=join_keys, how="left", validate="one_to_one"
    )
    if not current_runups.empty:
        close_confirmed = current_runups["classification"].eq("close-confirmed")
        current_runups["reference_time"] = current_runups["low_time"] + pd.Timedelta(hours=4)
        current_runups.loc[close_confirmed, "reference_time"] = (
            current_runups.loc[close_confirmed, "close_entry_time"] + pd.Timedelta(hours=4)
        )
        current_runups["reference_kind"] = "current_30d_runup_low"
        current_runups = current_runups.merge(
            futures_enrichment[enrichment_columns], on=join_keys, how="left", validate="one_to_one"
        )
    recent_runup_symbols = set(
        current_runups.loc[
            current_runups["classification"] == "close-confirmed", "symbol"
        ].tolist()
    ) if not current_runups.empty else set()
    watchlist[RECENT_RUNUP_COLUMN] = watchlist["symbol"].isin(
        recent_runup_symbols
    )
    watchlist["derivatives_crowding_risk"] = (
        watchlist["funding_bps"].abs().gt(5).fillna(False)
        | watchlist["oi_to_cmc_mcap"].gt(0.25).fillna(False)
        | watchlist["futures_to_spot_volume"].gt(20).fillna(False)
    )
    watchlist["stage"] = np.select(
        [
            watchlist[RECENT_RUNUP_COLUMN],
            watchlist["chase_risk"],
            watchlist["derivatives_crowding_risk"],
            (watchlist["ranking_score_pctile"] >= 0.98)
            & ~watchlist["futures_listed"].fillna(False),
            watchlist["ranking_score_pctile"] >= 0.98,
        ],
        [
            f"cooldown_after_{TARGET_PCT}pct_runup",
            "reject_chase",
            "wait_derivatives_crowding",
            "spot_only_research_watch",
            "research_watch",
        ],
        default="monitor_only",
    )
    current_runups.to_csv(REPORT_DIR / "current_30d_runups.csv", index=False)
    current_events.to_csv(REPORT_DIR / "current_30d_target_events.csv", index=False)
    watchlist.to_csv(REPORT_DIR / "current_watchlist.csv", index=False)

    best_strategy = strategy_summary[
        (strategy_summary["strategy"] == "factor_model")
        & (strategy_summary["top_k"] == 3)
    ].iloc[0].to_dict()
    baseline_strategy = strategy_summary[
        (strategy_summary["strategy"] == "random_baseline")
        & (strategy_summary["top_k"] == 3)
    ].iloc[0].to_dict()
    fragility_strategy = strategy_summary[
        (strategy_summary["strategy"] == "simple_fragility_score")
        & (strategy_summary["top_k"] == 3)
    ].iloc[0].to_dict()
    train_fragility_strategy = train_strategy_summary[
        (train_strategy_summary["strategy"] == "simple_fragility_score")
        & (train_strategy_summary["top_k"] == 3)
    ].iloc[0].to_dict()
    useful_factor_rows = factor_table[factor_table["stable"]].head(10)
    analysis_summary = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_as_of_utc": pd.to_datetime(window.server_time_ms, unit="ms", utc=True).isoformat(),
        "label_definition": {
            "prediction_time": "After UTC day t closes",
            "entry": "First 4-hour open of UTC day t+1",
            "horizon_days": TARGET_DAYS,
            "positive": (
                f"Maximum high during t+1..t+14 is at least {TARGET_MULTIPLE:g}x "
                f"entry (+{TARGET_PCT}%)"
            ),
            "target_return": TARGET_RETURN,
            "target_price_multiple": TARGET_MULTIPLE,
            "current_event_window_days": CURRENT_WINDOW_DAYS,
            "current_window_scan": (
                "Maximum over every chronological earlier 4h close/low to every later "
                "4h high inside the latest 30-day window; not first-day versus last-day return"
            ),
        },
        "universe": {
            "source_market": MARKET,
            "current_usdt_market_symbols": int(len(universe)),
            "current_usdt_spot_symbols": int(len(universe)) if MARKET == "spot" else None,
            "current_usdt_perpetual_symbols": int(len(universe)) if MARKET == "futures" else None,
            "current_altcoin_symbols": int(sum(bool(row["is_altcoin"]) for row in universe)),
            "model_rows": int(len(modeling)),
            "train_start": train["date"].min().isoformat(),
            "train_end": train["date"].max().isoformat(),
            "test_start": test["date"].min().isoformat(),
            "test_end": test["date"].max().isoformat(),
            "train_positive_rows": int(train["label_target_int"].sum()),
            "test_positive_rows": int(test["label_target_int"].sum()),
            "train_positive_symbols": int(train.loc[train["label_target"], "symbol"].nunique()),
            "test_positive_symbols": int(test.loc[test["label_target"], "symbol"].nunique()),
        },
        "current_30d": {
            "close_confirmed_target_runups": int(
                (current_runups["classification"] == "close-confirmed").sum()
            ) if not current_runups.empty else 0,
            "wick_only_target_runups": int(
                (current_runups["classification"] == "wick-only").sum()
            ) if not current_runups.empty else 0,
            "runup_symbols": sorted(
                current_runups.loc[
                    current_runups["classification"] == "close-confirmed", "symbol"
                ].unique().tolist()
            ) if not current_runups.empty else [],
            "runups": [
                {key: json_ready(value) for key, value in row.items()}
                for row in current_runups.head(30).to_dict(orient="records")
            ],
            "independent_14d_predictive_events": int(len(current_events)),
            "predictive_event_symbols": sorted(current_events["symbol"].unique().tolist()) if not current_events.empty else [],
            "events": [
                {key: json_ready(value) for key, value in row.items()}
                for row in current_events.head(30).to_dict(orient="records")
            ],
        },
        "factor_validation": {
            "stable_factor_count": int(factor_table["stable"].sum()),
            "stable_factors": [
                {key: json_ready(value) for key, value in row.items()}
                for row in useful_factor_rows.to_dict(orient="records")
            ],
            "selected_model_factors": selected_factors,
            "model_coefficients": [
                {key: json_ready(value) for key, value in row.items()}
                for row in coefficients.to_dict(orient="records")
            ],
            "model_metrics": model_metrics,
            "preferred_ranking": model_metrics["preferred_ranking"],
        },
        "strategy_validation": {
            "factor_model_top3_phase_robust": {key: json_ready(value) for key, value in best_strategy.items()},
            "simple_fragility_top3_phase_robust": {
                key: json_ready(value) for key, value in fragility_strategy.items()
            },
            "train_simple_fragility_top3_phase_robust": {
                key: json_ready(value) for key, value in train_fragility_strategy.items()
            },
            "random_top3_phase_robust": {key: json_ready(value) for key, value in baseline_strategy.items()},
            "all": [
                {key: json_ready(value) for key, value in row.items()}
                for row in strategy_summary.to_dict(orient="records")
            ],
        },
        "current_watchlist": [
            {key: json_ready(value) for key, value in row.items()}
            for row in watchlist.head(20).to_dict(orient="records")
        ],
        "source_errors": source_errors,
        "futures_enrichment_errors": futures_errors,
        "limitations": [
            "Current ExchangeInfo omits historical delistings, creating survivorship bias.",
            f"The +{TARGET_PCT}% label uses future daily highs; actual fills, gaps, slippage and exchange limits can make the realized return much lower.",
            "OI history is available only for the recent Binance futures window, so OI is an event-enrichment field rather than a one-year validated model factor.",
            "This is observational factor research. Test-period lift can decay and does not imply causal predictability.",
        ],
    }
    (REPORT_DIR / "model_metrics.json").write_text(
        json.dumps(model_metrics, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (REPORT_DIR / "analysis_summary.json").write_text(
        json.dumps(analysis_summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(analysis_summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
