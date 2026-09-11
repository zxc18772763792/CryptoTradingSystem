"""Exploratory 30-day OI/funding factor audit for the futures universe.

Binance exposes only a recent window for public open-interest history.  This
script therefore keeps OI analysis separate from the 400-day price-factor
validation and evaluates only daily decision rows whose 14-day outcome is
complete and whose OI snapshot existed before entry.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from sklearn.metrics import roc_auc_score


HERE = Path(__file__).resolve().parent
FAPI = "https://fapi.binance.com"
TARGET_RETURN = 2.0
MIN_DAILY_QUOTE_VOLUME = 250_000.0
OI_CACHE = HERE / "cache" / "oi_4h_30d"


def get_json(path: str, params: dict[str, Any]) -> Any:
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            response = requests.get(f"{FAPI}{path}", params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - retain source failure by symbol
            last_error = exc
            time.sleep(0.4 * (2**attempt))
    raise RuntimeError(f"GET failed: {path} {params}") from last_error


def fetch_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    OI_CACHE.mkdir(parents=True, exist_ok=True)
    cache_path = OI_CACHE / f"{symbol}.csv.gz"
    if cache_path.exists():
        oi = pd.read_csv(cache_path, parse_dates=["oi_time"])
    else:
        oi_raw = get_json(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": "4h", "limit": 500},
        )
        oi = pd.DataFrame(oi_raw)
        if not oi.empty:
            oi["oi_time"] = pd.to_datetime(pd.to_numeric(oi["timestamp"]), unit="ms", utc=True)
            oi["oi_usd"] = pd.to_numeric(oi["sumOpenInterestValue"], errors="coerce")
            oi["circulating_supply"] = pd.to_numeric(oi["CMCCirculatingSupply"], errors="coerce")
            oi = oi.sort_values("oi_time")
            oi["oi_change_1d"] = oi["oi_usd"] / oi["oi_usd"].shift(6) - 1.0
            oi["oi_change_3d"] = oi["oi_usd"] / oi["oi_usd"].shift(18) - 1.0
            oi["oi_volatility_3d"] = oi["oi_usd"].pct_change().rolling(18, min_periods=8).std()
            oi = oi[
                [
                    "oi_time",
                    "oi_usd",
                    "circulating_supply",
                    "oi_change_1d",
                    "oi_change_3d",
                    "oi_volatility_3d",
                ]
            ]
            oi.to_csv(cache_path, index=False, compression="gzip")
    if not oi.empty:
        oi["oi_time"] = pd.to_datetime(oi["oi_time"], utc=True)
    return oi, pd.DataFrame()


def factor_metric(rows: pd.DataFrame, factor: str) -> dict[str, Any]:
    data = rows[["date", "label_200", factor]].copy()
    data[factor] = pd.to_numeric(data[factor], errors="coerce").replace(
        [np.inf, -np.inf], np.nan
    )
    data = data.dropna()
    positives = data.loc[data["label_200"], factor]
    negatives = data.loc[~data["label_200"], factor]
    if positives.empty or negatives.empty:
        return {"factor": factor, "n": int(len(data)), "positives": int(data.label_200.sum())}
    positive_median = float(positives.median())
    negative_median = float(negatives.median())
    direction = 1 if positive_median >= negative_median else -1
    data["score"] = direction * data[factor]
    data["score_pctile"] = data.groupby("date")["score"].rank(pct=True, method="average")
    top10 = data[data["score_pctile"] >= 0.90]
    top20 = data[data["score_pctile"] >= 0.80]
    base_rate = float(data["label_200"].mean())
    top10_rate = float(top10["label_200"].mean()) if not top10.empty else np.nan
    top20_rate = float(top20["label_200"].mean()) if not top20.empty else np.nan

    rng = np.random.default_rng(20260718)
    dates = np.array(sorted(data["date"].unique()))
    lifts: list[float] = []
    for _ in range(1000):
        sampled = rng.choice(dates, size=len(dates), replace=True)
        sample = pd.concat([data[data["date"] == date] for date in sampled], ignore_index=True)
        sample_top = sample[sample["score_pctile"] >= 0.90]
        sample_base = float(sample["label_200"].mean())
        if sample_base > 0 and not sample_top.empty:
            lifts.append(float(sample_top["label_200"].mean() / sample_base))
    return {
        "factor": factor,
        "direction": "higher" if direction > 0 else "lower",
        "n": int(len(data)),
        "positives": int(data["label_200"].sum()),
        "positive_median": positive_median,
        "negative_median": negative_median,
        "auc": float(roc_auc_score(data["label_200"], data["score"])),
        "base_rate": base_rate,
        "top10_rate": top10_rate,
        "top10_lift": float(top10_rate / base_rate) if base_rate > 0 else np.nan,
        "top20_rate": top20_rate,
        "top20_lift": float(top20_rate / base_rate) if base_rate > 0 else np.nan,
        "bootstrap_top10_lift_median": float(np.median(lifts)),
        "bootstrap_top10_lift_lower_95": float(np.quantile(lifts, 0.025)),
        "bootstrap_top10_lift_upper_95": float(np.quantile(lifts, 0.975)),
    }


def main() -> None:
    universe = pd.read_csv(HERE / "futures_universe.csv")
    features = pd.read_csv(HERE / "daily_feature_panel.csv.gz", parse_dates=["date"])
    eligible = features.loc[
        features["is_altcoin"].fillna(False)
        & features["target_complete"].fillna(False)
        & (features["quote_volume"] >= MIN_DAILY_QUOTE_VOLUME)
        & (features["entry_open"] > 0)
        & (features["bars"] >= 4)
    ].copy()
    eligible["reference_time"] = eligible["date"] + pd.Timedelta(days=1)
    eligible["label_200"] = eligible["target_max_return_14d"] >= TARGET_RETURN

    histories: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    errors: list[dict[str, str]] = []
    symbols = universe.loc[universe["is_altcoin"], "symbol"].tolist()
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(fetch_symbol, symbol): symbol for symbol in symbols}
        for completed, future in enumerate(as_completed(futures), start=1):
            symbol = futures[future]
            try:
                histories[symbol] = future.result()
            except Exception as exc:  # noqa: BLE001 - record per-symbol availability
                errors.append({"symbol": symbol, "error": repr(exc)})
            if completed % 50 == 0 or completed == len(futures):
                print(f"OI progress {completed}/{len(futures)} loaded={len(histories)} errors={len(errors)}")

    joined: list[pd.DataFrame] = []
    for symbol, rows in eligible.groupby("symbol", sort=False):
        history = histories.get(symbol)
        if not history:
            continue
        oi, funding = history
        if oi.empty:
            continue
        group = rows.sort_values("reference_time").copy()
        group = pd.merge_asof(
            group,
            oi,
            left_on="reference_time",
            right_on="oi_time",
            direction="backward",
            tolerance=pd.Timedelta(hours=8),
        )
        group["oi_to_cmc_mcap"] = group["oi_usd"] / (
            group["circulating_supply"] * group["close"]
        )
        group.loc[group["circulating_supply"] <= 0, "oi_to_cmc_mcap"] = np.nan
        group["oi_to_cmc_mcap"] = group["oi_to_cmc_mcap"].replace(
            [np.inf, -np.inf], np.nan
        )
        joined.append(group)

    panel = pd.concat(joined, ignore_index=True)
    oi_factors = [
        "oi_to_cmc_mcap",
        "oi_change_1d",
        "oi_change_3d",
        "oi_volatility_3d",
    ]
    metrics = pd.DataFrame([factor_metric(panel, factor) for factor in oi_factors])
    main_events = pd.read_csv(HERE / "current_30d_runups.csv")
    main_events = main_events[main_events["classification"] == "close-confirmed"]

    summary = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "scope": {
            "market": "Binance USD-M USDT perpetuals",
            "decision_clock": "After UTC day t closes; enter at first 4h open of t+1",
            "label": "future 14-day high >= 3x entry (+200%)",
            "history_limit": "public OI history is recent only; this is a 30-day exploratory audit, not the 400-day validation",
        },
        "coverage": {
            "universe_altcoin_symbols": int(len(symbols)),
            "histories_loaded": int(len(histories)),
            "fetch_errors": int(len(errors)),
            "eligible_rows_with_oi": int(panel["oi_usd"].notna().sum()),
            "eligible_symbols_with_oi": int(panel.loc[panel["oi_usd"].notna(), "symbol"].nunique()),
            "first_decision_date": panel.loc[panel["oi_usd"].notna(), "date"].min().isoformat(),
            "last_decision_date": panel.loc[panel["oi_usd"].notna(), "date"].max().isoformat(),
            "positive_rows": int(panel.loc[panel["oi_usd"].notna(), "label_200"].sum()),
        },
        "factor_metrics": metrics.replace({np.nan: None}).to_dict(orient="records"),
        "current_close_confirmed_events": {
            "count": int(len(main_events)),
            "oi_to_mcap_available": int(main_events["oi_to_cmc_mcap"].notna().sum()),
            "oi_change_1d_available": int(main_events["oi_change_1d"].notna().sum()),
            "median_oi_to_mcap": float(main_events["oi_to_cmc_mcap"].median()),
            "median_oi_change_1d": float(main_events["oi_change_1d"].median()),
            "events_with_positive_oi_change_1d": int((main_events["oi_change_1d"] > 0).sum()),
        },
        "errors": errors,
        "limitations": [
            "Only recent public OI history is available, so the result is exploratory and highly regime-dependent.",
            "CMC circulating supply embedded in the Binance OI endpoint may be missing or noisy for new listings.",
            "Futures-only listings have no same-symbol Binance spot pair for a spot-volume comparison.",
        ],
    }
    panel.to_csv(HERE / "oi_daily_panel_30d.csv.gz", index=False, compression="gzip")
    metrics.to_csv(HERE / "oi_factor_metrics_30d.csv", index=False)
    (HERE / "oi_factor_summary_30d.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
