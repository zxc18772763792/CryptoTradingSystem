"""Binance small-cap + open-interest research snapshot.

This script uses only public Binance market-data endpoints plus the local
Altcoin Radar research-universe snapshot. It creates a bounded 30-day event
study and a current cross-sectional candidate table. The event study is
descriptive: Binance exposes only about one month of OI history and the
universe is selected as of the analysis date.
"""

from __future__ import annotations

import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests


REPORT_DIR = Path(__file__).resolve().parent
REPO_ROOT = REPORT_DIR.parents[1]
RADAR_SNAPSHOT = REPO_ROOT / "output" / "altcoin_radar_snapshot_2026-07-18.json"
FAPI = "https://fapi.binance.com"
SPOT_API = "https://api.binance.com"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "binance-oi-research/1.0"})


def get_json(base: str, path: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{base}{path}"
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            response = SESSION.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - bounded public API retry
            last_error = exc
            time.sleep(0.4 * (2**attempt))
    raise RuntimeError(f"GET failed after retries: {url}: {last_error}")


def rows_by_symbol(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("symbol") or ""): row for row in rows if row.get("symbol")}


def load_universe() -> tuple[list[str], dict[str, float]]:
    payload = json.loads(RADAR_SNAPSHOT.read_text(encoding="utf-8-sig"))
    rows = payload.get("rows", [])
    universe = [str(row["symbol"]).replace("/", "") for row in rows]
    comparison_caps = {
        str(row["symbol"]).replace("/", ""): float(
            (row.get("metrics") or {}).get("market_cap_usd") or 0.0
        )
        for row in rows
    }
    return universe, comparison_caps


def fetch_symbol_history(symbol: str, spot_symbols: set[str]) -> tuple[pd.DataFrame, str | None]:
    try:
        oi = get_json(
            FAPI,
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": "2h", "limit": 500},
        )
        futures_klines = get_json(
            FAPI,
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": "2h", "limit": 500},
        )
        funding = get_json(
            FAPI,
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "limit": 200},
        )
        spot_klines: list[list[Any]] = []
        if symbol in spot_symbols:
            spot_klines = get_json(
                SPOT_API,
                "/api/v3/klines",
                {"symbol": symbol, "interval": "2h", "limit": 500},
            )

        oi_df = pd.DataFrame(oi)
        if oi_df.empty:
            return pd.DataFrame(), f"{symbol}: no OI history"
        oi_df = oi_df.rename(
            columns={
                "timestamp": "ts",
                "sumOpenInterestValue": "oi_usd",
                "CMCCirculatingSupply": "circulating_supply",
            }
        )
        oi_df["ts"] = pd.to_datetime(pd.to_numeric(oi_df["ts"]), unit="ms", utc=True)
        oi_df["oi_usd"] = pd.to_numeric(oi_df["oi_usd"], errors="coerce")
        oi_df["circulating_supply"] = pd.to_numeric(
            oi_df["circulating_supply"], errors="coerce"
        )

        futures_df = pd.DataFrame(
            futures_klines,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "base_volume",
                "close_time",
                "futures_quote_volume",
                "trades",
                "taker_buy_base",
                "taker_buy_quote",
                "ignore",
            ],
        )
        futures_df["ts"] = pd.to_datetime(
            pd.to_numeric(futures_df["open_time"]), unit="ms", utc=True
        )
        for col in ["open", "high", "low", "close", "futures_quote_volume", "taker_buy_quote"]:
            futures_df[col] = pd.to_numeric(futures_df[col], errors="coerce")

        merged = pd.merge_asof(
            oi_df.sort_values("ts"),
            futures_df.sort_values("ts")[[
                "ts",
                "open",
                "high",
                "low",
                "close",
                "futures_quote_volume",
                "taker_buy_quote",
            ]],
            on="ts",
            direction="nearest",
            tolerance=pd.Timedelta("2h"),
        )

        funding_df = pd.DataFrame(funding)
        if not funding_df.empty:
            funding_df["ts"] = pd.to_datetime(
                pd.to_numeric(funding_df["fundingTime"]), unit="ms", utc=True
            )
            funding_df["funding_rate"] = pd.to_numeric(
                funding_df["fundingRate"], errors="coerce"
            )
            merged = pd.merge_asof(
                merged.sort_values("ts"),
                funding_df.sort_values("ts")[["ts", "funding_rate"]],
                on="ts",
                direction="backward",
                tolerance=pd.Timedelta("12h"),
            )
        else:
            merged["funding_rate"] = np.nan

        if spot_klines:
            spot_df = pd.DataFrame(
                spot_klines,
                columns=[
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "base_volume",
                    "close_time",
                    "spot_quote_volume",
                    "trades",
                    "taker_buy_base",
                    "taker_buy_quote",
                    "ignore",
                ],
            )
            spot_df["ts"] = pd.to_datetime(
                pd.to_numeric(spot_df["open_time"]), unit="ms", utc=True
            )
            spot_df["spot_quote_volume"] = pd.to_numeric(
                spot_df["spot_quote_volume"], errors="coerce"
            )
            merged = pd.merge_asof(
                merged.sort_values("ts"),
                spot_df.sort_values("ts")[["ts", "spot_quote_volume"]],
                on="ts",
                direction="nearest",
                tolerance=pd.Timedelta("2h"),
            )
        else:
            merged["spot_quote_volume"] = 0.0

        merged["symbol"] = symbol
        merged["spot_listed"] = symbol in spot_symbols
        return merged, None
    except Exception as exc:  # noqa: BLE001 - capture per-symbol data gaps
        return pd.DataFrame(), f"{symbol}: {type(exc).__name__}: {exc}"


def build_panel(universe: list[str]) -> tuple[pd.DataFrame, list[str]]:
    spot_info = get_json(SPOT_API, "/api/v3/exchangeInfo")
    spot_symbols = {
        str(row.get("symbol") or "")
        for row in spot_info.get("symbols", [])
        if row.get("status") == "TRADING"
    }
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(fetch_symbol_history, symbol, spot_symbols): symbol
            for symbol in universe
        }
        for future in as_completed(futures):
            frame, error = future.result()
            if not frame.empty:
                frames.append(frame)
            if error:
                errors.append(error)
    panel = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return panel, sorted(errors)


def engineer_features(panel: pd.DataFrame, comparison_caps: dict[str, float]) -> pd.DataFrame:
    frame = panel.copy().sort_values(["symbol", "ts"])
    by_symbol = frame.groupby("symbol", group_keys=False)
    frame["market_cap_usd"] = frame["circulating_supply"] * frame["close"]
    latest_caps = (
        frame.sort_values("ts").groupby("symbol", as_index=False).tail(1)
        .set_index("symbol")["market_cap_usd"]
        .to_dict()
    )
    conflict_symbols: set[str] = set()
    cap_ratios: dict[str, float] = {}
    for symbol, binance_cap in latest_caps.items():
        comparison_cap = float(comparison_caps.get(symbol) or 0.0)
        ratio = float(binance_cap / comparison_cap) if comparison_cap > 0 else np.nan
        cap_ratios[symbol] = ratio
        if pd.notna(ratio) and (ratio < 0.5 or ratio > 2.0):
            conflict_symbols.add(symbol)
    frame["mcap_source_ratio"] = frame["symbol"].map(cap_ratios)
    frame["mcap_source_conflict"] = frame["symbol"].isin(conflict_symbols)
    frame["oi_to_mcap"] = frame["oi_usd"] / frame["market_cap_usd"].replace(0, np.nan)
    frame["oi_change_24h"] = by_symbol["oi_usd"].pct_change(12)
    frame["price_change_24h"] = by_symbol["close"].pct_change(12)
    frame["fwd_return_24h"] = by_symbol["close"].shift(-12) / frame["close"] - 1
    frame["fwd_return_7d"] = by_symbol["close"].shift(-84) / frame["close"] - 1
    frame["futures_volume_24h"] = by_symbol["futures_quote_volume"].transform(
        lambda values: values.rolling(12, min_periods=8).sum()
    )
    frame["spot_volume_24h"] = by_symbol["spot_quote_volume"].transform(
        lambda values: values.rolling(12, min_periods=8).sum()
    )
    total_volume = frame["spot_volume_24h"] + frame["futures_volume_24h"]
    frame["spot_share"] = frame["spot_volume_24h"] / total_volume.replace(0, np.nan)
    frame["funding_bps"] = frame["funding_rate"] * 10_000
    frame["taker_buy_share"] = frame["taker_buy_quote"] / frame[
        "futures_quote_volume"
    ].replace(0, np.nan)
    frame["date"] = frame["ts"].dt.floor("D")

    frame["oi_mcap_pctile"] = frame.groupby("ts")["oi_to_mcap"].rank(pct=True)
    frame["cap_small_pctile"] = 1 - frame.groupby("ts")["market_cap_usd"].rank(pct=True)

    frame["base_small_high_oi"] = (
        frame["spot_listed"]
        & ~frame["mcap_source_conflict"]
        & frame["market_cap_usd"].between(50e6, 1.5e9)
        & (frame["oi_mcap_pctile"] >= 0.60)
        & (frame["spot_volume_24h"] >= 1e6)
    )
    frame["direction_confirmed"] = frame["base_small_high_oi"] & (
        (frame["oi_change_24h"] >= 0.03)
        & frame["price_change_24h"].between(0.0, 0.10)
    )
    frame["quality_ignition"] = frame["direction_confirmed"] & (
        frame["funding_rate"].between(-0.0002, 0.0003)
        & (frame["spot_share"] >= 0.15)
    )
    frame["crowded_chase"] = frame["base_small_high_oi"] & (
        (frame["funding_rate"].abs() > 0.0005)
        | (frame["price_change_24h"].abs() > 0.15)
        | (frame["oi_change_24h"].abs() > 0.20)
    )
    return frame


def signal_onsets(daily: pd.DataFrame, signal: str) -> pd.DataFrame:
    ordered = daily.sort_values(["symbol", "ts"]).copy()
    previous = ordered.groupby("symbol")[signal].shift(1).eq(True)
    return ordered[ordered[signal].astype(bool) & ~previous]


def summarise_signal(rows: pd.DataFrame, label: str) -> dict[str, Any]:
    def horizon_stats(column: str) -> dict[str, Any]:
        values = pd.to_numeric(rows[column], errors="coerce").dropna()
        if values.empty:
            return {"n": 0, "mean": None, "median": None, "win_rate": None}
        return {
            "n": int(values.size),
            "mean": float(values.mean()),
            "median": float(values.median()),
            "win_rate": float((values > 0).mean()),
        }

    return {
        "signal": label,
        "events": int(len(rows)),
        "return_24h": horizon_stats("fwd_return_24h"),
        "return_7d": horizon_stats("fwd_return_7d"),
    }


def summarise_oi_buckets(daily: pd.DataFrame) -> list[dict[str, Any]]:
    eligible = daily[
        daily["spot_listed"]
        & ~daily["mcap_source_conflict"]
        & daily["market_cap_usd"].between(50e6, 1.5e9)
        & (daily["spot_volume_24h"] >= 1e6)
    ].copy()
    eligible["oi_bucket"] = pd.cut(
        eligible["oi_mcap_pctile"],
        bins=[0.0, 0.25, 0.50, 0.75, 1.0],
        labels=["Q1 low", "Q2", "Q3", "Q4 high"],
        include_lowest=True,
    )
    output: list[dict[str, Any]] = []
    for bucket, rows in eligible.groupby("oi_bucket", observed=True):
        r24 = pd.to_numeric(rows["fwd_return_24h"], errors="coerce").dropna()
        r7 = pd.to_numeric(rows["fwd_return_7d"], errors="coerce").dropna()
        output.append(
            {
                "bucket": str(bucket),
                "rows": int(len(rows)),
                "symbols": int(rows["symbol"].nunique()),
                "median_oi_to_mcap": float(rows["oi_to_mcap"].median()),
                "return_24h_n": int(r24.size),
                "return_24h_mean": float(r24.mean()) if not r24.empty else None,
                "return_24h_median": float(r24.median()) if not r24.empty else None,
                "return_24h_win_rate": float((r24 > 0).mean()) if not r24.empty else None,
                "return_7d_n": int(r7.size),
                "return_7d_mean": float(r7.mean()) if not r7.empty else None,
                "return_7d_median": float(r7.median()) if not r7.empty else None,
                "return_7d_win_rate": float((r7 > 0).mean()) if not r7.empty else None,
            }
        )
    return output


def summarise_daily_correlations(daily: pd.DataFrame) -> list[dict[str, Any]]:
    eligible = daily[
        daily["spot_listed"]
        & ~daily["mcap_source_conflict"]
        & daily["market_cap_usd"].between(50e6, 1.5e9)
        & (daily["spot_volume_24h"] >= 1e6)
    ].copy()
    output: list[dict[str, Any]] = []
    for feature in [
        "oi_to_mcap",
        "oi_change_24h",
        "price_change_24h",
        "spot_share",
        "funding_rate",
    ]:
        correlations: list[float] = []
        for _, rows in eligible.groupby("date"):
            paired = rows[[feature, "fwd_return_24h"]].dropna()
            if len(paired) < 4:
                continue
            # Rank then compute Pearson manually. This avoids an optional SciPy
            # DLL dependency while remaining equivalent to Spearman correlation.
            x_rank = paired[feature].rank(method="average")
            y_rank = paired["fwd_return_24h"].rank(method="average")
            x_delta = x_rank - x_rank.mean()
            y_delta = y_rank - y_rank.mean()
            denominator = math.sqrt(float((x_delta**2).sum() * (y_delta**2).sum()))
            value = float((x_delta * y_delta).sum() / denominator) if denominator else np.nan
            if pd.notna(value):
                correlations.append(float(value))
        series = pd.Series(correlations, dtype=float)
        output.append(
            {
                "feature": feature,
                "days": int(series.size),
                "mean_spearman": float(series.mean()) if not series.empty else None,
                "median_spearman": float(series.median()) if not series.empty else None,
            }
        )
    return output


def score_current(latest: pd.DataFrame) -> pd.DataFrame:
    current = latest.copy()
    log_cap = np.log10(current["market_cap_usd"].clip(lower=1))
    small_cap = ((math.log10(1.5e9) - log_cap) / (math.log10(1.5e9) - math.log10(50e6))).clip(0, 1)
    oi_quality = (current["oi_to_mcap"] / 0.06).clip(0, 1)
    oi_growth = (current["oi_change_24h"] / 0.10).clip(0, 1)
    price_confirm = (current["price_change_24h"] / 0.08).clip(0, 1)
    funding_quality = (1 - (current["funding_bps"] - 0.5).abs() / 5).clip(0, 1)
    spot_quality = (current["spot_share"] / 0.25).clip(0, 1).fillna(0)
    liquidity = (np.log10(current["spot_volume_24h"].clip(lower=1)) - 6) / 2
    liquidity = liquidity.clip(0, 1)
    current["setup_score"] = (
        0.22 * small_cap
        + 0.22 * oi_quality
        + 0.16 * oi_growth
        + 0.12 * price_confirm
        + 0.10 * funding_quality
        + 0.10 * spot_quality
        + 0.08 * liquidity
    )
    hard_risk = (
        (~current["spot_listed"])
        | current["mcap_source_conflict"]
        | (current["market_cap_usd"] < 50e6)
        | (current["spot_volume_24h"] < 1e6)
        | (current["funding_rate"].abs() > 0.0005)
        | (current["price_change_24h"].abs() > 0.15)
        | (current["oi_change_24h"].abs() > 0.20)
    )
    current.loc[hard_risk, "setup_score"] *= 0.55
    current["stage"] = np.select(
        [current["quality_ignition"], current["direction_confirmed"], current["base_small_high_oi"]],
        ["quality_ignition", "direction_confirmed", "watch"],
        default="reject_or_wait",
    )
    current.loc[current["crowded_chase"], "stage"] = "crowded_chase"
    current.loc[current["mcap_source_conflict"], "stage"] = "data_conflict"
    return current.sort_values("setup_score", ascending=False)


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


def main() -> None:
    universe, comparison_caps = load_universe()
    raw_panel, errors = build_panel(universe)
    if raw_panel.empty:
        raise RuntimeError("No Binance history could be loaded")
    panel = engineer_features(raw_panel, comparison_caps)

    # One observation per asset/day reduces intraday overlap in the event study.
    daily = (
        panel.sort_values("ts")
        .groupby(["symbol", "date"], as_index=False)
        .tail(1)
        .sort_values(["symbol", "ts"])
    )
    signal_rows = {
        "small_cap_high_oi": signal_onsets(daily, "base_small_high_oi"),
        "direction_confirmed": signal_onsets(daily, "direction_confirmed"),
        "quality_ignition": signal_onsets(daily, "quality_ignition"),
        "crowded_chase": signal_onsets(daily, "crowded_chase"),
    }
    event_summary = [
        summarise_signal(rows, label) for label, rows in signal_rows.items()
    ]
    oi_bucket_summary = summarise_oi_buckets(daily)
    daily_correlations = summarise_daily_correlations(daily)

    latest = panel.sort_values("ts").groupby("symbol", as_index=False).tail(1)
    current = score_current(latest)
    current_columns = [
        "symbol",
        "ts",
        "spot_listed",
        "mcap_source_conflict",
        "mcap_source_ratio",
        "market_cap_usd",
        "oi_usd",
        "oi_to_mcap",
        "oi_change_24h",
        "price_change_24h",
        "funding_bps",
        "spot_volume_24h",
        "futures_volume_24h",
        "spot_share",
        "setup_score",
        "stage",
    ]

    panel.to_csv(REPORT_DIR / "binance_30d_panel.csv", index=False)
    current[current_columns].to_csv(REPORT_DIR / "current_candidate_snapshot.csv", index=False)
    for label, rows in signal_rows.items():
        rows.to_csv(REPORT_DIR / f"events_{label}.csv", index=False)

    summary = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "source": "Binance public USD-M futures and Spot REST APIs",
        "universe_source": str(RADAR_SNAPSHOT.name),
        "universe_requested": len(universe),
        "symbols_loaded": int(panel["symbol"].nunique()),
        "panel_rows": int(len(panel)),
        "start": panel["ts"].min().isoformat(),
        "end": panel["ts"].max().isoformat(),
        "errors": errors,
        "data_quality": {
            "duplicate_symbol_timestamp_rows": int(
                panel.duplicated(["symbol", "ts"]).sum()
            ),
            "oi_usd_non_null_rate": float(panel["oi_usd"].notna().mean()),
            "close_non_null_rate": float(panel["close"].notna().mean()),
            "funding_non_null_rate": float(panel["funding_rate"].notna().mean()),
            "market_cap_conflict_symbols": sorted(
                panel.loc[panel["mcap_source_conflict"], "symbol"].unique().tolist()
            ),
        },
        "event_summary": event_summary,
        "oi_bucket_summary": oi_bucket_summary,
        "daily_cross_sectional_correlations": daily_correlations,
        "current_top10": [
            {key: json_ready(value) for key, value in row.items()}
            for row in current[current_columns].head(10).to_dict(orient="records")
        ],
        "limitations": [
            "Binance OI history is limited to roughly the latest month.",
            "The universe is selected as of the analysis date, creating survivorship and selection bias.",
            "This event study evaluates 24-hour and 7-day follow-through, not 10x outcomes.",
            "Token unlock schedules, FDV, holder concentration, revenue and on-chain adoption are not available from Binance public market endpoints and remain hard gates for a long-horizon strategy.",
        ],
    }
    (REPORT_DIR / "analysis_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
