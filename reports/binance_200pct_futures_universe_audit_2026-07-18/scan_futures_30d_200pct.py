"""Scan Binance USD-M USDT perpetuals for an internal 30-day +200% path.

The prior study used the current Binance USDT spot universe.  This audit uses
the current TRADING USDT-margined perpetual universe, which includes
futures-only symbols such as AKEUSDT and BTWUSDT.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
import requests


HERE = Path(__file__).resolve().parent
TARGET_RETURN = 2.0
WINDOW_DAYS = 30
INTERVAL_MS = 4 * 60 * 60 * 1000
FAPI = "https://fapi.binance.com"
SPOT_API = "https://api.binance.com"


def get_json(url: str, params: dict[str, Any] | None = None) -> Any:
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - retain the request error text
            last_error = exc
            time.sleep(0.4 * (2**attempt))
    raise RuntimeError(f"request failed after retries: {url} {params}") from last_error


def fetch_symbol(
    symbol: str,
    base_asset: str,
    onboard_date: int,
    start_ms: int,
    end_open_ms: int,
) -> pd.DataFrame:
    raw = get_json(
        f"{FAPI}/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": "4h",
            "startTime": start_ms,
            "endTime": end_open_ms + INTERVAL_MS - 1,
            "limit": 300,
        },
    )
    columns = [
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
    frame = pd.DataFrame(raw, columns=columns)
    if frame.empty:
        return frame
    for column in ["open", "high", "low", "close", "base_volume", "quote_volume", "taker_buy_quote"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["trades"] = pd.to_numeric(frame["trades"], errors="coerce").astype("Int64")
    frame["open_time_ms"] = pd.to_numeric(frame["open_time_ms"], errors="coerce").astype("Int64")
    frame["open_time"] = pd.to_datetime(frame["open_time_ms"], unit="ms", utc=True)
    frame = frame.loc[frame["open_time_ms"] <= end_open_ms].copy()
    frame["symbol"] = symbol
    frame["base_asset"] = base_asset
    frame["onboard_date"] = pd.to_datetime(onboard_date, unit="ms", utc=True)
    return frame[
        [
            "symbol",
            "base_asset",
            "onboard_date",
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
    ]


def scan_symbol(rows: pd.DataFrame, spot_symbols: set[str]) -> dict[str, Any] | None:
    rows = rows.sort_values("open_time").reset_index(drop=True)
    if len(rows) < 2:
        return None
    closes = rows["close"].to_numpy(float)
    lows = rows["low"].to_numpy(float)
    highs = rows["high"].to_numpy(float)
    min_close = closes[0]
    min_close_index = 0
    min_low = lows[0]
    min_low_index = 0
    best_close = (-float("inf"), 0, 1)
    best_low = (-float("inf"), 0, 1)
    for peak_index in range(1, len(rows)):
        close_return = highs[peak_index] / min_close - 1.0
        low_return = highs[peak_index] / min_low - 1.0
        if close_return > best_close[0]:
            best_close = (close_return, min_close_index, peak_index)
        if low_return > best_low[0]:
            best_low = (low_return, min_low_index, peak_index)
        if closes[peak_index] < min_close:
            min_close = closes[peak_index]
            min_close_index = peak_index
        if lows[peak_index] < min_low:
            min_low = lows[peak_index]
            min_low_index = peak_index

    close_return, close_entry_index, close_peak_index = best_close
    low_return, low_entry_index, low_peak_index = best_low
    if close_return < TARGET_RETURN and low_return < TARGET_RETURN:
        return None
    return {
        "symbol": rows.loc[0, "symbol"],
        "base_asset": rows.loc[0, "base_asset"],
        "spot_listed_usdt": rows.loc[0, "symbol"] in spot_symbols,
        "onboard_date": rows.loc[0, "onboard_date"],
        "bars_in_window": int(len(rows)),
        "classification": "close-confirmed" if close_return >= TARGET_RETURN else "wick-only",
        "close_to_later_high_return": float(close_return),
        "close_entry_time": rows.loc[close_entry_index, "open_time"],
        "entry_close": float(rows.loc[close_entry_index, "close"]),
        "close_peak_time": rows.loc[close_peak_index, "open_time"],
        "close_peak_high": float(rows.loc[close_peak_index, "high"]),
        "close_duration_days": float(
            (rows.loc[close_peak_index, "open_time"] - rows.loc[close_entry_index, "open_time"])
            / pd.Timedelta(days=1)
        ),
        "low_to_later_high_return": float(low_return),
        "low_entry_time": rows.loc[low_entry_index, "open_time"],
        "entry_low": float(rows.loc[low_entry_index, "low"]),
        "low_peak_time": rows.loc[low_peak_index, "open_time"],
        "low_peak_high": float(rows.loc[low_peak_index, "high"]),
        "low_duration_days": float(
            (rows.loc[low_peak_index, "open_time"] - rows.loc[low_entry_index, "open_time"])
            / pd.Timedelta(days=1)
        ),
        "threshold_return": TARGET_RETURN,
        "threshold_price_multiple": 1.0 + TARGET_RETURN,
    }


def main() -> None:
    HERE.mkdir(parents=True, exist_ok=True)
    server_time_ms = int(get_json(f"{FAPI}/fapi/v1/time")["serverTime"])
    current_open_ms = (server_time_ms // INTERVAL_MS) * INTERVAL_MS
    end_open_ms = current_open_ms - INTERVAL_MS
    start_ms = end_open_ms - WINDOW_DAYS * 24 * 60 * 60 * 1000

    futures_info = get_json(f"{FAPI}/fapi/v1/exchangeInfo")
    universe_records = [
        {
            "symbol": item["symbol"],
            "base_asset": item["baseAsset"],
            "quote_asset": item["quoteAsset"],
            "contract_type": item["contractType"],
            "status": item["status"],
            "onboard_date_ms": int(item["onboardDate"]),
            "onboard_date": pd.to_datetime(item["onboardDate"], unit="ms", utc=True),
        }
        for item in futures_info["symbols"]
        if item.get("quoteAsset") == "USDT"
        and item.get("contractType") == "PERPETUAL"
        and item.get("status") == "TRADING"
    ]
    universe = pd.DataFrame(universe_records).sort_values("symbol").reset_index(drop=True)

    spot_info = get_json(f"{SPOT_API}/api/v3/exchangeInfo")
    spot_symbols = {
        item["symbol"]
        for item in spot_info["symbols"]
        if item.get("quoteAsset") == "USDT"
        and item.get("status") == "TRADING"
        and item.get("isSpotTradingAllowed", True)
    }
    universe["spot_listed_usdt"] = universe["symbol"].isin(spot_symbols)

    frames: list[pd.DataFrame] = []
    errors: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {
            pool.submit(
                fetch_symbol,
                row.symbol,
                row.base_asset,
                int(row.onboard_date_ms),
                start_ms,
                end_open_ms,
            ): row.symbol
            for row in universe.itertuples(index=False)
        }
        for completed, future in enumerate(as_completed(futures), start=1):
            symbol = futures[future]
            try:
                frame = future.result()
                if not frame.empty:
                    frames.append(frame)
            except Exception as exc:  # noqa: BLE001 - save exact per-symbol failure
                errors.append({"symbol": symbol, "error": repr(exc)})
            if completed % 50 == 0 or completed == len(futures):
                print(f"progress {completed}/{len(futures)} loaded={len(frames)} errors={len(errors)}")

    panel = pd.concat(frames, ignore_index=True).sort_values(["symbol", "open_time"])
    events = pd.DataFrame(
        [
            event
            for _, group in panel.groupby("symbol", sort=False)
            if (event := scan_symbol(group, spot_symbols)) is not None
        ]
    ).sort_values(
        ["classification", "close_to_later_high_return"],
        ascending=[True, False],
    ).reset_index(drop=True)

    invalid_ohlc = (
        (panel[["open", "high", "low", "close"]] <= 0).any(axis=1)
        | (panel["high"] < panel[["open", "low", "close"]].max(axis=1))
        | (panel["low"] > panel[["open", "high", "close"]].min(axis=1))
    )
    summary = {
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "server_time_utc": pd.to_datetime(server_time_ms, unit="ms", utc=True).isoformat(),
        "definition": {
            "window_start_utc": pd.to_datetime(start_ms, unit="ms", utc=True).isoformat(),
            "window_end_open_utc": pd.to_datetime(end_open_ms, unit="ms", utc=True).isoformat(),
            "window_days": WINDOW_DAYS,
            "path": "any earlier 4h close/low to a strictly later 4h high",
            "threshold_return": TARGET_RETURN,
            "threshold_price_multiple": 1.0 + TARGET_RETURN,
            "not_first_to_last": True,
        },
        "universe": {
            "current_trading_usdt_perpetuals": int(len(universe)),
            "with_usdt_spot_pair": int(universe["spot_listed_usdt"].sum()),
            "futures_only": int((~universe["spot_listed_usdt"]).sum()),
            "loaded_symbols": int(panel["symbol"].nunique()),
            "load_errors": int(len(errors)),
        },
        "events": {
            "total": int(len(events)),
            "close_confirmed": int((events["classification"] == "close-confirmed").sum()),
            "wick_only": int((events["classification"] == "wick-only").sum()),
            "futures_only_total": int((~events["spot_listed_usdt"]).sum()),
            "symbols": events["symbol"].tolist(),
        },
        "data_quality": {
            "panel_rows": int(len(panel)),
            "duplicate_symbol_time_rows": int(panel.duplicated(["symbol", "open_time"]).sum()),
            "invalid_ohlc_rows": int(invalid_ohlc.sum()),
            "min_bars_per_symbol": int(panel.groupby("symbol").size().min()),
            "median_bars_per_symbol": float(panel.groupby("symbol").size().median()),
            "max_bars_per_symbol": int(panel.groupby("symbol").size().max()),
        },
        "errors": errors,
        "scope_diagnosis": {
            "prior_scope": "current Binance USDT spot pairs",
            "corrected_scope": "current TRADING Binance USD-M USDT perpetuals",
            "AKEUSDT_spot_listed": "AKEUSDT" in spot_symbols,
            "BTWUSDT_spot_listed": "BTWUSDT" in spot_symbols,
            "AKEUSDT_futures_listed": "AKEUSDT" in set(universe["symbol"]),
            "BTWUSDT_futures_listed": "BTWUSDT" in set(universe["symbol"]),
        },
    }

    universe.to_csv(HERE / "futures_universe.csv", index=False)
    panel.to_csv(HERE / "futures_4h_panel_30d.csv.gz", index=False, compression="gzip")
    events.to_csv(HERE / "futures_30d_200pct_events.csv", index=False)
    (HERE / "futures_scan_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
