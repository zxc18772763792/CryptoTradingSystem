"""Independent QA for the corrected Binance USDT-perpetual study."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent


def future_max(series: pd.Series) -> pd.Series:
    return series.iloc[::-1].cummax().iloc[::-1].shift(-1)


def best(rows: pd.DataFrame, base: str) -> tuple[float, pd.Timestamp, pd.Timestamp]:
    returns = future_max(rows.high) / rows[base] - 1.0
    entry = int(returns.idxmax())
    peak = int(rows.loc[rows.index > entry, "high"].idxmax())
    return float(returns.loc[entry]), rows.loc[entry, "open_time"], rows.loc[peak, "open_time"]


def main() -> None:
    panel = pd.read_csv(HERE / "futures_4h_panel.csv.gz", parse_dates=["open_time"])
    expected = pd.read_csv(
        HERE / "current_30d_runups.csv",
        parse_dates=["close_entry_time", "close_peak_time", "low_time", "low_peak_time"],
    )
    end_time = panel.open_time.max()
    start_time = end_time - pd.Timedelta(days=30)
    scan = panel[(panel.open_time >= start_time) & (panel.open_time <= end_time)].copy()
    records: list[dict[str, object]] = []
    for symbol, group in scan.groupby("symbol", sort=False):
        rows = group.sort_values("open_time").reset_index(drop=True)
        if len(rows) < 2:
            continue
        close_return, close_entry, close_peak = best(rows, "close")
        low_return, low_entry, low_peak = best(rows, "low")
        if close_return < 2.0 and low_return < 2.0:
            continue
        records.append(
            {
                "symbol": symbol,
                "classification": "close-confirmed" if close_return >= 2.0 else "wick-only",
                "close_to_later_high_return": close_return,
                "close_entry_time": close_entry,
                "close_peak_time": close_peak,
                "low_to_later_high_return": low_return,
                "low_time": low_entry,
                "low_peak_time": low_peak,
            }
        )
    actual = pd.DataFrame(records)
    merged = expected.merge(actual, on="symbol", suffixes=("_pipeline", "_verify"), validate="one_to_one")
    max_return_diff = float(
        np.maximum(
            (merged.close_to_later_high_return_pipeline - merged.close_to_later_high_return_verify).abs(),
            (merged.low_to_later_high_return_pipeline - merged.low_to_later_high_return_verify).abs(),
        ).max()
    )
    time_match = all(
        (merged[f"{field}_pipeline"] == merged[f"{field}_verify"]).all()
        for field in ["close_entry_time", "close_peak_time", "low_time", "low_peak_time"]
    )
    invalid = (
        (panel[["open", "high", "low", "close"]] <= 0).any(axis=1)
        | (panel.high < panel[["open", "low", "close"]].max(axis=1))
        | (panel.low > panel[["open", "high", "close"]].min(axis=1))
    )
    summary = {
        "definition": {
            "market": "current TRADING Binance USD-M USDT perpetuals",
            "window_start_utc": start_time.isoformat(),
            "window_end_utc": end_time.isoformat(),
            "path": "any earlier 4h close/low to strictly later 4h high",
            "threshold_return": 2.0,
            "threshold_price_multiple": 3.0,
        },
        "events": {
            "total": int(len(actual)),
            "close_confirmed": int((actual.classification == "close-confirmed").sum()),
            "wick_only": int((actual.classification == "wick-only").sum()),
            "AKEUSDT_present": "AKEUSDT" in set(actual.symbol),
            "BTWUSDT_present": "BTWUSDT" in set(actual.symbol),
        },
        "comparison": {
            "symbol_set_exact_match": set(expected.symbol) == set(actual.symbol),
            "classification_exact_match": bool(
                (merged.classification_pipeline == merged.classification_verify).all()
            ),
            "event_times_exact_match": bool(time_match),
            "maximum_absolute_return_difference": max_return_diff,
        },
        "data_quality": {
            "panel_rows": int(len(panel)),
            "panel_symbols": int(panel.symbol.nunique()),
            "window_rows": int(len(scan)),
            "duplicate_symbol_time_rows": int(panel.duplicated(["symbol", "open_time"]).sum()),
            "invalid_ohlc_rows": int(invalid.sum()),
        },
    }
    assert all(summary["comparison"][key] for key in [
        "symbol_set_exact_match", "classification_exact_match", "event_times_exact_match"
    ])
    assert max_return_diff < 1e-12
    assert summary["events"]["AKEUSDT_present"] and summary["events"]["BTWUSDT_present"]
    assert summary["data_quality"]["duplicate_symbol_time_rows"] == 0
    assert summary["data_quality"]["invalid_ohlc_rows"] == 0
    actual.to_csv(HERE / "independent_30d_200pct_events.csv", index=False)
    (HERE / "verification_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
