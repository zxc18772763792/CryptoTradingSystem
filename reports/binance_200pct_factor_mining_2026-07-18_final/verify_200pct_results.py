"""Independent QA for the current 30-day +200% event scan.

This verifier deliberately does not import the production scan function.  It
computes, for every 4h bar, the maximum high available strictly later in the
same 30-day window by reverse cumulative maxima, then compares the best path
per symbol with the saved production result.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent
TARGET_RETURN = 2.0
WINDOW_DAYS = 30


def later_maximum(values: pd.Series) -> pd.Series:
    """Maximum value at a strictly later row."""
    return values.iloc[::-1].cummax().iloc[::-1].shift(-1)


def best_path(rows: pd.DataFrame, base_column: str) -> dict[str, object]:
    future_high = later_maximum(rows["high"])
    returns = future_high / rows[base_column] - 1.0
    entry_index = int(returns.idxmax())
    entry_time = rows.loc[entry_index, "open_time"]
    entry_value = float(rows.loc[entry_index, base_column])
    later = rows.loc[(rows.index > entry_index)]
    peak_index = int(later["high"].idxmax())
    return {
        "return": float(returns.loc[entry_index]),
        "entry_time": entry_time,
        "entry_value": entry_value,
        "peak_time": rows.loc[peak_index, "open_time"],
        "peak_high": float(rows.loc[peak_index, "high"]),
    }


def main() -> None:
    panel = pd.read_csv(HERE / "spot_4h_panel.csv.gz", parse_dates=["open_time"])
    universe = pd.read_csv(HERE / "spot_universe.csv")
    expected = pd.read_csv(
        HERE / "current_30d_runups.csv",
        parse_dates=["close_entry_time", "close_peak_time", "low_time", "low_peak_time"],
    )

    end_time = panel["open_time"].max()
    start_time = end_time - pd.Timedelta(days=WINDOW_DAYS)
    scan = panel.loc[
        (panel["open_time"] >= start_time) & (panel["open_time"] <= end_time)
    ].copy()

    records: list[dict[str, object]] = []
    for symbol, raw_rows in scan.groupby("symbol", sort=False):
        rows = raw_rows.sort_values("open_time").reset_index(drop=True)
        if len(rows) < 2:
            continue
        close_path = best_path(rows, "close")
        low_path = best_path(rows, "low")
        if close_path["return"] < TARGET_RETURN and low_path["return"] < TARGET_RETURN:
            continue
        first_last_return = float(rows.iloc[-1]["close"] / rows.iloc[0]["close"] - 1.0)
        records.append(
            {
                "symbol": symbol,
                "classification": (
                    "close-confirmed"
                    if close_path["return"] >= TARGET_RETURN
                    else "wick-only"
                ),
                "close_to_later_high_return": close_path["return"],
                "close_entry_time": close_path["entry_time"],
                "entry_close": close_path["entry_value"],
                "close_peak_time": close_path["peak_time"],
                "peak_high": close_path["peak_high"],
                "low_to_later_high_return": low_path["return"],
                "low_time": low_path["entry_time"],
                "entry_low": low_path["entry_value"],
                "low_peak_time": low_path["peak_time"],
                "first_to_last_close_return": first_last_return,
            }
        )

    actual = pd.DataFrame(records).sort_values(
        ["classification", "close_to_later_high_return"],
        ascending=[True, False],
    ).reset_index(drop=True)

    expected_symbols = set(expected["symbol"])
    actual_symbols = set(actual["symbol"])
    symbol_match = expected_symbols == actual_symbols
    classification_match = (
        expected.set_index("symbol")["classification"].sort_index()
        == actual.set_index("symbol")["classification"].sort_index()
    ).all()

    merged = expected.merge(
        actual,
        on="symbol",
        suffixes=("_production", "_independent"),
        validate="one_to_one",
    )
    return_diff = np.maximum(
        (
            merged["close_to_later_high_return_production"]
            - merged["close_to_later_high_return_independent"]
        ).abs(),
        (
            merged["low_to_later_high_return_production"]
            - merged["low_to_later_high_return_independent"]
        ).abs(),
    )
    time_match = (
        (
            merged["close_entry_time_production"]
            == merged["close_entry_time_independent"]
        )
        & (merged["close_peak_time_production"] == merged["close_peak_time_independent"])
        & (merged["low_time_production"] == merged["low_time_independent"])
        & (merged["low_peak_time_production"] == merged["low_peak_time_independent"])
    ).all()

    invalid_ohlc = (
        (panel[["open", "high", "low", "close"]] <= 0).any(axis=1)
        | (panel["high"] < panel[["open", "low", "close"]].max(axis=1))
        | (panel["low"] > panel[["open", "high", "close"]].min(axis=1))
    )
    duplicate_bars = int(panel.duplicated(["symbol", "open_time"]).sum())
    unordered_symbols = int(
        sum(
            not group["open_time"].is_monotonic_increasing
            for _, group in panel.groupby("symbol", sort=False)
        )
    )
    altcoin_symbols = set(universe.loc[universe["is_altcoin"], "symbol"])
    non_altcoin_events = sorted(actual_symbols - altcoin_symbols)

    summary = {
        "definition": {
            "window_days": WINDOW_DAYS,
            "window_start_utc": start_time.isoformat(),
            "window_end_utc": end_time.isoformat(),
            "threshold_return": TARGET_RETURN,
            "threshold_price_multiple": 1.0 + TARGET_RETURN,
            "path": "any earlier 4h close/low to a strictly later 4h high",
            "not_used": "first-window-close versus last-window-close",
        },
        "independent_events": {
            "total": int(len(actual)),
            "close_confirmed": int((actual["classification"] == "close-confirmed").sum()),
            "wick_only": int((actual["classification"] == "wick-only").sum()),
            "symbols": actual["symbol"].tolist(),
        },
        "production_comparison": {
            "symbol_set_exact_match": bool(symbol_match),
            "classification_exact_match": bool(classification_match),
            "event_times_exact_match": bool(time_match),
            "maximum_absolute_return_difference": float(return_diff.max()),
        },
        "data_quality": {
            "panel_rows": int(len(panel)),
            "panel_symbols": int(panel["symbol"].nunique()),
            "window_rows": int(len(scan)),
            "duplicate_symbol_time_rows": duplicate_bars,
            "invalid_ohlc_rows": int(invalid_ohlc.sum()),
            "unordered_symbols": unordered_symbols,
            "event_symbols_outside_altcoin_universe": non_altcoin_events,
        },
    }

    assert symbol_match, (expected_symbols, actual_symbols)
    assert classification_match
    assert time_match
    assert float(return_diff.max()) < 1e-12
    assert duplicate_bars == 0
    assert int(invalid_ohlc.sum()) == 0
    assert unordered_symbols == 0
    assert not non_altcoin_events

    actual.to_csv(HERE / "independent_30d_200pct_events.csv", index=False)
    (HERE / "verification_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
