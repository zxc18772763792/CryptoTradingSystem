"""Independent reverse-cumulative-high verification of the futures scan."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent
TARGET_RETURN = 2.0


def future_max(values: pd.Series) -> pd.Series:
    return values.iloc[::-1].cummax().iloc[::-1].shift(-1)


def path(rows: pd.DataFrame, base: str) -> dict[str, object]:
    returns = future_max(rows["high"]) / rows[base] - 1.0
    entry_index = int(returns.idxmax())
    peak_index = int(rows.loc[rows.index > entry_index, "high"].idxmax())
    return {
        "return": float(returns.loc[entry_index]),
        "entry_time": rows.loc[entry_index, "open_time"],
        "peak_time": rows.loc[peak_index, "open_time"],
    }


def main() -> None:
    panel = pd.read_csv(HERE / "futures_4h_panel_30d.csv.gz", parse_dates=["open_time"])
    expected = pd.read_csv(
        HERE / "futures_30d_200pct_events.csv",
        parse_dates=["close_entry_time", "close_peak_time", "low_entry_time", "low_peak_time"],
    )
    rows_out: list[dict[str, object]] = []
    for symbol, group in panel.groupby("symbol", sort=False):
        rows = group.sort_values("open_time").reset_index(drop=True)
        if len(rows) < 2:
            continue
        close_path = path(rows, "close")
        low_path = path(rows, "low")
        if close_path["return"] < TARGET_RETURN and low_path["return"] < TARGET_RETURN:
            continue
        rows_out.append(
            {
                "symbol": symbol,
                "classification": (
                    "close-confirmed"
                    if close_path["return"] >= TARGET_RETURN
                    else "wick-only"
                ),
                "close_to_later_high_return": close_path["return"],
                "close_entry_time": close_path["entry_time"],
                "close_peak_time": close_path["peak_time"],
                "low_to_later_high_return": low_path["return"],
                "low_entry_time": low_path["entry_time"],
                "low_peak_time": low_path["peak_time"],
            }
        )
    actual = pd.DataFrame(rows_out)
    merged = expected.merge(actual, on="symbol", suffixes=("_scan", "_verify"), validate="one_to_one")
    symbol_match = set(expected.symbol) == set(actual.symbol)
    class_match = (
        merged["classification_scan"] == merged["classification_verify"]
    ).all()
    time_match = all(
        (
            merged[f"{field}_scan"] == merged[f"{field}_verify"]
        ).all()
        for field in ["close_entry_time", "close_peak_time", "low_entry_time", "low_peak_time"]
    )
    max_diff = float(
        np.maximum(
            (
                merged["close_to_later_high_return_scan"]
                - merged["close_to_later_high_return_verify"]
            ).abs(),
            (
                merged["low_to_later_high_return_scan"]
                - merged["low_to_later_high_return_verify"]
            ).abs(),
        ).max()
    )
    summary = {
        "event_count": int(len(actual)),
        "close_confirmed": int((actual.classification == "close-confirmed").sum()),
        "wick_only": int((actual.classification == "wick-only").sum()),
        "symbol_set_exact_match": bool(symbol_match),
        "classification_exact_match": bool(class_match),
        "event_times_exact_match": bool(time_match),
        "maximum_absolute_return_difference": max_diff,
        "required_examples": {
            "AKEUSDT_present": "AKEUSDT" in set(actual.symbol),
            "BTWUSDT_present": "BTWUSDT" in set(actual.symbol),
        },
    }
    assert symbol_match and class_match and time_match
    assert max_diff < 1e-12
    assert summary["required_examples"]["AKEUSDT_present"]
    assert summary["required_examples"]["BTWUSDT_present"]
    actual.to_csv(HERE / "independent_futures_30d_200pct_events.csv", index=False)
    (HERE / "futures_verification_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
