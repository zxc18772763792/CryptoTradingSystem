from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from _common import add_common_args, ensure_output_dir, load_df


def main() -> None:
    parser = add_common_args(argparse.ArgumentParser(description="5m data QA report"))
    args = parser.parse_args()
    out_dir = ensure_output_dir(args.output_dir, "hf_research")

    df = load_df(args.exchange, args.symbol, args.timeframe, args.days)
    freq = pd.infer_freq(df.index[: min(len(df), 500)]) or "5min"
    expected = pd.date_range(df.index.min(), df.index.max(), freq=freq)
    missing = expected.difference(df.index)

    close = pd.to_numeric(df["close"], errors="coerce")
    ret = close.pct_change()
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    volume = pd.to_numeric(df.get("volume", 0), errors="coerce")

    qa_rows = [
        {"metric": "rows", "value": int(len(df))},
        {"metric": "start", "value": str(df.index.min())},
        {"metric": "end", "value": str(df.index.max())},
        {"metric": "inferred_freq", "value": str(freq)},
        {"metric": "missing_bars", "value": int(len(missing))},
        {"metric": "duplicate_index", "value": int(df.index.duplicated().sum())},
        {"metric": "close_nan", "value": int(close.isna().sum())},
        {"metric": "return_outlier_gt_5pct", "value": int((ret.abs() > 0.05).sum())},
        {"metric": "high_lt_low", "value": int((high < low).sum())},
        {"metric": "nonpositive_price", "value": int((close <= 0).sum())},
        {"metric": "zero_volume", "value": int((volume <= 0).sum())},
    ]

    report_df = pd.DataFrame(qa_rows)
    report_df.to_csv(out_dir / "data_quality_report.csv", index=False, encoding="utf-8-sig")

    if len(missing) > 0:
        pd.DataFrame({"missing_timestamp": missing.astype(str)}).to_csv(
            out_dir / "missing_bars.csv", index=False, encoding="utf-8-sig"
        )

    print(f"[data_qa] rows={len(df)} missing={len(missing)} out={out_dir}")


if __name__ == "__main__":
    main()
