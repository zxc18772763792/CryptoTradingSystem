from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from core.data import data_storage
from prediction_markets.polymarket import db as pm_db


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if "/" in raw:
        return raw
    if raw.endswith("USDT"):
        return f"{raw[:-4]}/USDT"
    return raw


async def _load_prices(exchange: str, symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=max(1, int(days)))
    df = await data_storage.load_klines_from_parquet(exchange=exchange, symbol=_normalize_symbol(symbol), timeframe=timeframe, start_time=start, end_time=end)
    if df.empty:
        return df
    out = df.copy()
    out.index = pd.to_datetime(out.index)
    out = out.sort_index()
    out["ret_1"] = out["close"].pct_change().shift(-1)
    out["ret_3"] = out["close"].pct_change(3).shift(-3)
    out["ret_6"] = out["close"].pct_change(6).shift(-6)
    out["ret_12"] = out["close"].pct_change(12).shift(-12)
    return out


async def _main(symbol: str, days: int, timeframe: str, out_dir: Path) -> None:
    await pm_db.init_pm_db()
    try:
        prices = await _load_prices("binance", symbol, timeframe, days)
        features = await pm_db.get_features_range(symbol=symbol, since=datetime.now(timezone.utc) - timedelta(days=days), until=datetime.now(timezone.utc), timeframe=timeframe)
        feat_df = pd.DataFrame(features)
        if feat_df.empty or prices.empty:
            raise RuntimeError("insufficient data for event study")
        feat_df["ts"] = pd.to_datetime(feat_df["ts"])
        feat_df = feat_df.sort_values("ts")
        prices = prices.reset_index().rename(columns={"index": "ts"}).sort_values("ts")
        merged = pd.merge_asof(feat_df, prices[["ts", "close", "ret_1", "ret_3", "ret_6", "ret_12"]], on="ts", direction="backward")
        events = merged[merged["pm_global_risk"] >= 0.6].copy()
        summary: Dict[str, float] = {"event_count": int(len(events))}
        for horizon in [1, 3, 6, 12]:
            col = f"ret_{horizon}"
            vals = events[col].dropna()
            if vals.empty:
                summary[f"avg_forward_return_{horizon}"] = 0.0
                summary[f"hit_rate_{horizon}"] = 0.0
                continue
            summary[f"avg_forward_return_{horizon}"] = float(vals.mean())
            summary[f"median_forward_return_{horizon}"] = float(vals.median())
            summary[f"p10_forward_return_{horizon}"] = float(vals.quantile(0.1))
            summary[f"p90_forward_return_{horizon}"] = float(vals.quantile(0.9))
            summary[f"hit_rate_{horizon}"] = float((vals > 0).mean())
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / f"event_study_polymarket_{symbol.replace('/', '_')}_{timeframe}.json"
        md_path = out_dir / f"event_study_polymarket_{symbol.replace('/', '_')}_{timeframe}.md"
        json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        md_lines = ["# Polymarket Event Study", "", f"- symbol: `{symbol}`", f"- timeframe: `{timeframe}`", f"- event_count: `{summary['event_count']}`", ""]
        for horizon in [1, 3, 6, 12]:
            md_lines.append(f"## t+{horizon} bars")
            md_lines.append(f"- avg_forward_return: `{summary.get(f'avg_forward_return_{horizon}', 0.0):.6f}`")
            md_lines.append(f"- median_forward_return: `{summary.get(f'median_forward_return_{horizon}', 0.0):.6f}`")
            md_lines.append(f"- hit_rate: `{summary.get(f'hit_rate_{horizon}', 0.0):.4f}`")
            md_lines.append("")
        md_path.write_text("\n".join(md_lines), encoding="utf-8")
        print(json.dumps({"json_path": str(json_path), "markdown_path": str(md_path), "summary": summary}, ensure_ascii=False, indent=2))
    finally:
        await pm_db.close_pm_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket event study")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--output-dir", default="data/reports")
    args = parser.parse_args()
    asyncio.run(_main(args.symbol.upper(), args.days, args.timeframe, Path(args.output_dir)))
