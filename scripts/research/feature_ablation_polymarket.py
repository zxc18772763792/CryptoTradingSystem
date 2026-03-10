from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


async def _main(symbol: str, days: int, timeframes: list[str], out_dir: Path) -> None:
    await pm_db.init_pm_db()
    rows = []
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        for tf in timeframes:
            klines = await data_storage.load_klines_from_parquet("binance", _normalize_symbol(symbol), tf, start, end)
            features = await pm_db.get_features_range(symbol=symbol, since=start, until=end, timeframe=tf)
            if klines.empty:
                continue
            klines = klines.copy()
            klines.index = pd.to_datetime(klines.index)
            klines = klines.sort_index()
            klines["ret"] = klines["close"].pct_change().shift(-1)
            feat_df = pd.DataFrame(features)
            if feat_df.empty:
                rows.append({"timeframe": tf, "variant": "baseline", "avg_ret": float(klines["ret"].dropna().mean()), "sample": int(len(klines))})
                rows.append({"timeframe": tf, "variant": "pm_enhanced", "avg_ret": float(klines["ret"].dropna().mean()), "sample": int(len(klines)), "note": "no pm features"})
                continue
            feat_df["ts"] = pd.to_datetime(feat_df["ts"])
            merged = pd.merge_asof(klines.reset_index().rename(columns={"index": "ts"}).sort_values("ts"), feat_df.sort_values("ts"), on="ts", direction="backward")
            baseline = merged["ret"].dropna()
            enhanced = (merged["ret"] * (1 - merged["pm_global_risk"].fillna(0.0).clip(0, 1) * 0.1) + merged["pm_price_signal"].fillna(0.0) * 0.001).dropna()
            rows.append({"timeframe": tf, "variant": "baseline", "avg_ret": float(baseline.mean()), "sample": int(len(baseline))})
            rows.append({"timeframe": tf, "variant": "pm_enhanced", "avg_ret": float(enhanced.mean()), "sample": int(len(enhanced))})
        df = pd.DataFrame(rows)
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"feature_ablation_polymarket_{symbol.replace('/', '_')}.csv"
        json_path = out_dir / f"feature_ablation_polymarket_{symbol.replace('/', '_')}.json"
        md_path = out_dir / f"feature_ablation_polymarket_{symbol.replace('/', '_')}.md"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        summary = {"rows": rows, "best_variant": df.sort_values("avg_ret", ascending=False).head(1).to_dict(orient="records")}
        json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text("# Polymarket Feature Ablation\n\n" + df.to_markdown(index=False), encoding="utf-8")
        print(json.dumps({"csv_path": str(csv_path), "json_path": str(json_path), "markdown_path": str(md_path)}, ensure_ascii=False, indent=2))
    finally:
        await pm_db.close_pm_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket feature ablation")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--timeframes", default="1m,5m")
    parser.add_argument("--output-dir", default="data/reports")
    args = parser.parse_args()
    timeframes = [x.strip() for x in str(args.timeframes or "1m,5m").split(",") if x.strip()]
    asyncio.run(_main(args.symbol.upper(), args.days, timeframes, Path(args.output_dir)))
