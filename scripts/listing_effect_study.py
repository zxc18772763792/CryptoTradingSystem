"""New perpetual listing effect study: what do the first 30 days look like?

Precedent: BSB +49.6% on its 2026-06 perp listing day. Tests, on every symbol
whose Binance perp onboardDate falls inside our 1y kline window:
  - the median/quartile forward path from the first daily close (day 0)
  - simple holds: buy day-0 close -> exit day 3/7/14/30
  - the fade trade: SHORT day-2 close -> cover day 14/30 (squeeze-stopped +25%)

Output: reports/ambush_modes_2026-07-18/listing_effect_study.{json,md-ish stdout}
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util

spec = importlib.util.spec_from_file_location("bt", SCRIPT_DIR / "backtest_ambush_modes.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

OUT_DIR = PROJECT_ROOT / "reports" / "ambush_modes_2026-07-18"
OOS_SPLIT = pd.Timestamp("2026-03-18")
FEE, SLIP = 0.0005, 0.0010
SQUEEZE_STOP = 0.25


def main() -> None:
    manifest = json.loads((bt.DATA_DIR / "manifest.json").read_text(encoding="utf-8"))
    rows = manifest.get("symbols") or {}

    paths: List[Dict[str, Any]] = []
    for base, meta in rows.items():
        onboard_ms = int(meta.get("onboard") or 0)
        if onboard_ms <= 0:
            continue
        onboard = pd.Timestamp(onboard_ms, unit="ms")
        kl_path = bt.DATA_DIR / "klines_1h" / f"{base}.parquet"
        if not kl_path.exists():
            continue
        kl = pd.read_parquet(kl_path)
        daily = pd.to_numeric(kl["close"], errors="coerce").resample("1D").last().dropna()
        if daily.empty:
            continue
        # require the listing to be inside our window (first bar within 2d of onboard)
        if abs((daily.index[0] - onboard.normalize()).days) > 2:
            continue
        if len(daily) < 8:
            continue
        base_close = float(daily.iloc[0])
        if base_close <= 0:
            continue
        rel = (daily / base_close - 1.0).iloc[:31]
        funding_path = bt.DATA_DIR / "funding" / f"{base}.parquet"
        funding = None
        if funding_path.exists():
            raw = pd.read_parquet(funding_path)
            funding = pd.to_numeric(raw["funding_rate"], errors="coerce").dropna()
        paths.append(
            {
                "base": base,
                "onboard": str(onboard.date()),
                "is_oos": bool(onboard >= OOS_SPLIT),
                "rel_path": [round(float(v), 4) for v in rel.tolist()],
                "dates": [str(ts.date()) for ts in rel.index],
                "_funding": funding,
            }
        )
    logger.info(f"listings inside window: {len(paths)}")

    horizon_stats: Dict[str, Any] = {}
    for day in (1, 2, 3, 7, 14, 30):
        vals = [p["rel_path"][day] for p in paths if len(p["rel_path"]) > day]
        if not vals:
            continue
        arr = np.array(vals)
        horizon_stats[f"day_{day}"] = {
            "n": int(len(arr)),
            "mean": round(float(arr.mean()), 4),
            "median": round(float(np.median(arr)), 4),
            "q25": round(float(np.percentile(arr, 25)), 4),
            "q75": round(float(np.percentile(arr, 75)), 4),
            "pct_positive": round(float((arr > 0).mean()), 3),
        }

    def hold_trade(p: Dict[str, Any], entry_day: int, exit_day: int, short: bool) -> Optional[float]:
        rel = p["rel_path"]
        if len(rel) <= exit_day:
            return None
        entry = 1.0 + rel[entry_day]
        exit_ = 1.0 + rel[exit_day]
        if entry <= 0:
            return None
        if short:
            # squeeze stop on the path between entry and exit
            stop_day = None
            seg = rel[entry_day + 1 : exit_day + 1]
            for offset, v in enumerate(seg, start=entry_day + 1):
                if (1.0 + v) / entry - 1.0 >= SQUEEZE_STOP:
                    stop_day = offset
                    break
            settle_day = stop_day if stop_day is not None else exit_day
            # funding on shorts: receive positive rates, PAY negative ones —
            # freshly listed perps are short-crowded and deeply negative early.
            funding_ret = 0.0
            funding = p.get("_funding")
            dates = p.get("dates") or []
            if funding is not None and len(dates) > settle_day:
                start_ts = pd.Timestamp(dates[entry_day]) + pd.Timedelta(days=1)
                end_ts = pd.Timestamp(dates[settle_day]) + pd.Timedelta(days=1)
                window = funding[(funding.index > start_ts) & (funding.index <= end_ts)]
                funding_ret = float(window.sum())
            if stop_day is not None:
                return -SQUEEZE_STOP + funding_ret - 2 * (FEE + SLIP)
            ret = (entry - exit_) / entry
            return ret + funding_ret - 2 * (FEE + SLIP)
        ret = exit_ / entry - 1.0
        return ret - 2 * (FEE + SLIP)

    trades: Dict[str, Any] = {}
    for label, entry_day, exit_day, short in (
        ("long_d0_d3", 0, 3, False),
        ("long_d0_d7", 0, 7, False),
        ("long_d0_d30", 0, 30, False),
        ("short_d2_d14", 2, 14, True),
        ("short_d2_d30", 2, 30, True),
    ):
        rets_all, rets_is, rets_oos = [], [], []
        for p in paths:
            ret = hold_trade(p, entry_day, exit_day, short)
            if ret is None:
                continue
            rets_all.append(ret)
            (rets_oos if p["is_oos"] else rets_is).append(ret)

        def stat(vals: List[float]) -> Dict[str, Any]:
            if not vals:
                return {"n": 0}
            arr = np.array(vals)
            return {
                "n": int(len(arr)),
                "win": round(float((arr > 0).mean()), 3),
                "avg": round(float(arr.mean()), 4),
                "median": round(float(np.median(arr)), 4),
                "best": round(float(arr.max()), 4),
                "worst": round(float(arr.min()), 4),
            }

        trades[label] = {"full": stat(rets_all), "is": stat(rets_is), "oos": stat(rets_oos)}

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "listings": len(paths),
        "horizon_stats": horizon_stats,
        "trades": trades,
    }
    serializable_paths = [{k: v for k, v in p.items() if not k.startswith("_")} for p in paths]
    (OUT_DIR / "listing_effect_study.json").write_text(
        json.dumps({**payload, "paths": serializable_paths}, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
