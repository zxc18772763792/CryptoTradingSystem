"""Reconstruct the historical unlock-overhang feature and join to the pump panel.

Unlock schedules are fully historical (DefiLlama datasets bucket carries every
past AND future timestamp), so for any panel date we can compute the forward
unlock overhang exactly as it was known then — no lookahead. This lets the
unlock feature be backtested NOW, unlike holder concentration which must
accumulate weekly snapshots.

Per (base, weekly date) it computes, from the cumulative vesting curve:
  unlock_next_7d_pct   = tokens unlocking in (t, t+7d]  * price_t / mcap_t
  unlock_next_30d_pct  = tokens unlocking in (t, t+30d] * price_t / mcap_t
  unlock_past_30d_pct  = tokens unlocked  in [t-30d, t) * price_t / mcap_t
  days_to_next_unlock  = days until the next positive increment (clipped 999)
  has_schedule         = 1 if the coin has a real vesting table else 0
Full-float coins (config/onchain_full_float_bases.json) are assigned 0 overhang
(legitimately no unlock pressure), has_schedule = 0.

Output: reports/ambush_modes_2026-07-18/unlock_feature_panel.parquet
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from loguru import logger

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

PANEL = PROJECT_ROOT / "reports" / "ambush_modes_2026-07-18" / "panel_ranked_cache.parquet"
OUT = PROJECT_ROOT / "reports" / "ambush_modes_2026-07-18" / "unlock_feature_panel.parquet"
CACHE_DIR = PROJECT_ROOT / "data" / "research" / "onchain" / "unlocks_cache"
LLAMA_BUCKET = "https://defillama-datasets.llama.fi/emissions"

SESSION = requests.Session()
SESSION.headers["User-Agent"] = "crypto-trading-system-onchain/1.0"


def load_schedule(slug: str) -> Optional[List[Tuple[int, float]]]:
    """Return cumulative (timestamp, unlocked_tokens) points, ascending."""
    cache = CACHE_DIR / f"{slug}.json"
    payload = None
    if cache.exists():
        try:
            payload = json.loads(cache.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            payload = None
    if payload is None:
        try:
            resp = SESSION.get(f"{LLAMA_BUCKET}/{slug}", timeout=20)
            if resp.status_code != 200 or not resp.text.startswith("{"):
                return None
            payload = resp.json()
        except Exception:  # noqa: BLE001
            return None
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(payload), encoding="utf-8")
        time.sleep(0.3)

    series: Dict[int, float] = {}
    for section in ("documentedData", "realTimeData"):
        data = (payload.get(section) or {}).get("data") or []
        for cat in data:
            for point in cat.get("data") or []:
                ts = int(point.get("timestamp") or 0)
                unlocked = float(point.get("unlocked") or 0.0)
                if ts > 0:
                    series[ts] = series.get(ts, 0.0) + unlocked
        if series:
            break
    if not series:
        return None
    return sorted(series.items())


def increments_from_cumulative(points: List[Tuple[int, float]]) -> List[Tuple[int, float]]:
    incs: List[Tuple[int, float]] = []
    prev = None
    for ts, cum in points:
        if prev is not None and cum > prev:
            incs.append((ts, cum - prev))
        prev = cum if (prev is None or cum > prev) else prev
    return incs


def features_at(incs: List[Tuple[int, float]], at_ts: float) -> Dict[str, float]:
    next_7d = sum(a for ts, a in incs if at_ts < ts <= at_ts + 7 * 86400)
    next_30d = sum(a for ts, a in incs if at_ts < ts <= at_ts + 30 * 86400)
    past_30d = sum(a for ts, a in incs if at_ts - 30 * 86400 <= ts < at_ts)
    future = [ts for ts, a in incs if ts > at_ts and a > 0]
    days_next = min((min(future) - at_ts) / 86400.0, 999.0) if future else 999.0
    return {
        "unlock_next_7d_tokens": next_7d,
        "unlock_next_30d_tokens": next_30d,
        "unlock_past_30d_tokens": past_30d,
        "days_to_next_unlock": round(days_next, 2),
    }


def main() -> None:
    panel = pd.read_parquet(PANEL)
    panel["date"] = pd.to_datetime(panel["date"])
    slugs: Dict[str, str] = json.loads((PROJECT_ROOT / "config" / "onchain_unlock_slugs.json").read_text(encoding="utf-8"))
    full_float = set(json.loads((PROJECT_ROOT / "config" / "onchain_full_float_bases.json").read_text(encoding="utf-8")))

    schedules: Dict[str, List[Tuple[int, float]]] = {}
    for base in panel["base"].unique():
        slug = slugs.get(base)
        if not slug:
            continue
        pts = load_schedule(slug)
        if pts:
            schedules[base] = increments_from_cumulative(pts)
    logger.info(f"loaded {len(schedules)} schedules for {panel['base'].nunique()} panel bases")

    rows = []
    for _, r in panel.iterrows():
        base = r["base"]
        at_ts = pd.Timestamp(r["date"]).timestamp()
        price = float(r["close"]) if pd.notna(r["close"]) else np.nan
        mcap = float(r["mcap"]) if pd.notna(r["mcap"]) else np.nan
        if base in schedules:
            f = features_at(schedules[base], at_ts)
            has_sched = 1
        elif base in full_float:
            f = {"unlock_next_7d_tokens": 0.0, "unlock_next_30d_tokens": 0.0, "unlock_past_30d_tokens": 0.0, "days_to_next_unlock": 999.0}
            has_sched = 0
        else:
            f = None
            has_sched = -1  # unknown (true gap) — excluded from unlock analysis
        out = {"base": base, "date": r["date"], "pump100": r.get("pump100"), "pump300": r.get("pump300"),
               "fwd30_maxret": r.get("fwd30_maxret"), "mcap": mcap, "close": price, "has_schedule": has_sched}
        if f is not None and mcap and mcap > 0 and price and price > 0:
            out["unlock_next_7d_pct"] = round(f["unlock_next_7d_tokens"] * price / mcap, 6)
            out["unlock_next_30d_pct"] = round(f["unlock_next_30d_tokens"] * price / mcap, 6)
            out["unlock_past_30d_pct"] = round(f["unlock_past_30d_tokens"] * price / mcap, 6)
            out["days_to_next_unlock"] = f["days_to_next_unlock"]
        else:
            out["unlock_next_7d_pct"] = np.nan
            out["unlock_next_30d_pct"] = np.nan
            out["unlock_past_30d_pct"] = np.nan
            out["days_to_next_unlock"] = np.nan
        rows.append(out)

    df = pd.DataFrame(rows)
    df.to_parquet(OUT)
    analyzable = df[df["has_schedule"] >= 0]
    with_sched = df[df["has_schedule"] == 1]
    logger.info(f"panel rows: {len(df)}, analyzable (sched or full-float): {len(analyzable)}, with real schedule: {len(with_sched)}")
    logger.info(f"unlock_next_30d_pct describe (schedule coins):\n{with_sched['unlock_next_30d_pct'].describe()}")
    print(f"WROTE {OUT}")


if __name__ == "__main__":
    main()
