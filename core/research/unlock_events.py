"""DefiLlama cliff-unlock parsing shared by the unlock study and the paper tracker.

A cliff = a one-off block in `unlockEvents[].cliffAllocations` (linear vesting
excluded). Its size is measured against tokens already unlocked just before
(a circulating-supply proxy). See docs/UNLOCK_EVENT_STUDY_2026-09-27.md.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

LLAMA_DATASETS = "https://defillama-datasets.llama.fi"
INSIDER_CATEGORIES = {"insiders", "privateSale"}


def unlocked_series(schedule: Dict[str, Any]) -> pd.Series:
    """Cumulative unlocked tokens per day, summed across allocation categories."""
    parts = []
    for cat in (schedule.get("documentedData") or {}).get("data") or []:
        pts = cat.get("data") or []
        if pts:
            parts.append(pd.Series({pd.Timestamp(p["timestamp"], unit="s", tz="UTC").normalize(): float(p.get("unlocked") or 0) for p in pts}))
    if not parts:
        return pd.Series(dtype=float)
    return pd.concat(parts, axis=1).sort_index().ffill().fillna(0).sum(axis=1)


def cliff_events(entry: Dict[str, Any], unlocked: pd.Series, min_pct: float, merge_days: int = 30) -> List[Dict[str, Any]]:
    """Cliffs >= min_pct of unlocked supply; within merge_days only the largest is kept."""
    out = []
    for ev in entry.get("unlockEvents") or []:
        allocs = ev.get("cliffAllocations") or []
        if not allocs:
            continue
        t = pd.Timestamp(ev["timestamp"], unit="s", tz="UTC").normalize()
        amount = sum(float(a.get("amount") or 0) for a in allocs)
        before = unlocked[unlocked.index < t]
        base = float(before.iloc[-1]) if len(before) else 0.0
        if amount <= 0 or base <= 0:
            continue
        insider = sum(float(a.get("amount") or 0) for a in allocs if a.get("category") in INSIDER_CATEGORIES)
        out.append({"date": t, "size_pct": amount / base * 100, "insider": insider / amount >= 0.5})
    events = sorted([e for e in out if e["size_pct"] >= min_pct], key=lambda e: e["date"])
    kept: List[Dict[str, Any]] = []
    for e in events:
        if kept and (e["date"] - kept[-1]["date"]).days < merge_days:
            if e["size_pct"] > kept[-1]["size_pct"]:
                kept[-1] = e
            continue
        kept.append(e)
    return kept


def entry_ticker(entry: Dict[str, Any]) -> str:
    prices = entry.get("tokenPrice") or []
    return str((prices[0] if prices else {}).get("symbol") or "").upper()


def entry_price(entry: Dict[str, Any]) -> float:
    prices = entry.get("tokenPrice") or []
    try:
        return float((prices[0] if prices else {}).get("price") or 0)
    except (TypeError, ValueError):
        return 0.0
