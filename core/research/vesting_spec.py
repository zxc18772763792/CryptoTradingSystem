"""Turn an LLM-extracted vesting spec into a cumulative unlocked-supply curve.

The spec is what scripts/vesting_llm_extract.py asks the research model for:
allocations with a share of total supply, the part unlocked at start, a cliff,
a vesting period and a release frequency. Allocations whose release is
discretionary (``schedule_known: false``) never count as unlocked, which is
how DefiLlama treats treasury / "noncirculating" sections. Curves are in
fractions of total supply; only ratios (growth) are used downstream.
See docs/LLM_TRADING_RESEARCH_ROUND6_2026-09-27.md.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

MONTH_DAYS = 30.4375
MIN_CONFIDENCE = 0.5
MIN_KNOWN_SHARE = 0.3


def schedule_from_spec(spec: Dict[str, Any], fallback_start: Optional[str], days: pd.DatetimeIndex) -> Tuple[pd.Series, float]:
    """(unlocked fraction per day, share of supply with a known schedule)."""
    total = np.zeros(len(days))
    tge = spec.get("tge_date") or fallback_start
    known = 0.0
    for alloc in spec.get("allocations") or []:
        try:
            share = float(alloc.get("pct_of_total") or 0) / 100
            if share <= 0 or not alloc.get("schedule_known", True):
                continue
            start = pd.Timestamp(alloc.get("start_date") or tge, tz="UTC").normalize()
            at_start = share * min(max(float(alloc.get("tge_unlock_pct") or 0), 0.0), 100.0) / 100
            rest = share - at_start
            cliff_end = start + pd.Timedelta(days=float(alloc.get("cliff_months") or 0) * MONTH_DAYS)
            months = float(alloc.get("vesting_months") or 0)
            freq = alloc.get("frequency") or "monthly"
        except (TypeError, ValueError):
            continue
        known += share
        curve = np.zeros(len(days))
        curve[days >= start] += at_start
        if months <= 0 or freq == "once":
            curve[days >= cliff_end] += rest
        elif freq == "daily":
            elapsed = ((days - cliff_end).days / (months * MONTH_DAYS)).to_numpy(dtype=float)
            curve += rest * np.clip(elapsed, 0, 1)
        else:
            step = 3 if freq == "quarterly" else 1
            n_steps = max(int(round(months / step)), 1)
            for k in range(1, n_steps + 1):
                curve[days >= cliff_end + pd.Timedelta(days=k * step * MONTH_DAYS)] += rest / n_steps
        total += curve
    return pd.Series(total, index=days), known


def usable(spec: Dict[str, Any], known_share: float) -> bool:
    """The gate used before an LLM schedule may enter the factor."""
    try:
        confidence = float(spec.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    return "error" not in spec and confidence >= MIN_CONFIDENCE and known_share >= MIN_KNOWN_SHARE


def growth(curve: pd.Series, day: pd.Timestamp, horizon_days: int = 90) -> float:
    end = day + pd.Timedelta(days=horizon_days)
    if day not in curve.index or end not in curve.index or curve[day] <= 0:
        return float("nan")
    return float(curve[end] / curve[day] - 1)
