"""Pre-registered retirement rules for the forward paper trackers.

Written 2026-09-27, before any tracker had a completed forward trade, so the
rule cannot be fitted to the outcome. Each tracker stores the rule in its own
state the first time it runs (``state["retirement_rule"]``) and is judged by
that stored copy: editing the constants here later does not silently move the
goalposts for a tracker that is already running.

Verdicts, from completed forward returns (percent per trade or per month):
  collecting  fewer than max(3, min_n // 2) results
  retire      the 90% upper bound is below zero (clearly losing), or, once
              min_n results exist, the mean is <= 0 or the upper bound is
              below a quarter of the backtest mean (the edge has decayed)
  watch       before min_n, the mean is negative
  confirmed   min_n reached and the 90% lower bound is above zero
  on_track    min_n reached, mean positive, not yet significant
Retiring only labels the tracker and alerts once; recording continues so the
decision can be audited. Nothing here trades.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional, Sequence

Z90 = 1.645
DECAY_FRACTION = 0.25
RULES: Dict[str, Dict[str, Any]] = {
    # 15.8 came from an entry/exit - 1 P&L bug; corrected 2026-09-28 before any forward trade
    "listing_short": {"min_n": 30, "unit": "trade", "backtest_mean_pct": 1.5},
    "unlock_short": {"min_n": 20, "unit": "trade", "backtest_mean_pct": 9.5},
    "supply_factor": {"min_n": 12, "unit": "month", "backtest_mean_pct": 2.6},
    # added 2026-09-28 with its tracker, before its first forward trade
    "upbit_caution": {"min_n": 20, "unit": "trade", "backtest_mean_pct": 7.6},
    "upbit_krw_listing": {"min_n": 30, "unit": "trade", "backtest_mean_pct": 6.7},
    # added 2026-09-28 with the unlock last-week shadow tracker
    "unlock_short_t7": {"min_n": 30, "unit": "trade", "backtest_mean_pct": 3.6},
    # added 2026-10-01 with the daily reversal tracker, before its first forward day;
    # unit = net % per position per day; mean from scripts/xs_reversal_backtest.py
    "xs_reversal": {"min_n": 180, "unit": "day", "backtest_mean_pct": 0.057},
    # added 2026-10-04 with the hedged Upbit caution tracker, before its first forward trade;
    # mean from scripts/upbit_caution_hedged_backtest.py
    "upbit_caution_hedged": {"min_n": 20, "unit": "trade", "backtest_mean_pct": 5.0},
}
VERDICT_LABELS = {
    "collecting": "积累样本",
    "watch": "观察（均值为负）",
    "on_track": "符合预期",
    "confirmed": "前向确认",
    "retire": "应退役",
}


def register(state: Dict[str, Any], tracker: str, registered_at: str) -> Dict[str, Any]:
    """Store the rule in the tracker state once; later calls keep the stored copy."""
    if not state.get("retirement_rule"):
        state["retirement_rule"] = {**RULES[tracker], "decay_fraction": DECAY_FRACTION, "registered_at": registered_at}
    return state["retirement_rule"]


def verdict(returns_pct: Sequence[float], rule: Dict[str, Any]) -> Dict[str, Any]:
    values = [float(r) for r in returns_pct if r is not None and math.isfinite(float(r))]
    n, min_n = len(values), int(rule["min_n"])
    out: Dict[str, Any] = {"n": n, "min_n": min_n, "unit": rule.get("unit"), "mean_pct": None,
                           "ci90_pct": None, "verdict": "collecting", "reason": f"不足 {max(3, min_n // 2)} 个样本"}
    if n == 0:
        out["label"] = VERDICT_LABELS["collecting"]
        return out
    mean = sum(values) / n
    lower: Optional[float] = None
    upper: Optional[float] = None
    if n >= 2:
        sd = math.sqrt(sum((v - mean) ** 2 for v in values) / (n - 1))
        half = Z90 * sd / math.sqrt(n)
        lower, upper = mean - half, mean + half
    out["mean_pct"] = round(mean, 2)
    out["ci90_pct"] = [round(lower, 2), round(upper, 2)] if lower is not None else None
    decay_bar = float(rule["backtest_mean_pct"]) * float(rule.get("decay_fraction", DECAY_FRACTION))

    if n < max(3, min_n // 2):
        pass
    elif upper is not None and upper < 0:
        out.update(verdict="retire", reason="90% 区间上沿低于 0：明确在亏")
    elif n < min_n:
        out.update(verdict="watch" if mean < 0 else "collecting",
                   reason=f"未满 {min_n} 个样本" + ("，均值为负" if mean < 0 else ""))
    elif mean <= 0:
        out.update(verdict="retire", reason=f"满 {min_n} 个样本后均值不为正")
    elif upper is not None and upper < decay_bar:
        out.update(verdict="retire", reason=f"90% 区间上沿低于回测均值的 {int(DECAY_FRACTION * 100)}%：优势已衰减")
    elif lower is not None and lower > 0:
        out.update(verdict="confirmed", reason="90% 区间下沿高于 0")
    else:
        out.update(verdict="on_track", reason="均值为正，尚不显著")
    out["label"] = VERDICT_LABELS[out["verdict"]]
    return out
