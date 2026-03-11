"""Runtime limit policy derived from expected strategy trade frequency."""
from __future__ import annotations

import math
import re
from typing import Any, Dict, Optional

_TIMEFRAME_RE = re.compile(r"^\s*(\d+)\s*([mhdw])\s*$", re.IGNORECASE)
_INTERVAL_PARAM_KEYS = (
    "cooldown_min",
    "cooldown_minutes",
    "rebalance_interval_minutes",
    "rebalance_minutes",
    "hold_minutes",
    "min_hold_minutes",
)


def parse_timeframe_minutes(timeframe: str) -> int:
    """Convert timeframe text like '15m'/'1h' into minutes."""
    raw = str(timeframe or "").strip().lower()
    if not raw:
        return 60
    match = _TIMEFRAME_RE.match(raw)
    if not match:
        return 60
    amount = max(1, int(match.group(1)))
    unit = match.group(2).lower()
    multiplier = {"m": 1, "h": 60, "d": 1440, "w": 10080}.get(unit, 60)
    return max(1, amount * int(multiplier))


def _safe_positive_float(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    if not math.isfinite(num) or num <= 0:
        return None
    return float(num)


def infer_effective_interval_minutes(timeframe: str, params: Optional[Dict[str, Any]] = None) -> int:
    """Infer effective trade interval using timeframe + cooldown/rebalance hints."""
    base_minutes = parse_timeframe_minutes(timeframe)
    intervals = [float(base_minutes)]
    payload = dict(params or {})
    for key in _INTERVAL_PARAM_KEYS:
        value = _safe_positive_float(payload.get(key))
        if value is not None:
            intervals.append(value)
    return int(max(1.0, max(intervals)))


def build_runtime_limit_policy(
    *,
    timeframe: str,
    params: Optional[Dict[str, Any]] = None,
    observed_trades_per_day: Optional[float] = None,
    target_trade_samples: int = 80,
    min_runtime_minutes: int = 12 * 60,
    max_runtime_minutes: int = 7 * 24 * 60,
) -> Dict[str, Any]:
    """Return runtime policy metadata with an auto runtime_limit_minutes."""
    target_samples = max(20, int(target_trade_samples or 80))
    min_runtime = max(60, int(min_runtime_minutes or 720))
    max_runtime = max(min_runtime, int(max_runtime_minutes or 10080))
    interval_min = infer_effective_interval_minutes(timeframe, params)

    observed = _safe_positive_float(observed_trades_per_day)
    source = "observed" if observed is not None else "inferred"
    trades_per_day = observed if observed is not None else (1440.0 / float(interval_min))
    trades_per_day = max(0.05, float(trades_per_day))

    runtime_minutes = int(math.ceil((target_samples / trades_per_day) * 1440.0))
    runtime_minutes = max(min_runtime, min(max_runtime, runtime_minutes))
    runtime_minutes = int(math.ceil(runtime_minutes / 30.0) * 30)
    runtime_minutes = max(min_runtime, min(max_runtime, runtime_minutes))

    return {
        "runtime_limit_minutes": int(runtime_minutes),
        "estimated_trades_per_day": float(round(trades_per_day, 4)),
        "effective_interval_minutes": int(interval_min),
        "target_trade_samples": int(target_samples),
        "source": source,
        "bounds": {"min": int(min_runtime), "max": int(max_runtime)},
    }

