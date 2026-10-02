"""CUSUM-based strategy decay detection for live/paper strategy monitoring.

Usage
-----
One-shot analysis of a return series::

    from core.monitoring.strategy_monitor import detect_strategy_decay
    result = detect_strategy_decay(returns=[0.001, -0.003, ...], target_return=0.0)
    if result["triggered"]:
        print(result["message"])

Stateful incremental monitoring::

    from core.monitoring.strategy_monitor import CUSUMMonitor
    monitor = CUSUMMonitor(strategy_name="MAStrategy", target_return=0.0)
    for bar_return in live_stream:
        status = monitor.update(bar_return)
        if status["triggered"]:
            alert(status["message"])
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _robust_std(values: List[float], center: Optional[float] = None) -> float:
    """Estimate return scale with MAD so isolated fat-tail bars do not mask decay."""
    import math

    cleaned = [float(item) for item in values if math.isfinite(float(item))]
    n = len(cleaned)
    if n < 2:
        return 1e-6

    ordered = sorted(cleaned)

    def _median(rows: List[float]) -> float:
        size = len(rows)
        mid = size // 2
        if size % 2:
            return rows[mid]
        return (rows[mid - 1] + rows[mid]) / 2.0

    baseline = float(center) if center is not None and math.isfinite(float(center)) else _median(ordered)
    deviations = sorted(abs(item - baseline) for item in ordered)
    mad = _median(deviations)
    if mad <= 0:
        nonzero_deviations = [item for item in deviations if item > 0]
        if nonzero_deviations:
            mad = _median(nonzero_deviations)
    if mad > 0:
        return max(1.4826 * mad, 1e-6)

    mean_r = sum(cleaned) / n
    var_r = sum((r - mean_r) ** 2 for r in cleaned) / max(n - 1, 1)
    return math.sqrt(max(var_r, 1e-12))


def detect_strategy_decay(
    returns: List[float],
    target_return: float = 0.0,
    h: float = 2.0,
    k: float = 0.5,
    min_bars: int = 20,
) -> Dict[str, Any]:
    """One-shot CUSUM downward-shift detection on a return series.

    Parameters
    ----------
    returns:
        Sequence of bar/period returns (e.g., daily PnL as fraction of capital).
    target_return:
        Expected return per bar under the null (no decay) hypothesis. 0.0 = break-even.
    h:
        Decision threshold (in units of std). Triggered when CUSUM_low <= -h * std.
        Higher h = fewer false positives. Recommended range: [1.5, 3.0].
    k:
        Allowance (slack). Filters out shifts smaller than k * std. Typical: 0.25–0.75.
    min_bars:
        Minimum number of bars required before the test is reliable.

    Returns
    -------
    dict with keys:
        triggered (bool): True if downward shift detected.
        cusum_low (List[float]): Lower CUSUM series (cumulative excess loss).
        trigger_idx (Optional[int]): Index where decay was first detected (None if not triggered).
        decay_pct (Optional[float]): Magnitude of cumulative excess loss at trigger, in %.
        n_bars (int): Number of bars analyzed.
        std (float): Rolling std of returns used for threshold.
        threshold (float): Decision boundary value (-h * std).
        message (str): Human-readable status string.
    """
    n = len(returns)
    if n < 2:
        return {
            "triggered": False,
            "cusum_low": [],
            "trigger_idx": None,
            "decay_pct": None,
            "n_bars": n,
            "std": 0.0,
            "threshold": 0.0,
            "message": "insufficient data",
        }

    std_r = _robust_std(returns, center=target_return)

    threshold = -abs(h) * std_r
    allowance = abs(k) * std_r  # slack: ignore shifts smaller than k*std

    # Lower CUSUM: detects downward shifts (strategy returning less than target - allowance)
    cusum_low: List[float] = []
    s_low = 0.0
    trigger_idx: Optional[int] = None
    reliable_from = max(0, min_bars - 1)

    for i, r in enumerate(returns):
        # Increment: excess return below (target - allowance)
        s_low = min(0.0, s_low + (r - target_return + allowance))
        cusum_low.append(round(s_low * 100.0, 6))  # store as %
        if trigger_idx is None and i >= reliable_from and s_low <= threshold:
            trigger_idx = i

    triggered = trigger_idx is not None
    decay_pct = round(cusum_low[trigger_idx] if trigger_idx is not None else cusum_low[-1] if cusum_low else 0.0, 4)

    if triggered:
        msg = (
            f"CUSUM decay triggered at bar {trigger_idx} — "
            f"cumulative excess loss {decay_pct:.2f}% (threshold {threshold * 100:.2f}%)"
        )
    elif n < min_bars:
        msg = f"warming up ({n}/{min_bars} bars)"
    else:
        last_cusum = cusum_low[-1] if cusum_low else 0.0
        pct_to_trigger = abs(threshold * 100 - last_cusum) if threshold != 0 else float("inf")
        msg = f"no decay detected — {pct_to_trigger:.2f}% buffer to threshold"

    return {
        "triggered": triggered,
        "cusum_low": cusum_low,
        "trigger_idx": trigger_idx,
        "decay_pct": decay_pct,
        "n_bars": n,
        "std": round(std_r * 100.0, 6),  # in %
        "threshold": round(threshold * 100.0, 6),  # in %
        "message": msg,
    }


@dataclass
class CUSUMMonitor:
    """Stateful incremental CUSUM monitor for a single strategy.

    Maintains running state so new bars can be fed one at a time without
    reprocessing the full history.

    Attributes
    ----------
    strategy_name:
        Identifier for logging / alerts.
    target_return:
        Expected return per bar under no-decay null (fraction, e.g., 0.0).
    h:
        Decision threshold multiplier (times std).
    k:
        Allowance multiplier (times std).
    min_bars:
        Warm-up period before triggering is enabled.
    reset_on_trigger:
        If True, reset CUSUM after trigger to continue monitoring.
    """

    strategy_name: str
    target_return: float = 0.0
    h: float = 2.0
    k: float = 0.5
    min_bars: int = 20
    reset_on_trigger: bool = True
    cooldown_bars: int = 5

    # Internal state
    _returns: List[float] = field(default_factory=list, repr=False)
    _cusum_low: float = field(default=0.0, repr=False)
    _triggered: bool = field(default=False, repr=False)
    _trigger_count: int = field(default=0, repr=False)
    _last_trigger_at: Optional[datetime] = field(default=None, repr=False)
    _cooldown_until_n: int = field(default=0, repr=False)

    def update(self, bar_return: float) -> Dict[str, Any]:
        """Feed a new bar return; return current status dict."""
        self._returns.append(float(bar_return))
        n = len(self._returns)

        # Recompute scale from full history (cheap enough for typical sizes < 10k)
        std_r = _robust_std(self._returns, center=self.target_return)

        threshold = -abs(self.h) * std_r
        allowance = abs(self.k) * std_r

        # Update lower CUSUM
        self._cusum_low = min(0.0, self._cusum_low + (bar_return - self.target_return + allowance))

        newly_triggered = False
        in_cooldown = n < int(self._cooldown_until_n or 0)
        if n >= self.min_bars and not in_cooldown and self._cusum_low <= threshold and not self._triggered:
            self._triggered = True
            self._trigger_count += 1
            self._last_trigger_at = _now_utc()
            newly_triggered = True
            if self.reset_on_trigger:
                self._cusum_low = 0.0
                self._triggered = False
                self._cooldown_until_n = n + max(0, int(self.cooldown_bars or 0))

        return {
            "strategy": self.strategy_name,
            "n_bars": n,
            "cusum_low_pct": round(self._cusum_low * 100.0, 4),
            "threshold_pct": round(threshold * 100.0, 4),
            "triggered": newly_triggered,
            "trigger_count": self._trigger_count,
            "last_trigger_at": self._last_trigger_at.isoformat() if self._last_trigger_at else None,
            "std_pct": round(std_r * 100.0, 4),
            "bar_return_pct": round(bar_return * 100.0, 4),
            "cooldown_bars_remaining": max(0, int(self._cooldown_until_n or 0) - n),
        }

    def reset(self) -> None:
        """Reset CUSUM state (keep history for std estimation)."""
        self._cusum_low = 0.0
        self._triggered = False
        self._cooldown_until_n = 0

    def full_reset(self) -> None:
        """Reset all state including return history."""
        self._returns.clear()
        self._cusum_low = 0.0
        self._triggered = False
        self._trigger_count = 0
        self._last_trigger_at = None
        self._cooldown_until_n = 0

    @property
    def n_bars(self) -> int:
        return len(self._returns)

    @property
    def trigger_count(self) -> int:
        return self._trigger_count

    def summary(self) -> Dict[str, Any]:
        """Return a summary dict of current monitor state."""
        return {
            "strategy": self.strategy_name,
            "n_bars": self.n_bars,
            "cusum_low_pct": round(self._cusum_low * 100.0, 4),
            "trigger_count": self._trigger_count,
            "last_trigger_at": self._last_trigger_at.isoformat() if self._last_trigger_at else None,
            "is_warm": self.n_bars >= self.min_bars,
        }
