"""WS market-data quality guard: a system-level circuit breaker that auto-degrades
to REST when the exchange WS feed's quality deteriorates in ``ui_primary`` /
``strategy_primary`` modes, and auto-recovers once it is healthy again for a
sustained window.

This complements two existing mechanisms rather than replacing them:

* the per-signal fail-closed path in ``execution_engine._resolve_price`` (rejects
  an individual live order when a fresh metadata-bearing price is unavailable), and
* the manual ``MARKET_WS_FORCE_REST`` kill switch.

The guard adds the missing middle layer: when the feed is *persistently* bad it
stops trusting WS automatically (so UI/strategy reads fall back to REST) without
waiting for a human, and it flips back only after a longer healthy window.

Design notes
------------
* **Pure decision logic.** The caller feeds a normalized :class:`WsQualitySample`
  each tick and acts on the returned :class:`GuardDecision` (e.g. set an effective
  force-REST flag, emit an alert). No I/O, no globals — trivially testable.
* **Hysteresis.** Degrade on sustained badness over a short rolling window;
  recover only after a *longer* continuous-healthy window. This prevents the mode
  from flapping on every transient blip (which plain REST fallback already covers).
* **Conservative + opt-in.** Disabled unless explicitly enabled; thresholds tuned
  to ignore brief blips.
"""
from __future__ import annotations

import time as _time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple


@dataclass
class WsQualitySample:
    """A normalized snapshot of WS feed health at one instant."""

    ts: float
    feed_healthy: bool = False
    hub_healthy: bool = False
    ws_stale_symbol_count: int = 0
    last_tick_age_ms: Optional[float] = None
    rest_fallback_count: int = 0      # cumulative counter (tracked, not yet a trigger)
    shadow_violation_count: int = 0   # cumulative counter (price-divergence breaches)
    invalid_payload_count: int = 0    # cumulative counter (malformed payloads)
    feed_last_error: Optional[str] = None


@dataclass
class GuardDecision:
    action: str                       # "none" | "degrade" | "recover"
    state: str                        # "ws" | "degraded"
    force_rest: bool                  # whether the caller should force REST now
    reasons: List[str] = field(default_factory=list)
    healthy_fraction: float = 1.0
    samples: int = 0


class WsQualityGuard:
    """Stateful, hysteresis-based WS-quality circuit breaker."""

    WS = "ws"
    DEGRADED = "degraded"

    def __init__(
        self,
        *,
        enabled: bool = False,
        window_sec: float = 120.0,
        max_tick_age_ms: float = 10000.0,
        min_samples: int = 4,
        degrade_unhealthy_fraction: float = 0.5,
        degrade_breach_delta: int = 1,
        recover_healthy_sec: float = 300.0,
    ) -> None:
        self.enabled = bool(enabled)
        self.window_sec = max(10.0, float(window_sec))
        self.max_tick_age_ms = max(0.0, float(max_tick_age_ms))
        self.min_samples = max(1, int(min_samples))
        self.degrade_unhealthy_fraction = min(1.0, max(0.0, float(degrade_unhealthy_fraction)))
        self.degrade_breach_delta = max(1, int(degrade_breach_delta))
        self.recover_healthy_sec = max(0.0, float(recover_healthy_sec))
        self._samples: Deque[Tuple[float, WsQualitySample, bool]] = deque()
        self._state = self.WS
        self._degraded_since: Optional[float] = None
        self._healthy_since: Optional[float] = None
        self._degrade_count = 0
        self._recover_count = 0

    @property
    def state(self) -> str:
        return self._state

    @property
    def force_rest(self) -> bool:
        """True when the guard is enabled and currently in the degraded state."""
        return self.enabled and self._state == self.DEGRADED

    def _is_sample_healthy(self, s: WsQualitySample) -> bool:
        if not s.feed_healthy or not s.hub_healthy:
            return False
        if s.ws_stale_symbol_count and s.ws_stale_symbol_count > 0:
            return False
        if s.feed_last_error:
            return False
        if s.last_tick_age_ms is not None and s.last_tick_age_ms > self.max_tick_age_ms:
            return False
        return True

    def observe(self, sample: WsQualitySample) -> GuardDecision:
        now = float(sample.ts)
        healthy = self._is_sample_healthy(sample)
        self._samples.append((now, sample, healthy))
        cutoff = now - self.window_sec
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

        n = len(self._samples)
        healthy_n = sum(1 for _, _, h in self._samples if h)
        healthy_frac = (healthy_n / n) if n else 1.0
        unhealthy_frac = 1.0 - healthy_frac
        violations = [s.shadow_violation_count for _, s, _ in self._samples]
        invalids = [s.invalid_payload_count for _, s, _ in self._samples]
        breach_delta = max(
            (max(violations) - min(violations)) if violations else 0,
            (max(invalids) - min(invalids)) if invalids else 0,
        )

        reasons: List[str] = []
        action = "none"

        if not self.enabled:
            return GuardDecision("none", self._state, False, ["guard disabled"], healthy_frac, n)

        if self._state == self.WS:
            if n >= self.min_samples and unhealthy_frac >= self.degrade_unhealthy_fraction:
                reasons.append(
                    f"unhealthy_fraction={unhealthy_frac:.2f}>={self.degrade_unhealthy_fraction:.2f} over {n} samples"
                )
            if breach_delta >= self.degrade_breach_delta:
                reasons.append(f"quality_breach_delta={breach_delta}>={self.degrade_breach_delta}")
            if reasons:
                self._state = self.DEGRADED
                self._degraded_since = now
                self._healthy_since = None
                self._degrade_count += 1
                action = "degrade"
        else:  # DEGRADED -> require a sustained healthy window to recover
            if healthy:
                if self._healthy_since is None:
                    self._healthy_since = now
                elapsed = now - self._healthy_since
                if elapsed >= self.recover_healthy_sec:
                    self._state = self.WS
                    self._degraded_since = None
                    self._healthy_since = None
                    self._recover_count += 1
                    action = "recover"
                    reasons.append(f"healthy>={self.recover_healthy_sec:.0f}s")
                else:
                    reasons.append(f"recovering {elapsed:.0f}s/{self.recover_healthy_sec:.0f}s")
            else:
                self._healthy_since = None
                reasons.append("still unhealthy")

        return GuardDecision(action, self._state, self.force_rest, reasons, healthy_frac, n)

    def status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "state": self._state,
            "force_rest": self.force_rest,
            "samples": len(self._samples),
            "degrade_count": self._degrade_count,
            "recover_count": self._recover_count,
            "degraded_since": self._degraded_since,
        }


def sample_from_market_ws_status(status: Optional[Dict[str, Any]], now: Optional[float] = None) -> WsQualitySample:
    """Build a :class:`WsQualitySample` from an ``/api/status.market_ws`` payload.

    Keeps the integration to a single call: feed ``status["market_ws"]`` here,
    then ``guard.observe(...)``.
    """
    s = status or {}

    def _i(key: str) -> int:
        try:
            return int(s.get(key) or 0)
        except (TypeError, ValueError):
            return 0

    def _f(key: str) -> Optional[float]:
        v = s.get(key)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    hub = s.get("ws_hub_healthy")
    if hub is None:
        hub = s.get("hub_healthy")
    stale = s.get("ws_stale_symbol_count")
    if stale is None:
        stale = s.get("stale_symbol_count")
    try:
        stale = int(stale or 0)
    except (TypeError, ValueError):
        stale = 0
    err = s.get("feed_last_error")
    return WsQualitySample(
        ts=float(now if now is not None else _time.time()),
        feed_healthy=bool(s.get("feed_healthy")),
        hub_healthy=bool(hub),
        ws_stale_symbol_count=stale,
        last_tick_age_ms=_f("last_tick_age_ms"),
        rest_fallback_count=_i("rest_fallback_count"),
        shadow_violation_count=_i("shadow_compare_violation_count"),
        invalid_payload_count=_i("invalid_payload_count"),
        feed_last_error=(str(err) if err not in (None, "", "none", "null") else None),
    )
