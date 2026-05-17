"""Reusable threshold helpers for noisy market labels."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HysteresisBand:
    enter_threshold: float
    exit_threshold: float
    uncertain_low: float
    uncertain_high: float


DEFAULT_TREND_BAND = HysteresisBand(
    enter_threshold=0.14,
    exit_threshold=0.08,
    uncertain_low=0.08,
    uncertain_high=0.14,
)

DEFAULT_SPREAD_ENTER_BPS = 8.0
DEFAULT_SPREAD_EXIT_BPS = 6.0
HALT_NEW_ENTRIES_SPREAD_BPS = 12.0


def classify_margin(value: float, *, enter_threshold: float, uncertain_low: float) -> tuple[str, float]:
    abs_value = abs(float(value or 0.0))
    if abs_value >= float(enter_threshold):
        return "confirmed", round(abs_value - float(enter_threshold), 6)
    if abs_value >= float(uncertain_low):
        return "borderline", round(abs_value - float(uncertain_low), 6)
    return "low_signal", round(abs_value - float(uncertain_low), 6)


def spread_risk_posture(spread_bps: float) -> str:
    spread = float(spread_bps or 0.0)
    if spread >= HALT_NEW_ENTRIES_SPREAD_BPS:
        return "halt_new_entries"
    if spread >= DEFAULT_SPREAD_ENTER_BPS:
        return "defensive"
    return "normal"

