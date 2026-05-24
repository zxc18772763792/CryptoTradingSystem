"""Shared oscillator helpers.

Many strategies (RSI, Stochastic, CCI, MFI, Williams %R, StochRSI, …) share
the same "the value just crossed back out of an extreme zone, so emit a
signal whose strength scales with how far we were inside the zone" pattern.
Centralising the strength formula here lets us tune one knob across the
suite instead of N different ad-hoc formulas.
"""
from __future__ import annotations


def oscillator_entry_strength(
    *,
    current: float,
    previous: float,
    threshold: float,
    direction: str,
    neutral: float | None = None,
    cap: float = 1.0,
    floor: float = 0.2,
) -> float:
    """Map an oscillator crossing back through ``threshold`` to a [floor, cap] strength.

    Parameters
    ----------
    current : current bar's oscillator value (already crossed).
    previous : prior bar's value (was on the extreme side of ``threshold``).
    threshold : the entry boundary (oversold for long, overbought for short).
    direction : ``"long"`` or ``"short"``. ``"long"`` expects ``previous <=
        threshold`` and ``current > threshold``; ``"short"`` is the mirror.
    neutral : centre of the oscillator scale (defaults to 50 for RSI-like,
        0 for CCI-like; pass explicitly for non-standard scales).
    cap : maximum returned strength.
    floor : minimum returned strength (so a marginal crossing still produces
        a real signal rather than 0).

    Returns
    -------
    A float in [floor, cap]. Larger when ``previous`` was deeper inside the
    extreme zone (i.e. the divergence between ``previous`` and the neutral
    line was larger).
    """
    if neutral is None:
        # Heuristic: RSI/Stoch oscillators live on [0, 100] with neutral=50,
        # CCI on roughly [-200, 200] with neutral=0. If threshold is close to
        # 50 we assume the RSI scale; otherwise treat 0 as neutral.
        neutral = 50.0 if abs(threshold) <= 100.0 and threshold > 0 else 0.0

    direction = (direction or "").strip().lower()
    if direction not in {"long", "short"}:
        raise ValueError("direction must be 'long' or 'short'")

    if direction == "long":
        # We're crossing UP out of an oversold zone. Strength scales with how
        # deep prev was below the neutral line.
        depth = max(0.0, neutral - float(previous))
        scale = max(1e-9, neutral - float(threshold))
    else:
        depth = max(0.0, float(previous) - neutral)
        scale = max(1e-9, float(threshold) - neutral)

    raw = depth / scale  # 0..~1+ when prev is near the neutral, ≥1 when deeper
    return float(min(cap, max(floor, raw)))
