"""Fail closed on malformed inputs without changing frozen paper trading rules.

Funding here retains the legacy rate-sum approximation. It is not the cash
funding formula for a fixed contract quantity; do not label it actual cash P&L.
"""
from __future__ import annotations

import math
from typing import Any, Optional

DAY_MS = 86_400_000


def positive_price(value: Any) -> float:
    """Return a usable observed price, never a boolean, NaN or infinity."""
    try:
        price = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("invalid_market_price") from exc
    if isinstance(value, bool) or not math.isfinite(price) or price <= 0:
        raise ValueError("invalid_market_price")
    return price


def validate_stop_bar(bar: Any) -> None:
    """Validate fields used by the high/close stop model, without adding filters."""
    if not isinstance(bar, (list, tuple)) or len(bar) < 5:
        raise ValueError("invalid_stop_bar")
    high, close = positive_price(bar[2]), positive_price(bar[4])
    if high < close:
        raise ValueError("bar_high_below_close")


def funding_rate_sum(rows: Any, start_ms: int, end_ms: int, *,
                     include_start: bool = False, symbol: Optional[str] = None) -> float:
    """Validate rate rows; duplicate payments must never be counted twice.

    Successful empty lists retain the legacy meaning. They alone do not prove
    coverage of a funding schedule. HTTP failures must be rejected by callers.
    """
    if not isinstance(rows, list):
        raise ValueError("funding_history_not_list")
    seen, values = set(), []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid_funding_row")
        stamp_raw = float(row["fundingTime"])
        if not math.isfinite(stamp_raw) or stamp_raw != int(stamp_raw):
            raise ValueError("invalid_funding_timestamp")
        stamp = int(stamp_raw)
        if not (start_ms <= stamp <= end_ms if include_start else start_ms < stamp <= end_ms):
            continue
        rate = float(row["fundingRate"])
        if not math.isfinite(rate):
            raise ValueError("nonfinite_funding_rate")
        if stamp in seen:
            raise ValueError("duplicate_funding_payment")
        if symbol and row.get("symbol") not in (None, symbol):
            raise ValueError("wrong_funding_symbol")
        seen.add(stamp)
        values.append(rate)
    return math.fsum(values)


def validate_daily_path(bars: Any, *, start_ms: Optional[int] = None,
                        end_ms: Optional[int] = None) -> None:
    """Reject gaps/duplicates and invalid prices in the supplied daily stop path.

    Missing trailing bars remain pending in the scorer; an internal missing bar
    may conceal a stop and cannot be skipped or shift the D2/D14 row index.
    """
    if not isinstance(bars, list):
        raise ValueError("daily_bars_not_list")
    previous = None
    for bar in bars:
        if not isinstance(bar, (list, tuple)) or len(bar) < 5:
            raise ValueError("invalid_daily_bar")
        raw = float(bar[0])
        if not math.isfinite(raw) or raw != int(raw) or int(raw) % DAY_MS:
            raise ValueError("invalid_daily_timestamp")
        stamp = int(raw)
        if end_ms is not None and stamp >= end_ms:
            continue
        if start_ms is not None and stamp < start_ms:
            raise ValueError("daily_bar_before_requested_start")
        if previous is None and start_ms is not None and stamp != start_ms:
            raise ValueError("missing_first_daily_bar")
        if previous is not None and stamp != previous + DAY_MS:
            raise ValueError("incomplete_daily_stop_path")
        validate_stop_bar(bar)
        previous = stamp
