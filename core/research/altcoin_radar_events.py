"""In-memory event and rank-history cache for the altcoin radar.

Stores recent per-symbol rank snapshots so that rank_jump_score can be
computed without any database round-trip.  Also records discrete events
(ignition cross-up, rank-jump) for the event-timeline panel.

Thread safety: all mutations use a Lock; the module is safe for asyncio
single-threaded use without the lock, but the lock is cheap.

Cache is bounded: at most MAX_HISTORY_PER_SYMBOL snapshots per symbol and
at most MAX_EVENTS_TOTAL events total.
"""
from __future__ import annotations

import math
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence


MAX_HISTORY_PER_SYMBOL = 24   # ~24 × 15min updates = 6 h of history
MAX_EVENTS_TOTAL = 500

# event types
EVENT_IGNITION_CROSS_UP = "altcoin_ignition_cross_up"
EVENT_RANK_JUMP = "altcoin_rank_jump_top_n"
EVENT_PERP_BURST = "altcoin_perp_burst"
EVENT_CROWDING_SPIKE = "altcoin_crowding_risk_spike"
EVENT_NARRATIVE_HEAT_SPIKE = "altcoin_narrative_heat_spike"

_lock = threading.Lock()

# symbol → list of {"ts": float, "rank": int, "ignition_score": float, ...}
_rank_history: Dict[str, List[Dict[str, Any]]] = {}

# list of event dicts, newest first
_events: List[Dict[str, Any]] = []


# ── helpers ────────────────────────────────────────────────────────────────

def _now_ts() -> float:
    return time.time()


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
    except Exception:
        return float(default)
    return float(default) if not math.isfinite(x) else x


# ── rank-history management ────────────────────────────────────────────────

def update_rank_snapshot(
    symbol: str,
    rank: int,
    scores: Optional[Dict[str, float]] = None,
) -> None:
    """Record current rank + key scores for a symbol.

    Call this once per scan cycle for each row returned by build_altcoin_rows.
    """
    key = str(symbol or "").strip().upper()
    if not key:
        return
    entry: Dict[str, Any] = {
        "ts": _now_ts(),
        "rank": int(rank),
    }
    if scores:
        entry.update({k: _f(v) for k, v in scores.items()})

    with _lock:
        history = _rank_history.setdefault(key, [])
        history.append(entry)
        # Keep only most recent N
        if len(history) > MAX_HISTORY_PER_SYMBOL:
            _rank_history[key] = history[-MAX_HISTORY_PER_SYMBOL:]


def get_rank_history(symbol: str) -> List[Dict[str, Any]]:
    """Return full rank history for a symbol (newest last)."""
    key = str(symbol or "").strip().upper()
    with _lock:
        return list(_rank_history.get(key, []))


def bulk_update_ranks(rows: Sequence[Dict[str, Any]]) -> None:
    """Update rank history for all rows returned by a scan.

    rows: list of row dicts from build_altcoin_rows (with 'symbol', 'rank',
          'ignition_score', 'layout_score', etc.)
    """
    for row in rows:
        sym = str((row or {}).get("symbol") or "").strip().upper()
        if not sym:
            continue
        update_rank_snapshot(
            sym,
            rank=int(_f(row.get("rank"), 99)),
            scores={
                "ignition_score": _f(row.get("ignition_score")),
                "layout_score": _f(row.get("layout_score")),
                "alert_score": _f(row.get("alert_score")),
                "crowding_late_score": _f(row.get("crowding_late_score")),
            },
        )


# ── rank_jump_score computation ────────────────────────────────────────────

def compute_rank_jump_score(
    symbol: str,
    current_rank: int,
    *,
    top_n: int = 15,
    lookback_sec: float = 900.0,   # 15 minutes
    min_jump: int = 3,
) -> float:
    """Return a [0, 1] score reflecting how dramatically rank improved recently.

    High score = entered top_n AND jumped ≥ min_jump positions in lookback_sec.
    """
    key = str(symbol or "").strip().upper()
    history = get_rank_history(key)
    if not history:
        return 0.0

    cutoff = _now_ts() - lookback_sec
    prev_entries = [e for e in history if e["ts"] < cutoff]
    if not prev_entries:
        # No old snapshot → rank jump unknown
        return 0.0

    oldest_recent = prev_entries[-1]  # most recent before cutoff
    prev_rank = int(_f(oldest_recent.get("rank"), 99))
    rank_jump = prev_rank - current_rank   # positive = improved

    if rank_jump < min_jump:
        return 0.0

    in_top_n = current_rank <= top_n
    jump_score = min(1.0, rank_jump / 20.0)  # 20+ places = max score

    # Boost if inside top-N
    if in_top_n:
        jump_score = min(1.0, jump_score * 1.3)

    return round(jump_score, 4)


# ── event recording ────────────────────────────────────────────────────────

def _add_event(event: Dict[str, Any]) -> None:
    with _lock:
        _events.insert(0, event)
        if len(_events) > MAX_EVENTS_TOTAL:
            del _events[MAX_EVENTS_TOTAL:]


def record_ignition_cross_up(
    symbol: str,
    ignition_score: float,
    prev_ignition_score: float,
    threshold: float = 0.60,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Record event if ignition_score crosses threshold from below.

    Returns the event dict if fired, else None.
    """
    ig = _f(ignition_score)
    prev = _f(prev_ignition_score)
    if not (prev < threshold <= ig):
        return None
    event = {
        "event_type": EVENT_IGNITION_CROSS_UP,
        "symbol": str(symbol or "").strip().upper(),
        "ts": _now_ts(),
        "ts_iso": datetime.now(timezone.utc).isoformat(),
        "ignition_score": round(ig, 4),
        "prev_ignition_score": round(prev, 4),
        "threshold": threshold,
        **(metadata or {}),
    }
    _add_event(event)
    return event


def record_rank_jump_event(
    symbol: str,
    current_rank: int,
    prev_rank: int,
    top_n: int = 15,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Record event if symbol entered top_n AND jumped significantly."""
    jump = prev_rank - current_rank
    if jump < 3 or current_rank > top_n:
        return None
    event = {
        "event_type": EVENT_RANK_JUMP,
        "symbol": str(symbol or "").strip().upper(),
        "ts": _now_ts(),
        "ts_iso": datetime.now(timezone.utc).isoformat(),
        "current_rank": current_rank,
        "prev_rank": prev_rank,
        "rank_jump": jump,
        "top_n": top_n,
        **(metadata or {}),
    }
    _add_event(event)
    return event


def record_crowding_spike(
    symbol: str,
    crowding_late_score: float,
    threshold: float = 0.65,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Record crowding-risk spike event."""
    sc = _f(crowding_late_score)
    if sc < threshold:
        return None
    event = {
        "event_type": EVENT_CROWDING_SPIKE,
        "symbol": str(symbol or "").strip().upper(),
        "ts": _now_ts(),
        "ts_iso": datetime.now(timezone.utc).isoformat(),
        "crowding_late_score": round(sc, 4),
        "threshold": threshold,
        **(metadata or {}),
    }
    _add_event(event)
    return event


def record_narrative_heat_spike(
    symbol: str,
    narrative_heat_score: float,
    threshold: float = 0.55,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Record narrative heat spike event when score crosses threshold.

    Fires when narrative_heat_score >= threshold (absolute, not cross-up).
    Returns the event dict if fired, else None.
    """
    sc = _f(narrative_heat_score)
    if sc < threshold:
        return None
    event = {
        "event_type": EVENT_NARRATIVE_HEAT_SPIKE,
        "symbol": str(symbol or "").strip().upper(),
        "ts": _now_ts(),
        "ts_iso": datetime.now(timezone.utc).isoformat(),
        "narrative_heat_score": round(sc, 4),
        "threshold": threshold,
        **(metadata or {}),
    }
    _add_event(event)
    return event


# ── query helpers ──────────────────────────────────────────────────────────

def get_recent_symbol_events(
    symbol: str,
    *,
    limit: int = 20,
    max_age_sec: float = 3600.0,
) -> List[Dict[str, Any]]:
    """Return recent events for a specific symbol."""
    key = str(symbol or "").strip().upper()
    cutoff = _now_ts() - max_age_sec
    with _lock:
        out = [
            e for e in _events
            if str(e.get("symbol") or "").strip().upper() == key
            and e.get("ts", 0) >= cutoff
        ]
    return out[:limit]


def get_all_recent_events(
    *,
    limit: int = 50,
    max_age_sec: float = 3600.0,
    event_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return recent events across all symbols."""
    cutoff = _now_ts() - max_age_sec
    with _lock:
        out = [
            e for e in _events
            if e.get("ts", 0) >= cutoff
            and (event_type is None or e.get("event_type") == event_type)
        ]
    return out[:limit]


def prune_old_events(max_age_sec: float = 7200.0) -> int:
    """Remove events older than max_age_sec. Returns number removed."""
    cutoff = _now_ts() - max_age_sec
    with _lock:
        before = len(_events)
        keep = [e for e in _events if e.get("ts", 0) >= cutoff]
        _events[:] = keep
        return before - len(keep)
