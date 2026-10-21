"""Read-only, date-matched attribution of the four frozen Coinglass books.

This module never forms, settles, edits or saves a tracker. A wide book and an
original book sharing a signal date can still have different entry times: the
expansion comparisons are consequently not a pure universe experiment.
All return contributions are percentage points per book (one unit long and
one unit short), not returns on total account capital.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

BASE_ROUND_TRIP_PCT = 0.12  # per leg, percent rather than decimal fraction
STRESS_ROUND_TRIP_PCT = 0.20
CELL_SPECS = {
    "original_ic": ("original", "main"),
    "original_equal": ("original", "shadow"),
    "wide_ic": ("wide", "shadow"),
    "wide_equal": ("wide", "main"),
}
CONTRIBUTIONS = ("net_pct", "price_pct", "funding_pct", "cost_pct", "stress_net_pct",
                 "long_price_pct", "short_price_pct", "long_funding_pct", "short_funding_pct",
                 "long_cost_pct", "short_cost_pct", "turnover")


def _number(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _stamp(value: Any) -> Optional[datetime]:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    # Time differences must never silently assume a timezone for old records.
    return stamp.astimezone(timezone.utc) if stamp.tzinfo is not None else None


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    rows = list(values)
    if not rows or any(v is None for v in rows):
        return None
    return round(sum(rows) / len(rows), 6)


def _week(day: str) -> str:
    year, week, _ = date.fromisoformat(day).isocalendar()
    return f"{year}-W{week:02d}"


def _stats(rows: list[Dict[str, Any]], field: str = "net_pct") -> Dict[str, Any]:
    valid = [r for r in rows if _number(r.get(field)) is not None]
    mean = _mean(r[field] for r in valid)
    groups: Dict[str, float] = {}
    if mean is not None:
        for row in valid:
            key = row.get("cluster_week")
            if key is not None:
                groups[key] = groups.get(key, 0.0) + row[field] - mean
    ci = None
    n, g = len(valid), len(groups)
    grouped = sum(row.get("cluster_week") is not None for row in valid)
    if n >= 2 and g >= 2 and grouped == n:
        half = 1.6448536269514722 * math.sqrt(g / (g - 1) * sum(v * v for v in groups.values())) / n
        ci = [round(mean - half, 6), round(mean + half, 6)]
    return {"n": n, "mean_pct": mean, "ci90_pct": ci, "weekly_groups": g,
            "weeks": sorted(groups), "grouping": "settlement_iso_week_latest_in_pair_or_four_cells",
            "missing_week_count": n - grouped,
            "status": "descriptive" if ci is not None else "insufficient_data"}


def _leg_turnover(records: Dict[str, Any], day: str, book_key: str,
                  book: Dict[str, Any]) -> Optional[tuple[float, float]]:
    """Recover each leg's share only when it reconciles to frozen turnover."""
    prior_day = (date.fromisoformat(day) - timedelta(days=7)).isoformat()
    prior = (records.get(prior_day) or {}).get(book_key) or {}
    shares = []
    for side in ("longs", "shorts"):
        current = book.get(side) or {}
        if not current:
            return None
        old = prior.get(side) or {}
        shares.append(len(set(current) - set(old)) / len(current) if old else 1.0)
    turnover = _number(book.get("turnover"))
    if turnover is None or abs(sum(shares) / 2 - turnover) > 0.000051:
        return None
    return shares[0], shares[1]


def _book_metrics(records: Dict[str, Any], day: str, book_key: str) -> Optional[Dict[str, Any]]:
    record = records[day]
    book = record.get(book_key) or {}
    net = _number(book.get("net_pct"))
    # Overdue trackers may close with missing names. Those partial returns are
    # not comparable complete books, and are explicitly excluded below.
    if book.get("status") != "closed" or net is None or book.get("missing_positions", 0) != 0:
        return None
    out: Dict[str, Any] = {"signal_date": day, "net_pct": net, "formed_at": record.get("formed_at"),
                           "settled_at": book.get("settled_at"), "partial_universe": bool(record.get("partial")),
                           "scored": book.get("scored"), "coins": record.get("coins")}
    settled = _stamp(book.get("settled_at"))
    out["cluster_week"] = _week(settled.date().isoformat()) if settled else None
    turnover, cost = _number(book.get("turnover")), _number(book.get("cost_pct"))
    if turnover is not None and not 0 <= turnover <= 1:
        turnover = None
    out.update(turnover=turnover, cost_pct=cost)
    # Baseline is the frozen observed net. Cost stress adds the change on BOTH
    # legs: full replacement costs 0.24% baseline and 0.40% under stress.
    extra = 2 * turnover * (STRESS_ROUND_TRIP_PCT - BASE_ROUND_TRIP_PCT) if turnover is not None else None
    out["stress_extra_cost_pct"] = round(extra, 6) if extra is not None else None
    out["stress_net_pct"] = round(net - extra, 6) if extra is not None else None
    for side, prefix, sign in (("longs", "long", 1), ("shorts", "short", -1)):
        positions = list((book.get(side) or {}).values())
        price_parts, funding_parts = [], []
        for pos in positions:
            entry, exit_price, funding = (_number(pos.get(k)) for k in ("entry", "exit", "funding"))
            price_parts.append(sign * (exit_price / entry - 1) * 100
                               if entry is not None and entry > 0 and exit_price is not None else None)
            funding_parts.append(-sign * funding * 100 if funding is not None else None)
        out[f"{prefix}_price_pct"] = _mean(price_parts)
        out[f"{prefix}_funding_pct"] = _mean(funding_parts)
    for name in ("price", "funding"):
        a, b = out[f"long_{name}_pct"], out[f"short_{name}_pct"]
        out[f"{name}_pct"] = round(a + b, 6) if a is not None and b is not None else None
    sides = _leg_turnover(records, day, book_key, book)
    total_share = sum(sides) if sides else 0.0
    for index, prefix in enumerate(("long", "short")):
        out[f"{prefix}_cost_pct"] = (round(cost * sides[index] / total_share, 6) if total_share and cost is not None
                                     else 0.0 if sides is not None and cost == 0 else None)
    components = [out.get(k) for k in ("price_pct", "funding_pct", "cost_pct")]
    out["reconciliation_error_pct"] = (round(net - (components[0] + components[1] - components[2]), 6)
                                        if all(v is not None for v in components) else None)
    out["component_status"] = "complete" if all(v is not None for v in components) else "unavailable"
    return out


def _cells(states: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for cell, (source, book_key) in CELL_SPECS.items():
        records = states[source].get("books") or {}
        rows, excluded = {}, []
        for day in sorted(records):
            try:
                date.fromisoformat(day)
            except (TypeError, ValueError):
                excluded.append({"signal_date": day, "reason": "invalid_signal_date"})
                continue
            book = (records[day] or {}).get(book_key) or {}
            row = _book_metrics(records, day, book_key)
            if row is not None:
                rows[day] = row
            elif book.get("status") == "closed":
                reason = "incomplete_positions" if book.get("missing_positions", 0) != 0 else "invalid_net_return"
                excluded.append({"signal_date": day, "reason": reason})
        out[cell] = {"source": source, "book_key": book_key, "n_completed": len(rows),
                     "signal_dates": sorted(rows), "rows": list(rows.values()), "excluded": excluded,
                     "stats": _stats(list(rows.values())),
                     "mean_contributions": {key: _mean(r.get(key) for r in rows.values()) for key in CONTRIBUTIONS}}
    return out


def _comparison(cells: Dict[str, Any], left: str, right: str,
                dates: Optional[list[str]] = None) -> Dict[str, Any]:
    a = {r["signal_date"]: r for r in cells[left]["rows"]}
    b = {r["signal_date"]: r for r in cells[right]["rows"]}
    common = sorted(set(a) & set(b) & (set(dates) if dates is not None else set(a)))
    rows = []
    for day in common:
        row: Dict[str, Any] = {"signal_date": day}
        for field in CONTRIBUTIONS:
            av, bv = a[day].get(field), b[day].get(field)
            row[field] = round(bv - av, 6) if av is not None and bv is not None else None
        at, bt = _stamp(a[day].get("formed_at")), _stamp(b[day].get("formed_at"))
        row["formation_delay_minutes"] = round((bt - at).total_seconds() / 60, 3) if at and bt else None
        ae, be = _stamp(a[day].get("settled_at")), _stamp(b[day].get("settled_at"))
        row["settlement_delay_minutes"] = round((be - ae).total_seconds() / 60, 3) if ae and be else None
        row["cluster_week"] = _week(max(ae, be).date().isoformat()) if ae and be else None
        rows.append(row)
    expansion = CELL_SPECS[left][0] != CELL_SPECS[right][0]
    return {"left": left, "right": right, "direction": "right_minus_left", "n_pairs": len(rows),
            "signal_dates": common, "rows": rows, **_stats(rows),
            "mean_contributions": {key: _mean(r.get(key) for r in rows) for key in CONTRIBUTIONS},
            "stress_stats": _stats(rows, "stress_net_pct"),
            "timing_confounded": any(r["formation_delay_minutes"] != 0 or r["settlement_delay_minutes"] != 0 for r in rows),
            "interpretation": "universe_and_formation_time" if expansion else "weighting_at_same_formation_time"}


def _timing(original: Dict[str, Any], wide: Dict[str, Any]) -> Dict[str, Any]:
    a, b = original.get("books") or {}, wide.get("books") or {}
    rows = []
    for day in sorted(set(a) & set(b)):
        at, bt = _stamp((a[day] or {}).get("formed_at")), _stamp((b[day] or {}).get("formed_at"))
        if at and bt:
            rows.append({"signal_date": day, "original_formed_at": at.isoformat(), "wide_formed_at": bt.isoformat(),
                         "wide_minus_original_minutes": round((bt - at).total_seconds() / 60, 3)})
    return {"n_dates": len(rows), "signal_dates": [r["signal_date"] for r in rows], "rows": rows,
            "mean_delay_minutes": _mean(r["wide_minus_original_minutes"] for r in rows),
            "pure_universe_effect_identified": False,
            "note": "Matching signal dates does not remove entry/exit-time differences; no timing-adjusted alpha is estimated."}


def load_attribution(original_state: Optional[Dict[str, Any]] = None,
                     wide_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Summarize supplied snapshots, or read the two existing state files.

    Missing files produce empty inputs, not a newly started tracker. Supplied
    snapshots are never modified. No market requests or disk writes occur.
    """
    if original_state is None or wide_state is None:
        import json
        from core.research.coinglass_composite_tracker import STATE_PATH, WIDE_STATE_PATH

        if original_state is None:
            original_state = json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
        if wide_state is None:
            wide_state = json.loads(WIDE_STATE_PATH.read_text(encoding="utf-8")) if WIDE_STATE_PATH.exists() else {}
    cells = _cells({"original": original_state, "wide": wide_state})
    specs = {"weighting_original": ("original_ic", "original_equal"),
             "weighting_wide": ("wide_ic", "wide_equal"),
             "expansion_ic": ("original_ic", "wide_ic"),
             "expansion_equal": ("original_equal", "wide_equal")}
    comparisons = {name: _comparison(cells, *pair) for name, pair in specs.items()}
    common = sorted(set.intersection(*(set(cell["signal_dates"]) for cell in cells.values())))
    matched = {name: _comparison(cells, *pair, dates=common) for name, pair in specs.items()}
    effect_rows: Dict[str, list] = {key: [] for key in ("weighting", "expansion", "interaction")}
    by_date = {name: {r["signal_date"]: r["net_pct"] for r in cell["rows"]} for name, cell in cells.items()}
    settled_by_date = {name: {r["signal_date"]: _stamp(r.get("settled_at")) for r in cell["rows"]}
                       for name, cell in cells.items()}
    for day in common:
        oi, oe, wi, we = (by_date[name][day] for name in CELL_SPECS)
        values = {"weighting": ((oe - oi) + (we - wi)) / 2,
                  "expansion": ((wi - oi) + (we - oe)) / 2,
                  "interaction": (we - wi) - (oe - oi)}
        for name, value in values.items():
            exits = [settled_by_date[cell][day] for cell in CELL_SPECS]
            week = _week(max(exits).date().isoformat()) if all(exits) else None
            effect_rows[name].append({"signal_date": day, "net_pct": round(value, 6), "cluster_week": week})
    factorial = {"n_pairs": len(common), "signal_dates": common, "comparisons": matched,
                 "effects": {name: {**_stats(rows), "rows": rows} for name, rows in effect_rows.items()}}
    status = factorial["effects"]["weighting"]["status"]
    return {"status": status,
            "readiness": {"n_four_cell_dates": len(common), "has_four_cell_attribution": bool(common),
                          "has_weekly_uncertainty": status == "descriptive", "strategy_change_supported": False},
            "unit": "percentage_points_per_book_one_unit_long_plus_one_unit_short",
            "cells": cells, "comparisons": comparisons, "factorial_common": factorial,
            "timing": _timing(original_state, wide_state),
            "cost_model": {"baseline_round_trip_pct_per_leg": BASE_ROUND_TRIP_PCT,
                           "stress_round_trip_pct_per_leg": STRESS_ROUND_TRIP_PCT,
                           "full_turnover_extra_cost_pct_per_book": 0.16,
                           "formula": "stress_net_pct = frozen_net_pct - 2 * turnover * (0.20 - 0.12)"},
            "caveats": ["Descriptive attribution only; no frozen strategy, trade or retirement rule is changed.",
                        "Seven-day books overlap; pair counts are not independent observations and weeks can overlap at boundaries.",
                        "CI90 uses settlement-week cluster-robust normal approximation (latest exit when paired); absent with fewer than two weekly groups, fragile with few groups.",
                        "Same signal date across universes does not imply the same entry or exit time, so expansion includes timing differences.",
                        "Weighting comparisons share formation time but settlement delays can also confound their realized difference.",
                        "Per-leg costs are reconstructed only when turnover reconciles; missing components remain null.",
                        "Price and funding are signed P&L contributions; cost fields are positive drags to subtract.",
                        "Returns sum one long and one short leg and are not account-capital returns or annualized performance."]}
