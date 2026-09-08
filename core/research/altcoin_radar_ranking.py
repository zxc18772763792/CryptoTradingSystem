"""Altcoin radar ranking, summary and signal-state helpers.

Percentile/weighting math, per-row sort keys, priority scoring, the public
sort_rows / summarize_rows, and the state-machine label + tag helpers.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from core.research.altcoin_radar_universe import normalize_altcoin_pair

from core.research.altcoin_radar_metrics import (
    STATE_ANOMALY,
    STATE_CONTROL_TRACK,
    STATE_CONTROL_WARN,
    STATE_DISTRIBUTION,
    STATE_LAYOUT,
    _clamp01,
    _to_float,
)


def _normalize_symbols(symbols: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for symbol in symbols:
        text = normalize_altcoin_pair(symbol)
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _series_percentiles(raw_map: Mapping[str, Optional[float]]) -> Dict[str, Optional[float]]:
    series = pd.Series({k: (_to_float(v) if v is not None else np.nan) for k, v in raw_map.items()}, dtype=float)
    valid = series.dropna()
    if valid.empty:
        return {k: None for k in raw_map}
    if len(valid) == 1:
        only_key = str(valid.index[0])
        return {k: (0.5 if str(k) == only_key else None) for k in raw_map}
    ranked = valid.rank(method="average", pct=True).clip(lower=0.0, upper=1.0)
    result: Dict[str, Optional[float]] = {}
    for key in raw_map:
        if key in ranked:
            result[key] = _clamp01(ranked[key])
        else:
            result[key] = None
    return result


def _weighted_score(values: Mapping[str, Optional[float]], weights: Mapping[str, float]) -> float:
    weighted = 0.0
    weight_sum = 0.0
    for key, weight in weights.items():
        value = values.get(key)
        if value is None:
            continue
        numeric = _clamp01(value)
        if weight <= 0:
            continue
        weighted += numeric * float(weight)
        weight_sum += float(weight)
    if weight_sum <= 0:
        return 0.0
    return weighted / weight_sum


def _sort_key_for_row(row: Mapping[str, Any], sort_by: str) -> float:
    normalized = str(sort_by or "layout").strip().lower()
    if normalized == "priority":
        return _priority_score(row)
    if normalized == "alert":
        return _to_float(row.get("alert_score"), 0.0)
    if normalized == "anomaly":
        return _to_float(row.get("anomaly_score"), 0.0)
    if normalized == "accumulation":
        return _to_float(row.get("accumulation_score"), 0.0)
    if normalized == "control":
        return _to_float(row.get("control_score"), 0.0)
    # Phase 1 new sort keys
    if normalized == "ignition":
        return _to_float(row.get("ignition_score"), 0.0)
    if normalized == "continuation":
        return _to_float(row.get("continuation_score"), 0.0)
    if normalized == "rank_jump":
        return _to_float(row.get("rank_jump_score"), 0.0)
    if normalized == "crowding":
        return _to_float(row.get("crowding_late_score"), 0.0)
    # Phase 2 new sort keys
    if normalized == "narrative":
        return _to_float(row.get("narrative_heat_score"), 0.0)
    if normalized == "meme_rotation":
        return _to_float(row.get("meme_rotation_score"), 0.0)
    if normalized == "upside":
        return _to_float(row.get("upside_score"), 0.0)
    if normalized == "chain":
        return _to_float(row.get("chain_confirmation_score"), 0.0)
    if normalized == "heat":
        return (
            _to_float(row.get("layout_score"), 0.0) * 0.25
            + _to_float(row.get("alert_score"), 0.0) * 0.20
            + _to_float(row.get("control_score"), 0.0) * 0.15
            + _to_float(row.get("chain_confirmation_score"), 0.0) * 0.10
            + _to_float(row.get("derivatives_heat_score"), 0.0) * 0.20
            + _to_float(row.get("flow_confirmation_score"), 0.0) * 0.10
        )
    return _to_float(row.get("layout_score"), 0.0)


def _priority_score(row: Mapping[str, Any]) -> float:
    freshness = dict(row.get("freshness") or {})
    data_quality = dict(row.get("data_quality") or {})
    degraded = bool(data_quality.get("degraded_reason"))
    derivatives_present = bool(
        (row.get("derivatives_context") or {}).get("available")
        or freshness.get("derivatives_present")
    )
    score = (
        _to_float(row.get("layout_score"), 0.0) * 0.22
        + _to_float(row.get("alert_score"), 0.0) * 0.18
        + _to_float(row.get("ignition_score"), 0.0) * 0.18
        + _to_float(row.get("control_score"), 0.0) * 0.12
        + _to_float(row.get("crowding_late_score"), 0.0) * 0.10
        + _to_float(row.get("narrative_heat_score"), 0.0) * 0.08
        + _to_float(row.get("derivatives_heat_score"), 0.0) * 0.08
        + (0.04 if derivatives_present else 0.0)
        + _to_float(row.get("upside_score"), 0.0) * 0.06
    )
    if degraded:
        score *= 0.75
    return _clamp01(score)


def _next_best_action(row: Mapping[str, Any]) -> str:
    if (
        _to_float(row.get("upside_score"), 0.0) >= 0.72
        and _to_float(row.get("risk_penalty"), 0.0) < 0.35
    ):
        return "generate_research_proposal"
    if _priority_score(row) >= 0.68:
        return "generate_research_proposal"
    if _to_float(row.get("crowding_late_score"), 0.0) >= 0.70:
        return "watch_crowding_risk"
    if _to_float(row.get("ignition_score"), 0.0) >= 0.65:
        return "inspect_ignition"
    return "watch"


def sort_rows(rows: Sequence[Mapping[str, Any]], sort_by: str = "layout") -> List[Dict[str, Any]]:
    ordered = [dict(row) for row in rows]
    ordered.sort(
        key=lambda row: (
            _sort_key_for_row(row, sort_by),
            _to_float(row.get("layout_score"), 0.0),
            _to_float(row.get("alert_score"), 0.0),
            _to_float(row.get("control_score"), 0.0),
        ),
        reverse=True,
    )
    out: List[Dict[str, Any]] = []
    for index, row in enumerate(ordered, start=1):
        row["rank"] = index
        row.setdefault("scores", {})
        if isinstance(row["scores"], dict):
            row["scores"]["priority"] = round(_priority_score(row), 6)
            row["scores"]["upside"] = round(_to_float(row.get("upside_score"), 0.0), 6)
        row["priority_score"] = round(_priority_score(row), 6)
        row["next_best_action"] = _next_best_action(row)
        out.append(row)
    return out


def summarize_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    exchange: str,
    timeframe: str,
    sort_by: str,
    symbols_requested: Sequence[str],
    symbols_used: Sequence[str],
    excluded_retired: Sequence[str],
    cache_key: str,
    warnings: Sequence[str],
) -> Dict[str, Any]:
    ordered = sort_rows(rows, sort_by=sort_by)
    leader = ordered[0] if ordered else {}
    degraded_count = sum(1 for row in ordered if row.get("data_quality", {}).get("degraded_reason"))
    # Every count here covers the rows passed in, which is the FULL scan -- the
    # caller truncates to `limit` only afterwards. So a summary saying "20
    # degraded" can sit above a table of 30 rows containing 10 degraded ones, and
    # the reader has no way to reconcile the two. State the scope explicitly so
    # the UI can label it instead of silently implying it describes the table.
    counts_row_total = len(ordered)
    summary = {
        "exchange": exchange,
        "timeframe": timeframe,
        "sort_by": sort_by,
        "scanned_count": len(symbols_used),
        "anomaly_count": sum(1 for row in ordered if row.get("signal_state") == STATE_ANOMALY),
        "accumulation_count": sum(1 for row in ordered if row.get("signal_state") == STATE_LAYOUT),
        "control_count": sum(
            1
            for row in ordered
            if row.get("signal_state") in {STATE_CONTROL_TRACK, STATE_CONTROL_WARN}
        ),
        "alpha_count": sum(1 for row in ordered if row.get("is_alpha")),
        "upside_count": sum(
            1
            for row in ordered
            if _to_float(row.get("upside_score"), 0.0) >= 0.70
            and _to_float(row.get("risk_penalty"), 0.0) < 0.35
        ),
        "degraded_count": degraded_count,
        # Denominator for every *_count above; not the number of rows returned.
        "counts_row_total": counts_row_total,
        "counts_scope": "all_scanned_rows",
        "leader": {
            "symbol": leader.get("symbol"),
            "signal_state": leader.get("signal_state"),
            "layout_score": leader.get("layout_score"),
            "alert_score": leader.get("alert_score"),
            "upside_score": leader.get("upside_score"),
            "is_alpha": bool(leader.get("is_alpha")),
        }
        if leader
        else None,
        "symbols_used": list(symbols_used),
    }
    return {
        "summary": summary,
        "rows": ordered,
        "scan_meta": {
            "exchange": exchange,
            "timeframe": timeframe,
            "sort_by": sort_by,
            "symbols_requested": list(symbols_requested),
            "symbols_used": list(symbols_used),
            "excluded_retired": list(excluded_retired),
            "cache_key": cache_key,
        },
        "warnings": list(warnings),
    }


def _signal_state_for_row(row: Mapping[str, Any]) -> str:
    if not bool(row.get("alt_eligible", True)):
        return ""
    anomaly = _to_float(row.get("anomaly_score"), 0.0)
    accumulation = _to_float(row.get("accumulation_score"), 0.0)
    control = _to_float(row.get("control_score"), 0.0)
    risk_penalty = _to_float(row.get("risk_penalty"), 0.0)
    if anomaly >= 0.70 and accumulation < 0.35 and control >= 0.65:
        return STATE_DISTRIBUTION
    if control >= 0.70 and risk_penalty >= 0.25:
        return STATE_CONTROL_WARN
    if accumulation >= 0.68 and control >= 0.55 and risk_penalty < 0.20:
        return STATE_LAYOUT
    if anomaly >= 0.72 and (accumulation >= 0.45 or control >= 0.45):
        return STATE_ANOMALY
    if control >= 0.70 and risk_penalty < 0.25:
        return STATE_CONTROL_TRACK
    return ""


def _state_tags(signal_state: str, *, degraded: bool, has_alert_rule: bool) -> List[str]:
    tags: List[str] = []
    if signal_state:
        tags.append(signal_state)
    if degraded:
        tags.append("数据降级")
    if has_alert_rule:
        tags.append("已建预警")
    return tags
