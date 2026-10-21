"""Read-only diagnostics for the frozen reversal baseline and its liquidation shadow.

The observation is a common calendar day, never a coin or a portfolio leg. These
sensitivities do not replace the registered retirement rule or confirm a strategy.
All percentage returns below use total long-plus-short gross notional.
"""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from statistics import mean, stdev
from typing import Any, Dict, Mapping, Optional, Sequence

DAY_MS = 86_400_000
HOUR_MS = 3_600_000
MIN_SENSITIVITY_DAYS = 32
MIN_SENSITIVITY_WEEKS = 8
HAC_LAGS = 7
Z90 = 1.6448536269514722


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _stamp(value: Any) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, TypeError, OverflowError):
        return None


def _stats(values: Sequence[float]) -> Dict[str, Any]:
    n = len(values)
    center = mean(values) if n else None
    half = Z90 * stdev(values) / math.sqrt(n) if n > 1 else None
    return {"n": n, "mean_pct": center, "sum_pct": sum(values) if n else None,
            "positive_days": sum(value > 0 for value in values),
            "positive_share": sum(value > 0 for value in values) / n if n else None,
            "nominal_ci90_pct": [center-half, center+half] if half is not None else None}


def _t90(df: int) -> float:
    """Conservative Student t critical value, rounded down to a tabulated df."""
    table = {1: 6.313752, 2: 2.919986, 3: 2.353364, 4: 2.131847, 5: 2.015049,
             6: 1.943181, 7: 1.894579, 8: 1.859548, 9: 1.833113, 10: 1.812462,
             12: 1.782288, 15: 1.753051, 20: 1.724719, 30: 1.697261,
             60: 1.670649, 120: 1.657651}
    return table[max(key for key in table if key <= max(1, df))]


def _dependence(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(rows)
    groups: Dict[str, list] = defaultdict(list)
    for row in rows:
        iso = date.fromisoformat(row["date"]).isocalendar()
        groups[f"{iso.year}-W{iso.week:02d}"].append(row)
    weeks = [{"week": key, "n_days": len(group),
              "mean_increment_pp": mean(row["increment_pp"] for row in group),
              "first_date": group[0]["date"], "last_date": group[-1]["date"],
              "complete_calendar_week": len(group) == 7} for key, group in sorted(groups.items())]
    intervals = [(row["date"], row.get("entry_close_ms")) for row in rows]
    overlaps = [[day_a, day_b] for i, (day_a, start_a) in enumerate(intervals) for day_b, start_b in intervals[i+1:]
                if start_a is not None and start_b is not None and abs(start_a-start_b) < DAY_MS]
    center = mean(row["increment_pp"] for row in rows) if n else None
    common = {"n_days": n, "n_week_groups": len(groups), "min_days": MIN_SENSITIVITY_DAYS,
              "min_week_groups": MIN_SENSITIVITY_WEEKS, "confirmation": "not_assessed"}
    reasons = []
    if n < MIN_SENSITIVITY_DAYS:
        reasons.append("too_few_common_days")
    if len(groups) < MIN_SENSITIVITY_WEEKS:
        reasons.append("too_few_calendar_week_groups")
    if overlaps:
        reasons.append("overlapping_daily_holding_intervals")
    weekly = {**common, "status": "insufficient_data" if reasons else "sensitivity_only",
              "reasons": list(reasons), "weeks": weeks, "ci90_pp": None,
              "method": "calendar_week_cluster_robust_mean_with_conservative_student_t"}
    if not reasons:
        residual_sums = [sum(row["increment_pp"]-center for row in group) for group in groups.values()]
        g = len(groups)
        se = math.sqrt(g/(g-1)*sum(value*value for value in residual_sums))/n
        half = _t90(g-1)*se
        weekly.update(standard_error_pp=se, ci90_pp=[center-half, center+half])
    ordinals = {date.fromisoformat(row["date"]).toordinal(): row["increment_pp"] for row in rows}
    span = max(ordinals)-min(ordinals)+1 if ordinals else 0
    coverage = n/span if span else None
    lag_counts = {str(lag): sum(day-lag in ordinals for day in ordinals) for lag in range(1, HAC_LAGS+1)}
    hac_reasons = list(reasons)
    if coverage is not None and coverage < .8:
        hac_reasons.append("sparse_calendar_coverage")
    hac = {**common, "status": "insufficient_data" if hac_reasons else "sensitivity_only",
           "reasons": hac_reasons, "lags_calendar_days": HAC_LAGS, "calendar_coverage": coverage,
           "calendar_lag_pair_counts": lag_counts, "ci90_pp": None,
           "method": "bartlett_newey_west_calendar_lags_with_week_group_student_t"}
    if not hac_reasons:
        # Missing days are not compressed into adjacent observations. Bartlett weights
        # use actual calendar separation, followed by the finite-sample n/(n-1) factor.
        residuals = {day: value-center for day, value in ordinals.items()}
        variance_sum = sum(value*value for value in residuals.values())
        for lag in range(1, HAC_LAGS+1):
            covariance_sum = sum(value*residuals[day-lag] for day, value in residuals.items() if day-lag in residuals)
            variance_sum += 2*(1-lag/(HAC_LAGS+1))*covariance_sum
        se = math.sqrt(max(0., variance_sum)*n/(n-1))/n
        half = _t90(len(groups)-1)*se
        hac.update(standard_error_pp=se, ci90_pp=[center-half, center+half])
    return {"weekly": weekly, "hac": hac, "holding_overlap_pairs": overlaps,
            "known_holding_intervals": sum(start is not None for _, start in intervals)}


def _position_components(positions: Mapping[str, Any], side: int) -> tuple:
    components, missing = {}, []
    for symbol, position in positions.items():
        entry, exit_price = _number(position.get("entry")), _number(position.get("exit"))
        funding, recorded = _number(position.get("funding")), _number(position.get("return"))
        if entry is None or entry <= 0 or exit_price is None or exit_price <= 0 or funding is None or recorded is None:
            missing.append(symbol)
            continue
        components[symbol] = {"price": side*(exit_price/entry-1)*100,
                              "funding": -side*funding*100, "net": recorded*100}
    return components, missing


def _attribution(record: Mapping[str, Any], shadow: Mapping[str, Any]) -> Dict[str, Any]:
    main_longs, main_missing = _position_components(record.get("longs") or {}, 1)
    new_longs, new_missing = _position_components(shadow.get("longs") or {}, 1)
    shorts, short_missing = _position_components(record.get("shorts") or {}, -1)
    main_names, new_names = set(record.get("longs") or {}), set(shadow.get("longs") or {})
    removed, replacement, shared = main_names-new_names, new_names-main_names, main_names & new_names
    out = {"removed": sorted(removed), "replacement": sorted(replacement), "shared": sorted(shared),
           "main_long_count": len(main_names), "shadow_long_count": len(new_names),
           "missing_main_longs": main_missing, "missing_shadow_longs": new_missing,
           "missing_shorts": short_missing, "status": "incomplete_legs"}
    if main_missing or new_missing or short_missing or not main_longs or not new_longs or not shorts:
        return out
    # The filtered decile may be smaller. Shared names therefore change weight;
    # ignoring that term would falsely attribute all improvement to exclusions.
    for component in ("price", "funding", "net"):
        out[f"removed_{component}_pp"] = -.5*sum(main_longs[s][component] for s in removed)/len(main_longs)
        out[f"replacement_{component}_pp"] = .5*sum(new_longs[s][component] for s in replacement)/len(new_longs)
        out[f"shared_reweight_{component}_pp"] = .5*sum(
            new_longs[s][component]/len(new_longs)-main_longs[s][component]/len(main_longs) for s in shared)
    price_delta = sum(out[f"{part}_price_pp"] for part in ("removed", "replacement", "shared_reweight"))
    fund_delta = sum(out[f"{part}_funding_pp"] for part in ("removed", "replacement", "shared_reweight"))
    out.update(price_delta_pp=price_delta, funding_delta_pp=fund_delta, cost_delta_pp=0., short_delta_pp=0.,
               explained_increment_pp=price_delta+fund_delta)
    actual = float(shadow["net_per_position_pct"])-float(record["net_per_position_pct"])
    error = actual-price_delta-fund_delta
    out.update(reconciliation_error_pp=error, status="complete" if abs(error) <= .00025 else "unreconciled")
    return out


def _quote_diagnostic(positions: Mapping[str, Any], fallback: Mapping[str, Any]) -> Dict[str, Any]:
    gaps, missing, sources = [], [], Counter()
    for symbol, position in positions.items():
        quote = _number(position.get("mark_at_formation"))
        source = "recorded_position_quote"
        if quote is None:
            quote = _number((fallback.get(symbol) or {}).get("mark_at_formation"))
            source = "shared_baseline_quote"
        entry = _number(position.get("entry"))
        if quote is None or quote <= 0 or entry is None or entry <= 0:
            missing.append(symbol)
        else:
            gaps.append((quote/entry-1)*100)
            sources[source] += 1
    return {"n_positions": len(positions), "n_quotes": len(gaps), "missing_symbols": missing,
            "mean_price_gap_pct": mean(gaps) if gaps else None, "quote_sources": dict(sources)}


def _execution(day: str, record: Mapping[str, Any], shadow: Mapping[str, Any]) -> Dict[str, Any]:
    audit = record.get("formation_audit") or {}
    close_ms = _number(record.get("entry_bar_ms"))
    close_ms = close_ms+HOUR_MS if close_ms is not None else None
    formed, signal_ready, shadow_ready = (_stamp(record.get("formed_at")), _stamp(audit.get("signal_ready_at")),
                                             _stamp(audit.get("shadow_ready_at")))
    quote_at = _stamp(audit.get("price_snapshot_observed_at"))
    def lag(stamp):
        return (stamp.timestamp()*1000-close_ms)/60000 if stamp is not None and close_ms is not None else None
    books = {"baseline_longs": _quote_diagnostic(record.get("longs") or {}, {}),
             "shared_shorts": _quote_diagnostic(record.get("shorts") or {}, {}),
             "shadow_longs": _quote_diagnostic(shadow.get("longs") or {}, record.get("longs") or {})}
    baseline_complete = bool(books["baseline_longs"]["n_positions"] and books["shared_shorts"]["n_positions"] and
                             all(not books[key]["missing_symbols"] for key in ("baseline_longs", "shared_shorts")))
    shadow_complete = bool(books["shadow_longs"]["n_positions"] and books["shared_shorts"]["n_positions"] and
                           all(not books[key]["missing_symbols"] for key in ("shadow_longs", "shared_shorts")))
    def disadvantage(long_key, complete):
        return .5*(books[long_key]["mean_price_gap_pct"]-books["shared_shorts"]["mean_price_gap_pct"]) if complete else None
    return {"date": day, "formation_lag_minutes": lag(formed), "legacy_formed_at_is_tick_start": not bool(audit),
            "signal_ready_lag_minutes": lag(signal_ready), "shadow_ready_lag_minutes": lag(shadow_ready),
            "quote_observed_at": quote_at.isoformat() if quote_at else None,
            "quote_age_at_shadow_ready_seconds": (shadow_ready-quote_at).total_seconds() if quote_at and shadow_ready else None,
            "quote_kind": audit.get("price_quote_kind", "legacy_last_trade_price_without_observation_timestamp"),
            "quote_coverage": books, "baseline_quotes_complete": baseline_complete, "shadow_quotes_complete": shadow_complete,
            "baseline_signed_entry_disadvantage_pp": disadvantage("baseline_longs", baseline_complete),
            "shadow_signed_entry_disadvantage_pp": disadvantage("shadow_longs", shadow_complete),
            "executable_return_available": False,
            "limitation": "Last-trade quotes are price drift diagnostics; no spread/depth/fill replay or executable return is inferred."}


def evaluate_reversal_pairs(days: Mapping[str, Any], *, round_trip_cost_pct: float = .15) -> Dict[str, Any]:
    """Compare only closed, timely baseline/shadow results from the same day.

    Ledger returns remain visible when a leg was missing; attribution and quote
    diagnostics explicitly identify the missing evidence. No state is mutated.
    """
    rows, attribution_rows, execution_rows, exclusions = [], [], [], Counter()
    eligible_main = eligible_shadow = 0
    for day, record in sorted(days.items()):
        try:
            date.fromisoformat(day)
        except (TypeError, ValueError):
            exclusions["invalid_date"] += 1
            continue
        if record.get("status") != "closed" or record.get("late") or record.get("backfilled"):
            exclusions["baseline_not_closed_or_not_forward"] += 1
            continue
        baseline = _number(record.get("net_per_position_pct"))
        if baseline is None:
            exclusions["missing_baseline_return"] += 1
            continue
        eligible_main += 1
        shadow = record.get("liq_filter") or {}
        if shadow.get("status") != "closed" or shadow.get("late") or shadow.get("backfilled"):
            exclusions["shadow_not_closed_or_not_forward"] += 1
            continue
        eligible_shadow += 1
        if shadow.get("date", day) != day or (shadow.get("entry_bar_ms") is not None and
                                                shadow.get("entry_bar_ms") != record.get("entry_bar_ms")):
            exclusions["shadow_date_or_interval_mismatch"] += 1
            continue
        filtered = _number(shadow.get("net_per_position_pct"))
        if filtered is None:
            exclusions["missing_shadow_return"] += 1
            continue
        entry_bar_ms = _number(record.get("entry_bar_ms"))
        rows.append({"date": day, "baseline_pct": baseline, "shadow_pct": filtered,
                     "increment_pp": filtered-baseline, "recorded_increment_pp": _number(shadow.get("diff_pct")),
                     "entry_close_ms": entry_bar_ms+HOUR_MS if entry_bar_ms is not None else None,
                     "baseline_missing_positions": record.get("missing_positions"),
                     "attribution_status": None})
        attribution = {"date": day, **_attribution(record, shadow)}
        rows[-1]["attribution_status"] = attribution["status"]
        attribution_rows.append(attribution)
        execution_rows.append(_execution(day, record, shadow))
    baseline_stats = _stats([row["baseline_pct"] for row in rows])
    shadow_stats = _stats([row["shadow_pct"] for row in rows])
    increment_stats = _stats([row["increment_pp"] for row in rows])
    # Explicit point-unit aliases prevent the paired difference being mistaken
    # for the filtered portfolio's own percent return by generic summary UIs.
    increment_stats.update(mean_pp=increment_stats["mean_pct"], sum_pp=increment_stats["sum_pct"],
                           nominal_ci90_pp=increment_stats["nominal_ci90_pct"])
    dependence = _dependence(rows)
    complete = [row for row in attribution_rows if row["status"] == "complete"]
    attribution_summary = {"complete_days": len(complete), "missing_days": sum(row["status"] == "incomplete_legs" for row in attribution_rows),
                           "unreconciled_days": sum(row["status"] == "unreconciled" for row in attribution_rows), "days": attribution_rows}
    for key in ("removed_price_pp", "removed_funding_pp", "replacement_price_pp", "replacement_funding_pp",
                "shared_reweight_price_pp", "shared_reweight_funding_pp", "price_delta_pp", "funding_delta_pp"):
        attribution_summary[f"mean_{key}"] = mean(row[key] for row in complete) if complete else None
    best = max(rows, key=lambda row: row["increment_pp"]) if rows else None
    rest = [row for row in rows if row is not best]
    costs = []
    for extra in (0., .05, .1, .2):
        costs.append({"extra_cost_pp": extra,
                      "baseline_mean_with_common_extra_cost_pct": baseline_stats["mean_pct"]-extra if rows else None,
                      "shadow_mean_with_common_extra_cost_pct": shadow_stats["mean_pct"]-extra if rows else None,
                      "increment_with_common_extra_cost_pp": increment_stats["mean_pct"],
                      "increment_with_shadow_only_extra_cost_pp": increment_stats["mean_pct"]-extra if rows else None})
    execution_summary = {"days": execution_rows, "paired_days": len(rows),
                         "baseline_complete_quote_days": sum(row["baseline_quotes_complete"] for row in execution_rows),
                         "shadow_complete_quote_days": sum(row["shadow_quotes_complete"] for row in execution_rows),
                         "unknown_decision_time_days": sum(row["shadow_ready_lag_minutes"] is None for row in execution_rows),
                         "shadow_ready_after_30min_days": sum(row["shadow_ready_lag_minutes"] is not None and row["shadow_ready_lag_minutes"] > 30
                                                              for row in execution_rows),
                         "executable_return_available": False}
    for key in ("formation_lag_minutes", "signal_ready_lag_minutes", "shadow_ready_lag_minutes",
                "quote_age_at_shadow_ready_seconds", "baseline_signed_entry_disadvantage_pp",
                "shadow_signed_entry_disadvantage_pp"):
        values = [row[key] for row in execution_rows if row[key] is not None]
        execution_summary[f"mean_{key}"] = mean(values) if values else None
        execution_summary[f"max_{key}"] = max(values) if values else None
    return {"schema_version": 1, "status": "insufficient_data" if dependence["weekly"]["status"] == "insufficient_data" else "descriptive_only",
            "unit": "pct_total_gross_notional_per_day", "increment_unit": "percentage_points_per_day",
            "sample_unit": "common_calendar_day", "confirmation_status": "not_assessed",
            "n_pairs": len(rows), "eligible_baseline_days": eligible_main, "eligible_shadow_days": eligible_shadow,
            "common_dates": [row["date"] for row in rows], "start_date": rows[0]["date"] if rows else None,
            "end_date": rows[-1]["date"] if rows else None, "exclusions": dict(exclusions),
            "baseline": baseline_stats, "shadow": shadow_stats, "increment": increment_stats,
            "sensitivity": {**dependence, "leave_best_increment_day_out": {
                "excluded_date": best["date"] if best else None, "n_pairs": len(rest),
                "baseline_mean_pct": mean(row["baseline_pct"] for row in rest) if rest else None,
                "shadow_mean_pct": mean(row["shadow_pct"] for row in rest) if rest else None,
                "increment_mean_pp": mean(row["increment_pp"] for row in rest) if rest else None},
                "recorded_round_trip_cost_pct": round_trip_cost_pct, "cost": costs},
            "attribution": attribution_summary, "execution_diagnostics": execution_summary, "daily": rows,
            "limitations": ["A common day is one observation; positions and the two versions are not independent samples.",
                            "Nominal normal intervals ignore dependence and are descriptive only, especially for small samples.",
                            "Weekly and HAC intervals require at least 32 common days and eight calendar week groups; no strategy confirmation is inferred.",
                            "Historical partial settlements remain ledger observations; missing legs are excluded from attribution, not silently invented.",
                            "Quotes measure entry-price drift, not executable returns; current spread, depth and fills are unavailable.",
                            "Existing frozen retirement rules and all historical records remain separate and unchanged."]}
