"""Exploratory follow-up of the already frozen Alastar labels; never executes trades.

The prior historical split and labels are retained, not reselected. All additional
scenarios are descriptive sensitivity checks, not new untouched validation sets.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.research_telegram_signal_strategy import (
    ROOT, LocalMarket, choose_book, equity_curve, evaluate, funding_cost, metrics, read_jsonl, utc,
)

ORIGINAL = ROOT / "data/research/telegram_signal_study_20261020"
OUT = ROOT / "data/research/alastar_followup_20261021"


def evaluate_variant(signal: dict, market: LocalMarket, *, delay_minutes: int = 1,
                     cost_bps: int = 20, stop_fraction: float | None = None) -> dict:
    """Next hour open after specified delay; a stop uses adverse gap-aware OHLC.

    No take-profit is modeled. Stops fill at the stop unless the hourly open is
    already worse. Within-hit-hour funding eligibility is bounded using all
    possible settlement prefixes, with the adverse bound used in summaries.
    """
    shifted = {**signal, "published_at": str(utc(signal["published_at"]) +
                                             pd.Timedelta(minutes=delay_minutes - 1))}
    row = evaluate(shifted, market, 24)
    row.update(published_at=signal["published_at"], delay_minutes=delay_minutes,
               round_trip_cost=cost_bps / 10000, cost_bps=cost_bps,
               stop_fraction=stop_fraction, stop_triggered=False)
    if row["status"] != "ok":
        return row
    if stop_fraction is not None:
        if not 0 < stop_fraction < 1:
            raise ValueError("stop_fraction must be between zero and one")
        start, planned = utc(row["entry_at"]), utc(row["planned_exit_at"])
        path = market.frame(row["symbol"]).loc[start:planned].iloc[:-1]
        side, p0 = row["direction"], row["entry_price"]
        stop = p0 * (1 - side * stop_fraction)
        hit = path["low"] <= stop if side > 0 else path["high"] >= stop
        if hit.any():
            hit_at = hit.index[np.flatnonzero(hit.to_numpy())[0]]
            open_ = float(path.loc[hit_at, "open"])
            fill = min(stop, open_) if side > 0 else max(stop, open_)
            gap = open_ <= stop if side > 0 else open_ >= stop
            end = hit_at if gap else hit_at + pd.Timedelta(hours=1)
            # A hit occurred strictly inside this bar if its open was safe. The
            # next-bar settlement is certainly too late; inside-bar ordering is
            # unknown from OHLC, so retain a pessimistic/optimistic prefix bound.
            effective_end = max(start, end - pd.Timedelta(nanoseconds=1))
            fund, settlements, error = funding_cost(
                market.frame(row["symbol"], "funding_rate"), start, effective_end, p0, side)
            lower = upper = fund
            if fund is not None and not gap:
                certain = [s for s in settlements if utc(s["at"]) <= hit_at]
                ambiguous = [s for s in settlements if utc(s["at"]) > hit_at]
                prefixes = np.r_[0., np.cumsum([s["return"] for s in ambiguous])]
                fixed = sum(s["return"] for s in certain)
                lower, upper = fixed + float(prefixes.min()), fixed + float(prefixes.max())
                settlements = certain + ambiguous[:int(prefixes.argmin())]
                fund = lower
            # If the opening gap itself breached approximate isolated collateral,
            # keep the liquidation outcome rather than allow an impossible fill.
            loss = side * (fill / p0 - 1)
            liquidated = loss <= -1
            row.update(exit_at=str(end), exit_price=fill, gross_return=max(-1., loss),
                       funding_return=fund, settlements=settlements, funding_error=error,
                       stop_triggered=True, liquidation_approx=liquidated,
                       liquidated_1x_approx=liquidated,
                       stop_hit_hour=str(hit_at),
                       stop_funding_lower_bound=lower, stop_funding_upper_bound=upper,
                       stop_timing_approximation="next-bar funding excluded; adverse prefix bound within hit hour")
            row.pop("max_adverse_excursion", None)
            row.pop("max_favorable_excursion", None)
    gross, fund, cost = row["gross_return"], row["funding_return"], cost_bps / 10000
    row.update(net_before_funding=gross - cost,
               net_return=gross - cost + fund if fund is not None else None,
               net_stress_40bps=gross - .004 + fund if fund is not None else None)
    return row


def describe(rows: list[dict], market: LocalMarket) -> dict:
    selected, skipped = choose_book(rows, market, funded=True)
    result = {**metrics(selected, "net_return"), "skipped": skipped,
              "trade_ids": [r["source_id"] for r in selected]}
    if not selected:
        return result
    values = np.array([r["net_return"] for r in selected])
    curve = equity_curve(selected, market, funded=True)
    equity = np.array([r["equity"] for r in curve])
    result.update(
        fixed_initial_20pct_portfolio_return=float(.2 * values.sum()),
        max_hourly_drawdown=float((equity / np.maximum.accumulate(equity) - 1).min()),
        best_trade_share_of_net=float(values.max() / values.sum()) if values.sum() > 1e-12 else None,
        stop_triggers=sum(r.get("stop_triggered", False) for r in selected),
        mean_cost_breakeven_bps=float(np.mean([r["gross_return"] + r["funding_return"] for r in selected])*10000),
    )
    return result


def compare_snapshots() -> dict:
    old_path = ROOT / "data/research/alastar_20261020/messages.jsonl"
    new_path = ROOT / "data/research/alastar_20261021/messages.jsonl"
    if not new_path.exists():
        return {"status": "refresh_unavailable"}
    old = {r["source_id"]: r for r in read_jsonl(old_path)}
    new = {r["source_id"]: r for r in read_jsonl(new_path)}
    added = [r for k, r in new.items() if k not in old]
    changes = [{"source_id": k, "old_sha256": hashlib.sha256(old[k]["content"].encode()).hexdigest(),
                "new_sha256": hashlib.sha256(new[k]["content"].encode()).hexdigest(),
                "current_visible_edited": new[k]["payload"]["telegram_edited"]}
               for k in sorted(old.keys() & new.keys()) if old[k]["content"] != new[k]["content"]]
    return {"old_messages": len(old), "new_messages": len(new), "added": added,
            "content_changes": changes, "absent_from_new_snapshot": sorted(old.keys() - new.keys()),
            "new_sha256": hashlib.sha256(new_path.read_bytes()).hexdigest(),
            "limitations": "Missing IDs do not prove deletion; snapshots are current public text only."}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    protocol = {"created_at": datetime.now(timezone.utc).isoformat(), "status": "exploratory_after_original_results",
                "frozen_label_file": str((ORIGINAL / "frozen_signals.json").relative_to(ROOT)),
                "holding_hours": 24, "delay_minutes": [1, 60, 180, 360], "round_trip_bps": [20, 40, 100],
                "stop_fraction": [None, .05, .10, .20], "no_parameter_selection": True,
                "original_split_retained": True,
                "randomization": "20,000 independent fair direction draws on matched entries; diagnostic only, not a causal p value"}
    (OUT / "followup_protocol.json").write_text(json.dumps(protocol, indent=2), encoding="utf-8")
    signals = [r for r in json.loads((ORIGINAL / "frozen_signals.json").read_text(encoding="utf-8"))
               if r["source"] == "alastartrades"]
    prior = json.loads((ORIGINAL / "summary.json").read_text(encoding="utf-8"))["alastartrades"]
    cutoff = utc(prior["historical_holdout_start"])
    market = LocalMarket()
    base = []
    for signal in signals:
        row = evaluate_variant(signal, market)
        row["phase"] = "holdout" if utc(signal["published_at"]) >= cutoff else "development"
        base.append(row)
    all_selected, skipped = choose_book(base, market, funded=True)
    expected = {r["source_id"]: r for r in read_jsonl(ORIGINAL / "event_outcomes.jsonl")
                if r["source"] == "alastartrades" and r["holding_hours"] == 24}
    reproduction = []
    for row in base:
        old = expected[row["source_id"]]
        reproduction.append({"source_id": row["source_id"], "status_matches": row["status"] == old["status"],
                             "return_delta": row.get("net_return", 0) - old.get("net_return", 0),
                             "entry_price_matches": row.get("entry_price") == old.get("entry_price")})
    assert all(r["status_matches"] and abs(r["return_delta"]) < 1e-10 and r["entry_price_matches"] for r in reproduction), "Prior study no longer reproduces; inspect market data drift"
    phases = {p: [r for r in base if p == "all" or r["phase"] == p]
              for p in ("all", "development", "holdout")}
    summaries = {p: describe(rows, market) for p, rows in phases.items()}
    scenarios = []
    scenario_outcomes = []
    for name, changes in [("delay", {"delay_minutes": d}) for d in (1, 60, 180, 360)] + [
        ("cost", {"cost_bps": c}) for c in (20, 40, 100)] + [
        ("stop", {"stop_fraction": s}) for s in (.05, .10, .20)]:
        rows = [evaluate_variant(s, market, **changes) for s in signals]
        scenario_outcomes.extend({**r, "scenario_family": name,
                                  "scenario_parameter": next(iter(changes.values()))} for r in rows)
        for phase in ("development", "holdout", "all"):
            part = [r for r in rows if phase == "all" or ("holdout" if utc(r["published_at"]) >= cutoff else "development") == phase]
            # Purge any development entry/holding window that crosses the fixed boundary.
            part = [r for r in part if phase != "development" or utc(r["planned_exit_at"]) <= cutoff]
            scenarios.append({"family": name, "parameter": next(iter(changes.values())), "phase": phase,
                              **describe(part, market)})
    grouped = {}
    for field in ("symbol", "direction", "phase"):
        grouped[field] = {str(key): describe([r for r in base if r[field] == key], market)
                          for key in sorted({r[field] for r in base})}
    monthly = []
    for month in sorted({r["entry_at"][:7] for r in base}):
        monthly.append({"month": month, **describe([r for r in base if r["entry_at"].startswith(month)], market)})
    leave_one_symbol = []
    for phase, rows in phases.items():
        for symbol in sorted({r["symbol"] for r in rows}):
            leave_one_symbol.append({"phase": phase, "excluded_symbol": symbol,
                                     **describe([r for r in rows if r["symbol"] != symbol], market)})
    baselines = {}
    randomization = {}
    for phase, rows in phases.items():
        selected, _ = choose_book(rows, market, funded=True)
        paired = [(r, evaluate({**r, "direction": -r["direction"]}, market, 24),
                   evaluate({**r, "direction": 1}, market, 24)) for r in selected]
        paired = [p for p in paired if all(r.get("net_return") is not None for r in p)]
        baselines[phase] = {name: metrics([p[i] for p in paired], "net_return")
                            for i, name in enumerate(("signal", "inverse", "always_long"))}
        if paired:
            same = np.array([p[0]["net_return"] for p in paired])
            inverse = np.array([p[1]["net_return"] for p in paired])
            rng = np.random.default_rng(20261021)
            draws = rng.integers(0, 2, size=(20000, len(paired)))
            means = np.where(draws, same, inverse).mean(axis=1)
            randomization[phase] = {"n": len(paired), "observed_mean": float(same.mean()),
                                    "random_direction_95pct": np.quantile(means, [.025, .975]).tolist(),
                                    "fraction_random_at_least_observed": float((means >= same.mean()).mean()),
                                    "caveat": "Direction exchangeability/independence is unproven; not an investable baseline or formal significance result."}
    original_sources = json.loads((ORIGINAL / "market_sources.json").read_text(encoding="utf-8"))
    if "files" in original_sources:
        original_sources = original_sources["files"]
    changed_files = [{"path": p, "previous_sha256": original_sources.get(p, {}).get("sha256"),
                      "current_sha256": info["sha256"]}
                     for p, info in market.sources.items()
                     if original_sources.get(p, {}).get("sha256") != info["sha256"]]
    result = {"as_of": datetime.now(timezone.utc).isoformat(), "original_signals": len(signals),
              "fixed_historical_holdout_start": str(cutoff), "original_result_reproduction": reproduction,
              "market_files_changed_since_prior_study": changed_files, "summaries": summaries,
              "scenarios": scenarios, "groups": grouped, "months": monthly,
              "leave_one_symbol_out": leave_one_symbol, "matched_baselines": baselines,
              "random_direction_diagnostic": randomization, "snapshot_comparison": compare_snapshots(),
              "limitations": ["All new checks are exploratory and correlated, not untouched forward validation.",
                              "Current public text cannot reconstruct deleted/earlier message versions.",
                              "Hourly stops/isolated liquidation and costs are approximations; no order-book capacity claim.",
                              "No new labels, assets, dates or scenarios are chosen based on improved returns."]}
    (OUT / "robustness.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    (OUT / "frozen26_outcomes.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in base), encoding="utf-8")
    (OUT / "scenario_outcomes.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in scenario_outcomes), encoding="utf-8")
    # Semantic expansion is frozen separately by a price-blind full-text review.
    # Keep the original time boundary; this is exploratory revision, not a new OOS.
    review_path = OUT / "coverage_review.json"
    if review_path.exists():
        review = json.loads(review_path.read_text(encoding="utf-8"))
        archive = {r["source_id"]: r for r in read_jsonl(ROOT / "data/research/alastar_20261020/messages.jsonl")}
        additions = []
        for label in review["additions"]:
            m = archive[label["source_id"]]
            assert hashlib.sha256(m["content"].encode()).hexdigest() == label["text_sha256"]
            assert label["evidence_quote"] in m["content"] and not m["payload"]["telegram_edited"]
            additions.append({"source_id": label["source_id"], "source": "alastartrades",
                              "symbol": label["symbol"], "direction": label["direction"],
                              "published_at": m["published_at"], "url": m["url"],
                              "confidence": label["confidence"], "kind": "expanded_offline_semantic_view",
                              "evidence": label["evidence_quote"]})
        (OUT / "expanded_frozen_signals.json").write_text(json.dumps(signals + additions, indent=2), encoding="utf-8")
        expanded = {}
        expansion_rows = []
        for name, pool in (("additions_only", additions), ("expanded_40", signals + additions)):
            rows = [evaluate_variant(s, market) for s in pool]
            expansion_rows.extend({**r, "pool": name} for r in rows)
            expanded[name] = {"text_views": len(pool), "phases": {}}
            for phase in ("development", "holdout", "all"):
                part = [r for r in rows if phase == "all" or ("holdout" if utc(r["published_at"]) >= cutoff else "development") == phase]
                part = [r for r in part if phase != "development" or utc(r["planned_exit_at"]) <= cutoff]
                expanded[name]["phases"][phase] = describe(part, market)
        expanded["review_sha256"] = hashlib.sha256(review_path.read_bytes()).hexdigest()
        expanded["all_422_reviewed"] = len(review["records"]) == len(archive)
        expanded["limitations"] = "Additional labels reinterpret known history. Neither phase becomes untouched future validation. Missing data and overlapping positions are excluded explicitly."
        (OUT / "expanded_results.json").write_text(json.dumps(expanded, ensure_ascii=False, indent=2), encoding="utf-8")
        (OUT / "expanded_outcomes.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in expansion_rows), encoding="utf-8")
        # Only one original selected view states an unambiguous numeric horizon:
        # #394 says 'within a month'. Inspect calendar month, not the best horizon.
        month_signal = next(s for s in signals if s["source_id"] == "telegram:alastartrades:394")
        assert "within a month" in archive[month_signal["source_id"]]["content"]
        start = (utc(month_signal["published_at"]) + pd.Timedelta(minutes=1)).ceil("h")
        end = start + pd.DateOffset(months=1)
        month_row = evaluate(month_signal, market, int((end-start)/pd.Timedelta(hours=1)))
        month_row.update(source_horizon_quote="My view: AKE trades back toward 0.0003 within a month.",
                         interpretation="Single-case descriptive text-aligned horizon; toward is not an exact limit-order target or mandatory exit instruction.")
        (OUT / "explicit_horizon_case.json").write_text(json.dumps(month_row, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT / "market_sources.json").write_text(json.dumps(market.sources, indent=2), encoding="utf-8")
    print(json.dumps({"summaries": summaries, "original_reproduced": True,
                      "changed_market_files": len(changed_files)}, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
