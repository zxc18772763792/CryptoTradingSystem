"""Broad caption signal taxonomy and timestamp-aware, public-data event study.

Offline labels are one-off model interpretations, not an API-backed signal bot.
No exchange orders, service reconfiguration, or credentials are used here.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd
import requests

from scripts.research_telegram_signal_strategy import ROOT, read_jsonl, utc, week_bootstrap

OUT = ROOT / "data/research/alastar_response_20261021"
ARCHIVE = ROOT / "data/research/alastar_20261021/messages.jsonl"
STEP = pd.Timedelta(minutes=5)
CUTOFF = utc("2026-07-17T06:29:15Z")
SYMBOL = re.compile(r"[A-Z0-9\u4e00-\u9fff]{1,30}USDT\Z")
HOLDS = (.5, 1, 4, 24, 72, 168)
DELAYS = (1, 15, 60)


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def prepare():
    OUT.mkdir(parents=True, exist_ok=True)
    protocol = {"created_at": datetime.now(timezone.utc).isoformat(), "status": "historical_exploratory_revision",
                "message_archive_sha256": hashlib.sha256(ARCHIVE.read_bytes()).hexdigest(),
                "holding_hours": HOLDS, "publication_delay_minutes": DELAYS,
                "price_interval": "5m", "cost_bps_round_trip": 20, "cost_stress_bps": [40, 100],
                "families": ["fresh_view", "broad_directional", "numeric_cross", "until_opposite_update"],
                "time": "UTC; next5m open at/after publication+delay. Numeric trigger requires a completed5m cross then at least60sec before next open.",
                "numeric_trigger_expiry_hours": 24, "fixed_historical_boundary": str(CUTOFF),
                "selection": "Full422 caption review before any new price outcome joins. Edits excluded from return books but counted in informational taxonomy.",
                "margin": "Approximate1x isolated loss capped at allocated collateral; no exact mark-price maintenance-margin simulation.",
                "portfolio": "20% fixed initial notional, max5positions, onepercoin, cash/margin check; no stacking/restarting same-direction updates.",
                "no_best_parameter_claim": True, "true_future_validation": False,
                "limitations": ["Current text/image versions; no original receive times or deleted versions.",
                                "Many correlated scenario tests; highest historical value is not evidence of a deployable strategy."]}
    target = OUT / "response_protocol.json"
    if not target.exists(): write(target, protocol)
    return protocol


def merge_labels():
    prepare()
    messages = {m["source_id"]: m for m in read_jsonl(ARCHIVE)}
    records, events = [], []
    for name in ("early", "middle", "late"):
        part = json.loads((OUT / f"labels_{name}.json").read_text(encoding="utf-8"))
        records.extend(part if isinstance(part, list) else part.get("records", part.get("labels", [])))
    assert len(records) == len(messages) == len({r["source_id"] for r in records})
    for r in records:
        m = messages[r["source_id"]]
        assert hashlib.sha256(m["content"].encode()).hexdigest() == r["text_sha256"]
        assert utc(r["published_at"]) == utc(m["published_at"])
        assert r["visible_edited"] == m["payload"]["telegram_edited"]
        if r.get("evidence_quote"): assert r["evidence_quote"] in m["content"]
        for e in r["events"]:
            assert e["evidence_quote"] in m["content"]
            assert e["direction"] in (-1, 1) and SYMBOL.fullmatch(e["symbol"])
            for field in ("condition_quote", "explicit_horizon_quote", "invalidation_quote"):
                if e.get(field): assert e[field] in m["content"]
            events.append({**e, "source_id": r["source_id"], "published_at": m["published_at"],
                           "url": m["url"], "visible_edited": r["visible_edited"],
                           "is_fresh_entry_view": r["is_fresh_entry_view"], "categories": r["categories"]})
    assert len(events) == len({e["event_id"] for e in events})
    events.sort(key=lambda e: (utc(e["published_at"]), e["event_id"]))
    categories = Counter(c for r in records for c in r["categories"])
    summary = {"messages": len(records), "information_messages": sum(r["is_information_signal"] for r in records),
               "fresh_view_messages": sum(r["is_fresh_entry_view"] for r in records),
               "directional_event_messages": len({e["source_id"] for e in events}), "events": len(events),
               "categories_overlapping": dict(categories),
               "edited_messages": sum(r["visible_edited"] for r in records),
               "eligible_events": len([e for e in events if eligible(e)]),
               "reaction_types": dict(Counter(e["reaction_type"] for e in events)),
               "event_exclusions": dict(Counter(reason for e in events if (reason := exclusion(e))))}
    write(OUT / "taxonomy.json", {"summary": summary, "records": records})
    write(OUT / "frozen_response_events.json", events)
    print(json.dumps(summary, ensure_ascii=True, indent=2))


def all_events():
    events = json.loads((OUT / "frozen_response_events.json").read_text(encoding="utf-8"))
    index = {e["event_id"]: e for e in events}
    for name in ("semantic_overrides_late.json", "root_semantic_overrides.json"):
        path = OUT / name
        if path.exists():
            review = json.loads(path.read_text(encoding="utf-8"))
            for item in review["reviews"]:
                e = index[item["event_id"]]
                e.update(item["overrides"])
                e["condition_semantics"] = item.get("condition_semantics")
                if item["overrides"]: e["semantic_override_reason"] = item["reason"]
    visual = OUT / "visual_review.json"
    if visual.exists(): events += json.loads(visual.read_text(encoding="utf-8"))["events"]
    return events


def horizon_events(pool, hours):
    """Resolve simultaneous opposed views using explicit horizon words only.

    This is a declared horizon-bucket response hypothesis, not an inferred
    numeric duration. Conditional opposed branches retain their own triggers.
    """
    groups = {}
    for e in pool: groups.setdefault((e["source_id"], e["symbol"]), []).append(e)
    selected, excluded = [], 0
    for group in groups.values():
        if len({e["direction"] for e in group}) < 2 or all(e["reaction_type"] == "price_condition" for e in group):
            selected.extend(group); continue
        pattern = r"short[ -]term|near[ -]term|intraday|scalp" if hours <= 24 else r"higher timeframe|higher time frame|long[ -]term|longer[ -]term|months|weeks|year"
        preferred = [e for e in group if re.search(pattern, e.get("explicit_horizon_quote") or "", re.I)]
        chosen = preferred if preferred and len({e["direction"] for e in preferred}) == 1 else []
        selected.extend(chosen)
        excluded += len(group)-len(chosen)
    return selected, excluded


def exclusion(e):
    if e["visible_edited"]: return "visible_edited"
    if e.get("requires_unavailable_data"): return "requires_unavailable_confirmation"
    if e.get("exclusion_reason"): return e["exclusion_reason"]
    if e.get("confidence", 0) < .65: return "interpretation_confidence_below_065"
    if e["reaction_type"] == "risk_only": return "risk_warning_is_not_opposite_entry"
    if e["reaction_type"] == "invalidation_update": return "invalidated_prior_side_is_not_new_entry"
    return None


def eligible(e):
    return exclusion(e) is None


def plan(events):
    intervals = {}
    for e in events:
        if not eligible(e): continue
        start = utc(e["published_at"]).floor("5min") - pd.Timedelta(hours=1)
        end = utc(e["published_at"]).ceil("5min") + pd.Timedelta(hours=194)
        intervals.setdefault(e["symbol"], []).append((start, end))
    merged = []
    for symbol, ranges in sorted(intervals.items()):
        active = []
        for a, b in sorted(ranges):
            if active and a <= active[-1][1]: active[-1] = (active[-1][0], max(active[-1][1], b))
            else: active.append((a, b))
        merged.extend({"symbol": symbol, "start": str(a), "end": str(b)} for a, b in active)
    return merged


def fetch(max_requests=600):
    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env
    ensure_proxy_env(settings)
    events = all_events()
    ranges = plan(events)
    write(OUT / "download_plan.json", ranges)
    cache = OUT / "official_5m"
    cache.mkdir(exist_ok=True)
    calls, fetched = 0, 0
    with requests.Session() as session:
        session.headers["User-Agent"] = "crypto-trading-system-public-research/1.0"
        for i, window in enumerate(ranges):
            cursor, end = utc(window["start"]), utc(window["end"])
            while cursor <= end:
                finish = min(end, cursor + 1499 * STEP)
                filename = f"{window['symbol']}_{int(cursor.timestamp())}_{int(finish.timestamp())}.json"
                path = cache / filename
                if path.exists():
                    item = json.loads(path.read_text(encoding="utf-8"))
                    if item.get("status") == "ok": cursor = finish + STEP; continue
                if calls >= max_requests: raise RuntimeError("Bounded public request budget reached; resume from cached chunks")
                params = {"symbol": window["symbol"], "interval": "5m", "startTime": int(cursor.timestamp()*1000),
                          "endTime": int(finish.timestamp()*1000), "limit": 1500}
                item = {"endpoint": "https://fapi.binance.com/fapi/v1/klines", "params": params,
                        "requested_at": datetime.now(timezone.utc).isoformat(), "window": window}
                for attempt in range(3):
                    try:
                        calls += 1
                        response = session.get(item["endpoint"], params=params, timeout=(10, 30))
                        if response.status_code in (418, 429):
                            write(path, {**item, "status": "rate_limited", "http_status": response.status_code})
                            raise RuntimeError("Public endpoint rate limited; preserve cache and stop")
                        response.raise_for_status()
                        payload = response.json()
                        if not isinstance(payload, list): raise ValueError("Non-list kline response")
                        item.update(status="ok", rows=payload, response_sha256=hashlib.sha256(response.content).hexdigest())
                        write(path, item)
                        fetched += 1
                        break
                    except (requests.RequestException, ValueError) as exc:
                        if attempt == 2: write(path, {**item, "status": "error", "error_type": type(exc).__name__})
                        else: time.sleep(1 + attempt)
                cursor = finish + STEP
                time.sleep(.4)
            print(json.dumps({"window": i+1, "windows": len(ranges), "requests": calls, "new_chunks": fetched}), flush=True)


def fetch_funding():
    """Complete official funding pagination, rather than infer absent payments.

    Store query coverage separately from records. A <limit response certifies
    the requested historical interval; a full page only covers through its last
    timestamp and must continue. No cadence inference or missing-as-zero fill.
    """
    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env
    ensure_proxy_env(settings)
    cache = OUT / "official_funding"; cache.mkdir(exist_ok=True)
    ranges = plan(all_events()); calls = 0
    with requests.Session() as session:
        for i, w in enumerate(ranges):
            start, end = utc(w["start"])-pd.Timedelta(hours=12), utc(w["end"])+pd.Timedelta(hours=12)
            cursor, stop = int(start.timestamp()*1000), int(end.timestamp()*1000)
            while cursor <= stop:
                path = cache/f"{w['symbol']}_{cursor}_{stop}.json"
                item = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
                if item.get("status") != "ok":
                    if calls >= 300: raise RuntimeError("Funding public request bound reached; resume cached plan")
                    params = {"symbol": w["symbol"], "startTime": cursor, "endTime": stop, "limit": 1000}
                    item = {"endpoint": "https://fapi.binance.com/fapi/v1/fundingRate", "params": params,
                            "requested_at": datetime.now(timezone.utc).isoformat()}
                    for attempt in range(3):
                        try:
                            calls += 1
                            response = session.get(item["endpoint"], params=params, timeout=(10,30))
                            if response.status_code in (418,429): raise RuntimeError("Funding endpoint rate limited; stop")
                            response.raise_for_status(); rows = response.json()
                            assert isinstance(rows,list)
                            item.update(status="ok", rows=rows,
                                        response_sha256=hashlib.sha256(response.content).hexdigest())
                            write(path,item); break
                        except requests.RequestException as exc:
                            if attempt == 2: write(path,{**item,"status":"error","error_type":type(exc).__name__})
                            else: time.sleep(1+attempt)
                    time.sleep(.4)
                if item.get("status") != "ok": break
                rows = item["rows"]
                if len(rows) < 1000: break
                next_cursor = int(rows[-1]["fundingTime"])+1
                if next_cursor <= cursor: raise RuntimeError("Nonadvancing funding pagination")
                cursor = next_cursor
            print(json.dumps({"funding_window":i+1,"windows":len(ranges),"requests":calls}),flush=True)


class Market:
    def __init__(self):
        self.prices, self.funds, self.sources, self.fund_coverage = {}, {}, {}, {}

    def prices_for(self, symbol):
        if symbol in self.prices: return self.prices[symbol]
        rows = []
        for path in sorted((OUT / "official_5m").glob(f"{symbol}_*.json")):
            raw = path.read_bytes()
            item = json.loads(raw)
            if item.get("status") != "ok": continue
            assert item["params"]["symbol"] == symbol and item["params"]["interval"] == "5m"
            self.sources[str(path.relative_to(ROOT))] = {"sha256": hashlib.sha256(raw).hexdigest(), "rows": len(item["rows"])}
            for x in item["rows"]:
                rows.append([pd.Timestamp(x[0], unit="ms", tz="UTC"), *map(float, x[1:6])])
        frame = pd.DataFrame(rows, columns=["at", "open", "high", "low", "close", "volume"]).set_index("at")
        if not frame.empty:
            dup = frame.index.duplicated(keep=False)
            if dup.any(): assert (frame[dup].groupby(level=0).nunique() <= 1).all().all()
            frame = frame[~frame.index.duplicated()].sort_index()
        self.prices[symbol] = frame
        return frame

    def funding_for(self, symbol):
        if symbol in self.funds: return self.funds[symbol]
        assert SYMBOL.fullmatch(symbol)
        rows, coverage = [], []
        for path in sorted((OUT/"official_funding").glob(f"{symbol}_*.json")):
            raw = path.read_bytes()
            item = json.loads(raw)
            if item.get("status") != "ok": continue
            assert item["params"]["symbol"] == symbol
            data = item["rows"]
            a = pd.Timestamp(item["params"]["startTime"],unit="ms",tz="UTC")
            b = pd.Timestamp(item["params"]["endTime"] if len(data)<1000 else data[-1]["fundingTime"],unit="ms",tz="UTC")
            coverage.append((a,b))
            rows.extend([pd.Timestamp(x["fundingTime"],unit="ms",tz="UTC"),float(x["fundingRate"]),float(x["markPrice"])] for x in data)
            self.sources[str(path.relative_to(ROOT))] = {"sha256": hashlib.sha256(raw).hexdigest(), "rows": len(data),
                                                       "complete_interval":[str(a),str(b)]}
        f = pd.DataFrame(rows,columns=["at","funding_rate","mark_price"]).set_index("at").sort_index()
        if not f.empty: f = f[~f.index.duplicated(keep="last")]
        self.funds[symbol] = f
        self.fund_coverage[symbol] = sorted(coverage)
        return f

    def funding_coverage_ok(self,symbol,start,end):
        self.funding_for(symbol)
        cursor = start
        for a,b in self.fund_coverage[symbol]:
            if a > cursor: return False
            if b >= cursor: cursor = b+pd.Timedelta(milliseconds=1)
            if cursor > end: return True
        return False


def available(e, delay):
    return (utc(e["published_at"]) + pd.Timedelta(minutes=delay)).ceil("5min")


def trigger_entry(e, frame, delay, policy="cross"):
    start = available(e, delay)
    if e["reaction_type"] != "price_condition": return start, None
    if e.get("condition_type") not in ("close_above", "close_below") or not e.get("condition_level"):
        return None, "non_numeric_or_unmodeled_condition"
    end = start + pd.Timedelta(hours=24)
    level = e["condition_level"]
    quote = e.get("condition_quote") or e.get("evidence_quote") or ""
    hold = bool(re.search(r"hold(?:s|ing)? (?:above|below)|acceptance|sustain|stays? (?:above|below)",quote,re.I))
    def beyond(value): return value > level if e["condition_type"] == "close_above" else value < level
    if policy == "state":
        known = (utc(e["published_at"])+pd.Timedelta(minutes=delay)).floor("5min")-STEP
        required = [known-STEP,known] if hold else [known]
        if not all(at in frame.index for at in required): return None,"state_observation_gap"
        known_closes = [float(frame.loc[at,"close"]) for at in required]
        if not np.isfinite(known_closes).all() or min(known_closes) <= 0: return None,"invalid_trigger_close"
        if all(beyond(value) for value in known_closes):
            return (utc(e["published_at"])+pd.Timedelta(minutes=delay+1)).ceil("5min"),None
    for at in pd.date_range(start, end-STEP, freq=STEP):
        if at-STEP not in frame.index or at not in frame.index: return None, "trigger_window_gap"
        previous, current = float(frame.loc[at-STEP, "close"]), float(frame.loc[at, "close"])
        if not np.isfinite([previous,current]).all() or min(previous,current) <= 0: return None, "invalid_trigger_close"
        cross = previous <= level < current if e["condition_type"] == "close_above" else previous >= level > current
        if cross or (policy == "state" and beyond(current)):
            confirmed_at = at+STEP
            if hold:
                if at+STEP not in frame.index: return None,"trigger_confirmation_gap"
                if not beyond(float(frame.loc[at+STEP,"close"])): continue
                confirmed_at += STEP
            if confirmed_at > end: continue
            # Only data available before the cross is required. A later search-
            # window gap cannot retrospectively remove this already-known entry.
            return (confirmed_at + pd.Timedelta(minutes=1)).ceil("5min"), None
    return None, "numeric_cross_not_triggered_in_24h"


def funding_range(frame, start, end, price, side, complete_query=False):
    if not {"funding_rate", "mark_price"}.issubset(frame.columns): return None
    if frame.empty:
        return {"lower":0.,"upper":0.,"settlements":[],"ambiguous_boundary_settlements":0} if complete_query else None
    if not complete_query:
        before, after = frame.index[frame.index <= start], frame.index[frame.index >= end]
        if not len(before) or not len(after): return None
        bracket = frame.loc[before[-1]:after[0]]
        if bracket.index.to_series().diff().max() > pd.Timedelta(hours=12, seconds=1): return None
    f = frame[(frame.index > start-pd.Timedelta(seconds=1)) & (frame.index <= end+pd.Timedelta(seconds=1))]
    if not np.isfinite(f[["funding_rate", "mark_price"]].to_numpy()).all() or (f["mark_price"] <= 0).any(): return None
    certain, ambiguous = [], []
    for at, x in f.iterrows():
        s = {"at": str(at), "return": -side*float(x.funding_rate)*float(x.mark_price)/price}
        if start+pd.Timedelta(seconds=1) < at < end-pd.Timedelta(seconds=1): certain.append(s)
        else: ambiguous.append(s)
    fixed = sum(x["return"] for x in certain)
    lower = fixed + sum(min(0., x["return"]) for x in ambiguous)
    upper = fixed + sum(max(0., x["return"]) for x in ambiguous)
    return {"lower": lower, "upper": upper, "settlements": certain+[x for x in ambiguous if x["return"] < 0],
            "ambiguous_boundary_settlements": len(ambiguous)}


def evaluate(e, market, hours, delay, end_cap=None, side=None, condition_policy="cross"):
    side = e["direction"] if side is None else side
    row = {**e, "direction": side, "holding_hours": hours, "delay_minutes": delay}
    f = market.prices_for(e["symbol"])
    if f.empty: return {**row, "status": "missing_5m_symbol"}
    start, error = trigger_entry(e, f, delay,condition_policy)
    if error: return {**row, "status": error}
    planned = start + pd.Timedelta(hours=hours)
    end = min(planned, end_cap) if end_cap is not None else planned
    if end <= start: return {**row, "status": "update_before_entry"}
    row.update(entry_at=str(start), planned_exit_at=str(planned))
    wanted = pd.date_range(start, end, freq=STEP)
    path = f.reindex(wanted)
    if start not in f.index: return {**row, "status": "holding_window_gap"}
    p0 = float(path.iloc[0].open)
    if not np.isfinite(p0) or p0 <= 0 or path.iloc[0].volume <= 0: return {**row, "status": "no_entry_or_exit_prints"}
    risk = path.iloc[:-1]
    bad = (~np.isfinite(risk.to_numpy()).all(axis=1) | (risk[["open","high","low","close"]] <= 0).any(axis=1).to_numpy() | (risk.volume < 0).to_numpy())
    first_bad = int(np.flatnonzero(bad)[0]) if bad.any() else len(risk)
    hit = risk.high >= p0*1.99 if side < 0 else risk.low <= p0*.005
    first_hit = int(np.flatnonzero(hit.to_numpy())[0]) if hit.any() else len(risk)
    liquidated = first_hit < first_bad
    if liquidated:
        end = hit.index[first_hit] + STEP
        p1 = p0*(1.99 if side < 0 else .005)
    else:
        if first_bad < len(risk):
            status = "holding_window_gap" if risk.index[first_bad] not in f.index else "invalid_price_or_volume"
            return {**row, "status": status}
        if end not in f.index: return {**row, "status": "holding_window_gap"}
        p1 = float(path.iloc[-1].open)
        if not np.isfinite(p1) or p1 <= 0 or path.iloc[-1].volume <= 0: return {**row, "status": "no_entry_or_exit_prints"}
    funding_frame = market.funding_for(e["symbol"])
    if hasattr(market,"funding_coverage_ok") and not market.funding_coverage_ok(e["symbol"],start-pd.Timedelta(seconds=1),end+pd.Timedelta(seconds=1)):
        return {**row,"status":"official_funding_query_coverage_gap"}
    fund = funding_range(funding_frame, start, end, p0, side,complete_query=hasattr(market,"funding_coverage_ok"))
    if fund is None: return {**row, "status": "funding_uncovered"}
    gross = side*(p1/p0-1)
    net = -1. if liquidated else max(-1., gross-.002+fund["lower"])
    return {**row, "status": "ok", "exit_at": str(end), "entry_price": p0, "exit_price": p1,
            "gross_return": -1. if liquidated else gross, "funding_return": fund["lower"],
            "funding_upper": fund["upper"], "settlements": fund["settlements"],
            "ambiguous_funding_settlements": fund["ambiguous_boundary_settlements"],
            "round_trip_cost": .002, "net_return": net,
            "net_40bps": max(-1., net-.002), "net_100bps": max(-1., net-.008),
            "liquidated_1x_approx": liquidated,
            "until_update_exit": not liquidated and end_cap is not None and end == end_cap and end_cap < planned}


def mark(row, at, market):
    if at < utc(row["entry_at"]): return 0.
    if at >= utc(row["exit_at"]): return row["net_return"]
    price = market.prices_for(row["symbol"]).loc[at, "open"]
    fund = sum(x["return"] for x in row["settlements"] if utc(x["at"]) <= at)
    return max(-1., row["direction"]*(price/row["entry_price"]-1)-.001+fund)


def book(rows, market):
    selected, skipped = [], Counter()
    for r in sorted(rows, key=lambda r: (r.get("entry_at", r["published_at"]), r["event_id"])):
        if r["status"] != "ok": skipped[r["status"]] += 1; continue
        at = utc(r["entry_at"])
        active = [x for x in selected if utc(x["exit_at"]) > at]
        if any(x["symbol"] == r["symbol"] for x in active): skipped["overlap_same_symbol"] += 1; continue
        if len(active) >= 5: skipped["five_positions"] += 1; continue
        equity = 1 + .2*sum(mark(x, at, market) for x in selected)
        if equity-.2*len(active) < .2: skipped["insufficient_collateral"] += 1; continue
        selected.append(r)
    return selected, dict(skipped)


def stats(rows, field="net_return"):
    if not rows: return {"n": 0}
    v = np.array([r[field] for r in rows])
    pos, neg = v[v>0].sum(), -v[v<0].sum()
    return {"n": len(v), "mean": float(v.mean()), "median": float(np.median(v)), "win_rate": float((v>0).mean()),
            "pf": float(pos/neg) if neg else None, "portfolio_fixed20": float(.2*v.sum()),
            "without_best": float(np.delete(v, v.argmax()).mean()) if len(v)>1 else None,
            "week_block_95": week_bootstrap(rows, field),
            "liquidations_approx": sum(r["liquidated_1x_approx"] for r in rows),
            "active_months": len({r["entry_at"][:7] for r in rows})}


def curve_stats(rows, market):
    if not rows: return {}, []
    timeline = pd.date_range(min(utc(r["entry_at"]) for r in rows)-STEP,
                             max(utc(r["exit_at"]) for r in rows), freq=STEP)
    equity = np.ones(len(timeline))
    for r in rows:
        start, end = utc(r["entry_at"]), utc(r["exit_at"])
        i, j = timeline.searchsorted(start), timeline.searchsorted(end)
        active = timeline[i:j]
        prices = market.prices_for(r["symbol"]).loc[active, "open"].to_numpy()
        pnl = r["direction"]*(prices/r["entry_price"]-1)-.001
        for settlement in r["settlements"]:
            pnl[active.searchsorted(utc(settlement["at"])):] += settlement["return"]
        equity[i:j] += .2*np.maximum(-1., pnl)
        equity[j:] += .2*r["net_return"]
    assert abs(float(equity[-1]-1)-.2*sum(r["net_return"] for r in rows)) < 1e-8
    daily = pd.Series(equity, index=timeline).resample("1D").last()
    return {"max_5m_open_drawdown": float((equity/np.maximum.accumulate(equity)-1).min())}, [
        {"at": str(at), "equity": float(value)} for at,value in daily.items()]


def evaluate_all():
    events = all_events()
    market = Market()
    valid = [e for e in events if eligible(e)]
    pools = {"fresh_view": [e for e in valid if e["is_fresh_entry_view"] and e["reaction_type"] == "immediate_view"],
             "broad_directional": [e for e in valid if e["reaction_type"] in ("immediate_view", "context_only")],
             "numeric_cross": [e for e in valid if e["reaction_type"] == "price_condition"]}
    pools["until_opposite_update"] = pools["broad_directional"]
    pools["numeric_state"] = pools["numeric_cross"]
    pools["broad_plus_images"] = pools["broad_directional"] + [e for e in valid if e.get("evidence_type") == "public_chart_image"]
    # Visual entries are a separate pool, never silently mixed into caption-only
    # baselines even though their reaction type is an immediate first-leg view.
    for key in ("fresh_view", "broad_directional", "until_opposite_update"):
        pools[key] = [e for e in pools[key] if not e.get("evidence_type")]
    pools["broad_plus_images"] = pools["broad_directional"] + [e for e in valid if e.get("evidence_type") == "public_chart_image"]
    controls = [e for e in events if e["reaction_type"] == "invalidation_update" and not e["visible_edited"]
                and not e.get("requires_unavailable_data") and e.get("confidence", 0) >= .65
                and e.get("condition_level") is None and e.get("condition_type") is None]
    results, outcomes, admitted, reference_curves = [], [], [], []
    for family, pool in pools.items():
        for delay in DELAYS:
            for hours in HOLDS:
                scenario = f"{family}|d{delay}|h{hours}"
                active_pool, horizon_exclusions = horizon_events(pool, hours)
                rows = []
                for e in active_pool:
                    cap = None
                    if family == "until_opposite_update":
                        later = [available(x, delay) for x in active_pool if x["symbol"] == e["symbol"] and x["direction"] != e["direction"] and utc(x["published_at"]) > utc(e["published_at"])]
                        later += [available(x, delay) for x in controls if x["symbol"] == e["symbol"] and x["direction"] == e["direction"] and utc(x["published_at"]) > utc(e["published_at"])]
                        if later: cap = min(later)
                    row = evaluate(e, market, hours, delay, cap,condition_policy="state" if family=="numeric_state" else "cross")
                    phase = "later" if utc(e["published_at"]) >= CUTOFF else "earlier"
                    if phase == "earlier" and row.get("planned_exit_at") and utc(row["planned_exit_at"]) > CUTOFF: phase = "purged"
                    rows.append({**row, "phase": phase, "scenario": scenario})
                outcomes.extend(rows)
                for phase in ("earlier", "later", "all"):
                    phase_rows = [r for r in rows if (r["phase"] == phase if phase != "all" else True)]
                    chosen, skipped = book(phase_rows, market)
                    curve, daily = curve_stats(chosen, market)
                    if delay == 15 and hours == 4 and phase == "all":
                        reference_curves.extend({**point, "family": family, "scenario": scenario} for point in daily)
                    admitted.extend({**r, "analysis_phase": phase} for r in chosen)
                    baseline = []
                    for r in chosen:
                        # Identical entry/exit times; no new entry selection. For
                        # cross-trigger events keep original direction condition.
                        original = next(e for e in pool if e["event_id"] == r["event_id"])
                        inv = evaluate(original, market, hours, delay, utc(r["exit_at"]), -r["direction"],
                                       condition_policy="state" if family=="numeric_state" else "cross")
                        if inv.get("status") == "ok": baseline.append(inv)
                    results.append({"scenario": scenario, "family": family, "delay": delay, "hours": hours,
                                    "phase": phase, "text_events": len(phase_rows), "skipped": skipped,
                                    "opposed_horizon_events_excluded": horizon_exclusions,
                                    **stats(chosen), **curve, "stress40": stats(chosen, "net_40bps"),
                                    "stress100": stats(chosen, "net_100bps"), "inverse_same_times": stats(baseline)})
                print(scenario, flush=True)
    write(OUT / "response_results.json", {"summary": results, "pools": {k: len(v) for k,v in pools.items()},
                                           "price_sources": market.sources,
                                           "caveats": ["All108scenario variants are exploratory/correlated, not a parameter search with valid untouched test.",
                                                       "Information messages are not all fresh entries; overlapping repeated updates share positions.",
                                                       "Funding missing is unknown; complete5m windows required. No minute interpolation from1h bars.",
                                                       "Inverse baseline uses same event times and is not an independently feasible trading account.",
                                                       "Approximate collateral loss cap, not mark-price liquidation/insurance fee simulation."]})
    (OUT / "response_outcomes.jsonl").write_text("".join(json.dumps(r,ensure_ascii=False)+"\n" for r in outcomes), encoding="utf-8")
    (OUT / "admitted_trades.jsonl").write_text("".join(json.dumps(r,ensure_ascii=False)+"\n" for r in admitted), encoding="utf-8")
    pd.DataFrame(reference_curves).to_csv(OUT / "reference_equity.csv", index=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "merge", "fetch", "fetch_funding", "evaluate"])
    parser.add_argument("--max-requests", type=int, default=600)
    args = parser.parse_args()
    if args.action == "prepare": prepare()
    elif args.action == "merge": merge_labels()
    elif args.action == "fetch": fetch(args.max_requests)
    elif args.action == "fetch_funding": fetch_funding()
    else: evaluate_all()


if __name__ == "__main__": main()
