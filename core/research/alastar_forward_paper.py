"""Local v2 paper journal. No scheduler, AI provider, account or order access.

Only a label frozen before entry can produce an observed paper fill. Completed
trades stay in the journal even when their original message is later edited.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re

from core.news.alastar_observation import (
    _archive_lock, _atomic_bytes, _json_bytes, _load_runs, _utc,
)

STEP = timedelta(minutes=5)
HOLD = timedelta(hours=1)
FILL_WINDOW_SECONDS = 30  # operational capture guard, fixed before any paper fills
IMPLEMENTATION_RULES = {"version": "v2-recorder-20261022", "fill_capture_seconds": 30,
                        "recent_live_observations": 4, "maximum_poll_gap_seconds": 600,
                        "clock_max_abs_offset_seconds": 2, "clock_max_round_trip_seconds": 10,
                        # 10/22, before any activation: gate on the tightest of several public time reads.
                        "clock_samples": 5,
                        "collateral": "realised free isolated collateral; unrealised gains not reused",
                        "activation": "explicit paper-only registration before publication",
                        "primary": "price-only20bps on both sides; actual funding reported separately"}
SYMBOL = re.compile(r"[A-Z0-9\u4e00-\u9fff]{1,30}USDT\Z")
ENDPOINT = "https://fapi.binance.com/fapi/v1/klines"
FUNDING_ENDPOINT = "https://fapi.binance.com/fapi/v1/fundingRate"


def now_utc():
    return datetime.now(timezone.utc)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def ceil5(at):
    seconds = _utc(at).timestamp()
    return datetime.fromtimestamp(math.ceil(seconds / 300) * 300, timezone.utc)


def number(value, *, positive=False):
    value = float(value)
    if not math.isfinite(value) or (positive and value <= 0):
        raise ValueError("Nonfinite or nonpositive market value")
    return value


def bars_by_time(rows):
    """Keep gaps and reject inconsistent duplicates instead of interpolating."""
    out = {}
    for row in rows:
        if len(row) < 5:
            raise ValueError("Invalid official kline row")
        stamp = int(row[0])
        if stamp % 300000:
            raise ValueError("Kline is not aligned to5m")
        o, h, lo, c = [number(v, positive=True) for v in row[1:5]]
        if h < max(o, c) or lo > min(o, c) or h < lo:
            raise ValueError("Invalid OHLC geometry")
        if len(row) >= 9:
            volume, trades = number(row[5]), number(row[8])
            if min(volume, trades) < 0:
                raise ValueError("Negative volume/trade count")
            if volume == 0 or trades == 0:
                continue  # no printed price: never invent an executable open
        bar = (o, h, lo, c)
        if stamp in out and out[stamp] != bar:
            raise ValueError("Inconsistent duplicate kline")
        out[stamp] = bar
    return out


def price_return(bars, start, direction, cost_bps=20):
    """One hour, complete5m path; endpoint's future high/low are never read."""
    if type(direction) is not int or direction not in (-1, 1):
        raise ValueError("Direction must be -1 or1")
    start_ms = int(_utc(start).timestamp() * 1000)
    times = [start_ms + i * 300000 for i in range(13)]
    if start_ms not in bars:
        return None
    entry = bars[start_ms][0]
    for stamp in times[:-1]:
        if stamp not in bars:
            return None
        b = bars[stamp]
        if (b[1] >= entry * 1.99 if direction == -1 else b[2] <= entry * .005):
            return {"gross": None, "net_price_only": -1., "liquidated_1x_approx": True,
                    "entry_price": entry, "exit_price": None,
                    "liquidation_bar_start_ms": stamp}
    if times[-1] not in bars:
        return None
    gross = direction * (bars[times[-1]][0] / entry - 1)
    return {"gross": gross, "net_price_only": max(-1., gross - cost_bps / 10000),
            "liquidated_1x_approx": False, "entry_price": entry, "exit_price": bars[times[-1]][0]}


def pre_entry_null(rows, entry_at, direction, minimum_starts=2000):
    """Exact mean of every valid pre-entry start, not a random fitted sample."""
    entry = _utc(entry_at)
    if entry != ceil5(entry):
        raise ValueError("Entry must be a5m open")
    bars = bars_by_time(rows)
    first, last = entry - timedelta(days=30), entry - HOLD
    values = []
    at = first
    while at <= last:
        value = price_return(bars, at, direction)
        if value is not None:
            values.append(value["net_price_only"])
        at += STEP
    return {"status": "ok" if len(values) >= minimum_starts else "insufficient_pre_entry_history",
            "n": len(values), "mean_net_price_only": sum(values) / len(values) if values else None,
            "first_start_at": first.isoformat(), "last_start_at": last.isoformat(),
            "latest_price_at": entry.isoformat(), "funding_in_primary_comparison": False,
            "cost_bps": 20, "overlapping_null_starts_are_not_independent_trades": True}


def review_gate(events, activated_at, at):
    """Stop at the predeclared sample/time gates; never early-stop for success."""
    activated = _utc(activated_at)
    try:
        deadline = activated.replace(year=activated.year+2)
    except ValueError:
        deadline = activated.replace(year=activated.year+2, day=28)
    if _utc(at) >= deadline:
        return "maximum24months_reached"
    completed = sorted((e for e in events.values() if e["status"] == "completed"),
                       key=lambda e: (e["entry"]["entry_at"], e["event_id"]))
    opened = sum(e["status"] == "open" for e in events.values())
    n = len(completed)
    if n >= 66: return "confirmatory66trade_review_due"
    if n >= 40:
        excess = [e["settlement"].get("primary_excess_price_only") for e in completed[:40]]
        if any(v is None for v in excess): return "futility40trade_data_incomplete"
        if sum(excess)/40 <= 0: return "futility40trade_stop"
    if n < 40 and n+opened >= 40: return "await40trade_futility_review"
    if n+opened >= 66: return "await66trade_confirmatory_review"
    return None


class PaperJournal:
    def __init__(self, output, archive, protocol):
        self.output, self.archive, self.protocol_path = map(Path, (output, archive, protocol))
        self.protocol_raw = self.protocol_path.read_bytes()
        self.protocol = json.loads(self.protocol_raw)
        if self.protocol.get("protocol_id") != "alastar_fresh_call_1h_paper_v2":
            raise ValueError("Only the immutable v2 fresh-call protocol is supported")
        expected = {"holding_minutes": 60, "fixed_initial_notional_fraction": .2,
                    "maximum_concurrent_positions": 5, "round_trip_cost_bps": 20,
                    "stress_round_trip_cost_bps": [40, 80], "one_position_per_symbol": True,
                    "no_discretionary_exit": True}
        if (any(self.protocol["book"].get(k) != v for k, v in expected.items())
                or self.protocol["clock"]["maximum_entry_delay_after_publication_minutes"] != 30
                or self.protocol["control"]["minimum_null_starts"] != 2000):
            raise ValueError("Protocol parameters changed; implementation must be reviewed before use")
        self.protocol_sha = sha(self.protocol_raw)
        provider_path = Path(__file__).resolve().parents[2]/"scripts/observe_alastar_paper.py"
        self.implementation_sources = {"core": sha(Path(__file__).read_bytes()), "provider_cli": sha(provider_path.read_bytes())}
        self.implementation_sha = sha(_json_bytes({"sources": self.implementation_sources, "rules": IMPLEMENTATION_RULES}))

    def health(self):
        """Recent real observations, including outages; no inferred delivery SLA."""
        with _archive_lock(self.archive):
            count, messages, tail = _load_runs(self.archive, verify_full=True)
            times = [_utc(r["observed_at_utc"]) for p in sorted((self.archive / "runs").glob("*.json"))
                     if (r := json.loads(p.read_bytes()))["mode"] == "live_public_preview"]
        recent = times[-4:]
        gaps = [(b-a).total_seconds() for a, b in zip(recent, recent[1:])]
        age = (now_utc()-times[-1]).total_seconds() if times else None
        healthy = len(recent) >= 4 and age is not None and 0 <= age <= 600 and all(0 < g <= 600 for g in gaps)
        window = [t for t in times if now_utc()-timedelta(hours=6) <= t <= now_utc()]
        window_gaps = sorted((b-a).total_seconds() for a,b in zip(window, window[1:]))
        def quantile(q):
            if not window_gaps: return None
            p = (len(window_gaps)-1)*q
            lo, hi = math.floor(p), math.ceil(p)
            return window_gaps[lo]+(window_gaps[hi]-window_gaps[lo])*(p-lo)
        return {"checked_at_utc": now_utc().isoformat(), "healthy_for_new_freezes": healthy,
                "live_run_count": len(times), "archive_run_count": count, "archive_tail_sha256": tail,
                "last_receipt_age_seconds": age, "last_three_poll_gaps_seconds": gaps,
                "last6h": {"observation_count": len(window), "gap_median_seconds": quantile(.5),
                           "gap_p90_seconds": quantile(.9), "max_gap_seconds": max(window_gaps) if window_gaps else None,
                           "outage_count": sum(g>600 for g in window_gaps)},
                "live_first_message_count": sum(x["first"]["mode"] == "live_public_preview" for x in messages.values()),
                "timely_first_message_count": sum(x["first"]["prospective_eligible_at_first_observation"] for x in messages.values()),
                "requirement": "four latest live observations, each gap<=600s, latest no older than600s",
                "delivery_sla_or_subminute_guarantee": False}

    def _records(self):
        records, previous, previous_at = [], None, None
        for i, path in enumerate(sorted((self.output / "journal").glob("*.json")), 1):
            raw = path.read_bytes()
            r = json.loads(raw)
            at = _utc(r["recorded_at_utc"])
            if (path.name != f"{i:012d}.json" or r["sequence"] != i or r["previous_sha256"] != previous
                    or r["protocol_sha256"] != self.protocol_sha
                    or r.get("implementation_sha256") != self.implementation_sha
                    or (previous_at and at < previous_at)):
                raise RuntimeError("Paper journal hash, protocol or clock sequence is invalid")
            records.append(r)
            previous, previous_at = sha(raw), at
        checkpoint = self.output / "checkpoint.json"
        if checkpoint.exists():
            saved = json.loads(checkpoint.read_bytes())
            position = saved["sequence"]
            path = self.output / "journal" / f"{position:012d}.json"
            if position > len(records) or sha(path.read_bytes()) != saved["sha256"]:
                raise RuntimeError("Paper journal checkpoint hash is invalid")
        return records, previous

    def _append(self, kind, payload):
        records, previous = self._records()
        at = now_utc()
        if records and at < _utc(records[-1]["recorded_at_utc"]):
            raise RuntimeError("Local UTC clock moved backwards")
        r = {"sequence": len(records) + 1, "previous_sha256": previous,
             "protocol_sha256": self.protocol_sha, "implementation_sha256": self.implementation_sha,
             "recorded_at_utc": at.isoformat(),
             "kind": kind, "payload": payload}
        path = self.output / "journal" / f"{r['sequence']:012d}.json"
        if path.exists():
            raise RuntimeError("Refusing to overwrite a committed paper record")
        raw = _json_bytes(r)
        _atomic_bytes(path, raw)
        _atomic_bytes(self.output / "checkpoint.json", _json_bytes({"sequence": r["sequence"], "sha256": sha(raw)}))
        return r

    def activate(self, null_validation, clock_check):
        """Explicit paper registration only; CLI validates the linked audit files."""
        health = self.health()
        if not health["healthy_for_new_freezes"] or not null_validation.get("ok") or not clock_check.get("ok"):
            raise RuntimeError("Paper activation prerequisites are not satisfied")
        if not null_validation.get("audit_sha256"):
            raise ValueError("A verified official null audit is required")
        with _archive_lock(self.output):
            records, _ = self._state()
            if records:
                raise RuntimeError("Paper registration already exists; do not reset it")
            return self._append("activation", {"paper_only": True, "protocol_snapshot": self.protocol,
                                "implementation_rules": IMPLEMENTATION_RULES, "implementation_sources": self.implementation_sources,
                                "collector_health": health,
                                "null_validation": null_validation, "clock_check": clock_check})

    def _state(self):
        records, _ = self._records()
        events = {}
        for r in records:
            p = r["payload"]
            if r["kind"] == "freeze":
                for e in p["events"]:
                    events[e["event_id"]] = {**e, "freeze": p, "freeze_committed_at_utc": r["recorded_at_utc"], "status": e["status"]}
            elif r["kind"] in ("entry", "missed", "settlement"):
                event = events[p["event_id"]]
                event[r["kind"]] = p
                event["status"] = {"entry": "open", "missed": "skipped", "settlement": "completed"}[r["kind"]]
            elif r["kind"] == "comparison":
                events[p["event_id"]]["settlement"].update(p)
        return records, events

    def status(self):
        with _archive_lock(self.output):
            records, events = self._state()
            counts = {}
            for e in events.values():
                counts[e["status"]] = counts.get(e["status"], 0) + 1
            return {"protocol": self.protocol["protocol_id"], "journal_records": len(records),
                    "counts": counts, "events": list(events.values()),
                    "continuous_runner": False, "live_orders": False,
                    "activation_registered_at_utc": records[0]["recorded_at_utc"] if records and records[0]["kind"] == "activation" else None,
                    "implementation_rules": IMPLEMENTATION_RULES,
                    "primary_measured_trades": sum(e.get("settlement", {}).get("primary_excess_price_only") is not None for e in events.values()),
                    "review_gate": review_gate(events, records[0]["recorded_at_utc"], now_utc()) if records else "not_activated",
                    "fill_capture_window_seconds": FILL_WINDOW_SECONDS}

    def freeze(self, label):
        """Host timestamps only. A later revision cannot rewrite this decision."""
        if any(k in label for k in ("label_frozen_at_utc", "first_received_at_utc", "published_at_utc")):
            raise ValueError("Label files cannot supply or override archive/host timestamps")
        source = label["source_id"]
        for field in ("reviewer_or_model_version", "rule_version"):
            if not isinstance(label.get(field), str) or not label[field].strip():
                raise ValueError(f"Missing {field}")
        items = label.get("events")
        if not isinstance(items, list) or not 1 <= len(items) <= 10:
            raise ValueError("Supply1to10 fully reviewed event interpretations")
        directions, symbols = set(), set()
        for e in items:
            if type(e.get("direction")) is not int or e["direction"] not in (-1, 1) or not SYMBOL.fullmatch(e.get("symbol", "")):
                raise ValueError("Invalid direction or native Binance symbol")
            if e["symbol"] in symbols:
                raise ValueError("Duplicate symbol in one label")
            symbols.add(e["symbol"]); directions.add(e["direction"])
            number(e["confidence"])
            for field in ("is_fresh_entry_view", "requires_unavailable_confirmation", "opposed_directions_in_message"):
                if type(e.get(field)) is not bool:
                    raise ValueError(f"Explicit boolean required: {field}")
        health = self.health()
        with _archive_lock(self.archive):
            _, state, _ = _load_runs(self.archive, verify_full=True)
            if source not in state:
                raise ValueError("Source has no verified first receipt")
            item = state[source]
            first = item["first"]
            receipt_path = next((p for p in sorted((self.archive / "runs").glob("*.json"))
                                 if any(x["message_key"] == source for x in json.loads(p.read_bytes())["new_revisions"])), None)
            assert receipt_path is not None
            receipt_sha = sha(receipt_path.read_bytes())
            reasons = list(item.get("current_candidate_exclusion_reasons", first["exclusion_reasons"]))
            if len(item["revisions"]) > 1:
                reasons.append("revised_before_label_freeze")
            for e in items:
                if not e.get("original_evidence_quote") or e["original_evidence_quote"] not in first["text"]:
                    raise ValueError("Interpretation must quote the exact first received text")
            with _archive_lock(self.output):
                records, current_events = self._state()
                if not records or records[0]["kind"] != "activation":
                    raise RuntimeError("Paper protocol is not activated; do not freeze real observations yet")
                if any(r["kind"] == "freeze" and r["payload"]["source_id"] == source for r in records):
                    raise ValueError("Source already frozen; repeated or corrected labels cannot replace it")
                frozen = now_utc()
                block = review_gate(current_events, records[0]["recorded_at_utc"], frozen)
                if block: reasons.append(block)
                pub, receipt = _utc(first["published_at_utc"]), _utc(first["observed_at_utc"])
                if receipt > frozen or pub > receipt:
                    raise ValueError("Receipt/publication is ahead of the host clock")
                if pub < _utc(self.protocol["documented_at_utc"]):
                    reasons.append("predates_v2_protocol")
                if pub < _utc(records[0]["recorded_at_utc"]):
                    reasons.append("predates_paper_activation")
                if not first["prospective_eligible_at_first_observation"]:
                    reasons.append("not_a_timely_live_first_receipt")
                if not health["healthy_for_new_freezes"]:
                    reasons.append("collector_health_gate_failed")
                entry = ceil5(max(receipt, frozen) + timedelta(seconds=60))
                if entry > pub + timedelta(minutes=30):
                    reasons.append("skipped_entry_too_late")
                event_rows = []
                for index, e in enumerate(items):
                    excluded = list(dict.fromkeys(reasons))
                    if (e.get("reaction_type") != "immediate_view" or not e["is_fresh_entry_view"]
                            or e["requires_unavailable_confirmation"] or e.get("chart_only", False)
                            or e["opposed_directions_in_message"] or len(directions) > 1
                            or not 0.65 <= float(e["confidence"]) <= 1):
                        excluded.append("not_an_unambiguous_v2_fresh_call")
                    event_rows.append({**e, "event_id": f"{source}:{index}",
                                       "planned_entry_at": entry.isoformat(), "planned_exit_at": (entry + HOLD).isoformat(),
                                       "status": "skipped" if excluded else "planned", "exclusion_reasons": excluded})
                payload = {"source_id": source, "message_revision_sha256": first["revision_sha256"],
                           "text_sha256": first["text_sha256"], "original_text": first["text"],
                           "published_at_utc": pub.isoformat(), "first_received_at_utc": receipt.isoformat(),
                           "first_receipt_run_file": receipt_path.relative_to(self.archive).as_posix(),
                           "first_receipt_run_sha256": receipt_sha, "label_frozen_at_utc": frozen.isoformat(),
                           "reviewer_or_model_version": label["reviewer_or_model_version"], "rule_version": label["rule_version"],
                           "fill_capture_window_seconds": FILL_WINDOW_SECONDS, "events": event_rows,
                           "collector_health_snapshot": health,
                           "protocol_snapshot": self.protocol if not records else None}
                return self._append("freeze", payload)

    def tick(self, provider):
        """One bounded capture/settlement pass; provider supplies official evidence."""
        with _archive_lock(self.output):
            records, events = self._state()
            if not records or records[0]["kind"] != "activation":
                raise RuntimeError("Paper protocol is not activated")
            actions = []
            for key, e in sorted(events.items(), key=lambda pair: (pair[1]["planned_entry_at"], pair[0])):
                at, entry_at = now_utc(), _utc(e["planned_entry_at"])
                if e["status"] != "planned" or at < entry_at:
                    continue
                reason = None
                if at > entry_at + timedelta(seconds=FILL_WINDOW_SECONDS):
                    reason = "missed_prospective_entry_window"
                elif _utc(e["freeze_committed_at_utc"]) >= entry_at:
                    reason = "freeze_was_not_committed_before_entry"
                else:
                    current_records, current = self._state()
                    reason = review_gate(current, current_records[0]["recorded_at_utc"], at)
                    opened = [x for x in current.values() if x["status"] == "open"]
                    closed = [x for x in current.values() if x["status"] == "completed"]
                    free_cash = 1 + .2 * sum(x["settlement"]["net_with_funding"] for x in closed) - .2 * len(opened)
                    if reason: pass
                    elif any(x["symbol"] == e["symbol"] for x in opened): reason = "overlap_same_symbol"
                    elif len(opened) >= 5: reason = "five_positions"
                    elif free_cash + 1e-12 < .2: reason = "insufficient_realised_collateral"
                if reason:
                    actions.append(self._append("missed", {"event_id": key, "reason": reason}))
                    continue
                try:
                    rows, evidence = provider.klines(e["symbol"], entry_at, entry_at)
                    bars = bars_by_time(rows)
                    price = bars[int(entry_at.timestamp() * 1000)][0]
                    captured = now_utc()
                    if not entry_at <= captured <= entry_at + timedelta(seconds=FILL_WINDOW_SECONDS):
                        raise ValueError("Entry response arrived after the capture deadline")
                    actions.append(self._append("entry", {"event_id": key, "price": price,
                                   "entry_at": entry_at.isoformat(), "captured_at_utc": captured.isoformat(), "evidence": evidence}))
                except (ValueError, KeyError, OSError, RuntimeError):
                    # Keep a retryable evidence failure; a later tick past the
                    # deadline records the missed fill, never a historical fill.
                    actions.append(self._append("entry_unavailable", {"event_id": key}))
            _, events = self._state()
            for key, e in events.items():
                exit_at = _utc(e["planned_exit_at"])
                if e["status"] != "open" or now_utc() < exit_at + STEP:
                    continue
                entry_at = _utc(e["planned_entry_at"])
                try:
                    rows, evidence = provider.klines(e["symbol"], entry_at, exit_at)
                    bars = bars_by_time(rows)
                    price = price_return(bars, entry_at, e["direction"])
                    if price is None or price["entry_price"] != e["entry"]["price"]:
                        raise ValueError("Missing path or entry evidence changed")
                    funding, funding_evidence = provider.funding(e["symbol"], entry_at, exit_at)
                    if funding_evidence.get("complete_query_coverage") is not True:
                        raise ValueError("Funding coverage is not proven complete")
                    fund = 0.0
                    seen_funding = set()
                    for f in funding:
                        stamp = int(f["fundingTime"])
                        if (stamp in seen_funding or f["symbol"] != e["symbol"]
                                or not int(entry_at.timestamp()*1000) <= stamp <= int(exit_at.timestamp()*1000)):
                            raise ValueError("Funding row outside query interval")
                        seen_funding.add(stamp)
                        liquidation_start = price.get("liquidation_bar_start_ms")
                        if liquidation_start is not None and stamp >= liquidation_start + 300000:
                            continue
                        contribution = -e["direction"] * number(f["fundingRate"]) * number(f["markPrice"], positive=True) / price["entry_price"]
                        # At either open boundary eligibility is ambiguous;
                        # include only the adverse payment in the lower bound.
                        boundary = min(abs(stamp-int(entry_at.timestamp()*1000)), abs(stamp-int(exit_at.timestamp()*1000))) <= 1000
                        boundary = boundary or (liquidation_start is not None and stamp >= liquidation_start)
                        fund += min(0., contribution) if boundary else contribution
                    net = -1. if price["liquidated_1x_approx"] else max(-1., price["net_price_only"] + fund)
                    try:
                        null_rows, null_evidence = provider.klines(e["symbol"], entry_at - timedelta(days=30), entry_at)
                        null = pre_entry_null(null_rows, entry_at, e["direction"])
                    except (ValueError, KeyError, OSError, RuntimeError) as exc:
                        null, null_evidence = {"status": "unavailable", "reason": type(exc).__name__}, []
                    excess = price["net_price_only"] - null["mean_net_price_only"] if null["status"] == "ok" else None
                    actions.append(self._append("settlement", {"event_id": key, **price,
                                   "funding_return_lower_bound": fund, "net_with_funding": net,
                                   "net_price_only_40bps": max(-1., price["net_price_only"] - .002),
                                   "net_price_only_80bps": max(-1., price["net_price_only"] - .006),
                                   "primary_excess_price_only": excess, "primary_null": null,
                                   "evidence": evidence, "funding_evidence": funding_evidence, "null_evidence": null_evidence}))
                except (ValueError, KeyError, OSError, RuntimeError):
                    actions.append(self._append("settlement_unavailable", {"event_id": key}))
            # A missing counterfactual must not keep a known closed position's
            # collateral tied up. Retry its comparison separately, never as0.
            _, events = self._state()
            for key, e in events.items():
                if (e["status"] != "completed" or e["settlement"]["primary_null"]["status"] != "unavailable"
                        or any(r["kind"] == "settlement" and r["payload"]["event_id"] == key for r in actions)):
                    continue
                try:
                    entry_at = _utc(e["planned_entry_at"])
                    rows, evidence = provider.klines(e["symbol"], entry_at-timedelta(days=30), entry_at)
                    null = pre_entry_null(rows, entry_at, e["direction"])
                    excess = e["settlement"]["net_price_only"]-null["mean_net_price_only"] if null["status"] == "ok" else None
                    actions.append(self._append("comparison", {"event_id": key, "primary_null": null,
                                   "primary_excess_price_only": excess, "null_evidence": evidence}))
                except (ValueError, KeyError, OSError, RuntimeError):
                    actions.append(self._append("comparison_unavailable", {"event_id": key}))
            return actions
