"""Unified research-tracker adapter for the explicit paper-only Alastar v2 book.

The fast worker monitors timely receipts and consumes reviewed labels. It never
guesses a trade from a news keyword and never calls an AI provider or order API.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path

from core.news.alastar_observation import _archive_lock, _atomic_bytes, _json_bytes, _utc
from core.research.alastar_forward_paper import PaperJournal, ceil5, now_utc, sha
from scripts.observe_alastar_paper import DEFAULT_OUTPUT, DEFAULT_PAPER, PROTOCOL, OfficialEvidence

TRACKER_ID = "alastar_fresh_call_1h_v2"
POLL_SECONDS = 5
HISTORY_RETRY_SECONDS = 60


def default_journal():
    return PaperJournal(DEFAULT_PAPER, DEFAULT_OUTPUT, PROTOCOL)


def read_runtime(output):
    path = Path(output)/"tracker_runtime.json"
    return json.loads(path.read_bytes()) if path.exists() else {"observations": {}, "processed_labels": {}, "errors": []}


class _GuardedProvider:
    def __init__(self, provider, allow_entries):
        self.provider, self.allow_entries = provider, allow_entries

    def klines(self, symbol, start, end):
        if start == end and not self.allow_entries:
            raise RuntimeError("Fresh clock check failed; no paper entry")
        return self.provider.klines(symbol, start, end)

    def funding(self, *args):
        return self.provider.funding(*args)


class Tracker:
    def __init__(self, journal=None, provider_factory=None, *, managed=False):
        self.journal = journal or default_journal()
        self.output = self.journal.output
        self.provider_factory = provider_factory or (lambda: OfficialEvidence(self.output))
        self.managed = managed

    def _save(self, runtime):
        runtime["errors"] = runtime.get("errors", [])[-20:]
        _atomic_bytes(self.output/"tracker_runtime.json", _json_bytes(runtime))

    def tick(self):
        with _archive_lock(self.output/"tracker_pass"):
            runtime = read_runtime(self.output)
            runtime.update(last_started_at_utc=now_utc().isoformat(), in_progress=True)
            self._save(runtime)
            state = self.journal.status()
            activation = state["activation_registered_at_utc"]
            if activation:
                self._observe_queue(runtime, state)
                self._consume_labels(runtime)
                state = self.journal.status()
                for e in state["events"]:
                    source = e["freeze"]["source_id"]
                    if source in runtime["observations"]: runtime["observations"][source]["status"] = "label_frozen"
                at = now_utc()
                planned = [e for e in state["events"] if e["status"] == "planned" and _utc(e["planned_entry_at"]) <= at]
                entry_due = any(at <= _utc(e["planned_entry_at"])+timedelta(seconds=30) for e in planned)
                history_due = any((e["status"] == "open" and at >= _utc(e["planned_exit_at"])+timedelta(minutes=5))
                                  or (e["status"] == "completed" and e["settlement"]["primary_null"]["status"] == "unavailable")
                                  for e in state["events"])
                last_history = runtime.get("last_history_attempt_at_utc")
                history_due = history_due and (not last_history or (at-_utc(last_history)).total_seconds() >= HISTORY_RETRY_SECONDS)
                if planned or history_due:
                    provider = self.provider_factory()
                    clock_ok = False
                    if entry_due:
                        try:
                            clock = provider.clock_check()
                            runtime["last_clock_check"] = clock
                            clock_ok = clock["ok"]
                            _atomic_bytes(self.output/"clock_check.json", _json_bytes(clock))
                        except Exception as exc:
                            runtime["errors"].append({"at": now_utc().isoformat(), "stage": "clock", "type": type(exc).__name__})
                    if history_due:
                        runtime["last_history_attempt_at_utc"] = now_utc().isoformat()
                    actions = self.journal.tick(_GuardedProvider(provider, clock_ok), settlement_enabled=history_due)
                    runtime["last_actions"] = [{"kind": r["kind"], "event_id": r["payload"].get("event_id")} for r in actions]
            runtime.update(last_tick_at_utc=now_utc().isoformat(), in_progress=False, activated=bool(activation),
                           continuous_runner=self.managed, process_id=os.getpid(), live_orders=False, last_pass_error=None)
            self._save(runtime)
            return runtime

    def _observe_queue(self, runtime, state):
        path = self.journal.archive/"candidate_review_queue.json"
        queue = json.loads(path.read_bytes()) if path.exists() else {"candidates": []}
        candidates = queue["candidates"]
        if not isinstance(candidates, list) or len(candidates) > 5000:
            raise ValueError("Invalid or unbounded observation queue")
        frozen = {e["freeze"]["source_id"] for e in state["events"]}
        active = _utc(state["activation_registered_at_utc"])
        seen = set()
        for c in candidates:
            if _utc(c["published_at_utc"]) < active: continue
            source = c["message_key"]
            seen.add(source)
            item = runtime["observations"].setdefault(source, {
                "source_id": source, "url": c["url"], "revision_sha256": c["revision_sha256"],
                "published_at_utc": c["published_at_utc"], "first_received_at_utc": c["first_observed_at_utc"],
                "first_seen_by_tracker_at_utc": now_utc().isoformat()})
            expired = ceil5(now_utc()+timedelta(seconds=60)) > _utc(c["published_at_utc"])+timedelta(minutes=30)
            item["status"] = "label_frozen" if source in frozen else "expired_without_interpretation" if expired else "awaiting_interpretation"
        for source, item in runtime["observations"].items():
            if source in frozen: item["status"] = "label_frozen"
            elif source not in seen and item["status"] == "awaiting_interpretation":
                item["status"] = "removed_from_current_review_queue"
        runtime["archive_run_file"] = queue.get("last_run_file")
        runtime["archive_run_sha256"] = queue.get("last_run_sha256")

    def _consume_labels(self, runtime):
        inbox = self.output/"labels_inbox"
        handled = 0
        for path in sorted(inbox.glob("*.json")):
            if path.stat().st_size > 1024*1024:
                continue
            raw = path.read_bytes()
            key = sha(raw)
            if key in runtime["processed_labels"]: continue
            if handled >= 10: break
            handled += 1
            evidence = self.output/"evidence"/f"label_{key}.json"
            if not evidence.exists(): _atomic_bytes(evidence, raw)
            result = {"file": path.name, "input_sha256": key, "observed_at_utc": now_utc().isoformat()}
            try:
                record = self.journal.freeze(json.loads(raw))
                result.update(status="frozen", journal_sequence=record["sequence"])
            except (ValueError, KeyError, OSError, RuntimeError) as exc:
                result.update(status="rejected", reason=str(exc)[:180], type=type(exc).__name__)
            runtime["processed_labels"][key] = result


def summary(journal=None):
    journal = journal or default_journal()
    state, runtime = journal.status(), read_runtime(journal.output)
    activated = state["activation_registered_at_utc"]
    stamp = runtime.get("last_tick_at_utc")
    running = bool(runtime.get("continuous_runner") and not runtime.get("last_pass_error")
                   and stamp and 0 <= (now_utc()-_utc(stamp)).total_seconds() <= 30)
    completed = [e for e in state["events"] if e["status"] == "completed"]
    measured = [e for e in completed if e["settlement"]["primary_excess_price_only"] is not None]
    excess = [e["settlement"]["primary_excess_price_only"]*100 for e in measured]
    actual = [e["settlement"]["net_with_funding"]*100 for e in completed]
    observations = runtime.get("observations", {}).values()
    pending = sum(x["status"] == "awaiting_interpretation" for x in observations)
    expired = sum(x["status"] == "expired_without_interpretation" for x in observations)
    gate = state["review_gate"]
    retired = gate in ("futility40trade_stop", "maximum24months_reached")
    label = ("未激活" if not activated else "停止新观察" if retired else "待协议复核" if gate else "积累新样本")
    status_label = ("未激活" if not activated else "运行异常" if runtime.get("last_pass_error") else
                    "运行心跳待恢复" if not running else "待语义复核" if pending else
                    "运行中 · 持仓待结算" if state["counts"].get("open") else "运行中 · 等待新帖")
    reason = f"固定1小时文字首次喊单；同币前30天超额为主统计；40笔无效停止、66笔集中检验。待解释{pending}，未解释超时{expired}。"
    if gate: reason += f" 当前门槛：{gate}。"
    return {"started_at": activated, "updated_at": stamp or activated,
            "retirement": {"n": len(measured), "min_n": 66, "mean_pct": sum(excess)/len(excess) if excess else None,
                           "ci90_pct": None, "verdict": "retire" if retired else "review_due" if gate and activated else "collecting",
                           "label": label, "reason": reason, "clusters": None},
            "forward_completed": len(completed), "forward_open": state["counts"].get("open", 0),
            "forward_waiting": state["counts"].get("planned", 0),
            "forward_late": expired+sum("skipped_entry_too_late" in e.get("exclusion_reasons", [])
                                        or e.get("missed", {}).get("reason") == "missed_prospective_entry_window" for e in state["events"]),
            "actual_mean_pct": sum(actual)/len(actual) if actual else None,
            "tracker_status_label": status_label, "runner_active": running, "pending_review": pending,
            "completed_without_primary": len(completed)-len(measured), "review_gate": gate,
            "observation_count": len(runtime.get("observations", {})), "continuous_runner": running,
            "runner_interval_seconds": POLL_SECONDS, "semantic_review_required": True,
            "last_error": (runtime.get("errors") or [None])[-1], "paper_only": True}


def detail():
    journal = default_journal()
    state, runtime = journal.status(), read_runtime(journal.output)
    return {"summary": summary(journal), "events": state["events"][-100:],
            "observations": list(runtime.get("observations", {}).values())[-100:],
            "labels": list(runtime.get("processed_labels", {}).values())[-30:],
            "activation": state["activation_registered_at_utc"]}


async def worker(stop_event, heartbeat=None):
    tracker = Tracker(managed=True)
    with _archive_lock(tracker.output/"tracker_owner"):
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.to_thread(tracker.tick)
                    if heartbeat: heartbeat(True)
                except Exception as exc:
                    runtime = read_runtime(tracker.output)
                    runtime.setdefault("errors", []).append({"at": now_utc().isoformat(), "stage": "worker", "type": type(exc).__name__, "reason": str(exc)[:180]})
                    runtime.update(last_error_at_utc=now_utc().isoformat(), in_progress=False, last_pass_error=type(exc).__name__)
                    tracker._save(runtime)
                    if heartbeat: heartbeat(False)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass
        finally:
            runtime = read_runtime(tracker.output)
            runtime.update(continuous_runner=False, stopped_at_utc=now_utc().isoformat())
            tracker._save(runtime)
