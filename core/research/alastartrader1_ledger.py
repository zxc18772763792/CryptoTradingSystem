"""Forward paper ledger alastartrader1_as_written_v1: @alastartrader1 calls executed as written. Records only.

@alastartrader1 is not Alastar: the 2026-10-22 review found it impersonating him (the official profile's DM
is @alastartrader), selling VIP and "recovery" deposits through @Albanwatts, and its 10/30 BEAMX call went
out in the same second as its "TP3 HIT" post, after price had passed every target. This ledger asks the
plain question a follower would: what happens when each structured call (``$SYM/USDT — LONG | 50x``,
``ENTRY``, ``TP``, ``SL``) is followed from its first public receipt?

The news collector archives the public preview t.me/s/alastartrader1. Each first receipt after activation
that the frozen kol-bracket rules (``core.research.kol_bracket_engine``) read as a full bracket is executed
on official Binance klines with the HUB ledger's execution (``core.research.hub_runup_ledger.simulate``):
the next 1-minute open after the freeze, limit legs as written (a leg already through the market fills at
the open), every target an equal slice, the written stop, entries cancelled once TP1 or the stop trades
first, 72 h to fill and 14 days at most; unlevered, with the written leverage's liquidation path recorded
alongside. The control re-runs the same relative bracket on the same coin over the month after entry.

Journal records are hash-chained and pin this module, the protocol, the bracket rules and the HUB module
it executes with; a change to any of them stops the journal. Message texts stay in the collector archive.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from core.news.alastar_observation import _load_runs, archive_dir
from core.research import hub_runup_ledger as hub
from core.research import kol_bracket_engine as rules
from core.research.hub_runup_ledger import atomic_write, dumps, naive, now_utc, sha, utc

ROOT = Path(__file__).resolve().parents[2]
TRACKER_ID = PROTOCOL_ID = "alastartrader1_as_written_v1"
CHANNEL = "alastartrader1"
OUTPUT = ROOT / "data/research/alastartrader1_as_written_v1"
PROTOCOL = ROOT / "config/research_protocols/alastartrader1_as_written_v1.json"
PINNED = {"protocol": PROTOCOL, "ledger": Path(__file__), "rules": Path(rules.__file__), "execution": Path(hub.__file__)}

ELIGIBLE = ("full_bracket", "targets_without_stop")
IGNORED_COLLECTOR_REASONS = ("visible_edit",)
MAX_RECEIPT_DELAY = timedelta(seconds=600)
MAX_FREEZE_DELAY = timedelta(seconds=600)
CLOCK_EVERY, CLOCK_MAX_AGE = timedelta(minutes=5), timedelta(minutes=10)
UNIVERSE_MAX_AGE, UNIVERSE_FALLBACK_MAX_AGE = timedelta(hours=6), timedelta(days=7)
SETTLE_EVERY = timedelta(minutes=10)
CONTROL_AFTER = timedelta(days=45)
REVIEW_N, MAX_MONTHS = 60, 12
POLL_SECONDS = 30
STATE_PATH = ROOT / "runtime/alastartrader1_ledger.json"


def pins() -> Dict[str, str]:
    return {name: sha(path.read_bytes()) for name, path in PINNED.items()}


class Journal:
    """Append-only, hash-chained JSONL (same layout as the HUB ledger); state is rebuilt from the records."""

    def __init__(self, output: Path = OUTPUT):
        self.output = Path(output)
        self.path = self.output / "journal.jsonl"

    def records(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        out, prev = [], None
        for line in self.path.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            body = {k: v for k, v in rec.items() if k != "hash"}
            if rec["prev"] != prev or rec["hash"] != sha(dumps(body)):
                raise RuntimeError(f"journal chain broken at seq {rec.get('seq')}")
            out.append(rec)
            prev = rec["hash"]
        return out

    def append(self, kind: str, data: Dict[str, Any]) -> Dict[str, Any]:
        recs = self.records()
        body = {"seq": len(recs), "at": now_utc().isoformat(), "kind": kind, "pins": pins(), "data": data,
                "prev": recs[-1]["hash"] if recs else None}
        rec = {**body, "hash": sha(dumps(body))}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("ab") as f:
            f.write(dumps(rec) + b"\n")
        return rec


def state(journal: Journal) -> Dict[str, Any]:
    recs = journal.records()
    st: Dict[str, Any] = {"activation": None, "events": {}, "drift": None, "last_seq": len(recs) - 1}
    for rec in recs:
        d = rec["data"]
        if rec["kind"] == "activation":
            st["activation"] = {**d, "at": rec["at"], "pins": rec["pins"]}
        elif rec["kind"] in ("decision", "outcome", "control"):
            st["events"].setdefault(str(d["message_id"]), {})[rec["kind"]] = d
    if st["activation"] and st["activation"]["pins"] != pins():
        st["drift"] = {"activated": st["activation"]["pins"], "current": pins()}
    return st


# ------------------------------------------------------------------------------------------- signal rules
def parse(text: str) -> Dict[str, Any]:
    """A call states its side in the first lines and carries targets; recaps ("TP3 HIT") and analyses do not."""
    text = text or ""
    head = " ".join(text.split("\n")[:3])
    long_, short = bool(re.search(r"\bLONG\b", head)), bool(re.search(r"\bSHORT\b", head))
    if long_ == short or not re.search(r"\bTP\b|target", text, re.I):
        return {"signal": False}
    symbol = rules.sym_from(head) or rules.sym_from(text)
    if not symbol:
        return {"signal": False}
    sig = rules.Signal(CHANNEL, 0, "", None, symbol, 1 if long_ else -1, "unknown", text=text)
    rules.generic_body(text.split("\n"), sig)
    sig.leverage = rules.leverage_of(head)
    out = {k: getattr(sig, k) for k in ("symbol", "side", "entry_type", "legs", "targets", "stop", "stop_stated_none",
                                         "leverage", "open_ended", "be_after_tp1")}
    out.update(signal=True, completeness=rules.classify(sig))
    out["eligible"] = out["entry_type"] in ("limit", "market") and out["completeness"] in ELIGIBLE
    return out


def overlaps(st: Dict[str, Any], native: str, entry: datetime, cache: hub.BarCache) -> bool:
    """One position per coin: an earlier entered call that has filled and not yet exited blocks a new one."""
    for ev in st["events"].values():
        d = ev.get("decision")
        if not d or d.get("native") != native or not d.get("entered") or utc(d["entry_at"]) >= entry:
            continue
        out = ev.get("outcome") or hub.simulate(d, cache, now_utc())
        if out.get("first_fill") and (out.get("exit_time") is None or naive(entry) < pd.Timestamp(out["exit_time"])):
            return True
        if not out.get("first_fill") and not out.get("final") and out.get("status") not in ("not_traded",):
            return True  # still resting: its legs could fill
    return False


# ---------------------------------------------------------------------------------------------- the ledger
class Ledger:
    def __init__(self, output: Path = OUTPUT, archive: Optional[Path] = None):
        self.output = Path(output)
        self.archive = Path(archive) if archive is not None else archive_dir(CHANNEL)
        self.journal = Journal(self.output)
        self.runtime_path = self.output / "runtime.json"

    def runtime(self) -> Dict[str, Any]:
        return json.loads(self.runtime_path.read_text(encoding="utf-8")) if self.runtime_path.exists() else {}

    def save_runtime(self, rt: Dict[str, Any]) -> None:
        atomic_write(self.runtime_path, json.dumps(rt, indent=1, default=str).encode("utf-8"))

    def messages(self) -> Dict[str, Dict[str, Any]]:
        return _load_runs(self.archive, channel=CHANNEL)[1]

    def activate(self, market: Optional[hub.Market] = None) -> Dict[str, Any]:
        if self.journal.records():
            raise RuntimeError("journal already exists; a new protocol needs a new directory")
        self.output.mkdir(parents=True, exist_ok=True)  # the evidence log lives here
        market = market or hub.Market(self.output)
        clock = market.clock(5)
        if not clock["ok"]:
            raise RuntimeError(f"clock check failed: {clock}")
        universe = market.universe()
        latest = max((int(m["first"]["message_id"]) for m in self.messages().values()), default=None)
        rec = self.journal.append("activation", {"protocol": PROTOCOL_ID, "clock": clock, "baseline_message_id": latest,
                                                 "universe_size": len(universe["symbols"]), "paper_only": True})
        self.save_runtime({"clock": clock, "universe": universe, "activated_at": rec["at"]})
        return rec

    def refresh(self, rt: Dict[str, Any], market: hub.Market, report: Dict[str, Any]) -> None:
        now = now_utc()
        if not rt.get("clock") or now - utc(rt["clock"]["checked_at"]) >= CLOCK_EVERY:
            try:
                rt["clock"] = market.clock(3)
            except Exception as exc:  # noqa: BLE001 - the last good reading stays valid for CLOCK_MAX_AGE
                report["clock_error"] = type(exc).__name__
        if not rt.get("universe") or now - utc(rt["universe"]["fetched_at"]) >= UNIVERSE_MAX_AGE:
            try:
                rt["universe"] = market.universe()
            except Exception as exc:  # noqa: BLE001 - the previous list stays usable for 7 days
                report["universe_error"] = type(exc).__name__

    def cycle(self, market: Optional[hub.Market] = None) -> Dict[str, Any]:
        st = state(self.journal)
        if not st["activation"]:
            return {"outcome": "not_activated"}
        if st["drift"]:
            return {"outcome": "drift_stopped", "drift": st["drift"]}
        rt, report = self.runtime(), {"outcome": "ok"}
        market = market or hub.Market(self.output)
        self.refresh(rt, market, report)
        report["decided"] = self.decide(rt, market)
        if not rt.get("settled_at") or now_utc() - utc(rt["settled_at"]) >= SETTLE_EVERY:
            report["settled"] = self.settle(market)
            rt["settled_at"] = now_utc().isoformat()
        self.save_runtime(rt)
        return report

    def decide(self, rt: Dict[str, Any], market: hub.Market) -> int:
        st = state(self.journal)
        activated = utc(st["activation"]["at"])
        cache = hub.BarCache(market, self.output)
        decided = 0
        for key, item in sorted(self.messages().items(), key=lambda kv: int(kv[1]["first"]["message_id"])):
            first = item["first"]
            mid = int(first["message_id"])
            if str(mid) in st["events"] or not first.get("published_at_utc") or utc(first["published_at_utc"]) < activated:
                continue
            now = now_utc()
            published, received = utc(first["published_at_utc"]), utc(first["observed_at_utc"])
            parsed = parse(first.get("text") or "")
            base = {"message_id": mid, "source_id": key, "freeze_at": now.isoformat(), "published_at": published.isoformat(),
                    "first_received_at": received.isoformat(), "delay_s": (received - published).total_seconds(),
                    "text_sha256": first.get("revision_sha256"), "parsed": parsed, "entered": False}
            collector = [r for r in first.get("exclusion_reasons") or [] if r not in IGNORED_COLLECTOR_REASONS]
            clock = rt.get("clock") or {}
            clock_ok = bool(clock.get("ok")) and abs(received - utc(clock.get("checked_at") or "2000-01-01")) <= CLOCK_MAX_AGE
            reason = None
            if not parsed.get("signal"):
                reason = "not_a_signal"
            elif not parsed.get("eligible"):
                reason = f"ineligible_{parsed.get('entry_type')}_{parsed.get('completeness')}"
            elif first.get("mode") != "live_public_preview" or collector:
                reason = "collector_" + "_".join(collector or ["not_live"])
            elif received - published > MAX_RECEIPT_DELAY:
                reason = "late_receipt"
            elif not clock_ok:
                reason = "clock_unverified"
            elif now - received > MAX_FREEZE_DELAY:
                reason = "freeze_too_late"
            elif now > activated + timedelta(days=30.44 * MAX_MONTHS):
                reason = "maximum_duration"
            if reason is None:
                universe = rt.get("universe") or {}
                fresh = universe and now - utc(universe["fetched_at"]) <= UNIVERSE_FALLBACK_MAX_AGE
                native, mult = hub.resolve(parsed["symbol"], universe.get("symbols", []) if fresh else [])
                if not fresh:
                    reason = "universe_unavailable"
                elif native is None:
                    reason = "no_trading_perpetual"
            if reason:
                self.journal.append("decision", {**base, "reason": reason})
                decided += 1
                continue
            entry = datetime.fromtimestamp(math.ceil(now.timestamp() / 60) * 60, timezone.utc)
            dec = {**base, "native": native, "mult": mult, "entry_at": entry.isoformat()}
            if overlaps(st, native, entry, cache):
                dec["reason"] = "skipped_overlap"
            else:
                dec["entered"] = True
            self.journal.append("decision", dec)
            decided += 1
            st = state(self.journal)
        return decided

    def settle(self, market: hub.Market) -> Dict[str, int]:
        st = state(self.journal)
        cache = hub.BarCache(market, self.output)
        counts = {"outcomes": 0, "controls": 0, "errors": 0}
        now = now_utc()
        for ev in st["events"].values():
            d = ev.get("decision")
            if not d or not d.get("entered"):
                continue
            try:
                if "outcome" not in ev:
                    r = hub.simulate(d, cache, now - timedelta(minutes=2))
                    if r.get("status") == "not_traded" or r.get("final"):
                        if r.get("first_fill"):
                            fund = market.funding(d["native"], utc(d["entry_at"]))
                            series = pd.Series([float(x["fundingRate"]) for x in fund["rows"]],
                                               index=pd.to_datetime([int(x["fundingTime"]) for x in fund["rows"]], unit="ms").floor("min"))
                            withf = hub.simulate(d, cache, now - timedelta(minutes=2), funding=series) if fund["complete"] else None
                            lev = d["parsed"].get("leverage")
                            levered = hub.simulate(d, cache, now - timedelta(minutes=2), leverage=lev) if lev else None
                            r.update(ret_with_funding=withf["ret_on_filled"] if withf else None, funding_complete=fund["complete"],
                                     lev={"leverage": lev, "exit_reason": levered.get("exit_reason"),
                                          "ret_on_margin": max(-1.0, levered["ret_on_filled"] * lev)}
                                     if levered and levered.get("ret_on_filled") is not None else None)
                        self.journal.append("outcome", {"message_id": d["message_id"], **r})
                        counts["outcomes"] += 1
                        ev["outcome"] = r
                out = ev.get("outcome")
                if out and out.get("plan") and "control" not in ev and now >= utc(d["entry_at"]) + CONTROL_AFTER:
                    self.journal.append("control", hub.control(d, out, market))
                    counts["controls"] += 1
            except Exception:  # noqa: BLE001 - retried on the next settlement pass
                counts["errors"] += 1
        return counts


# ------------------------------------------------------------------------------------------------ summary
def _mean(xs: List[Optional[float]]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return float(np.mean(xs)) if xs else None


def summary(output: Path = OUTPUT) -> Dict[str, Any]:
    if not PROTOCOL.exists():
        return {"status": "not_configured"}
    try:
        st = state(Journal(output))
    except RuntimeError as exc:
        return {"status": "journal_broken", "error": str(exc)}
    if not st["activation"]:
        return {"status": "not_activated", "review_n": REVIEW_N}
    decisions = [e["decision"] for e in st["events"].values() if e.get("decision")]
    entered = [e for e in st["events"].values() if e.get("decision", {}).get("entered")]
    outs = [e["outcome"] for e in entered if e.get("outcome")]
    traded = [o for o in outs if o.get("ret_on_filled") is not None]
    ctrl = [e["control"]["excess_post"] for e in entered if e.get("control", {}).get("excess_post") is not None]
    reasons: Dict[str, int] = {}
    for d in decisions:
        key = "entered" if d.get("entered") else d.get("reason", "unknown")
        reasons[key] = reasons.get(key, 0) + 1
    unfilled: Dict[str, int] = {}
    for o in outs:
        if o.get("ret_on_filled") is None:
            key = o.get("exit_reason") or o.get("reason") or o.get("status") or "unknown"
            unfilled[key] = unfilled.get(key, 0) + 1
    levered = [o["lev"]["ret_on_margin"] for o in traded if o.get("lev")]
    return {"status": "drift_stopped" if st["drift"] else "running", "activated_at": st["activation"]["at"],
            "posts": len(decisions), "decisions": reasons, "entered": len(entered), "settled": len(outs),
            "open": len(entered) - len(outs), "traded": len(traded), "not_filled": unfilled,
            "mean_net": _mean([o["ret_on_filled"] for o in traded]),
            "mean_net_with_funding": _mean([o.get("ret_with_funding") for o in traded]),
            "levered_n": len(levered), "levered_liquidated": sum(o["lev"]["exit_reason"] == "liquidated" for o in traded if o.get("lev")),
            "mean_ret_on_margin_as_written": _mean(levered), "controlled": len(ctrl), "mean_excess_post": _mean(ctrl),
            "review_n": REVIEW_N, "journal_records": st["last_seq"] + 1,
            "note": "descriptive; an impersonator channel's calls followed as written, never a strategy to trade"}


def registry_summary() -> Dict[str, Any]:
    """Shape expected by core.research.tracker_registry (same fields as the other Telegram paper ledgers)."""
    s = summary()
    runner = json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    fresh = bool(runner.get("at")) and 0 <= (now_utc() - utc(runner["at"])).total_seconds() <= 120
    n, mean = s.get("traded", 0), s.get("mean_net")
    label = {"not_activated": "未激活", "drift_stopped": "规则文件已变更 · 停账", "journal_broken": "日志校验失败",
             "not_configured": "未配置"}.get(s["status"], "积累新样本")
    status = label if s["status"] != "running" else (
        "运行异常" if runner.get("outcome") not in (None, "ok") else "运行心跳待恢复" if not fresh
        else "运行中 · 持仓待结算" if s.get("open") else "运行中 · 等待新喊单")
    activated = s.get("activated_at")
    return {
        "retirement": {"n": n, "min_n": REVIEW_N, "mean_pct": mean * 100 if mean is not None else None, "ci90_pct": None,
                       "verdict": "review_due" if n >= REVIEW_N else "collecting", "label": label,
                       "reason": "@alastartrader1 是冒充 Alastar 的频道。其结构化喊单（方向、入场、止盈、止损）从公开页首次收件起"
                                 "按原文执行：下一根1分钟开盘挂单，各止盈档平均分仓、原文止损，72小时内未成交作废、最长14天，"
                                 "不加杠杆，另记原文杠杆的爆仓结果。表中为不加杠杆的净收益；同币同几何对照入场45天后才有。",
                       "clusters": None},
        "started_at": activated, "updated_at": runner.get("at") or activated, "runner_active": fresh,
        "continuous_runner": fresh, "paper_only": True, "tracker_status_label": status,
        "forward_open": s.get("open", 0), "forward_waiting": 0,
        "forward_late": s.get("decisions", {}).get("late_receipt", 0), "review_gate": s["status"],
        "observation_count": s.get("posts", 0), "primary_sample_count": s.get("entered", 0), "ledger": s,
        "execution_authorized": False,
        "last_error": runner.get("error") if runner.get("outcome") not in (None, "ok") else None}


# ------------------------------------------------------------------------------------------- web worker
async def worker(stop_event: asyncio.Event, heartbeat: Optional[Callable[[bool], None]] = None) -> None:
    """Idles until activation; then one cycle every 30 s off the event loop. Paper ledger only."""
    ledger = Ledger()
    while not stop_event.is_set():
        if ledger.journal.path.exists():
            try:
                result = await asyncio.to_thread(ledger.cycle)
            except Exception as exc:  # noqa: BLE001 - recorded, retried next pass
                result = {"outcome": "error", "error": f"{type(exc).__name__}: {str(exc)[:160]}"}
            ok = result.get("outcome") == "ok"
        else:
            result, ok = {"outcome": "not_activated"}, True
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(STATE_PATH, json.dumps({"at": now_utc().isoformat(), **result}, default=str).encode("utf-8"))
        if heartbeat:
            heartbeat(ok)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
