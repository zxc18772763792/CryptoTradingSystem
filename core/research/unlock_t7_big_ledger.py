"""Forward paper overlay unlock_t7_big_v1 on the t-7 unlock short: cliffs >= 10% vs 5-10%. Records only; no orders.

Pre-registered in ``config/research_protocols/unlock_t7_big_v1.json`` after the 2026-11-05 tracker review:
in the history of the t-7 rule, cliffs of at least 10% of unlocked supply earned nearly three times the
hedged return of 5-10% cliffs (+5.85% vs +2.08% per trade, post hoc). Every forward trade of the ``unlock_short_t7`` tracker whose entry close falls after activation goes into
one of two books, by the size the source froze when it discovered the trade: ``big`` (>= 10%) or ``small``
(5-10%). Returns are the source's own settled hedged figures, so the two books partition its forward sample.

The source tracker file is read, never written. Journal records are hash-chained and carry the SHA-256 of
the protocol and this module; any change stops the journal. The single review is journaled once and frozen.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
TRACKER_ID = PROTOCOL_ID = "unlock_t7_big_v1"
OUTPUT = ROOT / "data/research/unlock_t7_big_v1"
PROTOCOL = ROOT / "config/research_protocols/unlock_t7_big_v1.json"
MODULE = Path(__file__)
SOURCE = ROOT / "data/research/unlock_short_t7/tracker.json"
SOURCE_MODULE = ROOT / "core/research/unlock_short_tracker.py"
STATE_PATH = ROOT / "runtime/unlock_t7_big_ledger.json"

BIG_MIN_PCT, SMALL_MIN_PCT = 10.0, 5.0
REVIEW_BATCHES, MIN_BIG, MIN_SMALL, REVIEW_AFTER_DAYS = 30, 15, 10, 548
PERMUTATIONS, SEED, ALPHA = 10_000, 20261105, 0.10
SETTLED = ("closed", "stopped", "schedule_changed")
DAY = timedelta(days=1)
SOURCE_MAX_AGE = timedelta(hours=24)
HEARTBEAT_MAX_AGE_SECONDS = 2 * 3600 + 60
LOCK_STALE_SECONDS = 600


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc(value: Any) -> datetime:
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("timezone required")
    return stamp.astimezone(timezone.utc)


def dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(raw)
    temp.replace(path)


def pins() -> Dict[str, str]:
    return {"protocol": sha(PROTOCOL.read_bytes()), "ledger": sha(MODULE.read_bytes())}


# ---------------------------------------------------------------------------------------------- the rule
def book_of(size_pct: Any) -> Optional[str]:
    """The book for a cliff size in percent of unlocked supply; None outside the source's range."""
    try:
        size = float(size_pct)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(size) or size < SMALL_MIN_PCT:
        return None
    return "big" if size >= BIG_MIN_PCT else "small"


def entry_close(entry_day: str) -> datetime:
    """The source enters at the close of its entry day: 00:00 UTC of the next day."""
    return utc(f"{entry_day}T00:00:00+00:00") + DAY


def eligible(trade: Dict[str, Any], activated: datetime) -> bool:
    try:
        close = entry_close(trade["entry_day"])
    except (KeyError, TypeError, ValueError):
        return False
    return (bool(trade.get("symbol")) and not trade.get("late") and bool(trade.get("exit_day"))
            and book_of(trade.get("size_pct")) is not None and close > activated)


def settle(trade: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The outcome of one source trade, or None while it is still running."""
    if trade is None:
        return {"void": "source_missing"}
    status = trade.get("status")
    if trade.get("late"):
        return {"void": "source_late", "status": status}
    if status == "entry_failed":
        return {"void": "source_entry_failed", "status": status}
    if status not in SETTLED:
        return None
    hedge = trade.get("hedge_settlement") or {}
    if trade.get("hedged_return_pct") is None:
        if hedge.get("status") == "pending":
            return None
        return {"void": "no_hedged_return", "status": status, "short_return_pct": trade.get("short_return_pct")}
    return {**{k: trade.get(k) for k in ("status", "hedged_return_pct", "short_return_pct", "funding_pct", "entry_price",
                                         "exit_price", "exit_day_actual", "basket_n")},
            "hedge_partial": bool(hedge.get("partial"))}


def batches(events: List[Dict[str, Any]]) -> List[Tuple[str, float]]:
    """(entry day, mean hedged return) per entry day, once every trade of that day has an outcome."""
    groups: Dict[str, List[Optional[float]]] = {}
    for e in events:
        day = e["event"]["entry_day"]
        outcome = e.get("outcome")
        if outcome is None:
            groups.setdefault(day, []).append(None)
        elif outcome.get("hedged_return_pct") is not None:
            groups.setdefault(day, []).append(float(outcome["hedged_return_pct"]))
    return [(day, float(np.mean(values))) for day, values in sorted(groups.items())
            if values and all(v is not None for v in values)]


def _mean(xs: List[float]) -> Optional[float]:
    return round(float(np.mean(xs)), 3) if xs else None


def sign_flip_p(values: List[float]) -> float:
    """One-sided: how often random signs give a mean at least as high as the observed one."""
    x = np.array(values, dtype=float)
    signs = np.random.default_rng(SEED).choice([-1.0, 1.0], size=(PERMUTATIONS, len(x)))
    hits = int(((signs * x).mean(axis=1) >= x.mean() - 1e-12).sum())
    return (hits + 1) / (PERMUTATIONS + 1)


def permutation_p(big: List[float], small: List[float]) -> float:
    """One-sided label permutation of the batches, statistic mean(big) - mean(small)."""
    pool = np.array(big + small, dtype=float)
    observed = np.mean(big) - np.mean(small)
    rng = np.random.default_rng(SEED)
    hits = 0
    for _ in range(PERMUTATIONS):
        x = rng.permutation(pool)
        hits += (x[:len(big)].mean() - x[len(big):].mean()) >= observed - 1e-12
    return (hits + 1) / (PERMUTATIONS + 1)


def evaluate(big: List[Tuple[str, float]], small: List[Tuple[str, float]], activated_at: Optional[str],
             now: Optional[datetime] = None) -> Dict[str, Any]:
    """The verdict at the single review; ``collecting`` before it."""
    now = now or now_utc()
    by_count = len(big) >= REVIEW_BATCHES
    by_time = activated_at is not None and now >= utc(activated_at) + timedelta(days=REVIEW_AFTER_DAYS)
    out: Dict[str, Any] = {"review_due": by_count or by_time, "p_absolute": None, "p_dose": None}
    if by_count:
        big = big[:REVIEW_BATCHES]
        out["cutoff_entry_day"] = big[-1][0]
        small = [row for row in small if row[0] <= big[-1][0]]
    big_values, small_values = [v for _, v in big], [v for _, v in small]
    out.update(big_batches=len(big), small_batches=len(small), big_mean_pct=_mean(big_values),
               small_mean_pct=_mean(small_values))
    if not out["review_due"]:
        out["verdict"] = "collecting"
    elif len(big) < MIN_BIG or len(small) < MIN_SMALL:
        out["verdict"] = "insufficient"
    elif np.mean(big_values) <= 0 or np.mean(big_values) <= np.mean(small_values):
        out["verdict"] = "rejected"
    else:
        out["p_absolute"] = round(sign_flip_p(big_values), 4)
        out["p_dose"] = round(permutation_p(big_values, small_values), 4)
        out["verdict"] = "supported" if out["p_absolute"] <= ALPHA and out["p_dose"] <= ALPHA else "inconclusive"
    return out


# -------------------------------------------------------------------------------------------- journal
class Journal:
    """Append-only, hash-chained JSONL. State is always rebuilt from the records."""

    def __init__(self, output: Path = OUTPUT):
        self.output = Path(output)
        self.path = self.output / "journal.jsonl"
        self._tail: Optional[tuple] = None

    def records(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            self._tail = (0, None)
            return []
        out, prev = [], None
        lines = self.path.read_bytes().split(b"\n")
        # every append ends with a newline, so a trailing fragment is a write still in progress
        for line in lines[:-1]:
            rec = json.loads(line.decode("utf-8"))
            body = {k: v for k, v in rec.items() if k != "hash"}
            if rec["prev"] != prev or rec["hash"] != sha(dumps(body)) or rec["seq"] != len(out):
                raise RuntimeError(f"journal chain broken at seq {rec.get('seq')}")
            out.append(rec)
            prev = rec["hash"]
        self._tail = (len(out), prev)
        return out

    def append(self, kind: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if self._tail is None:
            self.records()
        seq, prev = self._tail
        body = {"seq": seq, "at": now_utc().isoformat(), "kind": kind, "pins": pins(), "data": data, "prev": prev}
        rec = {**body, "hash": sha(dumps(body))}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("ab") as f:
            f.write(dumps(rec) + b"\n")
        self._tail = (seq + 1, rec["hash"])
        return rec


def state(journal: Journal) -> Dict[str, Any]:
    recs = journal.records()
    st: Dict[str, Any] = {"activation": None, "events": {}, "review": None, "drift": None, "last_seq": len(recs) - 1}
    for rec in recs:
        d = rec["data"]
        if rec["kind"] == "activation":
            st["activation"] = {**d, "at": rec["at"], "pins": rec["pins"]}
        elif rec["kind"] == "event":
            st["events"][d["source_id"]] = {"event": d}
        elif rec["kind"] == "outcome":
            st["events"][d["source_id"]]["outcome"] = d
        elif rec["kind"] == "review":
            st["review"] = {**d, "at": rec["at"]}
    if st["activation"] and st["activation"]["pins"] != pins():
        st["drift"] = {"activated": st["activation"]["pins"], "current": pins()}
    return st


@contextmanager
def exclusive(output: Path) -> Iterator[bool]:
    """One writer at a time (web job or CLI); a lock older than LOCK_STALE_SECONDS is taken over."""
    path = Path(output) / "journal.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if time.time() - path.stat().st_mtime > LOCK_STALE_SECONDS:
            path.unlink()
    except FileNotFoundError:
        pass
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        yield False
        return
    try:
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        yield True
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def load_source(path: Path = SOURCE) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def by_book(st: Dict[str, Any], book: str) -> List[Dict[str, Any]]:
    return [e for e in st["events"].values() if e["event"]["book"] == book]


# ---------------------------------------------------------------------------------------------- ledger
class Ledger:
    def __init__(self, output: Path = OUTPUT, source: Path = SOURCE):
        self.output = Path(output)
        self.source = Path(source)
        self.journal = Journal(self.output)

    def activate(self) -> Dict[str, Any]:
        with exclusive(self.output) as mine:
            if not mine:
                raise RuntimeError("another writer holds the journal lock")
            if self.journal.records():
                raise RuntimeError("journal already exists; a new protocol needs a new directory")
            src = load_source(self.source)
            now = now_utc()
            if now - utc(src["updated_at"]) > SOURCE_MAX_AGE:
                raise RuntimeError(f"source tracker is stale: {src['updated_at']}")
            trades = src.get("trades") or {}
            if not trades:
                raise RuntimeError("source tracker has no trades")
            return self.journal.append("activation", {
                "protocol": json.loads(PROTOCOL.read_text(encoding="utf-8"))["protocol_id"],
                "source_started_at": src.get("started_at"), "source_updated_at": src["updated_at"],
                "source_module_sha256": sha(SOURCE_MODULE.read_bytes()),
                "eligible_at_activation": sorted(k for k, t in trades.items() if eligible(t, now)),
                "excluded_at_activation": sorted(k for k, t in trades.items() if not eligible(t, now))})

    def cycle(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        with exclusive(self.output) as mine:
            if not mine:
                return {"outcome": "busy"}
            return self._cycle(now or now_utc())

    def _cycle(self, now: datetime) -> Dict[str, Any]:
        st = state(self.journal)
        if not st["activation"]:
            return {"outcome": "not_activated"}
        if st["drift"]:
            return {"outcome": "drift_stopped", "drift": st["drift"]}
        src = load_source(self.source)
        trades = src.get("trades") or {}
        activated = utc(st["activation"]["at"])
        report: Dict[str, Any] = {"outcome": "ok", "new_events": 0, "settled": 0, "reviewed": False,
                                  "source_updated_at": src.get("updated_at")}
        for key, t in sorted(trades.items(), key=lambda kv: (str(kv[1].get("entry_day")), kv[0])):
            if key in st["events"] or not eligible(t, activated):
                continue
            ev = {"source_id": key, "token": t.get("token"), "symbol": t["symbol"], "unlock_date": t.get("unlock_date"),
                  "size_pct": float(t["size_pct"]), "book": book_of(t["size_pct"]), "insider": t.get("insider"),
                  "entry_day": t["entry_day"], "exit_day": t["exit_day"],
                  "entry_close": entry_close(t["entry_day"]).isoformat(), "source_discovered_at": t.get("discovered_at"),
                  "first_seen_at": now.isoformat(), "seen_before_entry": now < entry_close(t["entry_day"])}
            self.journal.append("event", ev)
            st["events"][key] = {"event": ev}
            report["new_events"] += 1
        for key, e in st["events"].items():
            if "outcome" in e:
                continue
            out = settle(trades.get(key))
            if out is not None:
                e["outcome"] = {"source_id": key, **out}
                self.journal.append("outcome", e["outcome"])
                report["settled"] += 1
        if st["review"] is None:
            ev = evaluate(batches(by_book(st, "big")), batches(by_book(st, "small")), st["activation"]["at"], now)
            if ev["review_due"]:
                self.journal.append("review", ev)
                report["reviewed"] = True
        return report


# --------------------------------------------------------------------------------------------- summary
def _book(events: List[Dict[str, Any]], now: datetime) -> Dict[str, Any]:
    done = [e for e in events if (e.get("outcome") or {}).get("hedged_return_pct") is not None]
    values = [float(e["outcome"]["hedged_return_pct"]) for e in done]
    legs = [float(e["outcome"]["short_return_pct"]) for e in done if e["outcome"].get("short_return_pct") is not None]
    rows = batches(events)
    means = [v for _, v in rows]
    running = [e for e in events if "outcome" not in e]
    return {"events": len(events), "waiting": sum(now < utc(e["event"]["entry_close"]) for e in running),
            "open": sum(now >= utc(e["event"]["entry_close"]) for e in running),
            "void": sum("void" in (e.get("outcome") or {}) for e in events), "trades": len(values),
            "trade_mean_pct": _mean(values), "batches": len(rows), "batch_mean_pct": _mean(means),
            "positive_batches": sum(v > 0 for v in means), "worst_trade_pct": round(min(values), 3) if values else None,
            "stops": sum(e["outcome"].get("status") == "stopped" for e in done),
            "schedule_changed": sum(e["outcome"].get("status") == "schedule_changed" for e in done),
            "short_leg_mean_pct": _mean(legs)}


def summary(output: Path = OUTPUT, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or now_utc()
    try:
        st = state(Journal(output))
    except Exception as exc:  # noqa: BLE001
        return {"status": "journal_broken", "error": str(exc)[:200]}
    if not st["activation"]:
        return {"status": "not_activated"}
    big, small = by_book(st, "big"), by_book(st, "small")
    return {"status": "drift_stopped" if st["drift"] else "running", "activated_at": st["activation"]["at"],
            "books": {"big": _book(big, now), "small": _book(small, now)},
            "recorded_after_entry": sum(not e["event"]["seen_before_entry"] for e in st["events"].values()),
            "evaluation": st["review"] or evaluate(batches(big), batches(small), st["activation"]["at"], now),
            "drift": st["drift"]}


def registry_summary() -> Dict[str, Any]:
    """Shape expected by core.research.tracker_registry. Main number: batch mean of the >= 10% book."""
    s = summary()
    runner = json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    fresh = bool(runner.get("at")) and 0 <= (now_utc() - utc(runner["at"])).total_seconds() <= HEARTBEAT_MAX_AGE_SECONDS
    if s["status"] in ("not_activated", "journal_broken"):
        label = {"not_activated": "未激活", "journal_broken": "日志校验失败"}[s["status"]]
        return {"retirement": {"n": 0, "min_n": REVIEW_BATCHES, "mean_pct": None, "ci90_pct": None,
                               "verdict": s["status"], "label": label, "reason": None, "clusters": None},
                "tracker_status_label": label, "paper_only": True, "runner_active": fresh}
    big, small, ev = s["books"]["big"], s["books"]["small"], s["evaluation"]
    label = {"collecting": "积累样本", "insufficient": "到期样本不足", "rejected": "未通过",
             "supported": "通过", "inconclusive": "未定"}[ev["verdict"]]
    stale = bool(runner.get("source_updated_at")) and now_utc() - utc(runner["source_updated_at"]) > SOURCE_MAX_AGE
    status = "规则文件已变更 · 停账" if s["status"] == "drift_stopped" else (
        "运行异常" if runner.get("outcome") not in (None, "ok", "busy") else "运行心跳待恢复" if not fresh
        else "t−7 原账本超过一天未更新" if stale else "运行中 · 有待结算的交易" if big["open"] or small["open"]
        else "运行中 · 等待下一笔入场")

    def shown(book: Dict[str, Any]) -> str:
        return f"{book['batches']} 批，均值 " + ("—" if book["batch_mean_pct"] is None else f"{book['batch_mean_pct']:+.2f}%")

    return {
        "retirement": {"n": big["batches"], "min_n": REVIEW_BATCHES, "mean_pct": big["batch_mean_pct"], "ci90_pct": None,
                       "verdict": ev["verdict"], "label": label,
                       "reason": f"≥10%：{shown(big)}；5–10%：{shown(small)}。复核前只作描述；"
                                 "满 30 个 ≥10% 批次或 18 个月时判一次：≥10% 均值为正且高于 5–10%，两项单侧检验 p≤0.10 才算通过。",
                       "clusters": None},
        "started_at": s["activated_at"], "updated_at": runner.get("at") or s["activated_at"], "runner_active": fresh,
        "continuous_runner": fresh, "paper_only": True, "tracker_status_label": status,
        "forward_win_rate": round(big["positive_batches"] / big["batches"], 3) if big["batches"] else None,
        "forward_open": big["open"], "forward_waiting": big["waiting"], "forward_late": s["recorded_after_entry"],
        "books": s["books"], "execution_authorized": False,
        "last_error": runner.get("error_type") if runner.get("outcome") not in (None, "ok", "busy") else None}


# ------------------------------------------------------------------------------------------- web job
def run_once(output: Path = OUTPUT, source: Path = SOURCE, state_path: Path = STATE_PATH) -> Dict[str, Any]:
    """One local pass plus a heartbeat file. Idles until activation; reads files only, no network."""
    if (Path(output) / "journal.jsonl").exists():
        try:
            result = Ledger(output, source).cycle()
        except Exception as exc:  # noqa: BLE001 - retried next pass
            result = {"outcome": "failed", "error_type": type(exc).__name__}
    else:
        result = {"outcome": "not_activated"}
    atomic_write(state_path, json.dumps({"at": now_utc().isoformat(), **result}, default=str).encode("utf-8"))
    return result


async def tick() -> Dict[str, Any]:
    return await asyncio.to_thread(run_once)
