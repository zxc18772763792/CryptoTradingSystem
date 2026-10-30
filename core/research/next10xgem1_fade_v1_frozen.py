"""Forward paper ledger for fading @next10xgem1 long calls. Records only; no orders.

Pre-registered in ``config/research_protocols/next10xgem1_fade_24h_v1.json`` after the exploratory
study docs/TELEGRAM_NEXT10XGEM1_FADE_STUDY_20261026.md. The news collector archives every first
receipt of the public preview t.me/s/next10xgem1 (archive-only, ``core.news.alastar_observation``).
Each fresh unconditional long call that the frozen kol4-v1 labels (``core.research.kol4_call_labels``)
find in a first receipt after activation becomes one paper short on the Binance USDT perpetual: the
next 5-minute open at least 60 seconds after receipt, held exactly 24 hours, with a 50% catastrophe
stop recorded as a second book from the same entry. Each result is compared with the exact mean of
every hourly-start 24h short on that coin in the 30 days before entry.

The protocol and both source files are hash-pinned in every journal record, so a changed rule can
only start a new journal. This module has no scheduler of its own, never calls an AI provider, and
never touches an account or order endpoint. The journal layout follows cryptoquant_alert_shadow.
"""
from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.news.alastar_observation import (
    _archive_lock, _atomic_bytes, _json_bytes, _load_runs, _utc, archive_dir,
)
from core.research import kol4_call_labels as labels

ROOT = Path(__file__).resolve().parents[2]
CHANNEL = "next10xgem1"
TRACKER_ID = "next10xgem1_fade_24h_v1"
PROTOCOL_ID = "next10xgem1_fade_24h_v1"
OUTPUT = ROOT / "data/research/next10xgem1_fade_24h_v1"
PROTOCOL = ROOT / "config/research_protocols/next10xgem1_fade_24h_v1.json"
LABELS = Path(labels.__file__)
DEV_HISTORY = Path.home() / ".crypto_trading_system/research/telegram_new_kols_20261025/next10xgem1_history.json"
ELIGIBLE_LABELS = ("trade_plan", "direct_call")
HOLD_HOURS = 24
HOLD = timedelta(hours=HOLD_HOURS)
STEP_MS = 300_000
HOUR_MS = 3_600_000
STEP = timedelta(minutes=5)
STOP_LEVEL = .5
STOP_SLIP = .005
LIQUIDATION = 2.0
COST_BPS = 20
STRESS_BPS = 80
NOTIONAL_USDT = 1000
MAX_OPEN = 8
TARGET_N = 490
FUTILITY_N = 245
MAX_MONTHS = 24
MIN_NULL_STARTS = 168
NULL_DAYS = 30
UNAVAILABLE_AFTER = timedelta(days=7)
BOUNDARY_MS = 60_000
UNIVERSE_MAX_AGE = timedelta(hours=6)
POLL_SECONDS = 15
KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
TIME_URL = "https://fapi.binance.com/fapi/v1/time"
INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
IMPLEMENTATION_RULES = {
    "version": "next10xgem1-fade-20261026",
    "entry": "next 5m open at least 60s after max(first receipt, freeze commit)",
    "entry_price": "official 5m kline open at the planned entry; a freeze committed after it is recorded as missed",
    "maximum_entry_delay_after_publication_minutes": 30,
    "collector_gap_seconds_max": 600,
    "clock_max_abs_offset_seconds": 2,
    "clock_max_round_trip_seconds": 10,
    "clock_samples": 5,
    "zero_trade_bars": "kept: an official bar without trades still carries the exchange's OHLC",
    "path_gap": "a missing 5m bar inside the hold leaves the settlement unavailable; retried, never filled",
    "ignored_first_receipt_reasons": ["visible_edit"],
}


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ceil5(at: Any) -> datetime:
    return datetime.fromtimestamp(math.ceil(_utc(at).timestamp() / 300) * 300, timezone.utc)


def ms(at: Any) -> int:
    return int(round(_utc(at).timestamp() * 1000))


def number(value: Any, *, positive: bool = False) -> float:
    value = float(value)
    if not math.isfinite(value) or (positive and value <= 0):
        raise ValueError("Nonfinite or nonpositive market value")
    return value


# ------------------------------------------------------------------------------------------ market math
def bars_by_time(rows: List[List[Any]], step_ms: int = STEP_MS) -> Dict[int, tuple]:
    """Gaps stay gaps; inconsistent duplicates are rejected. Zero-trade bars are real bars here (small caps)."""
    out: Dict[int, tuple] = {}
    for row in rows:
        if len(row) < 5:
            raise ValueError("Invalid official kline row")
        stamp = int(row[0])
        if stamp % step_ms:
            raise ValueError("Kline is not aligned to its interval")
        o, h, lo, c = [number(v, positive=True) for v in row[1:5]]
        if h < max(o, c) or lo > min(o, c) or h < lo:
            raise ValueError("Invalid OHLC geometry")
        bar = (o, h, lo, c)
        if stamp in out and out[stamp] != bar:
            raise ValueError("Inconsistent duplicate kline")
        out[stamp] = bar
    return out


def short_path(bars: Dict[int, tuple], start_ms: int, steps: int, step_ms: int, stop: Optional[float] = None,
               cost_bps: int = COST_BPS) -> Optional[Dict[str, Any]]:
    """Fixed-length short from the open at start_ms; None if a bar of the path is missing.

    Without a stop a high at 2x entry is a -100% liquidation. With a stop the first bar whose high
    reaches entry*(1+stop) exits at that level, or at its open when it opens beyond it (bar 0 opens
    at the entry), plus STOP_SLIP. The exit bar of a time exit is read for its open only.
    """
    if start_ms not in bars:
        return None
    e = bars[start_ms][0]
    level = e * (1 + stop) if stop is not None else math.inf
    for i in range(steps):
        stamp = start_ms + i * step_ms
        if stamp not in bars:
            return None
        o, h = bars[stamp][0], bars[stamp][1]
        if i and o >= level:
            fill = o * (1 + STOP_SLIP)
            reason = "stop"
        elif h >= level and level < LIQUIDATION * e:
            fill, reason = level * (1 + STOP_SLIP), "stop"
        elif h >= LIQUIDATION * e:
            return {"entry_price": e, "exit_price": None, "exit_reason": "liquidated", "exit_bar_ms": stamp,
                    "gross": -1.0, "net_price_only": -1.0, "net_price_only_stress": -1.0}
        else:
            continue
        gross = max(-1.0, (e - fill) / e)
        return {"entry_price": e, "exit_price": fill, "exit_reason": reason, "exit_bar_ms": stamp, "gross": gross,
                "net_price_only": max(-1.0, gross - cost_bps / 10000),
                "net_price_only_stress": max(-1.0, gross - STRESS_BPS / 10000)}
    end = start_ms + steps * step_ms
    if end not in bars:
        return None
    gross = max(-1.0, (e - bars[end][0]) / e)
    return {"entry_price": e, "exit_price": bars[end][0], "exit_reason": "time", "exit_bar_ms": end, "gross": gross,
            "net_price_only": max(-1.0, gross - cost_bps / 10000),
            "net_price_only_stress": max(-1.0, gross - STRESS_BPS / 10000)}


def books(bars: Dict[int, tuple], entry_ms: int) -> Optional[Dict[str, Dict[str, Any]]]:
    plain = short_path(bars, entry_ms, HOLD_HOURS * 12, STEP_MS)
    stopped = short_path(bars, entry_ms, HOLD_HOURS * 12, STEP_MS, STOP_LEVEL)
    if plain is None or stopped is None:
        return None
    return {"plain": plain, "stop50": stopped}


def null_profile(rows: List[List[Any]], entry_ms: int, minimum: int = MIN_NULL_STARTS) -> Dict[str, Any]:
    """Exact mean of every hourly-start 24h short whose whole window lies in the 30 days before entry."""
    bars = bars_by_time(rows, HOUR_MS)
    first = -(-(entry_ms - NULL_DAYS * 86_400_000) // HOUR_MS) * HOUR_MS
    values: Dict[str, List[float]] = {"plain": [], "stop50": []}
    start = first
    while start + HOLD_HOURS * HOUR_MS <= entry_ms:
        for name, stop in (("plain", None), ("stop50", STOP_LEVEL)):
            outcome = short_path(bars, start, HOLD_HOURS, HOUR_MS, stop)
            if outcome is not None:
                values[name].append(outcome["net_price_only"])
        start += HOUR_MS
    out: Dict[str, Any] = {"cost_bps": COST_BPS, "funding_in_null": False, "window_days": NULL_DAYS,
                           "latest_window_end_ms": entry_ms, "books": {}}
    for name, items in values.items():
        out["books"][name] = {"n": len(items), "mean": sum(items) / len(items) if items else None,
                              "status": "ok" if len(items) >= minimum else "insufficient_preentry_history"}
    before = (entry_ms - HOLD_HOURS * HOUR_MS) // HOUR_MS * HOUR_MS
    out["context"] = {"open_24h_before_entry": bars[before][0] if before in bars else None}
    return out


def funding_net(rows: List[Dict[str, Any]], proof: Dict[str, Any], symbol: str, outcome: Dict[str, Any],
                entry_ms: int) -> Optional[float]:
    """Price result plus actual funding on the short; settlements near either boundary count only if adverse."""
    if proof.get("complete_coverage") is not True:
        return None
    if outcome["exit_reason"] == "liquidated":
        return -1.0
    timed = outcome["exit_reason"] == "time"
    end = outcome["exit_bar_ms"] if timed else outcome["exit_bar_ms"] + STEP_MS
    # A stop fills somewhere inside its bar: every settlement of that bar is a boundary payment.
    tail = end - BOUNDARY_MS if timed else outcome["exit_bar_ms"] - BOUNDARY_MS
    seen: set = set()
    total = 0.0
    for row in rows:
        stamp = int(row["fundingTime"])
        if row.get("symbol") != symbol or stamp in seen:
            raise ValueError("Duplicate or wrong-symbol funding")
        seen.add(stamp)
        if stamp < entry_ms - BOUNDARY_MS or stamp > end + BOUNDARY_MS:
            continue
        payment = number(row["fundingRate"]) * number(row["markPrice"], positive=True) / outcome["entry_price"]
        boundary = abs(stamp - entry_ms) <= BOUNDARY_MS or stamp >= tail
        total += min(0.0, payment) if boundary else payment
    return max(-1.0, outcome["net_price_only"] + total)


class PublicMarketData:
    """Public Binance USD-M reads with each response kept as hash-named evidence. No key, no account endpoint."""

    def __init__(self, output: Path, *, max_calls: int = 20):
        import requests
        from config.settings import settings
        from core.utils.proxy_env import ensure_proxy_env
        ensure_proxy_env(settings)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "next10xgem1-fade-research/1", "Cache-Control": "no-cache"})
        self.output, self.calls, self.max_calls, self.failed = Path(output), 0, max_calls, False

    def _fetch(self, url: str, params: Dict[str, Any], *, keep_rows: bool = True):
        if url not in (KLINES_URL, FUNDING_URL, TIME_URL, INFO_URL):
            raise ValueError("Unsupported public endpoint")
        if self.failed or self.calls >= self.max_calls:
            raise RuntimeError("Public data pass stopped or request bound reached")
        self.calls += 1
        requested = now_utc()
        try:
            response = self.session.get(url, params=params, timeout=(5, 20))
            response.raise_for_status()
            if len(response.content) > 4 * 1024 * 1024:
                raise ValueError("Public response exceeds evidence bound")
            rows = response.json()
        except Exception as exc:
            self.failed = True
            raise RuntimeError(f"Official public data unavailable ({type(exc).__name__})") from None
        received = now_utc()
        record = {"endpoint": url, "params": params, "requested_at_utc": requested.isoformat(),
                  "received_at_utc": received.isoformat(), "response_sha256": sha(response.content)}
        if keep_rows:
            record["rows"] = rows
        raw = _json_bytes(record)
        relative = f"evidence/{sha(raw)}.json"
        if not (self.output / relative).exists():
            _atomic_bytes(self.output / relative, raw)
        return rows, {"file": relative, "sha256": sha(raw)}, requested, received

    def clock_check(self, samples: int = IMPLEMENTATION_RULES["clock_samples"]) -> Dict[str, Any]:
        reads = []
        for _ in range(max(1, int(samples))):
            payload, evidence, requested, received = self._fetch(TIME_URL, {})
            reads.append({"round_trip_seconds": (received - requested).total_seconds(),
                          "offset_interval_seconds": [payload["serverTime"] / 1000 - received.timestamp(),
                                                      payload["serverTime"] / 1000 - requested.timestamp()],
                          "checked_at_utc": received.isoformat(), "evidence": evidence})
        best = dict(min(reads, key=lambda r: r["round_trip_seconds"]))
        best["ok"] = (max(abs(v) for v in best["offset_interval_seconds"]) <= IMPLEMENTATION_RULES["clock_max_abs_offset_seconds"]
                      and best["round_trip_seconds"] <= IMPLEMENTATION_RULES["clock_max_round_trip_seconds"])
        best["reads"], best["selection"] = reads, "minimum round trip of all reads"
        return best

    def universe(self) -> Dict[str, Any]:
        """TRADING USDT perpetuals; the 1 MB body is reduced to the symbol list before it is kept."""
        payload, evidence, _, received = self._fetch(INFO_URL, {}, keep_rows=False)
        symbols = sorted(s["symbol"] for s in payload["symbols"] if s.get("contractType") == "PERPETUAL"
                         and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING")
        if len(symbols) < 100:
            raise ValueError("Implausibly small perpetual universe")
        return {"fetched_at_utc": received.isoformat(), "symbols": symbols, "evidence": evidence,
                "symbols_sha256": sha(_json_bytes(symbols))}

    def klines(self, symbol: str, interval: str, start_ms: int, end_ms: int):
        step = {"5m": STEP_MS, "1h": HOUR_MS}[interval]
        if end_ms > ms(now_utc()) or end_ms < start_ms or (end_ms - start_ms) // step > 1499:
            raise ValueError("Kline window must be historical and fit one request")
        rows, evidence, _, _ = self._fetch(KLINES_URL, {"symbol": symbol, "interval": interval, "startTime": start_ms,
                                                        "endTime": end_ms, "limit": 1500})
        if not isinstance(rows, list) or any(not start_ms <= int(r[0]) <= end_ms for r in rows):
            raise ValueError("Invalid official klines payload")
        return rows, evidence

    def funding_since(self, symbol: str, entry_ms: int):
        """Latest-N settlements (no time window: the WAF answers windowed fundingRate reads with 403)."""
        limit = min(1000, (ms(now_utc()) - entry_ms) // HOUR_MS + 4)
        rows, evidence, _, _ = self._fetch(FUNDING_URL, {"symbol": symbol, "limit": int(limit)})
        if not isinstance(rows, list) or any(r.get("symbol") != symbol for r in rows):
            raise ValueError("Funding payload invalid or wrong symbol")
        covered = len(rows) < limit or bool(rows) and min(int(r["fundingTime"]) for r in rows) <= entry_ms - BOUNDARY_MS
        return rows, {**evidence, "limit": int(limit), "complete_coverage": covered}


# ------------------------------------------------------------------------------------------------ journal
def runtime(output: Path) -> Dict[str, Any]:
    path = Path(output) / "runtime.json"
    return json.loads(path.read_bytes()) if path.exists() else {"attempts": {}, "errors": [], "seen": {}}


def development_profile(path: Path = DEV_HISTORY) -> Dict[str, Any]:
    """The frozen labels over the study's 180-day history; must equal the sealed counts at activation."""
    raw = Path(path).read_bytes()
    counts: Dict[str, int] = {}
    posts = json.loads(raw)["posts"]
    for post in posts:
        label = labels.classify_post(post, set(), set())["label"]
        counts[label] = counts.get(label, 0) + 1
    return {"history_sha256": sha(raw), "posts": len(posts), "label_counts": dict(sorted(counts.items()))}


def call_signals(text: str) -> Dict[str, Any]:
    """kol4-v1 label of one first-received text; signals carry the raw ticker, resolved at freeze time."""
    label = labels.classify_post({"message_id": 0, "text": text}, set(), set())
    eligible = label["label"] in ELIGIBLE_LABELS and not label["conditional"]
    return {"label": label["label"], "reason": label["reason"], "conditional": label["conditional"],
            "inferred_symbol": label["inferred_symbol"], "eligible": eligible,
            "signals": [{"raw_symbol": s["raw_symbol"], "side": s["side"]} for s in label["signals"]]}


class Journal:
    """Hash-chained, protocol- and implementation-pinned record of every call and its settlement."""

    def __init__(self, output: Path = OUTPUT, archive: Optional[Path] = None, protocol: Path = PROTOCOL):
        self.output = Path(output)
        self.archive = Path(archive) if archive is not None else archive_dir(CHANNEL)
        self.protocol_path = Path(protocol)
        self.protocol_raw = self.protocol_path.read_bytes()
        self.protocol = json.loads(self.protocol_raw)
        book, sample = self.protocol.get("book") or {}, self.protocol.get("sample") or {}
        if (self.protocol.get("protocol_id") != PROTOCOL_ID or book.get("holding_hours") != HOLD_HOURS
                or book.get("round_trip_cost_bps") != COST_BPS or book.get("stress_cost_bps") != STRESS_BPS
                or book.get("notional_usdt_per_trade") != NOTIONAL_USDT or book.get("max_open_positions") != MAX_OPEN
                or sample.get("target_completed") != TARGET_N or sample.get("futility_at") != FUTILITY_N
                or sample.get("maximum_months") != MAX_MONTHS
                or (self.protocol.get("control") or {}).get("minimum_null_starts") != MIN_NULL_STARTS
                or (self.protocol.get("labels") or {}).get("module_sha256") != sha(LABELS.read_bytes())
                or self.protocol.get("execution_authorized") is not False):
            raise ValueError("Protocol parameters differ from the implementation; review before use")
        self.protocol_sha = sha(self.protocol_raw)
        self.paths = {"tracker": Path(__file__), "labels": LABELS}
        self.sources = {name: sha(p.read_bytes()) for name, p in self.paths.items()}
        self.implementation_sha = sha(_json_bytes({"sources": self.sources, "rules": IMPLEMENTATION_RULES}))
        self._cache: List[Dict[str, Any]] = []
        self._cache_tail: Optional[str] = None

    # -- chain -------------------------------------------------------------------------------------------
    def _check_sources(self) -> None:
        if sha(self.protocol_path.read_bytes()) != self.protocol_sha or any(
                sha(p.read_bytes()) != self.sources[name] for name, p in self.paths.items()):
            raise RuntimeError("Source/protocol drift; stop this journal and migrate explicitly")

    def _records(self) -> List[Dict[str, Any]]:
        """Verify and return the chain; only files newer than the verified prefix are read again."""
        self._check_sources()
        directory = self.output / "journal"
        position = len(self._cache)
        previous = self._cache_tail
        last_at = _utc(self._cache[-1]["recorded_at_utc"]) if self._cache else None
        while True:
            path = directory / f"{position + 1:012d}.json"
            if not path.exists():
                break
            raw = path.read_bytes()
            record = json.loads(raw)
            at = _utc(record["recorded_at_utc"])
            if (record["sequence"] != position + 1 or record["previous_sha256"] != previous
                    or record["protocol_sha256"] != self.protocol_sha
                    or record.get("implementation_sha256") != self.implementation_sha or (last_at and at < last_at)):
                raise RuntimeError("Fade journal hash, protocol or clock sequence is invalid")
            self._cache.append(record)
            previous, last_at, position = sha(raw), at, position + 1
        self._cache_tail = previous
        checkpoint = self.output / "checkpoint.json"
        if checkpoint.exists():
            saved = json.loads(checkpoint.read_bytes())
            tail = directory / f"{saved['sequence']:012d}.json"
            if saved["sequence"] > len(self._cache) or sha(tail.read_bytes()) != saved["sha256"]:
                raise RuntimeError("Fade journal checkpoint hash is invalid")
        return self._cache

    def verify(self) -> Dict[str, Any]:
        self._cache, self._cache_tail = [], None
        records = self._records()
        return {"records": len(records), "tail_sha256": self._cache_tail, "protocol_sha256": self.protocol_sha,
                "implementation_sha256": self.implementation_sha}

    def _append(self, kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        records = self._records()
        at = now_utc()
        if records and at < _utc(records[-1]["recorded_at_utc"]):
            raise RuntimeError("Local UTC clock moved backwards")
        record = {"sequence": len(records) + 1, "previous_sha256": self._cache_tail, "protocol_sha256": self.protocol_sha,
                  "implementation_sha256": self.implementation_sha, "recorded_at_utc": at.isoformat(),
                  "kind": kind, "payload": payload}
        path = self.output / "journal" / f"{record['sequence']:012d}.json"
        if path.exists():
            raise RuntimeError("Refusing to overwrite a committed journal record")
        raw = _json_bytes(record)
        _atomic_bytes(path, raw)
        _atomic_bytes(self.output / "checkpoint.json", _json_bytes({"sequence": record["sequence"], "sha256": sha(raw)}))
        self._cache.append(record)
        self._cache_tail = sha(raw)
        return record

    def _state(self):
        records = self._records()
        events: Dict[str, Dict[str, Any]] = {}
        for record in records:
            p, kind = record["payload"], record["kind"]
            if kind == "freeze":
                events[p["event_id"]] = {**p, "freeze_committed_at": record["recorded_at_utc"], "settlement": None,
                                         "null": None}
            elif kind == "entry":
                events[p["event_id"]].update(status="observing", entry=p)
            elif kind == "missed":
                events[p["event_id"]].update(status="skipped", missed=p)
            elif kind == "settlement":
                events[p["event_id"]]["settlement"] = copy.deepcopy(p)  # funding records fill it in below
            elif kind == "funding":
                for name, value in p["net_with_funding"].items():
                    events[p["event_id"]]["settlement"]["books"][name]["net_with_funding"] = value
                events[p["event_id"]]["settlement"]["funding_evidence"] = p["funding_evidence"]
            elif kind == "null":
                events[p["event_id"]]["null"] = p["profile"]
            elif kind == "unavailable":
                events[p["event_id"]].update(status="unavailable", unavailable=p)
            if kind in ("settlement", "null"):
                event = events[p["event_id"]]
                if event["settlement"] and event["null"] and event.get("status") == "observing":
                    event["status"] = "completed"
        return records, events

    # -- archive and health ------------------------------------------------------------------------------
    def health(self) -> Dict[str, Any]:
        """Recent real polls of the channel, outages included; the gate is four polls each <=600 s apart."""
        try:
            state = json.loads((self.archive / "state.json").read_bytes())
            count = int(state["run_count"])
        except (OSError, ValueError, KeyError, TypeError):
            return {"checked_at_utc": now_utc().isoformat(), "healthy_for_new_freezes": False, "reason": "archive_missing",
                    "live_run_count": 0}
        times = []
        for sequence in range(count, max(0, count - 12), -1):
            run = json.loads((self.archive / "runs" / f"{sequence:012d}.json").read_bytes())
            if run["mode"] == "live_public_preview":
                times.append(_utc(run["observed_at_utc"]))
            if len(times) == 4:
                break
        times.sort()
        gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
        age = (now_utc() - times[-1]).total_seconds() if times else None
        limit = IMPLEMENTATION_RULES["collector_gap_seconds_max"]
        healthy = len(times) >= 4 and age is not None and 0 <= age <= limit and all(0 < g <= limit for g in gaps)
        return {"checked_at_utc": now_utc().isoformat(), "healthy_for_new_freezes": healthy, "archive_run_count": count,
                "last_receipt_age_seconds": age, "last_poll_gaps_seconds": gaps,
                "requirement": f"four latest live polls, each gap<={limit}s, latest no older than {limit}s"}

    def _first_receipt_run(self, key: str, count: int) -> Dict[str, Any]:
        for sequence in range(count, max(0, count - 400), -1):
            raw = (self.archive / "runs" / f"{sequence:012d}.json").read_bytes()
            if any(x["message_key"] == key for x in json.loads(raw)["new_revisions"]):
                return {"file": f"runs/{sequence:012d}.json", "sha256": sha(raw)}
        return {"file": None, "sha256": None}

    # -- life cycle --------------------------------------------------------------------------------------
    def activate(self, clock: Dict[str, Any], profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        health = self.health()
        profile = profile or development_profile()
        expected = (self.protocol.get("labels") or {}).get("development_sample") or {}
        if (profile["history_sha256"] != expected.get("history_sha256") or profile["posts"] != expected.get("posts")
                or profile["label_counts"] != expected.get("label_counts")):
            raise RuntimeError("Frozen labels over the development sample differ from the pre-registered profile")
        if not health["healthy_for_new_freezes"] or not clock.get("ok"):
            raise RuntimeError("Activation prerequisites are not satisfied (collector health or clock)")
        with _archive_lock(self.output):
            if self._records():
                raise RuntimeError("Fade registration already exists; never reset it")
            return self._append("activation", {"paper_only": True, "live_orders": False, "protocol_snapshot": self.protocol,
                                               "implementation_rules": IMPLEMENTATION_RULES, "implementation_sources": self.sources,
                                               "collector_health": health, "clock_check": clock, "development_sample": profile})

    def gate(self, records: List[Dict[str, Any]], events: Dict[str, Dict[str, Any]]) -> Optional[str]:
        """Predeclared sample/time gates over primary-sample events; never an early stop for success."""
        if not records:
            return "not_activated"
        activation = _utc(records[0]["recorded_at_utc"])
        try:
            deadline = activation.replace(year=activation.year + MAX_MONTHS // 12)
        except ValueError:
            deadline = activation.replace(year=activation.year + MAX_MONTHS // 12, day=28)
        if now_utc() >= deadline:
            return "maximum24months_reached"
        done = completed(events)
        active = sum(e.get("primary_sample") and e["status"] in ("planned", "observing") for e in events.values())
        if len(done) >= TARGET_N:
            return "confirmatory490_review_due"
        if len(done) >= FUTILITY_N and sum(excess(e) for e in done[:FUTILITY_N]) / FUTILITY_N <= 0:
            return "futility245_stop"
        if len(done) < FUTILITY_N <= len(done) + active:
            return "await245_maturity"
        if len(done) + active >= TARGET_N:
            return "await490_maturity"
        return None

    def universe(self, state: Dict[str, Any], provider_factory) -> Optional[Dict[str, Any]]:
        cached = state.get("universe")
        if cached and 0 <= (now_utc() - _utc(cached["fetched_at_utc"])).total_seconds() <= UNIVERSE_MAX_AGE.total_seconds():
            return cached
        try:
            fresh = provider_factory().universe()
        except (ValueError, KeyError, OSError, RuntimeError, TypeError):
            return None
        state["universe"] = fresh
        return fresh

    def sync(self, state: Dict[str, Any], provider_factory) -> List[Dict[str, Any]]:
        """Freeze every eligible call first received live after activation; other posts are counted only."""
        records, events = self._state()
        if not records:
            return []
        activated = _utc(records[0]["recorded_at_utc"])
        try:
            count, messages, _ = _load_runs(self.archive, channel=CHANNEL)
        except (OSError, ValueError, KeyError):
            return []
        actions = []
        seen = state.setdefault("seen", {})
        for key, item in sorted(messages.items(), key=lambda pair: pair[1]["first"]["message_id"]):
            first = item["first"]
            if key in seen or first.get("published_at_utc") is None or _utc(first["published_at_utc"]) < activated:
                continue
            label = call_signals(first["text"])
            for index, signal in enumerate(label["signals"] if label["eligible"] else []):
                event_id = f"{key}#{index}"
                if event_id in events:
                    continue
                actions.append(self._freeze(event_id, key, item, label, signal, count, provider_factory, state))
                records, events = self._state()
            # Marked only once every signal is frozen, so a failed pass retries the post.
            seen[key] = label["label"] + (":conditional" if label["conditional"] else "")
        return actions

    def _freeze(self, event_id: str, key: str, item: Dict[str, Any], label: Dict[str, Any], signal: Dict[str, Any],
                run_count: int, provider_factory, state: Dict[str, Any]) -> Dict[str, Any]:
        first = item["first"]
        with _archive_lock(self.output):
            records, events = self._state()
            ignored = set(IMPLEMENTATION_RULES["ignored_first_receipt_reasons"])
            reasons = [r for r in (first.get("exclusion_reasons") or []) if r not in ignored]
            if signal["side"] != 1:
                reasons.append("not_a_long_call")
            universe = self.universe(state, provider_factory)
            base, symbol = None, None
            if universe is None:
                reasons.append("universe_unavailable")
            else:
                bases = {s[:-4] for s in universe["symbols"]}
                base, venue = labels.resolve_symbol(signal["raw_symbol"], bases, set())
                if venue != "futures":
                    reasons.append("no_usdt_perpetual")
                    base = None
                symbol = f"{base}USDT" if base else None
            health = self.health()
            if not health["healthy_for_new_freezes"]:
                reasons.append("collector_health_failed")
            gate = self.gate(records, events)
            if gate:
                reasons.append(gate)
            clock = None
            if not reasons:
                cached = state.get("last_clock_check")
                if cached and 0 <= (now_utc() - _utc(cached["checked_at_utc"])).total_seconds() <= 120 and cached.get("ok"):
                    clock = cached
                else:
                    try:
                        clock = provider_factory().clock_check()
                        state["last_clock_check"] = clock
                    except (ValueError, KeyError, OSError, RuntimeError):
                        clock = {"ok": False}
                if not clock.get("ok"):
                    reasons.append("clock_gate_failed")
            frozen = now_utc()
            published, received = _utc(first["published_at_utc"]), _utc(first["observed_at_utc"])
            if received > frozen or published > received:
                reasons.append("future_publication_or_receipt")
            entry = ceil5(max(received, frozen) + timedelta(seconds=60))
            if entry > published + timedelta(minutes=IMPLEMENTATION_RULES["maximum_entry_delay_after_publication_minutes"]):
                reasons.append("entry_too_late")
            if symbol and not reasons:
                held = [e for e in events.values() if e["status"] in ("planned", "observing", "completed")
                        and timedelta(0) <= entry - _utc(e["planned_entry_at"]) < HOLD]
                if any(e["symbol"] == symbol for e in held):
                    reasons.append("same_coin_position_open")
                elif len(held) >= MAX_OPEN:
                    reasons.append("notional_cap_reached")
            reasons = list(dict.fromkeys(reasons))
            event = {"event_id": event_id, "message_key": key, "symbol": symbol, "base": base,
                     "raw_symbol": signal["raw_symbol"], "side": -1, "call_side": signal["side"], "label": label["label"],
                     "inferred_symbol": label["inferred_symbol"], "original_text": first["text"],
                     "message_revision_sha256": first["revision_sha256"], "published_at_utc": published.isoformat(),
                     "first_received_at_utc": received.isoformat(), "first_receipt_run": self._first_receipt_run(key, run_count),
                     "label_frozen_at_utc": frozen.isoformat(), "planned_entry_at": entry.isoformat(),
                     "planned_exit_at": (entry + HOLD).isoformat(), "notional_usdt": NOTIONAL_USDT,
                     "universe_symbols_sha256": (universe or {}).get("symbols_sha256"),
                     "status": "skipped" if reasons else "planned", "exclusion_reasons": reasons,
                     "primary_sample": not reasons, "collector_health": health, "clock_check": clock}
            return self._append("freeze", event)

    def tick(self, provider_factory, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """One bounded pass: planned entries, then at most one fetch-bearing settlement job."""
        with _archive_lock(self.output):
            records, events = self._state()
            if not records:
                return []
            actions: List[Dict[str, Any]] = []
            provider = None

            def market():
                nonlocal provider
                if provider is None:
                    provider = provider_factory()
                return provider

            for event in sorted(events.values(), key=lambda e: (e["planned_entry_at"], e["event_id"])):
                if event["status"] != "planned":
                    continue
                at = _utc(event["planned_entry_at"])
                if now_utc() < at + timedelta(seconds=10):
                    continue
                if _utc(event["freeze_committed_at"]) >= at:
                    actions.append(self._append("missed", {"event_id": event["event_id"],
                                                           "reason": "freeze_not_committed_before_entry"}))
                    event["status"] = "skipped"
                    continue
                try:
                    rows, evidence = market().klines(event["symbol"], "5m", ms(at), ms(at))
                    price = bars_by_time(rows)[ms(at)][0]
                    record = self._append("entry", {"event_id": event["event_id"], "entry_at": at.isoformat(), "price": price,
                                                    "captured_at_utc": now_utc().isoformat(), "evidence": evidence})
                    actions.append(record)
                    event.update(status="observing", entry=record["payload"])
                except (ValueError, KeyError, OSError, RuntimeError) as exc:
                    if now_utc() > at + timedelta(hours=1):
                        actions.append(self._append("missed", {"event_id": event["event_id"], "reason": "entry_price_unavailable"}))
                        event["status"] = "skipped"
                    else:
                        state.setdefault("errors", []).append({"at": now_utc().isoformat(), "stage": "entry",
                                                               "type": type(exc).__name__})
            for event in events.values():
                if event.get("status") != "observing":
                    continue
                job = self._next_job(event, state)
                if job:
                    actions.extend(self._run_job(job, event, market, state))
                    return actions
            return actions

    def _next_job(self, event: Dict[str, Any], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        def due(key: str) -> bool:
            last = state.setdefault("attempts", {}).get(key)
            return not last or (now_utc() - _utc(last)).total_seconds() >= 60

        exit_at = _utc(event["planned_exit_at"])
        if now_utc() < exit_at + timedelta(minutes=5):
            return None
        late = now_utc() > exit_at + UNAVAILABLE_AFTER
        eid = event["event_id"]
        if event["settlement"] is None:
            return {"kind": "unavailable", "key": f"{eid}:settle", "reason": "price_path_unavailable"} if late else (
                {"kind": "settle", "key": f"{eid}:settle"} if due(f"{eid}:settle") else None)
        if event["null"] is None:
            return {"kind": "unavailable", "key": f"{eid}:null", "reason": "null_unavailable"} if late else (
                {"kind": "null", "key": f"{eid}:null"} if due(f"{eid}:null") else None)
        if not late and any(b.get("net_with_funding") is None for b in event["settlement"]["books"].values()) \
                and due(f"{eid}:funding"):
            return {"kind": "funding", "key": f"{eid}:funding"}
        return None

    def _funded(self, market, event: Dict[str, Any], outcomes: Dict[str, Dict[str, Any]]):
        rows, proof = market().funding_since(event["symbol"], ms(event["entry"]["entry_at"]))
        return {name: funding_net(rows, proof, event["symbol"], outcome, ms(event["entry"]["entry_at"]))
                for name, outcome in outcomes.items()}, proof

    def _run_job(self, job: Dict[str, Any], event: Dict[str, Any], market, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        state["attempts"][job["key"]] = now_utc().isoformat()
        entry_ms = ms(event["entry"]["entry_at"])
        if job["kind"] == "unavailable":
            return [self._append("unavailable", {"event_id": event["event_id"], "reason": job["reason"]})]
        try:
            if job["kind"] == "settle":
                rows, evidence = market().klines(event["symbol"], "5m", entry_ms, entry_ms + HOLD_HOURS * HOUR_MS)
                outcomes = books(bars_by_time(rows), entry_ms)
                if outcomes is None or outcomes["plain"]["entry_price"] != event["entry"]["price"]:
                    raise ValueError("Incomplete path or entry evidence changed")
                try:
                    funded, proof = self._funded(market, event, outcomes)
                except (ValueError, KeyError, OSError, RuntimeError) as exc:
                    funded, proof = {name: None for name in outcomes}, {"unavailable": type(exc).__name__}
                for name, value in funded.items():
                    outcomes[name]["net_with_funding"] = value
                return [self._append("settlement", {"event_id": event["event_id"], "books": outcomes,
                                                    "funding_evidence": proof, "price_evidence": evidence})]
            if job["kind"] == "funding":
                funded, proof = self._funded(market, event, event["settlement"]["books"])
                if any(v is None for v in funded.values()):
                    raise ValueError("Funding coverage unknown")
                return [self._append("funding", {"event_id": event["event_id"], "net_with_funding": funded,
                                                 "funding_evidence": proof})]
            start = entry_ms - (NULL_DAYS + 1) * 86_400_000
            rows, evidence = market().klines(event["symbol"], "1h", start // HOUR_MS * HOUR_MS, entry_ms - 1)
            return [self._append("null", {"event_id": event["event_id"], "profile": null_profile(rows, entry_ms),
                                          "evidence": evidence})]
        except (ValueError, KeyError, OSError, RuntimeError) as exc:
            # Retry in ~5 minutes, not every pass, so a long outage does not flood the journal.
            state["attempts"][job["key"]] = (now_utc() + timedelta(minutes=4)).isoformat()
            state.setdefault("errors", []).append({"at": now_utc().isoformat(), "stage": job["kind"],
                                                   "event_id": event["event_id"], "type": type(exc).__name__})
            return []


def completed(events: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Primary-sample events with a settlement and a usable plain null, in entry order."""
    done = [e for e in events.values() if e.get("primary_sample") and e["status"] == "completed"
            and (e["null"] or {}).get("books", {}).get("plain", {}).get("status") == "ok"]
    return sorted(done, key=lambda e: (e["entry"]["entry_at"], e["event_id"]))


def excess(event: Dict[str, Any], book: str = "plain") -> float:
    """Price-only net result minus the exact pre-entry same-coin hourly-start mean of the same book."""
    return event["settlement"]["books"][book]["net_price_only"] - event["null"]["books"][book]["mean"]


# ---------------------------------------------------------------------------------------- tracker and views
class Tracker:
    def __init__(self, journal: Optional[Journal] = None, provider_factory=None, managed: bool = False):
        self.journal = journal or Journal()
        self.factory = provider_factory or (lambda: PublicMarketData(self.journal.output))
        self.managed = managed

    def save(self, state: Dict[str, Any]) -> None:
        state["errors"] = state.get("errors", [])[-20:]
        _atomic_bytes(self.journal.output / "runtime.json", _json_bytes(state))

    def tick(self) -> Dict[str, Any]:
        with _archive_lock(self.journal.output / "pass"):
            state = runtime(self.journal.output)
            records, _ = self.journal._state()
            if records:
                actions = self.journal.sync(state, self.factory) + self.journal.tick(self.factory, state)
                state["last_actions"] = [{"kind": a["kind"], "event_id": a["payload"].get("event_id")} for a in actions]
            state.update(last_tick_at_utc=now_utc().isoformat(), continuous_runner=self.managed, process_id=os.getpid(),
                         last_pass_error=None, live_orders=False)
            self.save(state)
            return state


def _mean(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _not_configured(reason: str) -> Dict[str, Any]:
    return {"retirement": {"n": 0, "min_n": TARGET_N, "mean_pct": None, "ci90_pct": None, "verdict": "collecting",
                           "label": "未激活", "reason": reason, "clusters": None},
            "started_at": None, "updated_at": None, "runner_active": False, "paper_only": True, "tracker_status_label": "未激活",
            "forward_open": 0, "forward_waiting": 0, "forward_late": 0, "review_gate": "not_activated",
            "observation_count": 0, "execution_authorized": False, "last_error": None}


def book_summary(done: List[Dict[str, Any]], book: str) -> Dict[str, Any]:
    nets = [e["settlement"]["books"][book]["net_price_only"] for e in done]
    funded = [e["settlement"]["books"][book].get("net_with_funding") for e in done]
    funded = [v for v in funded if v is not None]
    with_null = [excess(e, book) for e in done if (e["null"]["books"][book] or {}).get("status") == "ok"]
    return {"n": len(done), "mean_net_pct": _mean([v * 100 for v in nets]),
            "mean_net_stress_pct": _mean([e["settlement"]["books"][book]["net_price_only_stress"] * 100 for e in done]),
            "funded_n": len(funded), "mean_funded_pct": _mean([v * 100 for v in funded]),
            "mean_excess_pp": _mean([v * 100 for v in with_null]),
            "pnl_usdt_price_only": round(sum(nets) * NOTIONAL_USDT, 2),
            "exit_reasons": {r: sum(e["settlement"]["books"][book]["exit_reason"] == r for e in done)
                             for r in ("time", "stop", "liquidated")}}


def summary(journal: Optional[Journal] = None) -> Dict[str, Any]:
    if journal is None:
        try:
            journal = Journal()
        except FileNotFoundError:
            return _not_configured("协议文件缺失")
    records, events = journal._state()
    state = runtime(journal.output)
    stamp = state.get("last_tick_at_utc")
    running = bool(records and state.get("continuous_runner") and not state.get("last_pass_error") and stamp
                   and 0 <= (now_utc() - _utc(stamp)).total_seconds() <= 60)
    done = completed(events)
    gate = journal.gate(records, events) if records else "not_activated"
    primary = [excess(e) * 100 for e in done]
    calls = sorted(events.values(), key=lambda e: e["published_at_utc"])
    last = calls[-1] if calls else None
    status = ("未激活" if not records else "运行异常" if state.get("last_pass_error") else "运行心跳待恢复" if not running
              else "运行中 · 持仓待结算" if any(e["status"] in ("planned", "observing") for e in events.values())
              else "运行中 · 等待新喊单")
    verdict = "review_due" if gate and records and not gate.startswith(("await", "not_")) else "collecting"
    seen = state.get("seen", {})
    label_counts: Dict[str, int] = {}
    for value in seen.values():
        label_counts[value] = label_counts.get(value, 0) + 1
    activated = records[0]["recorded_at_utc"] if records else None
    return {
        "retirement": {"n": len(done), "min_n": TARGET_N, "mean_pct": _mean(primary), "ci90_pct": None, "verdict": verdict,
                       "label": "待协议复核" if verdict == "review_due" else "积累新样本" if records else "未激活",
                       "reason": "next10xgem1 首次收件的无条件做多喊单，下一5分钟开盘做空永续，固定持有24小时；"
                                 "主统计为较入场前30天同币逐小时起点24h做空精确均值的超额（价格口径、20bps）；"
                                 f"245笔无效性检查，490笔复核。{('当前门槛：' + gate) if gate else ''}", "clusters": None},
        "started_at": activated, "updated_at": stamp or activated, "runner_active": running, "continuous_runner": running,
        "paper_only": True, "tracker_status_label": status,
        "forward_open": sum(e["status"] == "observing" for e in events.values()),
        "forward_waiting": sum(e["status"] == "planned" for e in events.values()),
        "forward_late": sum(e.get("missed", {}).get("reason") in ("freeze_not_committed_before_entry", "entry_price_unavailable")
                            or "entry_too_late" in e.get("exclusion_reasons", []) for e in events.values()),
        "review_gate": gate, "observation_count": len(events),
        "primary_sample_count": sum(bool(e.get("primary_sample")) for e in events.values()),
        "skipped_count": sum(e["status"] == "skipped" for e in events.values()),
        "unavailable_count": sum(e["status"] == "unavailable" for e in events.values()),
        "posts_seen_after_activation": label_counts,
        "books": {"plain": book_summary(done, "plain"), "stop50": book_summary(done, "stop50")},
        "notional_usdt_per_trade": NOTIONAL_USDT, "max_open_positions": MAX_OPEN,
        "last_call": ({"published_at_utc": last["published_at_utc"], "symbol": last["symbol"] or last["raw_symbol"],
                       "status": last["status"], "exclusion_reasons": last["exclusion_reasons"]} if last else None),
        "collector_health": journal.health() if records else None,
        "execution_authorized": False,
        "last_error": (state.get("errors") or [None])[-1]}


def detail() -> Dict[str, Any]:
    journal = Journal()
    _, events = journal._state()
    return {"summary": summary(journal), "events": list(events.values())[-100:]}


async def worker(stop_event: asyncio.Event, heartbeat=None) -> None:
    while True:  # a machine without the protocol file waits instead of crash-looping
        try:
            tracker = Tracker(managed=True)
            break
        except FileNotFoundError:
            if heartbeat:
                heartbeat(False)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=60)
                return
            except asyncio.TimeoutError:
                continue
    with _archive_lock(tracker.journal.output / "owner"):
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.to_thread(tracker.tick)
                    if heartbeat:
                        heartbeat(True)
                except Exception as exc:  # noqa: BLE001 - recorded, retried next pass
                    state = runtime(tracker.journal.output)
                    state.setdefault("errors", []).append({"at": now_utc().isoformat(), "type": type(exc).__name__,
                                                           "reason": str(exc)[:180]})
                    state["last_pass_error"] = type(exc).__name__
                    tracker.save(state)
                    if heartbeat:
                        heartbeat(False)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass
        finally:
            tracker.managed = False
            state = runtime(tracker.journal.output)
            state["continuous_runner"] = False
            tracker.save(state)
