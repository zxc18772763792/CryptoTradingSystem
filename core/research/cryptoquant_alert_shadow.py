"""Forward-only shadow ledger for the CryptoQuant Alert channel (@cryptoquant_alert). Records only; no orders.

The news collector's public-page polling archives every first receipt of the channel
(``core.news.alastar_observation``).  This module turns each *deterministically parsed* BTC
exchange-flow alert that was first received after activation into one frozen shadow observation:
the planned entry is the next 5-minute open at least 60 seconds after receipt, four exits
(1/4/24/72 h) are fixed together, and each exit is compared with the exact mean of every
same-side start in the preceding 30 days.  Every number is public Binance USD-M data.

The protocol (``prospective_shadow_protocol_v1.json``) is hash-pinned in every journal record, so a
changed rule can only start a new journal.  This module has no scheduler of its own, never calls an
AI provider, and never touches an account or order endpoint.
"""
from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

from core.news.alastar_observation import (
    _archive_lock, _atomic_bytes, _json_bytes, _load_runs, _utc, archive_dir,
)

ROOT = Path(__file__).resolve().parents[2]
CHANNEL = "cryptoquant_alert"
TRACKER_ID = "cryptoquant_alert_shadow_v1"
PROTOCOL_ID = "cryptoquant_alert_btc_flow_shadow_v1"
OUTPUT = ROOT / "data/research/cryptoquant_alert_shadow_v1"
PROTOCOL = ROOT / "data/research/cryptoquant_alert_20261024/prospective_shadow_protocol_v1.json"
DEV_SAMPLE = ROOT / "data/research/cryptoquant_alert_20261020/messages.jsonl"
SYMBOL = "BTCUSDT"
HOURS = (1, 4, 24, 72)
PRIMARY_HOURS = 24
STEP_MS = 300_000
STEP = timedelta(minutes=5)
COST_BPS = 20
MIN_NULL_STARTS = 2000
POLL_SECONDS = 15
KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
TIME_URL = "https://fapi.binance.com/fapi/v1/time"
IMPLEMENTATION_RULES = {
    "version": "cq-alert-shadow-20261024",
    "entry": "next 5m open at least 60s after max(first receipt, freeze commit)",
    "entry_price": "official 5m kline open at the planned entry; a freeze committed after it is recorded as missed",
    "maximum_entry_delay_after_publication_minutes": 30,
    "collector_gap_seconds_max": 600,
    "clock_max_abs_offset_seconds": 2,
    "clock_max_round_trip_seconds": 10,
    "clock_samples": 5,
    "primary": "price-only 20bps round trip, excess over the exact 30-day pre-entry same-side mean of the same horizon",
}


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ceil5(at: Any) -> datetime:
    return datetime.fromtimestamp(math.ceil(_utc(at).timestamp() / 300) * 300, timezone.utc)


def number(value: Any, *, positive: bool = False) -> float:
    value = float(value)
    if not math.isfinite(value) or (positive and value <= 0):
        raise ValueError("Nonfinite or nonpositive market value")
    return value


# ------------------------------------------------------------------------------------------------ parser
_NUM = r"[\d,]+(?:\.\d+)?"
_HEAD = rf"(?P<btc>{_NUM}) #(?P<asset>[A-Z0-9]{{2,12}}) \(\$(?P<usd>{_NUM})\)"
_EMOJI = r"(?P<emoji>[\U0001F6A8\U0001F433✅]{1,3})"
_SIMPLE = re.compile(
    rf"{_EMOJI} {_HEAD}\naggregated (?:inflows to #(?P<venue>[A-Za-z0-9]+(?: [A-Za-z0-9]+)*)|"
    rf"(?P<miners>flows from miners to exchanges))\n\nView Chart \| @cryptoquant_alert\Z")
_UNUSUAL = re.compile(
    rf"{_EMOJI} Unusual Exchange (?P<dir>Inflows|Outflows)\n{_HEAD}\naggregated "
    rf"(?:inflows to (?P<scope>spot|derivative) exchanges|outflows from multiple exchanges) in an hour\n\n"
    rf"(?P<rows>(?:\d{{1,2}}\. [A-Za-z0-9_]+ {_NUM} BTC \(\d{{1,3}}%\)\n)+)\nPlease note")
_ROW = re.compile(rf"\d{{1,2}}\. ([A-Za-z0-9_]+) ({_NUM}) BTC \((\d{{1,3}})%\)")


def _clean(text: str) -> str:
    text = str(text).replace("\xa0", " ").replace("\r\n", "\n")
    return "\n".join(line.rstrip() for line in text.split("\n")).strip()


def _float(text: str) -> float:
    return float(text.replace(",", ""))


def parse_alert(text: str) -> Optional[Dict[str, Any]]:
    """The structured fields of one observed alert template, or None (anything else is never a signal)."""
    body = _clean(text)
    match = _SIMPLE.fullmatch(body)
    if match:
        miners = match.group("miners") is not None
        return {"template": "simple_miners" if miners else "simple_inflow",
                "kind": "miner_to_exchange" if miners else "exchange_inflow", "side": -1,
                "asset": match.group("asset"), "btc": _float(match.group("btc")), "usd": _float(match.group("usd")),
                "venue": None if miners else match.group("venue"), "scope": None,
                "tier": len(match.group("emoji")), "breakdown": []}
    match = _UNUSUAL.match(body)
    if match:
        inflow = match.group("dir") == "Inflows"
        return {"template": "unusual_inflow" if inflow else "unusual_outflow",
                "kind": "exchange_inflow" if inflow else "exchange_outflow", "side": -1 if inflow else 1,
                "asset": match.group("asset"), "btc": _float(match.group("btc")), "usd": _float(match.group("usd")),
                "venue": None, "scope": match.group("scope") or "multiple", "tier": len(match.group("emoji")),
                "breakdown": [{"venue": v, "btc": _float(b), "pct": int(p)} for v, b, p in _ROW.findall(match.group("rows"))]}
    return None


def development_sample_profile(path: Path = DEV_SAMPLE) -> Dict[str, Any]:
    """Parser output over the 10/20 offline sample; frozen in the protocol, re-checked at activation."""
    raw = Path(path).read_bytes()
    counts: Dict[str, int] = {}
    total = 0
    for line in raw.decode("utf-8-sig").splitlines():
        if not line.strip():
            continue
        total += 1
        parsed = parse_alert(json.loads(line).get("content") or "")
        key = parsed["template"] if parsed else "unparsed"
        counts[key] = counts.get(key, 0) + 1
    return {"file": str(Path(path).relative_to(ROOT)).replace("\\", "/"), "sha256": sha(raw), "messages": total,
            "by_template": dict(sorted(counts.items()))}


# ------------------------------------------------------------------------------------------ market math
def bars_by_time(rows: List[List[Any]]) -> Dict[int, tuple]:
    """Gaps stay gaps; inconsistent duplicates and zero-print bars are never turned into executable opens."""
    out: Dict[int, tuple] = {}
    for row in rows:
        if len(row) < 5:
            raise ValueError("Invalid official kline row")
        stamp = int(row[0])
        if stamp % STEP_MS:
            raise ValueError("Kline is not aligned to 5m")
        o, h, lo, c = [number(v, positive=True) for v in row[1:5]]
        if h < max(o, c) or lo > min(o, c) or h < lo:
            raise ValueError("Invalid OHLC geometry")
        if len(row) >= 9:
            volume, trades = number(row[5]), number(row[8])
            if min(volume, trades) < 0:
                raise ValueError("Negative volume/trade count")
            if volume == 0 or trades == 0:
                continue
        bar = (o, h, lo, c)
        if stamp in out and out[stamp] != bar:
            raise ValueError("Inconsistent duplicate kline")
        out[stamp] = bar
    return out


def price_outcome(bars: Dict[int, tuple], at: Any, hours: int, side: int, cost_bps: int = COST_BPS) -> Optional[Dict[str, Any]]:
    """Fixed-horizon result on the 5m open path; the exit bar's own high/low are never read."""
    if hours not in HOURS or type(side) is not int or side not in (-1, 1):
        raise ValueError("Unsupported horizon/side")
    start = int(_utc(at).timestamp() * 1000)
    steps = hours * 12
    if start not in bars:
        return None
    entry = bars[start][0]
    for i in range(steps):
        stamp = start + i * STEP_MS
        if stamp not in bars:
            return None
        bar = bars[stamp]
        if (bar[1] >= entry * 1.99) if side == -1 else (bar[2] <= entry * 0.005):
            return {"entry_price": entry, "exit_price": None, "net_price_only": -1.0, "liquidated": True,
                    "liquidation_bar_start_ms": stamp}
    end = start + steps * STEP_MS
    if end not in bars:
        return None
    return {"entry_price": entry, "exit_price": bars[end][0], "liquidated": False,
            "net_price_only": max(-1.0, side * (bars[end][0] / entry - 1) - cost_bps / 10000)}


def null_profile(rows: List[List[Any]], entry: Any, side: int, minimum: int = MIN_NULL_STARTS) -> Dict[str, Any]:
    """Exact mean outcome of every valid 5m start in the 30 days before entry, per horizon (not a sample)."""
    if _utc(entry) != ceil5(entry):
        raise ValueError("Entry must be aligned to 5m")
    bars = bars_by_time(rows)
    stop = int(_utc(entry).timestamp() * 1000)
    start = stop - 30 * 86_400_000
    grid = [bars.get(t) for t in range(start, stop + 1, STEP_MS)]
    n = len(grid)
    next_missing = [n] * n
    missing = n
    for i in range(n - 1, -1, -1):
        if grid[i] is None:
            missing = i
        next_missing[i] = missing
    values: Dict[int, List[Optional[float]]] = {}
    for hours in HOURS:
        steps = hours * 12
        out: List[Optional[float]] = [None] * n
        window: deque = deque()
        right = 0
        for i in range(n - steps):
            end = min(i + steps, next_missing[i])
            while right < end:
                bar = grid[right]
                if bar is not None:
                    value = bar[1] if side == -1 else -bar[2]
                    while window and window[-1][1] <= value:
                        window.pop()
                    window.append((right, value))
                right += 1
            while window and window[0][0] < i:
                window.popleft()
            bar = grid[i]
            if bar is None:
                continue
            hit = window and (window[0][1] >= bar[0] * 1.99 if side == -1 else -window[0][1] <= bar[0] * 0.005)
            if hit:
                out[i] = -1.0
            elif next_missing[i] >= i + steps and grid[i + steps] is not None:
                out[i] = max(-1.0, side * (grid[i + steps][0] / bar[0] - 1) - COST_BPS / 10000)
        values[hours] = out
    result: Dict[str, Any] = {"cost_bps": COST_BPS, "funding_in_null": False, "latest_null_price_at": _utc(entry).isoformat(),
                              "horizons": {}}
    for hours in HOURS:
        own = [v for v in values[hours] if v is not None]
        result["horizons"][str(hours)] = {"n": len(own), "mean": sum(own) / len(own) if own else None,
                                          "status": "ok" if len(own) >= minimum else "insufficient_preentry_history"}
    if grid[-1] is not None:
        result["context"] = {"entry_price": grid[-1][0],
                             "return_prior_24h": grid[-1][0] / grid[-1 - 288][0] - 1 if n > 288 and grid[-1 - 288] else None,
                             "return_prior_7d": grid[-1][0] / grid[-1 - 2016][0] - 1 if n > 2016 and grid[-1 - 2016] else None}
    return result


def funding_net(rows: List[Dict[str, Any]], proof: Dict[str, Any], entry_price: float, side: int, start: Any, end: Any,
                outcome: Dict[str, Any]) -> Optional[float]:
    """Price result plus actual funding; a payment exactly at either boundary counts only if adverse."""
    if proof.get("complete_query_coverage") is not True:
        return None
    a, b = int(_utc(start).timestamp() * 1000), int(_utc(end).timestamp() * 1000)
    seen: set = set()
    total = 0.0
    liquidation = outcome.get("liquidation_bar_start_ms")
    for row in rows:
        stamp = int(row["fundingTime"])
        if stamp in seen or not a <= stamp <= b or row.get("symbol") != SYMBOL:
            raise ValueError("Duplicate/out-of-window/wrong-symbol funding")
        seen.add(stamp)
        if liquidation is not None and stamp >= liquidation + STEP_MS:
            continue
        payment = -side * number(row["fundingRate"]) * number(row["markPrice"], positive=True) / entry_price
        boundary = min(abs(stamp - a), abs(stamp - b)) <= 1000 or (liquidation is not None and stamp >= liquidation)
        total += min(0.0, payment) if boundary else payment
    return -1.0 if outcome["liquidated"] else max(-1.0, outcome["net_price_only"] + total)


class PublicMarketData:
    """Public Binance USD-M reads with the response kept as hash-named evidence. No key, no account endpoint."""

    def __init__(self, output: Path, *, max_calls: int = 80):
        import requests
        from config.settings import settings
        from core.utils.proxy_env import ensure_proxy_env
        ensure_proxy_env(settings)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "cq-alert-shadow-research/1", "Cache-Control": "no-cache"})
        self.output, self.calls, self.max_calls, self.failed = Path(output), 0, max_calls, False

    def _fetch(self, url: str, params: Dict[str, Any]):
        if url not in (KLINES_URL, FUNDING_URL, TIME_URL):
            raise ValueError("Unsupported public endpoint")
        if self.failed or self.calls >= self.max_calls:
            raise RuntimeError("Public data pass stopped or request bound reached")
        self.calls += 1
        requested = now_utc()
        try:
            response = self.session.get(url, params=params, timeout=(5, 15))
            response.raise_for_status()
            if len(response.content) > 2 * 1024 * 1024:
                raise ValueError("Public response exceeds evidence bound")
            rows = response.json()
        except Exception as exc:
            self.failed = True
            raise RuntimeError(f"Official public data unavailable ({type(exc).__name__})") from None
        received = now_utc()
        record = {"endpoint": url, "params": params, "requested_at_utc": requested.isoformat(),
                  "received_at_utc": received.isoformat(), "response_sha256": sha(response.content), "rows": rows}
        raw = _json_bytes(record)
        relative = f"evidence/{sha(raw)}.json"
        if not (self.output / relative).exists():
            _atomic_bytes(self.output / relative, raw)
        return rows, {"file": relative, "sha256": sha(raw)}, requested, received

    def clock_check(self, samples: int = IMPLEMENTATION_RULES["clock_samples"]) -> Dict[str, Any]:
        reads = []
        for _ in range(max(1, int(samples))):
            payload, evidence, requested, received = self._fetch(TIME_URL, {})
            midpoint = requested + (received - requested) / 2
            reads.append({"offset_seconds": payload["serverTime"] / 1000 - midpoint.timestamp(),
                          "round_trip_seconds": (received - requested).total_seconds(),
                          "offset_interval_seconds": [payload["serverTime"] / 1000 - received.timestamp(),
                                                      payload["serverTime"] / 1000 - requested.timestamp()],
                          "checked_at_utc": received.isoformat(), "evidence": evidence})
        best = dict(min(reads, key=lambda r: r["round_trip_seconds"]))
        best["ok"] = (max(abs(v) for v in best["offset_interval_seconds"]) <= IMPLEMENTATION_RULES["clock_max_abs_offset_seconds"]
                      and best["round_trip_seconds"] <= IMPLEMENTATION_RULES["clock_max_round_trip_seconds"])
        best["reads"], best["selection"] = reads, "minimum round trip of all reads"
        return best

    def klines(self, start: Any, end: Any):
        start, end = _utc(start), _utc(end)
        if end > now_utc() or end < start or end - start > timedelta(days=30):
            raise ValueError("Kline window must be historical and at most 30 days")
        cursor, stop = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        rows: List[List[Any]] = []
        evidence = []
        while cursor <= stop:
            finish = min(stop, cursor + 1499 * STEP_MS)
            part, receipt, _, _ = self._fetch(KLINES_URL, {"symbol": SYMBOL, "interval": "5m", "startTime": cursor,
                                                         "endTime": finish, "limit": 1500})
            if not isinstance(part, list) or len(part) > 1500 or any(not cursor <= int(r[0]) <= finish for r in part):
                raise ValueError("Invalid official klines payload")
            rows.extend(part)
            evidence.append(receipt)
            cursor = finish + STEP_MS
        return rows, evidence

    def funding(self, start: Any, end: Any):
        start, end = _utc(start), _utc(end)
        if end - start not in [timedelta(hours=h) for h in HOURS] or end > now_utc():
            raise ValueError("Invalid completed public funding window")
        a, b = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        rows, receipt, _, _ = self._fetch(FUNDING_URL, {"symbol": SYMBOL, "startTime": a, "endTime": b, "limit": 1000})
        if not isinstance(rows, list) or len(rows) >= 1000 or any(r.get("symbol") != SYMBOL for r in rows):
            raise ValueError("Funding query incomplete or wrong symbol")
        return rows, {**receipt, "complete_query_coverage": True, "start_ms": a, "end_ms": b}


# ------------------------------------------------------------------------------------------------ journal
def runtime(output: Path) -> Dict[str, Any]:
    path = Path(output) / "runtime.json"
    return json.loads(path.read_bytes()) if path.exists() else {"attempts": {}, "errors": [], "unparsed": {}, "counts": {}}


class Journal:
    """Hash-chained, protocol- and implementation-pinned record of every observation and its settlement."""

    def __init__(self, output: Path = OUTPUT, archive: Optional[Path] = None, protocol: Path = PROTOCOL):
        self.output = Path(output)
        self.archive = Path(archive) if archive is not None else archive_dir(CHANNEL)
        self.protocol_path = Path(protocol)
        self.protocol_raw = self.protocol_path.read_bytes()
        self.protocol = json.loads(self.protocol_raw)
        book = self.protocol.get("book") or {}
        if (self.protocol.get("protocol_id") != PROTOCOL_ID or book.get("symbol") != SYMBOL
                or book.get("holding_hours") != list(HOURS) or book.get("primary_holding_hours") != PRIMARY_HOURS
                or book.get("round_trip_cost_bps") != COST_BPS
                or (self.protocol.get("control") or {}).get("minimum_null_starts") != MIN_NULL_STARTS
                or self.protocol.get("execution_authorized") is not False):
            raise ValueError("Protocol parameters differ from the implementation; review before use")
        self.protocol_sha = sha(self.protocol_raw)
        self.paths = {"shadow": Path(__file__)}  # the CLI only drives this module; it carries no market rule
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
                raise RuntimeError("Shadow journal hash, protocol or clock sequence is invalid")
            self._cache.append(record)
            previous, last_at, position = sha(raw), at, position + 1
        self._cache_tail = previous
        checkpoint = self.output / "checkpoint.json"
        if checkpoint.exists():
            saved = json.loads(checkpoint.read_bytes())
            tail = directory / f"{saved['sequence']:012d}.json"
            if saved["sequence"] > len(self._cache) or sha(tail.read_bytes()) != saved["sha256"]:
                raise RuntimeError("Shadow journal checkpoint hash is invalid")
        return self._cache

    def verify(self) -> Dict[str, Any]:
        """Full re-read of the chain from disk (the cached path trusts what it already verified)."""
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
            raise RuntimeError("Refusing to overwrite a committed shadow record")
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
            p = record["payload"]
            kind = record["kind"]
            if kind == "freeze":
                events[p["event_id"]] = {**p, "freeze_committed_at": record["recorded_at_utc"], "outcomes": {}, "null": None}
            elif kind in ("entry", "missed", "null", "settlement", "funding"):
                event = events[p["event_id"]]
                if kind == "entry":
                    event.update(status="observing", entry=p)
                elif kind == "missed":
                    event.update(status="skipped", missed=p)
                elif kind == "null":
                    event["null"] = p["profile"]
                elif kind == "funding":
                    event["outcomes"][str(p["hours"])].update(net_with_funding=p["net_with_funding"], funding_evidence=p["funding_evidence"])
                else:
                    event["outcomes"][str(p["hours"])] = p
                    if len(event["outcomes"]) == len(HOURS):
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
                "requirement": f"four latest live polls, each gap<={limit}s, latest no older than {limit}s",
                "delivery_sla_or_subminute_guarantee": False}

    def _first_receipt_run(self, key: str, count: int) -> Dict[str, Any]:
        for sequence in range(count, max(0, count - 400), -1):
            path = self.archive / "runs" / f"{sequence:012d}.json"
            raw = path.read_bytes()
            if any(x["message_key"] == key for x in json.loads(raw)["new_revisions"]):
                return {"file": f"runs/{sequence:012d}.json", "sha256": sha(raw)}
        return {"file": None, "sha256": None}

    # -- life cycle --------------------------------------------------------------------------------------
    def activate(self, clock: Dict[str, Any]) -> Dict[str, Any]:
        health = self.health()
        profile = development_sample_profile()
        expected = (self.protocol.get("development_sample") or {})
        if (profile["sha256"] != expected.get("sha256") or profile["messages"] != expected.get("messages")
                or profile["by_template"] != expected.get("by_template")):
            raise RuntimeError("Parser output over the development sample differs from the pre-registered profile")
        if not health["healthy_for_new_freezes"] or not clock.get("ok"):
            raise RuntimeError("Shadow activation prerequisites are not satisfied")
        with _archive_lock(self.output):
            if self._records():
                raise RuntimeError("Shadow registration already exists; never reset it")
            return self._append("activation", {"paper_only": True, "live_orders": False, "protocol_snapshot": self.protocol,
                                               "implementation_rules": IMPLEMENTATION_RULES, "implementation_sources": self.sources,
                                               "collector_health": health, "clock_check": clock, "development_sample": profile})

    def gate(self, records: List[Dict[str, Any]], events: Dict[str, Dict[str, Any]]) -> Optional[str]:
        """Predeclared sample/time gates over primary-sample events; never an early stop for success."""
        if not records:
            return "not_activated"
        activation = _utc(records[0]["recorded_at_utc"])
        try:
            deadline = activation.replace(year=activation.year + 2)
        except ValueError:
            deadline = activation.replace(year=activation.year + 2, day=28)
        if now_utc() >= deadline:
            return "maximum24months_reached"
        primary = [e for e in events.values() if e.get("primary_sample") and e["status"] in ("observing", "completed")]
        done = sorted((e for e in primary if e["status"] == "completed"), key=lambda e: (e["entry"]["entry_at"], e["event_id"]))
        active = len(primary) - len(done)
        if len(done) >= 66:
            return "confirmatory66_review_due"
        if len(done) >= 40:
            first = done[:40]
            if any((e["null"] or {}).get("horizons", {}).get(str(PRIMARY_HOURS), {}).get("status") != "ok" for e in first):
                return "futility40_null_incomplete"
            if sum(excess(e, PRIMARY_HOURS) for e in first) / 40 <= 0:
                return "futility40_stop"
        if len(done) < 40 and len(done) + active >= 40:
            return "await40_maturity"
        if len(done) + active >= 66:
            return "await66_maturity"
        return None

    def sync(self, state: Dict[str, Any], provider_factory) -> List[Dict[str, Any]]:
        """Freeze every message first received live after activation; unparseable text is counted, not journaled."""
        records, events = self._state()
        if not records:
            return []
        activated = _utc(records[0]["recorded_at_utc"])
        try:
            count, messages, _ = _load_runs(self.archive, channel=CHANNEL)
        except (OSError, ValueError, KeyError):
            return []
        actions = []
        ordered = sorted(messages.items(), key=lambda pair: pair[1]["first"]["message_id"])
        for key, item in ordered:
            first = item["first"]
            if key in events or first.get("published_at_utc") is None or _utc(first["published_at_utc"]) < activated:
                continue
            parsed = parse_alert(first["text"])
            if parsed is None:
                state.setdefault("unparsed", {}).setdefault(key, {"text_sha256": first["text_sha256"], "seen_at": now_utc().isoformat()})
                continue
            actions.append(self._freeze(key, item, parsed, count, provider_factory, state))
            records, events = self._state()
        return actions

    def _freeze(self, key: str, item: Dict[str, Any], parsed: Dict[str, Any], run_count: int, provider_factory,
                state: Dict[str, Any]) -> Dict[str, Any]:
        first = item["first"]
        with _archive_lock(self.output):
            records, events = self._state()
            reasons = list(first.get("exclusion_reasons") or [])
            if len(item["revisions"]) > 1:
                reasons.append("revised_before_freeze")
            if parsed["asset"] != "BTC":
                reasons.append("non_btc_asset")
            health = self.health()
            if not health["healthy_for_new_freezes"]:
                reasons.append("collector_health_failed")
            gate = self.gate(records, events)
            if gate:
                reasons.append(gate)
            if not first.get("prospective_eligible_at_first_observation"):
                reasons.append("not_timely_live_first_receipt")
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
            reasons = list(dict.fromkeys(reasons))
            # Independence rule: an alert whose entry follows another eligible entry within 24h is still recorded
            # and settled, but only first-in-24h alerts count toward the predeclared statistics.
            overlap = any(e["status"] != "skipped" and e.get("planned_entry_at")
                          and timedelta(0) <= entry - _utc(e["planned_entry_at"]) < timedelta(hours=24) for e in events.values())
            event = {"event_id": key, "source_id": key, "symbol": SYMBOL, "side": parsed["side"], "alert": parsed,
                     "original_text": first["text"], "message_revision_sha256": first["revision_sha256"],
                     "published_at_utc": published.isoformat(), "first_received_at_utc": received.isoformat(),
                     "first_receipt_run": self._first_receipt_run(key, run_count), "label_frozen_at_utc": frozen.isoformat(),
                     "planned_entry_at": entry.isoformat(),
                     "planned_exits": {str(h): (entry + timedelta(hours=h)).isoformat() for h in HOURS},
                     "status": "skipped" if reasons else "planned", "exclusion_reasons": reasons,
                     "primary_sample": not reasons and not overlap, "overlaps_prior_entry_within_24h": overlap,
                     "collector_health": health, "clock_check": clock}
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
                why = None
                if _utc(event["freeze_committed_at"]) >= at:
                    why = "freeze_not_committed_before_entry"
                else:
                    why = self.gate(records, events) if event["primary_sample"] else None
                if why:
                    actions.append(self._append("missed", {"event_id": event["event_id"], "reason": why}))
                    event["status"] = "skipped"
                    continue
                try:
                    rows, evidence = market().klines(at, at)
                    price = bars_by_time(rows)[int(at.timestamp() * 1000)][0]
                    record = self._append("entry", {"event_id": event["event_id"], "entry_at": at.isoformat(), "price": price,
                                                    "captured_at_utc": now_utc().isoformat(), "evidence": evidence})
                    actions.append(record)
                    event.update(status="observing", entry=record["payload"])
                except (ValueError, KeyError, OSError, RuntimeError) as exc:
                    if now_utc() > at + timedelta(hours=1):
                        actions.append(self._append("missed", {"event_id": event["event_id"], "reason": "entry_price_unavailable"}))
                        event["status"] = "skipped"
                    else:
                        state.setdefault("errors", []).append({"at": now_utc().isoformat(), "stage": "entry", "type": type(exc).__name__})
            for event in events.values():
                if not event.get("entry"):
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

        for hours in HOURS:
            key = f"{event['event_id']}:{hours}"
            end = _utc(event["planned_exits"][str(hours)])
            if str(hours) not in event["outcomes"] and now_utc() >= end + timedelta(minutes=5) and due(key):
                return {"kind": "settle", "hours": hours, "key": key}
        for hours, outcome in event["outcomes"].items():
            key = f"{event['event_id']}:funding:{hours}"
            if outcome.get("net_with_funding") is None and due(key):
                return {"kind": "funding", "hours": int(hours), "key": key}
        key = f"{event['event_id']}:null"
        if event["outcomes"] and not event.get("null") and due(key):
            return {"kind": "null", "key": key}
        return None

    def _run_job(self, job: Dict[str, Any], event: Dict[str, Any], market, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        state["attempts"][job["key"]] = now_utc().isoformat()
        entry_at = _utc(event["planned_entry_at"])
        side = event["side"]
        try:
            if job["kind"] == "settle":
                hours = job["hours"]
                end = _utc(event["planned_exits"][str(hours)])
                rows, evidence = market().klines(entry_at, end)
                bars = bars_by_time(rows)
                outcome = price_outcome(bars, entry_at, hours, side)
                if outcome is None or outcome["entry_price"] != event["entry"]["price"]:
                    raise ValueError("Incomplete path or entry evidence changed")
                for earlier, old in event["outcomes"].items():
                    if int(earlier) < hours:
                        again = price_outcome(bars, entry_at, int(earlier), side)
                        if again is None or again["net_price_only"] != old["net_price_only"]:
                            raise ValueError("Previously recorded price outcome changed")
                funded, proof = None, None
                try:
                    funds, proof = market().funding(entry_at, end)
                    funded = funding_net(funds, proof, event["entry"]["price"], side, entry_at, end, outcome)
                except (ValueError, KeyError, OSError, RuntimeError) as exc:
                    proof = {"unavailable": type(exc).__name__}
                return [self._append("settlement", {
                    "event_id": event["event_id"], "hours": hours, **outcome,
                    "net_price_only_40bps": max(-1.0, outcome["net_price_only"] - 0.002),
                    "net_price_only_80bps": max(-1.0, outcome["net_price_only"] - 0.006),
                    "net_with_funding": funded, "funding_evidence": proof, "price_evidence": evidence})]
            if job["kind"] == "funding":
                hours = job["hours"]
                end = _utc(event["planned_exits"][str(hours)])
                outcome = event["outcomes"][str(hours)]
                funds, proof = market().funding(entry_at, end)
                funded = funding_net(funds, proof, event["entry"]["price"], side, entry_at, end, outcome)
                if funded is None:
                    raise ValueError("Funding coverage unknown")
                return [self._append("funding", {"event_id": event["event_id"], "hours": hours, "net_with_funding": funded,
                                                 "funding_evidence": proof})]
            rows, evidence = market().klines(entry_at - timedelta(days=30), entry_at)
            return [self._append("null", {"event_id": event["event_id"], "profile": null_profile(rows, entry_at, side),
                                          "evidence": evidence})]
        except (ValueError, KeyError, OSError, RuntimeError) as exc:
            # Retry in ~5 minutes, not every pass, so a long outage does not flood the journal.
            state["attempts"][job["key"]] = (now_utc() + timedelta(minutes=4)).isoformat()
            kind = {"settle": "settlement_unavailable", "funding": "funding_unavailable", "null": "null_unavailable"}[job["kind"]]
            return [self._append(kind, {"event_id": event["event_id"], "hours": job.get("hours"), "reason": type(exc).__name__})]


def excess(event: Dict[str, Any], hours: int) -> float:
    """Price-only net result minus the exact pre-entry same-side mean of that horizon."""
    return event["outcomes"][str(hours)]["net_price_only"] - event["null"]["horizons"][str(hours)]["mean"]


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
    return {"retirement": {"n": 0, "min_n": 66, "mean_pct": None, "ci90_pct": None, "verdict": "collecting", "label": "未激活",
                           "reason": reason, "clusters": None},
            "started_at": None, "updated_at": None, "runner_active": False, "paper_only": True, "tracker_status_label": "未激活",
            "forward_open": 0, "forward_waiting": 0, "forward_late": 0, "horizon_comparison": [], "review_gate": "not_activated",
            "observation_count": 0, "execution_authorized": False, "last_error": None}


def summary(journal: Optional[Journal] = None) -> Dict[str, Any]:
    if journal is None:
        try:
            journal = Journal()
        except FileNotFoundError:
            return _not_configured("协议文件不在本机（data/ 不入库）")
    records, events = journal._state()
    state = runtime(journal.output)
    stamp = state.get("last_tick_at_utc")
    running = bool(records and state.get("continuous_runner") and not state.get("last_pass_error") and stamp
                   and 0 <= (now_utc() - _utc(stamp)).total_seconds() <= 60)
    done = [e for e in events.values() if e["status"] == "completed" and e.get("primary_sample")
            and (e["null"] or {}).get("horizons", {}).get(str(PRIMARY_HOURS), {}).get("status") == "ok"]
    gate = journal.gate(records, events) if records else None
    per_horizon = []
    for hours in HOURS:
        mature = [e for e in done if str(hours) in e["outcomes"]]
        funded = [e["outcomes"][str(hours)]["net_with_funding"] for e in mature if e["outcomes"][str(hours)].get("net_with_funding") is not None]
        per_horizon.append({
            "hours": hours, "completed_n": len([e for e in events.values() if e.get("primary_sample") and str(hours) in e["outcomes"]]),
            "common_n": len(done), "mean_net_pct": _mean([e["outcomes"][str(hours)]["net_price_only"] * 100 for e in mature]),
            "excess_pp": _mean([excess(e, hours) * 100 for e in mature if e["null"]["horizons"][str(hours)]["status"] == "ok"]),
            "gain_vs_1h_pp": None, "funded_common_n": len(funded),
            "mean_funded_pct": _mean([v * 100 for v in funded]) if mature and len(funded) == len(mature) else None})
    primary_excess = [excess(e, PRIMARY_HOURS) * 100 for e in done]
    alerts = sorted((e for e in events.values()), key=lambda e: e["published_at_utc"])
    last = alerts[-1] if alerts else None
    status = ("未激活" if not records else "运行异常" if state.get("last_pass_error") else "运行心跳待恢复" if not running
              else "运行中 · 持仓待结算" if any(e["status"] == "observing" for e in events.values()) else "运行中 · 等待新告警")
    activated = records[0]["recorded_at_utc"] if records else None
    verdict = "review_due" if gate and records and not gate.startswith(("await", "not_")) else "collecting"
    return {
        "retirement": {"n": len(done), "min_n": 66, "mean_pct": _mean(primary_excess), "ci90_pct": None, "verdict": verdict,
                       "label": "待协议复核" if verdict == "review_due" else "积累新样本" if records else "未激活",
                       "reason": f"BTC 交易所流入告警（流入做空／流出做多），固定 24h 为主周期，较入场前30天同向精确均值的超额；"
                                 f"仅 24h 内首条告警计入统计。{('当前门槛：' + gate) if gate else ''}", "clusters": None},
        "started_at": activated, "updated_at": stamp or activated, "runner_active": running, "continuous_runner": running,
        "paper_only": True, "tracker_status_label": status, "forward_open": sum(e["status"] == "observing" for e in events.values()),
        "forward_waiting": sum(e["status"] == "planned" for e in events.values()),
        "forward_late": sum(e.get("missed", {}).get("reason") in ("freeze_not_committed_before_entry", "entry_price_unavailable")
                            or "entry_too_late" in e.get("exclusion_reasons", []) for e in events.values()),
        "horizon_comparison": per_horizon, "review_gate": gate, "observation_count": len(events),
        "primary_sample_count": sum(bool(e.get("primary_sample")) for e in events.values()),
        "skipped_count": sum(e["status"] == "skipped" for e in events.values()),
        "unparsed_after_activation": len(state.get("unparsed", {})),
        "last_alert": ({"published_at_utc": last["published_at_utc"], "btc": last["alert"]["btc"], "kind": last["alert"]["kind"],
                        "venue": last["alert"]["venue"], "status": last["status"]} if last else None),
        "collector_health": journal.health() if records else None,
        "not_a_portfolio_or_independent_trades": True, "execution_authorized": False,
        "last_error": (state.get("errors") or [None])[-1]}


def detail() -> Dict[str, Any]:
    journal = Journal()
    _, events = journal._state()
    return {"summary": summary(journal), "events": list(events.values())[-100:]}


async def worker(stop_event: asyncio.Event, heartbeat=None) -> None:
    while True:  # a machine without the pre-registered protocol waits instead of crash-looping
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
                    state.setdefault("errors", []).append({"at": now_utc().isoformat(), "type": type(exc).__name__, "reason": str(exc)[:180]})
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
