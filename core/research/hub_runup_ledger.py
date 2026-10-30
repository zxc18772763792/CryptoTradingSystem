"""Forward paper ledger hub_runup_filter_v1 for CRYPTO SIGNALS HUB. Records only; no orders.

Pre-registered in ``config/research_protocols/hub_runup_filter_v1.json`` after the retrospective study
docs/TELEGRAM_KOL_EXECUTION_BACKTEST_20261028.md. HUB is a private broadcast channel with no public
preview, so first receipts come from the user's existing independent Telethon session: a child-process
``cycle`` takes the shared ``reader.lock`` (skipping the poll when another reader holds it), asks for
messages after the last seen id and disconnects. Each market-entry signal parsed by the frozen hub-v1
rules (``core.research.kol_bracket_engine``) is executed as written on official Binance klines. The
primary book skips signals posted after a >= 1% run in their direction over the previous 30 minutes;
the secondary book keeps them. The control re-runs the same relative bracket on the same coin over the
month after each signal (available 45 days later).

Journal records are hash-chained and carry the SHA-256 of the protocol, this module and the rules
module; any change stops the journal. Message texts stay in the restricted per-user research directory;
the repository-side journal holds parsed fields and text hashes only.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from core.research import kol_bracket_engine as rules

ROOT = Path(__file__).resolve().parents[2]
TRACKER_ID = PROTOCOL_ID = "hub_runup_filter_v1"
CHANNEL_ID = 2095234574
OUTPUT = ROOT / "data/research/hub_runup_filter_v1"
PRIVATE = Path.home() / ".crypto_trading_system/research/hub_runup_filter_v1"
PROTOCOL = ROOT / "config/research_protocols/hub_runup_filter_v1.json"
MODULE = Path(__file__)
RULES = Path(rules.__file__)
CLI = ROOT / "scripts/hub_runup_ledger.py"

ELIGIBLE = ("full_bracket", "targets_without_stop")
MAX_RECEIPT_DELAY = timedelta(seconds=180)
MAX_EDIT_LAG = timedelta(seconds=600)
MAX_FREEZE_DELAY = timedelta(seconds=600)
CLOCK_MAX_AGE = timedelta(minutes=10)
CLOCK_EVERY = timedelta(minutes=5)
CLOCK_MAX_OFFSET, CLOCK_MAX_RTT = 2.0, 10.0
RUNUP = 0.01
HOLD = timedelta(days=14)
FIRST_1M = timedelta(hours=2)
CONTROL_AFTER = timedelta(days=45)
NULL_STARTS, NULL_STEP, MIN_NULL = 58, timedelta(hours=12), 20
UNIVERSE_MAX_AGE = timedelta(hours=6)
SETTLE_EVERY = timedelta(minutes=10)
TARGET_N, FUTILITY_N, MAX_MONTHS = 160, 80, 18
POLL_SECONDS = 30
CYCLE_TIMEOUT = 240
BASE = "https://fapi.binance.com"
ENDPOINTS = {"/fapi/v1/klines", "/fapi/v1/fundingRate", "/fapi/v1/time", "/fapi/v1/exchangeInfo"}
STEP = {"1m": 60_000, "5m": 300_000, "1h": 3_600_000}


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc(value: Any) -> datetime:
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)


def ms(at: datetime) -> int:
    return int(round(utc(at).timestamp() * 1000))


def naive(at: datetime) -> pd.Timestamp:
    return pd.Timestamp(utc(at)).tz_convert(None)


def dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(raw)
    temp.replace(path)


def pins() -> Dict[str, str]:
    return {"protocol": sha(PROTOCOL.read_bytes()), "ledger": sha(MODULE.read_bytes()), "rules": sha(RULES.read_bytes())}


# ------------------------------------------------------------------------------------------------ journal
class Journal:
    """Append-only, hash-chained JSONL. State is always rebuilt from the records."""

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
    st: Dict[str, Any] = {"activation": None, "events": {}, "drift": None, "stopped": None, "last_seq": len(recs) - 1}
    for rec in recs:
        d = rec["data"]
        if rec["kind"] == "activation":
            st["activation"] = {**d, "at": rec["at"], "pins": rec["pins"]}
        elif rec["kind"] in ("receipt", "decision", "outcome", "control"):
            st["events"].setdefault(str(d["message_id"]), {})[rec["kind"]] = d
        elif rec["kind"] == "stop_new_events":
            st["stopped"] = d
    if st["activation"] and st["activation"]["pins"] != pins():
        st["drift"] = {"activated": st["activation"]["pins"], "current": pins()}
    return st


# ------------------------------------------------------------------------------------------- market data
class Market:
    """Public Binance USD-M reads; every response is hashed into the evidence log. No key, no account endpoint."""

    def __init__(self, output: Path = OUTPUT, max_calls: int = 120):
        import requests
        from config.settings import settings
        from core.utils.proxy_env import ensure_proxy_env
        ensure_proxy_env(settings)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "hub-runup-ledger-research/1", "Cache-Control": "no-cache"})
        self.output, self.calls, self.max_calls = Path(output), 0, max_calls

    def get(self, path: str, params: Dict[str, Any]):
        if path not in ENDPOINTS:
            raise ValueError("unsupported public endpoint")
        if self.calls >= self.max_calls:
            raise RuntimeError("request bound reached for this cycle")
        import requests
        self.calls += 1
        for attempt in range(3):  # transport drops (proxy SSL EOF, resets) only; refusals are never retried
            requested = now_utc()
            try:
                response = self.session.get(BASE + path, params=params, timeout=(5, 20))
                break
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt == 2:
                    raise
        received = now_utc()
        if response.status_code in (403, 418, 429):
            raise RuntimeError(f"HTTP {response.status_code}: refused, not retried")
        response.raise_for_status()
        log = {"endpoint": path, "params": params, "requested_at": requested.isoformat(),
               "received_at": received.isoformat(), "status": response.status_code, "sha256": sha(response.content)}
        with (self.output / "evidence.jsonl").open("ab") as f:
            f.write(dumps(log) + b"\n")
        return response.json(), requested, received

    def clock(self, samples: int = 3) -> Dict[str, Any]:
        reads = []
        for _ in range(samples):
            payload, req, rec = self.get("/fapi/v1/time", {})
            server = payload["serverTime"] / 1000
            reads.append({"rtt": (rec - req).total_seconds(),
                          "offset": max(abs(server - rec.timestamp()), abs(server - req.timestamp()))})
        best = min(reads, key=lambda r: r["rtt"])
        return {**best, "checked_at": now_utc().isoformat(),
                "ok": best["offset"] <= CLOCK_MAX_OFFSET and best["rtt"] <= CLOCK_MAX_RTT}

    def universe(self) -> Dict[str, Any]:
        payload, _, rec = self.get("/fapi/v1/exchangeInfo", {})
        symbols = sorted(s["symbol"] for s in payload["symbols"] if s.get("contractType") == "PERPETUAL"
                         and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING")
        if len(symbols) < 100:
            raise ValueError("implausibly small perpetual universe")
        return {"fetched_at": rec.isoformat(), "symbols": symbols}

    def klines(self, symbol: str, interval: str, start: datetime, end: datetime) -> List[list]:
        """Closed official bars with open time in [start, end); paginated, gaps kept as gaps."""
        step, out = STEP[interval], {}
        cursor, stop = ms(start), min(ms(end), ms(now_utc())) - 1
        while cursor <= stop:
            rows, _, _ = self.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "startTime": cursor,
                                                      "endTime": stop, "limit": 1500})
            if not isinstance(rows, list):
                raise ValueError("invalid klines payload")
            for r in rows:
                if int(r[0]) % step or not cursor <= int(r[0]) <= stop:
                    raise ValueError("misaligned or out-of-window kline")
                if int(r[6]) < ms(now_utc()):  # closed bars only
                    out[int(r[0])] = r
            if len(rows) < 1500:
                break
            cursor = int(rows[-1][0]) + step
        return [out[k] for k in sorted(out)]

    def funding(self, symbol: str, since: datetime) -> Dict[str, Any]:
        limit = int(min(1000, (now_utc() - since).total_seconds() // 3600 + 4))
        rows, _, _ = self.get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})
        if not isinstance(rows, list) or any(r.get("symbol") != symbol for r in rows):
            raise ValueError("funding payload invalid")
        covered = len(rows) < limit or (bool(rows) and min(int(r["fundingTime"]) for r in rows) <= ms(since) - 60_000)
        return {"rows": rows, "complete": covered}


def frame(rows: List[list]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close"])
    f = pd.DataFrame([[float(x) for x in r[1:5]] for r in rows], columns=["open", "high", "low", "close"],
                     index=pd.to_datetime([int(r[0]) for r in rows], unit="ms"))
    bad = (f.high < f[["open", "close"]].max(axis=1)) | (f.low > f[["open", "close"]].min(axis=1)) | (f <= 0).any(axis=1)
    if bad.any():
        raise ValueError("invalid OHLC geometry")
    return f


class BarCache:
    """Per-event bar store so open positions only fetch new bars on each settlement pass."""

    def __init__(self, market: Market, output: Path = OUTPUT):
        self.market, self.folder = market, Path(output) / "bars"

    def get(self, key: str, symbol: str, interval: str, start: datetime, end: datetime) -> pd.DataFrame:
        path = self.folder / f"{key}_{interval}.json"
        rows = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
        have_until = utc(datetime.fromtimestamp(int(rows[-1][0]) / 1000, timezone.utc)) + timedelta(milliseconds=STEP[interval]) if rows else utc(start)
        end = min(utc(end), now_utc())
        if have_until < end:
            fresh = self.market.klines(symbol, interval, have_until, end)
            if fresh:
                rows = [r for r in rows if int(r[0]) < int(fresh[0][0])] + fresh
                atomic_write(path, json.dumps(rows).encode("utf-8"))
        return frame([r for r in rows if ms(start) <= int(r[0]) < ms(end)])


# ------------------------------------------------------------------------------------------- signal rules
def parse(message_id: int, published: datetime, edited: Optional[datetime], text: str) -> Dict[str, Any]:
    sig = rules.parse_hub({"message_id": message_id, "published_at": published.isoformat(),
                           "edited_at": edited.isoformat() if edited else None, "text": text or ""})
    if sig is None:
        return {"signal": False}
    out = {k: getattr(sig, k) for k in ("symbol", "side", "entry_type", "legs", "targets", "stop", "stop_stated_none",
                                         "leverage", "open_ended", "be_after_tp1")}
    out.update(signal=True, completeness=rules.classify(sig))
    out["eligible"] = out["entry_type"] == "market" and out["completeness"] in ELIGIBLE
    return out


def resolve(symbol: str, universe: List[str]) -> tuple:
    for name, mult in ((symbol, 1.0), ("1000" + symbol, 1000.0), ("1000000" + symbol, 1e6)):
        if name + "USDT" in universe:
            return name + "USDT", mult
    return None, 1.0


def pre_move(market: Market, native: str, side: int, published: datetime) -> Optional[float]:
    pub = naive(published).floor("min")
    f = frame(market.klines(native, "1m", utc(pub - pd.Timedelta(minutes=30)), utc(pub + pd.Timedelta(minutes=1))))
    first, last = pub - pd.Timedelta(minutes=30), pub
    if first not in f.index or last not in f.index:
        return None
    return side * (float(f.loc[last, "open"]) / float(f.loc[first, "open"]) - 1)


def label_sig(dec: Dict[str, Any]) -> Dict[str, Any]:
    p = dec["parsed"]
    return {"channel": "hub", **{k: p[k] for k in ("symbol", "side", "entry_type", "legs", "targets", "stop",
                                                   "leverage", "open_ended", "be_after_tp1")},
            "target_weights": None, "target_pct_margin": None, "note": ""}


def simulate(dec: Dict[str, Any], cache: BarCache, data_end: datetime, funding: Optional[pd.Series] = None,
             leverage: Optional[float] = None) -> Dict[str, Any]:
    """The study's execution on official bars up to data_end: 1m for two hours, then 5m with 1m replay."""
    entry = utc(dec["entry_at"])
    key, native = str(dec["message_id"]), dec["native"]
    m1 = cache.get(key, native, "1m", entry, min(entry + FIRST_1M, data_end))
    if m1.empty or naive(entry) not in m1.index:
        return {"status": "awaiting_entry_bar"}
    ref = float(m1.loc[naive(entry), "open"])
    plan, why = rules.build_plan(label_sig(dec), ref, dec["mult"], leverage)
    if plan is None:
        return {"status": "not_traded", "reason": why, "ref": ref}
    m5 = cache.get(key, native, "5m", entry + FIRST_1M, min(entry + HOLD + timedelta(minutes=5), data_end)) \
        if data_end > entry + FIRST_1M else m1.iloc[:0]

    def finer(t, bar_len):
        if bar_len == pd.Timedelta(minutes=5):
            f = frame(cache.market.klines(native, "1m", utc(t), utc(t + bar_len)))
            return f if len(f) > 1 else None
        return None

    act = naive(entry)
    eng = rules.Engine(plan, act, act + pd.Timedelta(HOLD), act + pd.Timedelta(hours=72), finer=finer,
                       funding_series=funding if funding is not None else pd.Series(dtype=float))
    for bars, blen in ((m1, pd.Timedelta(minutes=1)), (m5, pd.Timedelta(minutes=5))):
        eng.run(bars, blen)
        if eng.status in ("cancelled", "stale", "closed", "expired_unfilled"):
            break
    final = eng.status in ("cancelled", "stale", "closed", "expired_unfilled")
    if not final and data_end >= entry + HOLD and eng.qty > 0:
        last = m5 if len(m5) else m1
        expiry = act + pd.Timedelta(HOLD)
        if expiry in last.index:
            eng._exit(expiry, float(last.loc[expiry, "open"]), eng.qty, "expiry")
        else:
            return {"status": "path_gap_at_expiry", "ref": ref}
        final = True
    out = eng.result()
    out.update(final=final, ref=ref, plan_check=why, plan={"targets": plan.targets, "stop": plan.stop,
                                                           "weights": plan.weights})
    return out


def run_1h(side: int, relp: Dict[str, Any], start: pd.Timestamp, h: pd.DataFrame) -> Optional[float]:
    bars = h[(h.index >= start) & (h.index < start + pd.Timedelta(HOLD) + pd.Timedelta(hours=1))]
    if len(bars) < HOLD / timedelta(hours=1):
        return None
    ref = float(bars.iloc[0]["open"])
    plan = rules.Plan(side, True, [], [x * ref for x in relp["targets"]], relp["weights"],
                      relp["stop"] * ref if relp["stop"] else None, False)
    eng = rules.Engine(plan, start, start + pd.Timedelta(HOLD), start + pd.Timedelta(hours=72))
    eng.run(bars, pd.Timedelta(hours=1))
    if eng.qty > 0:
        eng._exit(bars.index[-1], float(bars.iloc[-1]["close"]), eng.qty, "expiry")
    r = eng.result()
    return r["ret_on_filled"] if r["ret_on_filled"] is not None else 0.0


def control(dec: Dict[str, Any], out: Dict[str, Any], market: Market) -> Dict[str, Any]:
    """Same relative bracket on the same coin (price only, 20 bps): signal on 1h vs 58 post / 58 pre starts."""
    entry = naive(dec["entry_at"])
    h = frame(market.klines(dec["native"], "1h", utc(entry - pd.Timedelta(days=31)), utc(entry + pd.Timedelta(days=45))))
    relp = {"targets": [x / out["ref"] for x in out["plan"]["targets"]], "weights": out["plan"]["weights"],
            "stop": out["plan"]["stop"] / out["ref"] if out["plan"]["stop"] else None}
    side = dec["parsed"]["side"]
    s = run_1h(side, relp, entry.ceil("h"), h)
    post = [v for k in range(NULL_STARTS)
            if (v := run_1h(side, relp, (entry + pd.Timedelta(days=1) + k * pd.Timedelta(NULL_STEP)).ceil("h"), h)) is not None]
    pre = [v for k in range(NULL_STARTS)
           if (v := run_1h(side, relp, (entry - pd.Timedelta(days=30) + k * pd.Timedelta(NULL_STEP)).ceil("h"), h)) is not None]
    rec = {"message_id": dec["message_id"], "signal_1h": s, "n_post": len(post), "n_pre": len(pre)}
    if s is not None and len(post) >= MIN_NULL:
        rec.update(null_post_mean=float(np.mean(post)), excess_post=s - float(np.mean(post)))
    if s is not None and len(pre) >= MIN_NULL:
        rec.update(null_pre_mean=float(np.mean(pre)), excess_pre=s - float(np.mean(pre)))
    return rec


# ------------------------------------------------------------------------------------------------ telegram
def private_dir() -> Path:
    PRIVATE.mkdir(parents=True, exist_ok=True)
    return PRIVATE


async def telegram_fetch(after_id: Optional[int], limit: int = 100) -> Dict[str, Any]:
    """One read under the shared reader.lock; returns new messages oldest first. Raises PermissionError if busy."""
    from telethon import types
    from telethon.sessions import StringSession
    from scripts import telegram_desktop_bridge as bridge

    try:
        lock = bridge.session_lock()
        lock.__enter__()
    except OSError as exc:
        raise PermissionError("reader.lock busy") from exc
    try:
        payload = json.loads(bridge.dpapi(bridge.store_path().read_bytes(), decrypt=True))
        client = bridge.make_client(StringSession(payload["session"]), payload["api_id"], payload["api_hash"])
        try:
            await bridge.connect(client)
            me = await client.get_me()
            if me is None or me.id != payload["account_id"]:
                raise RuntimeError("saved session identity verification failed")
            peer_path = private_dir() / "peer.json"
            if peer_path.exists():
                peer = json.loads(peer_path.read_text(encoding="utf-8"))
            else:
                peer = None
                async for dialog in client.iter_dialogs(limit=400):
                    if getattr(dialog.entity, "id", None) == CHANNEL_ID:
                        peer = {"channel_id": CHANNEL_ID, "access_hash": dialog.entity.access_hash}
                        break
                if peer is None:
                    raise RuntimeError("HUB channel not among the account's dialogs")
                atomic_write(peer_path, json.dumps(peer).encode("utf-8"))
            entity = types.InputPeerChannel(peer["channel_id"], peer["access_hash"])
            msgs = await client.get_messages(entity, limit=limit, min_id=after_id or 0) if after_id is not None \
                else await client.get_messages(entity, limit=1)
            received = now_utc()
            return {"received_at": received, "messages": sorted(
                [{"id": m.id, "date": utc(m.date), "edit_date": utc(m.edit_date) if m.edit_date else None,
                  "text": m.message or ""} for m in msgs], key=lambda m: m["id"]), "truncated": len(msgs) >= limit}
        finally:
            await client.disconnect()
    finally:
        lock.__exit__(None, None, None)


# ---------------------------------------------------------------------------------------------- the ledger
class Ledger:
    def __init__(self, output: Path = OUTPUT):
        self.output = Path(output)
        self.journal = Journal(self.output)
        self.runtime_path = self.output / "runtime.json"

    def runtime(self) -> Dict[str, Any]:
        return json.loads(self.runtime_path.read_text(encoding="utf-8")) if self.runtime_path.exists() else {}

    def save_runtime(self, rt: Dict[str, Any]) -> None:
        atomic_write(self.runtime_path, json.dumps(rt, indent=1, default=str).encode("utf-8"))

    # -- activation
    async def activate(self) -> Dict[str, Any]:
        if self.journal.records():
            raise RuntimeError("journal already exists; a new protocol needs a new directory")
        self.output.mkdir(parents=True, exist_ok=True)
        market = Market(self.output)
        clock = market.clock(5)
        if not clock["ok"]:
            raise RuntimeError(f"clock check failed: {clock}")
        universe = market.universe()
        latest = await telegram_fetch(None)
        if not latest["messages"]:
            raise RuntimeError("HUB returned no messages")
        baseline = latest["messages"][-1]["id"]
        rec = self.journal.append("activation", {"baseline_message_id": baseline, "clock": clock,
                                                 "universe_size": len(universe["symbols"]),
                                                 "protocol": json.loads(PROTOCOL.read_text(encoding="utf-8"))["protocol_id"]})
        self.save_runtime({"last_seen_id": baseline, "clock": clock, "universe": universe,
                           "activated_at": rec["at"]})
        return rec

    # -- one cycle
    async def cycle(self) -> Dict[str, Any]:
        st = state(self.journal)
        if not st["activation"]:
            return {"outcome": "not_activated"}
        if st["drift"]:
            return {"outcome": "drift_stopped", "drift": st["drift"]}
        rt = self.runtime()
        report: Dict[str, Any] = {"outcome": "ok", "new": 0}
        market = Market(self.output)
        now = now_utc()
        if not rt.get("clock") or now - utc(rt["clock"]["checked_at"]) >= CLOCK_EVERY:
            try:
                rt["clock"] = market.clock(3)
            except Exception as exc:
                # an unreachable time endpoint is not a bad clock: the last good reading stays valid for
                # CLOCK_MAX_AGE, after which receipts are marked clock_unverified
                rt["clock_error"] = {"at": now.isoformat(), "type": type(exc).__name__}
                rt.setdefault("clock", {"ok": False, "checked_at": now.isoformat()})
        if not rt.get("universe") or now - utc(rt["universe"]["fetched_at"]) >= UNIVERSE_MAX_AGE:
            try:
                rt["universe"] = market.universe()
            except Exception as exc:
                report["universe_error"] = type(exc).__name__
        if utc(rt.get("cooldown_until") or "2000-01-01") > now:
            report["telegram"] = "cooldown"
        else:
            try:
                got = await telegram_fetch(int(rt["last_seen_id"]))
                report["telegram"] = "ok"
                self.receive(got, rt)
                report["new"] = len(got["messages"])
                rt["last_poll_ok_at"] = got["received_at"].isoformat()
            except PermissionError:
                report["telegram"] = "lock_busy"
            except Exception as exc:
                seconds = getattr(exc, "seconds", None)
                if seconds:
                    rt["cooldown_until"] = (now + timedelta(seconds=int(seconds) + 5)).isoformat()
                report["telegram"] = f"error:{type(exc).__name__}"
        with (self.output / "polls.jsonl").open("ab") as f:
            f.write(dumps({"at": now.isoformat(), "telegram": report["telegram"], "new": report["new"],
                           "clock_ok": rt["clock"].get("ok")}) + b"\n")
        self.decide(rt, market)
        if not rt.get("settled_at") or now - utc(rt["settled_at"]) >= SETTLE_EVERY:
            report["settled"] = self.settle(market)
            rt["settled_at"] = now.isoformat()
        self.save_runtime(rt)
        return report

    def receive(self, got: Dict[str, Any], rt: Dict[str, Any]) -> None:
        received = got["received_at"]
        with (private_dir() / "receipts.jsonl").open("ab") as f:
            for m in got["messages"]:
                f.write(dumps({"message_id": m["id"], "first_received_at": received.isoformat(), "text": m["text"]}) + b"\n")
        for m in got["messages"]:
            self.journal.append("receipt", {
                "message_id": m["id"], "published_at": m["date"].isoformat(),
                "edited_at": m["edit_date"].isoformat() if m["edit_date"] else None,
                "first_received_at": received.isoformat(), "delay_s": (received - m["date"]).total_seconds(),
                "text_sha256": sha((m["text"] or "").encode("utf-8")), "truncated_batch": got["truncated"],
                "clock_ok": bool(rt["clock"].get("ok")) and received - utc(rt["clock"]["checked_at"]) <= CLOCK_MAX_AGE,
                "parsed": parse(m["id"], m["date"], m["edit_date"], m["text"])})
            rt["last_seen_id"] = max(int(rt["last_seen_id"]), m["id"])

    def open_in_book(self, st: Dict[str, Any], book: str, native: str, entry: datetime, cache: BarCache) -> bool:
        for ev in st["events"].values():
            d = ev.get("decision")
            if not d or d.get("native") != native or d["books"].get(book) != "entered" or utc(d["entry_at"]) >= entry:
                continue
            out = ev.get("outcome")
            if out:
                if out.get("exit_time") is None or naive(entry) < pd.Timestamp(out["exit_time"]):
                    if out.get("first_fill"):
                        return True
                continue
            r = simulate(d, cache, now_utc())
            if r.get("first_fill") and (r.get("exit_time") is None or naive(entry) < pd.Timestamp(r["exit_time"])):
                return True
        return False

    def decide(self, rt: Dict[str, Any], market: Market) -> None:
        st = state(self.journal)
        cache = BarCache(market, self.output)
        for mid, ev in sorted(st["events"].items(), key=lambda kv: int(kv[0])):
            rec = ev.get("receipt")
            if not rec or "decision" in ev:
                continue
            p, now = rec["parsed"], now_utc()
            base = {"message_id": rec["message_id"], "freeze_at": now.isoformat(), "parsed": p,
                    "books": {"filtered": "not_applicable", "all": "not_applicable"}}
            published, received = utc(rec["published_at"]), utc(rec["first_received_at"])
            reason = None
            if not p.get("signal"):
                reason = "not_a_signal"
            elif not p.get("eligible"):
                reason = f"ineligible_{p.get('entry_type')}_{p.get('completeness')}"
            elif st["stopped"]:
                reason = "ledger_stopped"
            elif received - published > MAX_RECEIPT_DELAY:
                reason = "late_receipt"
            elif rec["edited_at"] and utc(rec["edited_at"]) - published > MAX_EDIT_LAG:
                reason = "edited_after_10min"
            elif not rec["clock_ok"]:
                reason = "clock_unverified"
            elif rec["truncated_batch"]:
                reason = "truncated_batch"
            elif now - received > MAX_FREEZE_DELAY:
                reason = "freeze_too_late"
            if reason:
                self.journal.append("decision", {**base, "eligible": False, "reason": reason})
                continue
            native, mult = resolve(p["symbol"], rt.get("universe", {}).get("symbols", []))
            if native is None:
                self.journal.append("decision", {**base, "eligible": False, "reason": "no_trading_perpetual"})
                continue
            try:
                pm = pre_move(market, native, p["side"], published)
            except Exception:
                continue  # retried next cycle until the freeze deadline
            entry = datetime.fromtimestamp(math.ceil(now.timestamp() / 60) * 60, timezone.utc)
            dec = {**base, "eligible": True, "native": native, "mult": mult, "pre_move_30m": pm,
                   "filter_pass": pm is not None and pm < RUNUP, "entry_at": entry.isoformat()}
            for book in ("filtered", "all"):
                if book == "filtered" and not dec["filter_pass"]:
                    dec["books"][book] = "filtered_out" if pm is not None else "pre_move_unavailable"
                elif self.open_in_book(st, book, native, entry, cache):
                    dec["books"][book] = "skipped_overlap"
                else:
                    dec["books"][book] = "entered"
            self.journal.append("decision", dec)
            st = state(self.journal)

    def settle(self, market: Market) -> Dict[str, int]:
        st = state(self.journal)
        cache = BarCache(market, self.output)
        counts = {"outcomes": 0, "controls": 0, "errors": 0}
        now = now_utc()
        for mid, ev in st["events"].items():
            d = ev.get("decision")
            if not d or not d.get("eligible") or "entered" not in d["books"].values():
                continue
            try:
                if "outcome" not in ev:
                    r = simulate(d, cache, now - timedelta(minutes=2))
                    if r.get("status") in ("not_traded",) or r.get("final"):
                        if r.get("first_fill"):
                            fund = market.funding(d["native"], utc(d["entry_at"]))
                            series = pd.Series([float(x["fundingRate"]) for x in fund["rows"]],
                                               index=pd.to_datetime([int(x["fundingTime"]) for x in fund["rows"]], unit="ms").floor("min"))
                            withf = simulate(d, cache, now - timedelta(minutes=2), funding=series) if fund["complete"] else None
                            lev = simulate(d, cache, now - timedelta(minutes=2), leverage=d["parsed"]["leverage"]) \
                                if d["parsed"].get("leverage") else None
                            r.update(ret_with_funding=withf["ret_on_filled"] if withf else None,
                                     funding_complete=fund["complete"],
                                     lev={"exit_reason": lev.get("exit_reason"),
                                          "ret_on_margin": max(-1.0, lev["ret_on_filled"] * d["parsed"]["leverage"])} if lev and lev.get("ret_on_filled") is not None else None)
                        self.journal.append("outcome", {"message_id": d["message_id"],
                                                        **{k: v for k, v in r.items() if k != "events"},
                                                        "events": r.get("events")})
                        counts["outcomes"] += 1
                        ev["outcome"] = r
                out = ev.get("outcome")
                if out and out.get("plan") and "control" not in ev and now >= utc(d["entry_at"]) + CONTROL_AFTER:
                    self.journal.append("control", control(d, out, market))
                    counts["controls"] += 1
            except Exception:
                counts["errors"] += 1
        self.check_futility()
        return counts

    def check_futility(self) -> None:
        st = state(self.journal)
        if st["stopped"]:
            return
        done = primary_events(st)
        ex = [e["control"]["excess_post"] for e in done if e.get("control", {}).get("excess_post") is not None]
        if len(ex) >= FUTILITY_N and float(np.mean(ex[:FUTILITY_N])) <= 0:
            self.journal.append("stop_new_events", {"reason": "futility", "n": FUTILITY_N, "mean_excess": float(np.mean(ex[:FUTILITY_N]))})
        activated = utc(st["activation"]["at"])
        if now_utc() > activated + timedelta(days=30.44 * MAX_MONTHS):
            self.journal.append("stop_new_events", {"reason": "maximum_duration"})


def primary_events(st: Dict[str, Any], book: str = "filtered") -> List[Dict[str, Any]]:
    evs = [e for e in st["events"].values() if e.get("decision", {}).get("books", {}).get(book) == "entered"]
    return sorted(evs, key=lambda e: e["decision"]["entry_at"])


def _mean(xs: List[float]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return float(np.mean(xs)) if xs else None


def summary(output: Path = OUTPUT) -> Dict[str, Any]:
    journal = Journal(output)
    if not PROTOCOL.exists():
        return {"status": "not_configured"}
    try:
        st = state(journal)
    except RuntimeError as exc:
        return {"status": "journal_broken", "error": str(exc)}
    if not st["activation"]:
        return {"status": "not_activated", "target_n": TARGET_N}
    books = {}
    for book in ("filtered", "all"):
        evs = primary_events(st, book)
        outs = [e["outcome"] for e in evs if e.get("outcome")]
        traded = [o for o in outs if o.get("ret_on_filled") is not None]
        ctrl = [e["control"]["excess_post"] for e in evs if e.get("control", {}).get("excess_post") is not None]
        books[book] = {"entered": len(evs), "settled": len(outs), "traded": len(traded),
                       "open": len(evs) - len(outs), "not_traded": len(outs) - len(traded),
                       "mean_net": _mean([o["ret_on_filled"] for o in traded]),
                       "mean_net_with_funding": _mean([o.get("ret_with_funding") for o in traded]),
                       "controlled": len(ctrl), "mean_excess_post": _mean(ctrl)}
    decisions = [e["decision"] for e in st["events"].values() if e.get("decision")]
    reasons: Dict[str, int] = {}
    for d in decisions:
        reasons[d.get("reason", "eligible")] = reasons.get(d.get("reason", "eligible"), 0) + 1
    rt = Ledger(output).runtime()
    return {"status": "drift_stopped" if st["drift"] else ("stopped" if st["stopped"] else "running"),
            "activated_at": st["activation"]["at"], "receipts": sum(1 for e in st["events"].values() if e.get("receipt")),
            "decisions": reasons, "books": books, "target_n": TARGET_N, "futility_n": FUTILITY_N,
            "last_poll_ok_at": rt.get("last_poll_ok_at"), "clock": rt.get("clock"), "journal_records": st["last_seq"] + 1,
            "note": "descriptive only until the protocol sample is reached; no rule is tuned on these numbers"}


def registry_summary() -> Dict[str, Any]:
    """Shape expected by core.research.tracker_registry (same fields as the other Telegram paper ledgers)."""
    s = summary()
    runner = json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    fresh = bool(runner.get("at")) and 0 <= (now_utc() - utc(runner["at"])).total_seconds() <= 120
    activated = s.get("activated_at")
    books = s.get("books", {})
    primary = books.get("filtered", {})
    n, mean = primary.get("controlled", 0), primary.get("mean_excess_post")
    label = {"not_activated": "未激活", "drift_stopped": "规则文件已变更 · 停账",
             "stopped": "已停止新开仓", "journal_broken": "日志校验失败"}.get(s["status"], "积累新样本")
    status = label if s["status"] != "running" else (
        "运行异常" if runner.get("outcome") not in (None, "ok") else "运行心跳待恢复" if not fresh
        else "运行中 · 持仓待结算" if primary.get("open") else "运行中 · 等待新信号")
    return {
        "retirement": {"n": n, "min_n": TARGET_N, "mean_pct": mean * 100 if mean is not None else None, "ci90_pct": None,
                       "verdict": "review_due" if n >= TARGET_N else "collecting", "label": label,
                       "reason": "HUB 私密频道首次收件的市价信号，按原文多档止盈/止损/14天上限在币安永续执行；"
                                 "跳过发帖前30分钟已顺向走过≥1%的信号（主账本），另记不过滤账本。主统计为较入场后30天同币同几何"
                                 "1h起点均值的超额（价格口径、20bps），入场45天后才有；80笔无效性检查，160笔复核。",
                       "clusters": None},
        "started_at": activated, "updated_at": runner.get("at") or activated, "runner_active": fresh,
        "continuous_runner": fresh, "paper_only": True, "tracker_status_label": status,
        "forward_open": primary.get("open", 0), "forward_waiting": 0, "forward_late": s.get("decisions", {}).get("late_receipt", 0),
        "review_gate": s["status"], "observation_count": s.get("receipts", 0),
        "primary_sample_count": primary.get("entered", 0), "books": books,
        "execution_authorized": False, "last_error": runner.get("error_type") or runner.get("telegram")
        if runner.get("outcome") not in (None, "ok") else None}


# ------------------------------------------------------------------------------------------- web worker
STATE_PATH = ROOT / "runtime/hub_runup_ledger.json"


async def _run_cycle() -> Dict[str, Any]:
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    proc = await asyncio.create_subprocess_exec(sys.executable, str(CLI), "cycle", cwd=str(ROOT),
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                                                creationflags=flags)
    try:
        raw, _ = await asyncio.wait_for(proc.communicate(), timeout=CYCLE_TIMEOUT)
    except (asyncio.TimeoutError, asyncio.CancelledError) as exc:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await proc.communicate()
        if isinstance(exc, asyncio.CancelledError):
            raise
        return {"outcome": "timeout"}
    for line in reversed((raw or b"").decode("utf-8", "replace").splitlines()):
        try:
            value = json.loads(line)
            if isinstance(value, dict) and "outcome" in value:
                return {**value, "returncode": proc.returncode}
        except ValueError:
            continue
    return {"outcome": "failed", "returncode": proc.returncode}


async def worker(stop_event: asyncio.Event, heartbeat: Optional[Callable[[bool], None]] = None) -> None:
    """Idles until activation; then one child-process cycle every 30 s. Paper ledger only."""
    while not stop_event.is_set():
        if (OUTPUT / "journal.jsonl").exists():
            result = await _run_cycle()
            ok = result.get("outcome") == "ok" and str(result.get("telegram", "")).split(":")[0] in ("ok", "lock_busy", "cooldown")
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
