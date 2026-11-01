"""Forward paper overlay korea_frozen_skip_v1 on the pooled Korean caution short. Records only; no orders.

Pre-registered in ``config/research_protocols/korea_frozen_skip_v1.json`` after
docs/INIT_SQUEEZE_ACTORS_2026-10-31.md: shorting a Korean caution designation lost when the coin's Korean
wallets were already suspended in both directions (11 of 38 Upbit events, mean -7.2% vs +7.5%, post hoc,
mostly two tail losses). Every forward trade of the ``korea_caution_hedged`` tracker discovered after
activation is observed until its entry close: Upbit caution notices for the coin (body text) and Bithumb's
public wallet status. At the entry close the trade is skipped when either source shows deposits and
withdrawals both suspended; a skip counts 0% in the filtered book.

The baseline tracker file is read, never written; returns are the baseline's own settled figures, so the
two books differ only by the skipped trades. Journal records are hash-chained and carry the SHA-256 of the
protocol and this module; any change stops the journal.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
TRACKER_ID = PROTOCOL_ID = "korea_frozen_skip_v1"
OUTPUT = ROOT / "data/research/korea_frozen_skip_v1"
PROTOCOL = ROOT / "config/research_protocols/korea_frozen_skip_v1.json"
MODULE = Path(__file__)
SOURCE = ROOT / "data/research/korea_caution_hedged/tracker.json"
STATE_PATH = ROOT / "runtime/korea_frozen_skip_ledger.json"

UPBIT = "https://api-manager.upbit.com/api/v1/announcements"
BITHUMB_WALLET = "https://api.bithumb.com/public/assetsstatus/multichain/ALL"
HEADERS = {"User-Agent": "Mozilla/5.0"}
DAY = timedelta(days=1)
NOTICE_LOOKBACK = timedelta(days=7)
UPBIT_PAGES = 2
BITHUMB_RUN = timedelta(minutes=60)
BITHUMB_MAX_CLOSED_SHARE = 0.02
POLL_SECONDS = 900
CYCLE_TIMEOUT = 300
LOCK_STALE_SECONDS = 600
REVIEW_SKIPPED, MIN_SKIPPED, REVIEW_AFTER = 10, 5, timedelta(days=365)
PERMUTATIONS, SEED, ALPHA = 10_000, 20261101, 0.10
SETTLED = ("closed", "stopped")
VOID = ("no_perp", "delisted")

# Copied from the baseline's caution_tickers (core/research/upbit_caution_tracker.py) so this pinned
# module never changes behaviour when the baseline file is edited.
CAUTION = re.compile(r"유의\s*종목\s*지정")
RELEASED = re.compile(r"지정\s*해제")
NOT_TICKER = {"KRW", "BTC", "USDT", "ETH"}
# wallet_rule_v2: "입출금 서비스는 이미 중단", "입출금은 중단", "출금 중단/차단 시점". The bare heading
# "입출금 일시 중단 안내" also introduces deposit-only stops, so it is deliberately not a pattern.
BOTH_SUSPENDED = (
    re.compile(r"입출금\s*서비스(?:는|가|이)?\s*(?:이미\s*)?(?:일시\s*)?(?:중단|정지)"),
    re.compile(r"입출금(?:은|이)\s*(?:모두\s*)?(?:일시\s*)?(?:중단|정지)"),
    re.compile(r"(?<!입)출금\s*(?:지원\s*|서비스\s*)?(?:중단|차단|정지)\s*시점"),
)


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


# ------------------------------------------------------------------------------------------- evidence
def caution_tickers(title: str) -> List[str]:
    if not CAUTION.search(title) or RELEASED.search(title):
        return []
    found = {m for grp in re.findall(r"\(([^)]*)\)", title) for m in re.findall(r"[A-Z0-9]{2,10}", grp)}
    return sorted(found - NOT_TICKER)


def notice_text(body: str) -> str:
    text = re.sub(r"<[^>]+>", " ", body)
    return re.sub(r"\s+", " ", text.replace("&nbsp;", " "))


def wallet_phrase(body: str) -> Optional[str]:
    """The first both-directions suspension phrase in a notice body, or None."""
    text = notice_text(body)
    for pattern in BOTH_SUSPENDED:
        found = pattern.search(text)
        if found:
            return found.group(0)
    return None


def bithumb_view(payload: Any, ticker: str) -> Dict[str, Any]:
    """One coin's row from Bithumb's multichain wallet list, plus the market-wide closure share."""
    rows = payload.get("data") if isinstance(payload, dict) and str(payload.get("status")) == "0000" else None
    if not isinstance(rows, list) or not rows:
        raise ValueError("invalid Bithumb wallet list")
    nets: Dict[str, List[tuple]] = {}
    for row in rows:
        nets.setdefault(str(row["currency"]), []).append((int(str(row["deposit_status"])),
                                                          int(str(row["withdrawal_status"]))))
    closed = {c for c, n in nets.items() if all(d == 0 and w == 0 for d, w in n)}
    share = len(closed) / len(nets)
    mine = nets.get(ticker)
    return {"valid": share <= BITHUMB_MAX_CLOSED_SHARE, "both_closed_share": round(share, 4),
            "currencies": len(nets),
            "coin": None if mine is None else {"networks": len(mine), "both_closed": ticker in closed,
                                               "deposit_open": any(d == 1 for d, _ in mine),
                                               "withdrawal_open": any(w == 1 for _, w in mine)}}


def decide(event: Dict[str, Any], observations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Skip, keep or late, from observations received before the entry close only."""
    entry = utc(event["entry_at"])
    obs = sorted((o for o in observations if utc(o["observed_at"]) < entry), key=lambda o: utc(o["observed_at"]))
    if not obs:
        return {"action": "late", "reason": "no_observation_before_entry"}
    lower = utc(event["day0"]) - NOTICE_LOOKBACK
    notices: Dict[str, Dict[str, Any]] = {}
    listed, unread = set(), set()
    lists_read = 0
    for o in obs:
        up = o.get("upbit") or {}
        if "error" not in up:
            lists_read += 1
        listed.update(str(x) for x in up.get("listed", []))
        unread.update(str(x) for x in up.get("unread", []))
        for n in up.get("notices", []):
            if lower <= utc(n["first_listed_at"]) < entry:
                notices.setdefault(str(n["id"]), n)  # classified once, from the first body received
    upbit_hit = next((n for _, n in sorted(notices.items()) if n.get("phrase")), None)
    run_start, bithumb_hit, valid = None, None, 0
    for o in obs:
        bt = o.get("bithumb") or {}
        if "error" in bt or not bt.get("valid"):
            continue  # failed or maintenance-wide snapshot: neither extends nor ends a run
        valid += 1
        at = utc(o["observed_at"])
        if (bt.get("coin") or {}).get("both_closed"):
            run_start = run_start or at
            if bithumb_hit is None and at - run_start >= BITHUMB_RUN:
                bithumb_hit = {"from": run_start.isoformat(), "to": at.isoformat()}
        else:
            run_start = None
    arms = [name for name, hit in (("upbit_notice", upbit_hit), ("bithumb_wallet", bithumb_hit)) if hit]
    return {"action": "skip" if arms else "keep", "reason": "+".join(arms) or "no_both_suspension_evidence",
            "upbit_hit": upbit_hit and {k: upbit_hit.get(k) for k in ("id", "title", "first_listed_at", "phrase")},
            "bithumb_run": bithumb_hit, "observations_before_entry": len(obs), "upbit_lists_read": lists_read,
            "upbit_caution_notices": sorted(notices), "upbit_unread": sorted(unread - set(notices)),
            "bithumb_valid_snapshots": valid}


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
        # (the dashboard reads while the worker writes); it is not part of the chain yet
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
    st: Dict[str, Any] = {"activation": None, "events": {}, "drift": None, "last_seq": len(recs) - 1}
    for rec in recs:
        d = rec["data"]
        if rec["kind"] == "activation":
            st["activation"] = {**d, "at": rec["at"], "pins": rec["pins"]}
        elif rec["kind"] == "event":
            st["events"][d["source_id"]] = {"event": d, "observations": []}
        elif rec["kind"] == "observation":
            st["events"][d["source_id"]]["observations"].append(d)
        elif rec["kind"] in ("decision", "outcome"):
            st["events"][d["source_id"]][rec["kind"]] = d
    if st["activation"] and st["activation"]["pins"] != pins():
        st["drift"] = {"activated": st["activation"]["pins"], "current": pins()}
    return st


@contextmanager
def exclusive(output: Path) -> Iterator[bool]:
    """One writer at a time (web task or CLI); a lock older than LOCK_STALE_SECONDS is taken over."""
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


# --------------------------------------------------------------------------------------------- sources
def load_source(path: Path = SOURCE) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def eligible(trade: Dict[str, Any], activated: datetime) -> bool:
    try:
        discovered = utc(trade["discovered_at"])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(trade.get("symbol")) and not trade.get("backfilled") and not trade.get("late") and discovered >= activated


async def _json(client, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    response = await client.get(url, params=params or {}, headers=HEADERS)
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}")
    return response.json()


async def upbit_notices(client) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in range(1, UPBIT_PAGES + 1):
        data = await _json(client, UPBIT, {"os": "web", "page": page, "per_page": 20, "category": "trade"})
        rows = (data.get("data") or {}).get("notices")
        if not isinstance(rows, list):
            raise ValueError("invalid Upbit notice list")
        out += rows
    return out


async def upbit_body(client, notice_id: Any) -> str:
    data = await _json(client, f"{UPBIT}/{notice_id}")
    return str((data.get("data") or {}).get("body") or "")


# ---------------------------------------------------------------------------------------------- ledger
class Ledger:
    def __init__(self, output: Path = OUTPUT, source: Path = SOURCE):
        self.output = Path(output)
        self.source = Path(source)
        self.journal = Journal(self.output)

    async def activate(self, client) -> Dict[str, Any]:
        with exclusive(self.output) as mine:
            if not mine:
                raise RuntimeError("another writer holds the journal lock")
            if self.journal.records():
                raise RuntimeError("journal already exists; a new protocol needs a new directory")
            src = load_source(self.source)
            health = (src.get("collection_health") or {}).get("status")
            if health != "healthy":
                raise RuntimeError(f"source tracker not healthy: {health}")
            listed = await upbit_notices(client)
            wallet = bithumb_view(await _json(client, BITHUMB_WALLET), "BTC")
            if not wallet["valid"]:
                raise RuntimeError(f"Bithumb wallet list looks like maintenance: {wallet['both_closed_share']}")
            return self.journal.append("activation", {
                "protocol": json.loads(PROTOCOL.read_text(encoding="utf-8"))["protocol_id"],
                "source": self.source.relative_to(ROOT).as_posix() if self.source.is_relative_to(ROOT) else str(self.source),
                "source_started_at": src.get("started_at"), "source_trades_at_activation": sorted(src.get("trades", {})),
                "upbit_notices_listed": len(listed),
                "bithumb": {k: wallet[k] for k in ("currencies", "both_closed_share")}})

    async def _upbit_for(self, client, listed: List[Dict[str, Any]], event: Dict[str, Any],
                         known: set) -> Dict[str, Any]:
        lower = utc(event["day0"]) - NOTICE_LOOKBACK

        def published(n: Dict[str, Any]) -> Optional[datetime]:
            try:
                return utc(n["first_listed_at"])
            except (KeyError, TypeError, ValueError):
                return None

        matched = [n for n in listed if event["ticker"] in caution_tickers(str(n.get("title") or ""))
                   and published(n) is not None and lower <= published(n)]
        out: Dict[str, Any] = {"listed": sorted(str(n["id"]) for n in matched), "notices": [], "unread": []}
        for n in matched:
            if str(n["id"]) in known:
                continue
            try:
                body = await upbit_body(client, n["id"])
            except Exception as exc:  # noqa: BLE001 - retried next cycle while before the entry close
                out["unread"].append(str(n["id"]))
                out.setdefault("body_errors", {})[str(n["id"])] = type(exc).__name__
                continue
            if not body:
                out["unread"].append(str(n["id"]))
                continue
            out["notices"].append({"id": str(n["id"]), "title": n.get("title"), "first_listed_at": n["first_listed_at"],
                                   "body_sha256": sha(body.encode("utf-8")), "phrase": wallet_phrase(body)})
        return out

    async def cycle(self, client) -> Dict[str, Any]:
        with exclusive(self.output) as mine:
            if not mine:
                return {"outcome": "busy"}
            return await self._cycle(client)

    async def _cycle(self, client) -> Dict[str, Any]:
        st = await asyncio.to_thread(state, self.journal)
        if not st["activation"]:
            return {"outcome": "not_activated"}
        if st["drift"]:
            return {"outcome": "drift_stopped", "drift": st["drift"]}
        report: Dict[str, Any] = {"outcome": "ok", "new_events": 0, "observed": 0, "decided": 0, "settled": 0}
        src = await asyncio.to_thread(load_source, self.source)
        trades = src.get("trades", {})
        activated = utc(st["activation"]["at"])
        excluded = set(st["activation"]["source_trades_at_activation"])
        now = now_utc()
        for key, t in sorted(trades.items(), key=lambda kv: (int(kv[1].get("day0_ms") or 0), kv[0])):
            if key in st["events"] or key in excluded or not eligible(t, activated):
                continue
            day0 = datetime.fromtimestamp(int(t["day0_ms"]) / 1000, timezone.utc)
            ev = {"source_id": key, "ticker": t["ticker"], "symbol": t["symbol"], "notice_id": str(t.get("notice_id")),
                  "source_kind": t.get("source"), "notice_at": t.get("notice_at"), "day0": day0.isoformat(),
                  "entry_at": (day0 + DAY).isoformat(), "source_discovered_at": t["discovered_at"],
                  "first_seen_at": now.isoformat(), "seen_before_entry": now < day0 + DAY}
            self.journal.append("event", ev)
            st["events"][key] = {"event": ev, "observations": []}
            report["new_events"] += 1

        pending = [e for e in st["events"].values() if "decision" not in e and now < utc(e["event"]["entry_at"])]
        if pending:
            try:
                listed: Optional[List[Dict[str, Any]]] = await upbit_notices(client)
                upbit_error = None
            except Exception as exc:  # noqa: BLE001 - one failed source is recorded, never filled in
                listed, upbit_error = None, type(exc).__name__
            try:
                wallet: Any = await _json(client, BITHUMB_WALLET)
                wallet_error = None
            except Exception as exc:  # noqa: BLE001
                wallet, wallet_error = None, type(exc).__name__
            for e in pending:
                ev = e["event"]
                known = {str(n["id"]) for o in e["observations"] for n in (o.get("upbit") or {}).get("notices", [])}
                up = {"error": upbit_error} if listed is None else await self._upbit_for(client, listed, ev, known)
                if wallet is None:
                    bt: Dict[str, Any] = {"error": wallet_error}
                else:
                    try:
                        bt = bithumb_view(wallet, ev["ticker"])
                    except (KeyError, TypeError, ValueError) as exc:
                        bt = {"error": type(exc).__name__}
                at = now_utc()
                if at >= utc(ev["entry_at"]):
                    continue  # received at or after the entry close: not evidence for this decision
                obs = {"source_id": ev["source_id"], "observed_at": at.isoformat(), "upbit": up, "bithumb": bt}
                self.journal.append("observation", obs)
                e["observations"].append(obs)
                report["observed"] += 1

        now = now_utc()
        for key, e in st["events"].items():
            if "decision" in e or now < utc(e["event"]["entry_at"]):
                continue
            e["decision"] = {"source_id": key, **decide(e["event"], e["observations"])}
            self.journal.append("decision", e["decision"])
            report["decided"] += 1

        for key, e in st["events"].items():
            dec = e.get("decision")
            if not dec or dec["action"] == "late" or "outcome" in e:
                continue
            t = trades.get(key)
            out: Optional[Dict[str, Any]] = None
            if t is None:
                out = {"void": "source_missing"}
            elif t.get("status") in VOID or t.get("late") or t.get("backfilled"):
                out = {"void": f"source_{t.get('status')}"}
            elif t.get("status") in SETTLED and t.get("hedged_return_pct") is not None:
                out = {k: t.get(k) for k in ("status", "return_pct", "hedged_return_pct", "entry_price", "exit_price",
                                              "entry_at", "exit_at", "funding")}
            elif t.get("status") in SETTLED and (t.get("hedge") or {}).get("error"):
                out = {"void": "no_hedged_return", "status": t.get("status"), "return_pct": t.get("return_pct")}
            if out is not None:
                e["outcome"] = {"source_id": key, **out}
                self.journal.append("outcome", e["outcome"])
                report["settled"] += 1
        report["source_health"] = (src.get("collection_health") or {}).get("status")
        return report


# --------------------------------------------------------------------------------------------- summary
def _mean(xs: List[float]) -> Optional[float]:
    return round(float(np.mean(xs)), 3) if xs else None


def _book(values: List[float]) -> Dict[str, Any]:
    return {"n": len(values), "mean_pct": _mean(values), "worst_pct": round(min(values), 3) if values else None,
            "stops": None}


def episodes(settled: List[Dict[str, Any]]) -> Dict[str, List[float]]:
    groups: Dict[tuple, List[float]] = {}
    for e in settled:
        groups.setdefault((e["event"]["entry_at"], e["decision"]["action"]), []).append(
            float(e["outcome"]["hedged_return_pct"]))
    out: Dict[str, List[float]] = {"keep": [], "skip": []}
    for (_, action), values in sorted(groups.items()):
        out[action].append(float(np.mean(values)))
    return out


def permutation_p(kept: List[float], skipped: List[float]) -> float:
    pool = np.array(kept + skipped, dtype=float)
    observed = np.mean(kept) - np.mean(skipped)
    rng = np.random.default_rng(SEED)
    hits = 0
    for _ in range(PERMUTATIONS):
        x = rng.permutation(pool)
        hits += (x[:len(kept)].mean() - x[len(kept):].mean()) >= observed - 1e-12
    return (hits + 1) / (PERMUTATIONS + 1)


def evaluate(settled: List[Dict[str, Any]], activated_at: Optional[str], now: Optional[datetime] = None) -> Dict[str, Any]:
    eps = episodes(settled)
    kept, skipped = eps["keep"], eps["skip"]
    now = now or now_utc()
    due = len(skipped) >= REVIEW_SKIPPED or (activated_at is not None and now >= utc(activated_at) + REVIEW_AFTER)
    out: Dict[str, Any] = {"kept_episodes": len(kept), "skipped_episodes": len(skipped),
                           "kept_mean_pct": _mean(kept), "skipped_mean_pct": _mean(skipped), "review_due": due,
                           "p_value": None}
    if not due:
        out["verdict"] = "collecting"
    elif len(skipped) < MIN_SKIPPED or not kept:
        out["verdict"] = "insufficient"
    elif np.mean(skipped) >= np.mean(kept):
        out["verdict"] = "rejected"
    else:
        out["p_value"] = round(permutation_p(kept, skipped), 4)
        out["verdict"] = "supported" if np.mean(skipped) < 0 and out["p_value"] <= ALPHA else "inconclusive"
    return out


def summary(output: Path = OUTPUT, now: Optional[datetime] = None) -> Dict[str, Any]:
    journal = Journal(output)
    try:
        st = state(journal)
    except Exception as exc:  # noqa: BLE001
        return {"status": "journal_broken", "error": str(exc)[:200]}
    if not st["activation"]:
        return {"status": "not_activated"}
    events = list(st["events"].values())
    decided = [e for e in events if "decision" in e]
    action = lambda e: e["decision"]["action"]  # noqa: E731
    settled = [e for e in decided if action(e) in ("keep", "skip")
               and (e.get("outcome") or {}).get("hedged_return_pct") is not None]
    hedged = lambda e: float(e["outcome"]["hedged_return_pct"])  # noqa: E731
    stops = lambda es: sum((e["outcome"].get("status") == "stopped") for e in es)  # noqa: E731
    books = {"baseline": _book([hedged(e) for e in settled]),
             "filtered": _book([hedged(e) if action(e) == "keep" else 0.0 for e in settled]),
             "kept": _book([hedged(e) for e in settled if action(e) == "keep"]),
             "skipped": _book([hedged(e) for e in settled if action(e) == "skip"])}
    books["baseline"]["stops"] = stops(settled)
    books["filtered"]["stops"] = books["kept"]["stops"] = stops([e for e in settled if action(e) == "keep"])
    books["skipped"]["stops"] = stops([e for e in settled if action(e) == "skip"])
    plain = [e for e in settled if e["outcome"].get("return_pct") is not None]
    return {
        "status": "drift_stopped" if st["drift"] else "running", "activated_at": st["activation"]["at"],
        "events": len(events), "pending_decision": sum("decision" not in e for e in events),
        "decisions": {k: sum(action(e) == k for e in decided) for k in ("keep", "skip", "late")},
        "skips_by_arm": {k: sum(action(e) == "skip" and k in e["decision"]["reason"] for e in decided)
                         for k in ("upbit_notice", "bithumb_wallet")},
        "open": sum(action(e) in ("keep", "skip") and "outcome" not in e for e in decided),
        "void": sum("void" in (e.get("outcome") or {}) for e in decided),
        "books": books,
        "plain_short_reference": {"baseline_mean_pct": _mean([float(e["outcome"]["return_pct"]) for e in plain]),
                                  "skipped_mean_pct": _mean([float(e["outcome"]["return_pct"]) for e in plain
                                                             if action(e) == "skip"])},
        "coverage": {"decided_without_upbit_list": sum(action(e) != "late" and not e["decision"].get("upbit_lists_read")
                                                       for e in decided),
                     "decided_without_valid_bithumb": sum(action(e) != "late"
                                                          and not e["decision"].get("bithumb_valid_snapshots")
                                                          for e in decided)},
        "evaluation": evaluate(settled, st["activation"]["at"], now),
        "drift": st["drift"],
    }


def registry_summary() -> Dict[str, Any]:
    """Shape expected by core.research.tracker_registry. Main number: return avoided per skipped episode."""
    s = summary()
    runner = json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    fresh = bool(runner.get("at")) and 0 <= (now_utc() - utc(runner["at"])).total_seconds() <= 2 * POLL_SECONDS + 60
    if s["status"] in ("not_activated", "journal_broken"):
        label = {"not_activated": "未激活", "journal_broken": "日志校验失败"}[s["status"]]
        return {"retirement": {"n": 0, "min_n": REVIEW_SKIPPED, "mean_pct": None, "ci90_pct": None,
                               "verdict": s["status"], "label": label, "reason": None, "clusters": None},
                "tracker_status_label": label, "paper_only": True, "runner_active": fresh}
    ev = s["evaluation"]
    avoided = -ev["skipped_mean_pct"] if ev["skipped_mean_pct"] is not None else None
    label = {"collecting": "积累样本", "insufficient": "到期样本不足", "rejected": "过滤无效",
             "supported": "过滤有效", "inconclusive": "未定"}[ev["verdict"]]
    status = "规则文件已变更 · 停账" if s["status"] == "drift_stopped" else (
        "运行异常" if runner.get("outcome") not in (None, "ok", "busy") else "运行心跳待恢复" if not fresh
        else "运行中 · 有待判定的警示" if s["pending_decision"] else "运行中 · 等待新警示")
    return {
        "retirement": {"n": ev["skipped_episodes"], "min_n": REVIEW_SKIPPED, "mean_pct": avoided, "ci90_pct": None,
                       "verdict": ev["verdict"], "label": label,
                       "reason": "合并版韩国警示做空的每笔前向交易，入场收盘前读 Upbit 警示公告正文与 Bithumb 公开钱包状态；"
                                 "确认充提双停就跳过（记 0%）。主数字为每个被跳过批次规避的对冲收益（正数=过滤有帮助）；"
                                 "10 个被跳过批次或满一年复核，复核前只作描述。",
                       "clusters": None},
        "started_at": s["activated_at"], "updated_at": runner.get("at") or s["activated_at"],
        "runner_active": fresh, "continuous_runner": fresh, "paper_only": True, "tracker_status_label": status,
        "forward_open": s["open"], "forward_waiting": s["pending_decision"], "forward_late": s["decisions"]["late"],
        "books": s["books"], "execution_authorized": False,
        "last_error": runner.get("error_type") if runner.get("outcome") not in (None, "ok", "busy") else None}


# ------------------------------------------------------------------------------------------ web worker
def _default_client():
    from core.research.exchange_research_runner import _client
    return _client()


async def worker(stop_event: asyncio.Event, heartbeat: Optional[Callable[[bool], None]] = None,
                 client_factory: Optional[Callable[[], Any]] = None) -> None:
    """Idles until activation; then one cycle every POLL_SECONDS. Paper ledger only."""
    while not stop_event.is_set():
        if (OUTPUT / "journal.jsonl").exists():
            try:
                async with (client_factory or _default_client)() as client:
                    result = await asyncio.wait_for(Ledger().cycle(client), timeout=CYCLE_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - retried next cycle
                result = {"outcome": "failed", "error_type": type(exc).__name__}
            ok = result.get("outcome") in ("ok", "busy")
        else:
            result, ok = {"outcome": "not_activated"}, True
        await asyncio.to_thread(atomic_write, STATE_PATH,
                                json.dumps({"at": now_utc().isoformat(), **result}, default=str).encode("utf-8"))
        if heartbeat:
            heartbeat(ok)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
