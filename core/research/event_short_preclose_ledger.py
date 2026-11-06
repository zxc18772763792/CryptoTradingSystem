"""Forward paper ledger event_short_preclose_v1: short >=10% unlocks and Upbit KRW listings before the close. No orders.

Pre-registered in ``config/research_protocols/event_short_preclose_v1.json`` after the 2026-11-05 account study and its
independent red team (docs/EVENT_SHORT_PRECLOSE_LEDGER.md). The existing trackers book the daily close as the fill, yet read
the order book minutes later: KRW-listed coins fall about 2% in the hour after that close, so that fill was not available.
This ledger takes its fills from books captured at 23:55 UTC, before the close, on both legs: short the coin, long an equal
notional of the 30 most-traded perps. It settles funding itself, so the settlement at the close counts on entry, not on exit.

Events come from two tracker files, read and never written (``unlock_short_t7``, ``upbit_krw_listing``). Returns do not:
every price, funding row and cost below is this ledger's own. Journal records are hash-chained and carry the SHA-256 of the
protocol and this module; any change stops the journal. The single review is journaled once and frozen.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
TRACKER_ID = PROTOCOL_ID = "event_short_preclose_v1"
OUTPUT = ROOT / "data/research/event_short_preclose_v1"
PROTOCOL = ROOT / "config/research_protocols/event_short_preclose_v1.json"
MODULE = Path(__file__)
UNLOCK_SOURCE = ROOT / "data/research/unlock_short_t7/tracker.json"
KRW_SOURCE = ROOT / "data/research/upbit_krw_listing/tracker.json"
SOURCE_MODULES = (ROOT / "core/research/unlock_short_tracker.py", ROOT / "core/research/upbit_caution_tracker.py")
SCHEDULE_CACHE = ROOT / "data/research/unlock_short/cache"
PANEL = ROOT / "data/research/xs_daily_panel"
STATE_PATH = ROOT / "runtime/event_short_preclose_ledger.json"
FAPI = "https://fapi.binance.com/fapi/v1"

# the rule
MIN_UNLOCK_PCT, REVISION_MIN_PCT, REVISION_DATE_DAYS = 10.0, 7.5, 3
CAPTURE_OPEN_SEC, CAPTURE_LAST_SEC = 300, 30  # 23:55:00 to 23:59:30 UTC
ORDER_USD, BASKET_SIZE, MIN_BASKET = 10_000.0, 30, 20
COIN_ROUND_TRIP, BASKET_ROUND_TRIP = 0.002, 0.001
IMPACT_Y, DEFAULT_SIGMA, SIGMA_RETURNS, MIN_SIGMA_RETURNS = 1.0, 0.10, 30, 10
STOP_PCT, STOP_SLIPPAGE, MEMBER_CAP = 0.40, 0.02, 1.0
STABLES = {"USDC", "FDUSD", "TUSD", "DAI", "USDP", "BUSD", "USDE", "USD1", "BFUSD", "EUR", "PYUSD", "RLUSD", "USDT",
           "PAXG", "XAUT"}  # stablecoins and gold tokens: neither hedges the crypto market
ACCOUNT_FRACTION = 0.05
# the review
REVIEW_BATCHES, MIN_BATCHES, REVIEW_AFTER_DAYS = 30, 15, 365
REJECT_MEAN_PCT, STREAM_MIN_TRADES, STREAM_REJECT_PCT = 2.0, 15, 1.0
ENTRY_GAP_TRADES, ENTRY_GAP_MIN_TRADES, ENTRY_GAP_REJECT_PCT = 15, 8, -1.0
REVISION_SHARE, REVISION_MIN_EVENTS = 0.15, 10
UPTREND_PRIOR, UPTREND_MIN_TRADES, TOP_COINS, WITHOUT_TOP_COINS_MIN_PCT = 0.15, 10, 2, 2.0
PERMUTATIONS, SEED, ALPHA = 10_000, 20261106, 0.05
ALERT_DRAWDOWN_PCT, ALERT_TRADE_PCT = -8.0, -70.0
# operations
POLL_SECONDS, WINDOW_POLL_SECONDS, CYCLE_TIMEOUT_SECONDS = 900, 20, 240
SETTLE_DELAY_MS, RETRY_DAYS, PATH_AFTER_MS = 600_000, 7, 2 * 3_600_000
PATH_BEFORE_MS, PATH_BARS = 30 * 60_000, 150
SOURCE_MAX_AGE = timedelta(hours=24)
HEARTBEAT_MAX_AGE_SECONDS = 2 * POLL_SECONDS + 60
LOCK_STALE_SECONDS = 600
HOUR_MS, DAY_MS = 3_600_000, 86_400_000
FUNDING_TOLERANCE_MS, MAX_FUNDING_GAP_MS, FUNDING_LIMIT = 60_000, 8 * HOUR_MS, 1000
DAY = timedelta(days=1)


class Unavailable(RuntimeError):
    """Exchange data that cannot be trusted right now; retried on the next pass, never booked as zero."""


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc(value: Any) -> datetime:
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("timezone required")
    return stamp.astimezone(timezone.utc)


def ms(stamp: datetime) -> int:
    return int(stamp.timestamp() * 1000)


def dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(raw)
    temp.replace(path)


def pins() -> Dict[str, str]:
    return {"protocol": sha(PROTOCOL.read_bytes()), "ledger": sha(MODULE.read_bytes())}


def positive(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise Unavailable(f"bad price {value!r}")
    return number


# -------------------------------------------------------------------------------------------- calendar
def day_close(day: str) -> datetime:
    """The close of a UTC day: 00:00 of the next one."""
    return utc(f"{day}T00:00:00+00:00") + DAY


def capture_open(day: str) -> datetime:
    return day_close(day) - timedelta(seconds=CAPTURE_OPEN_SEC)


def capture_last(day: str) -> datetime:
    return day_close(day) - timedelta(seconds=CAPTURE_LAST_SEC)


def next_day(day: str) -> str:
    return str((utc(f"{day}T00:00:00+00:00") + DAY).date())


def sleep_seconds(now: datetime) -> float:
    """Until the next pass: every WINDOW_POLL_SECONDS inside the window, and never past its opening."""
    today = str(now.date())
    if now < capture_open(today):
        return max(1.0, min(POLL_SECONDS, (capture_open(today) - now).total_seconds()))
    return float(WINDOW_POLL_SECONDS) if now <= capture_last(today) else float(POLL_SECONDS)


# ---------------------------------------------------------------------------------------------- events
def unlock_candidate(key: str, trade: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        size = float(trade["size_pct"])
        entry_day, exit_day = str(trade["entry_day"]), str(trade["exit_day"])
        day_close(entry_day), day_close(exit_day)
    except (KeyError, TypeError, ValueError):
        return None
    if not trade.get("symbol") or not math.isfinite(size) or size < MIN_UNLOCK_PCT:
        return None
    return {"source_id": f"U|{key}", "stream": "U", "token": trade.get("token"), "symbol": trade["symbol"], "size_pct": size,
            "unlock_date": trade.get("unlock_date"), "entry_day": entry_day, "exit_day": exit_day,
            "source_discovered_at": trade.get("discovered_at"), "source_late": bool(trade.get("late"))}


def krw_candidate(key: str, trade: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if trade.get("backfilled") or not trade.get("symbol") or trade.get("status") in {"no_perp", "delisted"}:
        return None
    try:
        day0 = datetime.fromtimestamp(int(trade["day0_ms"]) / 1000, timezone.utc)
    except (KeyError, TypeError, ValueError):
        return None
    return {"source_id": f"K|{key}", "stream": "K", "token": trade.get("ticker"), "symbol": trade["symbol"],
            "notice_at": trade.get("notice_at"), "notice_id": trade.get("notice_id"), "entry_day": str(day0.date()),
            "exit_day": str((day0 + 7 * DAY).date()), "source_discovered_at": trade.get("discovered_at"),
            "source_late": bool(trade.get("late"))}


def candidates(unlock: Dict[str, Any], krw: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = [unlock_candidate(k, t) for k, t in (unlock.get("trades") or {}).items()]
    out += [krw_candidate(k, t) for k, t in (krw.get("trades") or {}).items()]
    return sorted((c for c in out if c), key=lambda c: (c["entry_day"], {"U": 0, "K": 1}[c["stream"]], c["symbol"], c["source_id"]))


# ------------------------------------------------------------------------------------- books and costs
def levels(side: Any) -> List[Tuple[float, float]]:
    return [(positive(p), float(q)) for p, q in side or []]


def vwap(side: List[Tuple[float, float]], usd: float) -> Tuple[float, bool]:
    """Average fill for a USDT amount, walking the book; what the book cannot fill goes at its last level."""
    if not side:
        raise Unavailable("empty book side")
    left, qty = usd, 0.0
    for price, size in side:
        take = min(left, price * size)
        qty += take / price
        left -= take
        if left <= 1e-9:
            return usd / qty, False
    qty += left / side[-1][0]
    return usd / qty, True


def book_record(depth: Dict[str, Any]) -> Dict[str, Any]:
    """Fills go at the best bid and ask, with impact modelled apart. The walked prices only show how good that model is."""
    bids, asks = levels(depth.get("bids")), levels(depth.get("asks"))
    if not bids or not asks:
        raise Unavailable("empty book")
    mid = (bids[0][0] + asks[0][0]) / 2
    sell, sell_short = vwap(bids, ORDER_USD)
    buy, buy_short = vwap(asks, ORDER_USD)
    return {"best_bid": bids[0][0], "best_ask": asks[0][0], "mid": mid, "spread_bps": round((asks[0][0] - bids[0][0]) / mid * 1e4, 3),
            "vwap_sell": sell, "vwap_buy": buy, "sell_short_of_depth": sell_short, "buy_short_of_depth": buy_short,
            "bid_usdt_1pct": round(sum(p * q for p, q in bids if p >= mid * 0.99), 2),
            "ask_usdt_1pct": round(sum(p * q for p, q in asks if p <= mid * 1.01), 2),
            "exchange_time_ms": depth.get("T") or depth.get("E")}


def sigma_of(closes: List[float]) -> float:
    """Sample standard deviation of the last SIGMA_RETURNS daily returns; DEFAULT_SIGMA when under MIN_SIGMA_RETURNS."""
    values = closes[-(SIGMA_RETURNS + 1):]
    returns = [values[i] / values[i - 1] - 1 for i in range(1, len(values))]
    if len(returns) < MIN_SIGMA_RETURNS:
        return DEFAULT_SIGMA
    return float(np.std(returns, ddof=1))


def impact_side(sigma: float, order_usd: float, quote_volume: float) -> float:
    """Square-root impact of one side as a fraction of notional. No volume reading costs the whole daily sigma."""
    return IMPACT_Y * sigma * math.sqrt(order_usd / quote_volume) if quote_volume > 0 else IMPACT_Y * sigma


def funding_cash(rows: List[Dict[str, Any]], start_ms: int, end_ms: int, entry_price: float, side: int) -> Tuple[float, List[int]]:
    """Cash per unit of entry notional for settlements in (start, end]; side +1 long, -1 short (a positive rate: longs pay)."""
    total, stamps = 0.0, []
    for row in rows:
        stamp = int(row["fundingTime"])
        if start_ms < stamp <= end_ms:
            rate = float(row["fundingRate"])
            try:
                mark = positive(row.get("markPrice"))
            except (TypeError, ValueError, Unavailable):
                mark = entry_price
            total += -side * rate * mark / entry_price
            stamps.append(stamp)
    return total, sorted(stamps)


def has_funding_gap(stamps: List[int], start_ms: int, end_ms: int) -> bool:
    points = [start_ms] + stamps + [end_ms]
    return max(b - a for a, b in zip(points, points[1:])) > MAX_FUNDING_GAP_MS + FUNDING_TOLERANCE_MS


def first_stop(bars: List[List[Any]], entry_price: float) -> Optional[Dict[str, Any]]:
    """The first hourly bar reaching +40%: filled 2% beyond the level, or beyond the open if it gapped through."""
    level = entry_price * (1 + STOP_PCT)
    for bar in bars:
        open_ms, opened, high = int(bar[0]), float(bar[1]), float(bar[2])
        if high >= level:
            return {"bar_open_ms": open_ms, "exit_ms": open_ms + HOUR_MS, "level": level, "high": high,
                    "fill": max(level, opened) * (1 + STOP_SLIPPAGE)}
    return None


def settle_numbers(entry: Dict[str, Any], leaving: Dict[str, Any], stop: Optional[Dict[str, Any]], coin_rows: List[Dict[str, Any]],
                   member_rows: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """All percentages are of one leg's entry notional."""
    entry_price, entry_ms = float(entry["entry_price"]), int(entry["entry_ms"])
    exit_price, exit_ms = float(leaving["coin"]["exit_price"]), int(leaving["coin"]["exit_ms"])
    price = (entry_price - exit_price) / entry_price
    coin_funding, stamps = funding_cash(coin_rows, entry_ms, exit_ms + FUNDING_TOLERANCE_MS, entry_price, -1)
    impact_coin = float(entry["impact_side"]) + float(leaving["coin"]["impact_side"])
    coin_net = price + coin_funding - COIN_ROUND_TRIP - impact_coin
    left = {m["s"]: m for m in leaving["basket"]}
    order = ORDER_USD / len(entry["basket"])
    rows, missing = [], []
    basket_end = int(leaving["basket_ms"])
    for member in entry["basket"]:
        symbol, later = member["s"], left.get(member["s"])
        if later is None or later.get("bid") is None or symbol not in member_rows:
            missing.append(symbol)
            continue
        ask = float(member["ask"])
        move = (float(later["bid"]) - ask) / ask
        cash, _ = funding_cash(member_rows[symbol], entry_ms, basket_end, ask, 1)
        cost = impact_side(float(member["sigma"]), order, float(member["qv"])) + impact_side(float(member["sigma"]), order, float(later.get("qv") or 0))
        rows.append((move, min(move, MEMBER_CAP), cash, cost))
    if len(rows) < MIN_BASKET:
        raise Unavailable(f"basket resolved only {len(rows)} members")
    moves, capped, cash, cost = (float(np.mean([r[i] for r in rows])) for i in range(4))
    basket_net = moves + cash - cost - BASKET_ROUND_TRIP
    basket_capped = capped + cash - cost - BASKET_ROUND_TRIP
    pct = lambda x: round(x * 100, 4)
    return {"entry_price": entry_price, "exit_price": exit_price, "exit_ms": exit_ms, "basket_exit_ms": basket_end,
            "price_pct": pct(price), "funding_pct": pct(coin_funding), "fixed_cost_pct": pct(COIN_ROUND_TRIP), "impact_coin_pct": pct(impact_coin),
            "coin_net_pct": pct(coin_net), "basket_price_pct": pct(moves), "basket_price_capped_pct": pct(capped), "basket_funding_pct": pct(cash),
            "basket_impact_pct": pct(cost), "basket_fixed_cost_pct": pct(BASKET_ROUND_TRIP), "basket_net_pct": pct(basket_net),
            "basket_capped_net_pct": pct(basket_capped), "hedged_pct": pct(coin_net + basket_net), "hedged_capped_pct": pct(coin_net + basket_capped),
            "members": len(rows), "members_missing": missing, "funding_gap": has_funding_gap(stamps, entry_ms, exit_ms + FUNDING_TOLERANCE_MS),
            "stopped": stop is not None}


# --------------------------------------------------------------------------------------------- journal
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


EVENT_KINDS = {"void": "void", "entry": "entry", "stop": "stop", "revision": "revision", "exit": "exit", "outcome": "outcome", "path": "path"}


def state(journal: Journal) -> Dict[str, Any]:
    recs = journal.records()
    st: Dict[str, Any] = {"activation": None, "events": {}, "review": None, "drift": None, "alerts": {}, "snapshot_day": None,
                          "last_seq": len(recs) - 1}
    for rec in recs:
        d = rec["data"]
        if rec["kind"] == "activation":
            st["activation"] = {**d, "at": rec["at"], "pins": rec["pins"]}
        elif rec["kind"] == "event":
            st["events"][d["source_id"]] = {"event": d}
        elif rec["kind"] in EVENT_KINDS:
            st["events"][d["source_id"]][EVENT_KINDS[rec["kind"]]] = d
        elif rec["kind"] == "schedule_snapshot":
            st["snapshot_day"] = d["date"]
        elif rec["kind"] == "safety_alert":
            st["alerts"][d["key"]] = {**d, "at": rec["at"]}
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


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------------------- trade phases
def is_waiting(e: Dict[str, Any]) -> bool:
    return "entry" not in e and "void" not in e


def is_open(e: Dict[str, Any]) -> bool:
    return "entry" in e and "exit" not in e


def is_exited(e: Dict[str, Any]) -> bool:
    return "exit" in e and "outcome" not in e


def exit_due_day(e: Dict[str, Any]) -> str:
    """The planned exit day, or the first window at or after a stop or revision was seen."""
    planned = e["event"]["exit_day"]
    mark = e.get("stop", {}).get("detected_at") or e.get("revision", {}).get("at")
    if mark is None:
        return planned
    stamp = utc(mark)
    day = str(stamp.date())
    if stamp > capture_last(day):
        day = next_day(day)
    return min(day, planned)


def revision_of(event: Dict[str, Any], tokens: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Why an unlock no longer qualifies in the latest schedule; None when it does, or when the token has no schedule file."""
    if event["stream"] != "U" or event.get("token") not in tokens or not event.get("unlock_date"):
        return None
    unlock = utc(f"{event['unlock_date']}T00:00:00+00:00")
    near = [c for c in tokens[event["token"]]["cliffs"] if abs((utc(f"{c['date']}T00:00:00+00:00") - unlock).days) <= REVISION_DATE_DAYS]
    if any(float(c["size_pct"]) >= REVISION_MIN_PCT for c in near):
        return None
    return {"frozen_size_pct": event.get("size_pct"), "unlock_date": event["unlock_date"], "near_cliffs": near}


# ------------------------------------------------------------------------------------------- statistics
def _mean(xs: List[float]) -> Optional[float]:
    return round(float(np.mean(xs)), 3) if xs else None


def settled_trades(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [e for e in events if (e.get("outcome") or {}).get("hedged_pct") is not None]


def batches(events: List[Dict[str, Any]]) -> List[Tuple[str, float, float]]:
    """(entry day, mean hedged, mean capped) per entry day, once every entered trade of that day has an outcome."""
    groups: Dict[str, List[Optional[Tuple[float, float]]]] = {}
    for e in events:
        if "entry" not in e:
            continue
        outcome = e.get("outcome")
        if outcome is None:
            groups.setdefault(e["event"]["entry_day"], []).append(None)
        elif outcome.get("hedged_pct") is not None:
            groups.setdefault(e["event"]["entry_day"], []).append((float(outcome["hedged_pct"]), float(outcome["hedged_capped_pct"])))
    return [(day, float(np.mean([v[0] for v in values])), float(np.mean([v[1] for v in values])))
            for day, values in sorted(groups.items()) if values and all(v is not None for v in values)]


def sign_flip_p(values: List[float]) -> float:
    """One-sided: how often random signs give a mean at least as high as the observed one."""
    x = np.array(values, dtype=float)
    signs = np.random.default_rng(SEED).choice([-1.0, 1.0], size=(PERMUTATIONS, len(x)))
    hits = int(((signs * x).mean(axis=1) >= x.mean() - 1e-12).sum())
    return (hits + 1) / (PERMUTATIONS + 1)


def review_scope(events: List[Dict[str, Any]], activated_at: Optional[str], now: datetime) -> Tuple[bool, Optional[str]]:
    """(due, last entry day in scope). By count: the first REVIEW_BATCHES entry days, once all of them are complete."""
    entered = sorted({e["event"]["entry_day"] for e in events if "entry" in e})
    complete = {row[0] for row in batches(events)}
    voided = {e["event"]["entry_day"] for e in events if "entry" in e and "outcome" in e and e["outcome"].get("hedged_pct") is None}
    counted = [d for d in entered if d in complete or d not in voided]
    if len(counted) >= REVIEW_BATCHES and all(d in complete for d in counted[:REVIEW_BATCHES]):
        return True, counted[REVIEW_BATCHES - 1]
    if activated_at is not None and now >= utc(activated_at) + timedelta(days=REVIEW_AFTER_DAYS):
        return True, None
    return False, None


def evaluate(events: List[Dict[str, Any]], activated_at: Optional[str], now: Optional[datetime] = None,
             prior_moves: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """The verdict at the single review; ``collecting`` before it. Before the review no p-value is computed."""
    now = now or now_utc()
    due, cutoff = review_scope(events, activated_at, now)
    scope = [e for e in events if cutoff is None or e["event"]["entry_day"] <= cutoff]
    rows = batches(scope)
    values, capped = [r[1] for r in rows], [r[2] for r in rows]
    trades = settled_trades(scope)
    out: Dict[str, Any] = {"review_due": due, "cutoff_entry_day": cutoff, "batches": len(rows), "batch_mean_pct": _mean(values),
                           "capped_batch_mean_pct": _mean(capped), "p_sign_flip": None, "trades": len(trades)}
    if not due:
        out["verdict"] = "collecting"
        return out
    streams: Dict[str, Dict[str, Any]] = {}
    for stream in ("U", "K"):
        got = [float(e["outcome"]["hedged_pct"]) for e in trades if e["event"]["stream"] == stream]
        verdict = "too_few" if len(got) < STREAM_MIN_TRADES else "rejected" if np.mean(got) <= STREAM_REJECT_PCT else "ok"
        streams[stream] = {"trades": len(got), "mean_pct": _mean(got), "verdict": verdict, "reasons": ["mean"] if verdict == "rejected" else []}
    # unlock events that were revised, moved, removed or first seen too late; the backtest could not see any of these
    unlock_events = [e for e in scope if e["event"]["stream"] == "U"]
    broken = [e for e in unlock_events if "revision" in e or e.get("void", {}).get("reason") == "late"]
    share = len(broken) / len(unlock_events) if unlock_events else None
    streams["U"].update(events=len(unlock_events), revised_or_late=len(broken), revised_or_late_share=round(share, 3) if share is not None else None)
    if len(unlock_events) >= REVISION_MIN_EVENTS and share > REVISION_SHARE:
        streams["U"].update(verdict="rejected", reasons=streams["U"]["reasons"] + ["schedule"])
    # does the unlock short hold up after a market rise? the backtest says this is its weak spot
    up = None
    if prior_moves is not None:
        up = [float(e["outcome"]["hedged_pct"]) for e in trades if e["event"]["stream"] == "U"
              and prior_moves.get(e["event"]["entry_day"], float("nan")) > UPTREND_PRIOR]
    covered = up is not None and len(up) >= UPTREND_MIN_TRADES
    streams["U"].update(uptrend_trades=None if up is None else len(up), uptrend_mean_pct=_mean(up or []), uptrend_covered=covered)
    if covered and np.mean(up) <= 0:
        streams["U"].update(verdict="rejected", reasons=streams["U"]["reasons"] + ["uptrend"])
    # did the pre-close fill actually get the close's price on the coins that fall right after it?
    krw = sorted((e for e in trades if e["event"]["stream"] == "K" and e["outcome"].get("entry_gap_pct") is not None), key=lambda e: e["event"]["entry_day"])
    gaps = [float(e["outcome"]["entry_gap_pct"]) for e in krw[:ENTRY_GAP_TRADES]]
    entry_rule_rejected = len(gaps) >= ENTRY_GAP_MIN_TRADES and float(np.mean(gaps)) <= ENTRY_GAP_REJECT_PCT
    # the rule must not rest on the two best coins: the other coins' trades have to clear the same bar on their own
    by_coin: Dict[str, float] = {}
    for e in trades:
        by_coin[e["event"]["symbol"]] = by_coin.get(e["event"]["symbol"], 0.0) + float(e["outcome"]["hedged_pct"])
    best = set(sorted(by_coin, key=by_coin.get, reverse=True)[:TOP_COINS])
    others = [float(e["outcome"]["hedged_pct"]) for e in trades if e["event"]["symbol"] not in best]
    concentrated = not others or float(np.mean(others)) <= WITHOUT_TOP_COINS_MIN_PCT
    out.update(streams=streams, entry_gap_trades=len(gaps), entry_gap_mean_pct=_mean(gaps), entry_rule_rejected=entry_rule_rejected,
               top_coins=sorted(best), without_top_coins_mean_pct=_mean(others), concentrated=concentrated)
    if len(rows) < MIN_BATCHES:
        out["verdict"] = "insufficient"
    elif np.mean(values) <= REJECT_MEAN_PCT or np.mean(capped) <= REJECT_MEAN_PCT:
        out["verdict"] = "rejected"
    else:
        out["p_sign_flip"] = round(sign_flip_p(values), 4)
        if entry_rule_rejected or any(s["verdict"] == "rejected" for s in streams.values()):
            out["verdict"] = "partly_rejected"
        elif out["p_sign_flip"] > ALPHA or concentrated:
            out["verdict"] = "inconclusive"
        else:
            out["verdict"] = "supported" if covered else "supported_uptrend_uncovered"
    return out


def realized_drawdown_pct(trades: List[Dict[str, Any]]) -> float:
    """Account at ACCOUNT_FRACTION per trade in exit order; open positions are not marked."""
    equity, peak, worst = 1.0, 1.0, 0.0
    for e in sorted(trades, key=lambda x: int(x["outcome"]["basket_exit_ms"])):
        equity *= 1 + ACCOUNT_FRACTION * float(e["outcome"]["hedged_pct"]) / 100
        peak = max(peak, equity)
        worst = min(worst, equity / peak - 1)
    return round(worst * 100, 3)


def prior_index_moves(days: List[str], panel: Path = PANEL, span: int = 60) -> Dict[str, float]:
    """Move of a daily-rebalanced equal-weight top-30 index over the days before each entry. Read only at the review."""
    import pandas as pd

    close = pd.read_parquet(Path(panel) / "col_close.parquet").astype("float64")
    volume = pd.read_parquet(Path(panel) / "col_volume.parquet").astype("float64")
    returns = close.pct_change(fill_method=None)
    rank = volume.shift(1).rank(axis=1, ascending=False)
    level = (1 + returns.where(rank <= BASKET_SIZE).mean(axis=1).fillna(0)).cumprod()
    out = {}
    for day in days:
        stamp = pd.Timestamp(day)
        if stamp in level.index and level.index.get_loc(stamp) >= span:
            at = level.index.get_loc(stamp)
            out[day] = float(level.iloc[at] / level.iloc[at - span] - 1)
    return out


# ------------------------------------------------------------------------------- point-in-time schedules
def schedule_snapshot(now: datetime, cache: Path = SCHEDULE_CACHE, output: Path = OUTPUT) -> Dict[str, Any]:
    """Today's view of every cached schedule: index hash, file hash where a cliff is near, cliffs >= 5% from t-7 to t+120.

    The research could not show what was known at t-7, so the forward test keeps that record. Reads local files only."""
    import pandas as pd

    from core.research.unlock_events import cliff_events, entry_ticker, unlocked_series

    raw_index = (Path(cache) / "emissions_index.json").read_bytes()
    parsed = json.loads(raw_index)
    index = parsed["data"] if isinstance(parsed, dict) else parsed
    today = pd.Timestamp(now.date(), tz="UTC")
    tokens: Dict[str, Any] = {}
    for entry in index:
        slug, ticker = entry.get("protocolSlug"), entry_ticker(entry)
        path = Path(cache) / "emissions" / f"{slug}.json"
        if not ticker or not path.exists():
            continue
        side = path.with_suffix(".unlocked.json")
        if side.exists() and side.stat().st_mtime >= path.stat().st_mtime:
            data = json.loads(side.read_text(encoding="utf-8"))
            series = pd.Series(data["v"], index=pd.to_datetime(data["t"], unit="s", utc=True), dtype=float)
        else:
            series = unlocked_series(json.loads(path.read_text(encoding="utf-8")))
        cliffs = [{"date": str(c["date"].date()), "size_pct": round(float(c["size_pct"]), 2)} for c in cliff_events(entry, series, 5.0, merge_days=1)
                  if today - pd.Timedelta(days=7) <= c["date"] <= today + pd.Timedelta(days=120)]
        item: Dict[str, Any] = {"slug": slug, "file_mtime": int(path.stat().st_mtime), "cliffs": cliffs}
        if cliffs:
            item["file_sha256"] = sha(path.read_bytes())
        tokens[ticker] = item
    day = str(now.date())
    payload = dumps({"date": day, "index_sha256": sha(raw_index), "tokens": tokens})
    target = Path(output) / "schedule_snapshots" / f"{day}.json.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(target.name + ".tmp")
    with open(temp, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as packed:
        packed.write(payload)
    temp.replace(target)
    return {"tokens": tokens, "record": {"date": day, "file": target.name, "sha256": sha(payload), "index_sha256": sha(raw_index),
                                         "tokens": len(tokens), "tokens_with_near_cliff": sum(bool(t["cliffs"]) for t in tokens.values())}}


# ----------------------------------------------------------------------------------------- exchange reads
async def get_json(client, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
    resp = await client.get(f"{FAPI}/{endpoint}", params=params or {})
    if resp.status_code != 200:
        raise Unavailable(f"{endpoint} http {resp.status_code}")
    return resp.json()


async def market_snapshot(client) -> Dict[str, Any]:
    info = await get_json(client, "exchangeInfo")
    tradable = [s for s in info["symbols"] if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"]
    if not tradable:
        raise Unavailable("no tradable contracts")
    books = {r["symbol"]: (float(r["bidPrice"]), float(r["askPrice"])) for r in await get_json(client, "ticker/bookTicker")}
    volumes = {r["symbol"]: float(r.get("quoteVolume") or 0) for r in await get_json(client, "ticker/24hr")}
    live = {s["symbol"] for s in tradable}
    pool = {s["symbol"] for s in tradable if s.get("underlyingType", "COIN") == "COIN" and s.get("baseAsset") not in STABLES}
    ranked = sorted((s for s in pool if s in volumes and s in books and min(books[s]) > 0), key=lambda s: volumes[s], reverse=True)
    return {"live": live, "ranked": ranked, "books": books, "volumes": volumes}


async def daily_closes(client, symbol: str, close_ms: int) -> List[float]:
    """Daily closes through the entry day. Its bar is still open at 23:55, so the last value is the current price."""
    rows = await get_json(client, "klines", {"symbol": symbol, "interval": "1d", "limit": SIGMA_RETURNS + 3})
    return [float(r[4]) for r in rows if int(r[0]) + DAY_MS <= close_ms]


async def funding_rows(client, symbol: str, start_ms: int, now_ms: int) -> List[Dict[str, Any]]:
    """Latest-N form only (the window form drew WAF 403s, see event_risk_shadow). Coverage back to the entry is proved, not assumed."""
    limit = min(FUNDING_LIMIT, (now_ms - start_ms) // HOUR_MS + 3)
    rows = await get_json(client, "fundingRate", {"symbol": symbol, "limit": limit})
    if not isinstance(rows, list):
        raise Unavailable(f"funding history malformed for {symbol}")
    if rows and len(rows) >= limit and min(int(r["fundingTime"]) for r in rows) > start_ms:
        raise Unavailable(f"funding history does not reach the entry for {symbol}")
    return rows


# ---------------------------------------------------------------------------------------------- ledger
class Ledger:
    def __init__(self, output: Path = OUTPUT, unlock_source: Path = UNLOCK_SOURCE, krw_source: Path = KRW_SOURCE,
                 cache: Path = SCHEDULE_CACHE, panel: Path = PANEL, clock: Callable[[], datetime] = now_utc):
        self.output, self.unlock_source, self.krw_source = Path(output), Path(unlock_source), Path(krw_source)
        self.cache, self.panel, self.clock = Path(cache), Path(panel), clock
        self.journal = Journal(self.output)

    def activate(self) -> Dict[str, Any]:
        with exclusive(self.output) as mine:
            if not mine:
                raise RuntimeError("another writer holds the journal lock")
            if self.journal.records():
                raise RuntimeError("journal already exists; a new protocol needs a new directory")
            now = self.clock()
            unlock, krw = load_json(self.unlock_source), load_json(self.krw_source)
            for name, source in (("unlock", unlock), ("krw", krw)):
                if now - utc(source["updated_at"]) > SOURCE_MAX_AGE:
                    raise RuntimeError(f"{name} source tracker is stale: {source['updated_at']}")
            return self.journal.append("activation", {
                "protocol": json.loads(PROTOCOL.read_text(encoding="utf-8"))["protocol_id"],
                "source_modules_sha256": {p.name: sha(p.read_bytes()) for p in SOURCE_MODULES},
                "source_updated_at": {"unlock": unlock["updated_at"], "krw": krw["updated_at"]},
                "upcoming_at_activation": [c["source_id"] for c in candidates(unlock, krw)
                                           if capture_open(c["entry_day"]) > now and not c["source_late"]]})

    async def cycle(self, client) -> Dict[str, Any]:
        with exclusive(self.output) as mine:
            if not mine:
                return {"outcome": "busy"}
            return await asyncio.wait_for(self._cycle(client), timeout=CYCLE_TIMEOUT_SECONDS)

    async def _cycle(self, client) -> Dict[str, Any]:
        st = await asyncio.to_thread(state, self.journal)
        if not st["activation"]:
            return {"outcome": "not_activated"}
        if st["drift"]:
            return {"outcome": "drift_stopped", "drift": st["drift"]}
        report: Dict[str, Any] = {"outcome": "ok", "new_events": 0, "entries": 0, "voids": 0, "stops": 0, "exits": 0, "settled": 0,
                                  "reviewed": False, "errors": []}
        unlock, krw = await asyncio.gather(asyncio.to_thread(load_json, self.unlock_source), asyncio.to_thread(load_json, self.krw_source))
        report["source_updated_at"] = {"unlock": unlock.get("updated_at"), "krw": krw.get("updated_at")}
        self._discover(st, unlock, krw, report)
        for step in (self._schedule, self._stops, self._capture, self._settle, self._paths):
            try:
                await step(client, st, report)
            except Exception as exc:  # noqa: BLE001 - each step is retried on the next pass
                report["errors"].append(f"{step.__name__}:{type(exc).__name__}")
        self._void_missed(st, report)
        self._alerts(st)
        await self._review(st, report)
        if report["errors"]:
            report["outcome"] = "degraded"
        return report

    def _void(self, st: Dict[str, Any], e: Dict[str, Any], reason: str, report: Dict[str, Any], **extra: Any) -> None:
        record = {"source_id": e["event"]["source_id"], "reason": reason, **extra}
        self.journal.append("void", record)
        e["void"] = record
        report["voids"] += 1

    def _discover(self, st: Dict[str, Any], unlock: Dict[str, Any], krw: Dict[str, Any], report: Dict[str, Any]) -> None:
        now, activated = self.clock(), utc(st["activation"]["at"])
        for c in candidates(unlock, krw):
            if c["source_id"] in st["events"] or capture_last(c["entry_day"]) <= activated:
                continue
            event = {**c, "first_seen_at": now.isoformat(), "seen_before_capture": now < capture_open(c["entry_day"])}
            self.journal.append("event", event)
            e = st["events"][c["source_id"]] = {"event": event}
            report["new_events"] += 1
            if c["source_late"] or not event["seen_before_capture"]:
                self._void(st, e, "late", report)

    async def _schedule(self, client, st: Dict[str, Any], report: Dict[str, Any]) -> None:
        now = self.clock()
        today = str(now.date())
        if st["snapshot_day"] == today or capture_open(today) <= now <= capture_last(today):
            return
        snap = await asyncio.to_thread(schedule_snapshot, now, self.cache, self.output)
        self.journal.append("schedule_snapshot", snap["record"])
        st["snapshot_day"] = today
        for e in st["events"].values():
            if "revision" in e or "void" in e or "exit" in e:
                continue
            why = revision_of(e["event"], snap["tokens"])
            if why is None:
                continue
            record = {"source_id": e["event"]["source_id"], "at": now.isoformat(), **why}
            self.journal.append("revision", record)
            e["revision"] = record
            if is_waiting(e):
                self._void(st, e, "revised_before_entry", report)

    async def _stops(self, client, st: Dict[str, Any], report: Dict[str, Any]) -> None:
        now = self.clock()
        today = str(now.date())
        windowed = capture_open(today) <= now <= capture_last(today)
        checked = st.setdefault("stop_checked", set())
        for e in st["events"].values():
            if not is_open(e) or "stop" in e:
                continue
            if windowed and exit_due_day(e) != today:
                continue  # inside the window only the trades about to exit need a look
            start = int(e["entry"]["entry_close_ms"])
            if ms(now) < start:
                continue
            try:
                bars = await get_json(client, "klines", {"symbol": e["event"]["symbol"], "interval": "1h", "startTime": start,
                                                         "limit": min(1000, (ms(now) - start) // HOUR_MS + 2)})
            except Exception as exc:  # noqa: BLE001 - the exit waits for a check, see _capture
                report["errors"].append(f"stop:{e['event']['symbol']}:{type(exc).__name__}")
                continue
            checked.add(e["event"]["source_id"])
            hit = first_stop(bars, float(e["entry"]["entry_price"]))
            if hit:
                record = {"source_id": e["event"]["source_id"], "detected_at": self.clock().isoformat(), **hit}
                self.journal.append("stop", record)
                e["stop"] = record
                report["stops"] += 1

    async def _capture(self, client, st: Dict[str, Any], report: Dict[str, Any]) -> None:
        now = self.clock()
        entries = [e for e in st["events"].values() if is_waiting(e) and capture_open(e["event"]["entry_day"]) <= now <= capture_last(e["event"]["entry_day"])]
        checked = st.get("stop_checked", set())
        # a time exit needs this pass's stop check; if the bars stay unreadable it goes after the window, marked late
        exits = [e for e in st["events"].values() if is_open(e) and now >= capture_open(exit_due_day(e))
                 and ("stop" in e or e["event"]["source_id"] in checked or now > capture_last(exit_due_day(e)))]
        if not entries and not exits:
            return
        market = await market_snapshot(client)
        sigmas: Dict[str, float] = {}
        # exits first: a coin leaving today is free to be shorted again by a new event today
        for e in exits:
            try:
                self._record_exit(e, await self._exit_record(client, e, market))
                report["exits"] += 1
            except Exception as exc:  # noqa: BLE001 - one coin must not block the others
                report["errors"].append(f"exit:{e['event']['symbol']}:{type(exc).__name__}")
        for e in sorted(entries, key=lambda x: ({"U": 0, "K": 1}[x["event"]["stream"]], x["event"]["symbol"])):
            symbol = e["event"]["symbol"]
            if any(is_open(other) and other["event"]["symbol"] == symbol for other in st["events"].values()):
                self._void(st, e, "coin_already_held", report)
                continue
            if symbol not in market["live"]:
                self._void(st, e, "not_trading", report)
                continue
            try:
                record = await self._entry_record(client, e, market, sigmas)
            except Exception as exc:  # noqa: BLE001 - retried until the window ends
                report["errors"].append(f"entry:{symbol}:{type(exc).__name__}")
                continue
            self.journal.append("entry", record)
            e["entry"] = record
            report["entries"] += 1

    async def _sigma(self, client, symbol: str, before_ms: int, cache: Dict[str, float]) -> float:
        if symbol not in cache:
            cache[symbol] = sigma_of(await daily_closes(client, symbol, before_ms))
        return cache[symbol]

    async def _entry_record(self, client, e: Dict[str, Any], market: Dict[str, Any], sigmas: Dict[str, float]) -> Dict[str, Any]:
        event = e["event"]
        symbol = event["symbol"]
        close_ms = ms(day_close(event["entry_day"]))
        book = book_record(await get_json(client, "depth", {"symbol": symbol, "limit": 100}))
        stamp = self.clock()
        members = [s for s in market["ranked"] if s != symbol][:BASKET_SIZE]
        if len(members) < MIN_BASKET:
            raise Unavailable("basket too small")
        semaphore = asyncio.Semaphore(6)

        async def one(member: str) -> Tuple[str, float]:
            async with semaphore:
                try:
                    return member, await self._sigma(client, member, close_ms, sigmas)
                except Exception:  # noqa: BLE001 - a missing history costs the default sigma, not the entry
                    return member, DEFAULT_SIGMA

        member_sigma = dict(await asyncio.gather(*(one(m) for m in members)))
        sigma = await self._sigma(client, symbol, close_ms, sigmas)
        volume = float(market["volumes"].get(symbol) or 0)
        return {"source_id": event["source_id"], "captured_at": stamp.isoformat(), "entry_ms": ms(stamp), "entry_close_ms": close_ms,
                "entry_price": book["best_bid"], "book": book, "sigma": sigma, "quote_volume": volume,
                "impact_side": impact_side(sigma, ORDER_USD, volume),
                "basket": [{"s": m, "bid": market["books"][m][0], "ask": market["books"][m][1], "qv": market["volumes"][m], "sigma": member_sigma[m]} for m in members]}

    async def _exit_record(self, client, e: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
        event, entry = e["event"], e["entry"]
        symbol = event["symbol"]
        stamp = self.clock()
        due = exit_due_day(e)
        volume = float(market["volumes"].get(symbol) or 0)
        coin: Dict[str, Any] = {"quote_volume": volume, "impact_side": impact_side(float(entry["sigma"]), ORDER_USD, volume)}
        if "stop" in e:
            reason = "stop"
            coin.update(exit_price=float(e["stop"]["fill"]), exit_ms=int(e["stop"]["exit_ms"]))
        elif symbol not in market["live"]:
            reason = "delisted"
            bars = await get_json(client, "klines", {"symbol": symbol, "interval": "1h", "limit": 2})
            if not bars:
                raise Unavailable(f"no last price for {symbol}")
            coin.update(exit_price=float(bars[-1][4]), exit_ms=ms(stamp))
        else:
            reason = "revised" if "revision" in e else "time"
            book = book_record(await get_json(client, "depth", {"symbol": symbol, "limit": 100}))
            coin.update(exit_price=book["best_ask"], exit_ms=ms(self.clock()), book=book)
        basket = []
        for member in entry["basket"]:
            quote = market["books"].get(member["s"]) if member["s"] in market["live"] else None
            later: Dict[str, Any] = {"s": member["s"], "qv": market["volumes"].get(member["s"])}
            if quote and min(quote) > 0:
                later.update(bid=quote[0], ask=quote[1])
            else:
                bars = await get_json(client, "klines", {"symbol": member["s"], "interval": "1h", "limit": 2})
                later.update(bid=float(bars[-1][4]) if bars else None, last_close=True)
            basket.append(later)
        return {"source_id": event["source_id"], "captured_at": stamp.isoformat(), "reason": reason, "due_day": due,
                "late_sec": max(0, round((stamp - capture_last(due)).total_seconds(), 1)), "coin": coin, "basket_ms": ms(stamp), "basket": basket}

    def _record_exit(self, e: Dict[str, Any], record: Dict[str, Any]) -> None:
        self.journal.append("exit", record)
        e["exit"] = record

    def _void_missed(self, st: Dict[str, Any], report: Dict[str, Any]) -> None:
        now = self.clock()
        for e in st["events"].values():
            if is_waiting(e) and now > capture_last(e["event"]["entry_day"]):
                self._void(st, e, "entry_missed", report)

    async def _settle(self, client, st: Dict[str, Any], report: Dict[str, Any]) -> None:
        now = self.clock()
        for e in st["events"].values():
            if not is_exited(e) or ms(now) < int(e["exit"]["basket_ms"]) + SETTLE_DELAY_MS:
                continue
            try:
                outcome = await self._outcome(client, e, now)
            except Exception as exc:  # noqa: BLE001 - retried next pass
                stale = now > utc(e["exit"]["captured_at"]) + RETRY_DAYS * DAY
                report["errors"].append(f"settle:{e['event']['symbol']}:{type(exc).__name__}")
                if not stale:
                    continue
                outcome = {"source_id": e["event"]["source_id"], "void": "unresolved", "error": type(exc).__name__}
            self.journal.append("outcome", outcome)
            e["outcome"] = outcome
            report["settled"] += 1

    async def _outcome(self, client, e: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        event, entry, leaving = e["event"], e["entry"], e["exit"]
        start, now_ms = int(entry["entry_ms"]), ms(now)
        coin_rows = await funding_rows(client, event["symbol"], start, now_ms)
        patient = now <= utc(leaving["captured_at"]) + RETRY_DAYS * DAY
        member_rows: Dict[str, List[Dict[str, Any]]] = {}
        semaphore = asyncio.Semaphore(6)

        async def one(member: str) -> None:
            async with semaphore:
                try:
                    member_rows[member] = await funding_rows(client, member, start, now_ms)
                except Exception:  # noqa: BLE001
                    if patient:
                        raise

        await asyncio.gather(*(one(m["s"]) for m in entry["basket"]))
        numbers = settle_numbers(entry, leaving, e.get("stop"), coin_rows, member_rows)
        if numbers["funding_gap"] and patient:
            raise Unavailable(f"funding settlements have a gap for {event['symbol']}")

        async def close_of(day: str) -> Optional[float]:
            opened = ms(day_close(day)) - DAY_MS
            try:
                rows = await get_json(client, "klines", {"symbol": event["symbol"], "interval": "1d", "startTime": opened, "limit": 1})
            except Unavailable:
                if patient:
                    raise
                return None  # the gap to the close is a measurement; losing it must not void the trade
            return float(rows[0][4]) if rows and int(rows[0][0]) == opened and opened + DAY_MS <= now_ms else None

        close_in, close_out = await close_of(event["entry_day"]), await close_of(event["exit_day"])
        mid_in, mid_out = entry["book"]["mid"], (leaving["coin"].get("book") or {}).get("mid")
        return {"source_id": event["source_id"], "reason": leaving["reason"], "revised": "revision" in e, "late_exit_sec": leaving["late_sec"], **numbers,
                "entry_close_price": close_in, "exit_close_price": close_out,
                "entry_gap_pct": round((entry["entry_price"] / close_in - 1) * 100, 4) if close_in else None,
                "exit_gap_pct": round((close_out / leaving["coin"]["exit_price"] - 1) * 100, 4) if close_out and leaving["reason"] == "time" else None,
                "coin_mid_price_pct": round((mid_in - mid_out) / mid_in * 100, 4) if mid_out else None}

    async def _paths(self, client, st: Dict[str, Any], report: Dict[str, Any]) -> None:
        """One-minute bars from 30 minutes before the close to 2 hours after it: what the first hour looks like. Best effort."""
        now = self.clock()
        for e in st["events"].values():
            if "entry" not in e or "path" in e:
                continue
            close_ms = int(e["entry"]["entry_close_ms"])
            if ms(now) < close_ms + PATH_AFTER_MS or now > utc(e["entry"]["captured_at"]) + RETRY_DAYS * DAY:
                continue
            start = close_ms - PATH_BEFORE_MS
            bars = await get_json(client, "klines", {"symbol": e["event"]["symbol"], "interval": "1m", "startTime": start, "limit": PATH_BARS})
            rows = [[int(b[0]), float(b[1]), float(b[2]), float(b[3]), float(b[4]), float(b[7])] for b in bars]
            raw = dumps(rows)
            name = re.sub(r"[^A-Za-z0-9_.-]", "_", e["event"]["source_id"]) + ".json"
            atomic_write(self.output / "paths" / name, raw)
            record = {"source_id": e["event"]["source_id"], "file": name, "sha256": sha(raw), "bars": len(rows)}
            self.journal.append("path", record)
            e["path"] = record

    def _alerts(self, st: Dict[str, Any]) -> None:
        """Warnings for a person to look at. The ledger keeps recording: stopping would only lose the sample."""
        trades = settled_trades(list(st["events"].values()))
        worst = realized_drawdown_pct(trades)
        if worst <= ALERT_DRAWDOWN_PCT and "drawdown" not in st["alerts"]:
            data = {"key": "drawdown", "drawdown_pct": worst}
            self.journal.append("safety_alert", data)
            st["alerts"]["drawdown"] = data
        for e in trades:
            key = f"trade:{e['event']['source_id']}"
            if float(e["outcome"]["hedged_pct"]) <= ALERT_TRADE_PCT and key not in st["alerts"]:
                data = {"key": key, "hedged_pct": e["outcome"]["hedged_pct"]}
                self.journal.append("safety_alert", data)
                st["alerts"][key] = data

    async def _review(self, st: Dict[str, Any], report: Dict[str, Any]) -> None:
        if st["review"] is not None:
            return
        events, now = list(st["events"].values()), self.clock()
        due, _ = review_scope(events, st["activation"]["at"], now)
        if not due:
            return
        days = sorted({e["event"]["entry_day"] for e in events if "entry" in e})
        try:
            moves = await asyncio.to_thread(prior_index_moves, days, self.panel)
        except Exception as exc:  # noqa: BLE001 - without the panel the rise stays uncovered
            moves = None
            report["errors"].append(f"review_panel:{type(exc).__name__}")
        result = evaluate(events, st["activation"]["at"], now, moves)
        self.journal.append("review", result)
        st["review"] = result
        report["reviewed"] = True


# ---------------------------------------------------------------------------------------------- summary
def _stream(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    done = settled_trades(events)
    values = [float(e["outcome"]["hedged_pct"]) for e in done]
    gaps = [float(e["outcome"]["entry_gap_pct"]) for e in done if e["outcome"].get("entry_gap_pct") is not None]
    return {"events": len(events), "trades": len(values), "trade_mean_pct": _mean(values), "worst_trade_pct": round(min(values), 3) if values else None,
            "stops": sum(e["outcome"]["reason"] == "stop" for e in done), "revised": sum("revision" in e for e in events),
            "entry_gap_mean_pct": _mean(gaps)}


def summary(output: Path = OUTPUT, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or now_utc()
    try:
        st = state(Journal(output))
    except Exception as exc:  # noqa: BLE001
        return {"status": "journal_broken", "error": str(exc)[:200]}
    if not st["activation"]:
        return {"status": "not_activated"}
    events = list(st["events"].values())
    done = settled_trades(events)
    rows = batches(events)
    voids: Dict[str, int] = {}
    for e in events:
        if "void" in e:
            voids[e["void"]["reason"]] = voids.get(e["void"]["reason"], 0) + 1
    costs = {k: _mean([float(e["outcome"][k]) for e in done]) for k in ("funding_pct", "impact_coin_pct", "basket_impact_pct", "basket_net_pct")}
    entered = [e["entry"] for e in events if "entry" in e]
    spreads = [float(x["book"]["spread_bps"]) for x in entered]
    depth = [float(x["book"]["bid_usdt_1pct"]) for x in entered]
    walked = [(float(x["book"]["best_bid"]) - float(x["book"]["vwap_sell"])) / float(x["book"]["best_bid"]) * 1e4 for x in entered]
    modelled = [float(x["impact_side"]) * 1e4 for x in entered]
    values = [r[1] for r in rows]
    return {"status": "drift_stopped" if st["drift"] else "running", "activated_at": st["activation"]["at"], "events": len(events),
            "waiting": sum(is_waiting(e) for e in events), "open": sum(is_open(e) for e in events), "settling": sum(is_exited(e) for e in events),
            "settled": len(done), "voids": voids, "unresolved": sum((e.get("outcome") or {}).get("void") is not None for e in events),
            "batches": len(rows), "batch_mean_pct": _mean(values), "capped_batch_mean_pct": _mean([r[2] for r in rows]),
            "positive_batches": sum(v > 0 for v in values), "trade_mean_pct": _mean([float(e["outcome"]["hedged_pct"]) for e in done]),
            "streams": {s: _stream([e for e in events if e["event"]["stream"] == s]) for s in ("U", "K")}, "costs": costs,
            "median_spread_bps": round(float(np.median(spreads)), 2) if spreads else None, "median_bid_usdt_1pct": round(float(np.median(depth))) if depth else None,
            "median_walked_book_bps": round(float(np.median(walked)), 2) if walked else None, "median_modelled_impact_bps": round(float(np.median(modelled)), 2) if modelled else None,
            "realized_drawdown_pct": realized_drawdown_pct(done), "alerts": list(st["alerts"].values()), "last_snapshot_day": st["snapshot_day"],
            "evaluation": st["review"] or evaluate(events, st["activation"]["at"], now), "drift": st["drift"]}


VERDICT_LABELS = {"collecting": "积累样本", "insufficient": "到期样本不足", "rejected": "未通过", "partly_rejected": "部分规则判废", "inconclusive": "未定",
                  "supported": "通过", "supported_uptrend_uncovered": "通过，但未覆盖上涨行情"}


def registry_summary() -> Dict[str, Any]:
    """Shape expected by core.research.tracker_registry. Main number: mean hedged return per complete batch."""
    s = summary()
    runner = json.loads(STATE_PATH.read_text(encoding="utf-8")) if STATE_PATH.exists() else {}
    fresh = bool(runner.get("at")) and 0 <= (now_utc() - utc(runner["at"])).total_seconds() <= HEARTBEAT_MAX_AGE_SECONDS
    if s["status"] in ("not_activated", "journal_broken"):
        label = {"not_activated": "未激活", "journal_broken": "日志校验失败"}[s["status"]]
        return {"retirement": {"n": 0, "min_n": REVIEW_BATCHES, "mean_pct": None, "ci90_pct": None, "verdict": s["status"], "label": label,
                               "reason": None, "clusters": None},
                "tracker_status_label": label, "paper_only": True, "runner_active": fresh}
    ev = s["evaluation"]
    status = "规则文件已变更 · 停账" if s["status"] == "drift_stopped" else (
        "运行异常" if runner.get("outcome") not in (None, "ok", "busy", "degraded") else "运行心跳待恢复" if not fresh
        else "运行中 · 有告警待看" if s["alerts"] else "运行中 · 有持仓" if s["open"] or s["settling"] else "运行中 · 等待下一笔入场")
    u, k = s["streams"]["U"], s["streams"]["K"]

    def shown(stream: Dict[str, Any]) -> str:
        return f"{stream['trades']} 笔，均值 " + ("—" if stream["trade_mean_pct"] is None else f"{stream['trade_mean_pct']:+.2f}%")

    return {
        "retirement": {"n": s["batches"], "min_n": REVIEW_BATCHES, "mean_pct": s["batch_mean_pct"], "ci90_pct": None, "verdict": ev["verdict"],
                       "label": VERDICT_LABELS[ev["verdict"]],
                       "reason": f"≥10% 解锁：{shown(u)}；韩元上币：{shown(k)}。复核前只作描述；满 30 个完整批次或 12 个月判一次："
                                 "均值和封顶版均值都要高于 +2%，单侧检验 p≤0.05。",
                       "clusters": None},
        "started_at": s["activated_at"], "updated_at": runner.get("at") or s["activated_at"], "runner_active": fresh, "continuous_runner": fresh,
        "paper_only": True, "tracker_status_label": status,
        "forward_win_rate": round(s["positive_batches"] / s["batches"], 3) if s["batches"] else None,
        "forward_open": s["open"], "forward_waiting": s["waiting"], "forward_late": s["voids"].get("late", 0),
        "streams": s["streams"], "alerts": s["alerts"], "execution_authorized": False,
        "last_error": ",".join(runner.get("errors") or [])[:200] or None}


# -------------------------------------------------------------------------------------------- web job
def _default_client():
    from core.research.exchange_research_runner import _client

    return _client()


async def worker(stop_event: asyncio.Event, heartbeat: Optional[Callable[[bool], None]] = None,
                 client_factory: Optional[Callable[[], Any]] = None, output: Path = OUTPUT, state_path: Path = STATE_PATH) -> None:
    """Idles until activation; then a pass every 15 minutes and every 20 seconds inside the capture window. Paper ledger only."""
    while not stop_event.is_set():
        if (Path(output) / "journal.jsonl").exists():
            try:
                async with (client_factory or _default_client)() as client:
                    result = await Ledger(output).cycle(client)
                ok = result.get("outcome") in ("ok", "busy", "degraded")
            except Exception as exc:  # noqa: BLE001 - retried next pass
                result, ok = {"outcome": "failed", "error_type": type(exc).__name__}, False
        else:
            result, ok = {"outcome": "not_activated"}, True
        await asyncio.to_thread(atomic_write, state_path, json.dumps({"at": now_utc().isoformat(), **result}, default=str).encode("utf-8"))
        if heartbeat:
            heartbeat(ok)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_seconds(now_utc()))
        except asyncio.TimeoutError:
            pass
