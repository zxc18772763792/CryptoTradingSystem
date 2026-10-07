"""Paper tracker: short Binance perps minutes after monitoring-tag / delisting notices (research only).

Why (scripts/announcement_intraday_study.py, docs/ANNOUNCEMENT_INTRADAY_2026-10-07.md):
the daily studies entered at the announcement day's close, after most of the
move. On 1-minute perp bars, shorting right after publication and holding a
few hours to a day was the profitable window:
  * monitoring tag ("Extend the Monitoring Tag to Include ..."), hold 24h,
    +50% catastrophe stop: +4.3%/trade net of 0.3% costs, 90% CI [+1.3, +7.1],
    89 trades in 24 notices, 2023-26 (no stop: +5.4%, but unbounded)
  * spot delisting ("Binance Will Delist X on <date>"), hold 4h, +30% stop:
    +5.8%/trade, 90% CI [+1.7, +9.4], 61 trades in 23 notices (no stop: worst -79%)
Both measured with a 2-minute entry delay; best of 120 delay x hold x source
cells, so the forward record is what decides.

Frozen rule:
  * poll Binance announcement catalogs 49 and 161 every POLL_SEC (own runtime
    task, not the 5-minute research scheduler)
  * on first sight of a qualifying notice, short each named coin's USDT perp
    (TRADING, listed > 1 day before the notice) at the LAST TRADE PRICE at that
    moment: the measured detection latency is part of the result
  * exit at the close of the 1m bar ending entry + hold, or at the stop
    (1m high >= entry * (1 + stop), filled 2% worse)
  * return = (entry - exit) / entry - 0.3% (0.1% fees + 0.2% assumed
    slippage, as in the backtest) + funding received over the hold
  * detected more than MAX_LAG after publication: "late", recorded, not counted
Evidence per trade: detection latency, the order book at entry with the
measured price impact of selling $10k, and a top-30 perp basket over the same
window (market control). Notices published before the tracker started are
only marked seen. Nothing here places orders.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from loguru import logger

from core.research import exchange_notices, retirement, signal_evidence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = PROJECT_ROOT / "data" / "research" / "announcement_short"
POLL_STATE_PATH = STATE_DIR / "poll.json"
FAPI = "https://fapi.binance.com/fapi/v1"
POLL_SEC = 90
CATALOGS = ("49", "161")
MIN_MS = 60_000
DAY_MS = 86_400_000
MAX_LAG_MS = 15 * MIN_MS
COST = 0.003
STOP_SLIPPAGE = 0.02
IMPACT_NOTIONAL = 10_000.0
KINDS: Dict[str, Dict[str, Any]] = {
    "binance_monitor": {"hold_min": 1440, "stop": 0.50, "rule": "announcement_short_monitor",
                        "state_path": STATE_DIR / "monitor.json",
                        "reference": "89 trades / 24 notices 2023-26: +4.3%/trade net, 90% CI [+1.3, +7.1], win 69%"},
    "binance_delist": {"hold_min": 240, "stop": 0.30, "rule": "announcement_short_delist",
                       "state_path": STATE_DIR / "delist.json",
                       "reference": "61 trades / 23 notices 2024-26: +5.8%/trade net, 90% CI [+1.7, +9.4], win 72%"},
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _load(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def _save(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


def notice_events(title: str) -> List[tuple]:
    """(kind, token) pairs a notice title opens trades for."""
    out = [("binance_monitor", t) for t in exchange_notices.parse_monitoring_changes(title)["added"]]
    notice = exchange_notices.parse_notice(title)
    if notice and notice["kind"] == "delist":
        out += [("binance_delist", t) for t in notice["tokens"]]
    return out


def sell_impact_pct(depth: Dict[str, Any], notional: float = IMPACT_NOTIONAL) -> Optional[float]:
    """Average fill below the best bid when selling `notional` USDT into the book (None if too thin)."""
    bids = [(float(p), float(q)) for p, q in depth.get("bids") or []]
    if not bids:
        return None
    left, cost_qty, value = notional, 0.0, 0.0
    for price, qty in bids:
        take = min(left, price * qty)
        value += take
        cost_qty += take / price
        left -= take
        if left <= 1e-9:
            break
    if left > 1e-9:
        return None
    return round((1 - (value / cost_qty) / bids[0][0]) * 100, 4)


async def _poll(client) -> List[Dict[str, Any]]:
    articles = []
    for cat in CATALOGS:
        resp = await client.get(exchange_notices.CMS_LIST_URL, params={"type": 1, "catalogId": int(cat), "pageNo": 1, "pageSize": 20},
                                headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        articles += [a for cc in ((resp.json().get("data") or {}).get("catalogs") or []) for a in (cc.get("articles") or [])]
    return articles


async def _close_at(client, symbol: str, end_ms: int) -> Optional[float]:
    """Close of the 1m bar ending at end_ms."""
    resp = await client.get(f"{FAPI}/klines", params={"symbol": symbol, "interval": "1m", "startTime": end_ms - MIN_MS, "limit": 1})
    rows = resp.json() if resp.status_code == 200 else []
    return float(rows[0][4]) if rows and int(rows[0][0]) == end_ms - MIN_MS else None


async def _open_trade(client, kind: str, token: str, article: Dict[str, Any], perps: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    release_ms = int(article["releaseDate"])
    lag_ms = now.timestamp() * 1000 - release_ms
    trade: Dict[str, Any] = {"kind": kind, "token": token, "notice_code": article.get("code"), "title": article.get("title"),
                             "published_at": datetime.fromtimestamp(release_ms / 1000, timezone.utc).isoformat(),
                             "detected_at": now.isoformat(), "latency_sec": round(lag_ms / 1000, 1),
                             "late": lag_ms > MAX_LAG_MS}
    symbol = next((s for s in (f"{token}USDT", f"1000{token}USDT")
                   if s in perps and perps[s] < release_ms - DAY_MS), None)
    if not symbol:
        trade["status"] = "no_perp"
        return trade
    tick = await client.get(f"{FAPI}/ticker/price", params={"symbol": symbol})
    if tick.status_code != 200:
        trade.update(status="no_price", symbol=symbol)
        return trade
    spec = KINDS[kind]
    entry_ms = int(now.timestamp() * 1000)
    trade.update(symbol=symbol, status="open", entry_price=float(tick.json()["price"]), entry_ms=entry_ms,
                 exit_due_ms=entry_ms + spec["hold_min"] * MIN_MS, stop_pct=spec["stop"])
    evidence: Dict[str, Any] = {}
    try:
        depth = await client.get(f"{FAPI}/depth", params={"symbol": symbol, "limit": 100})
        if depth.status_code == 200:
            payload = depth.json()
            evidence["book_at_entry"] = {**signal_evidence.book_metrics(payload, now),
                                         "sell_10k_impact_pct": sell_impact_pct(payload)}
        evidence["market_basket"] = await signal_evidence.market_basket(client, [s for s in perps], symbol, now)
    except Exception as exc:  # noqa: BLE001 - evidence never blocks the trade
        evidence["error"] = type(exc).__name__
    trade["evidence"] = evidence
    return trade


async def _settle(client, trade: Dict[str, Any], now: datetime) -> None:
    """Exit at the hold's end or the stop, from 1m bars; funding; basket over the same window."""
    entry, entry_ms, due_ms = float(trade["entry_price"]), int(trade["entry_ms"]), int(trade["exit_due_ms"])
    first_bar = (entry_ms // MIN_MS + 1) * MIN_MS  # bars fully after the entry moment
    resp = await client.get(f"{FAPI}/klines", params={"symbol": trade["symbol"], "interval": "1m",
                                                      "startTime": first_bar, "endTime": due_ms - 1, "limit": 1500})
    if resp.status_code == 400:
        trade.update(status="delisted", settled_at=now.isoformat())
        return
    if resp.status_code != 200:
        return
    bars = [[int(b[0]), float(b[2]), float(b[4])] for b in resp.json()]
    if not bars:
        return
    exit_px, exit_ms, status = None, None, "closed"
    for open_ms, high, _close in bars:
        if high >= entry * (1 + trade["stop_pct"]):
            exit_px, exit_ms, status = entry * (1 + trade["stop_pct"]) * (1 + STOP_SLIPPAGE), open_ms + MIN_MS, "stopped"
            break
    if exit_px is None:
        if bars[-1][0] + MIN_MS < due_ms:
            if now.timestamp() * 1000 < due_ms + 3 * DAY_MS:
                return  # bars still missing; a contract gone for good settles on its last close below
        exit_px, exit_ms = bars[-1][2], bars[-1][0] + MIN_MS
    fr = await client.get(f"{FAPI}/fundingRate", params={"symbol": trade["symbol"], "startTime": entry_ms + 1,
                                                         "endTime": exit_ms, "limit": 1000})
    rows = fr.json() if fr.status_code == 200 else None
    if not isinstance(rows, list):
        return  # never book missing funding as zero
    funding = sum(float(r["fundingRate"]) for r in rows if entry_ms < int(r["fundingTime"]) <= exit_ms)
    ret = (entry - exit_px) / entry - COST + funding
    trade.update(status=status, exit_price=exit_px, exit_ms=exit_ms, funding=round(funding, 6),
                 return_pct=round(ret * 100, 3), settled_at=now.isoformat())
    symbols = ((trade.get("evidence") or {}).get("market_basket") or {}).get("symbols") or []
    moves = []
    for s in symbols:
        try:
            a, b = await _close_at(client, s, (entry_ms // MIN_MS) * MIN_MS), await _close_at(client, s, (exit_ms // MIN_MS) * MIN_MS)
        except Exception:  # noqa: BLE001
            continue
        if a and b:
            moves.append(b / a - 1)
    if symbols and len(moves) * 2 >= len(symbols):
        basket = float(np.mean(moves)) * 100
        trade["evidence"]["market_control"] = {"basket_return_pct": round(basket, 3), "legs": len(moves),
                                               "hedged_return_pct": round(trade["return_pct"] + basket, 3)}


async def tick(client, *, state_dir: Optional[Path] = None) -> Dict[str, Any]:
    base = state_dir or STATE_DIR
    now = _now()
    poll = _load(base / "poll.json", {"started_at": now.isoformat(), "seen": {}})
    started_ms = datetime.fromisoformat(poll["started_at"]).timestamp() * 1000
    states = {kind: _load(base / KINDS[kind]["state_path"].name, {"started_at": poll["started_at"], "trades": {}})
              for kind in KINDS}
    for kind, state in states.items():
        retirement.register(state, KINDS[kind]["rule"], now.isoformat())

    articles = await _poll(client)
    fresh = [a for a in articles if a.get("code") and a["code"] not in poll["seen"]]
    perps: Dict[str, Any] = {}
    if any(int(a.get("releaseDate") or 0) >= started_ms and notice_events(a.get("title", "")) for a in fresh):
        info = await client.get(f"{FAPI}/exchangeInfo")
        info.raise_for_status()
        perps = {s["symbol"]: int(s.get("onboardDate") or 0) for s in info.json().get("symbols", [])
                 if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}
    for article in sorted(fresh, key=lambda a: int(a.get("releaseDate") or 0)):
        poll["seen"][article["code"]] = now.isoformat()
        if int(article.get("releaseDate") or 0) < started_ms:
            continue  # published before the tracker started
        for kind, token in notice_events(article.get("title", "")):
            key = f"{token}|{article['code']}"
            if key not in states[kind]["trades"]:
                states[kind]["trades"][key] = await _open_trade(client, kind, token, article, perps, now)
                logger.info(f"announcement short: {kind} {token} -> {states[kind]['trades'][key]['status']}")

    for kind, state in states.items():
        for trade in state["trades"].values():
            if trade.get("status") == "open" and now.timestamp() * 1000 >= int(trade["exit_due_ms"]) + 2 * MIN_MS:
                try:
                    await _settle(client, trade, now)
                except Exception as exc:  # noqa: BLE001 - retried next pass
                    logger.debug(f"announcement short: settle {trade.get('token')} failed: {exc}")
        state["updated_at"] = now.isoformat()
        _save(base / KINDS[kind]["state_path"].name, state)
    poll["last_poll_at"] = now.isoformat()
    poll["seen"] = dict(list(poll["seen"].items())[-2000:])
    _save(base / "poll.json", poll)
    return {kind: summary(states[kind], kind) for kind in KINDS}


def summary(state: Dict[str, Any], kind: str) -> Dict[str, Any]:
    spec = KINDS[kind]
    trades = list(state.get("trades", {}).values())
    counted = [t for t in trades if t.get("symbol") and not t.get("late")]
    done = [t for t in counted if t.get("status") in {"closed", "stopped"} and t.get("return_pct") is not None]
    returns = [float(t["return_pct"]) for t in done]
    hedged = [float(t["evidence"]["market_control"]["hedged_return_pct"]) for t in done
              if ((t.get("evidence") or {}).get("market_control") or {}).get("hedged_return_pct") is not None]
    lat = [float(t["latency_sec"]) for t in trades if t.get("latency_sec") is not None]
    impact = [float(t["evidence"]["book_at_entry"]["sell_10k_impact_pct"]) for t in trades
              if ((t.get("evidence") or {}).get("book_at_entry") or {}).get("sell_10k_impact_pct") is not None]
    return {
        "kind": kind,
        "hold_minutes": spec["hold_min"],
        "stop_pct": spec["stop"],
        "started_at": state.get("started_at"),
        "updated_at": state.get("updated_at"),
        "trades_total": len(trades),
        "no_perp": sum(t.get("status") == "no_perp" for t in trades),
        "late": sum(bool(t.get("late")) and bool(t.get("symbol")) for t in trades),
        "forward_open": sum(t.get("status") == "open" for t in counted),
        "forward_completed": len(done),
        "forward_mean_return_pct": round(float(np.mean(returns)), 2) if returns else None,
        "forward_win_rate": round(float(np.mean([r > 0 for r in returns])), 3) if returns else None,
        "forward_hedged_mean_return_pct": round(float(np.mean(hedged)), 2) if hedged else None,
        "median_latency_sec": round(float(np.median(lat)), 1) if lat else None,
        "median_sell_10k_impact_pct": round(float(np.median(impact)), 3) if impact else None,
        "assumed_slippage_pct": 0.2,
        "backtest_reference": spec["reference"],
        "retirement": retirement.verdict(returns, state.get("retirement_rule") or retirement.RULES[spec["rule"]]),
    }


def load_summaries(state_dir: Optional[Path] = None) -> Dict[str, Any]:
    base = state_dir or STATE_DIR
    out: Dict[str, Any] = {}
    for kind, spec in KINDS.items():
        state = _load(base / spec["state_path"].name, {"trades": {}})
        trades = sorted(state.get("trades", {}).values(), key=lambda t: t.get("detected_at") or "", reverse=True)
        out[kind] = {"summary": summary(state, kind), "trades": trades[:40]}
    poll = _load(base / "poll.json", {})
    out["poll"] = {"started_at": poll.get("started_at"), "last_poll_at": poll.get("last_poll_at"), "seen": len(poll.get("seen") or {})}
    return out
