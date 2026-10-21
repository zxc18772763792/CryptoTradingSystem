"""Prospective event-short risk experiment. Paper observations, never orders.

Source dictionaries contain source_id, symbol, source_entry_ms,
source_exit_due_ms, discovered_at, source_status, eligible, and optionally
hedge_symbols (a frozen, equal-notional long basket). Percentages use one
short-leg entry notional; a hedged position therefore has gross exposure 2.
The baseline and its retirement evidence are never changed.
"""
from __future__ import annotations

import copy
import asyncio
import math
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional

FAPI = "https://fapi.binance.com/fapi/v1"
STATE_KEY = "event_risk_shadow"
INVALID_SOURCE_STATUSES = {"schedule_changed", "cancelled", "invalidated", "entry_failed", "no_perp", "delisted"}
CONFIG = {
    "version": "event-risk-v1",
    "funding_budget_pct": 2.0,
    "total_loss_budget_pct": 10.0,
    "fee_per_fill": 0.0005,
    "adverse_fill_slippage": 0.001,
    "entry_window_sec": 900,
    "max_quote_age_sec": 30,
    "denominator": "one_short_leg_entry_notional",
    "basis": "Round prospective risk budgets, not fitted to past trades: funding may consume at most one fifth of the 10% loss budget.",
    "funding_projection": "Adverse latest observed rate held constant until scheduled exit; a scenario, not a forecast or bound.",
    "monitoring": "Each scheduler observation; gaps are unobserved and exits execute only at newly observed quotes.",
}


class Unavailable(ValueError):
    """Missing or stale evidence: do not replace it with a zero."""


def _ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


def _iso(at: datetime) -> str:
    return at.astimezone(timezone.utc).isoformat()


def _number(value: Any, name: str, *, positive: bool = False) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise Unavailable(f"missing {name}") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        raise Unavailable(f"invalid {name}")
    return result


def register(parent_state: Dict[str, Any], now: datetime,
             existing_source_ids: Iterable[str] = ()) -> Dict[str, Any]:
    """Freeze once. Call and persist this before taking any prospective entries."""
    if STATE_KEY not in parent_state:
        parent_state[STATE_KEY] = {
            "registered_at": _iso(now), "config": copy.deepcopy(CONFIG),
            "excluded_source_ids": sorted(set(existing_source_ids)), "trades": {},
        }
    return parent_state[STATE_KEY]


async def _json(client, endpoint: str, params: Optional[Dict[str, Any]] = None):
    response = await client.get(f"{FAPI}/{endpoint}", params=params or {})
    if response.status_code != 200:
        raise Unavailable(f"{endpoint}: HTTP {response.status_code}")
    return response.json()


async def _quote(client, symbol: str, config: Dict[str, Any], clock: Callable[[], datetime]) -> Dict[str, Any]:
    requested = clock()
    payload = await _json(client, "ticker/bookTicker", {"symbol": symbol})
    received = clock()
    if not isinstance(payload, dict) or payload.get("symbol") != symbol:
        raise Unavailable(f"invalid quote for {symbol}")
    bid = _number(payload.get("bidPrice"), "bidPrice", positive=True)
    ask = _number(payload.get("askPrice"), "askPrice", positive=True)
    stamp = _number(payload.get("time"), "quote time", positive=True)
    age = (_ms(received) - stamp) / 1000
    if ask < bid or not -5 <= age <= float(config["max_quote_age_sec"]):
        raise Unavailable(f"stale/crossed quote for {symbol}")
    return {"symbol": symbol, "bid": bid, "ask": ask, "exchange_time_ms": int(stamp),
            "requested_at": _iso(requested), "received_at": _iso(received),
            "observed_ms": _ms(received)}


def _fill(quote: Dict[str, Any], side: int, entering: bool, config: Dict[str, Any]) -> float:
    buying = side == 1 if entering else side == -1
    return float(quote["ask"] if buying else quote["bid"]) * (
        1 + float(config["adverse_fill_slippage"]) * (1 if buying else -1))


async def _funding_reference(client, symbol: str, due_ms: int,
                             clock: Callable[[], datetime], cache: Dict[str, Any]) -> Dict[str, Any]:
    payload = await _json(client, "premiumIndex", {"symbol": symbol})
    now = clock()
    if not isinstance(payload, dict):
        raise Unavailable(f"missing premium index for {symbol}")
    stamp = _number(payload.get("time"), "premium time", positive=True)
    if not -5_000 <= _ms(now) - stamp <= 60_000:
        raise Unavailable(f"stale funding reference for {symbol}")
    rate = _number(payload.get("lastFundingRate"), "lastFundingRate")
    next_ms = int(_number(payload.get("nextFundingTime"), "nextFundingTime", positive=True))
    if next_ms <= _ms(now):
        raise Unavailable(f"expired funding schedule for {symbol}")
    if "funding_info" not in cache:
        cache["funding_info"] = asyncio.create_task(_json(client, "fundingInfo"))
    info = await cache["funding_info"]
    row = next((r for r in info if r.get("symbol") == symbol), {}) if isinstance(info, list) else {}
    interval_hours = row.get("fundingIntervalHours")
    if interval_hours is not None:
        interval = int(_number(interval_hours, "fundingIntervalHours", positive=True) * 3_600_000)
        origin = "fundingInfo.fundingIntervalHours"
    else:
        # fundingInfo lists adjusted contracts only. The next scheduled timestamp
        # minus the latest actual settlement supplies this contract's current
        # interval without presuming an eight-hour default.
        history = await _json(client, "fundingRate", {"symbol": symbol, "endTime": _ms(now), "limit": 2})
        if not isinstance(history, list) or len(history) < 2:
            raise Unavailable(f"no settlement timestamp/interval for {symbol}")
        times = sorted({int(_number(r.get("fundingTime"), "fundingTime", positive=True)) for r in history})
        if len(times) < 2:
            raise Unavailable(f"insufficient settlement timestamps for {symbol}")
        last_ms = times[-1]
        interval = next_ms - last_ms
        if interval != times[-1] - times[-2]:
            raise Unavailable(f"funding cadence changed or latest settlement unpublished for {symbol}")
        origin = "nextFundingTime-minus-latest-settlement"
    if not 0 < interval <= 86_400_000:
        raise Unavailable(f"invalid funding interval for {symbol}")
    payments = max(0, (due_ms - next_ms) // interval + 1)
    return {"rate": rate, "next_funding_ms": next_ms, "interval_ms": interval,
            "interval_source": origin, "projected_payments": int(payments),
            "observed_at": _iso(now), "source_time_ms": int(stamp)}


async def _funding_cash(client, leg: Dict[str, Any], end_ms: int, clock, cache) -> float:
    """Settled cash per leg entry notional, including settlement mark-price scaling."""
    start = int(leg["entry_ms"])
    cursor, rows = start + 1, []
    while cursor <= end_ms:
        page = await _json(client, "fundingRate", {"symbol": leg["symbol"], "startTime": cursor,
                                                  "endTime": end_ms, "limit": 1000})
        if not isinstance(page, list):
            raise Unavailable(f"missing settlements for {leg['symbol']}")
        rows.extend(page)
        if len(page) < 1000:
            break
        latest = max(int(r["fundingTime"]) for r in page)
        if latest < cursor:
            raise Unavailable("funding pagination did not advance")
        cursor = latest + 1
    relevant = {int(r["fundingTime"]): r for r in rows if start < int(r["fundingTime"]) <= end_ms}
    if not relevant and end_ms >= int(leg["funding_reference"]["next_funding_ms"]):
        raise Unavailable(f"expected settlements absent for {leg['symbol']}")
    key = ("coverage_schedule", leg["symbol"])
    if key not in cache:
        cache[key] = asyncio.create_task(_funding_reference(client, leg["symbol"], end_ms, clock, cache))
    current = await cache[key]
    interval, next_ms = int(current["interval_ms"]), int(current["next_funding_ms"])
    expected_tail = next_ms - max(1, math.ceil((next_ms - end_ms) / interval)) * interval
    leg["funding_coverage"] = {"cash_through_ms": end_ms, "latest_settlement_ms": max(relevant, default=None),
                               "expected_tail_ms": expected_tail, "current_schedule": current}
    if start < expected_tail <= end_ms and expected_tail not in relevant:
        raise Unavailable(f"latest expected settlement unpublished for {leg['symbol']}")
    entry_schedule = leg["funding_reference"]
    if interval == int(entry_schedule["interval_ms"]):
        expected = range(int(entry_schedule["next_funding_ms"]), end_ms + 1, interval)
        if any(start < stamp and stamp not in relevant for stamp in expected):
            raise Unavailable(f"settlement coverage has gaps for {leg['symbol']}")
    cash = 0.0
    for row in relevant.values():
        rate = _number(row.get("fundingRate"), "settled fundingRate")
        mark = _number(row.get("markPrice"), "settlement markPrice", positive=True)
        cash += -int(leg["side"]) * rate * mark / float(leg["entry_price"])
    return cash


async def _bounded(items, operation):
    """Keep a 30-name hedge from serially aging its first quote."""
    semaphore = asyncio.Semaphore(4)

    async def one(item):
        async with semaphore:
            return await operation(item)

    results = await asyncio.gather(*(one(item) for item in items), return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result
    return results


def _net(legs, exits, funding, config):
    return _breakdown(legs, exits, funding, config)["net_pct"]


def _breakdown(legs, exits, funding, config):
    total, price_total, funding_total, fees, short, hedge = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    components = []
    for leg, price, cash in zip(legs, exits, funding):
        ratio = float(price) / float(leg["entry_price"])
        fee = float(config["fee_per_fill"]) * (1 + ratio)
        weight = float(leg["weight"])
        price_pnl = int(leg["side"]) * (ratio - 1)
        net = weight * (price_pnl + cash - fee)
        total += net
        price_total += weight * price_pnl
        funding_total += weight * cash
        fees += weight * fee
        if int(leg["side"]) == -1:
            short += net
        else:
            hedge += net
        components.append({"symbol": leg["symbol"], "side": leg["side"], "weight": weight,
                           "entry_price": leg["entry_price"], "exit_or_mark_fill": price,
                           "price_pnl_pct": price_pnl * weight * 100,
                           "settled_funding_pct": cash * weight * 100, "fees_pct": fee * weight * 100,
                           "net_pct": net * 100})
    return {"net_pct": total * 100, "price_pnl_pct": price_total * 100,
            "funding_pct": funding_total * 100, "fees_pct": fees * 100,
            "short_net_pct": short * 100, "hedge_net_pct": hedge * 100,
            "components": components, "price_pnl_includes_adverse_fills": True}


async def _enter(client, record, source, config, clock, cache):
    symbols = [str(source["symbol"])]
    basket = sorted(set(source.get("hedge_symbols") or []) - set(symbols))
    if source.get("hedged") and not basket:
        raise Unavailable("no frozen hedge basket")
    plan = [(symbols[0], -1, 1.0)] + [(s, 1, 1 / len(basket)) for s in basket]
    references, projected = {}, 0.0
    for symbol, side, weight in plan:
        ref = await _funding_reference(client, symbol, int(source["source_exit_due_ms"]), clock, cache)
        references[symbol] = ref
        projected += weight * max(0.0, side * ref["rate"]) * ref["projected_payments"] * 100
    record.update(funding_gate={"projected_adverse_pct": projected, "references": references,
                               "assessed_at": _iso(clock())})
    if projected > float(config["funding_budget_pct"]):
        record.update(status="funding_rejected", decision_at=_iso(clock()))
        return
    quotes = await _bounded([symbol for symbol, _, _ in plan], lambda symbol: _quote(client, symbol, config, clock))
    decision = clock()
    if _ms(decision) > int(source["source_entry_ms"]) + int(config["entry_window_sec"]) * 1000:
        record.update(status="missed", reason="entry evidence completed after entry window", decision_at=_iso(decision))
        return
    for quote in quotes:
        if _ms(decision) - quote["observed_ms"] > float(config["max_quote_age_sec"]) * 1000:
            raise Unavailable("entry basket quote sequence too slow")
    if any(ref["next_funding_ms"] <= _ms(decision) for ref in references.values()):
        raise Unavailable("funding boundary crossed before entry")
    record.update(status="open", entry_decision_at=_iso(decision), entry_ms=_ms(decision),
                  entry_delay_sec=(_ms(decision) - int(source["source_entry_ms"])) / 1000,
                  legs=[{"symbol": symbol, "side": side, "weight": weight,
                         "entry_price": _fill(q, side, True, config), "entry_quote": q,
                         "entry_ms": _ms(decision), "funding_reference": references[symbol]}
                        for (symbol, side, weight), q in zip(plan, quotes)])


async def _finish(client, record, config, clock, cache):
    legs = record["legs"]
    funding = await _bounded(legs, lambda leg: _funding_cash(client, leg, int(leg["exit_ms"]), clock, cache))
    breakdown = _breakdown(legs, [l["exit_price"] for l in legs], funding, config)
    record.update(status="closed", net_pct=round(breakdown["net_pct"], 6), exit_breakdown=breakdown,
                  funding_pct=round(sum(l["weight"] * f for l, f in zip(legs, funding)) * 100, 6),
                  finalized_at=_iso(clock()), monitoring_status="available")
    for leg, cash in zip(legs, funding):
        leg["settled_funding_pct"] = cash * 100


async def _monitor(client, record, config, clock, cache):
    if record["status"] == "closing_pending_funding":
        await _finish(client, record, config, clock, cache)
        return
    legs = record["legs"]
    observed = clock()
    due = _ms(observed) >= int(record["source_exit_due_ms"])
    invalidated = bool(record.get("source_invalidated_at"))
    funding_error = None
    try:
        funding = await _bounded(legs, lambda leg: _funding_cash(client, leg, _ms(observed), clock, cache))
        quotes = await _bounded(legs, lambda leg: _quote(client, leg["symbol"], config, clock))
        prices = [_fill(q, l["side"], False, config) for l, q in zip(legs, quotes)]
        decision = clock()
        # If a settlement boundary passed while reading evidence, retry with a
        # new cutoff rather than treating its possibly unpublished cash as zero.
        if any(_ms(observed) < l["funding_coverage"]["current_schedule"]["next_funding_ms"] <= _ms(decision) for l in legs):
            raise Unavailable("funding boundary crossed while observing mark")
        if any(_ms(decision) - q["observed_ms"] > float(config["max_quote_age_sec"]) * 1000 for q in quotes):
            raise Unavailable("mark quotes became stale while reading settlements")
        net = _net(legs, prices, funding, config)
        record["mark"] = {**_breakdown(legs, prices, funding, config), "observed_at": _iso(decision),
                          "funding_cash_through_at": _iso(observed),
                          "evaluated_at": _iso(decision), "quotes": quotes,
                          "elapsed_sec": (_ms(observed) - int(record["entry_ms"])) / 1000,
                          "includes": "both legs, settled funding, entry/exit fees, adverse paper fills"}
        risk_exit = net <= -float(config["total_loss_budget_pct"])
    except Unavailable as exc:
        funding_error, risk_exit = str(exc), False
        record["monitoring_status"], record["unavailable_reason"] = "unavailable", funding_error
    if not due and not risk_exit and not invalidated:
        if funding_error is None:
            record["monitoring_status"] = "available"
        return
    record.update(exit_reason="source_invalidated" if invalidated else "total_loss_budget" if risk_exit else "scheduled_end",
                  exit_detected_at=_iso(clock()), exit_detection_net_pct=record.get("mark", {}).get("net_pct") if not funding_error else None)
    # Fresh execution quotes after detection. We never claim an earlier crossing
    # or a historical due-time price, even following an outage or a gap.
    execution = await _bounded(legs, lambda leg: _quote(client, leg["symbol"], config, clock))
    executed = clock()
    if any(_ms(executed) - q["observed_ms"] > float(config["max_quote_age_sec"]) * 1000 for q in execution):
        raise Unavailable("exit basket quote sequence too slow")
    for leg, quote in zip(legs, execution):
        leg.update(exit_quote=quote, exit_price=_fill(quote, leg["side"], False, config), exit_ms=_ms(executed))
    record.update(status="closing_pending_funding", exit_executed_at=_iso(executed),
                  exit_delay_sec=(_ms(executed) - int(record["source_exit_due_ms"])) / 1000 if due else None)
    await _finish(client, record, config, clock, cache)


async def apply_to_tracker(client, parent_state: Dict[str, Any], sources: Iterable[Dict[str, Any]],
                           now: datetime, *, clock: Optional[Callable[[], datetime]] = None) -> Dict[str, Any]:
    """Best-effort lifecycle; caller persists parent_state even on baseline failures.

    Register/persist before calling with new entries when the baseline supports
    that hook. A first call after old entries excludes them. Future waiting
    signals are eligible. Unavailable entry decisions are terminal (no later
    rate substituted); unavailable monitoring is retried at the next tick.
    """
    sources = list(sources)
    entered = [s["source_id"] for s in sources if int(s["source_entry_ms"]) <= _ms(now)
               or s.get("source_status") in {"open", "closed", "stopped", "delisted"}]
    shadow = register(parent_state, now, entered)
    clock = clock or (lambda: datetime.now(timezone.utc))
    config, records = shadow["config"], shadow["trades"]
    entry_cache: Dict[str, Any] = {}
    seen = set()
    registered = _ms(datetime.fromisoformat(shadow["registered_at"]))
    for source in sources:
        key = str(source["source_id"])
        seen.add(key)
        record = records.get(key)
        if record and record["status"] in {"open", "closing_pending_funding"}:
            if source.get("source_status") in INVALID_SOURCE_STATUSES:
                record.setdefault("source_invalidated_at", _iso(clock()))
                record["source_invalidated_status"] = source["source_status"]
            try:
                await _monitor(client, record, config, clock, entry_cache)
            except Exception as exc:  # noqa: BLE001 - retry only ongoing positions
                record.update(monitoring_status="unavailable", unavailable_at=_iso(clock()),
                              unavailable_reason=f"{type(exc).__name__}: {str(exc)[:180]}")
            continue
        if not source.get("eligible") or key in shadow["excluded_source_ids"]:
            continue
        if int(source["source_entry_ms"]) < registered:
            continue
        if key not in records and _ms(clock()) >= int(source["source_entry_ms"]):
            record = {"source_id": key, "symbol": source["symbol"],
                      "source_entry_ms": int(source["source_entry_ms"]),
                      "source_exit_due_ms": int(source["source_exit_due_ms"]),
                      "source_discovered_at": source.get("discovered_at"), "decision_at": _iso(clock()),
                      "config_version": config["version"], "status": "entry_unavailable"}
            records[key] = record
            if source.get("source_status") in INVALID_SOURCE_STATUSES:
                record.update(status="source_invalidated", reason=f"source status: {source['source_status']}")
            elif _ms(clock()) > int(source["source_entry_ms"]) + int(config["entry_window_sec"]) * 1000:
                record.update(status="missed", reason="no observation within entry window")
            else:
                try:
                    await _enter(client, record, source, config, clock, entry_cache)
                except Exception as exc:  # noqa: BLE001 - baseline must continue
                    record["unavailable_reason"] = f"{type(exc).__name__}: {str(exc)[:180]}"
        record = records.get(key)
        if record and record["status"] in {"open", "closing_pending_funding"}:
            try:
                await _monitor(client, record, config, clock, entry_cache)
            except Exception as exc:  # noqa: BLE001 - retry only ongoing positions
                record.update(monitoring_status="unavailable", unavailable_at=_iso(clock()),
                              unavailable_reason=f"{type(exc).__name__}: {str(exc)[:180]}")
    # Source collection can fail or a baseline can remove an old row. An
    # already entered shadow owns frozen legs and a due time independently.
    for key, record in records.items():
        if key not in seen and record["status"] in {"open", "closing_pending_funding"}:
            try:
                await _monitor(client, record, config, clock, entry_cache)
            except Exception as exc:  # noqa: BLE001 - retry at the next observed tick
                record.update(monitoring_status="unavailable", unavailable_at=_iso(clock()),
                              unavailable_reason=f"{type(exc).__name__}: {str(exc)[:180]}")
    shadow["updated_at"] = _iso(clock())
    return summary(shadow)


def summary(shadow: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Read-only separate evidence; never a baseline retirement verdict."""
    if not shadow:
        return {"registered": False, "completed": 0, "open": 0}
    rows = list(shadow.get("trades", {}).values())
    closed = [r for r in rows if r.get("status") == "closed"]
    return {"registered": True, "registered_at": shadow.get("registered_at"),
            "config": copy.deepcopy(shadow.get("config")), "updated_at": shadow.get("updated_at"),
            "completed": len(closed), "open": sum(r.get("status") == "open" for r in rows),
            "closing_pending_funding": sum(r.get("status") == "closing_pending_funding" for r in rows),
            "funding_rejected": sum(r.get("status") == "funding_rejected" for r in rows),
            "entry_unavailable": sum(r.get("status") == "entry_unavailable" for r in rows),
            "missed": sum(r.get("status") == "missed" for r in rows),
            "source_invalidated": sum(r.get("status") == "source_invalidated" for r in rows),
            "monitoring_unavailable": sum(r.get("monitoring_status") == "unavailable" for r in rows),
            "mean_net_pct": round(sum(r["net_pct"] for r in closed) / len(closed), 6) if closed else None,
            "paired_source_ids": [r["source_id"] for r in closed],
            "positions": [{k: copy.deepcopy(r.get(k)) for k in (
                "source_id", "symbol", "status", "source_entry_ms", "source_exit_due_ms", "entry_ms",
                "entry_delay_sec", "entry_decision_at", "mark", "exit_reason", "exit_detected_at",
                "exit_executed_at", "exit_delay_sec", "exit_detection_net_pct", "net_pct", "exit_breakdown",
                "monitoring_status", "unavailable_reason", "funding_gate", "legs") if r.get(k) is not None} for r in rows],
            "note": "Prospective risk experiment only; distinct from baseline and retirement evidence."}
