"""Paper tracker: daily cross-sectional reversal on 1h bars (research only).

Why (scripts/ml_timeframe_study.py, docs/AGENT_ML_BIAS_2026-09-30.md
follow-up): across 5m..1d bars, pooled ML found no edge after costs except
1h bars ranked for a 24h hold, where one feature matched the model: coins
trading below their slow EMA outperformed the cross-section the next day.
It was the best of 9 timeframe/horizon cells, so it needs forward evidence.

Frozen rule (scripts/xs_reversal_backtest.py backtests exactly this):
  * universe frozen at the first pass from the backtest's universe file,
    restricted at each formation to perps whose status is TRADING
  * formation at 00:00 UTC from 1h perp klines closed by then: signal =
    EMA(21)/last close - 1; long the decile furthest below its EMA, short the
    decile furthest above; equal weight; >= MIN_COINS coins, decile = n // 10
  * entry = close of the 23:00 bar, exit = close of the next 23:00 bar
  * net per position = (long leg - short leg)/2 - 0.15%, funding paid by
    longs / received by shorts for settlements in (entry, exit]

A formation made more than FORMATION_WINDOW after 00:00 is ``late`` (the
00:00 close was not tradable by then) and stays out of the statistics, like a
missed day. The mark price at formation is recorded to measure that drift.
Nothing here places orders.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

from core.research import retirement

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = PROJECT_ROOT / "data" / "research" / "xs_reversal"
STATE_PATH = STATE_DIR / "tracker.json"
UNIVERSE_PATH = STATE_DIR / "universe.json"
FAPI = "https://fapi.binance.com/fapi/v1"
EMA_SPAN = 21
SIGNAL_BARS = 200          # EMA(21) warm-up: weight of bars older than 200 is < 1e-8
MIN_COINS = 30
ROUND_TRIP_COST = 0.0015   # per position
FORMATION_WINDOW = timedelta(minutes=30)
SETTLE_GRACE = timedelta(days=3)  # after this, settle without positions whose exit never arrived
HOUR_MS = 3_600_000
BACKTEST_REFERENCE = ("586 days 2025-02..2026-09, ~127 coins, 12 per side: net +0.057%/position/day after 0.15% "
                      "costs and funding, 90% CI [-0.046, +0.162], 10/20 months positive (not significant)")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def perp_symbols(exchange_info: Dict[str, Any]) -> Dict[str, str]:
    """base -> live USDT perpetual symbol (XXXUSDT preferred, else 1000XXXUSDT)."""
    live = {s["symbol"] for s in exchange_info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}
    out: Dict[str, str] = {}
    for symbol in sorted(live):
        base = symbol[:-4]
        if base.startswith("1000") and len(base) > 4:
            out.setdefault(base[4:], symbol)
        else:
            out[base] = symbol
    return out


def reversal_signal(closes: pd.DataFrame) -> pd.Series:
    """EMA(21) of each column's closes over the last close, minus 1 (high = furthest below its EMA)."""
    ema = closes.ewm(span=EMA_SPAN, adjust=False, min_periods=10).mean()
    return ema.iloc[-1] / closes.iloc[-1] - 1.0


def pick_legs(signal: pd.Series) -> Optional[Tuple[List[str], List[str]]]:
    signal = signal.dropna()
    if len(signal) < MIN_COINS:
        return None
    k = len(signal) // 10
    ranked = signal.sort_values(kind="mergesort")
    return list(ranked.index[-k:]), list(ranked.index[:k])  # longs: furthest below EMA; shorts: furthest above


def load_state(path: Path = STATE_PATH) -> Dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"started_at": _now().isoformat(), "days": {}}


def save_state(state: Dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


async def _closes_until(client, symbol: str, entry_bar_ms: int) -> Optional[pd.Series]:
    """1h closes up to and including the bar that opened at entry_bar_ms (it closed at 00:00)."""
    resp = await client.get(f"{FAPI}/klines", params={"symbol": symbol, "interval": "1h",
                                                      "endTime": entry_bar_ms + HOUR_MS - 1, "limit": SIGNAL_BARS})
    if resp.status_code != 200:
        return None
    rows = resp.json()
    if not rows or int(rows[-1][0]) != entry_bar_ms:
        return None  # no closed 23:00 bar: halted or newly listed
    return pd.Series([float(r[4]) for r in rows], index=[int(r[0]) for r in rows])


async def _form(client, state: Dict[str, Any], day: datetime, now: datetime) -> Dict[str, Any]:
    info = await client.get(f"{FAPI}/exchangeInfo")
    info.raise_for_status()
    perps = perp_symbols(info.json())
    entry_bar_ms = int((day - timedelta(hours=1)).timestamp() * 1000)
    closes: Dict[str, pd.Series] = {}
    for base in state["universe"]:
        symbol = perps.get(base)
        if not symbol:
            continue
        try:
            series = await _closes_until(client, symbol, entry_bar_ms)
        except Exception as exc:  # noqa: BLE001 - one coin never blocks the formation
            logger.debug(f"xs reversal: klines for {symbol} failed: {exc}")
            continue
        if series is not None and len(series) >= 50:
            closes[symbol] = series
    if not closes:
        return {"status": "no_data", "formed_at": now.isoformat()}
    # every series ends at the 23:00 bar, so the last row of the time-aligned frame is the signal row
    signal = reversal_signal(pd.DataFrame(closes).sort_index())
    legs = pick_legs(signal)
    late = now - day > FORMATION_WINDOW
    record: Dict[str, Any] = {"formed_at": now.isoformat(), "entry_bar_ms": entry_bar_ms, "coins": int(signal.notna().sum()),
                              "late": late}
    if legs is None:
        record["status"] = "too_few_coins"
        return record
    longs, shorts = legs
    marks: Dict[str, float] = {}
    try:
        tick = await client.get(f"{FAPI}/ticker/price")
        if tick.status_code == 200:
            marks = {r["symbol"]: float(r["price"]) for r in tick.json()}
    except Exception:  # noqa: BLE001 - diagnostic only
        pass
    record.update(
        status="open",
        longs={s: {"entry": float(closes[s].iloc[-1]), "mark_at_formation": marks.get(s), "signal": round(float(signal[s]), 6)} for s in longs},
        shorts={s: {"entry": float(closes[s].iloc[-1]), "mark_at_formation": marks.get(s), "signal": round(float(signal[s]), 6)} for s in shorts},
    )
    return record


async def _exit_close(client, symbol: str, exit_bar_ms: int) -> Tuple[Optional[float], bool]:
    """(close of the exit bar, gone): gone=True when the contract no longer exists."""
    resp = await client.get(f"{FAPI}/klines", params={"symbol": symbol, "interval": "1h", "startTime": exit_bar_ms, "limit": 1})
    if resp.status_code == 400:
        return None, True
    if resp.status_code != 200:
        return None, False
    rows = resp.json()
    if not rows or int(rows[0][0]) != exit_bar_ms:
        return None, False
    return float(rows[0][4]), False


async def _funding(client, symbol: str, lo_ms: int, hi_ms: int) -> Optional[float]:
    resp = await client.get(f"{FAPI}/fundingRate", params={"symbol": symbol, "startTime": lo_ms + 1, "endTime": hi_ms, "limit": 1000})
    if resp.status_code != 200 or not isinstance(resp.json(), list):
        return None
    return sum(float(r["fundingRate"]) for r in resp.json() if lo_ms < int(r["fundingTime"]) <= hi_ms)


async def _settle(client, record: Dict[str, Any], now: datetime) -> None:
    entry_close_ms = int(record["entry_bar_ms"]) + HOUR_MS
    exit_bar_ms = int(record["entry_bar_ms"]) + 24 * HOUR_MS
    exit_close_ms = exit_bar_ms + HOUR_MS
    for side in ("longs", "shorts"):
        for symbol, pos in record[side].items():
            if pos.get("exit") is not None or pos.get("gone"):
                continue
            price, gone = await _exit_close(client, symbol, exit_bar_ms)
            if gone:
                pos["gone"] = True
                continue
            funding = await _funding(client, symbol, entry_close_ms, exit_close_ms) if price is not None else None
            if price is None or funding is None:
                continue  # transient: retry next pass
            pos.update(exit=price, funding=round(funding, 8))
            ret = price / pos["entry"] - 1.0
            pos["return"] = round((ret - funding) if side == "longs" else (-ret + funding), 8)
    pending = [p for side in ("longs", "shorts") for p in record[side].values() if p.get("return") is None and not p.get("gone")]
    overdue = now.timestamp() * 1000 > exit_close_ms + SETTLE_GRACE.total_seconds() * 1000
    if pending and not overdue:
        return
    legs = []
    for side in ("longs", "shorts"):
        values = [p["return"] for p in record[side].values() if p.get("return") is not None]
        legs.append(float(np.mean(values)) if values else None)
    if None in legs:
        record.update(status="unresolved", settled_at=now.isoformat())
        return
    record.update(
        status="closed", settled_at=now.isoformat(),
        long_leg_pct=round(legs[0] * 100, 4), short_leg_pct=round(legs[1] * 100, 4),
        net_per_position_pct=round(((legs[0] + legs[1]) / 2 - ROUND_TRIP_COST) * 100, 4),
        missing_positions=sum(1 for side in ("longs", "shorts") for p in record[side].values() if p.get("return") is None),
    )


async def tick(client, state_path: Optional[Path] = None, universe_path: Optional[Path] = None) -> Dict[str, Any]:
    state_path = state_path or STATE_PATH
    state = load_state(state_path)
    now = _now()
    if not state.get("universe"):
        source = universe_path or UNIVERSE_PATH
        if not source.exists():
            return {**summary(state), "error": "universe file missing: run scripts/xs_reversal_backtest.py"}
        state["universe"] = json.loads(source.read_text(encoding="utf-8"))["bases"]
        state["universe_frozen_at"] = now.isoformat()
    retirement.register(state, "xs_reversal", now.isoformat())

    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    key = day.date().isoformat()
    started_today_after_window = datetime.fromisoformat(state["started_at"]) > day + timedelta(hours=6)
    if key not in state["days"] and not started_today_after_window:
        if now - day <= timedelta(hours=6):
            state["days"][key] = await _form(client, state, day, now)
        else:  # the tracker was down through the whole morning
            state["days"][key] = {"status": "missed", "checked_at": now.isoformat()}
        save_state(state, state_path)

    for record in state["days"].values():
        if record.get("status") == "open" and now.timestamp() * 1000 >= int(record["entry_bar_ms"]) + 25 * HOUR_MS + 300_000:
            await _settle(client, record, now)
    state["updated_at"] = now.isoformat()
    save_state(state, state_path)
    return summary(state)


def summary(state: Dict[str, Any]) -> Dict[str, Any]:
    days = state.get("days", {})
    counted = [d for d in days.values() if d.get("status") == "closed" and not d.get("late")]
    returns = [float(d["net_per_position_pct"]) for d in counted]
    rule = state.get("retirement_rule") or retirement.RULES["xs_reversal"]
    last = max(days) if days else None
    return {
        "started_at": state.get("started_at"),
        "updated_at": state.get("updated_at"),
        "universe_size": len(state.get("universe") or []),
        "days_total": len(days),
        "forward_completed": len(returns),
        "forward_open": sum(d.get("status") == "open" for d in days.values()),
        "late": sum(bool(d.get("late")) for d in days.values()),
        "missed": sum(d.get("status") == "missed" for d in days.values()),
        "forward_mean_net_pct": round(float(np.mean(returns)), 3) if returns else None,
        "forward_positive_days": round(float(np.mean([r > 0 for r in returns])), 3) if returns else None,
        "last_day": last,
        "last_day_status": days[last].get("status") if last else None,
        "last_day_legs": {"longs": sorted(days[last].get("longs") or {}), "shorts": sorted(days[last].get("shorts") or {})} if last else None,
        "backtest_reference": BACKTEST_REFERENCE,
        "retirement": retirement.verdict(returns, rule),
    }
