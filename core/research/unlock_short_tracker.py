"""Forward paper tracker: short the perp before large cliff unlocks (research only).

Backtest (docs/UNLOCK_EVENT_STUDY_2026-09-27.md, scripts/unlock_short_backtest.py):
cliffs >= 10% of unlocked supply, short the Binance USDT perp at the close 30
days before the unlock, exit at the close the day before, +40% stop on daily
highs (+2% slippage), real funding, 0.2% per leg, hedged with an equal-weight
basket of the other study perps that have no cliff within +/-45 days:
+9.5%/trade, 90% CI [+5.7, +13.4], 73% win, 2022-24 +12.6%, 2025-26 +8.0%.
That is in-sample; this tracker records the same rule on unlocks that have
not happened yet. Future schedules are public, so trades are known ~30 days
ahead and the forward sample grows at the backtest's ~30/year pace.

Honesty rules:
* unlocks first seen after their entry day are tracked as ``late`` (entered at
  the next close) and excluded from forward statistics;
* if a later schedule no longer shows the unlock (moved > 3 days, shrunk below
  the threshold or removed) the trade is closed as ``schedule_changed`` - the
  backtest could not see such revisions, so they must be counted, not hidden.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from core.research import retirement
from core.research.unlock_events import LLAMA_DATASETS, cliff_events, entry_price, entry_ticker, unlocked_series

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_PATH = PROJECT_ROOT / "data" / "research" / "unlock_short" / "tracker.json"
CACHE_DIR = PROJECT_ROOT / "data" / "research" / "unlock_short" / "cache"
FAPI = "https://fapi.binance.com/fapi/v1"

MIN_SIZE_PCT = 10.0
ENTRY_DAYS_BEFORE, EXIT_DAYS_BEFORE = 30, 1
STOP_PCT, STOP_SLIPPAGE, LEG_COST = 0.40, 0.02, 0.002
BASKET_EXCLUDE_DAYS, BASKET_CLIFF_PCT, MIN_BASKET = 45, 0.5, 5
LOOKAHEAD_DAYS = 60
PRICE_MATCH_TOLERANCE = 0.15
INDEX_TTL_SEC, SCHEDULE_TTL_SEC = 86400, 3 * 86400
BACKTEST_REFERENCE = "cliffs>=10%, t-30..t-1, +40% stop, basket-hedged: +9.5%/trade, 90% CI [+5.7, +13.4], win 73%"
DAY = pd.Timedelta(days=1)


def _now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def load_state(path: Path = STATE_PATH) -> Dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"started_at": datetime.now(timezone.utc).isoformat(), "trades": {}}


def save_state(state: Dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


async def _cached_json(client, url: str, path: Path, ttl_sec: float) -> Any:
    if path.exists() and time.time() - path.stat().st_mtime < ttl_sec:
        return json.loads(path.read_text(encoding="utf-8"))
    resp = await client.get(url)
    resp.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(resp.text, encoding="utf-8")
    return resp.json()


async def _daily_bars(client, symbol: str, start: pd.Timestamp) -> pd.DataFrame:
    resp = await client.get(f"{FAPI}/klines", params={"symbol": symbol, "interval": "1d",
                                                       "startTime": int(start.timestamp() * 1000), "limit": 120})
    rows = resp.json() if resp.status_code == 200 else []
    frame = pd.DataFrame([[r[0], float(r[2]), float(r[4])] for r in rows or []], columns=["t", "high", "close"])
    frame.index = pd.to_datetime(frame.pop("t"), unit="ms", utc=True)
    return frame


async def _funding(client, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> float:
    resp = await client.get(f"{FAPI}/fundingRate", params={"symbol": symbol, "startTime": int(start.timestamp() * 1000),
                                                            "endTime": int(end.timestamp() * 1000), "limit": 1000})
    rows = resp.json() if resp.status_code == 200 and isinstance(resp.json(), list) else []
    return float(sum(float(r.get("fundingRate") or 0) for r in rows))


def closed_close(bars: pd.DataFrame, day: pd.Timestamp, now: pd.Timestamp) -> Optional[float]:
    """Close of a daily bar, only once that day has fully ended."""
    day = day.normalize()
    if day not in bars.index or day + DAY > now:
        return None
    return float(bars.at[day, "close"])


async def build_universe(client) -> Dict[str, Dict[str, Any]]:
    """DefiLlama tokens with a Binance USDT perp whose price matches DefiLlama's."""
    index = await _cached_json(client, f"{LLAMA_DATASETS}/emissionsIndex", CACHE_DIR / "emissions_index.json", INDEX_TTL_SEC)
    index = index["data"] if isinstance(index, dict) else index
    info = (await client.get(f"{FAPI}/exchangeInfo")).json()
    perps = {s["symbol"] for s in info.get("symbols", []) if s.get("contractType") == "PERPETUAL"
             and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}
    prices = {p["symbol"]: float(p["price"]) for p in (await client.get(f"{FAPI}/ticker/price")).json()}
    universe: Dict[str, Dict[str, Any]] = {}
    for entry in index:
        ticker, llama_px = entry_ticker(entry), entry_price(entry)
        if not ticker or llama_px <= 0:
            continue
        for symbol, scale in ((f"{ticker}USDT", 1.0), (f"1000{ticker}USDT", 1000.0)):
            if symbol in perps and abs(prices.get(symbol, 0) / scale / llama_px - 1) <= PRICE_MATCH_TOLERANCE:
                universe[ticker] = {"symbol": symbol, "slug": entry.get("protocolSlug"), "entry": entry}
                break
    return universe


async def _cliffs(client, token: Dict[str, Any], min_pct: float) -> List[Dict[str, Any]]:
    schedule = await _cached_json(client, f"{LLAMA_DATASETS}/emissions/{token['slug']}",
                                  CACHE_DIR / "emissions" / f"{token['slug']}.json", SCHEDULE_TTL_SEC)
    return cliff_events(token["entry"], unlocked_series(schedule), min_pct)


async def _leg(client, symbol: str, entry_day: pd.Timestamp, exit_day: pd.Timestamp, side: int) -> Optional[float]:
    bars = await _daily_bars(client, symbol, entry_day - DAY)
    if entry_day not in bars.index or exit_day not in bars.index:
        return None
    fund = await _funding(client, symbol, entry_day + DAY, exit_day + DAY)
    return side * (bars.at[exit_day, "close"] / bars.at[entry_day, "close"] - 1) - side * fund - LEG_COST


async def tick(client, state_path: Path = STATE_PATH) -> Dict[str, Any]:
    state = load_state(state_path)
    now = _now()
    today = now.normalize()
    universe = await build_universe(client)
    cliffs: Dict[str, List[Dict[str, Any]]] = {}
    for ticker, token in universe.items():
        try:
            cliffs[ticker] = await _cliffs(client, token, BASKET_CLIFF_PCT)
        except Exception as exc:  # noqa: BLE001 - a missing schedule file must not stop the pass
            logger.debug(f"unlock tracker: schedule unavailable for {ticker}: {exc}")

    # 1) discover upcoming large unlocks
    for ticker, events in cliffs.items():
        for ev in events:
            if ev["size_pct"] < MIN_SIZE_PCT or not (today < ev["date"] <= today + pd.Timedelta(days=LOOKAHEAD_DAYS)):
                continue
            key = f"{ticker}|{ev['date'].date()}"
            if key in state["trades"]:
                continue
            entry_day = ev["date"] - pd.Timedelta(days=ENTRY_DAYS_BEFORE)
            state["trades"][key] = {
                "token": ticker, "symbol": universe[ticker]["symbol"], "unlock_date": str(ev["date"].date()),
                "size_pct": round(ev["size_pct"], 2), "insider": ev["insider"],
                "entry_day": str(max(entry_day, today).date()), "exit_day": str((ev["date"] - pd.Timedelta(days=EXIT_DAYS_BEFORE)).date()),
                "late": bool(entry_day < today), "discovered_at": now.isoformat(), "status": "scheduled",
            }

    # 2) enter / manage / close
    for key, trade in state["trades"].items():
        if trade["status"] in {"closed", "stopped", "schedule_changed", "entry_failed"}:
            continue
        try:
            await _update_trade(client, trade, universe, cliffs, now)
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"unlock tracker: update failed for {key}: {exc}")
    state["updated_at"] = now.isoformat()
    retirement.register(state, "unlock_short", now.isoformat())
    save_state(state, state_path)
    return summary(state)


async def _update_trade(client, trade, universe, cliffs, now) -> None:
    entry_day = pd.Timestamp(trade["entry_day"], tz="UTC")
    exit_day = pd.Timestamp(trade["exit_day"], tz="UTC")
    unlock = pd.Timestamp(trade["unlock_date"], tz="UTC")
    bars = await _daily_bars(client, trade["symbol"], entry_day - DAY)

    if trade["status"] == "scheduled":
        price = closed_close(bars, entry_day, now)
        if price is None:
            if now > entry_day + 3 * DAY:
                trade["status"] = "entry_failed"
            return
        members = [t for t, evs in cliffs.items() if t != trade["token"] and t in universe
                   and not any(abs((unlock - e["date"]).days) <= BASKET_EXCLUDE_DAYS for e in evs)]
        trade.update(status="open", entry_price=price, basket=[universe[t]["symbol"] for t in members])
        return

    # still scheduled in the latest DefiLlama data?
    events = cliffs.get(trade["token"])
    if events is not None and not any(abs((e["date"] - unlock).days) <= 3 and e["size_pct"] >= MIN_SIZE_PCT for e in events):
        return await _close(client, trade, now.normalize() - DAY, "schedule_changed", bars)

    entry = float(trade["entry_price"])
    after = bars[(bars.index > entry_day) & (bars.index <= min(exit_day, now.normalize() - DAY))]
    hit = after[after["high"] >= entry * (1 + STOP_PCT)]
    if len(hit):
        return await _close(client, trade, hit.index[0], "stopped", bars, stop_price=entry * (1 + STOP_PCT) * (1 + STOP_SLIPPAGE))
    if closed_close(bars, exit_day, now) is not None:
        return await _close(client, trade, exit_day, "closed", bars)
    last = closed_close(bars, now.normalize() - DAY, now)
    if last:
        trade["mark_return_pct"] = round((entry / last - 1) * 100, 2)  # unhedged, pre-funding, for display only


async def _close(client, trade, day, status, bars, stop_price: Optional[float] = None) -> None:
    entry_day = pd.Timestamp(trade["entry_day"], tz="UTC")
    day = max(pd.Timestamp(day).normalize(), entry_day)
    exit_px = stop_price if stop_price is not None else float(bars.at[day, "close"]) if day in bars.index else None
    if exit_px is None:
        return
    fund = await _funding(client, trade["symbol"], entry_day + DAY, day + DAY)
    short = -(exit_px / float(trade["entry_price"]) - 1) + fund - LEG_COST
    legs = [r for r in [await _leg(client, sym, entry_day, day, +1) for sym in trade.get("basket") or []] if r is not None]
    trade.update(
        status=status, exit_day_actual=str(day.date()), exit_price=exit_px,
        short_return_pct=round(short * 100, 3), funding_pct=round(fund * 100, 3), basket_n=len(legs),
        hedged_return_pct=round((short + float(np.mean(legs))) * 100, 3) if len(legs) >= MIN_BASKET else None,
    )


def summary(state: Dict[str, Any]) -> Dict[str, Any]:
    trades = list(state.get("trades", {}).values())
    forward = [t for t in trades if not t.get("late")]
    done = [t for t in forward if t.get("status") in {"closed", "stopped", "schedule_changed"}]
    hedged = [float(t["hedged_return_pct"]) for t in done if t.get("hedged_return_pct") is not None]
    return {
        "started_at": state.get("started_at"),
        "updated_at": state.get("updated_at"),
        "trades_total": len(trades),
        "late_trades": len(trades) - len(forward),
        "forward_scheduled": sum(t.get("status") == "scheduled" for t in forward),
        "forward_open": sum(t.get("status") == "open" for t in forward),
        "forward_completed": len(done),
        "forward_schedule_changed": sum(t.get("status") == "schedule_changed" for t in done),
        "forward_mean_hedged_pct": round(float(np.mean(hedged)), 2) if hedged else None,
        "forward_win_rate": round(float(np.mean([h > 0 for h in hedged])), 3) if hedged else None,
        "backtest_reference": BACKTEST_REFERENCE,
        "evaluation_ready": len(hedged) >= 20,
        "retirement": retirement.verdict(hedged, state.get("retirement_rule") or retirement.RULES["unlock_short"]),
    }
