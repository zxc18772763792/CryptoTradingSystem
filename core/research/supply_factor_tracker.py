"""Forward paper tracker: monthly long/short on scheduled supply growth (research only).

Backtest (scripts/supply_inflation_factor.py, docs/LLM_TRADING_RESEARCH_ROUND5_2026-09-27.md):
each month start, rank DefiLlama-scheduled tokens with a verified Binance spot
price by how much new supply their schedule releases over the next 90 days;
long the lowest third, short the highest third, hold 30 days, 0.4% cost:
+2.6%/month (76% of months positive), rank IC negative in 33/42 months, and
the same within age and market-cap groups. This tracker repeats exactly that
rule on months that have not happened yet.

Rules: the portfolio for month D uses the schedule as DefiLlama shows it on
the rebalance day, entry = close of day D, exit = close of day D+30, returns
from Binance spot daily closes. Months that started before the tracker are
recorded as ``backfill`` (schedules are today's, so they are not independent)
and excluded from forward statistics.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from core.research import retirement
from core.research import unlock_short_tracker as ut
from core.research.unlock_events import LLAMA_DATASETS, entry_price, entry_ticker, unlocked_series

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_PATH = PROJECT_ROOT / "data" / "research" / "supply_factor" / "tracker.json"
SPOT = "https://api.binance.com/api/v3"
HORIZON_DAYS, HOLD_DAYS, MONTH_COST = 90, 30, 0.004
MIN_TOKENS = 12
BACKTEST_REFERENCE = "long low / short high supply-growth terciles, 30d: +2.6%/month, 76% of months positive, IC<0 in 33/42 months"
DAY = pd.Timedelta(days=1)


def load_state(path: Path = STATE_PATH) -> Dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"started_at": datetime.now(timezone.utc).isoformat(), "months": {}}


def save_state(state: Dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


def supply_growth(schedule: Dict[str, Any], day: pd.Timestamp, horizon_days: int = HORIZON_DAYS) -> Optional[float]:
    """Scheduled growth of unlocked supply from `day` to `day + horizon` (None if not covered)."""
    unlocked = unlocked_series(schedule)
    if unlocked.empty:
        return None
    unlocked = unlocked.asfreq("1D").ffill()
    end = day + pd.Timedelta(days=horizon_days)
    if day not in unlocked.index or end not in unlocked.index or unlocked[day] <= 0:
        return None
    return float(unlocked[end] / unlocked[day] - 1)


def assign_legs(growth: Dict[str, float]) -> Dict[str, List[str]]:
    """Lowest third long, highest third short (the backtest's tercile rule)."""
    ranks = pd.Series(growth).rank(pct=True)
    return {"long": sorted(ranks[ranks <= 1 / 3].index), "short": sorted(ranks[ranks > 2 / 3].index)}


async def _spot_universe(client) -> Dict[str, Dict[str, Any]]:
    index = await ut._cached_json(client, f"{LLAMA_DATASETS}/emissionsIndex", ut.CACHE_DIR / "emissions_index.json", ut.INDEX_TTL_SEC)
    index = index["data"] if isinstance(index, dict) else index
    prices = {p["symbol"]: float(p["price"]) for p in (await client.get(f"{SPOT}/ticker/price")).json()}
    out: Dict[str, Dict[str, Any]] = {}
    for entry in index:
        ticker, llama_px = entry_ticker(entry), entry_price(entry)
        symbol = f"{ticker}USDT"
        if ticker and llama_px > 0 and symbol in prices and abs(prices[symbol] / llama_px - 1) <= ut.PRICE_MATCH_TOLERANCE:
            out.setdefault(ticker, {"symbol": symbol, "slug": entry.get("protocolSlug")})
    return out


async def _close_on(client, symbol: str, day: pd.Timestamp, now: pd.Timestamp) -> Optional[float]:
    """Close of the daily bar that opens on `day`, only once that day has ended."""
    if day + DAY > now:
        return None
    resp = await client.get(f"{SPOT}/klines", params={"symbol": symbol, "interval": "1d", "startTime": int(day.timestamp() * 1000), "limit": 1})
    rows = resp.json() if resp.status_code == 200 else []
    if not isinstance(rows, list) or not rows or pd.Timestamp(rows[0][0], unit="ms", tz="UTC") != day:
        return None
    return float(rows[0][4])


async def _open_month(client, day: pd.Timestamp, now: pd.Timestamp, backfill: bool) -> Optional[Dict[str, Any]]:
    universe = await _spot_universe(client)
    growth: Dict[str, float] = {}
    for ticker, token in universe.items():
        try:
            schedule = await ut._cached_json(client, f"{LLAMA_DATASETS}/emissions/{token['slug']}",
                                             ut.CACHE_DIR / "emissions" / f"{token['slug']}.json", ut.SCHEDULE_TTL_SEC)
        except Exception:  # noqa: BLE001 - some index entries have no schedule file
            continue
        g = supply_growth(schedule, day)
        if g is not None:
            growth[ticker] = g
    if len(growth) < MIN_TOKENS:
        logger.warning(f"supply factor: only {len(growth)} tokens with schedules for {day.date()}")
        return None
    legs = assign_legs(growth)
    entries: Dict[str, float] = {}
    for ticker in legs["long"] + legs["short"]:
        px = await _close_on(client, universe[ticker]["symbol"], day, now)
        if px:
            entries[ticker] = px
    return {
        "rebalance_day": str(day.date()), "exit_day": str((day + pd.Timedelta(days=HOLD_DAYS)).date()),
        "backfill": backfill, "status": "open", "universe": len(growth),
        "long": {t: {"growth": round(growth[t], 4), "entry": entries.get(t)} for t in legs["long"]},
        "short": {t: {"growth": round(growth[t], 4), "entry": entries.get(t)} for t in legs["short"]},
        "symbols": {t: universe[t]["symbol"] for t in legs["long"] + legs["short"]},
    }


async def _close_month(client, month: Dict[str, Any], now: pd.Timestamp) -> None:
    exit_day = pd.Timestamp(month["exit_day"], tz="UTC")
    rets: Dict[str, List[float]] = {"long": [], "short": []}
    for leg in ("long", "short"):
        for ticker, row in month[leg].items():
            if not row.get("entry"):
                continue
            px = await _close_on(client, month["symbols"][ticker], exit_day, now)
            row["exit"] = px
            if px:  # a pair delisted mid-month has no exit close and is dropped, as in the backtest
                rets[leg].append(px / row["entry"] - 1)
    if not rets["long"] or not rets["short"]:
        return
    spread = float(np.mean(rets["long"]) - np.mean(rets["short"]) - MONTH_COST)
    month.update(status="closed", long_return_pct=round(float(np.mean(rets["long"])) * 100, 3),
                 short_return_pct=round(float(np.mean(rets["short"])) * 100, 3), spread_pct=round(spread * 100, 3))


async def tick(client, state_path: Path = STATE_PATH, now: Optional[pd.Timestamp] = None) -> Dict[str, Any]:
    state = load_state(state_path)
    now = now or pd.Timestamp.now(tz="UTC")
    started = pd.Timestamp(state["started_at"]).tz_convert("UTC")
    month_start = now.normalize().replace(day=1)
    for day in [month_start]:
        key = str(day.date())
        if key in state["months"] or day + DAY > now:
            continue
        try:
            month = await _open_month(client, day, now, backfill=day < started.normalize())
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"supply factor: rebalance {key} failed: {exc}")
            month = None
        if month:
            state["months"][key] = month
    for month in state["months"].values():
        if month["status"] == "open" and pd.Timestamp(month["exit_day"], tz="UTC") + DAY <= now:
            try:
                await _close_month(client, month, now)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"supply factor: close {month['rebalance_day']} failed: {exc}")
    state["updated_at"] = now.isoformat()
    retirement.register(state, "supply_factor", now.isoformat())
    save_state(state, state_path)
    return summary(state)


def summary(state: Dict[str, Any]) -> Dict[str, Any]:
    months = list(state.get("months", {}).values())
    forward = [m for m in months if not m.get("backfill")]
    done = [m for m in forward if m.get("status") == "closed"]
    spreads = [float(m["spread_pct"]) for m in done]
    return {
        "started_at": state.get("started_at"),
        "updated_at": state.get("updated_at"),
        "months_total": len(months),
        "forward_open": sum(m.get("status") == "open" for m in forward),
        "forward_completed": len(done),
        "forward_mean_spread_pct": round(float(np.mean(spreads)), 2) if spreads else None,
        "forward_positive_months": round(float(np.mean([s > 0 for s in spreads])), 3) if spreads else None,
        "backtest_reference": BACKTEST_REFERENCE,
        "evaluation_ready": len(done) >= 12,
        "retirement": retirement.verdict(spreads, state.get("retirement_rule") or retirement.RULES["supply_factor"]),
    }
