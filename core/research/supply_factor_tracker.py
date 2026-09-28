"""Forward paper tracker: monthly long/short on scheduled supply growth (research only).

Backtest (scripts/supply_inflation_factor.py, docs/LLM_TRADING_RESEARCH_ROUND5_2026-09-27.md):
each month start, rank DefiLlama-scheduled tokens with a verified Binance spot
price by how much new supply their schedule releases over the next 90 days;
long the lowest third, short the highest third, hold 30 days, 0.4% cost:
+2.6%/month (76% of months positive), rank IC negative in 33/42 months, and
the same within age and market-cap groups. This tracker repeats exactly that
rule on months that have not happened yet.

Weekly measurement (added 2026-09-28, no trades): every Monday close the tracker
records each token's scheduled 90-day growth and price, and a week later the rank
correlation between growth and the week's return. The monthly trade needs 12
months for a verdict; the direction test gets ~4x the samples (backtest: IC < 0
in 141/194 weeks, p=1e-10), while weekly *trading* does not pay after costs.

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
from core.research.xs_evaluation import spearman
from core.research import unlock_short_tracker as ut
from core.research.unlock_events import LLAMA_DATASETS, entry_price, entry_ticker, unlocked_series

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_PATH = PROJECT_ROOT / "data" / "research" / "supply_factor" / "tracker.json"
SPOT = "https://api.binance.com/api/v3"
HORIZON_DAYS, HOLD_DAYS, MONTH_COST = 90, 30, 0.004
MIN_TOKENS = 12
CLOSE_GRACE_DAYS = 5  # after this, close with the exit prices that exist (true delistings)
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
    return growth_from_unlocked(unlocked_series(schedule), day, horizon_days)


def growth_from_unlocked(unlocked: pd.Series, day: pd.Timestamp, horizon_days: int = HORIZON_DAYS) -> Optional[float]:
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
    """Close of the daily bar that opens on `day`, only once that day has ended.

    None = the pair has no such bar (delisted: HTTP 400, or no row). Transient
    failures raise instead, so the caller retries rather than dropping the coin.
    """
    if day + DAY > now:
        return None
    resp = await client.get(f"{SPOT}/klines", params={"symbol": symbol, "interval": "1d", "startTime": int(day.timestamp() * 1000), "limit": 1})
    rows = ut._checked_json(resp)
    if not isinstance(rows, list) or not rows or pd.Timestamp(rows[0][0], unit="ms", tz="UTC") != day:
        return None
    return float(rows[0][4])


async def _universe_growth(client, day: pd.Timestamp):
    universe = await _spot_universe(client)
    growth: Dict[str, float] = {}
    for ticker, token in universe.items():
        try:
            unlocked = await ut.schedule_unlocked(client, token["slug"])
        except Exception:  # noqa: BLE001 - some index entries have no schedule file
            continue
        g = growth_from_unlocked(unlocked, day)
        if g is not None:
            growth[ticker] = g
    return universe, growth


async def _open_month(client, day: pd.Timestamp, now: pd.Timestamp, backfill: bool) -> Optional[Dict[str, Any]]:
    universe, growth = await _universe_growth(client, day)
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
    give_up = now >= exit_day + pd.Timedelta(days=CLOSE_GRACE_DAYS)
    rets: Dict[str, List[float]] = {"long": [], "short": []}
    for leg in ("long", "short"):
        for ticker, row in month[leg].items():
            if not row.get("entry"):
                continue
            if not row.get("exit"):
                try:
                    row["exit"] = await _close_on(client, month["symbols"][ticker], exit_day, now)
                except Exception:  # noqa: BLE001 - transient: keep the month open and retry next pass
                    if not give_up:
                        return
                    row["exit"] = None
            if row["exit"]:  # a pair delisted mid-month has no exit close and is dropped, as in the backtest
                rets[leg].append(row["exit"] / row["entry"] - 1)
    if not rets["long"] or not rets["short"]:
        if give_up:  # a whole leg without exit prices: record it and stop polling
            month["status"] = "unresolved"
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
    try:
        await _tick_weekly_ic(client, state, now, started)
    except Exception as exc:  # noqa: BLE001 - measurement only; retried next pass
        logger.warning(f"supply factor: weekly IC failed: {exc}")
    state["updated_at"] = now.isoformat()
    retirement.register(state, "supply_factor", now.isoformat())
    save_state(state, state_path)
    return summary(state)


async def _tick_weekly_ic(client, state: Dict[str, Any], now: pd.Timestamp, started: pd.Timestamp) -> None:
    weeks = state.setdefault("weekly_ic", {})
    monday = now.normalize() - pd.Timedelta(days=now.weekday())
    key = str(monday.date())
    if key not in weeks and monday + DAY <= now:
        universe, growth = await _universe_growth(client, monday)
        if len(growth) >= MIN_TOKENS:
            entries: Dict[str, float] = {}
            for ticker in growth:
                px = await _close_on(client, universe[ticker]["symbol"], monday, now)  # transient errors raise: retry
                if px:
                    entries[ticker] = px
            weeks[key] = {"week": key, "status": "open", "backfill": monday < started.normalize(),
                          "growth": {t: round(growth[t], 5) for t in entries}, "entry": entries,
                          "symbols": {t: universe[t]["symbol"] for t in entries}}
    for week in weeks.values():
        end = pd.Timestamp(week["week"], tz="UTC") + pd.Timedelta(days=7)
        if week["status"] != "open" or end + DAY > now:
            continue
        exits: Dict[str, float] = {}
        for ticker, symbol in week["symbols"].items():
            px = await _close_on(client, symbol, end, now)
            if px:
                exits[ticker] = px
        rets = {t: exits[t] / week["entry"][t] - 1 for t in exits}
        if len(rets) < MIN_TOKENS:
            week["status"] = "unresolved"
            continue
        g = pd.Series({t: week["growth"][t] for t in rets})
        r = pd.Series(rets)
        q = g.rank(pct=True)
        week.update(status="closed", n=len(rets), ic=round(spearman(g, r), 4),
                    spread_pct=round(float((r[q <= 1 / 3].mean() - r[q > 2 / 3].mean()) * 100), 3))
        week.pop("entry", None)  # keep the state small once the week is scored
        week.pop("symbols", None)


def weekly_ic_summary(state: Dict[str, Any]) -> Dict[str, Any]:
    from math import comb

    closed = [w for w in (state.get("weekly_ic") or {}).values() if w.get("status") == "closed" and not w.get("backfill")]
    n = len(closed)
    negative = sum(float(w["ic"]) < 0 for w in closed)
    return {
        "weeks_completed": n,
        "weeks_ic_negative": negative,
        "mean_ic": round(float(np.mean([w["ic"] for w in closed])), 4) if closed else None,
        "mean_gross_spread_pct": round(float(np.mean([w["spread_pct"] for w in closed])), 3) if closed else None,
        # one-sided sign test that low-growth tokens beat high-growth ones (IC < 0)
        "sign_test_p": round(sum(comb(n, i) for i in range(negative, n + 1)) / 2 ** n, 4) if n else None,
        "backtest_reference": "weekly rank IC < 0 in 141/194 weeks 2023-26 (p=1e-10); weekly trading does not pay after 0.4% costs",
    }


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
        "weekly_ic": weekly_ic_summary(state),
        "retirement": retirement.verdict(spreads, state.get("retirement_rule") or retirement.RULES["supply_factor"]),
    }
