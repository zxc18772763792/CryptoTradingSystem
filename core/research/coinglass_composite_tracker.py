"""Paper tracker: the 7-day Coinglass positioning composite, forward (research only, since 2026-10-11).

Why (docs/COINGLASS_COMBINATION_STUDY_2026-10-11.md): eight daily positioning signals from Coinglass
(liquidations, OI, funding, Binance long/short ratios, taker volume), combined with weights fitted
on 2024-01..2025-05, ranked alts for the next 7 days out of sample (IC +0.025, p 0.001); the
top-minus-bottom quintile held 7 days earned +0.17% per book after 0.12% costs and actual funding.
The study had already seen the later half's single-signal results, so only new data can confirm
it. This tracker runs that exact model forward.

Frozen rule:
  * universe: the 131 research coins (data/research/xs_reversal/universe.json) on Binance USDT perps;
  * data: day D's daily bars from core/research/coinglass_daily_store.py (updated after 00:15 UTC);
  * signals and composite exactly as the study: centred rank scores weighted by WEIGHTS (the
    training-period 7-day ICs), a coin needs half the total |weight|; >= MIN_COINS coins;
  * every day one new book: long the top fifth, short the bottom fifth, equal weight, entered at
    the market price when it forms - once the store has day D for READY_SHARE of the coins, or at
    FORM_DEADLINE after 00:00 with whatever coins are complete (>= MIN_COINS); a day with no book
    by then, or one that opened before the tracker started, has no book; held HOLD_DAYS and closed
    at the market. Entry is about 3 hours after the backtest's 00:00 (the store update's length);
  * net per book = long leg - short leg - funding (longs pay, shorts earn, Binance settlements in
    the hold) - 0.12% per round trip on the names replaced versus the book closing that day.
The book of the equal-weight variant (training-IC signs, equal magnitude) is recorded beside it as
a shadow and never judged. Verdict: core/research/retirement.py rule "coinglass_composite_7d" on
the main books, clustered by the ISO week they close in. Nothing here places orders.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

from core.research import coinglass_daily_store as store
from core.research import retirement

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_PATH = PROJECT_ROOT / "data" / "research" / "coinglass_composite" / "tracker.json"
FAPI = "https://fapi.binance.com/fapi/v1"
# 7-day IC of each signal over 2024-01-01..2025-05-21 (scripts/coinglass_combination_study.py, part A)
WEIGHTS: Dict[str, float] = {
    "liq_imbalance_1d": 0.0087, "liq_shock_z": -0.0077, "oi_change_3d": -0.0164, "funding_z": -0.0055,
    "retail_z": 0.0049, "top_z": -0.0055, "smart_vs_retail": -0.0068, "taker_imbalance_3d": 0.0142,
}
SHADOW_WEIGHTS: Dict[str, float] = {k: float(np.sign(v)) for k, v in WEIGHTS.items()}
HOLD_DAYS = 7
ROUND_TRIP_COST = 0.0012
MIN_COINS = 30
READY_SHARE = 0.9
FORM_DEADLINE = timedelta(hours=12)
LATE_GRACE = timedelta(minutes=30)  # a pass this long after the deadline with no book: the day is missed
SETTLE_GRACE = timedelta(days=1)
BACKTEST_REFERENCE = ("2025-05..2026-10 out of sample, weights from 2024-01..2025-05: +0.17% per 7-day book after 0.12% "
                      "costs and Binance funding (sd 3.1%), positive in 5 of 7 quarters; the equal-weight variant 7 of 7")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def load_state(path: Path = STATE_PATH) -> Dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"started_at": _now().isoformat(), "books": {}}


def save_state(state: Dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------------------- signals
def _series(rows: List[Dict[str, Any]], value) -> pd.Series:
    data = {pd.Timestamp(int(r["time"]), unit="ms", tz="UTC"): value(r) for r in rows or []}
    return pd.Series(data, dtype=float).sort_index()


def _num(row: Dict[str, Any], field: str) -> float:
    return float(pd.to_numeric(row.get(field), errors="coerce"))


def _ratio(row: Dict[str, Any], prefix: str) -> float:
    """Long/short from the percentages (two decimals of a percent) rather than the rounded ratio."""
    long_pct, short_pct = _num(row, f"{prefix}_long_percent"), _num(row, f"{prefix}_short_percent")
    if long_pct == long_pct and short_pct == short_pct and short_pct > 0:
        return long_pct / short_pct
    return _num(row, f"{prefix}_long_short_ratio")


def frames_from_store(coins: Dict[str, Dict[str, Any]], day: datetime) -> Dict[str, pd.DataFrame]:
    """Day-indexed frames (columns = perps) ending on day D, built like the study's panel."""
    fields = {
        "oi": ("oi", lambda r: _num(r, "close")),
        "funding": ("funding", lambda r: _num(r, "close")),
        "liq_long": ("liquidation", lambda r: _num(r, "aggregated_long_liquidation_usd")),
        "liq_short": ("liquidation", lambda r: _num(r, "aggregated_short_liquidation_usd")),
        "buy": ("taker", lambda r: _num(r, "aggregated_buy_volume_usd")),
        "sell": ("taker", lambda r: _num(r, "aggregated_sell_volume_usd")),
        "global": ("global_ratio", lambda r: _ratio(r, "global_account")),
        "top": ("top_ratio", lambda r: _ratio(r, "top_position")),
    }
    end = pd.Timestamp(day)
    grid = pd.date_range(end - pd.Timedelta(days=store.KEEP_DAYS), end, freq="1D")
    frames: Dict[str, pd.DataFrame] = {}
    for key, (dataset, value) in fields.items():
        columns = {}
        for perp, coin in coins.items():
            series = _series(coin.get(dataset) or [], value)
            if dataset in store.SNAPSHOT_AT_STAMP:
                series.index = series.index - pd.Timedelta(days=1)  # the D+1 00:00 snapshot closes day D
            columns[perp] = series[~series.index.duplicated(keep="last")]
        frames[key] = pd.DataFrame(columns).reindex(grid).sort_index(axis=1).astype(float)
    return frames


def _prior_z(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    prior = frame.shift(1).rolling(window, min_periods=max(5, window * 2 // 3))
    std = prior.std()
    return (frame - prior.mean()) / std.where(std > 1e-12)


def signals(frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """The study's eight signals (scripts/coinglass_positioning_study.py build_signals)."""
    log_glob = np.log(frames["global"].where(frames["global"] > 0))
    log_top = np.log(frames["top"].where(frames["top"] > 0))
    liq_total = frames["liq_long"] + frames["liq_short"]
    buy3, sell3 = frames["buy"].rolling(3).sum(), frames["sell"].rolling(3).sum()
    oi = frames["oi"].where(frames["oi"] > 0)
    return {
        "liq_imbalance_1d": (frames["liq_long"] - frames["liq_short"]) / liq_total.where(liq_total > 0),
        "liq_shock_z": _prior_z(np.log1p(liq_total.clip(lower=0)), 30),
        "oi_change_3d": np.log(oi) - np.log(oi.shift(3)),
        "funding_z": _prior_z(frames["funding"], 30),
        "retail_z": _prior_z(log_glob, 7),
        "top_z": _prior_z(log_top, 7),
        "smart_vs_retail": _prior_z(log_top - log_glob, 7),
        "taker_imbalance_3d": (buy3 - sell3) / (buy3 + sell3).where((buy3 + sell3) > 0),
    }


def composite_row(day_signals: Dict[str, pd.Series], weights: Dict[str, float]) -> pd.Series:
    """One day's composite: weighted centred rank scores; a coin needs half the total |weight|."""
    total = sum(abs(w) for w in weights.values())
    num: Optional[pd.Series] = None
    den: Optional[pd.Series] = None
    for name, w in weights.items():
        score = day_signals[name].rank(pct=True) - 0.5
        part, have = (score * w).fillna(0.0), score.notna() * abs(w)
        num = part if num is None else num.add(part, fill_value=0.0)
        den = have if den is None else den.add(have, fill_value=0.0)
    return (num / den).where(den >= 0.5 * total).dropna()


def books_from_scores(scores: pd.Series) -> Optional[Tuple[List[str], List[str]]]:
    if len(scores) < MIN_COINS:
        return None
    ranked = scores.sort_values(kind="mergesort")
    k = len(ranked) // 5
    return list(ranked.index[-k:]), list(ranked.index[:k])


# ------------------------------------------------------------------------------------ forward book
async def _prices(client) -> Dict[str, float]:
    resp = await client.get(f"{FAPI}/ticker/price")
    resp.raise_for_status()
    return {r["symbol"]: float(r["price"]) for r in resp.json()}


async def _funding(client, perp: str, lo_ms: int, hi_ms: int) -> Optional[float]:
    resp = await client.get(f"{FAPI}/fundingRate", params={"symbol": perp, "startTime": lo_ms + 1, "endTime": hi_ms, "limit": 1000})
    if resp.status_code != 200 or not isinstance(resp.json(), list):
        return None
    return sum(float(r["fundingRate"]) for r in resp.json() if lo_ms < int(r["fundingTime"]) <= hi_ms)


def _turnover(new: Tuple[List[str], List[str]], old: Optional[Dict[str, Any]]) -> float:
    """Average share of each leg replaced versus the book closing today (1 when there is none)."""
    if not old or old.get("status") not in {"open", "closed"}:
        return 1.0
    replaced_long = len(set(new[0]) - set(old["longs"])) / max(1, len(new[0]))
    replaced_short = len(set(new[1]) - set(old["shorts"])) / max(1, len(new[1]))
    return (replaced_long + replaced_short) / 2


def form(state: Dict[str, Any], day: datetime, now: datetime, prices: Dict[str, float],
         coins: Dict[str, Dict[str, Any]], partial: bool) -> Dict[str, Any]:
    frames = frames_from_store(coins, day)
    day_signals = {name: frame.loc[pd.Timestamp(day)] for name, frame in signals(frames).items()}
    record: Dict[str, Any] = {"formed_at": now.isoformat(), "coins": len(coins), "partial": partial}
    previous = state["books"].get((day - timedelta(days=HOLD_DAYS)).date().isoformat()) or {}
    for variant, weights in (("main", WEIGHTS), ("shadow", SHADOW_WEIGHTS)):
        scores = composite_row(day_signals, weights)
        scores = scores[[p for p in scores.index if prices.get(p)]]
        legs = books_from_scores(scores)
        if legs is None:
            record[variant] = {"status": "too_few_coins", "scored": int(len(scores))}
            continue
        record[variant] = {
            "status": "open", "scored": int(len(scores)), "turnover": round(_turnover(legs, previous.get(variant)), 4),
            "longs": {p: {"entry": prices[p], "score": round(float(scores[p]), 5)} for p in legs[0]},
            "shorts": {p: {"entry": prices[p], "score": round(float(scores[p]), 5)} for p in legs[1]},
        }
    return record


async def settle(client, record: Dict[str, Any], now: datetime, prices: Dict[str, float]) -> None:
    formed = datetime.fromisoformat(record["formed_at"])
    lo_ms, hi_ms = int(formed.timestamp() * 1000), int(now.timestamp() * 1000)
    overdue = now > formed + timedelta(days=HOLD_DAYS) + SETTLE_GRACE
    funding_cache: Dict[str, Optional[float]] = {}
    for variant in ("main", "shadow"):
        book = record.get(variant) or {}
        if book.get("status") != "open":
            continue
        for side in ("longs", "shorts"):
            for perp, pos in book[side].items():
                if pos.get("return") is not None or not prices.get(perp):
                    continue
                if perp not in funding_cache:
                    funding_cache[perp] = await _funding(client, perp, lo_ms, hi_ms)
                fund = funding_cache[perp]
                if fund is None:
                    continue
                ret = prices[perp] / pos["entry"] - 1.0
                pos.update(exit=prices[perp], funding=round(fund, 8),
                           **{"return": round((ret - fund) if side == "longs" else (-ret + fund), 8)})
        pending = [p for side in ("longs", "shorts") for p in book[side].values() if p.get("return") is None]
        if pending and not overdue:
            continue
        legs = [[p["return"] for p in book[side].values() if p.get("return") is not None] for side in ("longs", "shorts")]
        if not legs[0] or not legs[1]:
            book.update(status="unresolved", settled_at=now.isoformat())
            continue
        long_leg, short_leg = float(np.mean(legs[0])), float(np.mean(legs[1]))
        cost = 2 * float(book["turnover"]) * ROUND_TRIP_COST
        book.update(status="closed", settled_at=now.isoformat(), long_leg_pct=round(long_leg * 100, 4),
                    short_leg_pct=round(short_leg * 100, 4), cost_pct=round(cost * 100, 4),
                    net_pct=round((long_leg + short_leg - cost) * 100, 4), missing_positions=len(pending))


async def tick(client, *, state_path: Optional[Path] = None, store_root: Optional[Path] = None,
               status_path: Optional[Path] = None) -> Dict[str, Any]:
    state_path = state_path or STATE_PATH
    state = load_state(state_path)
    now = _now()
    retirement.register(state, "coinglass_composite_7d", now.isoformat())
    day = store.last_closed_day(now)
    key = day.date().isoformat()
    opens = day + timedelta(days=1)
    # a day's book only when the tracker was already running as the day opened
    if key not in state["books"] and datetime.fromisoformat(state["started_at"]) <= opens:
        coins: Dict[str, Dict[str, Any]] = {}
        for _, perp in store.universe():
            coin = store.load_coin(perp, store_root)
            if store.complete(coin, day):
                coins[perp] = coin
        share = len(coins) / max(1, len(store.universe()))
        status = store.load_status(status_path)
        finished = status.get("day") == key and bool(status.get("final"))  # no more passes coming today
        deadline = opens + FORM_DEADLINE
        if now > deadline + LATE_GRACE:
            state["books"][key] = {"status": "missed", "checked_at": now.isoformat(), "coins": len(coins)}
        elif share >= READY_SHARE or (finished and len(coins) >= MIN_COINS) or (now > deadline and len(coins) >= MIN_COINS):
            try:
                state["books"][key] = form(state, day, now, await _prices(client), coins, partial=share < READY_SHARE)
            except Exception as exc:  # noqa: BLE001 - retried next pass
                logger.warning(f"coinglass composite: forming {key} failed: {exc}")
        save_state(state, state_path)
    due = [r for r in state["books"].values() if any((r.get(v) or {}).get("status") == "open" for v in ("main", "shadow"))
           and now >= datetime.fromisoformat(r["formed_at"]) + timedelta(days=HOLD_DAYS)]
    if due:
        prices = await _prices(client)
        for record in due:
            await settle(client, record, now, prices)
    state["updated_at"] = now.isoformat()
    save_state(state, state_path)
    return summary(state)


def summary(state: Dict[str, Any]) -> Dict[str, Any]:
    records = state.get("books", {})
    out: Dict[str, Any] = {"started_at": state.get("started_at"), "updated_at": state.get("updated_at"),
                           "days_total": len(records), "missed": sum(r.get("status") == "missed" for r in records.values()),
                           "backtest_reference": BACKTEST_REFERENCE, "hold_days": HOLD_DAYS}
    for variant in ("main", "shadow"):
        closed = [(day, r[variant]) for day, r in sorted(records.items())
                  if (r.get(variant) or {}).get("status") == "closed"]
        returns = [float(b["net_pct"]) for _, b in closed]
        weeks = [pd.Timestamp(b["settled_at"]).isocalendar() for _, b in closed]
        clusters = [int(w[0]) * 100 + int(w[1]) for w in weeks]
        part = {
            "open": sum((r.get(variant) or {}).get("status") == "open" for r in records.values()),
            "completed": len(returns),
            "mean_net_pct": round(float(np.mean(returns)), 3) if returns else None,
            "positive_share": round(float(np.mean([v > 0 for v in returns])), 3) if returns else None,
            "mean_long_leg_pct": round(float(np.mean([b["long_leg_pct"] for _, b in closed])), 3) if closed else None,
            "mean_short_leg_pct": round(float(np.mean([b["short_leg_pct"] for _, b in closed])), 3) if closed else None,
        }
        if variant == "main":
            rule = state.get("retirement_rule") or retirement.RULES["coinglass_composite_7d"]
            part["retirement"] = retirement.verdict(returns, rule, clusters=clusters)
            out.update(retirement=part["retirement"], forward_completed=len(returns), forward_open=part["open"],
                       forward_mean_net_pct=part["mean_net_pct"], forward_win_rate=part["positive_share"])
        out[variant] = part
    latest = max((d for d, r in records.items() if (r.get("main") or {}).get("longs")), default=None)
    if latest:
        book = records[latest]["main"]
        out["latest_book"] = {"day": latest, "formed_at": records[latest].get("formed_at"),
                              "longs": sorted(book["longs"], key=lambda p: -book["longs"][p]["score"]),
                              "shorts": sorted(book["shorts"], key=lambda p: book["shorts"][p]["score"])}
    return out
