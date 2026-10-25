"""KOL post event study. Implements docs/X_KOL_RESEARCH_PROTOCOL.md; read that first.

Reads the research archive (core/news/x_archive.py) and the local Binance USDT-perp 1h
bars, builds one event per (post, coin), and asks whether following the post's direction
for 24 hours earns more than the same coin on random other days, after costs.

Everything that decides a result is a constant below or in the protocol, fixed before any
return was computed. Changing a constant after seeing results makes the new numbers
exploratory. Run once per data snapshot; the output records the hashes of the protocol
and of this file so a later reader can tell whether they were edited.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PRICE_DIR = ROOT / "data" / "historical" / "binance_futures"
HORIZON_PRIMARY = 24
HORIZONS_EXPLORATORY = (1, 4, 72)
WINDOW_DAYS = 90               # events and null days both come from the most recent 90 days: one market regime
MAX_COINS_PER_POST = 3          # a post naming more coins is a list or recap, not a call
DEDUP_WINDOW = pd.Timedelta(hours=24)
COST_BP_MAJOR = 6.0             # per side, BTC and ETH
COST_BP_OTHER = 10.0            # per side
BTC_LEG_BP = 6.0                # sensitivity only: the cost of hedging the market leg with BTC
N_RESAMPLES = 2000
SEED = 20261025
MIN_HANDLE_EVENTS = 30
MIN_POOLED_EVENTS = 60

STABLES = {"USDT", "USDC", "USD", "DAI", "FDUSD", "TUSD", "BUSD"}
MAJORS = {
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "TON", "SUI", "HYPE", "ENA", "DOT", "LTC",
    "BCH", "AAVE", "UNI", "ARB", "TRX", "APT", "INJ", "TAO", "WLD", "PEPE", "SHIB", "ONDO", "PENDLE", "FET",
    "TIA", "SEI", "JUP", "ZRO", "ETHFI", "AERO", "ATOM", "FIL", "ETC", "XLM", "HBAR", "ICP", "RENDER", "WIF",
    "BONK", "FLOKI", "CRV", "MKR", "LDO", "PUMP", "ASTER", "WLFI",
}
NAMES = {
    "bitcoin": "BTC", "比特币": "BTC", "大饼": "BTC", "ethereum": "ETH", "ether": "ETH", "以太坊": "ETH",
    "以太": "ETH", "solana": "SOL", "索拉纳": "SOL", "dogecoin": "DOGE", "狗狗币": "DOGE", "cardano": "ADA",
    "币安币": "BNB", "瑞波": "XRP",
}
CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Za-z][A-Za-z0-9]{1,9})(?![\w])")
UPPER_RE = re.compile(r"(?<![\w$#@])([A-Z][A-Z0-9]{1,9})(?![\w])")
URL_RE = re.compile(r"https?://\S+|@\w+")

BULL_EN = re.compile(
    r"\b(buy|buys|buying|bought|bullish|bull|long|longs|longed|pump|pumps|pumping|moon|breakout|accumulat\w*|ath|"
    r"all[- ]time high|surge\w*|soar\w*|rally\w*|upside|reclaim\w*|withdr\w*)\b", re.I)
BEAR_EN = re.compile(
    r"\b(sell|sells|selling|sold|bearish|bear|short|shorts|shorted|dump\w*|crash\w*|breakdown|plunge\w*|"
    r"liquidat\w*|rekt|downside|rug\w*|capitulat\w*|deposit\w*)\b", re.I)
_SIDE_LOST = r"(?:got\s+|were\s+|are\s+|being\s+)?(?:liquidat\w*|rekt|wiped)"
SQUEEZE_BULL = re.compile(r"\b(?:short[- ]squeez\w*|shorts?\s+" + _SIDE_LOST + r"|short\s+liquidations?)\b|空头(?:被)?(?:爆仓|清算)", re.I)
SQUEEZE_BEAR = re.compile(r"\b(?:long[- ]squeez\w*|longs?\s+" + _SIDE_LOST + r"|long\s+liquidations?)\b|多头(?:被)?(?:爆仓|清算)", re.I)
BULL_ZH = ("做多", "看多", "买入", "加仓", "突破", "上涨", "暴涨", "新高", "抄底", "拉升", "增持", "提币", "吸筹", "看涨", "反弹")
BEAR_ZH = ("做空", "看空", "卖出", "减仓", "跌破", "下跌", "暴跌", "清算", "爆仓", "砸盘", "抛售", "出逃", "减持", "充值",
           "存入", "看跌", "回调", "割肉")


# ---------- text -> events -------------------------------------------------

def available_coins(price_dir: Path = PRICE_DIR) -> Dict[str, str]:
    """Base ticker -> perp directory name, e.g. PEPE -> 1000PEPE_USDT."""
    found: Dict[str, str] = {}
    for path in glob.glob(str(price_dir / "*_USDT" / "1h.parquet")):
        name = Path(path).parent.name
        base = name[: -len("_USDT")]
        found[base] = name
        if base.startswith("1000") and base[4:] not in found:
            found[base[4:]] = name
    return found


def extract_coins(text: str, available: Dict[str, str]) -> List[str]:
    coins: List[str] = []

    def add(base: str) -> None:
        base = base.upper()
        if base in STABLES or base not in available or base in coins:
            return
        coins.append(base)

    plain = URL_RE.sub(" ", text)
    for match in CASHTAG_RE.finditer(plain):
        add(match.group(1))
    for match in UPPER_RE.finditer(plain):
        if match.group(1) in MAJORS:
            add(match.group(1))
    lowered = plain.lower()
    for name, base in NAMES.items():
        if name in lowered:
            add(base)
    return coins


def direction(text: str) -> int:
    plain = URL_RE.sub(" ", text)
    # Liquidation phrases name the side that lost, so they point the other way: shorts liquidated = squeeze up.
    bull = len(SQUEEZE_BULL.findall(plain))
    bear = len(SQUEEZE_BEAR.findall(plain))
    plain = SQUEEZE_BEAR.sub(" ", SQUEEZE_BULL.sub(" ", plain))
    bull += len(BULL_EN.findall(plain)) + sum(plain.count(w) for w in BULL_ZH)
    bear += len(BEAR_EN.findall(plain)) + sum(plain.count(w) for w in BEAR_ZH)
    return int(np.sign(bull - bear))


def build_events(records: Iterable[Dict[str, Any]], available: Dict[str, str]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for rec in records:
        handle = str(rec.get("handle") or "")
        if rec.get("is_retweet") or not rec.get("created_at") or rec.get("author") not in ("", handle):
            continue
        reply_to = rec.get("in_reply_to_user")
        if reply_to and reply_to != handle:
            continue
        text = str(rec.get("text") or "")
        coins = extract_coins(text, available)
        if not coins:
            continue
        created = pd.Timestamp(rec["created_at"]).tz_convert("UTC")
        sign = direction(text)
        for coin in coins:
            events.append({
                "handle": handle, "tweet_id": rec["tweet_id"], "created": created, "coin": coin, "dir": sign,
                "n_coins": len(coins), "provenance": rec.get("provenance"),
                "entry": created.floor("h") + pd.Timedelta(hours=1),
            })
    events.sort(key=lambda e: (e["created"], e["handle"], e["coin"]))
    return events


def dedupe(events: List[Dict[str, Any]], by_handle: bool) -> List[Dict[str, Any]]:
    """Keep the earliest event per (coin[, handle]) within a rolling 24 hours."""
    last: Dict[Any, pd.Timestamp] = {}
    kept = []
    for event in events:
        key = (event["handle"], event["coin"]) if by_handle else event["coin"]
        if key in last and event["created"] - last[key] < DEDUP_WINDOW:
            continue
        last[key] = event["created"]
        kept.append(event)
    return kept


# ---------- prices ---------------------------------------------------------

def load_open(coin_dir: str, price_dir: Path = PRICE_DIR) -> pd.Series:
    frame = pd.read_parquet(price_dir / coin_dir / "1h.parquet", columns=["open"])
    index = pd.DatetimeIndex(frame.index)
    index = index.tz_localize("UTC") if index.tz is None else index.tz_convert("UTC")
    series = pd.Series(frame["open"].to_numpy(dtype=float), index=index).sort_index()
    return series[~series.index.duplicated()].asfreq("h")


def forward_excess(open_px: pd.Series, btc_px: pd.Series, horizon: int, is_btc: bool) -> pd.Series:
    """Unsigned return over `horizon` hours from each hourly open; minus BTC's for every coin except BTC."""
    ret = open_px.shift(-horizon) / open_px - 1.0
    if is_btc:
        return ret
    return ret - (btc_px.shift(-horizon) / btc_px - 1.0).reindex(ret.index)


def cost_of(coin: str) -> float:
    return 2.0 * (COST_BP_MAJOR if coin in ("BTC", "ETH") else COST_BP_OTHER) / 1e4


# ---------- the test -------------------------------------------------------

def holm(pvalues: Dict[str, float]) -> Dict[str, float]:
    ordered = sorted(pvalues.items(), key=lambda kv: kv[1])
    adjusted, running = {}, 0.0
    for rank, (name, p) in enumerate(ordered):
        running = max(running, min(1.0, (len(ordered) - rank) * p))
        adjusted[name] = running
    return adjusted


class Panel:
    """Unsigned forward excess returns on an (coin, day, hour) grid for one horizon."""

    def __init__(self, prices: Dict[str, pd.Series], btc: pd.Series, horizon: int, start: pd.Timestamp):
        self.horizon = horizon
        self.coins = sorted(prices)
        self.coin_index = {c: i for i, c in enumerate(self.coins)}
        self.start_day = start.floor("D")
        end = max(s.index[-1] for s in prices.values())
        self.days = pd.date_range(self.start_day, end.floor("D"), freq="D", tz="UTC")
        grid = np.full((len(self.coins), len(self.days), 24), np.nan)
        for coin, series in prices.items():
            xr = forward_excess(series, btc, horizon, coin == "BTC").dropna()
            xr = xr[xr.index >= self.start_day]
            if xr.empty:
                continue
            day = ((xr.index.floor("D") - self.start_day) / pd.Timedelta(days=1)).astype(int)
            grid[self.coin_index[coin], day, xr.index.hour] = xr.to_numpy()
        self.grid = grid

    def actual(self, event: Dict[str, Any]) -> float:
        day = int((event["entry"].floor("D") - self.start_day) / pd.Timedelta(days=1))
        if day < 0 or day >= len(self.days):
            return float("nan")
        return float(self.grid[self.coin_index[event["coin"]], day, event["entry"].hour])

    def valid_days(self, coins: List[str]) -> np.ndarray:
        """Days on which every given coin has any value (the random days the null may land on)."""
        idx = [self.coin_index[c] for c in coins]
        present = ~np.isnan(self.grid[idx]).all(axis=2)
        return np.flatnonzero(present.all(axis=0))


def evaluate(events: List[Dict[str, Any]], panel: Panel, *, signed: bool = True, hedge_cost: bool = False,
             rng: Optional[np.random.Generator] = None, resamples: int = N_RESAMPLES) -> Dict[str, Any]:
    """Mean net return of following the events, against the same coins on random days.

    The null keeps each event's coin, hour of day, direction and cost, and moves whole event
    days to random days together, so clustering of posts on the same market day is preserved.
    """
    rng = rng or np.random.default_rng(SEED)
    rows = []
    for event in events:
        value = panel.actual(event)
        if not np.isnan(value):
            rows.append((event, value))
    n = len(rows)
    if n == 0:
        return {"n": 0}
    sign = np.array([e["dir"] if signed else 1 for e, _ in rows], dtype=float)
    coins = np.array([panel.coin_index[e["coin"]] for e, _ in rows])
    hours = np.array([e["entry"].hour for e, _ in rows])
    costs = np.array([cost_of(e["coin"]) + (2 * BTC_LEG_BP / 1e4 if hedge_cost and e["coin"] != "BTC" else 0.0)
                      for e, _ in rows])
    actual = sign * np.array([v for _, v in rows]) - costs
    day_keys = np.array([e["entry"].floor("D").value for e, _ in rows])
    group_ids = np.unique(day_keys, return_inverse=True)[1]
    valid = panel.valid_days(sorted({e["coin"] for e, _ in rows}))
    if len(valid) < 3:
        return {"n": n, "mean": float(actual.mean()), "note": "too few valid null days"}
    draws = rng.choice(valid, size=(resamples, int(group_ids.max()) + 1))
    sampled = panel.grid[coins[None, :], draws[:, group_ids], hours[None, :]]
    null_vals = sign[None, :] * sampled - costs[None, :]
    null_means = np.nanmean(null_vals, axis=1)
    center = float(np.nanmean(null_means))
    obs = float(actual.mean())
    p_two = (1 + int((np.abs(null_means - center) >= abs(obs - center)).sum())) / (resamples + 1)
    ordered = np.sort(actual)
    trimmed = ordered[: max(1, int(len(ordered) * 0.95))]
    created = np.array([e["created"].value for e, _ in rows])
    first_half = actual[created <= np.median(created)]
    second_half = actual[created > np.median(created)]
    return {
        "n": n, "mean_net": obs, "null_mean": center, "excess_over_null": obs - center,
        "p_two_sided": float(p_two), "hit_rate": float((actual > 0).mean()),
        "median_net": float(np.median(actual)), "mean_without_best_5pct": float(trimmed.mean()),
        "half_means": [float(first_half.mean()) if len(first_half) else None,
                       float(second_half.mean()) if len(second_half) else None],
        "std": float(actual.std(ddof=1)) if n > 1 else None,
    }


def passes_protocol(result: Dict[str, Any], holm_p: float) -> Dict[str, Any]:
    halves = result.get("half_means") or [None, None]
    checks = {
        "holm_p<0.01": holm_p < 0.01,
        "net_mean>0": result.get("mean_net", -1) > 0,
        "excess_over_null>0": result.get("excess_over_null", -1) > 0,
        "both_halves_positive": all(h is not None and h > 0 for h in halves),
        f"n>={MIN_POOLED_EVENTS}": result.get("n", 0) >= MIN_POOLED_EVENTS,
        "mean_without_best_5pct>0": result.get("mean_without_best_5pct", -1) > 0,
    }
    return {"passes": all(checks.values()), "checks": checks}


# ---------- run ------------------------------------------------------------

def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(records: List[Dict[str, Any]], prices_for, available: Dict[str, str], *, resamples: int = N_RESAMPLES,
        only_backfill: bool = False) -> Dict[str, Any]:
    """`prices_for(base)` returns the hourly open Series of a coin (injected so tests can use synthetic data)."""
    events = build_events(records, available)
    if only_backfill:
        events = [e for e in events if e["provenance"] == "backfill"]
    directional = [e for e in events if e["dir"] != 0 and e["n_coins"] <= MAX_COINS_PER_POST]
    out: Dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "counts": {"posts_with_coin": len({(e["handle"], e["tweet_id"]) for e in events}), "events": len(events),
                   "directional_events": len(directional)},
    }
    if not events:
        return out
    needed = sorted({e["coin"] for e in events} | {"BTC"})
    prices = {coin: prices_for(coin) for coin in needed}
    btc = prices["BTC"]
    # One window for every handle and for the null, so the null days come from the market the posts came from.
    start = btc.index[-1].floor("D") - pd.Timedelta(days=WINDOW_DAYS)
    events = [e for e in events if e["entry"] >= start]
    directional = [e for e in directional if e["entry"] >= start]
    out["counts"].update({"events_in_window": len(events), "directional_in_window": len(directional)})
    if not events:
        return out
    out["span"] = {"window_start": str(start), "first_event": str(min(e["entry"] for e in events)),
                   "last_event": str(max(e["entry"] for e in events)), "price_end": str(btc.index[-1])}
    panels = {h: Panel(prices, btc, h, start) for h in (HORIZON_PRIMARY, *HORIZONS_EXPLORATORY)}
    rng = np.random.default_rng(SEED)

    pooled = dedupe(directional, by_handle=False)
    out["counts"]["pooled_independent_directional"] = len(pooled)
    primary = evaluate(pooled, panels[HORIZON_PRIMARY], rng=rng, resamples=resamples)
    out["primary"] = {"definition": "pooled, directional, 24h, BTC-adjusted, net of coin-leg cost", **primary}
    if primary.get("n", 0) and "p_two_sided" in primary:
        out["primary"]["verdict"] = passes_protocol(primary, primary["p_two_sided"])  # single primary test: no Holm family
        out["primary"]["with_btc_hedge_cost"] = evaluate(pooled, panels[HORIZON_PRIMARY], hedge_cost=True,
                                                         rng=np.random.default_rng(SEED), resamples=resamples)

    per_handle, pvals = {}, {}
    for handle in sorted({e["handle"] for e in directional}):
        subset = dedupe([e for e in directional if e["handle"] == handle], by_handle=True)
        result = evaluate(subset, panels[HORIZON_PRIMARY], rng=np.random.default_rng(SEED), resamples=resamples)
        per_handle[handle] = result
        if result.get("n", 0) >= MIN_HANDLE_EVENTS and "p_two_sided" in result:
            pvals[handle] = result["p_two_sided"]
    for handle, adjusted in holm(pvals).items():
        per_handle[handle]["holm_p"] = adjusted
    out["per_handle_24h"] = per_handle

    exploratory: Dict[str, Any] = {}
    for horizon in HORIZONS_EXPLORATORY:
        exploratory[f"pooled_directional_{horizon}h"] = evaluate(
            pooled, panels[horizon], rng=np.random.default_rng(SEED), resamples=resamples)
    mentions = dedupe(events, by_handle=False)
    exploratory["mention_only_long_24h"] = evaluate(
        mentions, panels[HORIZON_PRIMARY], signed=False, rng=np.random.default_rng(SEED), resamples=resamples)
    by_coin: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for event in directional:
        by_coin.setdefault(event["coin"], {}).setdefault(event["handle"], []).append(event)
    consensus = []
    for coin, per in by_coin.items():
        stream = sorted((e for evs in per.values() for e in evs), key=lambda e: e["created"])
        for i, event in enumerate(stream):
            recent = [x for x in stream[: i + 1] if event["created"] - x["created"] < DEDUP_WINDOW]
            if len({x["handle"] for x in recent}) >= 2:
                net = sum(x["dir"] for x in recent)
                if net != 0:
                    consensus.append({**event, "dir": int(np.sign(net))})
    consensus = dedupe(consensus, by_handle=False)
    exploratory["consensus_2plus_handles_24h"] = evaluate(
        consensus, panels[HORIZON_PRIMARY], rng=np.random.default_rng(SEED), resamples=resamples)
    out["exploratory"] = exploratory
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=ROOT / "reports" / "x_kol_research_20261025")
    parser.add_argument("--resamples", type=int, default=N_RESAMPLES)
    parser.add_argument("--handles", nargs="*", help="restrict to these handles (default all archived)")
    parser.add_argument("--placebo-shift-days", type=int, default=0,
                        help="move every post this many whole days in time: a run with no information, "
                             "used to exercise the code and check the false-positive rate")
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    from core.news.x_archive import load_archive

    records = load_archive(args.handles)
    if args.placebo_shift_days:
        shift = pd.Timedelta(days=args.placebo_shift_days)
        records = [{**r, "created_at": (pd.Timestamp(r["created_at"]) + shift).isoformat() if r.get("created_at") else None}
                   for r in records]
    available = available_coins()
    cache: Dict[str, pd.Series] = {}

    def prices_for(coin: str) -> pd.Series:
        if coin not in cache:
            cache[coin] = load_open(available[coin])
        return cache[coin]

    result = run(records, prices_for, available, resamples=args.resamples)
    result["archive"] = {
        "records": len(records),
        "by_handle": pd.Series([r["handle"] for r in records]).value_counts().to_dict() if records else {},
        "by_provenance": pd.Series([r.get("provenance") for r in records]).value_counts().to_dict() if records else {},
    }
    result["hashes"] = {
        "protocol_sha256": file_sha256(ROOT / "docs" / "X_KOL_RESEARCH_PROTOCOL.md"),
        "script_sha256": file_sha256(Path(__file__)),
    }
    result["placebo_shift_days"] = args.placebo_shift_days
    args.out.mkdir(parents=True, exist_ok=True)
    kind = f"placebo{args.placebo_shift_days:+d}d" if args.placebo_shift_days else "result"
    target = args.out / f"{kind}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    print(f"\nwritten: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
