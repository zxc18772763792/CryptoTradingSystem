"""Deeper look at fading @next10xgem1 long calls: entry timing, holding time, stops, take-profits.

Exploratory follow-up to docs/TELEGRAM_NEW_KOLS_RESEARCH_20261025.md. The 24h fade there is a
hypothesis the data suggested after the fact, so nothing here is a pre-registered test. To keep
the grid honest the calls are split in time before any rule is scored (design = first half,
test = second half), every rule is compared with the same rule started at random hours on the
same coin, and two outside samples are scored with the rules the design half picks:

* matched pumps: other Binance perps that had a similar 24h run-up at the same hour as a call
  (does the call add anything beyond "the coin already pumped"?);
* long history: every perp hour with a 24h run-up >= 15% from 2021-01 to the day before the
  channel sample starts (does "short a pump with a stop" work on years of data?).

Short leg: (entry - exit)/entry. Hourly bars: within a bar the stop is checked before the
take-profit, an open beyond a level fills at that open, a stop fill pays STOP_SLIP more, and a
high at 2x entry is a -100% liquidation (1x collateral). Cost 20bps round trip + actual funding.
Local data only; no network. Outputs: per-trade files in the restricted study folder, aggregates
in reports/telegram_next10xgem1_fade_20261026/.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.research_telegram_new_kols import STUDY, load_bars, sha, signal_rows  # noqa: E402
from scripts.review_telegram_signals import BASE_COST, SMALLCAP_COST, SymbolPanel, entry_hour, week_bootstrap  # noqa: E402

CHANNEL = "next10xgem1"
OUT = STUDY / "fade_deep_20261026"
REPORT = ROOT / "reports/telegram_next10xgem1_fade_20261026"
FUTURES = ROOT / "data/historical/binance_futures"
STOP_SLIP = .005             # extra adverse fill on a stop: small caps squeeze through levels
NULL_WINDOW = 30 * 24
MIN_NULL = 50
HOLDS = (6, 12, 24, 48, 72, 120, 168)
STOPS = (None, .05, .08, .10, .15, .20, .30, .50)
TAKES = (None, .05, .10, .20, .30)
TRAILS = (.10, .20)          # trailing stop above the lowest low so far; only without a fixed take
DELAYS = (0, 1, 2, 3, 4, 6, 8, 12, 24)
PULLBACKS = (.03, .05, .10)  # short at the first open x below the post-call high
EXTENSIONS = (.05, .10, .20)  # short at the first open y above the call-hour open
TRIGGER_WINDOW = 24
HISTORY_START = pd.Timestamp("2021-01-01", tz="UTC")
PUMP_THRESHOLD = .15
MIN_DESIGN_TRADES = 60
BOOK_FRACTION = .10          # notional per position in the book simulation
PATH_CACHE = 12


def config_name(hold: int, stop, take, trail) -> str:
    def pct(x):
        return "none" if x is None else f"{round(x * 100):d}"
    return f"h{hold}_stop{pct(stop)}_take{pct(take)}_trail{pct(trail)}"


def configs() -> list[dict]:
    grid = []
    for hold in HOLDS:
        for stop in STOPS:
            for take in TAKES:
                grid.append({"hold": hold, "stop": stop, "take": take, "trail": None})
            for trail in TRAILS:
                grid.append({"hold": hold, "stop": stop, "take": None, "trail": trail})
    for c in grid:
        c["name"] = config_name(c["hold"], c["stop"], c["take"], c["trail"])
    return grid


def entry_rules() -> list[dict]:
    rules = [{"kind": "delay", "x": d, "name": f"delay{d}h"} for d in DELAYS]
    rules += [{"kind": "pullback", "x": x, "name": f"pullback{round(x * 100)}"} for x in PULLBACKS]
    rules += [{"kind": "extension", "x": x, "name": f"extension{round(x * 100)}"} for x in EXTENSIONS]
    return rules


def short_paths(o, h, l, fund, known, hold: int, stop=None, take=None, trail=None,
                slip: float = STOP_SLIP, cost: float = BASE_COST):
    """Net short return of a trade opened at every bar's open (NaN where a bar it needs is missing).

    Returns (net, reason, bars): reason 0 = held to time, 1 = stop/trailing stop, 2 = take-profit,
    3 = liquidation; bars = how many hourly bars the position was open (funding settlements paid).
    A settlement stamped at an hour's open is paid by a position opened at that open, as in
    SymbolPanel.outcomes.
    """
    n = len(o)
    net = np.full(n, np.nan)
    reason = np.full(n, -1, dtype=np.int8)
    bars = np.zeros(n, dtype=np.int64)
    m = n - hold
    if m <= 0:
        return net, reason, bars
    e = o[:m]
    active = np.isfinite(e) & (e > 0)
    bad = ~active
    stop_px = e * (1 + stop) if stop is not None else np.full(m, np.inf)
    take_px = e * (1 - take) if take is not None else np.full(m, -np.inf)
    liq_px = 2 * e
    low_run = np.full(m, np.inf)
    exit_px = np.full(m, np.nan)
    why = np.zeros(m, dtype=np.int8)
    held = np.full(m, hold, dtype=np.int64)
    for k in range(hold):
        ok, hk, lk = o[k:k + m], h[k:k + m], l[k:k + m]
        missing = active & ~(np.isfinite(ok) & np.isfinite(hk) & np.isfinite(lk))
        bad |= missing
        active &= ~missing
        level = stop_px if trail is None else np.minimum(stop_px, low_run * (1 + trail))
        gap = active & (ok >= level) if k else np.zeros(m, dtype=bool)  # bar 0 opens at the entry
        liq = active & ~gap & (level > liq_px) & (hk >= liq_px)
        stopped = active & ~liq & (gap | (hk >= level))
        rest = active & ~liq & ~stopped
        take_gap = rest & (ok <= take_px) if k else np.zeros(m, dtype=bool)
        took = rest & (take_gap | (lk <= take_px))
        exit_px = np.where(liq, liq_px, exit_px)
        exit_px = np.where(stopped, np.where(gap, ok, level) * (1 + slip), exit_px)
        exit_px = np.where(took, np.where(take_gap, ok, take_px), exit_px)
        why = np.where(liq, 3, np.where(stopped, 1, np.where(took, 2, why)))
        done = liq | stopped | took
        held = np.where(done, k + 1, held)
        active &= ~done
        low_run = np.where(active, np.minimum(low_run, lk), low_run)
    final = o[hold:hold + m]
    bad |= active & ~np.isfinite(final)
    exit_px = np.where(active, final, exit_px)
    with np.errstate(invalid="ignore", divide="ignore"):
        gross = np.maximum(-1.0, (e - exit_px) / e)
    cum_f = np.concatenate([[0.0], np.cumsum(fund)])
    cum_k = np.concatenate([[0], np.cumsum(known, dtype=np.int64)])
    idx = np.arange(m)
    paid = cum_f[idx + held] - cum_f[idx]
    funded = cum_k[idx + held] - cum_k[idx] == held
    value = np.maximum(-1.0, gross - cost + paid)  # short receives positive funding
    ok_all = ~bad & funded & np.isfinite(value)
    net[:m] = np.where(ok_all, value, np.nan)
    reason[:m] = np.where(ok_all, why, -1)
    bars[:m] = held
    return net, reason, bars


def trigger_entries(o, h, kind: str, x, window: int = TRIGGER_WINDOW) -> np.ndarray:
    """Entry bar for a call whose first tradable open is bar s, for every s (-1 = never entered)."""
    n = len(o)
    s = np.arange(n)
    if kind == "delay":
        return np.where(s + int(x) < n, s + int(x), -1)
    out = np.full(n, -1, dtype=np.int64)
    run_high = np.full(n, -np.inf)
    for k in range(1, window + 1):
        prev = np.full(n, np.nan)
        prev[:n - k + 1] = h[k - 1:]
        run_high = np.fmax(run_high, prev)
        cand = np.full(n, np.nan)
        cand[:n - k] = o[k:]
        with np.errstate(invalid="ignore"):
            hit = cand <= run_high * (1 - x) if kind == "pullback" else cand >= o * (1 + x)
        out = np.where((out < 0) & hit, s + k, out)
    return out


class Coin:
    """One perp's hourly arrays, cut to the span the calls on it need, plus cached rule outputs."""

    def __init__(self, symbol: str, panel: SymbolPanel, lo: int, hi: int):
        self.symbol = symbol
        self.offset = lo
        self.index = panel.index[lo:hi]
        self.o, self.h, self.l = panel.open[lo:hi], panel.high[lo:hi], panel.low[lo:hi]
        self.fund, self.known = panel.fund[lo:hi], panel.funding_known[lo:hi]
        self._paths: dict[str, tuple] = {}
        self._entries: dict[str, np.ndarray] = {}

    def paths(self, cfg: dict):
        if cfg["name"] not in self._paths:
            if len(self._paths) >= PATH_CACHE:  # 392 exits x ~190 coins would not fit in memory
                self._paths.pop(next(iter(self._paths)))
            self._paths[cfg["name"]] = short_paths(self.o, self.h, self.l, self.fund, self.known, cfg["hold"],
                                                   cfg["stop"], cfg["take"], cfg["trail"])
        return self._paths[cfg["name"]]

    def entries(self, rule: dict) -> np.ndarray:
        if rule["name"] not in self._entries:
            self._entries[rule["name"]] = trigger_entries(self.o, self.h, rule["kind"], rule["x"])
        return self._entries[rule["name"]]

    def pos(self, at: pd.Timestamp) -> int | None:
        if len(self.index) == 0 or at < self.index[0] or at > self.index[-1]:
            return None
        return int((at - self.index[0]) / pd.Timedelta(hours=1))


def load_calls(study: Path = STUDY) -> tuple[list[dict], dict[str, Coin], Counter]:
    sealed = json.loads((study / "events_before_prices.json").read_text(encoding="utf-8"))
    status = Counter()
    raw = []
    for s in signal_rows(sealed):
        if s["channel"] != CHANNEL or s["group"] != "fresh_call" or s["late_edit"]:
            continue
        if s["venue"] != "futures" or s["side"] != 1:
            status[f"skipped_{s['venue']}_side{s['side']}"] += 1
            continue
        raw.append({**s, "base_at": entry_hour(s["available_at"])})
    coins: dict[str, Coin] = {}
    span = NULL_WINDOW + max(HOLDS) + max(max(DELAYS), TRIGGER_WINDOW) + 2
    for symbol in sorted({s["symbol"] for s in raw}):
        panel = SymbolPanel(*load_bars(symbol, "futures"))
        stamps = [s["base_at"] for s in raw if s["symbol"] == symbol]
        positions = [panel.position(t) for t in stamps]
        positions = [p for p in positions if p is not None]
        if not positions:
            continue
        lo, hi = max(0, min(positions) - span), min(len(panel.index), max(positions) + span)
        coins[symbol] = Coin(symbol, panel, lo, hi)
    calls = []
    for s in sorted(raw, key=lambda r: (r["base_at"], r["signal_id"])):
        coin = coins.get(s["symbol"])
        pos = coin.pos(s["base_at"]) if coin else None
        if pos is None or not np.isfinite(coin.o[pos]):
            status["no_price_at_entry"] += 1
            continue
        if pos < 24 or not np.isfinite(coin.o[pos - 24]):
            run_up = None
        else:
            run_up = float(coin.o[pos] / coin.o[pos - 24] - 1)
        calls.append({"signal_id": s["signal_id"], "symbol": s["symbol"], "kind": s["kind"],
                      "published_at": s["published_at"], "base_at": s["base_at"], "pos": pos, "run_up_24h": run_up})
        status["call"] += 1
    return calls, coins, status


def simulate(calls: list[dict], coins: dict[str, Coin], rule: dict, cfg: dict, with_null: bool = True) -> list[dict]:
    """One trade per call that enters, one open position per coin (a call while one is open is skipped)."""
    trades = []
    busy_until: dict[str, int] = {}
    reach = (rule["x"] if rule["kind"] == "delay" else TRIGGER_WINDOW) + cfg["hold"]
    for call in calls:
        coin = coins[call["symbol"]]
        pos = call["pos"]
        if pos < busy_until.get(call["symbol"], -1):
            continue
        entries = coin.entries(rule)
        net, reason, bars = coin.paths(cfg)
        t = int(entries[pos])
        if t < 0:
            busy_until[call["symbol"]] = pos + (TRIGGER_WINDOW if rule["kind"] != "delay" else 0)
            continue
        if not np.isfinite(net[t]):
            continue
        busy_until[call["symbol"]] = t + int(bars[t])
        row = {"signal_id": call["signal_id"], "symbol": call["symbol"], "base_at": call["base_at"],
               "entry_at": coin.index[t], "wait_h": t - pos, "net": float(net[t]), "reason": int(reason[t]),
               "bars": int(bars[t]), "run_up_24h": call["run_up_24h"]}
        if with_null:
            lo, hi = max(0, pos - NULL_WINDOW), min(len(coin.o), pos + NULL_WINDOW + 1)
            cand = np.arange(lo, hi)
            cand = cand[np.abs(cand - pos) >= reach]
            ent = entries[cand]
            ent = ent[ent >= 0]
            values = net[ent]
            values = values[np.isfinite(values)]
            if len(values) < MIN_NULL:
                continue
            row["null_mean"] = float(values.mean())
            row["null_entry_rate"] = float(len(ent) / max(1, len(cand)))
        trades.append(row)
    return trades


def summarise(trades: list[dict], split: pd.Timestamp | None = None) -> dict:
    if len(trades) < 2:
        return {"n": len(trades)}
    net = np.array([t["net"] for t in trades])
    stamps = [pd.Timestamp(t["entry_at"]) for t in trades]
    order = np.sort(net)
    out = {"n": len(trades), "mean": float(net.mean()), "median": float(np.median(net)),
           "hit": float((net > 0).mean()), "p05": float(np.quantile(net, .05)), "worst": float(order[0]),
           "best": float(order[-1]), "mean_without_best": float(order[:-1].mean()),
           "mean_without_best5": float(order[:-5].mean()) if len(order) > 10 else None,
           "mean_80bps": float((net - (SMALLCAP_COST - BASE_COST)).mean()),
           "week_ci": week_bootstrap(net, stamps),
           "reasons": dict(Counter({0: "time", 1: "stop", 2: "take", 3: "liquidated"}[t["reason"]] for t in trades)),
           "mean_bars": float(np.mean([t["bars"] for t in trades]))}
    if all("null_mean" in t for t in trades):
        excess = net - np.array([t["null_mean"] for t in trades])
        out.update({"null_mean": float(np.mean([t["null_mean"] for t in trades])), "excess": float(excess.mean()),
                    "excess_week_ci": week_bootstrap(excess, stamps)})
    if split is not None:
        for name, part in (("design", [t for t in trades if t["base_at"] < split]),
                           ("test", [t for t in trades if t["base_at"] >= split])):
            out[name] = summarise(part) if len(part) >= 2 else {"n": len(part)}
    return out


def event_path(calls: list[dict], coins: dict[str, Coin], before: int = 48, after: int = 168) -> dict:
    """Mean/median price relative to the first tradable open, around every call with a full path."""
    seg = []
    for call in calls:
        coin, pos = coins[call["symbol"]], call["pos"]
        if pos < before or pos + after >= len(coin.o):
            continue
        piece = coin.o[pos - before:pos + after + 1]
        if np.isfinite(piece).all():
            seg.append(piece / piece[before] - 1)
    seg = np.array(seg)
    hours = list(range(-before, after + 1))
    return {"n": int(len(seg)), "hours": hours, "mean": seg.mean(0).tolist(), "median": np.median(seg, 0).tolist(),
            "q25": np.quantile(seg, .25, 0).tolist(), "q75": np.quantile(seg, .75, 0).tolist()}


def excursions(calls: list[dict], coins: dict[str, Coin], hours: int = 24) -> dict:
    """Worst adverse (max high) and best favourable (min low) move within `hours` of a short entry."""
    up, down, when_low, final = [], [], [], []
    for call in calls:
        coin, pos = coins[call["symbol"]], call["pos"]
        if pos + hours >= len(coin.o):
            continue
        e = coin.o[pos]
        hh, ll = coin.h[pos:pos + hours], coin.l[pos:pos + hours]
        if not (np.isfinite(hh).all() and np.isfinite(ll).all()):
            continue
        if not np.isfinite(coin.o[pos + hours]):
            continue
        up.append(hh.max() / e - 1)
        down.append(1 - ll.min() / e)
        when_low.append(int(ll.argmin()))
        final.append(max(-1.0, 1 - coin.o[pos + hours] / e))
    up, down, final = np.array(up), np.array(down), np.array(final)
    return {"n": int(len(up)), "hours": hours,
            "adverse_quantiles": {q: float(np.quantile(up, q)) for q in (.25, .5, .75, .9, .95)},
            "favourable_quantiles": {q: float(np.quantile(down, q)) for q in (.25, .5, .75, .9, .95)},
            "share_adverse_ge": {s: float((up >= s).mean()) for s in (.05, .08, .10, .15, .20, .30, .50)},
            # Trades a stop at s would have closed: how they ended without it (gross short return).
            "touched_then": {s: {"n": int((up >= s).sum()), "won_anyway": float((final[up >= s] > 0).mean()),
                                 "mean_final": float(final[up >= s].mean())}
                             for s in (.05, .10, .15, .20, .30, .50) if (up >= s).sum() >= 5},
            "untouched_mean_final": {s: float(final[up < s].mean()) for s in (.05, .10, .15, .20, .30, .50)},
            "median_hour_of_low": float(np.median(when_low))}


def universe_symbols() -> list[str]:
    return sorted(p.name[:-5] for p in FUTURES.iterdir() if p.name.endswith("_USDT") and (p / "1h.parquet").exists())


def matched_pumps(calls: list[dict], cfgs: list[dict], start: pd.Timestamp, end: pd.Timestamp) -> dict:
    """Other perps with a similar 24h run-up at the call hour, shorted with the same rule at the same open."""
    by_hour: dict[pd.Timestamp, list[dict]] = {}
    for call in calls:
        if call["run_up_24h"] is not None:
            by_hour.setdefault(call["base_at"], []).append(call)
    called = {(c["base_at"], c["symbol"]) for c in calls}
    rows = {c["name"]: [] for c in cfgs}
    for symbol in universe_symbols():
        try:
            panel = SymbolPanel(*load_bars(symbol, "futures"))
        except (OSError, ValueError, KeyError):
            continue
        if len(panel.index) < 48 or panel.index[-1] < start or panel.index[0] > end:
            continue
        lo = panel.position(max(start, panel.index[0]))
        hi = panel.position(min(end, panel.index[-1])) + 1
        coin = Coin(symbol, panel, lo, hi)
        pairs = []
        for at, group in by_hour.items():
            p = coin.pos(at)
            if p is None or p < 24 or not (np.isfinite(coin.o[p]) and np.isfinite(coin.o[p - 24])):
                continue
            r = coin.o[p] / coin.o[p - 24] - 1
            for call in group:
                band = max(.03, .25 * call["run_up_24h"])
                if (at, symbol) not in called and call["run_up_24h"] >= .05 and abs(r - call["run_up_24h"]) <= band:
                    pairs.append((call["signal_id"], p))
        for cfg in cfgs:
            if not pairs:
                break
            net = coin.paths(cfg)[0]
            rows[cfg["name"]].extend({"signal_id": sid, "symbol": symbol, "net": float(net[p])}
                                     for sid, p in pairs if np.isfinite(net[p]))
        del coin, panel
    out = {}
    for name, items in rows.items():
        per_call: dict[str, list[float]] = {}
        for item in items:
            per_call.setdefault(item["signal_id"], []).append(item["net"])
        out[name] = {"control_trades": len(items), "calls_with_controls": len(per_call),
                     "per_call_means": {k: float(np.mean(v)) for k, v in per_call.items()}}
    return out


def pump_history(cfgs: list[dict], end: pd.Timestamp, threshold: float = PUMP_THRESHOLD) -> dict:
    """Every perp hour with o[t]/o[t-24]-1 >= threshold, shorted at that open; one position per coin."""
    trades = {c["name"]: [] for c in cfgs}
    for symbol in universe_symbols():
        try:
            panel = SymbolPanel(*load_bars(symbol, "futures"))
        except (OSError, ValueError, KeyError):
            continue
        if len(panel.index) < 48 or panel.index[0] > end:
            continue
        lo = panel.position(max(HISTORY_START, panel.index[0])) or 0
        hi = min(len(panel.index), (panel.position(min(end, panel.index[-1])) or 0) + max(HOLDS) + 2)
        coin = Coin(symbol, panel, max(0, lo - 24), hi)
        o = coin.o
        run = np.full(len(o), np.nan)
        with np.errstate(invalid="ignore", divide="ignore"):
            run[24:] = o[24:] / o[:-24] - 1
        candidates = np.where((run >= threshold) & (coin.index < end))[0]
        for cfg in cfgs:
            net, reason, bars = coin.paths(cfg)
            busy = -1
            for p in candidates:
                if p < busy or not np.isfinite(net[p]):
                    continue
                busy = p + int(bars[p])
                trades[cfg["name"]].append({"symbol": symbol, "entry_at": coin.index[p], "base_at": coin.index[p],
                                            "net": float(net[p]), "reason": int(reason[p]), "bars": int(bars[p]),
                                            "run_up_24h": float(run[p])})
        del coin, panel
    return trades


def book(trades: list[dict], coins: dict[str, Coin], fraction: float = BOOK_FRACTION) -> dict:
    """Fixed notional per position (fraction of starting equity), marked hourly at the bar high."""
    if not trades:
        return {}
    start = min(pd.Timestamp(t["entry_at"]) for t in trades)
    end = max(pd.Timestamp(t["entry_at"]) + pd.Timedelta(hours=t["bars"]) for t in trades)
    grid = pd.date_range(start, end, freq="h")
    marked = np.zeros(len(grid))
    realised = np.zeros(len(grid))
    concurrent = np.zeros(len(grid))
    for t in trades:
        coin = coins[t["symbol"]]
        p = coin.pos(pd.Timestamp(t["entry_at"]))
        e = coin.o[p]
        i = grid.get_loc(pd.Timestamp(t["entry_at"]))
        k = t["bars"]
        marks = np.maximum(-1.0, (e - coin.h[p:p + k]) / e) - BASE_COST / 2
        marked[i:i + k] += fraction * marks
        concurrent[i:i + k] += 1
        realised[min(len(grid) - 1, i + k):] += fraction * t["net"]
    # Open positions are marked at the bar's high (worst case for a short); closed ones at their net.
    equity = 1 + realised + marked
    peak = np.maximum.accumulate(equity)
    monthly = pd.Series(realised, index=grid).resample("MS").last().diff()
    first = pd.Series(realised, index=grid).resample("MS").last()
    if len(first):
        monthly.iloc[0] = first.iloc[0]
    return {"fraction": fraction, "final_realised": float(realised[-1]), "max_drawdown": float((equity / peak - 1).min()),
            "max_concurrent": int(concurrent.max()), "mean_concurrent": float(concurrent.mean()),
            "monthly_realised": {str(k.date()): float(v) for k, v in monthly.items()}}


def json_ready(value):
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def run(study: Path = STUDY, out: Path = OUT, report: Path = REPORT) -> dict:
    calls, coins, status = load_calls(study)
    times = sorted(c["base_at"] for c in calls)
    split = times[len(times) // 2]   # fixed before any rule is scored: median call time
    grid, rules = configs(), entry_rules()
    results = []
    for cfg in grid:            # exits outermost: the per-coin path cache holds a few exits only
        for rule in rules:
            trades = simulate(calls, coins, rule, cfg)
            results.append({"entry": rule["name"], "exit": cfg["name"], **summarise(trades, split)})
    table = pd.DataFrame([{k: v for k, v in r.items() if not isinstance(v, (dict, list))} |
                          {"design_mean": r.get("design", {}).get("mean"), "design_n": r.get("design", {}).get("n"),
                           "test_mean": r.get("test", {}).get("mean"), "test_n": r.get("test", {}).get("n"),
                           "design_excess": r.get("design", {}).get("excess"),
                           "test_excess": r.get("test", {}).get("excess")} for r in results])
    eligible = table[(table["design_n"] >= MIN_DESIGN_TRADES) & table["test_n"].notna()]
    ranked = eligible.sort_values("design_mean", ascending=False)
    top = ranked.head(20)
    pick = ranked.iloc[0]
    baseline = table[(table["entry"] == "delay0h") & (table["exit"] == config_name(24, None, None, None))].iloc[0]
    persistence = float(eligible[["design_mean", "test_mean"]].corr(method="spearman").iloc[0, 1])
    by_rule = {r["name"]: r for r in rules}
    by_cfg = {c["name"]: c for c in grid}
    chosen = [("baseline_24h", "delay0h", baseline["exit"]), ("design_pick", pick["entry"], pick["exit"])]
    for hold in (24, 72, 168):
        for stop in (.10, .20, .30):
            chosen.append((f"h{hold}_stop{round(stop * 100)}", "delay0h", config_name(hold, stop, None, None)))
    chosen_cfgs = {e: by_cfg[e] for _, _, e in chosen}
    detail = {}
    for label, entry, exit_ in chosen:
        trades = simulate(calls, coins, by_rule[entry], by_cfg[exit_])
        by_run = {}
        for name, lo, hi in (("run_up_lt5", -9, .05), ("run_up_5_15", .05, .15), ("run_up_15_30", .15, .30),
                             ("run_up_ge30", .30, 99)):
            part = [t for t in trades if t["run_up_24h"] is not None and lo <= t["run_up_24h"] < hi]
            by_run[name] = summarise(part)
        months = {}
        for t in trades:
            months.setdefault(str(pd.Timestamp(t["entry_at"]).to_period("M")), []).append(t)
        detail[label] = {"entry": entry, "exit": exit_, "summary": summarise(trades, split), "by_run_up": by_run,
                         "by_month": {m: summarise(v) for m, v in sorted(months.items())},
                         "book": book(trades, coins)}
    start, end = min(times) - pd.Timedelta(days=2), max(times) + pd.Timedelta(days=8)
    matched = matched_pumps(calls, list(chosen_cfgs.values()), start, end)
    matched_summary = {}
    for label, entry, exit_ in chosen:
        if entry != "delay0h":
            continue
        trades = simulate(calls, coins, by_rule[entry], by_cfg[exit_], with_null=False)
        per_call = matched[exit_]["per_call_means"]
        paired = [(t["net"], per_call[t["signal_id"]]) for t in trades if t["signal_id"] in per_call]
        if len(paired) >= 2:
            diff = np.array([a - b for a, b in paired])
            stamps = [pd.Timestamp(t["entry_at"]) for t in trades if t["signal_id"] in per_call]
            matched_summary[label] = {"calls": len(paired), "call_mean": float(np.mean([a for a, _ in paired])),
                                      "matched_mean": float(np.mean([b for _, b in paired])),
                                      "call_minus_matched": float(diff.mean()), "week_ci": week_bootstrap(diff, stamps),
                                      "control_trades": matched[exit_]["control_trades"]}
    history_end = min(times) - pd.Timedelta(hours=1)
    history = pump_history(list(chosen_cfgs.values()), history_end)
    history_summary = {}
    for label, entry, exit_ in chosen:
        if entry != "delay0h":
            continue
        trades = history[exit_]
        years = {}
        for t in trades:
            years.setdefault(str(pd.Timestamp(t["entry_at"]).year), []).append(t)
        history_summary[label] = {"exit": exit_, "summary": summarise(trades),
                                  "by_year": {y: summarise(v) for y, v in sorted(years.items())}}
    result = {
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "script_sha256": sha(Path(__file__).read_bytes()),
        "events_sha256": sha((study / "events_before_prices.json").read_bytes()),
        "status": dict(status), "calls": len(calls), "split_at": split,
        "first_call": times[0], "last_call": times[-1],
        "grid": {"entries": [r["name"] for r in rules], "exits": len(grid), "combinations": len(results)},
        "rules": {"short": "(entry-exit)/entry, 1x collateral (high >= 2x entry = -100%)",
                  "bars": "hourly; stop before take-profit in a bar; open beyond a level fills at the open",
                  "stop_slip": STOP_SLIP, "cost": BASE_COST, "stress_cost": SMALLCAP_COST,
                  "null": "same rule from random hourly starts within +/-30d, excluding the call's own reach",
                  "dedup": "one open position per coin; a call while one is open is skipped",
                  "split": "design = calls before the median call time, test = after; fixed before scoring",
                  "matched": "other perps at the call hour with |run-up - call run-up| <= max(3pp, 25%), call run-up >= 5%",
                  "history": f"perp hours with 24h run-up >= {PUMP_THRESHOLD:.0%}, {HISTORY_START.date()} to the day before the sample"},
        "event_path": event_path(calls, coins),
        "excursions_24h": excursions(calls, coins, 24),
        "excursions_72h": excursions(calls, coins, 72),
        "baseline": baseline.to_dict(),
        "design_pick": pick.to_dict(),
        "top20_design": {"mean_design": float(top["design_mean"].mean()), "mean_test": float(top["test_mean"].mean()),
                         "rows": top[["entry", "exit", "n", "design_mean", "test_mean", "excess"]].to_dict("records")},
        "design_test_spearman": persistence,
        "share_positive_test_among_eligible": float((eligible["test_mean"] > 0).mean()),
        "detail": detail, "matched_pumps": matched_summary, "pump_history": history_summary,
    }
    out.mkdir(parents=True, exist_ok=True)
    table.to_csv(out / "grid.csv", index=False, encoding="utf-8-sig")
    for label, entry, exit_ in chosen:
        pd.DataFrame(simulate(calls, coins, by_rule[entry], by_cfg[exit_])).to_csv(
            out / f"trades_{label}.csv", index=False, encoding="utf-8-sig")
    (out / "results.json").write_text(json.dumps(json_ready(result), ensure_ascii=False, indent=2, default=str),
                                      encoding="utf-8")
    report.mkdir(parents=True, exist_ok=True)
    aggregate = {k: v for k, v in result.items() if k not in ("detail",)}
    aggregate["detail"] = {k: {kk: vv for kk, vv in v.items()} for k, v in detail.items()}
    (report / "summary.json").write_text(json.dumps(json_ready(aggregate), ensure_ascii=False, indent=2, default=str),
                                         encoding="utf-8")
    grid_public = table[["entry", "exit", "n", "mean", "median", "hit", "p05", "worst", "mean_without_best",
                         "mean_80bps", "excess", "design_n", "design_mean", "test_n", "test_mean"]]
    grid_public.to_csv(report / "grid.csv", index=False, encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--study", type=Path, default=STUDY)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    result = run(args.study)
    print(json.dumps(json_ready({k: result[k] for k in ("calls", "split_at", "baseline", "design_pick",
                                                         "top20_design", "design_test_spearman", "matched_pumps")}),
                     ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
