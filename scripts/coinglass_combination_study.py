"""Do Coinglass positioning signals work combined, or as filters on existing strategies? (2026-10-11)

WHY THIS EXISTS:
scripts/coinglass_positioning_study.py (docs/COINGLASS_POSITIONING_STUDY_2026-10-11.md) tested
eight signals one at a time: liquidation shocks and OI build-ups carry small, robust information
but none pays its trading costs alone. The user asked for combinations, a multi-factor model and
filter use. Same data, panel and timing as that study (the cached daily Coinglass history of the
131-coin universe).

PRE-REGISTERED before the first run. The single-signal results were already known, so every
choice below is mechanical; nothing is picked by looking at them.

A. Multi-factor composite of the same eight signals
   - Each day each signal becomes a centred cross-sectional rank score in (-0.5, 0.5].
   - Weights for horizon h (1, 3, 7 days): each signal's mean daily IC at h over the TRAINING days
     only (before 2025-05-22). Composite = weighted sum of the scores a coin has, divided by the
     sum of |weights| of those scores; a coin needs at least half the total |weight|.
     Secondary: training-IC signs with equal magnitude.
   - Tested on the TEST days only (from 2025-05-22): mean daily IC, week-block bootstrap 90% CI,
     whole-coin permutation (1000 reps). Economics at 0.12% per round trip, both reported:
       plain     top-minus-bottom quintile rebalanced every h days (averaged over start days);
       buffered  checked daily: a coin enters the long (short) book in the top (bottom) quintile
                 and leaves when it is no longer in the top (bottom) 40%.
   - Pass: IC > 0 with the CI excluding 0, permutation p < 0.05/3, and a positive net per day
     for the plain or the buffered book.
B. Five classic positioning patterns (an event = a coin-day meeting all conditions on day D):
     P1 crowded longs   3d return top 30%, 3d OI change top 30%, funding_z > 1    -> weaker
     P2 short build-up  3d return bottom 30%, 3d OI change top 30%, funding_z < -1 -> stronger
     P3 long flush      liq imbalance > 0.5, liq_shock_z > 1.5, 1d OI change < 0   -> stronger
     P4 short flush     liq imbalance < -0.5, liq_shock_z > 1.5, 1d OI change < 0  -> weaker
     P5 retail chasing  retail_z > 1 and top_z < -1                                 -> weaker
   Outcome: the coin's forward return minus that day's cross-sectional mean, h = 1, 3, 7.
   Statistics: mean over events; week-block bootstrap CI; null from permuting whole coins' event
   series, 1000 reps. Economics: minus 0.24% per event (the coin and a market hedge, round trips).
   Pass: >= 100 events on >= 30 days, the expected sign, CI excluding 0, p < 0.05/15, positive
   after 0.24%, the same sign in both halves.
C. Filters on existing strategies
   C1 the frozen daily cross-sectional reversal rule (core/research/xs_reversal_tracker.py),
      replayed exactly as scripts/xs_reversal_backtest.py does on the local 1h store and the cached
      funding. Coinglass bar D is used at the 00:00 formation that ends day D.
        F1 drop the day's top quintile of liq_shock_z from the long candidates;
           judged on all overlapping days, both halves reported;
        F2 drop the composite's (1d training weights) bottom quintile from the long candidates and
           its top quintile from the short candidates; judged on test days only.
      Metric: daily net per position (the backtest's unit), filtered minus unfiltered, week-block
      bootstrap 90% CI. Pass: difference > 0 with the CI excluding 0 (F1: and > 0 in both halves).
   C2 the autonomous agent's closed paper trades: net return of the entries a filter would have
      blocked (F1: longs on a top-quintile liq_shock_z coin; F2: longs on a bottom-quintile or
      shorts on a top-quintile composite coin, using the bar of the day before entry) versus the
      rest. Descriptive only: about 130 short-lived trades cannot carry a verdict.

    python scripts/coinglass_combination_study.py            # all parts (C1 needs the local 1h store)
    python scripts/coinglass_combination_study.py --skip-c1  # without the reversal replay
    python scripts/coinglass_combination_study.py --robustness   # post-hoc checks below

POST-HOC, added after the first run (they qualify the verdict, never change it): the returns above
are price only. A long-short perp book held for days also pays or earns funding, and the reversal
backtest's funding cache turned out to hold settlements for 35 of its 131 perps. --robustness
fetches Binance's actual funding for every perp (cached in funding_binance.json here) and reports,
for the composite books: net with funding, net at 0.20% per round trip, each quarter of the test
days, and each leg; and the reversal filter F1 again with the complete funding.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts import coinglass_positioning_study as single  # noqa: E402
from scripts.agent_call_edge import day_bootstrap_ci  # noqa: E402
from scripts.lsr_positioning_study import row_ic  # noqa: E402

OUT_DIR = single.OUT_DIR
SPLIT = single.SPLIT
HORIZONS = single.HORIZONS
SIGNALS = single.SIGNALS
MIN_COINS = single.MIN_COINS
COST = single.COST_ROUND_TRIP
HEDGED_COST = 2 * COST
N_PERM = 1000
COMPOSITE_ALPHA = 0.05 / len(HORIZONS)
MIN_EVENTS, MIN_EVENT_DAYS = 100, 30
ENTER_Q, EXIT_Q = 0.2, 0.4
PATTERNS: Dict[str, int] = {"P1_crowded_longs": -1, "P2_short_buildup": +1, "P3_long_flush": +1,
                            "P4_short_flush": -1, "P5_retail_chasing": -1}
PATTERN_ALPHA = 0.05 / (len(PATTERNS) * len(HORIZONS))


# ------------------------------------------------------------------------------------- features
def add_features(panel: Dict[str, pd.DataFrame], frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """past_3d and the 1-day OI change, on the panel's day grid (both known at D+1 00:00)."""
    out = dict(panel)
    log_00 = np.log(frames["open_00"].where(frames["open_00"] > 0))
    oi = np.log(frames["oi"].where(frames["oi"] > 0))
    index = panel["fwd_1d"].index
    out["past_3d"] = (log_00.shift(-1) - log_00.shift(2)).reindex(index)
    out["oi_change_1d"] = (oi - oi.shift(1)).reindex(index)
    return out


def rank_score(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.rank(axis=1, pct=True) - 0.5


def training_weights(panel: Dict[str, pd.DataFrame], horizon: int, split: pd.Timestamp = SPLIT) -> Dict[str, float]:
    label = panel[f"fwd_{horizon}d"]
    return {name: float(row_ic(panel[name][panel[name].index < split], label[label.index < split]).mean())
            for name in SIGNALS}


def composite(panel: Dict[str, pd.DataFrame], weights: Dict[str, float]) -> pd.DataFrame:
    total = sum(abs(w) for w in weights.values())
    num, den = None, None
    for name, w in weights.items():
        score = rank_score(panel[name])
        part, have = (score * w).fillna(0.0), score.notna() * abs(w)
        num = part if num is None else num + part
        den = have if den is None else den + have
    return (num / den).where(den >= 0.5 * total)


# ------------------------------------------------------------------------------------ portfolios
def buffered_book(score: pd.DataFrame, fwd_1d: pd.DataFrame, cost: float = COST) -> Dict[str, float]:
    """Daily check: enter long in the top ENTER_Q, leave once outside the top EXIT_Q (shorts mirrored)."""
    longs: Set[str] = set()
    shorts: Set[str] = set()
    gross, net, turnover = [], [], []
    for t in score.index:
        s, f = score.loc[t], fwd_1d.loc[t]
        ok = s.notna() & f.notna()
        if ok.sum() < MIN_COINS:
            longs, shorts = set(), set()
            continue
        pct = s[ok].rank(pct=True)
        new_longs = {c for c in longs if c in pct.index and pct[c] > 1 - EXIT_Q} | set(pct.index[pct > 1 - ENTER_Q])
        new_shorts = {c for c in shorts if c in pct.index and pct[c] <= EXIT_Q} | set(pct.index[pct <= ENTER_Q])
        if not new_longs or not new_shorts:
            continue
        turn_long = len(new_longs - longs) / len(new_longs)
        turn_short = len(new_shorts - shorts) / len(new_shorts)
        spread = float(f[list(new_longs)].mean() - f[list(new_shorts)].mean())
        gross.append(spread)
        net.append(spread - (turn_long + turn_short) * cost)
        turnover.append((turn_long + turn_short) / 2)
        longs, shorts = new_longs, new_shorts
    if not gross:
        return {"days": 0, "gross_per_day": float("nan"), "net_per_day": float("nan"), "turnover": float("nan")}
    return {"days": len(gross), "gross_per_day": float(np.mean(gross)), "net_per_day": float(np.mean(net)),
            "turnover": float(np.mean(turnover))}


# ------------------------------------------------------------------------------------------ part A
def part_a(panel: Dict[str, pd.DataFrame], reps: int = N_PERM) -> List[Dict[str, Any]]:
    rows = []
    test = panel["fwd_1d"].index >= SPLIT
    for h in HORIZONS:
        label = panel[f"fwd_{h}d"]
        weights = training_weights(panel, h)
        for variant, w in (("ic_weighted", weights), ("sign_equal", {k: float(np.sign(v)) for k, v in weights.items()})):
            score = composite(panel, w)
            ic, ci, days = single._ic_ci(score[test], label[test])
            perm = single.coin_permutation(score[test], label[test], reps=reps)
            plain = single.quintile_spread(score[test], label[test], direction=1.0, horizon=h)
            buffered = buffered_book(score[test], panel["fwd_1d"][test])
            passes = (ic > 0 and ci[0] > 0 and perm["p"] < COMPOSITE_ALPHA
                      and max(plain["net_per_day"], buffered["net_per_day"]) > 0)
            rows.append({"horizon": h, "variant": variant, "weights": w, "test_days": days, "ic": ic, "ci": ci,
                         "perm_p": perm["p"], "plain": plain, "buffered": buffered,
                         "passes": bool(passes) if variant == "ic_weighted" else None})
    return rows


# ------------------------------------------------------------------------------------------ part B
def pattern_events(panel: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    pct = lambda f: f.rank(axis=1, pct=True)  # noqa: E731
    p3, oi3 = pct(panel["past_3d"]), pct(panel["oi_change_3d"])
    fz, imb, shock = panel["funding_z"], panel["liq_imbalance_1d"], panel["liq_shock_z"]
    oi1, rz, tz = panel["oi_change_1d"], panel["retail_z"], panel["top_z"]
    return {
        "P1_crowded_longs": (p3 >= 0.7) & (oi3 >= 0.7) & (fz > 1),
        "P2_short_buildup": (p3 <= 0.3) & (oi3 >= 0.7) & (fz < -1),
        "P3_long_flush": (imb > 0.5) & (shock > 1.5) & (oi1 < 0),
        "P4_short_flush": (imb < -0.5) & (shock > 1.5) & (oi1 < 0),
        "P5_retail_chasing": (rz > 1) & (tz < -1),
    }


def relative(fwd: pd.DataFrame) -> pd.DataFrame:
    return fwd.sub(fwd.mean(axis=1), axis=0)


def event_values(events: pd.DataFrame, rel: pd.DataFrame) -> Tuple[np.ndarray, pd.DatetimeIndex]:
    """Relative returns of the event coin-days and the day of each."""
    rel = rel.reindex(index=events.index, columns=events.columns)
    values = rel.to_numpy(float)
    mask = events.to_numpy(bool) & ~np.isnan(values)
    rows, _ = np.nonzero(mask)
    return values[mask], events.index[rows]


def event_mean(events: pd.DataFrame, rel: pd.DataFrame) -> float:
    values, _ = event_values(events, rel)
    return float(values.mean()) if len(values) else float("nan")


def event_permutation(events: pd.DataFrame, rel: pd.DataFrame, reps: int = N_PERM, seed: int = 0) -> float:
    observed = event_mean(events, rel)
    rng = np.random.default_rng(seed)
    cols = list(events.columns)
    null = []
    for _ in range(reps):
        shuffled = events.copy()
        shuffled.columns = [cols[i] for i in rng.permutation(len(cols))]
        null.append(event_mean(shuffled[cols], rel))
    null_arr = np.array([v for v in null if v == v])
    return float((np.sum(np.abs(null_arr) >= abs(observed)) + 1) / (len(null_arr) + 1))


def part_b(panel: Dict[str, pd.DataFrame], reps: int = N_PERM) -> List[Dict[str, Any]]:
    rows = []
    for name, events in pattern_events(panel).items():
        expected = PATTERNS[name]
        for h in HORIZONS:
            rel = relative(panel[f"fwd_{h}d"])
            values, days = event_values(events, rel)
            n, n_days = int(len(values)), int(days.nunique())
            mean = float(values.mean()) if n else float("nan")
            ci = day_bootstrap_ci(pd.Series(values), week_of(days)) if n else (float("nan"), float("nan"))
            first = event_mean(events[events.index < SPLIT], rel[rel.index < SPLIT])
            second = event_mean(events[events.index >= SPLIT], rel[rel.index >= SPLIT])
            p = event_permutation(events, rel, reps=reps) if n else float("nan")
            net = expected * mean - HEDGED_COST if n else float("nan")
            passes = (n >= MIN_EVENTS and n_days >= MIN_EVENT_DAYS and np.sign(mean) == expected
                      and (ci[0] > 0 or ci[1] < 0) and p < PATTERN_ALPHA and net > 0
                      and np.sign(first) == expected and np.sign(second) == expected)
            rows.append({"pattern": name, "expected": expected, "horizon": h, "events": n, "days": n_days,
                         "mean_rel": mean, "ci": ci, "perm_p": p, "net_after_cost": net,
                         "first_half": first, "second_half": second, "passes": bool(passes)})
    return rows


def week_of(index: pd.Index) -> pd.Series:
    iso = pd.DatetimeIndex(index).isocalendar()
    return pd.Series((iso["year"] * 100 + iso["week"]).to_numpy())


# ------------------------------------------------------------------------------------------ part C
def _base_to_perp() -> Dict[str, str]:
    return json.loads(single.PERP_SYMBOLS.read_text(encoding="utf-8"))


def _quintile_sets(row: pd.Series) -> Tuple[Set[str], Set[str]]:
    row = row.dropna()
    if len(row) < MIN_COINS:
        return set(), set()
    pct = row.rank(pct=True)
    return set(pct.index[pct <= 0.2]), set(pct.index[pct > 0.8])


def reversal_replay(panel: Dict[str, pd.DataFrame], score_1d: pd.DataFrame,
                    funding: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """The frozen reversal backtest day by day, unfiltered and with F1/F2 (net per position).
    Funding defaults to the backtest's own cache, as pre-registered."""
    from core.research import xs_reversal_tracker as rule
    from scripts.xs_reversal_backtest import FUNDING_CACHE, load_local_1h

    closes = asyncio.run(load_local_1h())
    perps = _base_to_perp()
    closes = {b: s for b, s in closes.items() if b in perps}
    prices = pd.DataFrame(closes).sort_index()
    if funding is None:
        funding = json.loads(FUNDING_CACHE.read_text(encoding="utf-8")) if FUNDING_CACHE.exists() else {}
    # cumulative funding per perp: a window sum is two binary searches instead of a scan
    cum_funding = {}
    for perp, rows in funding.items():
        rows = sorted((int(t), float(r)) for t, r in rows)
        cum_funding[perp] = (np.array([t for t, _ in rows], dtype=np.int64), np.concatenate([[0.0], np.cumsum([r for _, r in rows])]))
    shock = panel["liq_shock_z"]
    days = []
    for entry_ts in prices.index[prices.index.hour == 23]:
        exit_ts = entry_ts + pd.Timedelta(days=1)
        day = entry_ts.floor("D")  # Coinglass bar D closes at this formation (00:00 after the 23:00 bar)
        if exit_ts not in prices.index or day not in shock.index:
            continue
        signal = rule.reversal_signal(prices.loc[:entry_ts].tail(rule.SIGNAL_BARS))
        entry, exit_ = prices.loc[entry_ts], prices.loc[exit_ts]
        valid = signal.dropna().index.intersection(entry.dropna().index).intersection(exit_.dropna().index)
        base = rule.pick_legs(signal.loc[valid])
        if base is None:
            continue
        perp_of = {b: perps[b] for b in valid}
        by_perp = {p: b for b, p in perp_of.items()}
        _, top_shock = _quintile_sets(shock.loc[day].reindex(list(by_perp)))
        bottom_score, top_score = _quintile_sets(score_1d.loc[day].reindex(list(by_perp))) if day in score_1d.index else (set(), set())
        to_base = lambda perp_set: {by_perp[p] for p in perp_set}  # noqa: E731
        lo_ms, hi_ms = int(entry_ts.timestamp() * 1000) + 3_600_000, int(exit_ts.timestamp() * 1000) + 3_600_000
        ret = exit_ / entry - 1.0

        def fund(b: str) -> float:
            times, cum = cum_funding.get(perps[b], (np.array([], dtype=np.int64), np.array([0.0])))
            # settlements with lo_ms < t <= hi_ms, as in scripts/xs_reversal_backtest.py
            return float(cum[np.searchsorted(times, hi_ms, side="right")] - cum[np.searchsorted(times, lo_ms, side="right")])

        def net(longs: List[str], shorts: List[str]) -> float:
            long_leg = float(np.mean([ret[b] - fund(b) for b in longs]))
            short_leg = float(np.mean([-ret[b] + fund(b) for b in shorts]))
            return (long_leg + short_leg) / 2 - rule.ROUND_TRIP_COST

        def legs_without(drop_long: Set[str], drop_short: Set[str]) -> Optional[Tuple[List[str], List[str]]]:
            longs = rule.pick_legs(signal.loc[[b for b in valid if b not in drop_long]])
            shorts = rule.pick_legs(signal.loc[[b for b in valid if b not in drop_short]])
            return (longs[0], shorts[1]) if longs and shorts else None

        record = {"date": day, "base": net(*base)}
        f1 = legs_without(to_base(top_shock), set())
        record["f1"] = net(*f1) if f1 else np.nan
        f2 = legs_without(to_base(bottom_score), to_base(top_score)) if bottom_score else None
        record["f2"] = net(*f2) if f2 else np.nan
        days.append(record)
    return pd.DataFrame(days).set_index("date")


def filter_verdicts(replay: pd.DataFrame) -> List[Dict[str, Any]]:
    rows = []
    for name, window in (("F1", replay.index >= replay.index.min()), ("F2", replay.index >= SPLIT)):
        diff = (replay[name.lower()] - replay["base"])[window].dropna()
        if diff.empty:
            rows.append({"filter": name, "days": 0})
            continue
        lo, hi = day_bootstrap_ci(diff, week_of(diff.index).set_axis(diff.index))
        first = float(diff[diff.index < SPLIT].mean()) if (diff.index < SPLIT).any() else float("nan")
        second = float(diff[diff.index >= SPLIT].mean())
        passes = diff.mean() > 0 and lo > 0 and (name == "F2" or (first > 0 and second > 0))
        rows.append({"filter": name, "days": int(len(diff)), "base_net": float(replay["base"][window].mean()),
                     "filtered_net": float(replay[name.lower()][window].mean()), "diff": float(diff.mean()),
                     "ci": (lo, hi), "first_half": first, "second_half": second, "passes": bool(passes)})
    return rows


def agent_filter(panel: Dict[str, pd.DataFrame], score_1d: pd.DataFrame) -> List[Dict[str, Any]]:
    from scripts.agent_trade_attribution import load_trades

    trades = load_trades()
    perps = _base_to_perp()
    rows = []
    for _, t in trades.iterrows():
        perp = perps.get(str(t["symbol"]).split("/")[0])
        day = pd.Timestamp(t["opened"]).floor("D") - pd.Timedelta(days=1)  # the last bar complete at entry
        if not perp or day not in panel["liq_shock_z"].index:
            continue
        shock_pct = panel["liq_shock_z"].loc[day].rank(pct=True).get(perp, np.nan)
        score_pct = score_1d.loc[day].rank(pct=True).get(perp, np.nan) if day in score_1d.index else np.nan
        long = t["side"] == "long"
        rows.append({"ret_net": float(t["ret_net"]),
                     "f1_blocked": bool(long and shock_pct > 0.8),
                     "f2_blocked": bool((long and score_pct <= 0.2) or (not long and score_pct > 0.8))})
    frame = pd.DataFrame(rows)
    out = []
    for name in ("f1_blocked", "f2_blocked"):
        if frame.empty:
            break
        blocked, kept = frame[frame[name]], frame[~frame[name]]
        out.append({"filter": name, "trades": int(len(frame)), "blocked": int(len(blocked)),
                    "blocked_mean": float(blocked["ret_net"].mean()) if len(blocked) else float("nan"),
                    "kept_mean": float(kept["ret_net"].mean()) if len(kept) else float("nan")})
    return out


# ------------------------------------------------------------------------------------------ report
def _f(x: float, d: int = 4) -> str:
    return "   n/a" if x != x else f"{x:+.{d}f}"


def report(a: List[Dict[str, Any]], b: List[Dict[str, Any]], c1: List[Dict[str, Any]], c2: List[Dict[str, Any]]) -> str:
    lines = [f"A. composite (weights from days before {SPLIT:%Y-%m-%d}; test days only) | pass: IC>0, CI excl. 0, "
             f"p<{COMPOSITE_ALPHA:.4f}, plain or buffered net>0"]
    for r in a:
        lines.append(f"  {r['horizon']}d {r['variant']:12s} IC {_f(r['ic'])} [{_f(r['ci'][0])}, {_f(r['ci'][1])}] p {r['perm_p']:.4f}"
                     f" | plain gross/net {_f(r['plain']['gross_per_day'] * 100, 3)}%/{_f(r['plain']['net_per_day'] * 100, 3)}%"
                     f" | buffered gross/net {_f(r['buffered']['gross_per_day'] * 100, 3)}%/{_f(r['buffered']['net_per_day'] * 100, 3)}%"
                     f" turnover {r['buffered']['turnover']:.2f}" + ("" if r["passes"] is None else f" | {'PASS' if r['passes'] else 'no'}"))
    lines.append("  1d training weights: " + ", ".join(f"{k} {v:+.4f}" for k, v in a[0]["weights"].items()))
    lines.append(f"B. patterns (forward return minus the day's cross-sectional mean) | pass: >= {MIN_EVENTS} events on >= "
                 f"{MIN_EVENT_DAYS} days, expected sign, CI excl. 0, p<{PATTERN_ALPHA:.4f}, net of {HEDGED_COST:.2%} > 0, both halves")
    for r in b:
        lines.append(f"  {r['pattern']:18s} {r['horizon']}d events {r['events']:5d}/{r['days']:4d}d mean {_f(r['mean_rel'] * 100, 3)}% "
                     f"[{_f(r['ci'][0] * 100, 3)}, {_f(r['ci'][1] * 100, 3)}] p {r['perm_p']:.4f} net {_f(r['net_after_cost'] * 100, 3)}% "
                     f"halves {_f(r['first_half'] * 100, 3)}/{_f(r['second_half'] * 100, 3)} {'PASS' if r['passes'] else 'no'}")
    lines.append("C1. reversal rule with filters (net per position per day; difference = filtered - unfiltered)")
    for r in c1:
        if not r.get("days"):
            lines.append(f"  {r['filter']}: no days")
            continue
        lines.append(f"  {r['filter']} days {r['days']} base {_f(r['base_net'] * 100, 3)}% filtered {_f(r['filtered_net'] * 100, 3)}% "
                     f"diff {_f(r['diff'] * 100, 3)}% [{_f(r['ci'][0] * 100, 3)}, {_f(r['ci'][1] * 100, 3)}] "
                     f"halves {_f(r['first_half'] * 100, 3)}/{_f(r['second_half'] * 100, 3)} {'PASS' if r['passes'] else 'no'}")
    lines.append("C2. agent trades (descriptive, not a verdict)")
    for r in c2:
        lines.append(f"  {r['filter']}: {r['blocked']}/{r['trades']} blocked, mean net {_f(r['blocked_mean'] * 100, 3)}% "
                     f"vs kept {_f(r['kept_mean'] * 100, 3)}%")
    return "\n".join(lines)


# ------------------------------------------------------------------------------------ post-hoc
FUNDING_PATH = OUT_DIR / "funding_binance.json"


def fetch_binance_funding(perps: List[str], start_ms: int, path: Path = FUNDING_PATH) -> Dict[str, List[List[float]]]:
    """Every funding settlement of each perp since start_ms (resumable; retries instead of giving up)."""
    import time

    import httpx

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)
    cache: Dict[str, List[List[float]]] = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    failed: List[str] = []
    with httpx.Client(timeout=30, trust_env=True) as client:
        for perp in perps:
            rows = cache.get(perp) or []
            cursor = max([start_ms] + [int(r[0]) + 1 for r in rows])
            while cursor < time.time() * 1000 - 3_600_000:
                page = None
                for attempt in range(6):
                    try:
                        resp = client.get(f"{single.FAPI}/fapi/v1/fundingRate",
                                          params={"symbol": perp, "startTime": cursor, "limit": 1000})
                        if resp.status_code == 200:
                            page = resp.json()
                            break
                        if resp.status_code == 400:  # unknown symbol: nothing to fetch
                            page = []
                            break
                    except Exception:  # noqa: BLE001 - the proxy drops connections now and then
                        pass
                    time.sleep(2 * (attempt + 1))
                if page is None:  # every attempt failed: never price this perp without funding
                    failed.append(perp)
                    break
                if not page:
                    break
                rows += [[int(r["fundingTime"]), float(r["fundingRate"])] for r in page]
                cursor = int(page[-1]["fundingTime"]) + 1
                time.sleep(0.2)
            cache[perp] = rows
            path.write_text(json.dumps(cache), encoding="utf-8")
    if failed:
        raise RuntimeError(f"funding fetch failed for {len(failed)} perps (progress kept, rerun): {failed[:10]}")
    return cache


def daily_funding(cache: Dict[str, List[List[float]]], index: pd.DatetimeIndex, columns: List[str]) -> pd.DataFrame:
    """F[D, perp]: funding settled in (D+1 00:00, D+2 00:00], the first day held after bar D."""
    out = {}
    for perp in columns:
        rows = cache.get(perp) or []
        if not rows:
            continue
        settle = pd.Series([r[1] for r in rows], index=pd.to_datetime([int(r[0]) for r in rows], unit="ms", utc=True))
        # a settlement at exactly 00:00 belongs to the day that ends there
        day = (settle.index - pd.Timedelta(milliseconds=1)).floor("D") - pd.Timedelta(days=1)
        out[perp] = settle.groupby(day).sum()
    return pd.DataFrame(out).reindex(index=index, columns=columns).fillna(0.0)


def funded_books(score: pd.DataFrame, fwd: pd.DataFrame, fwd_1d: pd.DataFrame, funding: pd.DataFrame, h: int,
                 cost: float) -> Dict[str, Any]:
    """The plain h-day and the buffered daily book, each net of costs AND funding (long pays, short earns)."""
    held = funding.rolling(h).sum().shift(-(h - 1))  # funding over days D+1 .. D+h
    plain_net, plain_fund, long_leg, short_leg, quarters = [], [], [], [], []
    days = list(score.index)
    for offset in range(h):
        prev = None
        for t in days[offset::h]:
            s, f = score.loc[t], fwd.loc[t]
            ok = s.notna() & f.notna()
            if ok.sum() < MIN_COINS:
                prev = None
                continue
            ranked = s[ok].sort_values(kind="mergesort")
            k = len(ranked) // 5
            short, long_ = list(ranked.index[:k]), list(ranked.index[-k:])
            fund = float(held.loc[t, long_].mean() - held.loc[t, short].mean())
            turn = 1.0 if prev is None else 0.5 * (len(set(long_) - prev[0]) + len(set(short) - prev[1])) / k
            spread = float(f[long_].mean() - f[short].mean())
            net = spread - fund - 2 * turn * cost
            plain_net.append(net / h)
            plain_fund.append(fund / h)
            long_leg.append(float(f[long_].mean() - fwd.loc[t][ok].mean()) / h)
            short_leg.append(float(fwd.loc[t][ok].mean() - f[short].mean()) / h)
            quarters.append(pd.Timestamp(t).tz_localize(None).to_period("Q"))
            prev = (set(long_), set(short))
    by_quarter = pd.Series(plain_net, index=quarters).groupby(level=0).mean()
    buffered_fund = []
    longs, shorts = set(), set()
    buffered_net = []
    for t in score.index:
        s, f = score.loc[t], fwd_1d.loc[t]
        ok = s.notna() & f.notna()
        if ok.sum() < MIN_COINS:
            longs, shorts = set(), set()
            continue
        pct = s[ok].rank(pct=True)
        new_longs = {c for c in longs if c in pct.index and pct[c] > 1 - EXIT_Q} | set(pct.index[pct > 1 - ENTER_Q])
        new_shorts = {c for c in shorts if c in pct.index and pct[c] <= EXIT_Q} | set(pct.index[pct <= ENTER_Q])
        if not new_longs or not new_shorts:
            continue
        turn = len(new_longs - longs) / len(new_longs) + len(new_shorts - shorts) / len(new_shorts)
        fund = float(funding.loc[t, list(new_longs)].mean() - funding.loc[t, list(new_shorts)].mean())
        buffered_net.append(float(f[list(new_longs)].mean() - f[list(new_shorts)].mean()) - fund - turn * cost)
        buffered_fund.append(fund)
        longs, shorts = new_longs, new_shorts
    return {"plain_net": float(np.mean(plain_net)), "plain_funding": float(np.mean(plain_fund)),
            "long_leg": float(np.mean(long_leg)), "short_leg": float(np.mean(short_leg)),
            "plain_by_quarter": {str(q): float(v) for q, v in by_quarter.items()},
            "buffered_net": float(np.mean(buffered_net)), "buffered_funding": float(np.mean(buffered_fund))}


def robustness(panel: Dict[str, pd.DataFrame]) -> Tuple[str, Dict[str, Any]]:
    perps = list(panel["fwd_1d"].columns)
    cache = fetch_binance_funding(perps, single._ms(single.START) - 2 * 86_400_000)
    covered = sum(bool(cache.get(p)) for p in perps)
    test = panel["fwd_1d"].index >= SPLIT
    funding = daily_funding(cache, panel["fwd_1d"].index, perps)
    lines = [f"post-hoc (not the verdict): Binance funding for {covered}/{len(perps)} perps; test days only; "
             "per day; long pays funding, short earns it"]
    for h in (3, 7):
        weights = training_weights(panel, h)
        for variant, w in (("ic_weighted", weights), ("sign_equal", {k: float(np.sign(v)) for k, v in weights.items()})):
            score = composite(panel, w)[test]
            fwd, fwd_1d, fund = panel[f"fwd_{h}d"][test], panel["fwd_1d"][test], funding[test]
            at_cost = funded_books(score, fwd, fwd_1d, fund, h, COST)
            at_high = funded_books(score, fwd, fwd_1d, fund, h, 0.002)
            quarters = ", ".join(f"{q} {v * 100:+.3f}%" for q, v in at_cost["plain_by_quarter"].items())
            lines.append(f"  {h}d {variant:12s} plain net incl. funding {at_cost['plain_net'] * 100:+.3f}% "
                         f"(funding {-at_cost['plain_funding'] * 100:+.3f}%; at 0.20% cost {at_high['plain_net'] * 100:+.3f}%) | "
                         f"buffered {at_cost['buffered_net'] * 100:+.3f}% (funding {-at_cost['buffered_funding'] * 100:+.3f}%; "
                         f"at 0.20% {at_high['buffered_net'] * 100:+.3f}%) | legs long {at_cost['long_leg'] * 100:+.3f}% "
                         f"short {at_cost['short_leg'] * 100:+.3f}%")
            lines.append(f"      quarters (plain, incl. funding): {quarters}")
    return "\n".join(lines), cache


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--reps", type=int, default=N_PERM)
    parser.add_argument("--skip-c1", action="store_true", help="skip the reversal replay (needs the local 1h store)")
    parser.add_argument("--robustness", action="store_true", help="post-hoc: funding, higher costs, quarters, legs")
    args = parser.parse_args(argv)
    frames = single.build_panel(single.load_cached())
    panel = add_features(single.build_signals(frames), frames)
    if args.robustness:
        text, cache = robustness(panel)
        score_1d = composite(panel, training_weights(panel, 1))
        if not args.skip_c1:
            replay = reversal_replay(panel, score_1d, funding=cache)
            for r in filter_verdicts(replay):
                text += (f"\n  reversal {r['filter']} with complete funding: base {r['base_net'] * 100:+.3f}% filtered "
                         f"{r['filtered_net'] * 100:+.3f}% diff {r['diff'] * 100:+.3f}% "
                         f"[{r['ci'][0] * 100:+.3f}, {r['ci'][1] * 100:+.3f}] halves {r['first_half'] * 100:+.3f}/"
                         f"{r['second_half'] * 100:+.3f}")
        print(text)
        (OUT_DIR / "combination_robustness.txt").write_text(text, encoding="utf-8")
        return 0
    a = part_a(panel, reps=args.reps)
    b = part_b(panel, reps=args.reps)
    score_1d = composite(panel, training_weights(panel, 1))
    c1 = [] if args.skip_c1 else filter_verdicts(reversal_replay(panel, score_1d))
    c2 = agent_filter(panel, score_1d)
    text = report(a, b, c1, c2)
    print(text)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "combination_results.json").write_text(json.dumps(
        {"generated_at": datetime.now(timezone.utc).isoformat(), "report": text, "a": a, "b": b, "c1": c1, "c2": c2},
        indent=1, default=lambda v: list(v) if isinstance(v, tuple) else float(v)), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
