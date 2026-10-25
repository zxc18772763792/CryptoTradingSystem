"""Second-round KOL study: a coin x day panel. Implements docs/X_KOL_RESEARCH_PROTOCOL.md section 8.

Round one treated each post as an event and compared it with random days of the same coin. Here every
liquid altcoin on every day is a row, "mentioned yesterday by tracked accounts" is the signal, and the coins
nobody mentioned are the control. Same posts, a second way of testing them, so the protocol counts both rounds'
tests together. Constants below are part of the protocol; changing one after seeing a result makes the result
exploratory.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import research_x_kol as kol  # noqa: E402  (same folder; reused for coin extraction and direction)

WINDOW_DAYS = 90
MIN_DOLLAR_VOLUME = 20e6          # 30-day mean daily dollar volume, up to the day before the signal day
VOLUME_LOOKBACK = 30
MIN_COVERING_HANDLES = 3          # a day needs at least this many accounts with data, or one account decides it
MIN_CONTROL_COINS = 5
MIN_DAYS = 60
LEG_COST_PER_SIDE = 0.0008
SPREAD_COST = 4 * LEG_COST_PER_SIDE   # two legs, in and out, every day
FLIPS = 20000
SEED = 20261025


def load_daily(coin_dir: str, since: pd.Timestamp, price_dir: Path = kol.PRICE_DIR) -> Optional[pd.DataFrame]:
    """Daily 00:00 UTC open and daily dollar volume from the 1h bars."""
    frame = pd.read_parquet(price_dir / coin_dir / "1h.parquet", columns=["open", "close", "volume"])
    index = pd.DatetimeIndex(frame.index)
    frame.index = index.tz_localize("UTC") if index.tz is None else index.tz_convert("UTC")
    frame = frame[frame.index >= since]
    if frame.empty:
        return None
    midnight = frame.loc[frame.index.hour == 0, "open"]
    out = pd.DataFrame({"open": midnight.groupby(midnight.index.floor("D")).first()})
    out["dvol"] = (frame["close"] * frame["volume"]).groupby(frame.index.floor("D")).sum()
    return out


def coverage_starts(records: List[Dict[str, Any]]) -> Dict[str, pd.Timestamp]:
    """First full day with data per handle: its 3rd-oldest post, so one pinned old post cannot stretch it."""
    stamps: Dict[str, List[pd.Timestamp]] = {}
    for rec in records:
        if rec.get("created_at"):
            stamps.setdefault(str(rec["handle"]), []).append(pd.Timestamp(rec["created_at"]).tz_convert("UTC"))
    return {h: sorted(v)[min(2, len(v) - 1)].ceil("D") for h, v in stamps.items()}


def signal_tables(events: List[Dict[str, Any]], cover: Dict[str, pd.Timestamp], days: pd.DatetimeIndex,
                  coins: List[str]) -> Dict[str, Any]:
    """S1 (distinct covering accounts mentioning the coin the day before) and S2 (summed direction) per day t."""
    s1 = pd.DataFrame(0.0, index=days, columns=coins)
    s2 = pd.DataFrame(0.0, index=days, columns=coins)
    seen = set()
    for event in events:
        day = event["created"].floor("D")
        handle = event["handle"]
        if handle not in cover or day < cover[handle] or event["coin"] not in s1.columns:
            continue
        t = day + pd.Timedelta(days=1)
        if t not in s1.index:
            continue
        if (t, handle, event["coin"]) not in seen:
            seen.add((t, handle, event["coin"]))
            s1.at[t, event["coin"]] += 1.0
        if event["dir"] and event["n_coins"] <= kol.MAX_COINS_PER_POST:
            s2.at[t, event["coin"]] += float(event["dir"])
    covering = pd.Series({t: sum(1 for c in cover.values() if c <= t - pd.Timedelta(days=1)) for t in days})
    return {"s1": s1, "s2": s2, "covering": covering}


def spread_series(fwd: pd.DataFrame, universe: pd.DataFrame, group_a: pd.DataFrame, group_b: pd.DataFrame,
                  days_ok: pd.Series, min_b: int = MIN_CONTROL_COINS) -> pd.Series:
    out = {}
    for t in fwd.index:
        if not days_ok.get(t, False):
            continue
        ok = universe.loc[t]
        a = fwd.loc[t][ok & group_a.loc[t]]
        b = fwd.loc[t][ok & group_b.loc[t]]
        if len(a) >= 1 and len(b) >= min_b:
            out[t] = float(a.mean() - b.mean())
    return pd.Series(out, dtype=float)


def test_spread(series: pd.Series, rng: np.random.Generator, flips: int = FLIPS) -> Dict[str, Any]:
    n = len(series)
    if n < 5:
        return {"days": n, "note": "too few days"}
    values = series.to_numpy()
    mean, std = float(values.mean()), float(values.std(ddof=1))
    se = std / np.sqrt(n)
    signs = rng.choice([-1.0, 1.0], size=(flips, n))
    flipped = (signs * values[None, :]).mean(axis=1)
    p = (1 + int((np.abs(flipped) >= abs(mean)).sum())) / (flips + 1)
    order = np.argsort(series.index.values)
    half = n // 2
    return {
        "days": n, "mean_gross": mean, "mean_net": mean - SPREAD_COST, "std": std, "se": float(se),
        "t": float(mean / se) if se > 0 else None, "p_two_sided": float(p),
        "halves_gross": [float(values[order][:half].mean()), float(values[order][half:].mean())],
        "min_detectable_gross_80pct_power": float(2.8 * se),
        "cost_per_day": SPREAD_COST,
    }


def holm(pvalues: Dict[str, float]) -> Dict[str, float]:
    ordered = sorted(pvalues.items(), key=lambda kv: kv[1])
    adjusted, running = {}, 0.0
    for rank, (name, p) in enumerate(ordered):
        running = max(running, min(1.0, (len(ordered) - rank) * p))
        adjusted[name] = running
    return adjusted


def analyze(events: List[Dict[str, Any]], cover: Dict[str, pd.Timestamp], opens: pd.DataFrame, dvol: pd.DataFrame,
            *, window_days: int = WINDOW_DAYS, flips: int = FLIPS, seed: int = SEED) -> Dict[str, Any]:
    """opens / dvol: days x coins (UTC midnight index, base tickers); must contain a BTC column."""
    btc = opens["BTC"]
    coins = [c for c in opens.columns if c != "BTC" and c not in kol.STABLES]
    nxt = opens.shift(-1)
    fwd = ((nxt[coins] / opens[coins] - 1.0).sub(nxt["BTC"] / btc - 1.0, axis=0)).iloc[:-1]
    last_day = fwd.dropna(how="all").index.max()
    days = fwd.index[(fwd.index > last_day - pd.Timedelta(days=window_days)) & (fwd.index <= last_day)]
    fwd = fwd.loc[days]
    liquid = dvol[coins].shift(1).rolling(VOLUME_LOOKBACK, min_periods=VOLUME_LOOKBACK).mean() >= MIN_DOLLAR_VOLUME
    universe = liquid.loc[days] & fwd.notna()
    # Remove each coin's own average over the window. KOLs keep naming the same few coins, and those were the strong
    # ones this quarter, so without this the panel measures "named coins did better" (a coin effect), not timing.
    fwd = fwd.where(universe)
    fwd = fwd - fwd.mean(axis=0)
    tables = signal_tables(events, cover, days, coins)
    s1, s2, covering = tables["s1"], tables["s2"], tables["covering"]
    days_ok = covering >= MIN_COVERING_HANDLES
    out: Dict[str, Any] = {
        "window": [str(days.min().date()), str(days.max().date())],
        "days_with_enough_accounts": int(days_ok.sum()),
        "universe_coins_per_day_median": float(universe.sum(axis=1).median()),
        "mentioned_coin_days": int(((s1 >= 1) & universe).to_numpy().sum()),
        "tests": {},
    }
    # T2 compares two mentioned groups, so its second group only needs one coin; T1/T3 need a real control group.
    specs = {
        "T1_mentioned_vs_not": ((s1 >= 1), (s1 == 0), MIN_CONTROL_COINS),
        "T2_net_positive_vs_negative": ((s2 > 0), (s2 < 0), 1),
        "T3_two_plus_accounts_vs_not": ((s1 >= 2), (s1 == 0), MIN_CONTROL_COINS),
    }
    for name, (a, b, min_b) in specs.items():
        series = spread_series(fwd, universe, a, b, days_ok, min_b)
        out["tests"][name] = test_spread(series, np.random.default_rng(seed), flips)
    adj = holm({k: v["p_two_sided"] for k, v in out["tests"].items() if "p_two_sided" in v})
    for name, value in adj.items():
        result = out["tests"][name]
        result["holm_p"] = value
        halves = result["halves_gross"]
        checks = {
            "holm_p<0.01": value < 0.01,
            "net_mean>0": result["mean_net"] > 0,
            "both_halves_positive_gross": all(h > 0 for h in halves),
            f"days>={MIN_DAYS}": result["days"] >= MIN_DAYS,
        }
        result["verdict"] = {"passes": all(checks.values()), "checks": checks}
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=ROOT / "reports" / "x_kol_research_20261025")
    parser.add_argument("--flips", type=int, default=FLIPS)
    parser.add_argument("--placebo-shift-days", type=int, default=0, help="shift every post in time (no-information run)")
    parser.add_argument("--provenance", choices=["all", "live"], default="all",
                        help="live = only posts the collector saw in real time (the forward sample); added after the "
                             "2026-10-25 run, analysis logic unchanged")
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    from core.news.x_archive import FROZEN_COHORT, load_archive

    records = load_archive(FROZEN_COHORT)  # later additions to the polled list stay out of the frozen cohort
    if args.provenance == "live":
        records = [r for r in records if r.get("provenance") == "live"]
        if not records:
            print(json.dumps({"note": "no live-provenance posts archived yet (the news worker or a poller must run first)"}))
            return 0
    if args.placebo_shift_days:
        shift = pd.Timedelta(days=args.placebo_shift_days)
        records = [{**r, "created_at": (pd.Timestamp(r["created_at"]) + shift).isoformat() if r.get("created_at") else None}
                   for r in records]
    available = kol.available_coins()
    events = kol.build_events(records, available)
    cover = coverage_starts(records)
    columns = {b: d for b, d in available.items() if not (b.startswith("1000") and b[4:] in available)}
    since = pd.Timestamp.now(tz="UTC").floor("D") - pd.Timedelta(days=WINDOW_DAYS + VOLUME_LOOKBACK + 15)
    opens, dvol = {}, {}
    for base, coin_dir in columns.items():
        daily = load_daily(coin_dir, since)
        if daily is not None and len(daily) > 20:
            opens[base], dvol[base] = daily["open"], daily["dvol"]
    result = analyze(events, cover, pd.DataFrame(opens), pd.DataFrame(dvol), flips=args.flips)
    result["created_at"] = datetime.now(timezone.utc).isoformat()
    result["placebo_shift_days"] = args.placebo_shift_days
    result["archive_records"] = len(records)
    result["hashes"] = {
        "protocol_sha256": hashlib.sha256((ROOT / "docs" / "X_KOL_RESEARCH_PROTOCOL.md").read_bytes()).hexdigest(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    args.out.mkdir(parents=True, exist_ok=True)
    kind = f"panel_placebo{args.placebo_shift_days:+d}d" if args.placebo_shift_days else "panel_result"
    target = args.out / f"{kind}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    print(f"\nwritten: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
