"""Tradability backtest: short the perp around large token unlocks.

Builds on scripts/unlock_event_study.py (run it first: events.csv and
cliff_dates.json in data/research/unlock_study/). The event study showed unlock
tokens lagging calendar/age-matched controls by -5.2% over t-30..t+30 (-17% for
cliffs >= 10% of supply). This asks whether that survives real instruments:

* short the token's Binance USDT-M perpetual (it must exist at entry; 1000x
  contracts are handled), real 8h funding received/paid, 0.2% round-trip per leg;
* hedges: none / BTC perp / equal-weight basket of the other study perps with no
  cliff within +/-45 days of the unlock (schedules are public, so the basket is
  known at entry);
* optional stop on the short leg using daily highs (intraday gaps unmodelled);
* one trade per event, returns on the short notional; 90% CIs bootstrap over
  entry months; overlap reported so per-trade means are not read as annual.

Usage:
  python scripts/unlock_short_backtest.py
  python scripts/unlock_short_backtest.py --min-size 10 --entry -30 --exit -1
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUT = PROJECT_ROOT / "data" / "research" / "unlock_study"
FAPI = "https://fapi.binance.com/fapi/v1"
LEG_COST = 0.002


def _session():
    import requests

    from config.settings import settings

    s = requests.Session()
    proxy = settings.HTTPS_PROXY or settings.HTTP_PROXY
    if proxy:
        s.proxies = {"https": proxy, "http": proxy}
    return s


def _get(s, path: str, params: Dict) -> Optional[list]:
    for attempt in range(3):
        try:
            r = s.get(f"{FAPI}/{path}", params=params, timeout=30)
            time.sleep(0.15)
            if r.status_code == 200 and isinstance(r.json(), list):
                return r.json()
            if r.status_code == 400:
                return None  # unknown symbol
        except Exception:  # noqa: BLE001
            time.sleep(2 * (attempt + 1))
    return None


def perp_data(s, token: str, start_ms: int) -> Optional[Dict[str, pd.Series]]:
    """Daily close/high and 8h funding for TOKENUSDT or 1000TOKENUSDT, cached."""
    cache = OUT / "perps" / f"{token}.json"
    if cache.exists():
        raw = json.loads(cache.read_text(encoding="utf-8"))
        if raw is None:
            return None
    else:
        raw = None
        for symbol in (f"{token}USDT", f"1000{token}USDT"):
            bars, cursor = [], start_ms
            while True:
                batch = _get(s, "klines", {"symbol": symbol, "interval": "1d", "startTime": cursor, "limit": 1500})
                if not batch:
                    break
                bars += batch
                if len(batch) < 1500:
                    break
                cursor = batch[-1][0] + 86_400_000
            if not bars:
                continue
            funding, cursor = [], bars[0][0]
            while True:
                batch = _get(s, "fundingRate", {"symbol": symbol, "startTime": cursor, "limit": 1000})
                if not batch:
                    break
                funding += batch
                if len(batch) < 1000:
                    break
                cursor = int(batch[-1]["fundingTime"]) + 1
            raw = {"symbol": symbol, "bars": [[b[0], float(b[2]), float(b[4])] for b in bars],
                   "funding": [[int(f["fundingTime"]), float(f["fundingRate"])] for f in funding]}
            break
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(raw), encoding="utf-8")
        if raw is None:
            return None
    idx = pd.to_datetime([b[0] for b in raw["bars"]], unit="ms", utc=True)
    return {
        "close": pd.Series([b[2] for b in raw["bars"]], index=idx),
        "high": pd.Series([b[1] for b in raw["bars"]], index=idx),
        "funding": pd.Series([f[1] for f in raw["funding"]], index=pd.to_datetime([f[0] for f in raw["funding"]], unit="ms", utc=True), dtype=float),
    }


def leg_return(d: Dict[str, pd.Series], entry: pd.Timestamp, exit_: pd.Timestamp, side: int, stop: Optional[float]):
    """Return on notional for a long (+1) or short (-1) leg from entry close to exit close."""
    close = d["close"]
    if entry not in close.index or exit_ not in close.index:
        return None
    p0 = close[entry]
    end, stopped = exit_, False
    if stop is not None and side < 0:
        window = d["high"][(d["high"].index > entry) & (d["high"].index <= exit_)]
        hit = window[window >= p0 * (1 + stop)]
        if len(hit):
            end, stopped = hit.index[0], True
    p1 = p0 * (1 + stop) * 1.02 if stopped else close[end]
    fund = d["funding"][(d["funding"].index > entry) & (d["funding"].index <= end + pd.Timedelta(days=1))].sum()
    price_ret = side * (p1 / p0 - 1)
    worst = (d["high"][(d["high"].index > entry) & (d["high"].index <= end)].max() / p0 - 1) if side < 0 else np.nan
    return {"ret": price_ret - side * fund - LEG_COST, "funding": -side * fund, "stopped": stopped, "worst_up": worst, "end": end}


def month_boot(values: pd.Series, months: pd.Series, reps: int = 2000) -> tuple:
    frame = pd.DataFrame({"v": values.to_numpy(), "m": months.to_numpy()}).dropna()
    if len(frame) < 5:
        return (np.nan, np.nan)
    groups = [g["v"].to_numpy() for _, g in frame.groupby("m")]
    rng = np.random.default_rng(0)
    means = [np.concatenate([groups[i] for i in rng.integers(0, len(groups), len(groups))]).mean() for _ in range(reps)]
    return tuple(np.percentile(means, [5, 95]))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--min-size", type=float, default=1.0)
    parser.add_argument("--entry", type=int, default=-30, help="entry day relative to unlock (close of that day)")
    parser.add_argument("--exit", type=int, default=30)
    parser.add_argument("--stop", type=float, default=None, help="e.g. 0.4 = stop the short at +40%%")
    args = parser.parse_args(argv)

    events = pd.read_csv(OUT / "events.csv", parse_dates=["date"])
    events["date"] = pd.to_datetime(events["date"], utc=True)
    events = events[events["size_pct"] >= args.min_size]
    cliffs = {t: pd.to_datetime(v, utc=True) for t, v in json.loads((OUT / "cliff_dates.json").read_text(encoding="utf-8")).items()}
    s = _session()
    start_ms = int(pd.Timestamp("2022-06-01", tz="UTC").timestamp() * 1000)
    perps = {t: perp_data(s, t, start_ms) for t in sorted(set(cliffs) | set(events["token"]))}
    perps = {t: d for t, d in perps.items() if d is not None}
    btc = perp_data(s, "BTC", start_ms)
    print(f"tokens with a Binance perp: {len(perps)} of {len(set(cliffs) | set(events['token']))}")

    rows = []
    for ev in events.itertuples(index=False):
        d = perps.get(ev.token)
        if d is None:
            continue
        entry = (ev.date + pd.Timedelta(days=args.entry)).normalize()
        exit_ = (ev.date + pd.Timedelta(days=args.exit)).normalize()
        short = leg_return(d, entry, exit_, -1, args.stop)
        if short is None:
            continue  # perp did not exist at entry, or delisted before exit
        rec = {"token": ev.token, "date": ev.date, "entry": entry, "size_pct": ev.size_pct, "short": short["ret"],
               "funding": short["funding"], "stopped": short["stopped"], "worst_up": short["worst_up"]}
        close_at = short["end"]  # a stopped short closes its hedge the same day
        b = leg_return(btc, entry, close_at, +1, None)
        rec["btc_hedged"] = short["ret"] + b["ret"] if b else np.nan  # equal notional long BTC, per short notional
        basket = []
        for t, other in perps.items():
            if t == ev.token or any(abs((ev.date - c).days) <= 45 for c in cliffs.get(t, [])):
                continue
            leg = leg_return(other, entry, close_at, +1, None)
            if leg is not None:
                basket.append(leg["ret"])
        rec["basket_n"] = len(basket)
        rec["basket_hedged"] = short["ret"] + float(np.mean(basket)) if len(basket) >= 5 else np.nan
        rows.append(rec)

    R = pd.DataFrame(rows)
    R.to_csv(OUT / f"short_backtest_{args.entry}_{args.exit}_{args.min_size:g}.csv", index=False)
    span_days = (R["entry"].max() - R["entry"].min()).days or 1
    concurrency = len(R) * (args.exit - args.entry) / span_days
    print(f"trades: {len(R)} on {R['token'].nunique()} tokens | window t{args.entry:+d}..t{args.exit:+d} | "
          f"min size {args.min_size:g}% | stop {args.stop} | avg concurrent positions ~{concurrency:.1f}")

    def report(sub: pd.DataFrame, label: str) -> None:
        if len(sub) < 10:
            return
        months = sub["entry"].dt.strftime("%Y-%m")
        print(f"\n== {label}: n={len(sub)}")
        for col in ("short", "btc_hedged", "basket_hedged"):
            x = sub[col].dropna()
            lo, hi = month_boot(x, months.loc[x.index])
            print(f"  {col:14s} mean {x.mean() * 100:+6.2f}%  median {x.median() * 100:+6.2f}%  win {(x > 0).mean():.0%}  "
                  f"worst {x.min() * 100:+.0f}%  90%CI [{lo * 100:+.2f}, {hi * 100:+.2f}]")
        print(f"  funding received by the short: mean {sub['funding'].mean() * 100:+.2f}% | stopped {sub['stopped'].mean():.0%} | "
              f"short-leg max adverse move: median {sub['worst_up'].median() * 100:+.0f}%, worst {sub['worst_up'].max() * 100:+.0f}%")

    report(R, "all")
    report(R[R["entry"] < pd.Timestamp("2025-01-01", tz="UTC")], "entries 2022-2024")
    report(R[R["entry"] >= pd.Timestamp("2025-01-01", tz="UTC")], "entries 2025-2026")
    report(R[R["size_pct"] >= 10], "cliffs >= 10%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
