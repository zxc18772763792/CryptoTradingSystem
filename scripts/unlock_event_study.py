"""Event study: what does price do around large cliff token unlocks?

WHY THIS EXISTS (2026-09-27): the July test of unlocks inside the altcoin pump
panel had almost no power (17 coins, 9 pumps) because free unlock data covers
only ~27 of those 110 coins. DefiLlama's own universe (370 tokens, historical
schedules, free) is where unlocks can actually be measured.

Method:
* events: DefiLlama `unlockEvents` cliff allocations (one-off blocks, not
  linear vesting); size = tokens released / tokens already unlocked just
  before (a proxy for circulating supply); keep >= 1%; within 30 days per
  token only the largest counts (no overlapping windows);
* "insider" = >= 50% of the block goes to team / investors (categories
  insiders, privateSale);
* prices: Binance spot daily closes; token matched by DefiLlama's ticker and
  rejected unless Binance's latest price is within 15% of DefiLlama's;
* returns BTC-adjusted, measured from the close of the day BEFORE the unlock;
* placebo: the same token on random dates >= 45 days from any >= 0.5% cliff,
  same windows. Unlock tokens drift down vs BTC anyway; only event minus
  placebo is an unlock effect;
* 90% CIs by bootstrap over calendar months (unlocks cluster on month starts).

Usage:
  python scripts/unlock_event_study.py            # fetch (cached) + report
  python scripts/unlock_event_study.py --min-size 3
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUT = PROJECT_ROOT / "data" / "research" / "unlock_study"
LLAMA = "https://defillama-datasets.llama.fi"
SPOT = "https://api.binance.com/api/v3"
START = pd.Timestamp("2022-10-01", tz="UTC")
INSIDER_CATEGORIES = {"insiders", "privateSale"}
WINDOWS = {"pre30": (-31, -1), "pre7": (-8, -1), "post1": (-1, 1), "post7": (-1, 7), "post30": (-1, 30)}


def _session():
    import requests

    from config.settings import settings

    s = requests.Session()
    proxy = settings.HTTPS_PROXY or settings.HTTP_PROXY
    if proxy:
        s.proxies = {"https": proxy, "http": proxy}
    return s


def _cached_json(s, url: str, path: Path, max_age_days: float = 14) -> Any:
    if path.exists() and (time.time() - path.stat().st_mtime) < max_age_days * 86400:
        return json.loads(path.read_text(encoding="utf-8"))
    r = s.get(url, timeout=60)
    r.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(r.text, encoding="utf-8")
    time.sleep(0.3)
    return r.json()


def unlocked_series(schedule: Dict[str, Any]) -> pd.Series:
    """Cumulative unlocked tokens per day, summed across allocation categories."""
    parts = []
    for cat in (schedule.get("documentedData") or {}).get("data") or []:
        pts = cat.get("data") or []
        if pts:
            parts.append(pd.Series({pd.Timestamp(p["timestamp"], unit="s", tz="UTC").normalize(): float(p.get("unlocked") or 0) for p in pts}))
    if not parts:
        return pd.Series(dtype=float)
    return pd.concat(parts, axis=1).sort_index().ffill().fillna(0).sum(axis=1)


def cliff_events(entry: Dict[str, Any], unlocked: pd.Series, min_pct: float) -> List[Dict[str, Any]]:
    out = []
    for ev in entry.get("unlockEvents") or []:
        allocs = ev.get("cliffAllocations") or []
        if not allocs:
            continue
        t = pd.Timestamp(ev["timestamp"], unit="s", tz="UTC").normalize()
        amount = sum(float(a.get("amount") or 0) for a in allocs)
        before = unlocked[unlocked.index < t]
        base = float(before.iloc[-1]) if len(before) else 0.0
        if amount <= 0 or base <= 0:
            continue
        insider = sum(float(a.get("amount") or 0) for a in allocs if a.get("category") in INSIDER_CATEGORIES)
        out.append({"date": t, "size_pct": amount / base * 100, "insider": insider / amount >= 0.5})
    events = sorted([e for e in out if e["size_pct"] >= min_pct], key=lambda e: e["date"])
    kept: List[Dict[str, Any]] = []
    for e in events:  # largest event per 30-day cluster
        if kept and (e["date"] - kept[-1]["date"]).days < 30:
            if e["size_pct"] > kept[-1]["size_pct"]:
                kept[-1] = e
            continue
        kept.append(e)
    return kept


def daily_closes(s, symbol: str) -> Optional[pd.Series]:
    rows, start = [], int(START.timestamp() * 1000)
    while True:
        r = s.get(f"{SPOT}/klines", params={"symbol": symbol, "interval": "1d", "startTime": start, "limit": 1000}, timeout=30)
        time.sleep(0.2)
        if r.status_code != 200:
            return None
        batch = r.json()
        rows += batch
        if len(batch) < 1000:
            break
        start = batch[-1][0] + 86_400_000
    if not rows:
        return None
    return pd.Series([float(x[4]) for x in rows], index=pd.to_datetime([x[0] for x in rows], unit="ms", utc=True))


def window_returns(k: pd.Series, btc: pd.Series, t0: pd.Timestamp) -> Dict[str, float]:
    out = {}
    for name, (a, b) in WINDOWS.items():
        ta, tb = t0 + pd.Timedelta(days=a), t0 + pd.Timedelta(days=b)
        if ta in k.index and tb in k.index and ta in btc.index and tb in btc.index:
            out[name] = (k[tb] / k[ta] - 1) - (btc[tb] / btc[ta] - 1)
        else:
            out[name] = np.nan
    return out


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
    parser.add_argument("--min-size", type=float, default=1.0, help="minimum cliff size, %% of unlocked supply")
    parser.add_argument("--placebos", type=int, default=3)
    args = parser.parse_args(argv)
    s = _session()
    index = _cached_json(s, f"{LLAMA}/emissionsIndex", OUT / "emissions_index.json")
    index = index["data"] if isinstance(index, dict) else index
    spot = {x["symbol"] for x in s.get(f"{SPOT}/exchangeInfo", timeout=60).json()["symbols"] if x.get("quoteAsset") == "USDT"}
    last = {t["symbol"]: float(t["lastPrice"]) for t in s.get(f"{SPOT}/ticker/24hr", timeout=60).json()}
    btc = daily_closes(s, "BTCUSDT")

    rows, placebo_rows, skipped = [], [], {"no_binance": 0, "price_mismatch": 0, "no_events": 0}
    closes: Dict[str, pd.Series] = {}
    all_cliffs: Dict[str, List[str]] = {}
    rng = np.random.default_rng(1)
    for entry in index:
        prices = entry.get("tokenPrice") or []
        ticker = str((prices[0] if prices else {}).get("symbol") or "").upper()
        symbol = f"{ticker}USDT"
        if not ticker or symbol not in spot:
            skipped["no_binance"] += 1
            continue
        llama_px = float((prices[0] or {}).get("price") or 0)
        if not llama_px or abs(last.get(symbol, 0) / llama_px - 1) > 0.15:
            skipped["price_mismatch"] += 1  # different coin sharing a ticker
            continue
        try:
            schedule = _cached_json(s, f"{LLAMA}/emissions/{entry['protocolSlug']}", OUT / "emissions" / f"{entry['protocolSlug']}.json")
        except Exception:  # noqa: BLE001 - a few index entries have no schedule file
            skipped["no_schedule"] = skipped.get("no_schedule", 0) + 1
            continue
        unlocked = unlocked_series(schedule)
        events = [e for e in cliff_events(entry, unlocked, args.min_size) if START + pd.Timedelta(days=40) <= e["date"] <= pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=32)]
        if not events:
            skipped["no_events"] += 1
            continue
        k = daily_closes(s, symbol)
        if k is None or len(k) < 90:
            continue
        closes[ticker] = k
        for e in events:
            rows.append({"token": ticker, "date": e["date"], "size_pct": e["size_pct"], "insider": e["insider"],
                         "age_days": (e["date"] - k.index[0]).days})
        # placebo dates for the same token, far from any meaningful cliff
        blocked = [e["date"] for e in cliff_events(entry, unlocked, 0.5)]
        all_cliffs.setdefault(ticker, []).extend(str(b.date()) for b in blocked)
        candidates = [d for d in k.index[40:-35] if all(abs((d - b).days) >= 45 for b in blocked)]
        for d in rng.choice(candidates, size=min(len(candidates), args.placebos * len(events)), replace=False) if candidates else []:
            placebo_rows.append({"token": ticker, "date": d, "age_days": (d - k.index[0]).days})

    # Benchmark = equal-weighted index of all study tokens (alts fell vs BTC in 2024-26,
    # so a BTC benchmark makes almost any alt window look negative).
    pd.DataFrame(closes).to_parquet(OUT / "closes.parquet")  # reused by the matched-control analysis
    (OUT / "cliff_dates.json").write_text(json.dumps(all_cliffs), encoding="utf-8")
    daily = pd.DataFrame({t: k.pct_change() for t, k in closes.items()})
    alt_index = (1 + daily.mean(axis=1, skipna=True).fillna(0)).cumprod()
    bench = {"btc": btc, "alt": alt_index}
    for row in rows + placebo_rows:
        k = closes[row["token"]]
        for name, ref in bench.items():
            for w, value in window_returns(k, ref, row["date"]).items():
                row[w if name == "alt" else f"{w}_btc"] = value
    E, P = pd.DataFrame(rows), pd.DataFrame(placebo_rows)
    E.to_csv(OUT / "events.csv", index=False)
    print(f"tokens skipped: {skipped} | events (>= {args.min_size:g}% of unlocked supply): {len(E)} on {E['token'].nunique()} tokens "
          f"| placebo draws: {len(P)}")

    def report(sub: pd.DataFrame, label: str) -> None:
        if len(sub) < 10:
            return
        m = sub["date"].dt.strftime("%Y-%m")
        print(f"\n== {label}: n={len(sub)} ({sub['token'].nunique()} tokens), median size {sub['size_pct'].median():.1f}%")
        for w in WINDOWS:
            x = sub[w]
            # placebo drawn from the same age buckets as these events (young tokens drift down)
            p = sum(P[P["age_bucket"] == b][w].mean() * n for b, n in sub["age_bucket"].value_counts().items()
                    if (P["age_bucket"] == b).any()) / max(1, sub["age_bucket"].isin(P["age_bucket"]).sum())
            lo, hi = month_boot(x - p, m)
            print(f"  {w:7s} event {x.mean() * 100:+6.2f}% (median {x.median() * 100:+6.2f}%, down {(x < 0).mean():.0%}) | "
                  f"placebo {p * 100:+6.2f}% | excess {(x.mean() - p) * 100:+6.2f}%  90%CI [{lo * 100:+.2f}, {hi * 100:+.2f}]")

    age_bins = [-1, 180, 365, 730, 100000]
    E["age_bucket"] = pd.cut(E["age_days"], age_bins).astype(str)
    P["age_bucket"] = pd.cut(P["age_days"], age_bins).astype(str)
    print("benchmark: equal-weighted index of the study tokens; placebo matched on token-age bucket")
    report(E, "all cliff unlocks")
    for label, grp in E.groupby("age_bucket"):
        report(grp, f"token age {label} days")
    for lo, hi in ((1, 3), (3, 10), (10, 1e9)):
        report(E[(E["size_pct"] >= lo) & (E["size_pct"] < hi)], f"size {lo}-{hi if hi < 1e9 else 'inf'}%")
    report(E[E["insider"]], "insider/VC blocks")
    report(E[~E["insider"]], "non-insider blocks")
    for year, grp in E.groupby(E["date"].dt.year):
        report(grp, f"year {year}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
