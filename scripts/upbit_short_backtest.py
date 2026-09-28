"""Tradability of the Upbit notice drift: short the Binance USDT perp (round 7).

Events from scripts/upbit_event_study.py (data/research/upbit/events.csv).
For caution designations, "investment caution urged" notices and KRW
listings: short the USDT-margined perp (XXXUSDT or 1000XXXUSDT, listed on
Binance futures before the notice) at the close of the notice's UTC day,
cover at the close 7 days later. Costs: 0.1% round trip in fees, funding (all
settlements: some perps settle hourly, so the request limit must cover 168+)
actually paid or received over the hold (fapi fundingRate), a +40% intraday
stop filled 2% worse. Reported raw and against the equal-weight move of all
perps over the same window (a rough market hedge).

Historical output is exploratory: exchangeInfo is today's contract universe,
not a point-in-time listing archive. Missing funding is excluded, not zero.
"""
import json
import sys
import time
from math import comb
from pathlib import Path

import httpx
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

OUT = Path("data/research/upbit")
FAPI = "https://fapi.binance.com/fapi/v1"
HOLD, FEE, STOP, SLIP = 7, 0.001, 0.40, 0.02
DAY_MS = 86_400_000


def score_short_trade(kl, fr, day0_ms):
    """Score one complete daily-bar window, charging funding only while open."""
    if len(kl) < HOLD + 1 or any(int(kl[i][0]) != day0_ms + i * DAY_MS for i in range(HOLD + 1)):
        raise ValueError("incomplete_daily_bars")
    if not fr:
        raise ValueError("missing_funding")
    entry_ms, planned_exit_ms = day0_ms + DAY_MS, day0_ms + (HOLD + 1) * DAY_MS
    funding_times = sorted(int(f["fundingTime"]) for f in fr
                           if entry_ms <= int(f["fundingTime"]) <= planned_exit_ms)
    if (not funding_times or len(set(funding_times)) != len(funding_times)
            or funding_times[0] - entry_ms > DAY_MS
            or planned_exit_ms - funding_times[-1] > DAY_MS
            or any(b - a > DAY_MS for a, b in zip(funding_times, funding_times[1:]))):
        raise ValueError("incomplete_funding")
    entry = float(kl[0][4])
    exit_px, exit_ms, stopped = float(kl[HOLD][4]), planned_exit_ms, False
    for bar in kl[1:HOLD + 1]:
        if float(bar[2]) >= entry * (1 + STOP):
            exit_px, exit_ms, stopped = entry * (1 + STOP) * (1 + SLIP), int(bar[0]) + DAY_MS, True
            break
    funding = sum(float(f["fundingRate"]) for f in fr
                  if entry_ms <= int(f["fundingTime"]) <= exit_ms)
    return (entry - exit_px) / entry - FEE + funding, funding, stopped


def get(c, url, params):
    for attempt in range(4):
        try:
            r = c.get(url, params=params)
            if r.status_code == 200:
                return r.json()
        except Exception:  # noqa: BLE001
            pass
        time.sleep(2 * (attempt + 1))
    return None


def main():
    ev = pd.read_csv(OUT / "events.csv", parse_dates=["date"])
    ev = ev[ev.kind.isin(["caution", "warning_urged", "listing_krw"])]
    cache = OUT / "perp_cache.json"
    store = json.loads(cache.read_text(encoding="utf-8")) if cache.exists() else {}
    with httpx.Client(timeout=30) as c:
        info = get(c, f"{FAPI}/exchangeInfo", {})
        perps = {s["symbol"]: s for s in info["symbols"] if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"}
        rows = []
        for _, e in ev.iterrows():
            d0 = e.date.tz_convert("UTC") if e.date.tzinfo else e.date.tz_localize("UTC")
            sym = next((s for s in (f"{e.token}USDT", f"1000{e.token}USDT") if s in perps and perps[s].get("onboardDate", 0) < d0.timestamp() * 1000), None)
            if not sym:
                rows.append({"kind": e.kind, "token": e.token, "date": d0, "status": "no_perp"})
                continue
            key = f"v2|{sym}|{d0.date()}"  # v2: funding limit 1000 (v1 used 100, truncating 1h-funding perps)
            if key not in store or not store[key].get("funding"):
                start = int(d0.timestamp() * 1000)
                kl = get(c, f"{FAPI}/klines", {"symbol": sym, "interval": "1d", "startTime": start, "limit": HOLD + 1}) or []
                fr = get(c, f"{FAPI}/fundingRate", {"symbol": sym, "startTime": start + DAY_MS, "endTime": start + (HOLD + 1) * DAY_MS, "limit": 1000}) or []
                store[key] = {"klines": kl, "funding": fr}
                time.sleep(0.2)
            kl, fr = store[key]["klines"], store[key]["funding"]
            try:
                ret, funding, stopped = score_short_trade(kl, fr, int(d0.timestamp() * 1000))
            except ValueError as exc:
                rows.append({"kind": e.kind, "token": e.token, "date": d0, "status": str(exc)})
                continue
            rows.append({"kind": e.kind, "token": e.token, "date": d0, "status": "ok", "symbol": sym,
                         "short_ret": ret, "funding": funding, "stopped": stopped})
    cache.write_text(json.dumps(store), encoding="utf-8")
    r = pd.DataFrame(rows)
    # market leg: equal-weight 7-day move of all spot tokens (panel median), same window
    close = pd.read_parquet("data/research/delist_risk/close.parquet")
    close.index = pd.to_datetime(close.index, utc=True)
    mkt = (1 + close.asfreq("1D").pct_change(fill_method=None).median(axis=1).fillna(0)).cumprod()
    r["mkt7"] = [mkt.get(d + pd.Timedelta(days=HOLD), np.nan) / mkt.get(d, np.nan) - 1 for d in r.date]
    r["hedged"] = r.short_ret + r.mkt7 - FEE  # short token, long market basket
    r.to_csv(OUT / "short_backtest.csv", index=False)
    print("status:", r.groupby(["kind", "status"]).size().to_dict())
    ok = r[r.status == "ok"]
    for kind in ["caution", "warning_urged", "listing_krw", "ALL"]:
        f = ok if kind == "ALL" else ok[ok.kind == kind]
        f = f.drop_duplicates(["token", "date"])
        if len(f) < 8:
            print(f"== {kind}: n={len(f)} too few")
            continue
        for col in ("short_ret", "hedged"):
            x = f[col].dropna()
            g = [gg[col].dropna().to_numpy() for _, gg in f.groupby(f.date.dt.to_period("M"))]
            rng = np.random.default_rng(0)
            bs = [np.concatenate([g[i] for i in rng.integers(0, len(g), len(g))]).mean() for _ in range(2000)]
            k, n = int((x > 0).sum()), len(x)
            print(f"== {kind} {col}: n={n} mean {x.mean() * 100:+.1f}% median {x.median() * 100:+.1f}% 90% CI [{np.percentile(bs, 5) * 100:+.1f}, {np.percentile(bs, 95) * 100:+.1f}] "
                  f"wins {k}/{n} (p={sum(comb(n, i) for i in range(k, n + 1)) / 2 ** n:.4f}) | stops {int(f.stopped.sum())} | mean funding {f.funding.mean() * 100:+.2f}%")


if __name__ == "__main__":
    main()
