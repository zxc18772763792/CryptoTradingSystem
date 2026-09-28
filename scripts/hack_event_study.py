"""Do tokens of hacked protocols keep falling after the news? (round 7)

WHY: every surviving edge so far is a structural event the market under-reads.
A hack is public within hours and an LLM can map the news to the token, so if
prices drift for days afterwards it is tradable at our speed. Events: the
DefiLlama hacks list (api.llama.fi/hacks) mapped to protocol tokens
(lite/protocols2 symbol + gecko_id) that trade on Binance spot, prices from the
survivorship-free panel data/research/delist_risk/close.parquet.

Timeline per event (day 0 = the hack's UTC date):
  reaction  close(-1) -> close(+1)   what the news already did
  drift     close(+1) -> close(+8), close(+1) -> close(+31)   what a short
            opened after day +1 closes would earn (sign flipped: positive =
            short profitable), market-adjusted by the panel's median return.
Month-clustered bootstrap CIs; one event per token per 60 days.
"""
import json
import sys
from math import comb

import httpx
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

MIN_AMOUNT = 1_000_000


def main():
    hacks = httpx.get("https://api.llama.fi/hacks", timeout=60).json()
    lite = httpx.get("https://api.llama.fi/lite/protocols2", timeout=60).json()
    by_id = {str(p.get("defillamaId") or p.get("id")): p for p in lite["protocols"]}
    by_id.update({str(p["id"]): p for p in lite.get("parentProtocols", [])})
    close = pd.read_parquet("data/research/delist_risk/close.parquet")
    close.index = pd.to_datetime(close.index, utc=True)
    close = close.asfreq("1D")
    market = close.pct_change(fill_method=None).median(axis=1)
    mkt_level = (1 + market.fillna(0)).cumprod()

    events, skipped = [], {"small": 0, "no_protocol": 0, "no_token": 0, "no_prices": 0}
    for h in hacks:
        if (h.get("amount") or 0) < MIN_AMOUNT:
            skipped["small"] += 1
            continue
        p = by_id.get(str(h.get("defillamaId") or ""))
        if not p and h.get("defillamaId"):
            p = by_id.get(f"parent#{h['defillamaId']}")
        if not p:
            skipped["no_protocol"] += 1
            continue
        parent = by_id.get(str(p.get("parentProtocol") or "")) or {}
        sym = str(p.get("symbol") or parent.get("symbol") or "").upper()
        if not sym or sym == "-" or sym not in close.columns:
            skipped["no_token"] += 1
            continue
        d0 = pd.Timestamp(h["date"], unit="s", tz="UTC").normalize()
        pts = {k: d0 + pd.Timedelta(days=k) for k in (-1, 1, 8, 31)}
        if any(t not in close.index or pd.isna(close.at[t, sym]) for t in pts.values()):
            skipped["no_prices"] += 1
            continue
        px = {k: close.at[t, sym] for k, t in pts.items()}
        mk = {k: mkt_level[t] for k, t in pts.items()}
        adj = lambda a, b: (px[b] / px[a] - 1) - (mk[b] / mk[a] - 1)  # noqa: E731
        events.append({"date": d0, "token": sym, "name": h.get("name"), "amount_m": h["amount"] / 1e6,
                       "technique": h.get("technique"), "returned": bool(h.get("returnedFunds")),
                       "reaction": adj(-1, 1), "drift7": -adj(1, 8), "drift30": -adj(1, 31)})
    ev = pd.DataFrame(events).sort_values("date")
    kept, last = [], {}
    for _, r in ev.iterrows():  # one event per token per 60 days
        if r.token in last and (r.date - last[r.token]).days < 60:
            continue
        last[r.token] = r.date
        kept.append(r)
    ev = pd.DataFrame(kept)
    print(f"hacks >= $1M mapped to Binance tokens: {len(ev)} events on {ev.token.nunique()} tokens | skipped {skipped}")
    ev.to_csv("data/research/hack_events.csv", index=False)

    def ci(col, frame):
        m = frame.groupby(frame.date.dt.to_period("M"))[col]
        groups = [g.to_numpy() for _, g in m]
        rng = np.random.default_rng(0)
        boots = []
        for _ in range(2000):
            pick = rng.integers(0, len(groups), len(groups))
            boots.append(np.concatenate([groups[i] for i in pick]).mean())
        return np.percentile(boots, 5) * 100, np.percentile(boots, 95) * 100

    def report(frame, label):
        if len(frame) < 8:
            print(f"== {label}: n={len(frame)} (too few)")
            return
        print(f"== {label}: n={len(frame)}")
        print(f"  news reaction day -1 -> +1 (market-adj): mean {frame.reaction.mean() * 100:+.1f}%  median {frame.reaction.median() * 100:+.1f}%")
        for col, name in (("drift7", "short +1 -> +8"), ("drift30", "short +1 -> +31")):
            lo, hi = ci(col, frame)
            k = int((frame[col] > 0).sum())
            p = sum(comb(len(frame), i) for i in range(k, len(frame) + 1)) / 2 ** len(frame)
            print(f"  {name}: mean {frame[col].mean() * 100:+.1f}%  median {frame[col].median() * 100:+.1f}%  90% CI [{lo:+.1f}, {hi:+.1f}]  short wins {k}/{len(frame)} (sign p={p:.3f})")

    report(ev, "all hacks >= $1M")
    report(ev[ev.amount_m >= 10], "hacks >= $10M")
    report(ev[ev.reaction < -0.10], "news already cut the price > 10%")
    report(ev[ev.reaction >= -0.10], "news cut the price <= 10% (under-reaction?)")
    report(ev[ev.date >= "2025-01-01"], "2025-26 only")


if __name__ == "__main__":
    main()
