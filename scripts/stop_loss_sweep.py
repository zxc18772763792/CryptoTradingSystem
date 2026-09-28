"""Stop-loss sweep for the new-perp and Upbit shorts from cached daily bars + funding (2026-09-28).

Run scripts/exchange_event_studies.py (listing klines cache via the horizon study) and
scripts/upbit_short_backtest.py first. Unlock stops: scripts/unlock_short_backtest.py --stop.
See docs/LLM_TRADING_RESEARCH_ROUND7_2026-09-28.md (supplement 3).
"""
import json
import numpy as np
import pandas as pd

DAY = 86_400_000
STOPS = [None, 0.50, 0.40, 0.30, 0.20, 0.15, 0.10]
SLIP = 0.02


def sim(entry, highs, closes, bar_ends, funding, entry_ms, stop, fee):
    """Short from `entry`; daily bars after entry; stop on high >= entry*(1+stop) filled 2% worse."""
    exit_px, end = closes[-1], bar_ends[-1]
    stopped = False
    if stop is not None:
        for h, t in zip(highs, bar_ends):
            if h >= entry * (1 + stop):
                exit_px, end, stopped = entry * (1 + stop) * (1 + SLIP), t, True
                break
    fund = sum(r for ft, r in funding if entry_ms <= ft <= end)
    return (entry - exit_px) / entry + fund - fee, stopped  # linear USDT-perp short


def report(name, trades, fee):
    print(f"\n== {name} (n={len(trades)})")
    print(f"   {'stop':>6} | {'mean':>6} {'90% CI':>16} | {'median':>6} | {'win':>4} | {'worst':>5} | {'5% worst avg':>12} | stops hit")
    for stop in STOPS:
        res = [sim(*t, stop=stop, fee=fee) for t in trades]
        x = np.array([r for r, _ in res]); hit = sum(s for _, s in res)
        rng = np.random.default_rng(0)
        boot = [rng.choice(x, len(x)).mean() for _ in range(2000)]
        tail = np.sort(x)[: max(1, len(x) // 20)].mean()
        label = "none" if stop is None else f"+{int(stop*100)}%"
        print(f"   {label:>6} | {x.mean()*100:+5.1f}% [{np.percentile(boot,5)*100:+5.1f},{np.percentile(boot,95)*100:+5.1f}] | {np.median(x)*100:+5.1f}% | {np.mean(x>0):4.0%} | {x.min()*100:+4.0f}% | {tail*100:+11.1f}% | {hit}")


# new perps: D2 close entry (0.5% worse), bars D3..D14, 0.3% costs
cache = json.load(open("data/research/binance_announcements/listing_klines_cache.json", encoding="utf-8"))
listing = []
for v in cache.values():
    if not v:
        continue
    t, h, c, f = v["t"], v["h"], v["c"], v["f"]
    entry = c[2] * 0.995
    listing.append((entry, h[3:15], c[3:15], [x + DAY for x in t[3:15]], f, t[2] + DAY))
report("new-perp short D2->D14", listing, fee=0.003)

# Upbit: entry = day-0 close, bars day 1..7, 0.1% fees
store = json.load(open("data/research/upbit/perp_cache.json", encoding="utf-8"))
ev = pd.read_csv("data/research/upbit/events.csv", parse_dates=["date"])
for kind, label in (("caution", "Upbit caution short 7d"), ("listing_krw", "Upbit KRW-listing short 7d")):
    trades = []
    for _, e in ev[ev.kind == kind].drop_duplicates(["token", "date"]).iterrows():
        key = next((k for k in store if k.startswith("v2|") and k.endswith(str(e.date.date()))
                    and (f"|{e.token}USDT|" in k or f"|1000{e.token}USDT|" in k)), None)
        if not key or len(store[key]["klines"]) < 8:
            continue
        kl, fr = store[key]["klines"], store[key]["funding"]
        entry = float(kl[0][4])
        d0 = int(kl[0][0])
        trades.append((entry, [float(b[2]) for b in kl[1:8]], [float(b[4]) for b in kl[1:8]],
                       [int(b[0]) + DAY for b in kl[1:8]], [(int(x["fundingTime"]), float(x["fundingRate"])) for x in fr], d0 + DAY))
    report(label, trades, fee=0.001)
