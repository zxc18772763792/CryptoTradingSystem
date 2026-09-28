"""New-perp short from the D2 close: return by exit day D3..D14 (same rules as study_listing, +40% stop).

Writes/reads the daily-bar cache data/research/binance_announcements/listing_klines_cache.json,
which scripts/stop_loss_sweep.py also uses. Linear short P&L since 2026-09-28.
"""
import json
import re
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, ".")
sys.path.insert(0, "scripts")
import exchange_event_studies as ees  # noqa: E402

hist = json.loads((ees.OUT_DIR / "history.json").read_text(encoding="utf-8"))
tickers = {}
for a in hist["48"]:
    t = a["title"]
    if a["release_ms"] >= ees.SINCE_MS and "Binance Futures Will Launch" in t and not ees.EXCLUDE.search(t):
        for tk in re.findall(r"\b([A-Z0-9]{2,20}USDT)\b", t):
            tickers.setdefault(tk, a["release_ms"])
cache_path = ees.OUT_DIR / "listing_klines_cache.json"
cache = json.loads(cache_path.read_text(encoding="utf-8")) if cache_path.exists() else {}
with ees._client() as c:
    for tk, ms in tickers.items():
        if tk in cache:
            continue
        k = ees.klines(c, ees.FAPI, tk, ms - 86400_000, interval="1d")
        if k is None or len(k) < 15:
            cache[tk] = None
            continue
        r = c.get(ees.FUNDING, params={"symbol": tk, "startTime": int(k.index[0].timestamp() * 1000), "limit": 1000})
        time.sleep(ees.PACE)
        fund = r.json() if r.status_code == 200 and isinstance(r.json(), list) else []
        cache[tk] = {"t": [int(x.timestamp() * 1000) for x in k.index[:15]], "h": list(map(float, k["h"].iloc[:15])),
                     "c": list(map(float, k["c"].iloc[:15])), "f": [[int(x["fundingTime"]), float(x["fundingRate"])] for x in fund]}
cache_path.write_text(json.dumps(cache), encoding="utf-8")

STOP = 0.40
rows, years = [], []
for tk, v in cache.items():
    if not v:
        continue
    t, h, cl, f = v["t"], v["h"], v["c"], v["f"]
    entry = cl[2] * 0.995
    entry_ms = t[2] + 86_400_000
    path, stopped = [], None
    for d in range(3, 15):
        if stopped is None and h[d] >= entry * (1 + STOP):
            stopped = d
        if stopped is not None:
            px, end = entry * (1 + STOP) * 1.02, t[stopped] + 86_400_000
        else:
            px, end = cl[d], t[d] + 86_400_000
        fund = sum(r for ft, r in f if entry_ms <= ft <= end)
        path.append((entry - px) / entry + fund - 0.003)  # linear USDT-perp short P&L
    rows.append(path)
    years.append(pd.Timestamp(t[2], unit="ms").year)
a = np.array(rows)
print(f"new perps with 15 daily bars: {len(a)}")
for i, d in enumerate(range(3, 15)):
    x = a[:, i]
    rng = np.random.default_rng(0)
    boot = [rng.choice(x, len(x)).mean() for _ in range(2000)]
    print(f"  D2->D{d:<2}: mean {x.mean()*100:+5.1f}% [{np.percentile(boot,5)*100:+.1f},{np.percentile(boot,95)*100:+.1f}]  median {np.median(x)*100:+5.1f}%  win {np.mean(x>0):.0%}  per-day {x.mean()/(d-2)*100:+.2f}%  worst {x.min()*100:+.0f}%")
