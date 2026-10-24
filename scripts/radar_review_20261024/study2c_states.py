"""Study 2c: the page's state tags (异动启动 / 高控盘警戒 / 布局吸筹 / 待跟踪...) as event studies:
forward return of flagged coins minus same-date unflagged coins, date-clustered."""
from __future__ import annotations
import math, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SP = Path(sys.argv[1])
FUT = ROOT / "data/historical/binance_futures"
HOR = {"4h": 1, "24h": 6, "72h": 18, "7d": 42}

parts = []
for tag in ("A", "B"):
    d = pd.read_parquet(SP / tag / "study2_scores.parquet"); d["period"] = tag; parts.append(d)
D = pd.concat(parts, ignore_index=True)
opens = {}
for sym in D.symbol.unique():
    b = sym.split("/")[0]
    for cand in (b, "1000" + b):
        p = FUT / f"{cand}_USDT" / "4h.parquet"
        if p.exists():
            o = pd.read_parquet(p, columns=["open"])["open"]
            opens[sym] = o[~o.index.duplicated(keep="last")].sort_index(); break
for h, n in HOR.items():
    vals = []
    for sym, g in D.groupby("symbol"):
        s = opens[sym]; idx = s.index
        i0 = np.minimum(idx.searchsorted(g.ts.values), len(idx) - 1)
        ok0 = idx[i0] == pd.DatetimeIndex(g.ts.values)
        i1 = i0 + n; ok1 = i1 < len(idx); i1c = np.minimum(i1, len(idx) - 1)
        cont = (idx[i1c] - idx[i0]) == pd.Timedelta(hours=4 * n)
        vals.append(pd.Series(np.where(ok0 & ok1 & cont, s.values[i1c] / s.values[i0] - 1, np.nan), index=g.index))
    D["fwd_" + h] = pd.concat(vals)


def nw(x, lag):
    x = np.asarray(x, float); x = x[~np.isnan(x)]; n = len(x)
    if n < 15:
        return float("nan"), float("nan")
    m = x.mean(); e = x - m; v = (e * e).sum() / n
    for l in range(1, lag + 1):
        v += 2 * (1 - l / (lag + 1)) * (e[l:] * e[:-l]).sum() / n
    return m, m / math.sqrt(max(v, 1e-18) / n)


D["signal_state"] = D["signal_state"].fillna("").replace("", "（无标签）")
print("state counts (all rows):")
print(D.signal_state.value_counts().to_string())
print("\nper-date share flagged: ", D.groupby("signal_state").size().div(D.ts.nunique()).round(2).to_dict())
for per, sub in (("B 2025-04..2026-01", D[D.period == "B"]), ("A 2026-02..10", D[D.period == "A"]), ("ALL", D)):
    print(f"\n===== {per}: flagged minus same-date unflagged (mean fwd return diff, NW t) =====")
    rows = []
    for st in [s for s in sub.signal_state.unique() if s != "（无标签）"]:
        for h, n in HOR.items():
            diffs = []
            for ts, g in sub.groupby("ts"):
                f = g[g.signal_state == st]["fwd_" + h].dropna(); r = g[g.signal_state != st]["fwd_" + h].dropna()
                if len(f) >= 1 and len(r) >= 20:
                    diffs.append(f.mean() - r.mean())
            m, t = nw(diffs, max(1, math.ceil(n / 6)))
            rows.append({"state": st, "h": h, "n_dates": len(diffs), "diff": m, "t": t})
    R = pd.DataFrame(rows)
    if len(R):
        R["v"] = R["diff"].map("{:+.4f}".format) + " (t=" + R["t"].map("{:+.1f}".format) + ", n=" + R["n_dates"].astype(str) + ")"
        print(R.pivot(index="state", columns="h", values="v")[list(HOR)].to_string())
