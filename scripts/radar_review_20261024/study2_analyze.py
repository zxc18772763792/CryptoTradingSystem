"""Study 2 analysis: does the radar's real-time score predict forward returns?"""
from __future__ import annotations
import json, math, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SP = Path(sys.argv[1])
FUT = ROOT / "data/historical/binance_futures"
SCORES = ["upside_score", "priority_score", "layout_score", "alert_score", "anomaly_score",
          "accumulation_score", "control_score", "ignition_score", "continuation_score",
          "risk_penalty", "liquidity_trap_score"]
HOR = {"4h": 1, "24h": 6, "72h": 18, "7d": 42}  # bars of 4h
PRIMARY = ["upside_score", "priority_score"]

parts = []
for tag in ("A", "B"):
    p = SP / tag / "study2_scores.parquet"
    if p.exists():
        d = pd.read_parquet(p); d["period"] = tag; parts.append(d)
D = pd.concat(parts, ignore_index=True)
for c in SCORES:
    D[c] = pd.to_numeric(D[c], errors="coerce")
print("score rows", len(D), "timestamps", D.ts.nunique(), "coins", D.symbol.nunique())

# forward returns from open of bar at ts to open of bar at ts+H
opens = {}
for sym in D.symbol.unique():
    b = sym.split("/")[0]
    for cand in (b, "1000" + b):
        p = FUT / f"{cand}_USDT" / "4h.parquet"
        if p.exists():
            df = pd.read_parquet(p, columns=["open"])
            opens[sym] = df[~df.index.duplicated(keep="last")]["open"].sort_index()
            break
for h, n in HOR.items():
    vals = []
    for sym, g in D.groupby("symbol"):
        s = opens[sym]
        idx = s.index
        i0 = idx.searchsorted(g.ts.values)
        i0c = np.minimum(i0, len(idx) - 1)
        exact0 = idx[i0c] == pd.DatetimeIndex(g.ts.values)
        i1 = i0c + n
        ok1 = i1 < len(idx)
        i1c = np.minimum(i1, len(idx) - 1)
        contiguous = (idx[i1c] - idx[i0c]) == pd.Timedelta(hours=4 * n)
        r = np.where(exact0 & ok1 & contiguous, s.values[i1c] / s.values[i0c] - 1.0, np.nan)
        vals.append(pd.Series(r, index=g.index))
    D["fwd_" + h] = pd.concat(vals)


def nw_t(x, lag):
    x = np.asarray(x, float); x = x[~np.isnan(x)]
    n = len(x)
    if n < 10:
        return float("nan"), float("nan")
    m = x.mean(); e = x - m
    v = (e * e).sum() / n
    for l in range(1, lag + 1):
        w = 1 - l / (lag + 1)
        v += 2 * w * (e[l:] * e[:-l]).sum() / n
    se = math.sqrt(max(v, 1e-18) / n)
    return m, m / se


def ic_table(sub, label):
    rows = []
    for sc in SCORES:
        for h, n in HOR.items():
            lag = max(1, int(math.ceil(n / 6)))
            ics = []
            for ts, g in sub.groupby("ts"):
                gg = g[[sc, "fwd_" + h]].dropna()
                if len(gg) < 30 or gg[sc].nunique() < 5:
                    continue
                ics.append(gg[sc].rank().corr(gg["fwd_" + h].rank()))
            m, t = nw_t(ics, lag)
            rows.append({"period": label, "score": sc, "h": h, "n_ts": len(ics), "mean_ic": m, "t_nw": t,
                         "pct_pos": float(np.mean(np.array(ics) > 0)) if ics else np.nan})
    return pd.DataFrame(rows)


pd.set_option("display.width", 200); pd.set_option("display.max_rows", 500)
res = []
for lab, sub in [("A 2026-02..10", D[D.period == "A"]), ("B 2025-04..2026-01", D[D.period == "B"]), ("ALL", D)]:
    res.append(ic_table(sub, lab))
T = pd.concat(res)
T.to_csv(SP / "study2_ic.csv", index=False)
for lab in T.period.unique():
    t = T[T.period == lab]
    print(f"\n===== rank IC, {lab} =====")
    print(t.pivot(index="score", columns="h", values="mean_ic")[list(HOR)].round(4).to_string())
    print("-- NW t-stat")
    print(t.pivot(index="score", columns="h", values="t_nw")[list(HOR)].round(2).to_string())

print("\n===== top-10% by score, excess vs universe EW mean (gross) =====")
out = []
for lab, sub in [("A", D[D.period == "A"]), ("B", D[D.period == "B"]), ("ALL", D)]:
    for sc in PRIMARY + ["anomaly_score", "control_score", "ignition_score", "accumulation_score"]:
        for h, n in HOR.items():
            ex, exb = [], []
            for ts, g in sub.groupby("ts"):
                gg = g[[sc, "fwd_" + h]].dropna()
                if len(gg) < 30:
                    continue
                k = max(3, int(len(gg) * 0.1))
                top = gg.nlargest(k, sc)["fwd_" + h].mean()
                bot = gg.nsmallest(k, sc)["fwd_" + h].mean()
                ex.append(top - gg["fwd_" + h].mean())
                exb.append(top - bot)
            lag = max(1, int(math.ceil(n / 6)))
            m, t = nw_t(ex, lag)
            m2, t2 = nw_t(exb, lag)
            out.append({"per": lab, "score": sc, "h": h, "top10_excess": m, "t": t, "top_bottom": m2, "t_tb": t2, "n_ts": len(ex)})
O = pd.DataFrame(out)
O.to_csv(SP / "study2_topbucket.csv", index=False)
for lab in ("A", "B", "ALL"):
    t = O[O.per == lab]
    print(f"\n-- {lab}: top10% excess vs universe (mean per-period return, NW t)")
    print(t.assign(v=lambda x: x.top10_excess.map("{:+.4f}".format) + " (t=" + x.t.map("{:+.1f}".format) + ")")
          .pivot(index="score", columns="h", values="v")[list(HOR)].to_string())
