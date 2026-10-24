"""Study 3 analysis on the point-in-time, survivorship-free universe."""
from __future__ import annotations
import math, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SP = Path(sys.argv[1])
FUT = ROOT / "data/historical/binance_futures"
STORE_END = pd.Timestamp("2026-10-20 00:00")
COST = 0.0016
HOR = {"24h": 6, "72h": 18, "7d": 42}
SC = ["upside_score", "layout_score", "alert_score", "anomaly_score", "accumulation_score", "control_score",
      "ignition_score", "continuation_score", "risk_penalty", "narrative_heat_score"]

D = pd.concat([pd.read_parquet(SP / n / "study3_scores.parquet") for n in ("P1", "P2", "P3")], ignore_index=True)
for c in SC:
    D[c] = pd.to_numeric(D[c], errors="coerce")
D["signal_state"] = D["signal_state"].fillna("").replace("", "none")
print("rows", len(D), "dates", D.ts.nunique(), "coins", D.symbol.nunique(), "avg coins/date", round(len(D) / D.ts.nunique(), 1))

opens, funds, ends = {}, {}, {}
for sym in D.symbol.unique():
    base = sym.split("/")[0]
    o = pd.read_parquet(FUT / f"{base}_USDT" / "4h.parquet", columns=["open"])["open"]
    o = o[~o.index.duplicated(keep="last")].sort_index(); opens[sym] = o
    fp = FUT / f"{base}_USDT" / "funding_rate.parquet"
    funds[sym] = pd.read_parquet(fp, columns=["funding_rate"])["funding_rate"].sort_index().cumsum() if fp.exists() else None
    ends[sym] = o.index[-1]
delisted = {s for s, e in ends.items() if e < STORE_END - pd.Timedelta(days=3)}
print("delisted coins that appeared in the PIT universe:", len(delisted))

for h, n in HOR.items():
    rr, ff, tr = [], [], []
    for sym, g in D.groupby("symbol"):
        s = opens[sym]; idx = s.index
        i0 = np.minimum(idx.searchsorted(g.ts.values), len(idx) - 1)
        ok0 = idx[i0] == pd.DatetimeIndex(g.ts.values)
        i1 = i0 + n
        beyond = i1 >= len(idx)
        i1c = np.minimum(i1, len(idx) - 1)
        cont = (idx[i1c] - idx[i0]) <= pd.Timedelta(hours=4 * n)
        gap_ok = cont & ((idx[i1c] - idx[i0]) == pd.Timedelta(hours=4 * n)) | (beyond & (sym in delisted))
        # exit at last available open for delisted coins
        r = np.where(ok0 & gap_ok, s.values[i1c] / s.values[i0] - 1, np.nan)
        rr.append(pd.Series(r, index=g.index)); tr.append(pd.Series(beyond & (sym in delisted) & ok0, index=g.index))
        cs = funds[sym]
        if cs is None:
            ff.append(pd.Series(np.nan, index=g.index)); continue
        fi = cs.index
        t0 = pd.DatetimeIndex(g.ts.values); t1 = t0 + pd.Timedelta(hours=4 * n)
        a = cs.values[np.clip(fi.searchsorted(t1, side="left") - 1, 0, len(cs) - 1)]
        b = cs.values[np.clip(fi.searchsorted(t0, side="left") - 1, 0, len(cs) - 1)]
        ff.append(pd.Series(np.where(np.isnan(r), np.nan, a - b), index=g.index))
    D["r_" + h] = pd.concat(rr); D["f_" + h] = pd.concat(ff)
    D["del_" + h] = pd.concat(tr)
print("forward windows that ended at a delisting (exit at last price):", int(D["del_7d"].sum()), "of", len(D))


def nw(x, lag):
    x = np.asarray(x, float); x = x[~np.isnan(x)]; n = len(x)
    if n < 15:
        return float("nan"), float("nan")
    m = x.mean(); e = x - m; v = (e * e).sum() / n
    for l in range(1, lag + 1):
        v += 2 * (1 - l / (lag + 1)) * (e[l:] * e[:-l]).sum() / n
    return m, m / math.sqrt(max(v, 1e-18) / n)


pd.set_option("display.width", 220)
# ---------------- IC ----------------
print("\n===== PIT universe: mean rank IC (NW t) =====")
rows = []
halves = {"H1 2025-04..2025-12": D.ts < "2026-01-01", "H2 2026-01..2026-10": D.ts >= "2026-01-01"}
for sc in SC:
    for h, n in HOR.items():
        rec = {"score": sc, "h": h}
        for lab, mask in [("ALL", D.ts >= "2000-01-01")] + list(halves.items()):
            ics = []
            for ts, g in D[mask].groupby("ts"):
                gg = g[[sc, "r_" + h]].dropna()
                if len(gg) >= 40 and gg[sc].nunique() > 5:
                    ics.append(gg[sc].rank().corr(gg["r_" + h].rank()))
            m, t = nw(ics, max(1, math.ceil(n / 6)))
            rec[lab] = f"{m:+.3f}({t:+.1f})"
        rows.append(rec)
R = pd.DataFrame(rows)
for h in HOR:
    print(f"-- {h}"); print(R[R.h == h].drop(columns="h").to_string(index=False))

# ---------------- state event studies ----------------
print("\n===== state tags: flagged minus same-date unflagged, NW t =====")
for lab, mask in [("ALL", D.ts >= "2000-01-01")] + list(halves.items()):
    sub = D[mask]; rows = []
    for st in ("异动启动", "高控盘警戒", "派发风险"):
        for h, n in HOR.items():
            dif = []
            for ts, g in sub.groupby("ts"):
                f = g[g.signal_state == st]["r_" + h].dropna(); r = g[g.signal_state != st]["r_" + h].dropna()
                if len(f) and len(r) > 30:
                    dif.append(f.mean() - r.mean())
            m, t = nw(dif, max(1, math.ceil(n / 6)))
            rows.append({"state": st, "h": h, "v": f"{m:+.4f}(t={t:+.1f},n={len(dif)})"})
    print(f"-- {lab}"); print(pd.DataFrame(rows).pivot(index="state", columns="h", values="v")[list(HOR)].to_string())

# ---------------- strategy test: long 高控盘警戒 (72h hold) ----------------
print("\n===== Strategy: long every 高控盘警戒 coin, hold 72h; net of 0.16% RT + funding; vs liquidity-matched control =====")
rng = np.random.default_rng(5)
D["liq_rank"] = D.groupby("ts")["dv30"].rank(ascending=False)
dates = sorted(D.ts.unique())


def run(offset, state="高控盘警戒", h="72h", step=3, sub=None):
    out = []
    for ts in dates[offset::step]:
        g = D[D.ts == ts]
        if sub is not None and not sub(ts):
            continue
        g = g.dropna(subset=["r_" + h])
        fl = g[g.signal_state == state]; rest = g[g.signal_state != state]
        if len(fl) < 2 or len(rest) < 30:
            continue
        net = (fl["r_" + h] - fl["f_" + h].fillna(0)).mean() - COST
        uni = (rest["r_" + h] - rest["f_" + h].fillna(0)).mean() - COST
        # liquidity-matched: nearest liq_rank among unflagged for each flagged coin
        mt = []
        rl = rest["liq_rank"].values; rv = (rest["r_" + h] - rest["f_" + h].fillna(0)).values
        for lr in fl["liq_rank"].values:
            mt.append(rv[np.argmin(np.abs(rl - lr))])
        matched = np.mean(mt) - COST
        out.append({"ts": ts, "net": net, "uni": uni, "matched": matched, "n": len(fl), "syms": list(fl.symbol),
                    "ex_uni": net - uni, "ex_match": net - matched, "dv_rank": fl.liq_rank.mean()})
    return pd.DataFrame(out)


allrows = []
for off in range(3):
    P = run(off)
    allrows.append(P)
    m1, t1 = nw(P.net, 1); m2, t2 = nw(P.ex_uni, 1); m3, t3 = nw(P.ex_match, 1)
    print(f"offset {off}: periods={len(P)} avg flagged/date={P.n.mean():.1f} avg liq-rank={P.dv_rank.mean():.0f}/150 | net {m1:+.4f}(t{t1:+.1f}) excess-vs-universe {m2:+.4f}(t{t2:+.1f}) excess-vs-liq-matched {m3:+.4f}(t{t3:+.1f}) hit {np.mean(P.ex_uni>0):.2f}")
P = pd.concat(allrows)
print("mean over offsets: net %+.4f  ex-universe %+.4f  ex-liq-matched %+.4f" % (P.net.mean(), P.ex_uni.mean(), P.ex_match.mean()))
for lab, f in [("H1", lambda ts: ts < pd.Timestamp("2026-01-01")), ("H2", lambda ts: ts >= pd.Timestamp("2026-01-01"))]:
    Q = pd.concat([run(o, sub=f) for o in range(3)])
    print(f"  {lab}: net {Q.net.mean():+.4f} ex-uni {Q.ex_uni.mean():+.4f} ex-match {Q.ex_match.mean():+.4f} (periods {len(Q)})")

# permutation null: same count of random coins per date
P0 = run(0)
obs = P0.ex_uni.mean()
null = []
Dd = D.dropna(subset=["r_72h"])
groups = {ts: g for ts, g in Dd.groupby("ts")}
for _ in range(400):
    vals = []
    for _, r in P0.iterrows():
        g = groups[r.ts]; k = int(r.n)
        pick = g.sample(k, random_state=int(rng.integers(1e9)))
        vals.append((pick["r_72h"] - pick["f_72h"].fillna(0)).mean() - COST - r.uni)
    null.append(np.mean(vals))
null = np.array(null)
print(f"permutation (random same-count picks): null mean {null.mean():+.4f} sd {null.std():.4f}; observed {obs:+.4f}; p={np.mean(null >= obs):.3f}")

# ex-top-3 coin check
contrib = {}
for _, r in P0.iterrows():
    g = groups[r.ts].set_index("symbol")
    for s in r.syms:
        v = g.loc[s, "r_72h"] - (0 if np.isnan(g.loc[s, "f_72h"]) else g.loc[s, "f_72h"])
        contrib[s] = contrib.get(s, 0) + (v - COST - r.uni) / len(r.syms)
cs = pd.Series(contrib).sort_values(ascending=False)
print("top contributors:", cs.head(6).round(3).to_dict(), "| total", round(cs.sum(), 3), "ex-top3", round(cs.sum() - cs.head(3).sum(), 3),
      "| #coins", len(cs), "periods", len(P0))
print("delisted coins among flagged picks:", len(set(cs.index) & delisted))
