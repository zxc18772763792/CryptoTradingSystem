"""Study 3b: does the radar's accumulation / continuation structure survive as a portfolio?
PIT survivorship-free universe, weekly non-overlapping 7d holds, net of costs + funding.
Signs/weights fixed on H1 (2025-04..2025-12) only; H2 (2026-01..2026-10) is out-of-sample."""
from __future__ import annotations
import math, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SP = Path(sys.argv[1])
FUT = ROOT / "data/historical/binance_futures"
STORE_END = pd.Timestamp("2026-10-20 00:00")
COST_LEG = 0.0016
SC = ["upside_score", "layout_score", "alert_score", "anomaly_score", "accumulation_score", "control_score",
      "ignition_score", "continuation_score", "risk_penalty", "narrative_heat_score"]
n = 42

D = pd.concat([pd.read_parquet(SP / k / "study3_scores.parquet") for k in ("P1", "P2", "P3")], ignore_index=True)
for c in SC:
    D[c] = pd.to_numeric(D[c], errors="coerce")
opens, funds, ends = {}, {}, {}
for sym in D.symbol.unique():
    base = sym.split("/")[0]
    o = pd.read_parquet(FUT / f"{base}_USDT" / "4h.parquet", columns=["open"])["open"]
    opens[sym] = o[~o.index.duplicated(keep="last")].sort_index(); ends[sym] = opens[sym].index[-1]
    fp = FUT / f"{base}_USDT" / "funding_rate.parquet"
    funds[sym] = pd.read_parquet(fp, columns=["funding_rate"])["funding_rate"].sort_index().cumsum() if fp.exists() else None
delisted = {s for s, e in ends.items() if e < STORE_END - pd.Timedelta(days=3)}
rr, ff = [], []
for sym, g in D.groupby("symbol"):
    s = opens[sym]; idx = s.index
    i0 = np.minimum(idx.searchsorted(g.ts.values), len(idx) - 1)
    ok0 = idx[i0] == pd.DatetimeIndex(g.ts.values)
    i1 = i0 + n; beyond = i1 >= len(idx); i1c = np.minimum(i1, len(idx) - 1)
    full = (idx[i1c] - idx[i0]) == pd.Timedelta(hours=4 * n)
    ok = ok0 & (full | (beyond & (sym in delisted)))
    r = np.where(ok, s.values[i1c] / s.values[i0] - 1, np.nan)
    rr.append(pd.Series(r, index=g.index))
    cs = funds[sym]
    if cs is None:
        ff.append(pd.Series(0.0, index=g.index)); continue
    fi = cs.index; t0 = pd.DatetimeIndex(g.ts.values); t1 = t0 + pd.Timedelta(hours=4 * n)
    a = cs.values[np.clip(fi.searchsorted(t1, side="left") - 1, 0, len(cs) - 1)]
    b = cs.values[np.clip(fi.searchsorted(t0, side="left") - 1, 0, len(cs) - 1)]
    ff.append(pd.Series(np.where(np.isnan(r), np.nan, a - b), index=g.index))
D["r"] = pd.concat(rr); D["f"] = pd.concat(ff).fillna(0.0)
D["liq_rank"] = D.groupby("ts")["dv30"].rank(ascending=False)
D = D.dropna(subset=["r"])


def zr(s):
    return (s.rank() - 0.5) / s.count() - 0.5


def nw(x, lag=1):
    x = np.asarray(x, float); x = x[~np.isnan(x)]; m = len(x)
    if m < 12:
        return float("nan"), float("nan")
    mu = x.mean(); e = x - mu; v = (e * e).sum() / m
    for l in range(1, lag + 1):
        v += 2 * (1 - l / (lag + 1)) * (e[l:] * e[:-l]).sum() / m
    return mu, mu / math.sqrt(max(v, 1e-18) / m)


H1 = D.ts < "2026-01-01"; H2 = ~H1
# signs on H1 by 7d IC
signs = {}
for c in SC:
    ics = []
    for ts, g in D[H1].groupby("ts"):
        gg = g[[c, "r"]].dropna()
        if len(gg) >= 40 and gg[c].nunique() > 5:
            ics.append(gg[c].rank().corr(gg["r"].rank()))
    signs[c] = 1.0 if np.mean(ics) > 0 else -1.0
print("signs from H1:", signs)
for c in SC:
    D["z_" + c] = D.groupby("ts")[c].transform(zr)
D["sig_acc"] = D["z_accumulation_score"]
D["sig_negcont"] = -D["z_continuation_score"]
D["sig_comp"] = sum(signs[c] * D["z_" + c] for c in SC) / len(SC)
D["sig_acc_cont"] = (D["z_accumulation_score"] - D["z_continuation_score"]) / 2
D["sig_naive_priority"] = (0.22 * D.layout_score + 0.18 * D.alert_score + 0.18 * D.ignition_score + 0.12 * D.control_score + 0.08 * D.narrative_heat_score + 0.06 * D.upside_score)
D["sig_naive_priority"] = D.groupby("ts")["sig_naive_priority"].transform(zr)


def quint(col):
    D["q"] = D.groupby("ts")[col].transform(lambda s: pd.qcut(s.rank(method="first"), 5, labels=False))
    out = {}
    for per, mask in (("H1", H1), ("H2", H2)):
        g = D[mask].copy(); g["xr"] = g.r - g.f - g.groupby("ts")["r"].transform("mean")
        t = g.groupby("q").agg(mean=("r", "mean"), median=("r", "median"), excess_mean=("xr", "mean"), p95=("r", lambda x: x.quantile(0.95)), p05=("r", lambda x: x.quantile(0.05)))
        out[per] = t
    return out
for col in ("accumulation_score", "continuation_score", "narrative_heat_score"):
    res = quint(col)
    print("=== 7d return by quintile of", col, "(q0 lowest -> q4 highest), PIT universe ===")
    for per, t in res.items():
        print(per); print((t * 100).round(2).to_string())
