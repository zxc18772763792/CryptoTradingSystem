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

dates = sorted(D.ts.unique())


def ports(df, col, offset, q=0.2, liq_max=None):
    out = []
    for ts in dates[offset::7]:
        g = df[df.ts == ts]
        if liq_max is not None:
            g = g[g.liq_rank <= liq_max]
        if len(g) < 30:
            continue
        k = max(4, int(len(g) * q))
        top = g.nlargest(k, col); bot = g.nsmallest(k, col)
        lg = (top.r - top.f).mean(); sh = (-(bot.r) + bot.f).mean(); uni = (g.r - g.f).mean()
        out.append({"ts": ts, "ls": lg + sh - 2 * COST_LEG, "long_ex": lg - COST_LEG - uni, "hedged": lg - COST_LEG - uni,
                    "long": lg - COST_LEG, "uni": uni, "top": list(top.symbol), "bot": list(bot.symbol),
                    "ls_gross": (top.r.mean() - bot.r.mean())})
    return pd.DataFrame(out)


rng = np.random.default_rng(11)
print("\n=== weekly portfolios (mean over 7 offsets), per-week returns ===")
for col, lab in [("sig_acc", "accumulation_score (long high)"), ("sig_negcont", "-continuation_score"), ("sig_acc_cont", "accumulation - continuation"),
                 ("sig_comp", "H1-sign composite of 10 radar scores"), ("sig_naive_priority", "radar PRIORITY (what the board ranks by)")]:
    for per, mask in (("H1 in-sample", H1), ("H2 OUT-OF-SAMPLE", H2)):
        sub = D[mask]
        res = [ports(sub, col, o) for o in range(7)]
        M = pd.concat([r[["ls", "ls_gross", "long", "long_ex", "uni"]].mean() for r in res], axis=1).mean(axis=1)
        P0 = res[0]
        m, t = nw(P0.ls, 1); m2, t2 = nw(P0.long_ex, 1)
        print(f"[{per:17s}] {lab:42s} weeks={len(P0)}  LS gross {M.ls_gross:+.4f} LS net {M.ls:+.4f}(t{t:+.1f})  long-only net {M.long:+.4f}  long-ex-universe {M.long_ex:+.4f}(t{t2:+.1f})  uni {M.uni:+.4f}  hit {np.mean(P0.ls>0):.2f}")


print("=== liquidity-restricted variants: H1 vs H2 (7 offsets mean, net of cost+funding) ===")
for col in ("sig_acc", "sig_acc_cont", "sig_naive_priority"):
    for lm in (40, 50, 75, 100):
        line = []
        for per, mask in (("H1", H1), ("H2", H2)):
            res = [ports(D[mask], col, o, liq_max=lm) for o in range(7)]
            M = pd.concat([r[["ls", "ls_gross", "long_ex"]].mean() for r in res], axis=1).mean(axis=1)
            P0 = res[0]; m, t = nw(P0.ls, 1)
            line.append(f"{per}: gross {M.ls_gross:+.4f} net {M.ls:+.4f}(t{t:+.1f}) long-ex-uni {M.long_ex:+.4f}")
        print(f"{col:20s} liq<= {lm:3d} | " + " | ".join(line))
# permutation for the liquid-50 acc-cont in H2 (all offsets averaged is costly; use offset 0)
sub = D[H2].copy(); sub = sub[sub.liq_rank <= 50]
Pobs = ports(sub, "sig_acc_cont", 0); obs = Pobs.ls.mean(); null = []
for _ in range(300):
    sub["shuf"] = sub.groupby("ts")["sig_acc_cont"].transform(lambda s: pd.Series(rng.permutation(s.values), index=s.index))
    null.append(ports(sub, "shuf", 0).ls.mean())
print(f"liq<=50 acc-cont H2 offset0: obs {obs:+.4f} null {np.mean(null):+.4f}+-{np.std(null):.4f} p={np.mean(np.array(null)>=obs):.3f}")
