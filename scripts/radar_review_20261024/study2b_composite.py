"""Study 2b: sign-only composite of the radar's component scores, signs fixed on period B
(2025-04..2026-01), evaluated out-of-sample on period A (2026-02..2026-10).
Weekly non-overlapping 7d holds, gross / net of costs and funding, permutation null."""
from __future__ import annotations
import math, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SP = Path(sys.argv[1])
FUT = ROOT / "data/historical/binance_futures"
COMP = ["layout_score", "alert_score", "anomaly_score", "accumulation_score", "control_score",
        "ignition_score", "continuation_score", "upside_score", "risk_penalty", "narrative_heat_score"]
COST_LEG = 0.0016  # round trip per leg (taker 5bp x2 + 3bp slippage x2)

parts = []
for tag in ("A", "B"):
    d = pd.read_parquet(SP / tag / "study2_scores.parquet"); d["period"] = tag; parts.append(d)
D = pd.concat(parts, ignore_index=True)
for c in COMP + ["liquidity_trap_score"]:
    D[c] = pd.to_numeric(D[c], errors="coerce")
D["priority_proxy"] = (0.22 * D.layout_score + 0.18 * D.alert_score + 0.18 * D.ignition_score + 0.12 * D.control_score
                       + 0.08 * D.narrative_heat_score + 0.06 * D.upside_score)
print("identical risk_penalty vs liquidity_trap_score:", bool((D.risk_penalty - D.liquidity_trap_score).abs().max() < 1e-9))

# ---- forward 7d return (open->open, 4h bars) and perp funding over the hold ----
H = 42
fwd = {}; fund = {}
for sym in D.symbol.unique():
    b = sym.split("/")[0]
    for cand in (b, "1000" + b):
        p = FUT / f"{cand}_USDT" / "4h.parquet"
        if p.exists():
            o = pd.read_parquet(p, columns=["open"])["open"]
            o = o[~o.index.duplicated(keep="last")].sort_index()
            fwd[sym] = o
            fp = FUT / f"{cand}_USDT" / "funding_rate.parquet"
            fund[sym] = pd.read_parquet(fp, columns=["funding_rate"])["funding_rate"].sort_index() if fp.exists() else None
            break
r7 = []; f7 = []
for sym, g in D.groupby("symbol"):
    s = fwd[sym]; idx = s.index
    i0 = np.minimum(idx.searchsorted(g.ts.values), len(idx) - 1)
    ok0 = idx[i0] == pd.DatetimeIndex(g.ts.values)
    i1 = i0 + H; ok1 = i1 < len(idx); i1c = np.minimum(i1, len(idx) - 1)
    cont = (idx[i1c] - idx[i0]) == pd.Timedelta(hours=4 * H)
    r = np.where(ok0 & ok1 & cont, s.values[i1c] / s.values[i0] - 1, np.nan)
    r7.append(pd.Series(r, index=g.index))
    fs = fund.get(sym)
    if fs is None:
        f7.append(pd.Series(np.nan, index=g.index)); continue
    cs = fs.cumsum(); fi = cs.index
    t0 = pd.DatetimeIndex(g.ts.values); t1 = t0 + pd.Timedelta(hours=4 * H)
    a = cs.values[np.clip(fi.searchsorted(t1, side="left") - 1, 0, len(cs) - 1)]
    bb = cs.values[np.clip(fi.searchsorted(t0, side="left") - 1, 0, len(cs) - 1)]
    f7.append(pd.Series(np.where(ok0 & ok1 & cont, a - bb, np.nan), index=g.index))
D["r7"] = pd.concat(r7); D["f7"] = pd.concat(f7)  # f7: sum of funding rates paid by longs over hold


def zrank(s):
    return (s.rank(method="average") - 0.5) / s.count() - 0.5


def per_date_ic(df, col, y="r7"):
    out = {}
    for ts, g in df.groupby("ts"):
        gg = g[[col, y]].dropna()
        if len(gg) >= 30 and gg[col].nunique() > 5:
            out[ts] = gg[col].rank().corr(gg[y].rank())
    return pd.Series(out)


def nw(x, lag):
    x = np.asarray(x, float); x = x[~np.isnan(x)]; n = len(x)
    m = x.mean(); e = x - m; v = (e * e).sum() / n
    for l in range(1, lag + 1):
        v += 2 * (1 - l / (lag + 1)) * (e[l:] * e[:-l]).sum() / n
    return m, m / math.sqrt(max(v, 1e-18) / n)


# ---- signs from period B only ----
B = D[D.period == "B"]; A = D[D.period == "A"]
sign = {}
print("\ncomponent mean 7d IC: B (discovery) | A (out-of-sample)")
for c in COMP + ["priority_proxy"]:
    ib = per_date_ic(B, c); ia = per_date_ic(A, c)
    sign[c] = 1.0 if ib.mean() > 0 else -1.0
    print(f"  {c:22s} B {ib.mean():+.4f} (t {nw(ib,7)[1]:+.1f}) | A {ia.mean():+.4f} (t {nw(ia,7)[1]:+.1f})  sign_B={sign[c]:+.0f}  consistent={np.sign(ib.mean())==np.sign(ia.mean())}")

keep = [c for c in COMP if c != "priority_proxy"]
D["comp"] = 0.0
for c in keep:
    D["comp"] += sign[c] * D.groupby("ts")[c].transform(zrank).fillna(0.0)
D["comp"] /= len(keep)
D["pp"] = D.groupby("ts")["priority_proxy"].transform(zrank)
D["rp"] = D.groupby("ts")["risk_penalty"].transform(zrank)
B = D[D.period == "B"]; A = D[D.period == "A"]
for lab, df in (("B (in-sample)", B), ("A (OUT-OF-SAMPLE)", A)):
    ic = per_date_ic(df, "comp")
    print(f"\ncomposite IC {lab}: mean {ic.mean():+.4f}  NW t {nw(ic,7)[1]:+.2f}  pos {np.mean(ic>0):.2f}  n={len(ic)}")

# ---- weekly portfolios, 7 offsets, A period ----
rng = np.random.default_rng(3)


def weekly_ports(df, col, offset, q=0.2, long_only=False):
    dates = sorted(df.ts.unique())[offset::7]
    rows = []
    for ts in dates:
        g = df[df.ts == ts][[col, "r7", "f7", "symbol"]].dropna(subset=[col, "r7"])
        if len(g) < 40:
            continue
        k = max(4, int(len(g) * q))
        top = g.nlargest(k, col); bot = g.nsmallest(k, col)
        f_top = top["f7"].fillna(0).mean(); f_bot = bot["f7"].fillna(0).mean()
        lg = top.r7.mean() - f_top          # long pays funding
        sh = -(bot.r7.mean()) + f_bot       # short receives funding
        uni = g.r7.mean()
        rows.append({"ts": ts, "long": lg, "short": sh, "uni": uni, "ls_gross": top.r7.mean() - bot.r7.mean(),
                     "ls_net": lg + sh - 2 * COST_LEG, "long_net": lg - COST_LEG, "ex_net": lg - COST_LEG - uni,
                     "top_syms": list(top.symbol), "bot_syms": list(bot.symbol)})
    return pd.DataFrame(rows)


for lab, df in (("A OUT-OF-SAMPLE", A), ("B in-sample", B)):
    for col in ("comp", "pp", "rp"):
        res = []
        for off in range(7):
            P = weekly_ports(df, col, off)
            res.append(P[["ls_gross", "ls_net", "long_net", "ex_net", "uni"]].mean())
        R = pd.concat(res, axis=1)
        P0 = weekly_ports(df, col, 0)
        m, t = nw(P0.ls_net, 1)
        print(f"\n[{lab}] portfolio by {col}: weeks/offset={len(P0)}, mean across 7 offsets (per week):")
        print("   " + "  ".join(f"{k}={v:+.4f}" for k, v in R.mean(axis=1).items()), f"| LS net t(offset0)={t:+.2f}  hit={np.mean(P0.ls_net>0):.2f}")
    # permutation null for composite LS net
    null = []
    P0 = weekly_ports(df, "comp", 0)
    obs = P0.ls_net.mean()
    dd = df.copy()
    for _ in range(300):
        dd["shuf"] = dd.groupby("ts")["comp"].transform(lambda s: pd.Series(rng.permutation(s.values), index=s.index))
        null.append(weekly_ports(dd, "shuf", 0).ls_net.mean())
    null = np.array(null)
    print(f"   permutation null LS net/week: mean {null.mean():+.4f} sd {null.std():.4f}; observed {obs:+.4f}; p(one-sided)={np.mean(null>=obs):.3f}")

# ex-top3 coin check on A, composite long-short
P = weekly_ports(A, "comp", 0)
contrib = {}
for _, r in P.iterrows():
    gA = A[A.ts == r.ts].set_index("symbol")
    for s in r.top_syms:
        contrib[s] = contrib.get(s, 0) + (gA.loc[s, "r7"] - (gA.loc[s, "f7"] if not np.isnan(gA.loc[s, "f7"]) else 0)) / len(r.top_syms)
    for s in r.bot_syms:
        contrib[s] = contrib.get(s, 0) + (-gA.loc[s, "r7"] + (gA.loc[s, "f7"] if not np.isnan(gA.loc[s, "f7"]) else 0)) / len(r.bot_syms)
cs = pd.Series(contrib).sort_values(ascending=False)
total = cs.sum()
print("\nA composite LS: top contributors", cs.head(5).round(3).to_dict(), "total", round(total, 3), "ex-top3", round(total - cs.head(3).sum(), 3), "weeks", len(P))
