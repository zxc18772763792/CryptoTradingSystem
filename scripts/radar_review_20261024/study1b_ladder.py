"""Study 1b: the model's own tradable form (ladder basket) OOS + event dedupe + lottery diagnostics."""
from __future__ import annotations
import json, math, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
FUT = ROOT / "data/historical/binance_futures"
SPOT = ROOT / "data/historical/binance"
LISTS = ["2026-07-20", "2026-08-03", "2026-08-10", "2026-09-07", "2026-09-14",
         "2026-09-21", "2026-09-28", "2026-10-05", "2026-10-12"]
END = pd.Timestamp("2026-10-20 00:00")
FEE = 0.001  # per side, spot taker-ish
H = 30
_c = {}


def load(base):
    if base in _c:
        return _c[base]
    df = None
    for b in [base, "1000" + base]:
        for root in (FUT, SPOT):
            p = root / f"{b}_USDT" / "1h.parquet"
            if p.exists():
                df = pd.read_parquet(p, columns=["open", "high", "low", "close"])
                df = df[~df.index.duplicated(keep="last")].sort_index()
                break
        if df is not None:
            break
    _c[base] = df
    return df


def ladder(df, t0, hold_days, stop=None):
    i0 = df.index.searchsorted(t0)
    if i0 >= len(df) or (df.index[i0] - t0) > pd.Timedelta(hours=3):
        return None
    entry = float(df["open"].iloc[i0])
    t_end = df.index[i0] + pd.Timedelta(days=hold_days) - pd.Timedelta(hours=1)
    last_avail = df.index[-1]
    t_cap = min(t_end, END)
    seg = df.iloc[i0 : df.index.searchsorted(t_cap, side="right")]
    path = seg["close"].values / entry
    complete = t_end <= END
    remaining, realized = 1.0, 0.0
    hit2 = (path >= 2.0).any(); hit4 = (path >= 4.0).any()
    if hit2:
        realized += 0.40 * 1.0; remaining -= 0.40
    if hit4:
        realized += 0.30 * 3.0; remaining -= 0.30
    final = path[-1]
    total = realized + remaining * (final - 1.0)
    return {"ladder": total - 2 * FEE, "hold": final - 1.0 - 2 * FEE, "hit2": bool(hit2), "complete": complete,
            "maxup": float(seg["high"].max() / entry - 1)}


rows = []
for d in LISTS:
    j = json.load(open(ROOT / f"data/research/pump_watchlist/watchlist_{d}.json", encoding="utf-8"))
    t0 = pd.Timestamp(j["generated_at"]).tz_convert("UTC").tz_localize(None).ceil("h")
    for rk, it in enumerate(j["full_ranking"], start=1):
        df = load(it["base"])
        if df is None:
            continue
        for hd in (H,):
            r = ladder(df, t0, hd)
            if r is None:
                continue
            rows.append({"list": d, "rank": rk, "base": it["base"], "score": it["score"], "hd": hd, **r})
R = pd.DataFrame(rows)
print("rows", len(R))
pd.set_option("display.width", 220)

# Only fully complete 30d windows for the headline
C = R[R.complete]
print("lists with complete 30d windows:", sorted(C["list"].unique()))
rng = np.random.default_rng(7)
res = []
for d, g in C.groupby("list"):
    g = g.sort_values("rank")
    rec = {"list": d, "n": len(g)}
    for K in (5, 10, 15, 25):
        rec[f"top{K}_ladder"] = g.head(K)["ladder"].mean()
        rec[f"top{K}_hold"] = g.head(K)["hold"].mean()
    rec["uni_ladder"] = g["ladder"].mean(); rec["uni_hold"] = g["hold"].mean()
    draws = [g["ladder"].sample(15, random_state=int(rng.integers(1e9))).mean() for _ in range(300)]
    rec["rand15_ladder"] = float(np.mean(draws))
    rec["top15_vs_rand_pctile"] = float(np.mean(np.array(draws) < g.head(15)["ladder"].mean()))
    res.append(rec)
O = pd.DataFrame(res)
print(O.round(3).to_string(index=False))
print("\nmean across lists:")
print(O.drop(columns=["list", "n"]).mean().round(3).to_string())

# ---- event dedupe: coin-episode level pump counting among top15 vs rest (30d complete windows) ----
print("\n== pump>=+100% (max high within 30d), de-duplicated by coin ==")
top = C[C["rank"] <= 15]
rest = C[C["rank"] > 15]
tp = top.groupby("base")["maxup"].max()
print("top15 distinct coins:", len(tp), " pumped(>=100%):", int((tp >= 1.0).sum()), sorted(tp[tp >= 1.0].index.tolist()))
rp = rest.groupby("base")["maxup"].max()
print("rest distinct coins:", len(rp), " pumped:", int((rp >= 1.0).sum()), sorted(rp[rp >= 1.0].index.tolist()))
# per-appearance vs per-episode
print("top15 appearances:", len(top), "pump appearances:", int((top.maxup >= 1.0).sum()))
print("rest appearances:", len(rest), "pump appearances:", int((rest.maxup >= 1.0).sum()))
