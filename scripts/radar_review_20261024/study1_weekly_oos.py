"""Study 1: out-of-sample forward performance of the archived weekly pump lists.

Model frozen 2026-07-19 (train_end 2026-02-15). Every archived list since is OOS.
For each list (full_ranking of ~125 coins) compute forward returns from the first
1h bar after generation, then compare top-N vs the rest of the same list.
Long-only spot-style returns (no funding, no costs) -- a ceiling for the strategy.
"""
from __future__ import annotations
import glob, json, math, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
FUT = ROOT / "data/historical/binance_futures"
SPOT = ROOT / "data/historical/binance"
LISTS = ["2026-07-20", "2026-08-03", "2026-08-10", "2026-09-07", "2026-09-14",
         "2026-09-21", "2026-09-28", "2026-10-05", "2026-10-12"]
HORIZONS = [7, 14, 30]
END = pd.Timestamp("2026-10-20 00:00")  # local store tail

_cache: dict[str, pd.DataFrame | None] = {}


def load(base: str):
    if base in _cache:
        return _cache[base]
    df = None
    cands = [base]
    if base.startswith("1000"):
        pass
    else:
        cands += ["1000" + base]
    for b in cands:
        for root in (FUT, SPOT):
            p = root / f"{b}_USDT" / "1h.parquet"
            if p.exists():
                df = pd.read_parquet(p, columns=["open", "high", "low", "close"])
                df = df[~df.index.duplicated(keep="last")].sort_index()
                break
        if df is not None:
            break
    _cache[base] = df
    return df


def fwd(df: pd.DataFrame, t0: pd.Timestamp, days: int):
    """entry = open of first 1h bar >= t0; exit = close of bar at entry+days-1h."""
    i0 = df.index.searchsorted(t0)
    if i0 >= len(df):
        return None
    t_entry = df.index[i0]
    if (t_entry - t0) > pd.Timedelta(hours=3):
        return None  # coin not trading at that time
    entry = float(df["open"].iloc[i0])
    t_end = t_entry + pd.Timedelta(days=days) - pd.Timedelta(hours=1)
    if t_end > END:
        return None  # window not complete
    i1 = df.index.searchsorted(t_end, side="right") - 1
    seg = df.iloc[i0 : i1 + 1]
    truncated = (df.index[i1] < t_end - pd.Timedelta(hours=2))  # delisted / data gap
    ex = float(seg["close"].iloc[-1])
    return {
        "ret": ex / entry - 1,
        "maxup": float(seg["high"].max()) / entry - 1,
        "maxdd": float(seg["low"].min()) / entry - 1,
        "trunc": bool(truncated),
    }


rows = []
for d in LISTS:
    j = json.load(open(ROOT / f"data/research/pump_watchlist/watchlist_{d}.json", encoding="utf-8"))
    gen = pd.Timestamp(j["generated_at"]).tz_convert("UTC").tz_localize(None)
    t0 = gen.ceil("h")
    fr = j["full_ranking"]
    for rk, it in enumerate(fr, start=1):
        base = it["base"]
        df = load(base)
        if df is None:
            continue
        for H in HORIZONS:
            r = fwd(df, t0, H)
            if r is None:
                continue
            rows.append({"list": d, "rank": rk, "base": base, "score": it["score"], "H": H, **r})

R = pd.DataFrame(rows)
R.to_csv(Path(sys.argv[1]) / "study1_rows.csv", index=False)
print("rows", len(R), "lists", R["list"].nunique())

out = []
for H in HORIZONS:
    for d, g in R[R.H == H].groupby("list"):
        if len(g) < 40:
            continue
        top = g[g["rank"] <= 15]
        rest = g[g["rank"] > 15]
        ic = g["score"].rank().corr(g["ret"].rank())
        out.append({
            "H": H, "list": d, "n": len(g),
            "top15_mean": top["ret"].mean(), "top15_med": top["ret"].median(),
            "rest_mean": rest["ret"].mean(), "rest_med": rest["ret"].median(),
            "uni_mean": g["ret"].mean(), "uni_med": g["ret"].median(),
            "excess_mean": top["ret"].mean() - g["ret"].mean(),
            "top15_win": (top["ret"] > 0).mean(), "uni_win": (g["ret"] > 0).mean(),
            "top15_pump": (top["maxup"] >= 1.0).mean(), "rest_pump": (rest["maxup"] >= 1.0).mean(),
            "top15_up50": (top["maxup"] >= 0.5).mean(), "rest_up50": (rest["maxup"] >= 0.5).mean(),
            "top15_maxdd_med": top["maxdd"].median(),
            "top5_mean": g[g["rank"] <= 5]["ret"].mean(),
            "ic": ic,
        })
O = pd.DataFrame(out)
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 40)
for H in HORIZONS:
    s = O[O.H == H]
    print(f"\n===== H={H}d  (lists={len(s)}) =====")
    print(s.drop(columns=["H"]).round(3).to_string(index=False))
    if len(s) >= 3:
        for col in ["excess_mean", "ic"]:
            x = s[col].values
            t = x.mean() / (x.std(ddof=1) / math.sqrt(len(x)))
            print(f"  {col}: mean {x.mean():+.4f}  t={t:+.2f}  positive {int((x>0).sum())}/{len(x)}")
        print(f"  top15 mean {s.top15_mean.mean():+.3f} med {s.top15_med.mean():+.3f} | uni mean {s.uni_mean.mean():+.3f} med {s.uni_med.mean():+.3f}")
        print(f"  pump>=100% top15 {s.top15_pump.mean():.3f} vs rest {s.rest_pump.mean():.3f};  up>=50% top15 {s.top15_up50.mean():.3f} vs rest {s.rest_up50.mean():.3f}")
O.to_csv(Path(sys.argv[1]) / "study1_summary.csv", index=False)
