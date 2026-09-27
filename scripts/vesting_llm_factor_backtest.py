"""Round 6 step 2b: does the supply-inflation factor hold on tokens only an LLM could schedule?

Groups, each ranked on its own every month start (tercile long low / short
high scheduled 90-day supply growth, 30-day hold, 0.4% cost, returns relative
to the group's equal-weight mean), prices from the survivorship-free Binance
spot panel (data/research/delist_risk/close.parquet):
  llama     DefiLlama schedules (the round-5 factor on the bigger price panel)
  llm_new   tokens DefiLlama does NOT cover, schedules from
            scripts/vesting_llm_discover.py + vesting_llm_extract.py
            -> the clean out-of-sample test of both factor and pipeline
  pooled    llama + llm_new ranked together (what an extended tracker would trade)
LLM specs must pass core.research.vesting_spec.usable unless --ungated.
Caveat: docs are read today; a schedule revised after the fact would leak.
"""
import json
import sys
from math import comb
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, ".")
from core.research import unlock_short_tracker as ut
from core.research.unlock_events import entry_ticker, unlocked_series
from core.research.vesting_spec import growth, schedule_from_spec, usable
from core.research.xs_evaluation import spearman

OUT = Path("data/research/vesting_llm")
COST = 0.004
DAYS = pd.date_range("2019-01-01", "2028-12-31", freq="D", tz="UTC")


def sign_p(k, n):
    """One-sided binomial p of >= k right-signed months out of n."""
    return sum(comb(n, i) for i in range(k, n + 1)) / 2 ** n


def panel(curves, close, months):
    rows = []
    for d in months:
        end = d + pd.Timedelta(days=30)
        if d not in close.index or end not in close.index:
            continue
        for t, curve in curves.items():
            if t not in close.columns:
                continue
            p0, p1 = close.at[d, t], close.at[end, t]
            g = growth(curve, d)
            if pd.notna(p0) and pd.notna(p1) and p0 > 0 and np.isfinite(g):
                rows.append({"date": d, "tok": t, "infl90": g, "ret30": p1 / p0 - 1})
    return pd.DataFrame(rows)


def run(P, label, min_tokens=9):
    if P.empty:
        print(f"== {label}: no data")
        return
    P = P[P.groupby("date")["tok"].transform("size") >= min_tokens].copy()
    if P.empty:
        print(f"== {label}: fewer than {min_tokens} tokens in every month")
        return
    P["ret_x"] = P["ret30"] - P.groupby("date")["ret30"].transform("mean")
    P["q"] = P.groupby("date")["infl90"].rank(pct=True)
    spread = P.groupby("date").apply(lambda g: g[g.q <= 1 / 3].ret_x.mean() - g[g.q > 2 / 3].ret_x.mean() - COST).dropna()
    ic = P.groupby("date").apply(lambda g: spearman(g.infl90, g.ret30)).dropna()
    rng = np.random.default_rng(0)
    boot = [spread.sample(len(spread), replace=True, random_state=int(s)).mean() for s in rng.integers(0, 10 ** 9, 2000)]
    k = int((ic < 0).sum())
    early, late = spread[spread.index < "2025-01-01"], spread[spread.index >= "2025-01-01"]
    print(f"== {label}: {len(spread)} months, median {int(P.groupby('date').size().median())} tokens/month")
    print(f"  long low / short high growth, 30d, after costs: {spread.mean() * 100:+.2f}%/month  90% CI [{np.percentile(boot, 5) * 100:+.2f}, {np.percentile(boot, 95) * 100:+.2f}]  positive months {(spread > 0).mean():.0%}")
    print(f"  rank IC negative in {k}/{len(ic)} months (one-sided sign p={sign_p(k, len(ic)):.4f}) | 2023-24 {early.mean() * 100:+.2f}% (n={len(early)}) | 2025-26 {late.mean() * 100:+.2f}% (n={len(late)})")


def main():
    ungated = "--ungated" in sys.argv
    close = pd.read_parquet("data/research/delist_risk/close.parquet")
    close.index = pd.to_datetime(close.index, utc=True)
    close = close.asfreq("1D")
    last_month = (close.index.max() - pd.Timedelta(days=31)).normalize().replace(day=1)
    months = pd.date_range(pd.Timestamp("2023-01-01", tz="UTC"), last_month, freq="MS")

    idx = json.loads((ut.CACHE_DIR / "emissions_index.json").read_text(encoding="utf-8"))
    idx = idx["data"] if isinstance(idx, dict) else idx
    llama = {}
    for e in idx:
        t, slug = entry_ticker(e), e.get("protocolSlug")
        f = ut.CACHE_DIR / "emissions" / f"{slug}.json"
        if t and t not in llama and t in close.columns and f.exists():
            u = unlocked_series(json.loads(f.read_text(encoding="utf-8")))
            if len(u):
                llama[t] = u.asfreq("1D").ffill()
    ex_path = OUT / "discovered_extractions.json"
    ex = json.loads(ex_path.read_text(encoding="utf-8")) if ex_path.exists() else {}
    new, rejected = {}, 0
    for t, e in ex.items():
        if t in llama or e.get("covered"):
            continue
        spec = e.get("docs") or {}
        if "error" in spec or not spec.get("allocations"):
            rejected += 1
            continue
        curve, known = schedule_from_spec(spec, e.get("binance_first_day"), DAYS)
        if known < 0.3 or (not ungated and not usable(spec, known)):
            rejected += 1
            continue
        new[t] = curve
    print(f"DefiLlama tokens with prices: {len(llama)} | LLM-only tokens usable: {len(new)} (rejected {rejected}){' [ungated]' if ungated else ''}")
    A, B = panel(llama, close, months), panel(new, close, months)
    run(A, "llama (DefiLlama schedules)")
    run(B, "llm_new (tokens DefiLlama does not cover)", min_tokens=6)
    run(pd.concat([A, B]), "pooled")


if __name__ == "__main__":
    main()
