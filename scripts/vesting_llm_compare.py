"""Round 6 stage 3: does the LLM-built schedule rank tokens' 90-day supply growth like DefiLlama's?"""
import json, sys
from pathlib import Path
import numpy as np
import pandas as pd
sys.path.insert(0, ".")
from core.research import unlock_short_tracker as ut
from core.research.unlock_events import unlocked_series
from core.research.xs_evaluation import spearman

OUT = Path("data/research/vesting_llm")
MONTH = 30.4375
HORIZON = 90

def llm_series(spec, fallback_start, days):
    total = pd.Series(0.0, index=days)
    tge = spec.get("tge_date") or fallback_start
    used = 0.0
    for a in spec.get("allocations") or []:
        try:
            pct = float(a.get("pct_of_total") or 0) / 100
            if pct <= 0 or not a.get("schedule_known", True):
                continue
            start = pd.Timestamp(a.get("start_date") or tge, tz="UTC").normalize()
            tge_part = pct * min(max(float(a.get("tge_unlock_pct") or 0), 0), 100) / 100
            rest = pct - tge_part
            cliff_end = start + pd.Timedelta(days=float(a.get("cliff_months") or 0) * MONTH)
            v = float(a.get("vesting_months") or 0)
            freq = a.get("frequency") or "monthly"
        except (TypeError, ValueError):
            continue
        used += pct
        el = np.zeros(len(days))
        el[days >= start] += tge_part
        if v <= 0 or freq == "once":
            el[days >= cliff_end] += rest
        elif freq == "daily":
            frac = ((days - cliff_end).days / (v * MONTH)).to_numpy(dtype=float)
            el += rest * np.clip(frac, 0, 1)
        else:
            step = 3 if freq == "quarterly" else 1
            n = max(int(round(v / step)), 1)
            for k in range(1, n + 1):
                el[days >= cliff_end + pd.Timedelta(days=k * step * MONTH)] += rest / n
        total += el
    return total, used

def growth(series, d):
    end = d + pd.Timedelta(days=HORIZON)
    if d not in series.index or end not in series.index or series[d] <= 0:
        return np.nan
    return float(series[end] / series[d] - 1)

def main(mode):
    ex = json.loads((OUT / "extractions.json").read_text(encoding="utf-8"))
    days = pd.date_range("2019-01-01", "2028-12-31", freq="D", tz="UTC")
    months = pd.date_range("2023-01-01", "2027-06-01", freq="MS", tz="UTC")
    rows, usable = [], 0
    for ticker, e in ex.items():
        spec = e.get(mode) or {}
        if "error" in spec or not spec.get("allocations"):
            continue
        ours, used = llm_series(spec, e["binance_first_day"], days)
        if used < 0.3:
            continue
        usable += 1
        sched = json.loads((ut.CACHE_DIR / "emissions" / f"{e['slug']}.json").read_text(encoding="utf-8"))
        ref = unlocked_series(sched).asfreq("1D").ffill()
        first = pd.Timestamp(e["binance_first_day"], tz="UTC")
        for d in months:
            if d < first:
                continue
            rows.append({"ticker": ticker, "date": d, "llm": growth(ours, d), "ref": growth(ref, d)})
    df = pd.DataFrame(rows).dropna()
    print(f"[{mode}] usable specs {usable}/{len(ex)} | token-months compared {len(df)}")
    stats = []
    for d, g in df.groupby("date"):
        if len(g) < 10:
            continue
        rl, rr = g["llm"].rank(pct=True), g["ref"].rank(pct=True)
        tl = np.ceil(rl * 3).clip(1, 3); tr = np.ceil(rr * 3).clip(1, 3)
        stats.append({"date": d, "n": len(g), "rho": spearman(g["llm"], g["ref"]),
                      "same_tercile": float((tl == tr).mean()), "opposite_leg": float(((tl == 1) & (tr == 3) | (tl == 3) & (tr == 1)).mean())})
    s = pd.DataFrame(stats)
    for label, part in (("past (<=2026-09)", s[s["date"] <= "2026-09-01"]), ("forward (>=2026-10)", s[s["date"] >= "2026-10-01"])):
        if len(part):
            print(f"  {label}: {len(part)} months, tokens/month {part['n'].median():.0f} | rank corr median {part['rho'].median():.2f} "
                  f"(IQR {part['rho'].quantile(.25):.2f}-{part['rho'].quantile(.75):.2f}) | same tercile {part['same_tercile'].mean():.0%} (chance 33%) | long<->short swapped {part['opposite_leg'].mean():.1%}")
    df["abs_err_pp"] = (df["llm"] - df["ref"]).abs() * 100
    per_tok = df.groupby("ticker").apply(lambda g: pd.Series({"rho_time": spearman(g["llm"], g["ref"]) if g["ref"].nunique() > 2 else np.nan, "mae_pp": g["abs_err_pp"].median()}))
    print(f"  per-token median abs error of 90d growth: {df['abs_err_pp'].median():.1f}pp (ref median growth {df['ref'].median()*100:.1f}%)")
    worst = per_tok.sort_values("mae_pp", ascending=False).head(8)
    print("  worst tokens:", ", ".join(f"{t} {r.mae_pp:.0f}pp" for t, r in worst.iterrows()))
    return df, s

if __name__ == "__main__":
    for m in sys.argv[1:] or ["docs", "memory"]:
        main(m)
