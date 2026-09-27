"""Round 6: does an LLM-built schedule rank tokens' 90-day supply growth like DefiLlama's?

Usage: vesting_llm_compare.py [extractions file] [modes] [--gated]
  step 1  extractions.json  (doc URLs taken from DefiLlama's adapters)
  step 2  discovered_extractions.json (docs found by scripts/vesting_llm_discover.py)
--gated keeps only specs passing core.research.vesting_spec.usable (the
confidence / known-share gate the factor would use).
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, ".")
from core.research import unlock_short_tracker as ut
from core.research.unlock_events import entry_ticker, unlocked_series
from core.research.vesting_spec import growth, schedule_from_spec, usable
from core.research.xs_evaluation import spearman

OUT = Path("data/research/vesting_llm")
DAYS = pd.date_range("2019-01-01", "2028-12-31", freq="D", tz="UTC")
MONTHS = pd.date_range("2023-01-01", "2027-06-01", freq="MS", tz="UTC")


def llama_slugs():
    idx = json.loads((ut.CACHE_DIR / "emissions_index.json").read_text(encoding="utf-8"))
    idx = idx["data"] if isinstance(idx, dict) else idx
    out = {}
    for e in idx:
        slug = e.get("protocolSlug")
        if (ut.CACHE_DIR / "emissions" / f"{slug}.json").exists():
            out.setdefault(entry_ticker(e), slug)
    return out


def main(file, mode, gated):
    ex = json.loads((OUT / file).read_text(encoding="utf-8"))
    slugs = llama_slugs()
    rows, usable_n, compared = [], 0, set()
    for ticker, e in ex.items():
        spec = e.get(mode) or {}
        if "error" in spec or not spec.get("allocations"):
            continue
        ours, known = schedule_from_spec(spec, e.get("binance_first_day"), DAYS)
        if known < 0.3 or (gated and not usable(spec, known)):
            continue
        usable_n += 1
        slug = slugs.get(ticker)
        if not slug:
            continue
        ref = unlocked_series(json.loads((ut.CACHE_DIR / "emissions" / f"{slug}.json").read_text(encoding="utf-8"))).asfreq("1D").ffill()
        first = pd.Timestamp(e.get("binance_first_day") or "2019-01-01", tz="UTC")
        compared.add(ticker)
        for d in MONTHS:
            if d >= first:
                rows.append({"ticker": ticker, "date": d, "llm": growth(ours, d), "ref": growth(ref, d)})
    df = pd.DataFrame(rows).dropna()
    print(f"[{file} {mode}{' gated' if gated else ''}] usable specs {usable_n}/{len(ex)} | compared with DefiLlama {len(compared)} tokens, {len(df)} token-months")
    stats = []
    for d, g in df.groupby("date"):
        if len(g) < 8:
            continue
        tl = np.ceil(g["llm"].rank(pct=True) * 3).clip(1, 3)
        tr = np.ceil(g["ref"].rank(pct=True) * 3).clip(1, 3)
        stats.append({"date": d, "n": len(g), "rho": spearman(g["llm"], g["ref"]), "same": float((tl == tr).mean()),
                      "swap": float((((tl == 1) & (tr == 3)) | ((tl == 3) & (tr == 1))).mean())})
    s = pd.DataFrame(stats)
    for label, part in (("past (<=2026-09)", s[s["date"] <= "2026-09-01"] if len(s) else s),
                        ("forward (>=2026-10)", s[s["date"] >= "2026-10-01"] if len(s) else s)):
        if len(part):
            print(f"  {label}: {len(part)} months, tokens/month {part['n'].median():.0f} | rank corr median {part['rho'].median():.2f} "
                  f"(IQR {part['rho'].quantile(.25):.2f}-{part['rho'].quantile(.75):.2f}) | same tercile {part['same'].mean():.0%} (chance 33%) | long<->short swapped {part['swap'].mean():.1%}")
    if len(df):
        err = (df["llm"] - df["ref"]).abs() * 100
        worst = err.groupby(df["ticker"]).median().sort_values(ascending=False).head(8)
        print(f"  median abs error of 90d growth {err.median():.1f}pp (reference median {df['ref'].median() * 100:.1f}%) | worst: "
              + ", ".join(f"{t} {v:.0f}pp" for t, v in worst.items()))
    return df, s


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    file = args[0] if args else "extractions.json"
    modes = (args[1] if len(args) > 1 else "docs,memory").split(",")
    for m in modes:
        main(file, m, "--gated" in sys.argv)
