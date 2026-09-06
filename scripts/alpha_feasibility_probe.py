"""Feasibility probe for a Binance Alpha new-coin takeoff model (2026-09-06).

Answers, reproducibly: can we build a validatable "which new Alpha coin will take
off" model NOW? Findings (see docs/ALPHA_NEWCOIN_FEASIBILITY_2026-09-06.md):
  1. NO historical fundamentals — the Alpha collector persists only the current
     catalog (token_snapshots.jsonl rotates within days); market_snapshots and
     collector_runs in alpha_market.db are all one day. A fundamental
     cross-sectional model cannot be backtested; only forward collection can.
  2. Price history IS deep (1d klines ~300 bars/token) and listingTime is a clean
     anchor, so the one backtestable angle is post-listing PRICE behavior — and
     it is dead: clean (non-overlapping) early-momentum -> takeoff spearman ~0.12,
     high vs low early-momentum terciles have identical 2x rates.
  3. 17% of the catalog is fullyDelisted — real new-coin mortality.
Run: python scripts/alpha_feasibility_probe.py
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "research" / "binance_alpha" / "alpha_market.db"
JSONL = ROOT / "data" / "research" / "binance_alpha" / "token_snapshots.jsonl"


def main() -> None:
    lines = [json.loads(l) for l in JSONL.read_text(encoding="utf-8").splitlines() if l.strip()]
    toks = lines[-1]["tokens"]
    meta = {str(t["alphaId"]): t for t in toks if t.get("alphaId") is not None}
    delisted = sum(1 for t in toks if t.get("fullyDelisted"))
    print(f"catalog: {len(toks)} tokens, {delisted} fullyDelisted ({delisted / len(toks) * 100:.0f}%)")

    # confirm no multi-day fundamental history
    days = sorted({str(o.get("updated_at"))[:10] for o in lines})
    print(f"catalog-snapshot distinct days on disk: {days}  (single day => no fundamental history)")

    con = sqlite3.connect(str(DB))
    rows = con.execute(
        "select alpha_id, open_time_ms, close from alpha_klines where timeframe='1d' order by alpha_id, open_time_ms"
    ).fetchall()
    series = defaultdict(list)
    for aid, ot, cl in rows:
        series[str(aid)].append((ot, cl))

    recs = []
    for aid, s in series.items():
        lt = meta.get(aid, {}).get("listingTime")
        if not lt:
            continue
        post = [(ot, cl) for ot, cl in s if ot >= lt and cl and cl > 0]
        if len(post) < 40:
            continue
        closes = [cl for _, cl in post]
        c0, c3 = closes[0], closes[3]
        early3 = closes[3] / c0 - 1                     # predictor: first 3 days
        out = max(closes[4:35]) / c3 - 1                # NON-overlapping fwd-30d max from day3
        recs.append((early3, out))
    e = np.array([r[0] for r in recs])
    o = np.array([r[1] for r in recs])
    print(f"\nlisting-anchored tokens (>=40d post-listing 1d history): {len(recs)}")
    print(f"outcome base: median {np.median(o) * 100:.0f}%  2x {(o >= 1).mean() * 100:.0f}%  4x {(o >= 3).mean() * 100:.0f}%")
    print(f"clean early-momentum -> takeoff spearman: {spearmanr(e, o).correlation:.3f}")
    idx = np.argsort(e)
    k = len(e) // 3
    top, bot = o[idx[-k:]], o[idx[:k]]
    print(f"high early-mom 2x-rate {(top >= 1).mean() * 100:.0f}%  vs low {(bot >= 1).mean() * 100:.0f}%  (separation => signal)")
    print("\nVERDICT: no backtestable fundamental history; price-only post-listing signal is dead."
          " Only forward fundamental collection (scripts/snapshot_alpha_fundamentals.py) can enable a model.")


if __name__ == "__main__":
    main()
