"""Post-hoc diagnostic (not a verdict): is the 7-day Coinglass composite mostly a size bet?

Asked on 2026-10-12 when the tracker's first book (core/research/coinglass_composite_tracker.py)
came out long BTC/ETH/SOL/BNB and short small alts. Uses the study's own cached panel and the
tracker's frozen weights (test half, >= SPLIT); no network. Size proxy = log aggregated OI (USD).
  (a) daily rank correlation between the composite score and size
  (b) gross 7-day quintile spread per day: composite vs a pure size sort, and their correlation
  (c) size-neutral composite: the score's rank residualised on the size rank each day, then quintiles
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research import coinglass_composite_tracker as ct  # noqa: E402
from scripts import coinglass_combination_study as combo  # noqa: E402
from scripts import coinglass_positioning_study as single  # noqa: E402


def corr(a, b) -> float:
    """Pearson correlation by elementwise sums (np.corrcoef goes through BLAS, which crashes here)."""
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    a, b = a - a.mean(), b - b.mean()
    return float((a * b).sum() / np.sqrt((a * a).sum() * (b * b).sum()))


def spread(score: pd.DataFrame, fwd: pd.DataFrame) -> pd.Series:
    out = {}
    for t in score.index:
        s, f = score.loc[t], fwd.loc[t]
        ok = s.notna() & f.notna()
        if ok.sum() < combo.MIN_COINS:
            continue
        ranked = s[ok].sort_values(kind="mergesort")
        k = len(ranked) // 5
        out[t] = float(f[ranked.index[-k:]].mean() - f[ranked.index[:k]].mean())
    return pd.Series(out)


def main() -> None:
    frames = single.build_panel(single.load_cached())
    panel = single.build_signals(frames)
    panel = combo.add_features(panel, frames)
    test = panel["fwd_1d"].index >= combo.SPLIT
    score = combo.composite(panel, ct.WEIGHTS)[test]
    fwd7 = panel["fwd_7d"][test]
    size = np.log(frames["oi"].where(frames["oi"] > 0)).reindex(index=score.index, columns=score.columns)

    rho = []
    long_size, short_size = [], []
    for t in score.index:
        s, z = score.loc[t], size.loc[t]
        ok = s.notna() & z.notna()
        if ok.sum() < combo.MIN_COINS:
            continue
        rho.append(corr(s[ok].rank(), z[ok].rank()))
        ranked = s[ok].sort_values(kind="mergesort")
        k = len(ranked) // 5
        pct = z[ok].rank(pct=True)
        long_size.append(float(pct[ranked.index[-k:]].mean()))
        short_size.append(float(pct[ranked.index[:k]].mean()))
    print(f"(a) days {len(rho)}: mean rank corr(score, size) {np.mean(rho):+.3f} (share of days > 0: {np.mean(np.array(rho) > 0):.0%})")
    print(f"    mean size percentile: longs {np.mean(long_size):.2f}, shorts {np.mean(short_size):.2f} (0.5 = middle)")

    comp = spread(score, fwd7)
    by_size = spread(size, fwd7)
    both = pd.concat([comp, by_size], axis=1, keys=["comp", "size"]).dropna()
    print(f"(b) gross 7d spread per day: composite {comp.mean() / 7 * 100:+.4f}%  size sort {by_size.mean() / 7 * 100:+.4f}%  "
          f"corr {corr(both['comp'], both['size']):+.2f}")

    resid = {}
    for t in score.index:
        s, z = score.loc[t], size.loc[t]
        ok = s.notna() & z.notna()
        if ok.sum() < combo.MIN_COINS:
            continue
        rs, rz = s[ok].rank(pct=True) - 0.5, z[ok].rank(pct=True) - 0.5
        beta = float((rs * rz).sum() / (rz * rz).sum())
        resid[t] = rs - beta * rz
    neutral = pd.DataFrame(resid).T.reindex(columns=score.columns)
    neu = spread(neutral, fwd7)
    print(f"(c) size-neutral composite gross 7d spread {neu.mean() / 7 * 100:+.4f}%/day "
          f"(original {comp.mean() / 7 * 100:+.4f}%); corr with size sort {corr(*pd.concat([neu, by_size], axis=1).dropna().T.values):+.2f}")
    for label, part in pd.concat([comp, neu, by_size], axis=1, keys=["comp", "neutral", "size"]).groupby(
            lambda t: pd.Timestamp(t).tz_localize(None).to_period("Q")):
        print(f"    {label}: composite {part['comp'].mean() / 7 * 100:+.3f}  neutral {part['neutral'].mean() / 7 * 100:+.3f}  "
              f"size {part['size'].mean() / 7 * 100:+.3f}  (%/day, gross)")


if __name__ == "__main__":
    main()
