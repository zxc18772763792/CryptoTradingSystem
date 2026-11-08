"""Which trailing window should the radar's risk meter calibrate on?

Walk-forward: at each test day, every coin's 14-day Parkinson volatility decile gets the touch rate its decile
had over the trailing window, using only 7-day windows already finished by that day. Compare with what happened
next. The window is picked on DEV (2023-2024) by Brier score and then only reported on HOLD (2025 on).

Usage (conda run needed): python scripts/radar_risk_meter_calibration.py
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import weekly_rank_rotation_study as rot  # noqa: E402

PANEL = ROOT / "data/research/xs_daily_panel"
WINDOWS = (60, 90, 180, 365, 730)
TARGETS = {"up40": ("up", 0.40), "dn30": ("dn", 0.30), "long5x": ("dn", 0.20), "short5x": ("up", 0.20), "long10x": ("dn", 0.10), "short3x": ("up", 1 / 3)}
DEV_START, DEV_END = pd.Timestamp("2023-01-02"), pd.Timestamp("2024-12-31")


def main() -> None:
    pd.set_option("display.width", 200, "display.max_columns", 30, "display.max_rows", 100)
    universe = pd.read_parquet(PANEL / "universe.parquet") > 0.5
    idx, cols = universe.index, universe.columns
    majors = [c for c in cols if rot.strip_1000(rot.base_of(c)) in rot.MAJORS | rot.NON_ALT]
    universe[majors] = False
    read = lambda n: pd.read_parquet(PANEL / f"col_{n}.parquet").reindex(index=idx, columns=cols)
    close, high, low = read("close"), read("high"), read("low")
    park = np.sqrt((np.log(high / low) ** 2) / (4 * math.log(2))).rolling(14, min_periods=8).mean()
    pct = park.where(universe).rank(axis=1, pct=True)
    decile = np.ceil(pct * 10).clip(1, 10)
    up7 = high.iloc[::-1].rolling(7, min_periods=1).max().iloc[::-1].shift(-1) / close - 1
    dn7 = low.iloc[::-1].rolling(7, min_periods=1).min().iloc[::-1].shift(-1) / close - 1
    done = pd.Series(np.arange(len(idx)) <= len(idx) - 8, index=idx)

    daily_rate, daily_n = {}, {}
    for name, (side, level) in TARGETS.items():
        hit = (up7 >= level) if side == "up" else (dn7 <= -level)
        hit = hit.where(decile.notna())
        daily_rate[name] = pd.DataFrame({d: hit.where(decile == d).sum(axis=1) for d in range(1, 11)})
    daily_n = pd.DataFrame({d: (decile == d).sum(axis=1) for d in range(1, 11)})

    test_days = idx[(idx >= DEV_START) & done.to_numpy()][::7]
    rows = []
    calib = []
    for name, (side, level) in TARGETS.items():
        hit = (up7 >= level) if side == "up" else (dn7 <= -level)
        hits, ns = daily_rate[name], daily_n
        for w in WINDOWS:
            for t in test_days:
                pos = idx.get_loc(t)
                # A 7-day window starting at day i is finished only once i + 7 <= t.
                a, b = max(0, pos - 7 - w + 1), pos - 7 + 1
                pred = hits.iloc[a:b].sum() / ns.iloc[a:b].sum().replace(0, np.nan)
                dec = decile.loc[t].dropna()
                actual = hit.loc[t, dec.index].astype(float)
                p = dec.map(pred)
                base = float(hits.iloc[a:b].sum().sum() / max(ns.iloc[a:b].sum().sum(), 1))
                brier = float(((p - actual) ** 2).mean())
                brier0 = float(((base - actual) ** 2).mean())
                per = "DEV" if t <= DEV_END else "HOLD"
                rows.append({"target": name, "window": w, "period": per, "brier": brier, "brier0": brier0, "abs_err": abs(float(p.mean()) - float(actual.mean()))})
                if w in (90, 180, 365):
                    for d in range(1, 11):
                        s = dec[dec == d].index
                        calib.append({"target": name, "window": w, "period": per, "decile": d, "pred": float(p[s].mean()), "real": float(actual[s].mean())})
    T = pd.DataFrame(rows)
    g = T.groupby(["target", "window", "period"]).agg(brier=("brier", "mean"), brier0=("brier0", "mean"), abs_err=("abs_err", "mean")).reset_index()
    g["skill"] = 1 - g["brier"] / g["brier0"]
    print("Skill = 1 - Brier / Brier of the trailing base rate with no deciles. Higher is better.")
    print(g.pivot_table(index=["target", "period"], columns="window", values="skill").round(4).to_string())
    print("\nMean Brier x 100:")
    print((g.pivot_table(index=["target", "period"], columns="window", values="brier") * 100).round(3).to_string())
    print("\nMean weekly error of the average predicted rate (percentage points):")
    print((g.pivot_table(index=["target", "period"], columns="window", values="abs_err") * 100).round(2).to_string())
    C = pd.DataFrame(calib)
    print("\nPredicted vs realized by decile, up40 and dn30")
    for name in ("up40", "dn30"):
        pv = C[C["target"] == name].groupby(["window", "period", "decile"])[["pred", "real"]].mean().unstack("window")
        print(name)
        print((pv * 100).round(1).to_string())


if __name__ == "__main__":
    main()
