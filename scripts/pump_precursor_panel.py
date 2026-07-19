"""Pump-precursor panel study: can a descriptive model rank coins by 30d pump probability?

Builds a weekly (coin, date) panel from the ambush dataset with PAST-ONLY features,
labels each row with forward 30d max return, then reports:
  1. base rates and per-feature decile lift (top vs bottom decile pump rate)
  2. walk-forward model precision@K vs random baseline (time split, no shuffling)
  3. a no-stop spot basket simulation on the test period (weekly top-K, ladder exits)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util

spec = importlib.util.spec_from_file_location("bt", SCRIPT_DIR / "backtest_ambush_modes.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)

TRAIN_END = pd.Timestamp("2026-02-15")
LABEL_H = 30
TOP_K = 15
FEE = 0.001  # spot 10bps/side


from core.research.pump_precursor import FEATURES, build_daily_features  # noqa: E402


def build_daily(base: str, frame: pd.DataFrame) -> pd.DataFrame:
    d = pd.DataFrame(
        {
            "close": pd.to_numeric(frame["close"], errors="coerce").resample("1D").last(),
            "volume": pd.to_numeric(frame["volume"], errors="coerce").resample("1D").sum(),
            "oi": pd.to_numeric(frame["oi_usd"], errors="coerce").resample("1D").last(),
            "mcap": pd.to_numeric(frame["mcap_usd"], errors="coerce").resample("1D").last(),
            "funding": pd.to_numeric(frame["funding_rate"], errors="coerce").resample("1D").mean(),
        }
    ).dropna(subset=["close"])
    if len(d) < 45:
        return pd.DataFrame()
    d = build_daily_features(d)
    d["base"] = base
    c = d["close"]
    fwd_max = c.shift(-1).rolling(LABEL_H).max().shift(-(LABEL_H - 1))
    d["fwd30_maxret"] = fwd_max / c - 1
    return d


def main() -> None:
    bases = sorted(p.stem for p in (bt.DATA_DIR / "klines_1h").glob("*.parquet"))
    close_by_base = {}
    for base in bases:
        kl = pd.read_parquet(bt.DATA_DIR / "klines_1h" / f"{base}.parquet", columns=["close"])
        close_by_base[base] = pd.to_numeric(kl["close"], errors="coerce").resample("1D").last().dropna()

    cache = SCRIPT_DIR.parent / "reports" / "ambush_modes_2026-07-18" / "panel_ranked_cache.parquet"
    if cache.exists():
        ranked = pd.read_parquet(cache)
        ranked["date"] = pd.to_datetime(ranked["date"])
        snap = ranked
        print(f"loaded cached panel ({len(ranked)} rows)")
    else:
        frames = {}
        for base in bases:
            frame = bt.load_enriched_frame(base)
            if frame is None:
                continue
            if pd.to_numeric(frame["mcap_usd"], errors="coerce").notna().sum() < 24:
                continue
            frames[base] = frame
        panel_parts = []
        for base, frame in frames.items():
            d = build_daily(base, frame)
            if len(d):
                panel_parts.append(d)
        daily = pd.concat(panel_parts)

        snap = daily[daily.index.dayofweek == 0].copy()
        snap = snap[snap["fwd30_maxret"].notna()]
        snap = snap[snap["mcap"].between(2e6, 1.5e9)]
        snap = snap.reset_index().rename(columns={"index": "date"})
        snap["pump100"] = (snap["fwd30_maxret"] >= 1.0).astype(int)
        snap["pump300"] = (snap["fwd30_maxret"] >= 3.0).astype(int)

    print(f"panel: {len(snap)} coin-weeks, {snap['base'].nunique()} coins, "
          f"{snap['date'].min().date()} .. {snap['date'].max().date()}")
    print(f"base rate pump100={snap['pump100'].mean()*100:.1f}%  pump300={snap['pump300'].mean()*100:.2f}%")

    # cross-sectional ranks per date
    if not cache.exists():
        ranked = snap.copy()
        for feat in FEATURES:
            ranked[feat + "_r"] = ranked.groupby("date")[feat].rank(pct=True)

    print("\n=== univariate decile lift (pump100 rate, top-20% vs bottom-20% by feature) ===")
    rows = []
    for feat in FEATURES:
        r = ranked[[feat + "_r", "pump100"]].dropna()
        top = r[r[feat + "_r"] >= 0.8]["pump100"].mean()
        bot = r[r[feat + "_r"] <= 0.2]["pump100"].mean()
        rows.append((feat, len(r), bot * 100, top * 100))
    rows.sort(key=lambda x: x[3] - x[2], reverse=True)
    for feat, n, bot, top in rows:
        print(f"  {feat:<22} bottom20%={bot:5.1f}%  top20%={top:5.1f}%  spread={top-bot:+5.1f}pp")

    # walk-forward model
    feat_r = [f + "_r" for f in FEATURES]
    model_df = ranked.dropna(subset=feat_r + ["pump100"])
    train = model_df[model_df["date"] <= TRAIN_END]
    test = model_df[model_df["date"] > TRAIN_END]
    print(f"\ntrain {len(train)} rows (to {TRAIN_END.date()}), test {len(test)} rows")

    cache = SCRIPT_DIR.parent / "reports" / "ambush_modes_2026-07-18" / "panel_ranked_cache.parquet"
    try:
        ranked.to_parquet(cache)
        print(f"panel cached -> {cache}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"panel cache failed: {exc}", flush=True)

    # hand-rolled L2 logistic on rank features (sklearn DLL-crashes in this env)
    X_train = train[feat_r].to_numpy(dtype=float) - 0.5
    y_train = train["pump100"].to_numpy(dtype=float)
    X_test = test[feat_r].to_numpy(dtype=float) - 0.5
    # NOTE: this box's conda numpy hard-crashes (exit 127) inside BLAS matmul,
    # so everything below sticks to elementwise ufuncs + reductions — no `@`.
    w = np.zeros(X_train.shape[1])
    b = float(np.log(max(y_train.mean(), 1e-4) / max(1 - y_train.mean(), 1e-4)))
    lr, lam = 0.5, 1e-2
    for _ in range(3000):
        z = (X_train * w).sum(axis=1) + b
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))
        resid = p - y_train
        grad_w = (X_train * resid[:, None]).sum(axis=0) / len(y_train) + lam * w
        grad_b = float(resid.mean())
        w -= lr * grad_w
        b -= lr * grad_b
    test = test.copy()
    z_test = (X_test * w).sum(axis=1) + b
    test["score"] = 1.0 / (1.0 + np.exp(-np.clip(z_test, -30, 30)))
    coef = sorted(zip(FEATURES, w), key=lambda kv: abs(kv[1]), reverse=True)
    print("logistic weights (rank features, train-only fit):")
    for name, weight in coef[:8]:
        print(f"  {name:<22} {weight:+.2f}")

    weights_path = PROJECT_ROOT / "data" / "research" / "pump_watchlist" / "model_weights.json"
    weights_path.parent.mkdir(parents=True, exist_ok=True)
    import json as _json
    weights_path.write_text(
        _json.dumps(
            {
                "version": 1,
                "trained_at": pd.Timestamp.utcnow().isoformat(),
                "train_end": str(TRAIN_END.date()),
                "label": f"fwd{LABEL_H}d_max_ret>=100%",
                "features": FEATURES,
                "weights": [float(x) for x in w],
                "bias": float(b),
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
            },
            indent=1,
        ),
        encoding="utf-8",
    )
    print(f"model weights exported -> {weights_path}")

    # precision@K per week on test
    weekly = []
    for date, grp in test.groupby("date"):
        top = grp.nlargest(TOP_K, "score")
        weekly.append(
            {
                "date": date,
                "hit100": top["pump100"].mean(),
                "hit300": top["pump300"].mean(),
                "base100": grp["pump100"].mean(),
                "avg_fwd_top": top["fwd30_maxret"].mean(),
                "avg_fwd_all": grp["fwd30_maxret"].mean(),
            }
        )
    wk = pd.DataFrame(weekly)
    print(f"\n=== test period precision@{TOP_K}/week ===")
    print(f"model top{TOP_K}: pump100 hit={wk['hit100'].mean()*100:.1f}%  (universe base {wk['base100'].mean()*100:.1f}%)")
    print(f"model top{TOP_K}: pump300 hit={wk['hit300'].mean()*100:.2f}%")
    print(f"avg forward 30d MAX ret: top{TOP_K}={wk['avg_fwd_top'].mean()*100:.1f}%  universe={wk['avg_fwd_all'].mean()*100:.1f}%")

    # ---- basket simulation on test period: weekly top-K spot, no stop, ladder ----
    def episode_return(entry_date, base):
        series = close_by_base.get(base)
        if series is None:
            return None
        d = series[series.index >= entry_date]
        if len(d) < 2:
            return None
        entry = float(d.iloc[0])
        hold = d.iloc[1 : LABEL_H + 1]
        if not len(hold) or entry <= 0:
            return None
        path = hold / entry
        remaining, realized = 1.0, 0.0
        if (path >= 2.0).any():
            realized += 0.40 * (2.0 - 1.0)
            remaining -= 0.40
        if (path >= 4.0).any():
            realized += 0.30 * (4.0 - 1.0)
            remaining -= 0.30
        final = float(path.iloc[-1])
        total = realized + remaining * (final - 1.0)
        return total - 2 * FEE

    rng = np.random.default_rng(11)
    model_rets, random_rets = [], []
    for date, grp in test.groupby("date"):
        top = grp.nlargest(TOP_K, "score")
        rets = [episode_return(date, b) for b in top["base"]]
        rets = [r for r in rets if r is not None]
        if rets:
            model_rets.append(np.mean(rets))
        pool = grp["base"].tolist()
        draws = []
        for _ in range(50):
            sample = rng.choice(pool, size=min(TOP_K, len(pool)), replace=False)
            rr = [episode_return(date, b) for b in sample]
            rr = [r for r in rr if r is not None]
            if rr:
                draws.append(np.mean(rr))
        if draws:
            random_rets.append(np.mean(draws))

    model_arr, rand_arr = np.array(model_rets), np.array(random_rets)
    print(f"\n=== spot basket sim (test period, hold {LABEL_H}d, ladder 40%@+100%/30%@+300%, NO stop) ===")
    print(f"weeks={len(model_arr)}")
    print(f"model top{TOP_K}: mean weekly-basket ret={model_arr.mean()*100:+.1f}%  median={np.median(model_arr)*100:+.1f}%  "
          f"win_weeks={(model_arr>0).mean()*100:.0f}%")
    print(f"random {TOP_K}:  mean weekly-basket ret={rand_arr.mean()*100:+.1f}%  median={np.median(rand_arr)*100:+.1f}%")
    # rolling 30d holds → ~4 overlapping weekly cohorts, each ~25% of capital
    ann = (1.0 + model_arr.mean() / 4.0) ** 52 - 1.0
    ann_rand = (1.0 + rand_arr.mean() / 4.0) ** 52 - 1.0
    print(f"rough annualization (4 cohorts x 25% capital): model {ann*100:+.0f}%/yr vs random {ann_rand*100:+.0f}%/yr "
          f"(in-window, survivorship-biased — see report caveats)")

    out = SCRIPT_DIR.parent / "reports" / "ambush_modes_2026-07-18" / "pump_precursor_panel.csv"
    ranked.to_csv(out, index=False)
    print(f"\npanel saved -> {out}")


if __name__ == "__main__":
    main()
