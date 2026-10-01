"""Is the ML signal model weak because of training, or because there is no signal? (2026-10-01)

Pooled XGB on the v2 scale-free features (core/ml/pipeline.py) across every
local coin, at several bar sizes and holding horizons, judged by money:

  * targets: raw direction (fwd > 0) and market-relative (fwd minus the
    same-bar mean over coins > 0)
  * models: default (depth 5, 300 trees) vs regularized (depth 3, 200 trees,
    min_child_weight 200, subsample/colsample 0.7, lambda 10); baseline = the
    single feature with the best train AUC, sign chosen on train
  * split on bar time, 80/20, purged by the horizon; train vs test AUC
    shows overfitting
  * metric: top-minus-bottom decile of market-relative forward return on
    non-overlapping bars; net per position = spread/2 - 0.15% round trip;
    CI by calendar day (week for >= 4h bars)
  * --walk-forward: monthly retrain on everything before the month for the
    long horizons (the single split leaves only ~3 months of test)

Result on 2026-10-01 data (docs/AGENT_ML_BIAS_2026-09-30.md, section 6):
test AUC 0.50-0.54 everywhere. At <= 4h horizons the spread is real but
1/4-1/2 of costs; 4h/1d bars overfit badly with default trees (train AUC ~0.8,
test ~0.5). The only cell near break-even is 1h bars held 24h, and there a
single feature (distance below the slow EMA: short-term reversal) matches the
model: walk-forward spread +0.52%/day, 13/15 months positive, net/position
+0.11% [0.00, +0.21]. It became the frozen rule of
core/research/xs_reversal_tracker.py (scripts/xs_reversal_backtest.py).

  python scripts/ml_timeframe_study.py --cache data/research/ml_timeframe_cache
  python scripts/ml_timeframe_study.py --cache ... --timeframes 1h,1d --walk-forward
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.ml.pipeline import FEATURE_COLUMNS, build_feature_frame  # noqa: E402

COST = 0.0015
GRID: List[Tuple[str, int]] = [("5m", 12), ("15m", 4), ("15m", 16), ("1h", 4), ("1h", 24),
                               ("4h", 6), ("4h", 42), ("1d", 1), ("1d", 7)]
WALK_FORWARD = [("1h", 24), ("4h", 42), ("1d", 7), ("1d", 1)]
MIN_BARS = {"5m": 288 * 60, "15m": 96 * 90, "1h": 24 * 120, "4h": 6 * 200, "1d": 300}
BAR = {"5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h", "1d": "1D"}
MAX_TRAIN = 2_000_000
PARAMS = {
    "default": dict(n_estimators=300, max_depth=5, learning_rate=0.05),
    "regularized": dict(n_estimators=200, max_depth=3, learning_rate=0.05, min_child_weight=200,
                        subsample=0.7, colsample_bytree=0.7, reg_lambda=10.0),
}


def auc(y, s) -> float:
    # float math: Windows numpy ints are 32-bit and pos*neg overflows on large samples
    y = np.asarray(y, dtype=np.int64)
    r = pd.Series(np.asarray(s, dtype=float)).rank().to_numpy()
    pos = float(y.sum()); neg = float(len(y)) - pos
    return float((r[y == 1].sum() - pos * (pos + 1) / 2) / (pos * neg)) if pos and neg else float("nan")


async def build_cache(cache: Path, timeframes: Sequence[str]) -> None:
    from scripts import train_ml_signal as trainer

    cache.mkdir(parents=True, exist_ok=True)
    symbols = trainer._local_symbols("binance")
    for tf in timeframes:
        if (cache / f"{tf}.parquet").exists():
            continue
        parts = []
        for sym in symbols:
            df = await trainer.load_ohlcv("binance", sym, tf, 3000)
            if df is None or df.empty:
                continue
            df = df[["open", "high", "low", "close", "volume"]].astype(float)
            df.index = pd.to_datetime(df.index, utc=True)
            parts.append(df.assign(symbol=sym))
        if parts:
            pd.concat(parts).to_parquet(cache / f"{tf}.parquet")
            print(f"cached {tf}: {len(parts)} coins", flush=True)


def build(cache: Path, tf: str, h: int) -> pd.DataFrame:
    raw = pd.read_parquet(cache / f"{tf}.parquet")
    parts = []
    for sym, g in raw.groupby("symbol"):
        g = g.drop(columns="symbol").sort_index()
        g = g[~g.index.duplicated(keep="last")]
        if len(g) < MIN_BARS[tf]:
            continue
        f = build_feature_frame(g)
        f["fwd"] = g["close"].shift(-h) / g["close"] - 1.0
        f["symbol"] = sym
        f["bar"] = np.arange(len(f))
        parts.append(f.dropna())
    d = pd.concat(parts)
    d.index.name = "ts"
    d = d.reset_index()
    lo, hi = d["fwd"].quantile([0.001, 0.999])  # one squeeze must not own a decile mean
    d["fwd"] = d["fwd"].clip(lo, hi)
    d["excess"] = d["fwd"] - d.groupby("ts")["fwd"].transform("mean")
    return d[d.groupby("ts")["fwd"].transform("size") >= 10].reset_index(drop=True)


def decile_spread(test: pd.DataFrame, score, h: int) -> pd.Series:
    t = test.assign(s=score)
    t = t[t["bar"] % h == 0]  # non-overlapping forward windows per coin
    t = t[t.groupby("ts")["s"].transform("size") >= 10]
    pct = t.groupby("ts")["s"].rank(method="first", pct=True)
    dec = np.minimum((pct.to_numpy() * 10 - 1e-9).astype(int), 9)
    spread = (t[dec == 9].groupby("ts")["excess"].mean() - t[dec == 0].groupby("ts")["excess"].mean()).dropna()
    spread.index = pd.DatetimeIndex(spread.index).tz_localize(None)
    return spread


def clustered_ci(x: pd.Series, unit: str, reps: int = 2000) -> Tuple[float, float]:
    groups = [g.to_numpy() for _, g in x.groupby(x.index.to_period(unit))]
    rng = np.random.default_rng(0)
    boots = [np.concatenate([groups[i] for i in rng.integers(0, len(groups), len(groups))]).mean() for _ in range(reps)]
    return float(np.percentile(boots, 5)), float(np.percentile(boots, 95))


def best_single_feature(train: pd.DataFrame) -> Tuple[str, int]:
    scores = {c: auc(train["excess"] > 0, train[c]) for c in FEATURE_COLUMNS}
    col = max(scores, key=lambda c: abs(scores[c] - 0.5))
    return col, (1 if scores[col] >= 0.5 else -1)


def fit(train: pd.DataFrame, target: str, params: Dict):
    import xgboost as xgb

    model = xgb.XGBClassifier(tree_method="hist", eval_metric="logloss", verbosity=0, random_state=0, **params)
    model.fit(train[FEATURE_COLUMNS], (train[target] > 0).astype(int))
    return model


def report(label: str, spread: pd.Series, unit: str, extra: str = "") -> None:
    if spread.empty:
        print(f"  {label:28s} no cross-sections")
        return
    lo, hi = clustered_ci(spread, unit)
    print(f"  {label:28s}{extra} spread {spread.mean() * 100:+.3f}% [{lo * 100:+.3f},{hi * 100:+.3f}] n={len(spread)} | "
          f"net/position {(spread.mean() / 2 - COST) * 100:+.3f}%", flush=True)


def single_split(cache: Path, grid) -> None:
    for tf, h in grid:
        d = build(cache, tf, h)
        cut = d["ts"].quantile(0.8)
        train = d[d["ts"] < cut - pd.Timedelta(BAR[tf]) * h]
        test = d[d["ts"] >= cut].reset_index(drop=True)
        if len(train) > MAX_TRAIN:
            train = train.sample(MAX_TRAIN, random_state=0)
        unit = "W" if tf in ("4h", "1d") else "D"
        print(f"\n### {tf} horizon {h} | coins {d.symbol.nunique()} | train {len(train):,} test {len(test):,} "
              f"| test from {cut:%Y-%m-%d}", flush=True)
        col, sign = best_single_feature(train)
        report(f"baseline {col}{'+' if sign > 0 else '-'}", decile_spread(test, sign * test[col].to_numpy(), h), unit)
        for target in ("fwd", "excess"):
            for name, params in PARAMS.items():
                model = fit(train, target, params)
                a_tr = auc(train[target] > 0, model.predict_proba(train[FEATURE_COLUMNS])[:, 1])
                p_te = model.predict_proba(test[FEATURE_COLUMNS])[:, 1]
                a_te = auc(test[target] > 0, p_te)
                report(f"{name} target={'raw' if target == 'fwd' else 'excess'}", decile_spread(test, p_te, h), unit,
                       extra=f" AUC train {a_tr:.3f} test {a_te:.3f} |")


def walk_forward(cache: Path, grid) -> None:
    for tf, h in grid:
        d = build(cache, tf, h)
        bar = pd.Timedelta(BAR[tf])
        months = pd.period_range(d.ts.min().tz_localize(None) + pd.Timedelta(days=150), d.ts.max().tz_localize(None), freq="M")
        model_parts, base_parts = [], []
        for m in months:
            start, end = pd.Timestamp(m.start_time, tz="UTC"), pd.Timestamp(m.end_time, tz="UTC")
            train = d[d.ts < start - bar * h]
            test = d[(d.ts >= start) & (d.ts <= end)].reset_index(drop=True)
            if test.empty or len(train) < 20_000:
                continue
            if len(train) > MAX_TRAIN:
                train = train.sample(MAX_TRAIN, random_state=0)
            model = fit(train, "excess", PARAMS["regularized"])
            model_parts.append(decile_spread(test, model.predict_proba(test[FEATURE_COLUMNS])[:, 1], h))
            col, sign = best_single_feature(train)
            base_parts.append(decile_spread(test, sign * test[col].to_numpy(), h))
        print(f"\n### walk-forward {tf} horizon {h} | coins {d.symbol.nunique()} | months {months[0]}..{months[-1]}")
        for label, parts in (("xgb regularized", model_parts), ("baseline", base_parts)):
            x = pd.concat(parts) if parts else pd.Series(dtype=float)
            monthly = x.groupby(x.index.to_period("M")).mean() if len(x) else x
            report(label, x, "W", extra=f" months+ {int((monthly > 0).sum())}/{len(monthly)} |")


def main() -> int:
    warnings.filterwarnings("ignore")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--cache", type=Path, default=ROOT / "data" / "research" / "ml_timeframe_cache")
    parser.add_argument("--timeframes", default="5m,15m,1h,4h,1d")
    parser.add_argument("--walk-forward", action="store_true")
    args = parser.parse_args()
    tfs = [t.strip() for t in args.timeframes.split(",") if t.strip()]
    asyncio.run(build_cache(args.cache, tfs))
    if args.walk_forward:
        walk_forward(args.cache, [g for g in WALK_FORWARD if g[0] in tfs])
    else:
        single_split(args.cache, [g for g in GRID if g[0] in tfs])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
