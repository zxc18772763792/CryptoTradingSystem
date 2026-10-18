"""Research loop v3 command line (core/ai/research_loop_v3.py, docs/RESEARCH_LOOP_V3.md).

  python scripts/research_loop_v3.py --round [--force]   one round (the research scheduler runs this)
  python scripts/research_loop_v3.py --build-panel       rebuild data/research/xs_daily_panel/ now
  python scripts/research_loop_v3.py --calibrate         null / power checks on the real panel
  python scripts/research_loop_v3.py --status            print the loop state

--calibrate scores formulas that cannot know the future (random persistent series per coin),
a planted signal (next 7 days' return plus heavy noise) and copies of known factors, with the
same code the loop uses, and writes data/research/ai/research_loop_v3_calibration.json. Expected:
about 5% of random series past z 1.645, the planted signal far past it, copies with residual
z near 0. Not part of the trial ledger.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CALIBRATION_PATH = ROOT / "data" / "research" / "ai" / "research_loop_v3_calibration.json"


def _log(message: str) -> None:
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def calibrate(n_random: int, n_perm: int, seed: int = 7) -> dict:
    from core.research.xs_daily_panel import load_panel
    from core.research.xs_return_evaluation import EvalContext, evaluate

    panel = load_panel(log=_log)
    ctx = EvalContext(panel)
    life = np.isfinite(panel.columns["close"].to_numpy(dtype=float))
    rows, cols = life.shape
    rng = np.random.default_rng(seed)
    out = {"started_at": datetime.now(timezone.utc).isoformat(), "n_perm": n_perm, "periods": ctx.period_bounds,
           "random": [], "planted": {}, "copies": {}}

    def score(name: str, feature: np.ndarray, i: int) -> dict:
        started = time.time()
        metrics = evaluate({"name": name, "direction": "high", "expr": {"col": "close"}}, ctx, n_perm=n_perm, seed=i, feature=feature)
        keep = ("coverage", "ic", "z", "resid_ic", "resid_z", "net_per_book", "net_per_book_ex_top3", "nearest_factor", "nearest_factor_corr")
        row = {"name": name, "seconds": round(time.time() - started, 1),
               **{p: {k: metrics[p].get(k) for k in keep} for p in metrics}}
        _log(f"{name}: dev z {row['dev'].get('z')} resid {row['dev'].get('resid_z')} | holdout z {row['holdout'].get('z')} "
             f"resid {row['holdout'].get('resid_z')} net {row['holdout'].get('net_per_book')} ({row['seconds']}s)")
        return row

    def save() -> None:
        CALIBRATION_PATH.parent.mkdir(parents=True, exist_ok=True)
        CALIBRATION_PATH.write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")

    kinds = ("ar1", "walk", "coin_level")
    for i in range(n_random):
        kind = kinds[i % len(kinds)]
        if kind == "ar1":  # persistent noise (phi 0.97, ~1 month half-life)
            x = np.zeros((rows, cols))
            shocks = rng.normal(size=(rows, cols))
            for t in range(1, rows):
                x[t] = 0.97 * x[t - 1] + shocks[t]
        elif kind == "walk":
            x = rng.normal(size=(rows, cols)).cumsum(axis=0)
        else:  # a fixed trait per coin plus small daily noise
            x = rng.normal(size=(1, cols)) + 0.2 * rng.normal(size=(rows, cols))
        x[~life] = np.nan
        out["random"].append(score(f"random_{kind}_{i}", x, i))
        save()
    fwd = panel.fwd7.to_numpy(dtype=float)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # days before any coin is ranked
        spread = np.nanmedian(np.nanstd(np.where(ctx.universe, fwd, np.nan), axis=1))
    for label, noise in (("planted_strong", 8.0), ("planted_weak", 25.0)):
        x = np.where(np.isfinite(fwd), fwd, 0.0) + noise * spread * rng.normal(size=fwd.shape)
        x[~life] = np.nan
        out["planted"][label] = score(label, x, 1000 + len(out["planted"]))
        save()
    for name in ("rev21", "vol30", "cg_composite"):
        out["copies"][name] = score(f"copy_{name}", panel.factors[name].to_numpy(dtype=float), 2000 + len(out["copies"]))
        save()

    def share(rows_, period, key, bar=1.645):
        values = [r[period].get(key) for r in rows_ if r[period].get(key) is not None]
        return {"n": len(values), "share_past_bar": round(float(np.mean(np.asarray(values) >= bar)), 3) if values else None,
                "mean": round(float(np.mean(values)), 3) if values else None, "sd": round(float(np.std(values)), 3) if values else None}

    out["summary"] = {f"{p}_{k}": share(out["random"], p, k) for p in ("dev", "holdout") for k in ("z", "resid_z")}
    out["finished_at"] = datetime.now(timezone.utc).isoformat()
    save()
    _log(f"calibration summary: {json.dumps(out['summary'])}")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--round", action="store_true")
    parser.add_argument("--force", action="store_true", help="run even if disabled or over today's budget")
    parser.add_argument("--build-panel", action="store_true")
    parser.add_argument("--calibrate", action="store_true")
    parser.add_argument("--n-random", type=int, default=45)
    parser.add_argument("--n-perm", type=int, default=100)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    if args.build_panel:
        from core.research.xs_daily_panel import build_panel, save_panel

        save_panel(build_panel(_log))
    if args.calibrate:
        calibrate(args.n_random, args.n_perm)
    if args.round:
        from core.ai.research_loop_v3 import run_round

        _log("round start")
        result = run_round(force=args.force, log=_log)
        _log(f"round end: {json.dumps({k: result.get(k) for k in ('skipped', 'round_id', 'status', 'evaluated', 'frozen', 'error')}, ensure_ascii=False)}")
    if args.status:
        from core.ai.research_loop_v3 import status

        print(json.dumps(status(), ensure_ascii=False, indent=1, default=str)[:6000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
