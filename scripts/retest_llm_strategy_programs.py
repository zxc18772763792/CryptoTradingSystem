"""Honest re-test of every strategy program the AI research loop has written.

WHY THIS EXISTS (2026-09-25):
The research loop backtests each LLM-drafted program on 30 days of BTC/ETH 1h
(config days=30), which yields a median of 4 trades per candidate; 88% have
<=10. Picking the best of thousands of 4-trade backtests selects noise: the
two "validated" proposals scored Sharpe 12.4 on 6 trades and 7.8 on 5 trades,
and the BTC one then went 0/3 in forward observation.

This script re-runs every distinct program, exactly as written (no parameter
search on our side), across all coins with full local 1h history, so each gets
thousands of trades, and splits time into development / holdout.

First run (85 distinct programs, 50 coins, 2026-04-20..09-24, 6bp/side):
median 3461 trades per program; 2% had positive timing-alpha in development,
27% in holdout, 0 with holdout t>2 (Bonferroni bar for 85 programs ~3.2).

Timing-alpha = strategy PnL minus exposure x the coin's average daily return.
The programs are long-only, so in a rising market they earn drift just by being
exposed; only the part beyond that is evidence of skill. Signals are decided at
bar close and earn the next bar (no lookahead). Uses the research module's own
indicator / condition code so semantics match the loop.

Usage:
  python scripts/retest_llm_strategy_programs.py
  python scripts/retest_llm_strategy_programs.py --cost-bp 10 --split 2026-08-01
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.research.strategy_program import (  # noqa: E402
    _coerce_program_from_payload,
    _combine_conditions,
    _series_for_indicator,
)

CANDIDATES = PROJECT_ROOT / "data" / "research" / "ai" / "candidates.json"
KLINES = PROJECT_ROOT / "data" / "historical" / "binance"


def load_programs(path: Path) -> List[Dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("candidates") if isinstance(raw, dict) else raw
    seen: Dict[str, Dict[str, Any]] = {}
    for cand in items or []:
        program_raw = (cand.get("metadata") or {}).get("strategy_program")
        if not isinstance(program_raw, dict):
            continue
        program = _coerce_program_from_payload(
            program_raw,
            fallback_id=cand["candidate_id"],
            fallback_name=cand.get("strategy") or cand["candidate_id"],
            fallback_description="",
            fallback_params={},
            fallback_tags=[],
        )
        if program is None:
            continue
        key = json.dumps(
            [program_raw.get(k) for k in ("indicators", "entry_conditions", "exit_conditions", "execution_mode")],
            sort_keys=True,
            ensure_ascii=False,
        )
        best = (((cand.get("validation_summary") or {}).get("metrics") or {}).get("best") or {})
        seen.setdefault(key, {
            "candidate_id": cand["candidate_id"],
            "program": program,
            "loop_sharpe": best.get("sharpe_ratio"),
            "loop_trades": best.get("total_trades"),
        })
    return list(seen.values())


def load_universe(start: str, end: str, min_bars: int) -> Dict[str, pd.DataFrame]:
    data: Dict[str, pd.DataFrame] = {}
    for folder in sorted(glob.glob(str(KLINES / "*_USDT" / "1h_parts"))):
        symbol = Path(folder).parent.name
        frame = pd.concat([pd.read_parquet(p) for p in glob.glob(folder + "/*.parquet")])
        frame = frame[~frame.index.duplicated(keep="last")].sort_index().loc[start:end]
        if len(frame) >= min_bars and frame["close"].gt(0).all():
            data[symbol] = frame
    return data


def program_positions(program, frame: pd.DataFrame) -> np.ndarray:
    series = {c: pd.to_numeric(frame[c], errors="coerce").fillna(0.0) for c in ("open", "high", "low", "close", "volume")}
    for spec in program.indicators:
        series[str(spec.name)] = _series_for_indicator(frame, spec)
    entry = _combine_conditions(list(program.entry_conditions), series, frame.index, program.entry_combine).to_numpy()
    if program.execution_mode == "signal_long" or not program.exit_conditions:
        return entry.astype(float)
    exit_ = _combine_conditions(list(program.exit_conditions), series, frame.index, program.exit_combine).to_numpy()
    pos = np.zeros(len(frame))
    holding = 0.0
    for i in range(len(frame)):  # same state machine as build_program_positions, on arrays
        if holding > 0.0 and exit_[i]:
            holding = 0.0
        elif holding <= 0.0 and entry[i]:
            holding = 1.0
        pos[i] = holding
    return pos


def evaluate(entry: Dict[str, Any], data: Dict[str, pd.DataFrame], cost: float, split: pd.Timestamp) -> Dict[str, Any]:
    pnl_d, expo_d, ret_d = {}, {}, {}
    trades = 0
    for symbol, frame in data.items():
        pos = program_positions(entry["program"], frame)
        ret = frame["close"].pct_change().fillna(0.0).to_numpy()
        held = np.concatenate([[0.0], pos[:-1]])  # decided at close t, earns bar t+1
        turnover = np.abs(np.diff(np.concatenate([[0.0], held])))
        daily = pd.DataFrame(
            {"pnl": held * ret - turnover * cost, "expo": held, "ret": ret}, index=frame.index
        ).resample("1D").agg({"pnl": "sum", "expo": "mean", "ret": "sum"})
        pnl_d[symbol], expo_d[symbol], ret_d[symbol] = daily["pnl"], daily["expo"], daily["ret"]
        trades += int((np.diff(np.concatenate([[0.0], held])) > 0).sum())
    pnl, expo, ret = (pd.concat(d, axis=1) for d in (pnl_d, expo_d, ret_d))
    alpha = (pnl - expo * ret.mean(axis=0)).mean(axis=1)

    def stats(s: pd.Series):
        s = s.dropna()
        sd = s.std()
        if not sd or sd != sd:
            return 0.0, 0.0, float(s.sum())
        return float(s.mean() / sd * math.sqrt(365)), float(s.mean() / sd * math.sqrt(len(s))), float(s.sum())

    out = {
        "candidate_id": entry["candidate_id"],
        "name": entry["program"].name,
        "loop_sharpe": entry["loop_sharpe"],
        "loop_trades": entry["loop_trades"],
        "trades": trades,
        "exposure": float(expo.mean().mean()),
    }
    for label, part in (("dev", alpha[alpha.index < split]), ("hold", alpha[alpha.index >= split])):
        out[f"{label}_sharpe"], out[f"{label}_t"], out[f"{label}_alpha"] = stats(part)
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--candidates", type=Path, default=CANDIDATES)
    parser.add_argument("--start", default="2026-04-20")
    parser.add_argument("--end", default=pd.Timestamp.utcnow().strftime("%Y-%m-%d"))
    parser.add_argument("--split", default="2026-07-21", help="holdout starts here (UTC date)")
    parser.add_argument("--cost-bp", type=float, default=6.0, help="per side; the loop assumes 4bp fee + 2bp slippage")
    parser.add_argument("--min-bars", type=int, default=3000)
    parser.add_argument("--csv", type=Path, help="write per-program results")
    args = parser.parse_args(argv)

    programs = load_programs(args.candidates)
    data = load_universe(args.start, args.end, args.min_bars)
    print(f"distinct programs: {len(programs)} | coins with full 1h history: {len(data)} | {args.start}..{args.end}")
    if not programs or not data:
        return 1
    split = pd.Timestamp(args.split)
    results = pd.DataFrame([evaluate(p, data, args.cost_bp / 1e4, split) for p in programs])
    if args.csv:
        results.to_csv(args.csv, index=False)

    bonferroni = float(pd.Series([0.05 / len(results)]).map(lambda a: abs(np.sqrt(2) * _erfinv(1 - 2 * a)))[0])
    print(f"median trades per program: {results['trades'].median():.0f} (loop's own median: {results['loop_trades'].median()})")
    print(f"positive timing-alpha: development {(results['dev_alpha'] > 0).mean():.0%} | holdout {(results['hold_alpha'] > 0).mean():.0%}")
    print(f"holdout t > 2: {(results['hold_t'] > 2).sum()} | holdout t > {bonferroni:.2f} (Bonferroni, {len(results)} programs): {(results['hold_t'] > bonferroni).sum()}")
    print(f"spearman(loop Sharpe, holdout alpha): {results[['loop_sharpe', 'hold_alpha']].dropna().corr(method='spearman').iloc[0, 1]:+.3f}")
    top = results.sort_values("dev_sharpe", ascending=False).head(10)
    print("\ntop 10 by development Sharpe -> holdout:")
    print(top[["name", "trades", "exposure", "dev_sharpe", "hold_sharpe", "hold_t", "hold_alpha"]].round(3).to_string(index=False))
    return 0


def _erfinv(y: float) -> float:
    # One-sided normal quantile via bisection on erf; avoids scipy for a single number.
    lo, hi = 0.0, 6.0
    for _ in range(80):
        mid = (lo + hi) / 2
        if math.erf(mid) < y:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


if __name__ == "__main__":
    raise SystemExit(main())
