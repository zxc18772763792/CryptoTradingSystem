"""Phase 0 backtest runtime benchmark.

Records wall time, bars/sec, key metrics, and a result hash for a fixed set of
backtest workloads, plus a cProfile dump for the heaviest single case.

Run:
    python scripts/benchmark_backtest_runtime.py

Outputs:
    .bench/baseline_<git_sha>.json   metrics for every case
    .bench/baseline_latest.json      symlink-style copy of the latest run
    .bench/baseline_<git_sha>.prof   cProfile dump for the MultiFactorHF case
    .bench/baseline_<git_sha>.top.txt    pstats top-30 of that profile

The benchmark intentionally avoids HTTP / FastAPI so the numbers reflect engine
cost only. It calls _load_backtest_inputs + _run_backtest_core directly.
"""
from __future__ import annotations

import argparse
import asyncio
import cProfile
import hashlib
import io
import json
import os
import pstats
import subprocess
import sys
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure the project root is importable when run from anywhere.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Heavy imports are deferred to inside main() so that --help is fast.


_BENCH_DIR = _ROOT / ".bench"


def _git_sha() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=_ROOT, stderr=subprocess.DEVNULL
        )
        return out.decode().strip()
    except Exception:
        return "nogit"


def _hash_result(result: Dict[str, Any]) -> str:
    """Stable hash of key numeric outputs so we can detect parity drift."""
    keys = [
        "total_return",
        "sharpe_ratio",
        "max_drawdown",
        "win_rate",
        "total_trades",
        "final_capital",
    ]
    payload = {k: round(float(result.get(k) or 0.0), 8) for k in keys if k in result}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]


def _resolve_anchor() -> datetime:
    """End of the benchmark's data window.

    Defaults to now, which makes two runs read DIFFERENT data and silently
    invalidates any before/after comparison -- a real run of this script reported
    changed result hashes for RSI/MACD/MA purely from an hour's drift, none of
    which had any code change between the runs. Pin it with --anchor (or
    BACKTEST_BENCH_ANCHOR) to an ISO timestamp whenever the numbers are being
    compared across runs rather than just recorded.
    """
    raw = (os.getenv("BACKTEST_BENCH_ANCHOR") or "").strip()
    if not raw:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def _prepare_inputs(
    strategy: str, symbol: str, timeframe: str, days: int
) -> tuple[Any, Any, str]:
    """Load OHLCV once for a case."""
    from web.api.backtest import _load_backtest_inputs

    end_time = _resolve_anchor()
    start_time = end_time - timedelta(days=days)
    df, bundle, resolved = await _load_backtest_inputs(
        strategy=strategy,
        symbol=symbol,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time,
    )
    return df, bundle, resolved


def _run_one(
    strategy: str,
    df,
    timeframe: str,
    bundle,
    params: Optional[Dict[str, Any]] = None,
    include_series: bool = False,
) -> Dict[str, Any]:
    from web.api.backtest import _run_backtest_core

    return _run_backtest_core(
        strategy=strategy,
        df=df,
        timeframe=timeframe,
        initial_capital=10000.0,
        params=params,
        include_series=include_series,
        market_bundle=bundle,
        use_stop_take=False,
    )


def _bench_case(
    case_name: str,
    strategy: str,
    df,
    timeframe: str,
    bundle,
    params: Optional[Dict[str, Any]] = None,
    repeat: int = 1,
    track_memory: bool = False,
) -> Dict[str, Any]:
    timings: List[float] = []
    peak_mem_mb: Optional[float] = None
    last_result: Dict[str, Any] = {}

    for i in range(repeat):
        if track_memory and i == 0:
            tracemalloc.start()
        t0 = time.perf_counter()
        last_result = _run_one(strategy, df, timeframe, bundle, params=params)
        timings.append(time.perf_counter() - t0)
        if track_memory and i == 0:
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            peak_mem_mb = round(peak / (1024 * 1024), 2)

    wall = min(timings)  # min over repeats to reduce noise
    bars = int(len(df))
    return {
        "case": case_name,
        "strategy": strategy,
        "timeframe": timeframe,
        "bars": bars,
        "repeats": repeat,
        "wall_seconds": round(wall, 4),
        "wall_seconds_mean": round(sum(timings) / len(timings), 4),
        "bars_per_second": round(bars / wall, 1) if wall > 0 else None,
        "peak_memory_mb": peak_mem_mb,
        "total_return": round(float(last_result.get("total_return") or 0.0), 6),
        "sharpe_ratio": round(float(last_result.get("sharpe_ratio") or 0.0), 4),
        "max_drawdown": round(float(last_result.get("max_drawdown") or 0.0), 6),
        "win_rate": round(float(last_result.get("win_rate") or 0.0), 4),
        "total_trades": int(last_result.get("total_trades") or 0),
        "result_hash": _hash_result(last_result),
    }


def _bench_compare(
    case_name: str,
    strategies: List[str],
    df,
    timeframe: str,
    bundle,
) -> Dict[str, Any]:
    """Simulate compare mode: run N strategies on the same df, time the whole batch."""
    t0 = time.perf_counter()
    per_strategy = []
    for strat in strategies:
        try:
            t1 = time.perf_counter()
            res = _run_one(strat, df, timeframe, bundle)
            per_strategy.append(
                {
                    "strategy": strat,
                    "wall_seconds": round(time.perf_counter() - t1, 4),
                    "total_trades": int(res.get("total_trades") or 0),
                    "result_hash": _hash_result(res),
                }
            )
        except Exception as exc:
            per_strategy.append({"strategy": strat, "error": str(exc)})
    wall = time.perf_counter() - t0
    return {
        "case": case_name,
        "n_strategies": len(strategies),
        "bars": int(len(df)),
        "timeframe": timeframe,
        "wall_seconds": round(wall, 4),
        "per_strategy": per_strategy,
    }


def _bench_optimize(
    case_name: str,
    strategy: str,
    df,
    timeframe: str,
    bundle,
    grid: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Simulate optimize mode: same strategy, N param sets."""
    t0 = time.perf_counter()
    trials = []
    for i, params in enumerate(grid):
        t1 = time.perf_counter()
        try:
            res = _run_one(strategy, df, timeframe, bundle, params=params)
            trials.append(
                {
                    "trial": i,
                    "wall_seconds": round(time.perf_counter() - t1, 4),
                    "sharpe_ratio": round(float(res.get("sharpe_ratio") or 0.0), 4),
                }
            )
        except Exception as exc:
            trials.append({"trial": i, "error": str(exc)})
    wall = time.perf_counter() - t0
    return {
        "case": case_name,
        "strategy": strategy,
        "n_trials": len(grid),
        "bars": int(len(df)),
        "timeframe": timeframe,
        "wall_seconds": round(wall, 4),
        "wall_per_trial_mean": round(wall / max(1, len(grid)), 4),
        "trials": trials,
    }


def _build_optimize_grid() -> List[Dict[str, Any]]:
    """A small but representative param grid for MAStrategy."""
    grid: List[Dict[str, Any]] = []
    for fast in (5, 7, 10, 12):
        for slow in (20, 26, 30, 35):
            for sig in (3, 5):
                if fast >= slow:
                    continue
                grid.append({"fast_period": fast, "slow_period": slow, "signal_period": sig})
    return grid[:32]


def _profile_case(strategy: str, df, timeframe: str, bundle, out_prof: Path) -> str:
    """Run one case under cProfile, write .prof + a top-30 text summary."""
    profiler = cProfile.Profile()
    profiler.enable()
    _run_one(strategy, df, timeframe, bundle)
    profiler.disable()
    profiler.dump_stats(str(out_prof))

    buf = io.StringIO()
    stats = pstats.Stats(profiler, stream=buf).sort_stats("cumulative")
    stats.print_stats(30)
    return buf.getvalue()


async def amain(args: argparse.Namespace) -> int:
    _BENCH_DIR.mkdir(parents=True, exist_ok=True)
    sha = _git_sha()

    # Silence strategy chatter — we care about wall time, not signal logs.
    try:
        from loguru import logger as _lg  # noqa: PLC0415

        _lg.remove()
        _lg.add(sys.stderr, level=os.environ.get("BENCH_LOG_LEVEL", "WARNING"))
    except Exception:
        pass

    symbol = args.symbol
    cases: List[Dict[str, Any]] = []

    # Filter out strategies the registry refuses, so the bench doesn't crash
    # on workloads that aren't single-symbol OHLCV friendly.
    from config.strategy_registry import is_strategy_backtest_supported  # noqa: PLC0415

    # === Workload definitions ===
    # MultiFactorHF on 5m/1y can take many minutes; default profile uses 1mo
    # which is enough to characterize bars/sec for each strategy.
    if args.full:
        workloads_raw = [
            ("ma_5m_3mo", "MAStrategy", "5m", 90, 2, False),
            ("ma_5m_1y", "MAStrategy", "5m", 365, 1, False),
            ("rsi_5m_3mo", "RSIStrategy", "5m", 90, 2, False),
            ("bollinger_5m_3mo", "BollingerStrategy", "5m", 90, 2, False),
            ("macd_5m_3mo", "MACDStrategy", "5m", 90, 2, False),
            ("multifactor_5m_3mo", "MultiFactorHFStrategy", "5m", 90, 1, True),
        ]
    else:
        workloads_raw = [
            # (case_name, strategy, timeframe, days, repeat, track_memory)
            ("ma_5m_1mo", "MAStrategy", "5m", 30, 1, False),
            ("rsi_5m_1mo", "RSIStrategy", "5m", 30, 1, False),
            ("bollinger_5m_1mo", "BollingerStrategy", "5m", 30, 1, False),
            ("macd_5m_1mo", "MACDStrategy", "5m", 30, 1, False),
            ("multifactor_5m_1mo", "MultiFactorHFStrategy", "5m", 30, 1, True),
        ]
    workloads = []
    for w in workloads_raw:
        if not is_strategy_backtest_supported(w[1]):
            print(f"[bench] skipping {w[0]}: {w[1]} not backtest-supported in registry")
            continue
        workloads.append(w)

    print(f"[bench] git sha = {sha}")
    print(f"[bench] symbol  = {symbol}")
    print(f"[bench] output  = {_BENCH_DIR}")
    print()

    loaded: Dict[tuple, Any] = {}  # cache (strategy, timeframe, days) → (df, bundle)

    for case_name, strategy, timeframe, days, repeat, track_mem in workloads:
        key = (strategy, timeframe, days)
        if key not in loaded:
            print(f"[bench] loading data for {case_name}: {strategy} {timeframe} {days}d")
            df, bundle, _ = await _prepare_inputs(strategy, symbol, timeframe, days)
            loaded[key] = (df, bundle)
        df, bundle = loaded[key]
        if df is None or len(df) == 0:
            cases.append({"case": case_name, "skipped": "no data"})
            print(f"  -> SKIPPED (no data)")
            continue
        print(f"[bench] running {case_name} ({len(df)} bars, x{repeat})")
        rec = _bench_case(
            case_name, strategy, df, timeframe, bundle, repeat=repeat, track_memory=track_mem
        )
        cases.append(rec)
        print(
            f"  -> wall={rec['wall_seconds']}s  bars/s={rec['bars_per_second']}  "
            f"trades={rec['total_trades']}  hash={rec['result_hash']}"
        )

    # Compare mode: use whichever MA frame we loaded
    cmp_key = ("MAStrategy", "5m", 90 if args.full else 30)
    if cmp_key in loaded and len(loaded[cmp_key][0]) > 0:
        df, bundle = loaded[cmp_key]
        cmp_strats_raw = [
            "MAStrategy",
            "EMAStrategy",
            "RSIStrategy",
            "BollingerStrategy",
            "MACDStrategy",
            "AroonStrategy",
            "WilliamsRStrategy",
            "CCIStrategy",
            "StochRSIStrategy",
            "ROCStrategy",
        ]
        cmp_strats = [s for s in cmp_strats_raw if is_strategy_backtest_supported(s)]
        print(f"\n[bench] compare ({len(cmp_strats)} strategies, 5m/3mo)")
        rec = _bench_compare("compare_5m_3mo", cmp_strats, df, "5m", bundle)
        cases.append(rec)
        print(f"  -> wall={rec['wall_seconds']}s for {rec['n_strategies']} strategies")

        # Optimize mode: 32 trials on MAStrategy
        grid = _build_optimize_grid()
        print(f"\n[bench] optimize MAStrategy ({len(grid)} trials, 5m/3mo)")
        rec = _bench_optimize("optimize_ma_5m_3mo", "MAStrategy", df, "5m", bundle, grid)
        cases.append(rec)
        print(
            f"  -> wall={rec['wall_seconds']}s  per_trial≈{rec['wall_per_trial_mean']}s"
        )

    # Profile MultiFactorHF on whichever frame we have
    prof_text = ""
    mf_key = ("MultiFactorHFStrategy", "5m", 90 if args.full else 30)
    if not args.skip_profile and mf_key in loaded and len(loaded[mf_key][0]) > 0:
        df, bundle = loaded[mf_key]
        prof_path = _BENCH_DIR / f"baseline_{sha}.prof"
        txt_path = _BENCH_DIR / f"baseline_{sha}.top.txt"
        print(f"\n[bench] cProfile MultiFactorHF -> {prof_path.name}")
        prof_text = _profile_case("MultiFactorHFStrategy", df, "5m", bundle, prof_path)
        txt_path.write_text(prof_text, encoding="utf-8")
        # Print top-30 to stdout for quick eyeball
        print("\n".join(prof_text.splitlines()[:35]))

    # Write JSON
    payload = {
        "git_sha": sha,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        # Recorded so a later comparison can tell whether two runs actually read
        # the same data. Two baselines with different anchors are not comparable,
        # however similar their case names look.
        "data_anchor": _resolve_anchor().isoformat(),
        "data_anchor_pinned": bool(os.getenv("BACKTEST_BENCH_ANCHOR")),
        "python": sys.version.split()[0],
        "symbol": symbol,
        "cases": cases,
    }
    json_path = _BENCH_DIR / f"baseline_{sha}.json"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    latest = _BENCH_DIR / "baseline_latest.json"
    latest.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n[bench] wrote {json_path}")
    print(f"[bench] wrote {latest}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest runtime benchmark")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument(
        "--skip-profile",
        action="store_true",
        help="Skip cProfile pass (saves ~one full MultiFactorHF run)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run the full 3mo/1y workload set (slow, ~25 min). Default is 1mo smoke profile.",
    )
    parser.add_argument(
        "--anchor",
        default=None,
        help=(
            "ISO timestamp to end the data window at (e.g. 2026-09-01T00:00:00Z). "
            "REQUIRED for before/after comparisons: the default 'now' makes two runs "
            "read different data, so result hashes and timings change on their own. "
            "Also settable via BACKTEST_BENCH_ANCHOR."
        ),
    )
    args = parser.parse_args()
    if args.anchor:
        os.environ["BACKTEST_BENCH_ANCHOR"] = args.anchor
    return asyncio.run(amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
