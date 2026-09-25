"""Offline edge check for the AI autonomous agent's entry calls.

WHY THIS EXISTS (2026-09-25):
Paper-running a changed agent for weeks to learn whether it "works" is slow and
noisy. Every decision the agent makes is already in its journal with a
timestamp, symbol and direction, and local 5m klines say what price did next.
So any change (timeframe, prompt, model, extra inputs) can be judged in minutes
by asking one question: do its buy/sell calls beat buying/selling the same coin
at random times nearby, by more than trading costs?

First run (2026-09-10..25, deepseek flash, 15m): 1172 BUY calls -> 136
independent after de-dup; edge vs random timing 1h -0.08%, 4h -0.14%,
24h -1.60%, every CI spanning zero or below. The model agreed with the rule
aggregator on 100% of calls, i.e. it added no information.

Method (keep it honest):
* entry = close of the last FULLY CLOSED 5m bar before the call (no lookahead);
* edge = signed forward return minus the mean forward return of the same coin
  over all start times within +/- --baseline-days (strips market drift);
* repeated calls on one coin/side within --dedup-hours count once (the agent
  re-issues the same call every tick; without this n is inflated ~10x);
* confidence interval = bootstrap over calendar days (calls on one day share
  the same market move, so they are not independent).

Verdict PASS needs BOTH: edge CI lower bound > 0 (real skill) and mean signed
return > --cost-pct (survives fees + slippage). Otherwise keep it in paper.

Usage:
  python scripts/agent_call_edge.py
  python scripts/agent_call_edge.py --since 2026-09-18 --by conf
  python scripts/agent_call_edge.py --by model --horizon 4h --cost-pct 0.7
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

AI_CACHE = PROJECT_ROOT / "data" / "cache" / "ai"
KLINE_ROOT = PROJECT_ROOT / "data" / "historical"
BAR = pd.Timedelta("5min")
HORIZONS = {"15m": 3, "1h": 12, "4h": 48, "24h": 288}


def default_journals() -> List[Path]:
    paths = sorted((AI_CACHE / "journal_archive").glob("autonomous_agent_journal.*.jsonl"))
    live = AI_CACHE / "autonomous_agent_journal.jsonl"
    if live.exists():
        paths.append(live)
    return paths


def extract_calls(paths: Iterable[Path], since: Optional[str] = None, until: Optional[str] = None) -> pd.DataFrame:
    """Every buy/sell decision (executed or not) with its context."""
    rows: List[Dict[str, Any]] = []
    for path in paths:
        with open(path, "rb") as fh:
            for line in fh:
                # Rows are ~40KB; skip the JSON parse for the ~90% that are holds.
                if b'"action": "buy"' not in line and b'"action": "sell"' not in line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                decision = row.get("decision") or {}
                action = decision.get("action")
                if action not in ("buy", "sell"):
                    continue
                execution = row.get("execution") or {}
                signal = execution.get("signal") or {}
                selection = row.get("selection") or {}
                config = row.get("config") or {}
                context = row.get("context") or {}
                symbol = signal.get("symbol") or selection.get("selected_symbol") or config.get("symbol")
                if not symbol or not row.get("timestamp"):
                    continue
                rows.append({
                    "ts": row["timestamp"],
                    "symbol": str(symbol).split(":")[0].upper(),
                    "exchange": str(config.get("exchange") or "binance").lower(),
                    "side": 1 if action == "buy" else -1,
                    "confidence": decision.get("confidence"),
                    "model": config.get("model"),
                    "timeframe": config.get("timeframe"),
                    "submitted": bool(execution.get("submitted")),
                    "aggregator": (context.get("aggregated_signal") or {}).get("direction"),
                })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")
    if since:
        df = df[df["ts"] >= pd.Timestamp(since, tz="UTC")]
    if until:
        df = df[df["ts"] < pd.Timestamp(until, tz="UTC")]
    return df.sort_values("ts").reset_index(drop=True)


def dedupe(df: pd.DataFrame, hours: float) -> pd.DataFrame:
    """Keep a call only if >= ``hours`` after the last kept call on that coin/side."""
    if df.empty or hours <= 0:
        return df
    gap = pd.Timedelta(hours=hours)
    keep: List[int] = []
    last: Dict[tuple, pd.Timestamp] = {}
    for idx, row in df.sort_values("ts").iterrows():
        key = (row["symbol"], row["side"])
        if key not in last or row["ts"] - last[key] >= gap:
            keep.append(idx)
            last[key] = row["ts"]
    return df.loc[keep].sort_values("ts").reset_index(drop=True)


_KLINES: Dict[tuple, Optional[pd.Series]] = {}


def load_closes(exchange: str, symbol: str, root: Path = KLINE_ROOT) -> Optional[pd.Series]:
    key = (str(root), exchange, symbol)
    if key not in _KLINES:
        parts = glob.glob(str(root / exchange / symbol.replace("/", "_") / "5m_parts" / "*.parquet"))
        if not parts:
            _KLINES[key] = None
        else:
            frame = pd.concat([pd.read_parquet(p, columns=["close"]) for p in parts])
            frame = frame[~frame.index.duplicated(keep="last")].sort_index()
            closes = frame["close"].astype(float)
            closes.index = pd.to_datetime(closes.index, utc=True)
            _KLINES[key] = closes
    return _KLINES[key]


def score_calls(df: pd.DataFrame, horizons: Dict[str, int], baseline_days: float, root: Path = KLINE_ROOT) -> pd.DataFrame:
    """Add signed forward return (``ret_<h>``) and edge vs random timing (``edge_<h>``)."""
    window = pd.Timedelta(days=baseline_days)
    out = []
    for row in df.itertuples(index=False):
        closes = load_closes(row.exchange, row.symbol, root)
        if closes is None:
            continue
        # Index = bar open time; a bar is fully closed once open + 5m <= call time.
        pos = int(closes.index.searchsorted(row.ts - BAR, side="right")) - 1
        if pos < 0:
            continue
        entry = closes.iloc[pos]
        record = row._asdict()
        lo = int(closes.index.searchsorted(row.ts - window))
        hi = int(closes.index.searchsorted(row.ts + window))
        for name, bars in horizons.items():
            if pos + bars >= len(closes):
                record[f"ret_{name}"] = record[f"edge_{name}"] = np.nan
                continue
            fwd = closes.iloc[pos + bars] / entry - 1.0
            seg = closes.iloc[lo:min(hi, len(closes) - bars) + bars].to_numpy()
            base = float(np.mean(seg[bars:] / seg[:-bars] - 1.0)) if len(seg) > bars else np.nan
            record[f"ret_{name}"] = row.side * fwd
            record[f"edge_{name}"] = row.side * (fwd - base)
        out.append(record)
    return pd.DataFrame(out)


def day_bootstrap_ci(values: pd.Series, days: pd.Series, reps: int = 2000, q=(5, 95), seed: int = 0):
    frame = pd.DataFrame({"v": values.to_numpy(), "d": days.to_numpy()}).dropna()
    if frame.empty:
        return (np.nan, np.nan)
    groups = [g["v"].to_numpy() for _, g in frame.groupby("d")]
    rng = np.random.default_rng(seed)
    means = []
    for _ in range(reps):
        pick = rng.integers(0, len(groups), len(groups))
        sample = np.concatenate([groups[i] for i in pick])
        means.append(sample.mean())
    lo, hi = np.percentile(means, q)
    return float(lo), float(hi)


def _pct(x: float) -> str:
    return "   n/a  " if x != x else f"{x * 100:+7.3f}%"


def report(scored: pd.DataFrame, horizon: str, cost_pct: float, by: Optional[str]) -> Dict[str, Any]:
    days = scored["ts"].dt.date
    print(f"\n{'horizon':8s}{'n':>5}{'signed ret':>12}{'edge vs random':>16}{'90% CI (day bootstrap)':>28}")
    summary: Dict[str, Any] = {}
    for name in [c[4:] for c in scored.columns if c.startswith("ret_")]:
        ret = scored[f"ret_{name}"]
        edge = scored[f"edge_{name}"]
        lo, hi = day_bootstrap_ci(edge, days)
        n = int(edge.notna().sum())
        summary[name] = {"n": n, "ret": float(ret.mean()), "edge": float(edge.mean()), "edge_ci": [lo, hi]}
        print(f"{name:8s}{n:5d}{_pct(ret.mean()):>12}{_pct(edge.mean()):>16}      [{_pct(lo)}, {_pct(hi)}]")

    main = summary.get(horizon)
    if main:
        skill = main["edge_ci"][0] > 0
        pays = main["ret"] > cost_pct / 100.0
        verdict = "PASS" if (skill and pays) else "FAIL"
        print(
            f"\nverdict @ {horizon}: {verdict}  "
            f"(edge CI low {_pct(main['edge_ci'][0]).strip()} {'>' if skill else '<='} 0; "
            f"signed ret {_pct(main['ret']).strip()} {'>' if pays else '<='} cost {cost_pct:.2f}%)"
        )
        summary["verdict"] = {"horizon": horizon, "result": verdict, "cost_pct": cost_pct}

    if by:
        col = by
        frame = scored.copy()
        if by == "conf":
            frame["conf"] = pd.cut(frame["confidence"].astype(float), [0, 0.6, 0.7, 0.75, 0.8, 1.0])
        elif by == "month":
            frame["month"] = frame["ts"].dt.strftime("%Y-%m")
        grouped = frame.groupby(col, observed=True)[f"edge_{horizon}"].agg(["count", "mean"])
        print(f"\nedge @ {horizon} by {by}:")
        for key, g in grouped.iterrows():
            print(f"  {str(key):32s} n={int(g['count']):4d}  edge {_pct(g['mean'])}")
    return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--journal", action="append", type=Path, help="journal file(s); default live + archives")
    parser.add_argument("--since", help="UTC date/time, inclusive")
    parser.add_argument("--until", help="UTC date/time, exclusive")
    parser.add_argument("--dedup-hours", type=float, default=4.0)
    parser.add_argument("--baseline-days", type=float, default=3.0)
    parser.add_argument("--horizon", default="4h", choices=list(HORIZONS), help="horizon for the verdict / --by table")
    parser.add_argument("--cost-pct", type=float, default=0.7, help="round-trip cost to beat, percent (alts ~0.7)")
    parser.add_argument("--by", choices=["model", "conf", "symbol", "side", "month", "submitted", "aggregator"])
    parser.add_argument("--json", type=Path, help="write the summary here")
    args = parser.parse_args(argv)

    calls = extract_calls(args.journal or default_journals(), args.since, args.until)
    if calls.empty:
        print("no buy/sell calls in the selected journal window")
        return 1
    independent = dedupe(calls, args.dedup_hours)
    agree = (calls["aggregator"] == calls["side"].map({1: "LONG", -1: "SHORT"})).mean()
    print(
        f"calls: {len(calls)} ({(calls['side'] > 0).mean():.0%} buy) "
        f"{calls['ts'].min():%Y-%m-%d} -> {calls['ts'].max():%Y-%m-%d} | "
        f"independent after {args.dedup_hours:g}h de-dup: {len(independent)} | "
        f"same direction as rule aggregator: {agree:.0%}"
    )
    scored = score_calls(independent, HORIZONS, args.baseline_days)
    if scored.empty:
        print("no calls could be matched to local 5m klines")
        return 1
    summary = report(scored, args.horizon, args.cost_pct, args.by)
    if args.json:
        args.json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
